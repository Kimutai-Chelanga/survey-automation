// modules/chrome/ChromeSessionManager.js

/**
 * Manages Chrome sessions with persistent pages to prevent Chrome from exiting
 * 
 * FIX HISTORY (2026-02-20):
 *   - Added --disable-background-timer-throttling to prevent extension suspension
 *   - Added --disable-backgrounding-occluded-windows to keep pages active
 *   - Added --disable-renderer-backgrounding to prevent background throttling
 *   - Added --keep-alive-for-test to ensure Chrome stays running
 *   - Now creates a persistent blank page immediately after launch
 */

export class ChromeSessionManager {
    constructor(config) {
        this.config = config;
        this.sessions = new Map();
        this.debugPort = config.debugPort || 9222;
        this.display = config.display || 99;
        this.chromeExecutable = config.chromeExecutable || '/usr/bin/google-chrome-stable';
        this.downloadsDir = config.downloadsDir || '/opt/airflow/workspace/downloads';
    }

    async startSession(account, sessionId) {
        console.log(`\nStarting Chrome session for: ${account.username}`);
        
        const { profilePath } = account;
        
        // Ensure profile directory exists
        await this.ensureProfileDir(profilePath);
        
        // Check if Chrome is already running
        const existingChrome = await this.checkExistingChrome();
        if (existingChrome) {
            console.log('Chrome already running - will connect to existing instance');
            return this.connectToExisting(account, sessionId);
        }
        
        console.log('Starting NEW Chrome instance...');
        
        // Patch Chrome preferences for download directory
        await this.patchChromePreferences(profilePath);
        
        // Start Xvfb if not already running
        await this.startXvfb();
        
        // Build Chrome arguments with ALL stability flags
        const chromeArgs = [
            `--user-data-dir=${profilePath}`,
            `--remote-debugging-port=${this.debugPort}`,
            '--no-sandbox',
            '--disable-dev-shm-usage',
            '--disable-gpu',
            '--disable-setuid-sandbox',
            '--disable-software-rasterizer',
            '--disable-web-security',
            '--disable-features=VizDisplayCompositor',
            '--disable-features=TranslateUI',
            '--disable-features=ChromeWhatsNewUI',
            '--disable-background-timer-throttling',      // CRITICAL: Prevents extension suspension
            '--disable-backgrounding-occluded-windows',   // CRITICAL: Keeps pages active when occluded
            '--disable-renderer-backgrounding',           // CRITICAL: Prevents renderer throttling
            '--disable-renderer-backgrounding-throttling',
            '--disable-timer-throttling-for-background-tabs',
            '--disable-ipc-flooding-protection',
            '--disable-hang-monitor',
            '--disable-prompt-on-repost',
            '--disable-sync',
            '--disable-client-side-phishing-detection',
            '--disable-component-update',
            '--disable-default-apps',
            '--disable-notifications',
            '--disable-logging',
            '--disable-breakpad',
            '--disable-crash-reporter',
            '--disable-speech-api',
            '--disable-save-password-bubble',
            '--disable-session-crashed-bubble',
            '--disable-ntp-most-likely-favicons',
            '--disable-ntp-popular-sites',
            '--disable-ntp-remote-suggestions',
            '--disable-ntp-snippets',
            '--disable-offer-store-unmasked-wallet-cards',
            '--disable-offer-upload-credit-cards',
            '--disable-password-generation',
            '--disable-prompt-on-repost',
            '--disable-remote-fonts',
            '--disable-remote-playback-api',
            '--disable-resize-lock',
            '--disable-restore-background-contents',
            '--disable-sync-standalone-transport',
            '--disable-tab-groups',
            '--disable-tab-group-suggestions',
            '--disable-threaded-scrolling',
            '--disable-translate',
            '--disable-v8-idle-tasks',
            '--disable-validation-work-for-client-side-detection',
            '--disable-wake-on-wifi',
            '--disable-web-resource-scheduler',
            '--disable-webaudio',
            '--disable-webgl',
            '--disable-webgl2',
            '--enable-features=NetworkService,NetworkServiceInProcess',
            '--force-color-profile=srgb',
            '--metrics-recording-only',
            '--no-first-run',
            '--no-default-browser-check',
            '--no-pings',
            '--no-zygote',
            '--password-store=basic',
            '--use-mock-keychain',
            '--aggressive-cache-discard',
            '--aggressive-tab-discard',
            '--allow-running-insecure-content',
            '--autoplay-policy=no-user-gesture-required',
            '--disk-cache-dir=/tmp/chrome-cache',
            '--disk-cache-size=104857600',
            '--max-old-space-size=2048',
            '--shm-size=2gb',
            `--window-size=1920,1080`,
            `--display=:${this.display}`,
            '--kiosk',
            '--start-maximized',
            // CRITICAL: Ensures Chrome stays alive even with no visible windows
            '--keep-alive-for-test',
            'about:blank'  // Start with a blank page
        ];

        console.log('[DEBUG] Starting Chrome with args:');
        console.log(`[DEBUG]   Executable: ${this.chromeExecutable}`);
        console.log(`[DEBUG]   Profile: ${profilePath}`);
        console.log(`[DEBUG]   Debug Port: ${this.debugPort}`);
        console.log(`[DEBUG]   Display: :${this.display}`);

        // Spawn Chrome process
        const { spawn } = await import('child_process');
        const chromeProcess = spawn(this.chromeExecutable, chromeArgs, {
            detached: true,
            stdio: ['ignore', 'pipe', 'pipe'],
            env: {
                ...process.env,
                DISPLAY: `:${this.display}`,
                TZ: this.config.timezone || 'Africa/Nairobi',
            }
        });

        // Log Chrome output for debugging
        chromeProcess.stdout.on('data', (data) => {
            const msg = data.toString().trim();
            if (msg) console.log(`[Chrome] ${msg}`);
        });

        chromeProcess.stderr.on('data', (data) => {
            const msg = data.toString().trim();
            if (msg && !msg.includes('ERROR:dbus') && !msg.includes('machine-id')) {
                console.log(`[Chrome] ${msg}`);
            }
        });

        chromeProcess.on('exit', (code, signal) => {
            console.log(`[Chrome Process Exit] Code: ${code}, Signal: ${signal}`);
            const session = this.sessions.get(sessionId);
            if (session) {
                session.chromeExited = true;
            }
        });

        console.log('[DEBUG] Waiting 8s for Chrome to initialize...');
        await this.sleep(8000);

        // Wait for Chrome debug port to be ready
        const browser = await this.waitForChromeDebugPort();
        
        // CRITICAL FIX: Create a persistent blank page to keep Chrome alive
        console.log('\n🔧 Creating persistent blank page to keep Chrome alive...');
        const pages = await browser.pages();
        
        let persistentPage;
        if (pages.length === 0) {
            persistentPage = await browser.newPage();
        } else {
            persistentPage = pages[0];
        }
        
        // Navigate to about:blank to ensure a clean page
        await persistentPage.goto('about:blank', { waitUntil: 'domcontentloaded' });
        console.log('✓ Persistent blank page created - Chrome will stay alive');
        
        // Set download behavior via CDP
        const cdp = await persistentPage.target().createCDPSession();
        await cdp.send('Browser.setDownloadBehavior', {
            behavior: 'allow',
            downloadPath: this.downloadsDir
        });
        console.log('✓ CDP download dir set →', this.downloadsDir);

        // Store session info
        this.sessions.set(sessionId, {
            browser,
            chromeProcess,
            profilePath,
            username: account.username,
            displayNum: this.display,
            startTime: new Date(),
            persistentPage,  // Store reference to persistent page
            cdp
        });

        console.log('✓ Chrome session started successfully');
        
        return {
            browser,
            displayNum: this.display,
            persistentPage
        };
    }

    async waitForChromeDebugPort(maxAttempts = 10, delayMs = 1000) {
        const { default: puppeteer } = await import('puppeteer-core');
        
        console.log(`[DEBUG] Waiting for Chrome debug port at: http://localhost:${this.debugPort}/json/version`);
        
        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            try {
                console.log(`[DEBUG] Fetch attempt ${attempt}/${maxAttempts}...`);
                
                const response = await fetch(`http://localhost:${this.debugPort}/json/version`);
                if (response.ok) {
                    const data = await response.json();
                    console.log(`[DEBUG] Response status: ${response.status}`);
                    console.log(`✓ Chrome responding: ${data.Browser}`);
                    console.log(`[DEBUG] WebSocket debugger URL: ${data.webSocketDebuggerUrl}`);
                    
                    console.log(`[DEBUG] Connecting Puppeteer to: http://localhost:${this.debugPort}`);
                    const browser = await puppeteer.connect({
                        browserURL: `http://localhost:${this.debugPort}`,
                        defaultViewport: null,
                    });
                    
                    return browser;
                }
            } catch (error) {
                if (attempt === maxAttempts) {
                    throw new Error(`Chrome debug port not ready after ${maxAttempts} attempts: ${error.message}`);
                }
                console.log(`[DEBUG] Attempt ${attempt} failed, retrying in ${delayMs}ms...`);
                await this.sleep(delayMs);
            }
        }
        
        throw new Error('Failed to connect to Chrome debug port');
    }

    async checkExistingChrome() {
        try {
            const response = await fetch(`http://localhost:${this.debugPort}/json/version`);
            return response.ok;
        } catch {
            return false;
        }
    }

    async connectToExisting(account, sessionId) {
        const { default: puppeteer } = await import('puppeteer-core');
        
        const browser = await puppeteer.connect({
            browserURL: `http://localhost:${this.debugPort}`,
            defaultViewport: null,
        });

        // CRITICAL: Ensure there's at least one page open
        const pages = await browser.pages();
        if (pages.length === 0) {
            const page = await browser.newPage();
            await page.goto('about:blank');
        }

        this.sessions.set(sessionId, {
            browser,
            profilePath: account.profilePath,
            username: account.username,
            displayNum: this.display,
            startTime: new Date(),
            isReused: true
        });

        return { browser, displayNum: this.display };
    }

    async ensureProfileDir(profilePath) {
        const { promises: fs } = await import('fs');
        try {
            await fs.mkdir(profilePath, { recursive: true, mode: 0o777 });
            await fs.chmod(profilePath, 0o777);
        } catch (error) {
            console.warn(`⚠ Could not create/chmod profile dir: ${error.message}`);
        }
    }

    async patchChromePreferences(profilePath) {
        const { promises: fs } = await import('fs');
        const prefsPath = `${profilePath}/Default/Preferences`;
        
        try {
            let prefs = {};
            try {
                const data = await fs.readFile(prefsPath, 'utf8');
                prefs = JSON.parse(data);
            } catch {
                // Preferences file doesn't exist yet
            }

            // Set download preferences
            prefs.download = prefs.download || {};
            prefs.download.default_directory = this.downloadsDir;
            prefs.download.prompt_for_download = false;
            prefs.download.directory_upgrade = true;
            
            // Disable various prompts
            prefs.credentials_enable_service = false;
            prefs.profile = prefs.profile || {};
            prefs.profile.password_manager_enabled = false;
            
            // Ensure directory exists
            await fs.mkdir(`${profilePath}/Default`, { recursive: true, mode: 0o777 });
            await fs.writeFile(prefsPath, JSON.stringify(prefs, null, 2));
            await fs.chmod(prefsPath, 0o666);
            
            console.log('✓ Preferences patched → download dir:', this.downloadsDir);
        } catch (error) {
            console.warn(`⚠ Could not patch Chrome preferences: ${error.message}`);
        }
    }

    async startXvfb() {
        const { spawn } = await import('child_process');
        const { promises: fs } = await import('fs');
        
        // Check if Xvfb is already running on this display
        try {
            const { execSync } = await import('child_process');
            execSync(`xdpyinfo -display :${this.display} >/dev/null 2>&1`);
            console.log(`✓ Xvfb already running on display :${this.display}`);
            return;
        } catch {
            // Xvfb not running, start it
        }

        // Ensure /tmp/.X11-unix exists
        await fs.mkdir('/tmp/.X11-unix', { recursive: true, mode: 0o1777 }).catch(() => {});

        console.log(`Starting Xvfb on display :${this.display}...`);
        
        const xvfb = spawn('Xvfb', [
            `:${this.display}`,
            '-screen', '0', '1920x1080x24',
            '-nolisten', 'tcp',
            '-ac'
        ], {
            detached: true,
            stdio: ['ignore', 'pipe', 'pipe']
        });

        xvfb.stderr.on('data', (data) => {
            const msg = data.toString().trim();
            if (msg && !msg.includes('xinerama') && !msg.includes('composite')) {
                console.log(`[Xvfb] ${msg}`);
            }
        });

        // Wait for Xvfb to start
        await this.sleep(2000);
        
        // Verify Xvfb is running
        try {
            const { execSync } = await import('child_process');
            execSync(`xdpyinfo -display :${this.display} >/dev/null 2>&1`);
            console.log('✓ Xvfb started');
        } catch (error) {
            console.warn(`⚠ Xvfb may not have started properly: ${error.message}`);
        }
    }

    async stopSession(sessionId) {
        const session = this.sessions.get(sessionId);
        if (!session) {
            console.log(`No session found for ID: ${sessionId}`);
            return;
        }

        console.log(`Stopping Chrome session: ${sessionId}`);

        try {
            // Close browser but keep persistent page if it's the only one?
            if (session.browser) {
                await session.browser.close();
                console.log('  ✓ Browser disconnected');
            }
        } catch (error) {
            console.warn(`  ⚠ Error closing browser: ${error.message}`);
        }

        // Kill Chrome process if it's still running
        if (session.chromeProcess && !session.chromeExited) {
            try {
                session.chromeProcess.kill('SIGTERM');
                await this.sleep(2000);
                if (!session.chromeExited) {
                    session.chromeProcess.kill('SIGKILL');
                }
                console.log('  ✓ Chrome terminated');
            } catch (error) {
                console.warn(`  ⚠ Error killing Chrome: ${error.message}`);
            }
        }

        // Clean up lock files
        try {
            const { promises: fs } = await import('fs');
            const files = await fs.readdir(session.profilePath).catch(() => []);
            for (const file of files) {
                if (file.includes('Singleton') || file.includes('.lock')) {
                    await fs.unlink(`${session.profilePath}/${file}`).catch(() => {});
                }
            }
        } catch (error) {
            // Ignore errors
        }

        this.sessions.delete(sessionId);
        console.log('✓ Session cleaned up');
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
