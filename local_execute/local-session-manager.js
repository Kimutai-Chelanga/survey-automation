// Local Chrome Session Manager - FIXED VERSION
// Manages persistent Chrome profiles for each account

import { MongoClient, ObjectId } from 'mongodb';
import fs from 'fs/promises';
import { existsSync } from 'fs';
import path from 'path';
import { spawn, execSync } from 'child_process';
import net from 'net';

/**
 * Find Chrome executable on the system
 */
function findChromeExecutable() {
    const possiblePaths = [
        '/usr/bin/google-chrome-stable',
        '/usr/bin/google-chrome',
        '/usr/bin/chromium',
        '/usr/bin/chromium-browser',
        '/snap/bin/chromium',
    ];
    
    for (const chromePath of possiblePaths) {
        if (existsSync(chromePath)) {
            console.log(`Found Chrome at: ${chromePath}`);
            return chromePath;
        }
    }
    
    try {
        const result = execSync('which google-chrome-stable', { encoding: 'utf8' }).trim();
        if (result && existsSync(result)) {
            console.log(`Found Chrome via which: ${result}`);
            return result;
        }
    } catch (e) {
        // Ignore
    }
    
    throw new Error(
        'Chrome not found. Install with:\n' +
        'apt-get update && apt-get install -y google-chrome-stable'
    );
}

/**
 * Check if a port is available
 */
async function isPortAvailable(port) {
    return new Promise((resolve) => {
        const server = net.createServer();
        
        server.once('error', (err) => {
            if (err.code === 'EADDRINUSE') {
                resolve(false);
            } else {
                resolve(false);
            }
        });
        
        server.once('listening', () => {
            server.close();
            resolve(true);
        });
        
        server.listen(port, '0.0.0.0');
    });
}

export class LocalChromeSessionManager {
    constructor() {
        this.mongoUri = process.env.MONGODB_URI || 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin';
        this.dbName = process.env.MONGODB_DB_NAME || 'messages_db';
        this.baseProfileDir = process.env.CHROME_PROFILES_BASE_DIR || '/opt/airflow/chrome_profiles';
        this.chromeDebugPortStart = parseInt(process.env.CHROME_DEBUG_PORT_START || '9223');
        
        this.chromeExecutable = findChromeExecutable();
        
        this.mongoClient = null;
        this.mongodb = null;
        this.activeSessions = new Map();
    }

    async connect() {
        this.mongoClient = new MongoClient(this.mongoUri);
        await this.mongoClient.connect();
        this.mongodb = this.mongoClient.db(this.dbName);
        console.log('LocalChromeSessionManager connected to MongoDB');
    }

    async disconnect() {
        if (this.mongoClient) {
            await this.mongoClient.close();
            console.log('LocalChromeSessionManager disconnected from MongoDB');
        }
    }

    /**
     * Create a local Chrome profile for an account
     */
    async createLocalProfile(accountId, username) {
        try {
            const profileId = `local_chrome_${accountId}_${Date.now()}`;
            const profilePath = path.join(this.baseProfileDir, profileId);
            
            // Create profile directory
            await fs.mkdir(profilePath, { recursive: true });
            await fs.chmod(profilePath, 0o777);
            
            console.log(`Created local Chrome profile: ${profilePath}`);
            
            // Store profile metadata in MongoDB
            const profileDoc = {
                profile_id: profileId,
                profile_type: 'local_chrome',
                profile_path: profilePath,
                postgres_account_id: accountId,
                username: username,
                created_at: new Date(),
                is_active: true,
                last_used_at: new Date(),
                usage_count: 0,
                debug_port: null,
                sessions_created: 0,
                profile_settings: {
                    screen_width: 1920,
                    screen_height: 1080,
                    user_agent: null,
                    disable_images: false,
                    disable_javascript: false
                }
            };
            
            const result = await this.mongodb.collection('chrome_profiles_local').insertOne(profileDoc);
            
            console.log(`Stored profile metadata in MongoDB: ${result.insertedId}`);
            
            return {
                success: true,
                profile_id: profileId,
                profile_path: profilePath,
                mongodb_id: result.insertedId.toString()
            };
            
        } catch (error) {
            console.error('Error creating local profile:', error.message);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Start a Chrome session for a profile - FIXED VERSION
     */
    async startSession(profileId, accountId, sessionConfig = {}) {
        try {
            // Get profile from MongoDB
            const profile = await this.mongodb.collection('chrome_profiles_local').findOne({
                profile_id: profileId
            });
            
            if (!profile) {
                throw new Error(`Profile not found: ${profileId}`);
            }
            
            const profilePath = profile.profile_path;
            
            // CRITICAL FIX 1: Clean up ALL lock files before starting
            await this.cleanupProfileLocks(profilePath);
            
            // CRITICAL FIX 2: Kill any existing Chrome processes using this profile
            await this.killExistingChromeProcesses(profilePath);
            
            // CRITICAL FIX 3: Find truly available debug port
            const debugPort = await this.findTrulyAvailablePort();
            console.log(`Using debug port: ${debugPort}`);
            
            // Generate session ID
            const sessionId = `session_${profileId}_${Date.now()}`;
            
            // CRITICAL FIX 4: Enhanced Chrome arguments for better stability
            const chromeArgs = [
                '--no-sandbox',
                '--disable-setuid-sandbox',
                '--disable-gpu',
                '--disable-dev-shm-usage',
                '--disable-software-rasterizer',
                `--user-data-dir=${profilePath}`,
                `--remote-debugging-port=${debugPort}`,
                '--remote-debugging-address=0.0.0.0',
                '--remote-allow-origins=*',
                '--disable-features=UseOzonePlatform,VizDisplayCompositor',
                '--window-size=1920,1080',
                '--start-maximized',
                '--disable-background-timer-throttling',
                '--disable-backgrounding-occluded-windows',
                '--disable-renderer-backgrounding',
                '--no-first-run',
                '--disable-default-apps',
                '--disable-translate',
                '--disable-sync',
                '--disable-extensions-except=' + (sessionConfig.extensionPath || ''),
                '--load-extension=' + (sessionConfig.extensionPath || ''),
                '--aggressive-cache-discard',
                '--disable-cache',
                '--disable-application-cache',
                '--disable-offline-load-stale-cache',
                '--disk-cache-size=0',
                // Force X11 (don't use Wayland)
                '--use-gl=desktop',
                '--enable-features=UseOzonePlatform',
                '--ozone-platform=x11'
            ];
            
            // Add start URL
            const startUrl = sessionConfig.startUrl || 'https://twitter.com';
            chromeArgs.push(startUrl);
            
            console.log(`Starting Chrome with args:`, chromeArgs.slice(0, 10).join(' '), '...');
            
            // CRITICAL FIX 5: Spawn Chrome with proper environment
            const chromeProcess = spawn(this.chromeExecutable, chromeArgs, {
                detached: false, // Changed to false for better control
                stdio: ['ignore', 'pipe', 'pipe'], // Capture stdout/stderr
                env: {
                    ...process.env,
                    DISPLAY: process.env.DISPLAY || ':99',
                    CHROME_DEVEL_SANDBOX: '/usr/local/sbin/chrome-devel-sandbox'
                }
            });
            
            // Log Chrome output for debugging
            chromeProcess.stdout?.on('data', (data) => {
                console.log(`Chrome stdout: ${data.toString().trim()}`);
            });
            
            chromeProcess.stderr?.on('data', (data) => {
                console.error(`Chrome stderr: ${data.toString().trim()}`);
            });
            
            chromeProcess.on('error', (error) => {
                console.error(`Chrome process error: ${error.message}`);
            });
            
            chromeProcess.on('exit', (code, signal) => {
                console.log(`Chrome process exited with code ${code}, signal ${signal}`);
            });
            
            const chromePid = chromeProcess.pid;
            console.log(`Chrome PID: ${chromePid}`);
            
            // CRITICAL FIX 6: Wait longer and verify Chrome is actually running
            console.log('Waiting for Chrome to initialize...');
            await this.sleep(8000); // Increased wait time
            
            // Verify Chrome process is still running
            try {
                process.kill(chromePid, 0); // Signal 0 checks if process exists
                console.log('Chrome process is running');
            } catch (e) {
                throw new Error('Chrome process died immediately after starting');
            }
            
            // CRITICAL FIX 7: Retry logic for WebSocket endpoint
            let wsEndpoint = null;
            let retries = 0;
            const maxRetries = 10;
            
            while (!wsEndpoint && retries < maxRetries) {
                console.log(`Checking for WebSocket endpoint (attempt ${retries + 1}/${maxRetries})...`);
                wsEndpoint = await this.getChromeWebSocketEndpoint(debugPort);
                
                if (!wsEndpoint) {
                    retries++;
                    await this.sleep(2000);
                }
            }
            
            if (!wsEndpoint) {
                // Kill the Chrome process since it's not usable
                try {
                    process.kill(chromePid, 'SIGKILL');
                } catch (e) {
                    // Ignore
                }
                
                throw new Error(
                    `Chrome started but WebSocket endpoint not available after ${maxRetries} attempts. ` +
                    `Port ${debugPort} may be blocked or Chrome may have crashed.`
                );
            }
            
            console.log(`WebSocket endpoint found: ${wsEndpoint}`);
            
            // Store session in MongoDB
            const sessionDoc = {
                session_id: sessionId,
                profile_id: profileId,
                postgres_account_id: accountId,
                account_username: profile.username,
                session_type: 'local_chrome',
                debug_port: debugPort,
                ws_endpoint: wsEndpoint,
                browser_url: `http://localhost:${debugPort}`,
                chrome_pid: chromePid,
                is_active: true,
                session_status: 'active',
                created_at: new Date(),
                started_at: new Date(),
                session_purpose: sessionConfig.purpose || 'Manual Session',
                session_config: sessionConfig
            };
            
            await this.mongodb.collection('browser_sessions').insertOne(sessionDoc);
            
            // Update profile usage
            await this.mongodb.collection('chrome_profiles_local').updateOne(
                { profile_id: profileId },
                {
                    $set: {
                        last_used_at: new Date(),
                        debug_port: debugPort
                    },
                    $inc: {
                        usage_count: 1,
                        sessions_created: 1
                    }
                }
            );
            
            // Track active session
            this.activeSessions.set(sessionId, {
                sessionId,
                profileId,
                debugPort,
                wsEndpoint,
                chromePid
            });
            
            console.log(`Successfully started Chrome session: ${sessionId} on port ${debugPort}`);
            
            return {
                success: true,
                session_id: sessionId,
                debug_port: debugPort,
                ws_endpoint: wsEndpoint,
                browser_url: `http://localhost:${debugPort}`,
                chrome_pid: chromePid
            };
            
        } catch (error) {
            console.error('Error starting Chrome session:', error.message);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * CRITICAL FIX: Clean up profile lock files
     */
    async cleanupProfileLocks(profilePath) {
        const lockFiles = [
            'SingletonLock',
            'SingletonSocket',
            'SingletonCookie',
            'lockfile'
        ];
        
        for (const lockFile of lockFiles) {
            const lockPath = path.join(profilePath, lockFile);
            try {
                await fs.unlink(lockPath);
                console.log(`Removed lock file: ${lockFile}`);
            } catch (e) {
                // Ignore if doesn't exist
            }
        }
    }

    /**
     * CRITICAL FIX: Kill existing Chrome processes using this profile
     */
    async killExistingChromeProcesses(profilePath) {
        try {
            // Find Chrome processes using this profile directory
            const psOutput = execSync(
                `ps aux | grep "${profilePath}" | grep -v grep || true`,
                { encoding: 'utf8' }
            );
            
            if (psOutput.trim()) {
                console.log('Found existing Chrome processes using this profile:');
                console.log(psOutput);
                
                // Extract PIDs and kill them
                const lines = psOutput.trim().split('\n');
                for (const line of lines) {
                    const parts = line.trim().split(/\s+/);
                    if (parts.length > 1) {
                        const pid = parseInt(parts[1]);
                        if (pid) {
                            try {
                                process.kill(pid, 'SIGKILL');
                                console.log(`Killed existing Chrome process: ${pid}`);
                            } catch (e) {
                                console.log(`Could not kill process ${pid}: ${e.message}`);
                            }
                        }
                    }
                }
                
                // Wait for processes to die
                await this.sleep(2000);
            }
        } catch (e) {
            console.log('Error checking for existing Chrome processes:', e.message);
        }
    }

    /**
     * CRITICAL FIX: Find truly available port by testing actual connectivity
     */
    async findTrulyAvailablePort() {
        const activeSessions = await this.mongodb.collection('browser_sessions').find({
            is_active: true,
            session_type: 'local_chrome'
        }).toArray();
        
        const usedPorts = new Set(
            activeSessions
                .map(s => s.debug_port)
                .filter(p => p)
        );
        
        let port = this.chromeDebugPortStart;
        let attempts = 0;
        const maxAttempts = 100;
        
        while (attempts < maxAttempts) {
            if (!usedPorts.has(port)) {
                // Actually test if port is available
                const available = await isPortAvailable(port);
                if (available) {
                    return port;
                }
            }
            port++;
            attempts++;
        }
        
        throw new Error(`Could not find available port after ${maxAttempts} attempts`);
    }

    /**
     * Get Chrome WebSocket endpoint for a debug port
     */
    async getChromeWebSocketEndpoint(debugPort) {
        try {
            const controller = new AbortController();
            const timeout = setTimeout(() => controller.abort(), 5000);
            
            const response = await fetch(`http://localhost:${debugPort}/json/version`, {
                signal: controller.signal
            });
            
            clearTimeout(timeout);
            
            if (!response.ok) {
                return null;
            }
            
            const data = await response.json();
            return data.webSocketDebuggerUrl || null;
        } catch (error) {
            console.error(`Could not get WebSocket endpoint for port ${debugPort}:`, error.message);
            return null;
        }
    }

    /**
     * Stop a Chrome session
     */
    async stopSession(sessionId) {
        try {
            const session = await this.mongodb.collection('browser_sessions').findOne({
                session_id: sessionId
            });
            
            if (!session) {
                throw new Error(`Session not found: ${sessionId}`);
            }
            
            const chromePid = session.chrome_pid;
            
            if (chromePid) {
                try {
                    process.kill(chromePid, 'SIGTERM');
                    await this.sleep(3000);
                    
                    try {
                        process.kill(chromePid, 0);
                        // Still running, force kill
                        process.kill(chromePid, 'SIGKILL');
                    } catch (e) {
                        // Already dead
                    }
                } catch (e) {
                    console.warn(`Could not kill Chrome process ${chromePid}: ${e.message}`);
                }
            }
            
            await this.mongodb.collection('browser_sessions').updateOne(
                { session_id: sessionId },
                {
                    $set: {
                        is_active: false,
                        session_status: 'stopped',
                        ended_at: new Date()
                    }
                }
            );
            
            this.activeSessions.delete(sessionId);
            
            console.log(`Stopped Chrome session: ${sessionId}`);
            
            return {
                success: true,
                message: `Session ${sessionId} stopped successfully`
            };
            
        } catch (error) {
            console.error('Error stopping Chrome session:', error.message);
            return {
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Get all active sessions for an account
     */
    async getActiveSessionsForAccount(accountId) {
        try {
            const sessions = await this.mongodb.collection('browser_sessions').find({
                postgres_account_id: accountId,
                is_active: true,
                session_type: 'local_chrome'
            }).toArray();
            
            return sessions;
            
        } catch (error) {
            console.error('Error getting active sessions:', error.message);
            return [];
        }
    }

    /**
     * Cleanup stale sessions
     */
    async cleanupStaleSessions(maxAgeHours = 24) {
        try {
            const cutoffTime = new Date();
            cutoffTime.setHours(cutoffTime.getHours() - maxAgeHours);
            
            const staleSessions = await this.mongodb.collection('browser_sessions').find({
                is_active: true,
                session_type: 'local_chrome',
                created_at: { $lt: cutoffTime }
            }).toArray();
            
            for (const session of staleSessions) {
                await this.stopSession(session.session_id);
            }
            
            console.log(`Cleaned up ${staleSessions.length} stale sessions`);
            
            return {
                success: true,
                cleaned: staleSessions.length
            };
            
        } catch (error) {
            console.error('Error cleaning up stale sessions:', error.message);
            return {
                success: false,
                error: error.message
            };
        }
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

export default LocalChromeSessionManager;