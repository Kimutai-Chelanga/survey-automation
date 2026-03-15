import { Hyperbrowser } from '@hyperbrowser/sdk';
import puppeteer from 'puppeteer-core';

class HyperbrowserService {
    constructor(config) {
        this.config = config.hyperbrowser;
        this.client = null;
        this.activeSessions = new Set();
        this.sessionDetails = new Map();
        this.initialize();
    }

    initialize() {
        if (!this.config.apiKey) {
            throw new Error('Hyperbrowser API key is required');
        }
        this.client = new Hyperbrowser({
            apiKey: this.config.apiKey,
            baseUrl: this.config.baseUrl || 'https://api.hyperbrowser.ai'
        });
        console.log('Hyperbrowser client initialized');
    }

    async createSession(sessionOptions) {
        const sessionParams = {
            screen: sessionOptions.screen || this.config.sessionConfig?.screen,
            use_stealth: sessionOptions.use_stealth ?? this.config.sessionConfig?.use_stealth,
            profile: {
                id: sessionOptions.profileId,
                persist_changes: sessionOptions.persistChanges ?? true
            },
            browser_type: sessionOptions.browserType || this.config.sessionConfig?.browser_type,
            timeout: sessionOptions.timeout || this.config.defaultTimeout
        };

        // FIXED: Use correct parameter names (camelCase) for recording
        // For video recording, BOTH parameters must be true
        if (sessionOptions.enableVideoRecording) {
            sessionParams.enableWebRecording = true;
            sessionParams.enableVideoWebRecording = true;
            console.log('✓ Video recording enabled (both web and video)');
        } else if (sessionOptions.enableWebRecording) {
            sessionParams.enableWebRecording = true;
            console.log('✓ Web recording enabled');
        }

        // Handle extension configuration
        if (sessionOptions.extensionId) {
            const ext = sessionOptions.extensionId;
            sessionParams.extension_ids = [ext];
            sessionParams.extensions = [{ id: ext, enabled: true }];
            
            // Additional chrome flags for extension support
            sessionParams.chrome_flags = [
                `--disable-extensions-except=/extensions/${ext}`,
                `--load-extension=/extensions/${ext}`
            ];
        }

        console.log('Creating session with params:', JSON.stringify(sessionParams, null, 2));

        const sessionResp = await this.client.sessions.create(sessionParams);
        if (!sessionResp || !sessionResp.id) {
            throw new Error('Failed to create session - no session ID returned');
        }

        this.activeSessions.add(sessionResp.id);
        this.sessionDetails.set(sessionResp.id, sessionResp);

        console.log(`Session created: ${sessionResp.id}`);
        console.log(`WebSocket Endpoint: ${sessionResp.wsEndpoint || 'Not available'}`);
        
        // Log recording status
        if (sessionParams.enableWebRecording) {
            console.log(`Recording enabled: Web=${sessionParams.enableWebRecording}, Video=${sessionParams.enableVideoWebRecording || false}`);
        }

        return {
            sessionId: sessionResp.id,
            wsEndpoint: sessionResp.wsEndpoint,
            raw: sessionResp
        };
    }

    async connectToBrowser(sessionInfo) {
        if (!sessionInfo.wsEndpoint) {
            throw new Error(`No wsEndpoint available for session ${sessionInfo.sessionId}`);
        }

        console.log(`Connecting puppeteer to session ${sessionInfo.sessionId}`);
        console.log(`WebSocket endpoint: ${sessionInfo.wsEndpoint}`);

        try {
            const browser = await puppeteer.connect({
                browserWSEndpoint: sessionInfo.wsEndpoint,
                defaultViewport: null,
                slowMo: 50 // Add slight delay for stability
            });

            console.log('✓ Puppeteer successfully connected to browser');
            return browser;

        } catch (error) {
            console.error('Failed to connect puppeteer to browser:', error.message);
            throw new Error(`Browser connection failed: ${error.message}`);
        }
    }

    async navigate(sessionId, url) {
        const session = this.sessionDetails.get(sessionId);
        if (!session) {
            throw new Error(`Session ${sessionId} not found`);
        }

        console.log(`Navigating session ${sessionId} to: ${url}`);
        
        try {
            // Use the SDK's navigation method if available
            if (this.client.sessions.navigate) {
                await this.client.sessions.navigate(sessionId, { url });
            } else {
                // Fallback: this would need the session to have a page reference
                console.warn('Direct navigation via SDK not available, use puppeteer page.goto() instead');
            }
        } catch (error) {
            console.error(`Navigation error: ${error.message}`);
            throw error;
        }
    }

    async stopSession(sessionId) {
        if (!this.activeSessions.has(sessionId)) {
            console.log(`Session ${sessionId} already stopped or not found`);
            return;
        }

        try {
            console.log(`Stopping session ${sessionId}...`);
            await this.client.sessions.stop(sessionId);
            console.log(`✓ Session ${sessionId} stopped successfully`);
            
            // IMPORTANT: Wait for recording to finalize after stopping
            console.log('Waiting 5s for recording finalization...');
            await this.sleep(5000);
            
        } catch (e) {
            console.error(`Error stopping session ${sessionId}:`, e.message);
        } finally {
            this.activeSessions.delete(sessionId);
            this.sessionDetails.delete(sessionId);
        }
    }

    async getSessionInfo(sessionId) {
        return this.sessionDetails.get(sessionId);
    }

    async listActiveSessions() {
        return Array.from(this.activeSessions);
    }

    async cleanup() {
        console.log('Cleaning up all active sessions...');
        const sessionIds = Array.from(this.activeSessions);
        
        for (const sid of sessionIds) {
            await this.stopSession(sid);
        }
        
        console.log('✓ All sessions cleaned up');
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

export default HyperbrowserService;