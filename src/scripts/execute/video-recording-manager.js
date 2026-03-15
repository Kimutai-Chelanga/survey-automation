// Fixed VideoRecordingManager with proper Hyperbrowser SDK integration
// CORRECTED: Based on official Hyperbrowser documentation

import { EventEmitter } from 'events';
import fs from 'fs/promises';
import path from 'path';

// Proper Hyperbrowser SDK import based on documentation
let Hyperbrowser;
let hyperbrowserAvailable = false;

try {
    const hyperbrowserModule = await import('@hyperbrowser/sdk');
    Hyperbrowser = hyperbrowserModule.Hyperbrowser;
    hyperbrowserAvailable = true;
    console.log('Hyperbrowser SDK loaded successfully');
} catch (sdkError) {
    console.warn('Hyperbrowser SDK not available, using mock implementation');
    // Mock implementation for development/testing
    Hyperbrowser = class MockHyperbrowser {
        constructor(options) {
            this.apiKey = options.apiKey;
            console.log('Using mock Hyperbrowser implementation');
        }
        
        get sessions() {
            return {
                create: async (options) => ({
                    id: `mock_${Date.now()}`,
                    status: 'active',
                    wsEndpoint: `ws://mock-browser/${Date.now()}`
                }),
                getRecordingURL: async (sessionId) => ({
                    status: 'completed',
                    recordingUrl: `mock://recording/${sessionId}`
                }),
                getVideoRecordingURL: async (sessionId) => ({
                    status: 'completed',
                    recordingUrl: `mock://video-recording/${sessionId}`
                }),
                stop: async (sessionId) => ({
                    id: sessionId,
                    status: 'stopped'
                })
            };
        }
    };
    hyperbrowserAvailable = false;
}

/**
 * FIXED: VideoRecordingManager with proper Hyperbrowser SDK integration
 */
export class VideoRecordingManager extends EventEmitter {
    constructor(apiKey, baseUrl = null) {
        super();
        this.apiKey = apiKey;
        this.baseUrl = baseUrl;
        this.client = null;
        this.recordings = new Map(); // sessionId -> recording data
        this.pollingIntervals = new Map(); // sessionId -> interval
        this.sdkAvailable = hyperbrowserAvailable;
        
        this.initializeClient();
    }

    initializeClient() {
        if (!this.apiKey) {
            console.warn('No API key provided for VideoRecordingManager');
            return;
        }

        try {
            // FIXED: Correct client initialization pattern from documentation
            this.client = new Hyperbrowser({
                apiKey: this.apiKey
            });
            
            if (this.sdkAvailable) {
                console.log('VideoRecordingManager initialized with real SDK');
            } else {
                console.log('VideoRecordingManager initialized with mock SDK');
            }
        } catch (error) {
            console.error('Failed to initialize Hyperbrowser client:', error.message);
            this.client = new Hyperbrowser({ apiKey: this.apiKey }); // Fallback to mock
        }
    }

    /**
     * FIXED: Enable video recording with correct parameter names
     * Based on official documentation: enableWebRecording and enableVideoWebRecording
     */
    enableVideoRecording(sessionParams) {
        return {
            ...sessionParams,
            // FIXED: Use correct parameter names from documentation
            enableWebRecording: true,
            enableVideoWebRecording: true // Required for video recording
        };
    }

    /**
     * FIXED: Start monitoring session for recording completion
     */
    startRecordingMonitor(sessionId, options = {}) {
        const { 
            pollInterval = 15000, // 15 seconds
            maxWaitTime = 600000, // 10 minutes
            onProgress = null,
            onComplete = null,
            onError = null
        } = options;

        console.log(`Starting recording monitor for session: ${sessionId}`);

        let elapsedTime = 0;
        const intervalId = setInterval(async () => {
            try {
                elapsedTime += pollInterval;
                
                if (elapsedTime >= maxWaitTime) {
                    this.stopRecordingMonitor(sessionId);
                    const error = `Recording monitor timeout after ${maxWaitTime/1000}s`;
                    console.error(error);
                    this.emit('recordingTimeout', { sessionId, error });
                    if (onError) onError(error);
                    return;
                }

                const status = await this.checkRecordingStatus(sessionId);
                
                if (onProgress) {
                    onProgress({ sessionId, status: status.status, elapsedTime });
                }

                this.emit('recordingProgress', { sessionId, status: status.status, elapsedTime });

                if (status.status === 'completed' && status.url) {
                    this.stopRecordingMonitor(sessionId);
                    console.log(`Recording completed for session: ${sessionId}`);
                    
                    const recordingData = {
                        sessionId,
                        status: 'completed',
                        webRecordingUrl: status.webRecordingUrl,
                        videoRecordingUrl: status.videoRecordingUrl,
                        completedAt: new Date().toISOString(),
                        elapsedTime,
                        rawResponse: status
                    };

                    this.recordings.set(sessionId, recordingData);
                    this.emit('recordingCompleted', recordingData);
                    
                    if (onComplete) onComplete(recordingData);

                } else if (status.status === 'failed' || status.status === 'error') {
                    this.stopRecordingMonitor(sessionId);
                    const error = status.error || 'Recording failed';
                    console.error(`Recording failed for session ${sessionId}: ${error}`);
                    this.emit('recordingFailed', { sessionId, error });
                    if (onError) onError(error);
                } else if (status.status === 'not_enabled') {
                    this.stopRecordingMonitor(sessionId);
                    const error = 'Recording was not enabled for this session';
                    console.error(`Recording not enabled for session ${sessionId}`);
                    this.emit('recordingFailed', { sessionId, error });
                    if (onError) onError(error);
                }

            } catch (error) {
                console.error(`Error monitoring recording for session ${sessionId}:`, error.message);
                this.emit('recordingMonitorError', { sessionId, error: error.message });
                
                if (error.message.includes('404') || error.message.includes('not found')) {
                    this.stopRecordingMonitor(sessionId);
                    if (onError) onError(`Session ${sessionId} not found`);
                }
            }
        }, pollInterval);

        this.pollingIntervals.set(sessionId, intervalId);
    }

    /**
     * FIXED: Check recording status using correct Hyperbrowser SDK methods
     * Uses getRecordingURL() and getVideoRecordingURL() from official SDK
     */
    async checkRecordingStatus(sessionId) {
        try {
            if (!this.client) {
                throw new Error('Hyperbrowser client not initialized');
            }

            // FIXED: Use correct SDK methods from documentation
            const webRecording = await this.client.sessions.getRecordingURL(sessionId);
            const videoRecording = await this.client.sessions.getVideoRecordingURL(sessionId);

            // Recording status can be: "not_enabled" | "pending" | "in_progress" | "completed" | "failed"
            const webStatus = webRecording.status || 'unknown';
            const videoStatus = videoRecording.status || 'unknown';
            
            // Overall status is the "worst" of the two
            let overallStatus = 'pending';
            if (webStatus === 'not_enabled' || videoStatus === 'not_enabled') {
                overallStatus = 'not_enabled';
            } else if (webStatus === 'failed' || videoStatus === 'failed') {
                overallStatus = 'failed';
            } else if (webStatus === 'completed' && videoStatus === 'completed') {
                overallStatus = 'completed';
            } else if (webStatus === 'in_progress' || videoStatus === 'in_progress') {
                overallStatus = 'in_progress';
            }

            return {
                status: overallStatus,
                url: webRecording.recordingUrl || videoRecording.recordingUrl,
                webRecordingUrl: webRecording.recordingUrl,
                videoRecordingUrl: videoRecording.recordingUrl,
                webStatus: webStatus,
                videoStatus: videoStatus,
                error: webRecording.error || videoRecording.error
            };

        } catch (error) {
            console.error(`Failed to check recording status for ${sessionId}:`, error.message);
            
            if (error.message.includes('404') || error.message.includes('not found')) {
                return {
                    status: 'not_found',
                    url: null,
                    error: 'Session not found or recording not enabled'
                };
            }
            
            // Mock response for development
            if (!this.sdkAvailable) {
                return {
                    status: 'completed',
                    url: `mock://recording/${sessionId}`,
                    webRecordingUrl: `mock://web-recording/${sessionId}`,
                    videoRecordingUrl: `mock://video-recording/${sessionId}`,
                    error: null
                };
            }
            
            return {
                status: 'error',
                url: null,
                error: error.message
            };
        }
    }

    /**
     * FIXED: Get recording URLs directly using correct SDK methods
     */
    async getRecordingUrls(sessionId) {
        try {
            const status = await this.checkRecordingStatus(sessionId);
            return {
                success: status.status === 'completed' && !!status.url,
                webRecordingUrl: status.webRecordingUrl,
                videoRecordingUrl: status.videoRecordingUrl,
                status: status.status,
                webStatus: status.webStatus,
                videoStatus: status.videoStatus,
                error: status.error
            };
        } catch (error) {
            return {
                success: false,
                webRecordingUrl: null,
                videoRecordingUrl: null,
                status: 'error',
                error: error.message
            };
        }
    }

    /**
     * FIXED: Get recording status for compatibility with orchestrator
     */
    async getRecordingStatus(sessionId) {
        try {
            const urls = await this.getRecordingUrls(sessionId);
            return {
                webRecording: {
                    success: urls.success,
                    status: urls.webStatus || urls.status,
                    url: urls.webRecordingUrl,
                    error: urls.error
                },
                videoRecording: {
                    success: urls.success,
                    status: urls.videoStatus || urls.status,
                    url: urls.videoRecordingUrl,
                    error: urls.error
                }
            };
        } catch (error) {
            return {
                webRecording: {
                    success: false,
                    status: 'error',
                    url: null,
                    error: error.message
                },
                videoRecording: {
                    success: false,
                    status: 'error',
                    url: null,
                    error: error.message
                }
            };
        }
    }

    /**
     * Stop monitoring a session's recording
     */
    stopRecordingMonitor(sessionId) {
        const intervalId = this.pollingIntervals.get(sessionId);
        if (intervalId) {
            clearInterval(intervalId);
            this.pollingIntervals.delete(sessionId);
            console.log(`Stopped recording monitor for session: ${sessionId}`);
        }
    }

    /**
     * Get stored recording data
     */
    getRecording(sessionId) {
        return this.recordings.get(sessionId);
    }

    /**
     * Get all stored recordings
     */
    getAllRecordings() {
        return Array.from(this.recordings.values());
    }

    /**
     * FIXED: Download recording with proper error handling
     */
    async downloadRecording(sessionId, outputDir = './recordings') {
        const recording = this.recordings.get(sessionId);
        if (!recording || recording.status !== 'completed') {
            throw new Error(`No completed recording found for session: ${sessionId}`);
        }

        const downloads = [];

        try {
            await fs.mkdir(outputDir, { recursive: true });

            // Download web recording
            if (recording.webRecordingUrl && !recording.webRecordingUrl.startsWith('mock://')) {
                const webPath = path.join(outputDir, `${sessionId}_web_recording.json`);
                await this.downloadFile(recording.webRecordingUrl, webPath);
                downloads.push({ 
                    type: 'web_recording', 
                    path: webPath, 
                    url: recording.webRecordingUrl 
                });
            }

            // Download video recording
            if (recording.videoRecordingUrl && !recording.videoRecordingUrl.startsWith('mock://')) {
                const videoPath = path.join(outputDir, `${sessionId}_video_recording.mp4`);
                await this.downloadFile(recording.videoRecordingUrl, videoPath);
                downloads.push({ 
                    type: 'video_recording', 
                    path: videoPath, 
                    url: recording.videoRecordingUrl 
                });
            }

            // Handle mock URLs
            if (recording.webRecordingUrl?.startsWith('mock://') || recording.videoRecordingUrl?.startsWith('mock://')) {
                console.log(`Mock recording URLs detected - skipping download`);
                downloads.push({ 
                    type: 'mock', 
                    path: 'mock', 
                    url: recording.webRecordingUrl || recording.videoRecordingUrl 
                });
            }

            console.log(`Recording download completed for session ${sessionId}:`, downloads);
            return downloads;

        } catch (error) {
            console.error(`Failed to download recordings for ${sessionId}:`, error.message);
            throw error;
        }
    }

    /**
     * Download file with modern fetch API
     */
    async downloadFile(url, outputPath) {
        try {
            // Use native fetch (Node.js 18+) or import node-fetch
            let fetchFunction;
            try {
                fetchFunction = fetch;
            } catch {
                const nodeFetch = await import('node-fetch');
                fetchFunction = nodeFetch.default;
            }

            const response = await fetchFunction(url);
            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const buffer = Buffer.from(await response.arrayBuffer());
            await fs.writeFile(outputPath, buffer);
            
            console.log(`Downloaded: ${url} -> ${outputPath}`);
            return outputPath;

        } catch (error) {
            console.error(`Download failed: ${url}`, error.message);
            throw error;
        }
    }

    /**
     * Cleanup - stop all monitors
     */
    cleanup() {
        for (const sessionId of this.pollingIntervals.keys()) {
            this.stopRecordingMonitor(sessionId);
        }
        this.recordings.clear();
        console.log('VideoRecordingManager cleaned up');
    }

    /**
     * Get current monitoring status
     */
    getMonitoringStatus() {
        return {
            sdkAvailable: this.sdkAvailable,
            activeMonitors: Array.from(this.pollingIntervals.keys()),
            totalRecordings: this.recordings.size,
            completedRecordings: Array.from(this.recordings.values())
                .filter(r => r.status === 'completed').length,
            apiKey: this.apiKey ? '***set***' : 'not set',
            clientInitialized: !!this.client
        };
    }
}

/**
 * FIXED: Hyperbrowser Recording Integration with correct SDK usage
 */
export class HyperbrowserRecordingIntegration {
    constructor(hyperbrowserService, apiKey) {
        this.hyperbrowserService = hyperbrowserService;
        this.apiKey = apiKey;
        this.activeRecordings = new Map();
    }

    /**
     * FIXED: Create session with proper recording parameters
     */
    async createSessionWithRecording(sessionOptions) {
        // FIXED: Use correct recording parameters from documentation
        const recordingOptions = {
            ...sessionOptions,
            enableWebRecording: true,
            enableVideoWebRecording: true // Required for video recording
        };

        const sessionResult = await this.hyperbrowserService.createSession(recordingOptions);
        
        if (sessionResult.sessionId) {
            this.activeRecordings.set(sessionResult.sessionId, {
                createdAt: new Date().toISOString(),
                webRecording: true,
                videoRecording: true
            });
        }

        return sessionResult;
    }

    /**
     * Stop session and handle recording finalization
     */
    async stopSessionWithRecordings(sessionId, downloadPath = null) {
        try {
            // Stop the session first
            await this.hyperbrowserService.stopSession(sessionId);
            
            // Wait for recording finalization (already done in stopSession)
            console.log('Waiting additional 3s for recording processing...');
            await new Promise(resolve => setTimeout(resolve, 3000));
            
            // Try to get final recording URLs using the VideoRecordingManager
            try {
                const recordingManager = new VideoRecordingManager(this.apiKey);
                const recordingUrls = await recordingManager.getRecordingUrls(sessionId);
                console.log(`Final recording URLs for ${sessionId}:`, recordingUrls);
                
                return {
                    sessionStopped: true,
                    recordings: recordingUrls
                };
            } catch (recordingError) {
                console.warn(`Could not retrieve final recordings: ${recordingError.message}`);
                return {
                    sessionStopped: true,
                    recordings: null,
                    recordingError: recordingError.message
                };
            }
            
        } finally {
            this.activeRecordings.delete(sessionId);
        }
    }

    /**
     * FIXED: Get recording URLs using VideoRecordingManager
     */
    async getRecordingUrls(sessionId) {
        try {
            const recordingManager = new VideoRecordingManager(this.apiKey);
            return await recordingManager.getRecordingUrls(sessionId);
        } catch (error) {
            console.error(`Failed to get recording URLs for ${sessionId}:`, error.message);
            return {
                webRecordingUrl: null,
                videoRecordingUrl: null,
                success: false,
                error: error.message
            };
        }
    }

    /**
     * Cleanup integration
     */
    async cleanup() {
        this.activeRecordings.clear();
        console.log('HyperbrowserRecordingIntegration cleaned up');
    }
}

/**
 * FIXED: Helper functions with correct parameter names
 */
export function createVideoRecordingSession(baseParams = {}) {
    return {
        screen: { width: 1920, height: 1080 },
        stealth: true,
        ...baseParams,
        // FIXED: Use correct parameter names from documentation
        enableWebRecording: true,
        enableVideoWebRecording: true // Required for video recording
    };
}

export function createVideoRecordingManager(apiKey) {
    if (!apiKey) {
        console.warn('No API key provided for VideoRecordingManager - using mock mode');
    }
    return new VideoRecordingManager(apiKey);
}

export default VideoRecordingManager;