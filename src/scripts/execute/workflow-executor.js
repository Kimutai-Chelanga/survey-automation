import { WebSocket } from 'ws';
import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

class WorkflowExecutorService {
    constructor(config) {
        this.config = config;
        this.tempDirectory = path.join(__dirname, 'temp');
        this.ensureTempDirectory();
    }

    ensureTempDirectory() {
        if (!fs.existsSync(this.tempDirectory)) {
            fs.mkdirSync(this.tempDirectory, { recursive: true });
        }
    }

    async executeWorkflow(executionContext) {
        const startTime = Date.now();
        let websocket = null;
        let stepCount = 0;
        
        console.log(`Opening Automa extension for: ${executionContext.accountUsername}`);
        console.log(`Session ID: ${executionContext.sessionId}`);
        console.log(`WebSocket URL: ${executionContext.websocketUrl?.substring(0, 60)}...`);

        try {
            // Validate execution context
            this.validateExecutionContext(executionContext);

            // Connect to WebSocket
            websocket = await this.connectToWebSocket(executionContext.websocketUrl);
            stepCount++;

            // Open Automa extension popup
            const navigationResult = await this.openAutomaPopup(websocket, executionContext);
            stepCount += navigationResult.steps || 0;

            const executionTime = Date.now() - startTime;

            return {
                success: true,
                timestamp: new Date().toISOString(),
                executionTime,
                steps: stepCount,
                message: navigationResult.message || 'Successfully opened Automa extension popup',
                data: navigationResult.data,
                extensionContextMethod: 'automa-popup-open',
                extensionReady: true
            };

        } catch (error) {
            const executionTime = Date.now() - startTime;
            
            console.error(`Failed to open Automa extension: ${error.message}`);
            
            return {
                success: false,
                timestamp: new Date().toISOString(),
                executionTime,
                steps: stepCount,
                error: error.message,
                message: `Failed to open extension: ${error.message}`,
                extensionContextMethod: 'failed',
                extensionReady: false
            };

        } finally {
            if (websocket) {
                try {
                    websocket.close();
                } catch (closeError) {
                    console.error('Error closing WebSocket:', closeError.message);
                }
            }
        }
    }

    validateExecutionContext(context) {
        const required = ['websocketUrl', 'sessionId', 'accountUsername'];
        
        for (const field of required) {
            if (!context[field]) {
                throw new Error(`Missing required execution context field: ${field}`);
            }
        }
    }

    async connectToWebSocket(websocketUrl, maxRetries = 3) {
        for (let attempt = 0; attempt < maxRetries; attempt++) {
            try {
                console.log(`Connecting to WebSocket (attempt ${attempt + 1}/${maxRetries})...`);
                
                const websocket = new WebSocket(websocketUrl);
                
                return new Promise((resolve, reject) => {
                    const timeout = setTimeout(() => {
                        websocket.close();
                        reject(new Error('WebSocket connection timeout'));
                    }, 15000);

                    websocket.on('open', () => {
                        clearTimeout(timeout);
                        console.log('WebSocket connected successfully');
                        resolve(websocket);
                    });

                    websocket.on('error', (error) => {
                        clearTimeout(timeout);
                        reject(new Error(`WebSocket connection error: ${error.message}`));
                    });

                    websocket.on('close', (code, reason) => {
                        clearTimeout(timeout);
                        if (code !== 1000) {
                            reject(new Error(`WebSocket closed unexpectedly: ${code} ${reason}`));
                        }
                    });
                });

            } catch (error) {
                console.error(`WebSocket connection attempt ${attempt + 1} failed:`, error.message);
                
                if (attempt === maxRetries - 1) {
                    throw error;
                }
                
                await this.sleep(2000 * (attempt + 1));
            }
        }
    }

    async openAutomaPopup(websocket, executionContext, timeout = 60000) {
        console.log(`Opening Automa extension popup for account: ${executionContext.accountUsername}`);
        
        return new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                reject(new Error('Extension popup open timeout'));
            }, timeout);

            let navigationSteps = 0;

            // Use Runtime.evaluate to open the extension popup
            const navigateCommand = {
                id: Date.now(),
                method: 'Runtime.evaluate',
                params: {
                    expression: `window.open('chrome-extension://infppggnoaenmfagbfknfkancpbljcca/popup.html', '_blank')`
                }
            };

            const onMessage = (data) => {
                try {
                    const response = JSON.parse(data.toString());
                    
                    if (response.id === navigateCommand.id) {
                        websocket.removeListener('message', onMessage);
                        clearTimeout(timeoutId);
                        navigationSteps++;
                        
                        if (response.result) {
                            console.log(`✓ Successfully opened Automa extension popup for ${executionContext.accountUsername}`);
                            
                            // Wait for extension page to load
                            setTimeout(() => {
                                resolve({
                                    success: true,
                                    message: `Successfully opened Automa extension popup for ${executionContext.accountUsername}`,
                                    steps: navigationSteps,
                                    url: 'chrome-extension://infppggnoaenmfagbfknfkancpbljcca/popup.html',
                                    data: { 
                                        method: 'extension-popup-open',
                                        result: response.result
                                    }
                                });
                            }, 3000);
                            
                        } else if (response.error) {
                            reject(new Error(`Extension popup open error: ${response.error.message}`));
                        } else {
                            reject(new Error('Unexpected response format from navigation'));
                        }
                    }
                } catch (parseError) {
                    // Ignore parsing errors for other messages
                }
            };

            websocket.on('message', onMessage);
            websocket.send(JSON.stringify(navigateCommand));
        });
    }

    async cleanupTempDirectory() {
        try {
            if (fs.existsSync(this.tempDirectory)) {
                const files = fs.readdirSync(this.tempDirectory);
                for (const file of files) {
                    const filePath = path.join(this.tempDirectory, file);
                    const stats = fs.statSync(filePath);
                    
                    // Remove files older than 1 hour
                    if (Date.now() - stats.mtime.getTime() > 3600000) {
                        fs.unlinkSync(filePath);
                    }
                }
            }
        } catch (error) {
            console.error('Error cleaning temp directory:', error.message);
        }
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

export default WorkflowExecutorService;