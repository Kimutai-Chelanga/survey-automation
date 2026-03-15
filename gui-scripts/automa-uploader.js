#!/usr/bin/env node

/**
 * Upload and trigger Automa workflows via Chrome DevTools Protocol.
 * Each workflow is triggered sequentially, with a 2-minute interval.
 * Logs are stored in a timestamped file.
 */

const fs = require('fs');
const path = require('path');
const http = require('http');
const WebSocket = require('ws');

const CHROME_DEBUG_URL = "http://localhost:9222/json";
const WORKFLOW_DIRS = [
    "/workspace/gui-scripts/workflows"  // Only this folder is used now
];
const LOG_FILE = "automa_trigger_log.txt";
const INTERVAL_SECONDS = 120;  // 2 minutes

// Helper function to make HTTP requests
async function httpRequest(url, options = {}) {
    return new Promise((resolve, reject) => {
        const req = http.get(url, options, (res) => {
            let data = '';
            
            res.on('data', (chunk) => {
                data += chunk;
            });
            
            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    try {
                        resolve(JSON.parse(data));
                    } catch (e) {
                        resolve(data);
                    }
                } else {
                    reject(new Error(`HTTP ${res.statusCode}: ${data}`));
                }
            });
        });
        
        req.on('error', reject);
        req.setTimeout(10000, () => {
            req.destroy();
            reject(new Error('Request timeout'));
        });
    });
}

// Get Chrome tabs via DevTools Protocol
async function getChromeTabs() {
    try {
        return await httpRequest(CHROME_DEBUG_URL);
    } catch (e) {
        console.error(`❌ Failed to get Chrome targets: ${e.message}`);
        return [];
    }
}

// Find the Automa extension context
async function findAutomaContext() {
    const tabs = await getChromeTabs();
    
    for (const tab of tabs) {
        if (tab.type === 'background_page' && 
            tab.title && tab.title.toLowerCase().includes('automa')) {
            return tab.webSocketDebuggerUrl;
        }
    }
    
    for (const tab of tabs) {
        if (tab.url && tab.url.toLowerCase().includes('chrome-extension') && 
            tab.url.toLowerCase().includes('automa')) {
            return tab.webSocketDebuggerUrl;
        }
    }
    
    for (const tab of tabs) {
        if (tab.title && tab.title.toLowerCase().includes('automa')) {
            return tab.webSocketDebuggerUrl;
        }
    }
    
    return null;
}

// Load workflows from JSON files
function loadWorkflows() {
    const workflows = [];
    
    for (const workflowDir of WORKFLOW_DIRS) {
        if (!fs.existsSync(workflowDir)) {
            console.log(`⚠️ Skipping missing directory: ${workflowDir}`);
            continue;
        }
        
        const files = fs.readdirSync(workflowDir)
            .filter(file => file.endsWith('.json'));
            
        if (files.length === 0) {
            console.log(`⚠️ No JSON files in: ${workflowDir}`);
            continue;
        }
        
        const dirname = path.basename(workflowDir);
        
        for (const file of files) {
            try {
                const filePath = path.join(workflowDir, file);
                const data = JSON.parse(fs.readFileSync(filePath, 'utf8'));
                const fname = path.basename(file, '.json');
                
                data.id = data.id || `${dirname}_${fname}`;
                data.name = data.name || `${dirname.charAt(0).toUpperCase() + dirname.slice(1)} - ${fname}`;
                
                const ts = Date.now();
                data.createdAt = data.createdAt || ts;
                data.updatedAt = data.updatedAt || ts;
                data.isDisabled = data.isDisabled || false;
                data.description = data.description || `Imported ${dirname} workflow: ${fname}`;
                
                workflows.push(data);
                console.log(`  ✅ Loaded: ${data.name}`);
            } catch (e) {
                console.error(`  ❌ Failed parsing ${file}: ${e.message}`);
            }
        }
    }
    
    console.log(`📊 Total workflows loaded: ${workflows.length}`);
    return workflows;
}

// Inject and trigger workflows via WebSocket
async function injectAndTriggerWorkflows(wsUrl, workflows, variablesMap = null) {
    return new Promise((resolve, reject) => {
        const ws = new WebSocket(wsUrl);
        const wfMap = {};
        
        workflows.forEach(wf => {
            wfMap[wf.id] = wf;
        });
        
        ws.on('open', () => {
            console.log('Connected to Chrome DevTools');
            
            // Upload workflows
            const uploadJs = `
                if (chrome?.storage?.local) {
                    chrome.storage.local.set({workflows: ${JSON.stringify(wfMap)}}, () => {});
                    'uploaded';
                } else {
                    'storage_unavailable';
                }
            `;
            
            ws.send(JSON.stringify({
                id: 1,
                method: "Runtime.evaluate",
                params: { expression: uploadJs }
            }));
            
            // Handle responses
            ws.on('message', async (data) => {
                const response = JSON.parse(data);
                
                // Check if this is the response to our upload
                if (response.id === 1) {
                    console.log("Upload response:", response);
                    
                    // Now trigger each workflow sequentially
                    for (const wf of workflows) {
                        try {
                            const wfId = wf.id;
                            const wfName = wf.name || wfId;
                            const varsForWf = (variablesMap || {})[wfId] || {};
                            
                            const detail = { id: wfId };
                            if (Object.keys(varsForWf).length > 0) {
                                detail.data = { variables: varsForWf };
                            }
                            
                            const triggerJs = `
                                new Promise((resolve, reject) => {
                                    try {
                                        const timeout = setTimeout(() => reject('timeout'), 3000);
                                        window.dispatchEvent(new CustomEvent('automa:execute-workflow', { detail: ${JSON.stringify(detail)} }));
                                        setTimeout(() => { clearTimeout(timeout); resolve('dispatched'); }, 100);
                                    } catch (e) {
                                        reject(e.message);
                                    }
                                });
                            `;
                            
                            const payload = {
                                id: Date.now() % 10000 + 500,
                                method: "Runtime.evaluate",
                                params: { expression: triggerJs, awaitPromise: true }
                            };
                            
                            // Send trigger command
                            ws.send(JSON.stringify(payload));
                            
                            // Wait for response with a timeout
                            const triggerResponse = await new Promise((res, rej) => {
                                const handler = (msg) => {
                                    const parsed = JSON.parse(msg);
                                    if (parsed.id === payload.id) {
                                        res(parsed);
                                        ws.off('message', handler);
                                    }
                                };
                                ws.on('message', handler);
                                
                                // Set timeout for response
                                setTimeout(() => {
                                    rej(new Error('Timeout waiting for response'));
                                    ws.off('message', handler);
                                }, 5000);
                            });
                            
                            const val = triggerResponse.result && triggerResponse.result.result ? 
                                        triggerResponse.result.result.value : '';
                            const success = val.includes('dispatched');
                            const status = success ? "SUCCESS" : "FAILURE";
                            const logLine = `${new Date().toISOString().replace('T', ' ').substring(0, 19)} - ${status} triggering '${wfName}': ${val || JSON.stringify(triggerResponse)}\n`;
                            
                            console.log(logLine.trim());
                            fs.appendFileSync(LOG_FILE, logLine, 'utf8');
                            
                            // Wait for the interval before next workflow
                            await new Promise(r => setTimeout(r, INTERVAL_SECONDS * 1000));
                            
                        } catch (e) {
                            console.error(`Error triggering workflow: ${e.message}`);
                            const logLine = `${new Date().toISOString().replace('T', ' ').substring(0, 19)} - ERROR triggering workflow: ${e.message}\n`;
                            fs.appendFileSync(LOG_FILE, logLine, 'utf8');
                        }
                    }
                    
                    ws.close();
                    resolve();
                }
            });
        });
        
        ws.on('error', reject);
        ws.on('close', () => {
            console.log('WebSocket connection closed');
        });
    });
}

// Check Chrome status and get info
async function checkChromeStatus() {
    console.log("🔍 Checking Chrome GUI status...");
    
    try {
        const info = await httpRequest(`${CHROME_DEBUG_URL}/version`);
        console.log("✅ Chrome GUI is running and accessible");
        console.log(`📋 Chrome Version: ${info.Browser || 'Unknown'}`);
        
        const tabs = await httpRequest(CHROME_DEBUG_URL);
        console.log("📊 Available Chrome contexts:");
        
        tabs.slice(0, 5).forEach((tab, i) => {
            const title = (tab.title || 'Unknown').substring(0, 40);
            const tabType = tab.type || 'unknown';
            const url = (tab.url || '').substring(0, 50);
            
            console.log(`  ${i+1}. ${title} (${tabType})`);
            if (url.includes('chrome-extension')) {
                console.log(`      Extension URL: ${url}`);
            }
        });
        
        if (tabs.length > 5) {
            console.log(`  ... and ${tabs.length - 5} more`);
        }
        
        return true;
    } catch (e) {
        console.error("❌ Chrome GUI is not running!");
        console.error("💡 Make sure to run './start-gui.sh' first");
        console.error("💡 Or try: docker exec -it <container> /usr/local/bin/start-gui.sh");
        return false;
    }
}

// Main function
async function main() {
    console.log("🚀 Automa Workflow Uploader");
    console.log("==========================");
    
    process.chdir('/workspace');
    
    // Check Chrome status
    const chromeRunning = await checkChromeStatus();
    if (!chromeRunning) {
        process.exit(1);
    }
    
    // Check workflows directory
    const WORKFLOW_PATH = "/workspace/gui-scripts/workflows";
    if (!fs.existsSync(WORKFLOW_PATH)) {
        console.error("❌ No '/workspace/gui-scripts/workflows' directory found!");
        console.error("💡 Create the directory and add your .json workflow files");
        process.exit(1);
    }
    
    // Count and list workflow files
    const files = fs.readdirSync(WORKFLOW_PATH)
        .filter(file => file.endsWith('.json'));
    
    console.log(`📊 Found ${files.length} JSON workflow files in ${WORKFLOW_PATH}`);
    
    if (files.length === 0) {
        console.error("❌ No .json files found in /workspace/gui-scripts/workflows");
        console.error("💡 Add some .json workflow files to the directory");
        process.exit(1);
    }
    
    console.log("📋 Workflow files found:");
    files.forEach(file => console.log(`  - ${file}`));
    
    // Check if WebSocket dependency is available
    try {
        require.resolve('ws');
    } catch (e) {
        console.error("❌ WebSocket library 'ws' is not available");
        console.error("💡 Install it with: npm install ws");
        process.exit(1);
    }
    
    console.log("");
    console.log("🔄 Uploading workflows to running Chrome instance...");
    console.log("==================================================");
    
    try {
        // Load workflows
        console.log("🔄 Starting Automa workflow upload & trigger process");
        const workflows = loadWorkflows();
        
        if (workflows.length === 0) {
            console.log("No workflows loaded. Exiting.");
            process.exit(1);
        }
        
        // Find Automa context
        const wsUrl = await findAutomaContext();
        if (!wsUrl) {
            console.error("⚠️ Automa context not found. Please open Automa in Chrome.");
            process.exit(1);
        }
        
        console.log(`Using websocket: ${wsUrl.substring(0, 50)}…`);
        
        // Inject and trigger workflows
        await injectAndTriggerWorkflows(wsUrl, workflows);
        
        console.log("");
        console.log("==================================================");
        console.log("🎉 Upload process completed!");
        console.log("");
        console.log("📖 How to access your workflows:");
        console.log("  1. Open Chrome GUI: http://localhost:6080/vnc.html");
        console.log("     Password: secret");
        console.log("  2. Look for Automa extension icon in Chrome toolbar");
        console.log("  3. Click on Automa extension or go to chrome-extension://[extension-id]/src/newtab/index.html");
        console.log("  4. Your workflows should appear in the dashboard");
        console.log("");
        console.log("🔧 Troubleshooting tips:");
        console.log("  - If workflows don't appear, try refreshing the Automa page");
        console.log("  - Check browser console (F12) for any errors");
        console.log("  - Ensure the workflow JSON files are valid");
        console.log("  - Try restarting Chrome if needed");
        console.log("==================================================");
        
    } catch (e) {
        console.error("");
        console.error("==================================================");
        console.error("❌ Upload process failed!");
        console.error("");
        console.error("🔧 Debugging steps:");
        console.error("  1. Check if Chrome is running: curl http://localhost:9222/json");
        console.error("  2. Verify Automa extension is loaded in Chrome");
        console.error("  3. Check Chrome logs: tail -f /tmp/chrome.log");
        console.error("  4. Restart the GUI: ./start-gui.sh");
        console.error("==================================================");
        console.error(`Error details: ${e.message}`);
        process.exit(1);
    }
}

// Run the main function
main().catch(console.error);