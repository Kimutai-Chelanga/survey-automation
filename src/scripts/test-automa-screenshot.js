/**
 * Test script to verify Automa can take screenshots
 * This creates a minimal workflow with just a screenshot block
 */

import puppeteer from 'puppeteer-core';

const CHROME_DEBUG_PORT = 9222;
const AUTOMA_EXTENSION_ID = 'infppggnoaenmfagbfknfkancpbljcca';
const DOWNLOADS_DIR = '/opt/airflow/workspace/downloads';

async function testAutomaScreenshot() {
    console.log('====================================================================');
    console.log('TESTING AUTOMA SCREENSHOT FUNCTIONALITY');
    console.log('====================================================================\n');

    let browser;
    try {
        // Connect to Chrome
        console.log('1. Connecting to Chrome...');
        browser = await puppeteer.connect({
            browserURL: `http://localhost:${CHROME_DEBUG_PORT}`,
            defaultViewport: null
        });
        console.log('✓ Connected to Chrome\n');

        // Open Automa popup
        console.log('2. Opening Automa extension...');
        const automaUrl = `chrome-extension://${AUTOMA_EXTENSION_ID}/popup.html`;
        const page = await browser.newPage();
        await page.goto(automaUrl, { waitUntil: 'networkidle0', timeout: 10000 });
        console.log('✓ Automa loaded\n');

        // Wait a bit for extension to initialize
        await page.waitForTimeout(2000);

        // Create minimal test workflow with screenshot
        console.log('3. Creating test workflow with screenshot block...');
        const testWorkflow = {
            id: 'test_screenshot_' + Date.now(),
            name: 'Screenshot Test',
            description: 'Minimal workflow to test screenshot functionality',
            icon: 'riScreenshot2Line',
            version: '2.1.0',
            drawflow: {
                Home: {
                    data: {
                        // Trigger node
                        '1': {
                            id: '1',
                            name: 'trigger',
                            data: {},
                            class: 'trigger',
                            inputs: {},
                            outputs: {
                                output_1: {
                                    connections: [{ node: '2', output: 'input_1' }]
                                }
                            },
                            pos_x: 100,
                            pos_y: 100
                        },
                        // New tab node
                        '2': {
                            id: '2',
                            name: 'new-tab',
                            data: {
                                url: 'https://example.com',
                                description: 'Open test page'
                            },
                            class: 'new-tab',
                            inputs: {
                                input_1: {
                                    connections: [{ node: '1', input: 'output_1' }]
                                }
                            },
                            outputs: {
                                output_1: {
                                    connections: [{ node: '3', output: 'input_1' }]
                                }
                            },
                            pos_x: 250,
                            pos_y: 100
                        },
                        // Screenshot node
                        '3': {
                            id: '3',
                            name: 'screenshot',
                            data: {
                                type: 'fullscreen',
                                fileName: `test_screenshot_${Date.now()}.png`,
                                saveToComputer: true,
                                assignVariable: false,
                                variableName: '',
                                description: 'Test screenshot'
                            },
                            class: 'screenshot',
                            inputs: {
                                input_1: {
                                    connections: [{ node: '2', input: 'output_1' }]
                                }
                            },
                            outputs: {},
                            pos_x: 400,
                            pos_y: 100
                        }
                    }
                }
            },
            settings: {
                blockDelay: 1,
                debugMode: true,
                saveLog: true,
                notification: true,
                reuseLastState: false,
                restartTimes: 0,
                executedBlockOnWeb: true,
                onError: 'stop-workflow'
            },
            globalData: '{}'
        };

        console.log('4. Injecting workflow into Automa...');
        
        // Get existing workflows from storage
        const workflows = await page.evaluate(async () => {
            return new Promise((resolve) => {
                chrome.storage.local.get('workflows', (result) => {
                    resolve(result.workflows || {});
                });
            });
        });

        workflows[testWorkflow.id] = testWorkflow;

        // Save updated workflows
        await page.evaluate(async (workflows) => {
            return new Promise((resolve) => {
                chrome.storage.local.set({ workflows }, () => resolve());
            });
        }, workflows);

        console.log('✓ Test workflow injected\n');

        // Reload extension page to see the workflow
        await page.reload({ waitUntil: 'networkidle0' });
        await page.waitForTimeout(2000);

        console.log('5. Executing test workflow...');
        
        // Navigate to execute page
        const executeUrl = `chrome-extension://${AUTOMA_EXTENSION_ID}/execute.html#/${testWorkflow.id}`;
        await page.goto(executeUrl, { waitUntil: 'load', timeout: 10000 });
        
        console.log('✓ Workflow execution started\n');

        // Wait for execution to complete (screenshot should be quick)
        console.log('6. Waiting for workflow to complete (30 seconds)...');
        await page.waitForTimeout(30000);

        console.log('\n7. Checking for screenshot file...');
        
        // Check if file was created
        const { exec } = await import('child_process');
        const { promisify } = await import('util');
        const execAsync = promisify(exec);

        try {
            const { stdout } = await execAsync(`find ${DOWNLOADS_DIR} -name "test_screenshot_*.png" -mmin -2`);
            if (stdout.trim()) {
                console.log('✅ SUCCESS! Screenshot file found:');
                console.log(stdout.trim());
                
                const { stdout: fileInfo } = await execAsync(`ls -lah ${stdout.trim()}`);
                console.log(fileInfo);
            } else {
                console.log('❌ No screenshot file found');
                console.log(`   Expected location: ${DOWNLOADS_DIR}/test_screenshot_*.png`);
                
                // Show what files are in the directory
                const { stdout: dirContents } = await execAsync(`ls -la ${DOWNLOADS_DIR}`);
                console.log('\n   Current directory contents:');
                console.log(dirContents);
            }
        } catch (error) {
            console.log('❌ Error checking for screenshot:', error.message);
        }

        console.log('\n8. Fetching workflow logs from Automa...');
        try {
            const logs = await page.evaluate(async (workflowId) => {
                return new Promise((resolve) => {
                    const request = indexedDB.open('automa', 1);
                    
                    request.onsuccess = () => {
                        const db = request.result;
                        const transaction = db.transaction(['logs'], 'readonly');
                        const store = transaction.objectStore('logs');
                        const getAllRequest = store.getAll();
                        
                        getAllRequest.onsuccess = () => {
                            const allLogs = getAllRequest.result;
                            const workflowLogs = allLogs.filter(log => log.workflowId === workflowId);
                            resolve(workflowLogs[workflowLogs.length - 1] || null);
                        };
                        
                        getAllRequest.onerror = () => resolve(null);
                    };
                    
                    request.onerror = () => resolve(null);
                });
            }, testWorkflow.id);

            if (logs) {
                console.log('Workflow execution log:');
                console.log(JSON.stringify(logs, null, 2));
            } else {
                console.log('⚠️  No logs found in Automa IndexedDB');
            }
        } catch (error) {
            console.log('❌ Error fetching logs:', error.message);
        }

        await page.close();

        console.log('\n====================================================================');
        console.log('TEST COMPLETE');
        console.log('====================================================================');

    } catch (error) {
        console.error('\n❌ Test failed:', error.message);
        console.error(error.stack);
    } finally {
        if (browser) {
            await browser.disconnect();
        }
    }
}

testAutomaScreenshot().catch(console.error);