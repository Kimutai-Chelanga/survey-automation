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
        console.log('1. Connecting to Chrome...');
        browser = await puppeteer.connect({
            browserURL: `http://localhost:${CHROME_DEBUG_PORT}`,
            defaultViewport: null
        });
        console.log('✓ Connected to Chrome\n');

        console.log('2. Opening Automa extension...');
        const automaUrl = `chrome-extension://${AUTOMA_EXTENSION_ID}/popup.html`;
        const page = await browser.newPage();
        await page.goto(automaUrl, { waitUntil: 'networkidle0', timeout: 10000 });
        console.log('✓ Automa loaded\n');

        await page.waitForTimeout(2000);

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
        
        const workflows = await page.evaluate(async () => {
            return new Promise((resolve) => {
                chrome.storage.local.get('workflows', (result) => {
                    resolve(result.workflows || {});
                });
            });
        });

        workflows[testWorkflow.id] = testWorkflow;

        await page.evaluate(async (workflows) => {
            return new Promise((resolve) => {
                chrome.storage.local.set({ workflows }, () => resolve());
            });
        }, workflows);

        console.log('✓ Test workflow injected\n');

        await page.reload({ waitUntil: 'networkidle0' });
        await page.waitForTimeout(2000);

        console.log('5. Executing test workflow...');
        
        const executeUrl = `chrome-extension://${AUTOMA_EXTENSION_ID}/execute.html#/${testWorkflow.id}`;
        await page.goto(executeUrl, { waitUntil: 'load', timeout: 10000 });
        
        console.log('✓ Workflow execution started\n');

        console.log('6. Waiting for workflow to complete (30 seconds)...');
        await page.waitForTimeout(30000);

        console.log('\n7. Checking for screenshot file...');
        
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
                
                const { stdout: dirContents } = await execAsync(`ls -la ${DOWNLOADS_DIR}`);
                console.log('\n   Current directory contents:');
                console.log(dirContents);
            }
        } catch (error) {
            console.log('❌ Error checking for screenshot:', error.message);
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
