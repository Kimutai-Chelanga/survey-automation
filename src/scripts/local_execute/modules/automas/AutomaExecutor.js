// modules/automa/AutomaExecutor.js

/**
 * Handles Automa workflow import, execution, and LOG CAPTURE via IndexedDB
 *
 * FIX HISTORY (2026-02-20 v26) — DETACHED FRAME FIX:
 *   - importWorkflow() no longer calls page.reload() — eliminated the root cause
 *     of "Attempted to use detached Frame". Instead, it re-navigates to popup.html
 *     ONLY when the caller explicitly asks, keeping the page reference stable.
 *   - executeWorkflow() always re-fetches the live page reference after each
 *     navigation via _getStablePage() — never trusts a stale handle.
 *   - Execute button click uses page.evaluate() (not page.click()) so there is
 *     no Puppeteer element handle that can go stale.
 *   - Added waitForSelectorStable() helper that re-queries the DOM on each poll.
 *
 * FIX HISTORY (2026-02-20 v25) — FIXED WORKFLOW EXECUTION:
 *   - Added actual clicking of execute button to start workflow
 *   - Added verification that workflow is running
 *   - Improved tab detection for video recording
 *
 * FIX HISTORY (2026-02-20 v24) — IMPROVED TAB DETECTION
 */

export class AutomaExecutor {
    constructor(extensionId) {
        this.extensionId = extensionId;
        this.workflowId  = null;
        this.logId       = null;
        this.popupPage   = null;
    }

    // ─── Internal helpers ──────────────────────────────────────────────────────

    /**
     * Always return the CURRENT live page for the given URL pattern.
     * Never trust a reference stored before a reload/navigate happened.
     */
    async _getStablePage(browser, urlPattern) {
        const pages = await browser.pages();
        const match = pages.find(p => {
            try { return p.url().includes(urlPattern); } catch { return false; }
        });
        return match || null;
    }

    /**
     * Open (or reuse) the Automa popup page and return a stable reference.
     */
    async _openPopup(browser) {
        const popupUrl = `chrome-extension://${this.extensionId}/popup.html`;

        // Re-use an existing popup page if one is already open
        let page = await this._getStablePage(browser, `chrome-extension://${this.extensionId}`);

        if (!page) {
            const allPages = await browser.pages();
            page = allPages.length > 0 ? allPages[0] : await browser.newPage();
        }

        await page.goto(popupUrl + '#/', { waitUntil: 'domcontentloaded', timeout: 30000 });
        await this.sleep(3000);
        return page;
    }

    /**
     * Poll for a DOM selector using page.evaluate() so we never hold a stale
     * ElementHandle. Returns true when found, throws on timeout.
     */
    async _waitForSelectorStable(browser, extensionUrlFragment, selector, timeoutMs = 15000) {
        const deadline = Date.now() + timeoutMs;
        while (Date.now() < deadline) {
            const page = await this._getStablePage(browser, extensionUrlFragment);
            if (!page) { await this.sleep(500); continue; }

            const found = await page.evaluate((sel) => {
                const el = document.querySelector(sel);
                return !!(el && el.offsetParent !== null); // visible
            }, selector).catch(() => false);

            if (found) return true;
            await this.sleep(500);
        }
        throw new Error(`Selector "${selector}" not found within ${timeoutMs}ms`);
    }

    // ─── Public API ────────────────────────────────────────────────────────────

    /**
     * Execute Automa workflow.
     * Returns { success, workflowId, subWorkflowIds }
     */
    async executeWorkflow(browser, automaWorkflow, linkUrl) {
        console.log('');
        console.log('█'.repeat(80));
        console.log('IMPORTING & EXECUTING AUTOMA WORKFLOW');
        console.log('█'.repeat(80));
        console.log(`Workflow     : ${automaWorkflow.name}`);
        console.log(`Extension ID : ${this.extensionId}`);
        console.log('');

        // ── Step 1: Open Automa popup ──────────────────────────────────────────
        console.log('Step 1: Opening Automa popup...');
        let page;
        try {
            page = await this._openPopup(browser);
            console.log('✓ Automa popup loaded');
            this.popupPage = page;
        } catch (error) {
            throw new Error(`Failed to load Automa popup: ${error.message}`);
        }

        // ── Step 1.5: Enable workflow logging ──────────────────────────────────
        console.log('\nStep 1.5: Enabling workflow logging...');
        // Always re-fetch page here — the popup might have reloaded
        page = await this._openPopup(browser);
        await this.ensureLoggingEnabled(page);

        // ── Step 2: Import sub-workflows then main orchestrator ────────────────
        console.log('\nStep 2: Importing workflow(s) to Automa extension...');

        const subEntries     = Object.entries(automaWorkflow.includedWorkflows || {});
        const subWorkflowIds = [];

        if (subEntries.length > 0) {
            console.log(`  Found ${subEntries.length} sub-workflow(s) — importing sub-workflows first...`);
            for (let i = 0; i < subEntries.length; i++) {
                const [, subWorkflow] = subEntries[i];
                console.log(`  [${i + 1}/${subEntries.length}] Importing: "${subWorkflow.name}"...`);

                // Re-open popup fresh before each import so the page reference is stable
                page = await this._openPopup(browser);
                const subId = await this.importWorkflow(page, subWorkflow, browser);
                subWorkflowIds.push({ id: subId, name: subWorkflow.name || subId });
                console.log(`    ✓ Imported with ID: ${subId}`);
            }
            console.log(`  ✓ All ${subEntries.length} sub-workflow(s) imported`);
        }

        console.log(`  Importing main workflow: "${automaWorkflow.name}"...`);
        page = await this._openPopup(browser);
        this.workflowId = await this.importWorkflow(page, automaWorkflow, browser);
        console.log(`✓ Main workflow imported with ID: ${this.workflowId}`);

        // ── Step 3: Navigate to execute page ──────────────────────────────────
        console.log('\nStep 3: Navigating to execute page...');
        const executeUrl = `chrome-extension://${this.extensionId}/execute.html#/${this.workflowId}`;
        console.log(`  URL: ${executeUrl}`);

        // Re-fetch stable page AFTER all imports are done
        page = await this._getStablePage(browser, `chrome-extension://${this.extensionId}`)
            || (await browser.pages())[0]
            || await browser.newPage();

        await page.goto(executeUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });

        // Wait for the Vue SPA to mount and render the execute button
        console.log('  Waiting for execute page to fully mount...');
        try {
            await this._waitForSelectorStable(browser, 'execute.html', 'button', 15000);
            console.log('  ✓ Execute page is ready');
        } catch (waitErr) {
            console.warn(`  ⚠️  Timed out waiting for button — will attempt click anyway: ${waitErr.message}`);
        }

        // Small additional buffer for Vue reactivity to settle
        await this.sleep(1000);

        // ── Step 4: Click the execute button ──────────────────────────────────
        console.log('\nStep 4: Clicking execute button to start workflow...');

        try {
            // ALWAYS re-query the live page — never reuse the old reference
            const livePage = await this._getStablePage(browser, 'execute.html');
            if (!livePage) {
                throw new Error('execute.html page not found in browser');
            }

            // Confirm the URL hash matches our workflow ID
            const currentUrl = livePage.url();
            console.log(`  Current page URL: ${currentUrl}`);
            if (!currentUrl.includes(this.workflowId)) {
                console.warn(`  ⚠️  URL does not contain workflow ID — page may have changed`);
            }

            // Use page.evaluate() so we never hold a stale ElementHandle
            const clickResult = await livePage.evaluate(() => {
                // Strategy 1: button with matching text
                const buttons = Array.from(document.querySelectorAll('button'));
                const byText  = buttons.find(b =>
                    b.textContent.trim().match(/^(Execute|Start|Run)$/i) ||
                    b.getAttribute('aria-label')?.match(/execute|start|run/i)
                );
                if (byText) { byText.click(); return { clicked: true, method: 'text match', text: byText.textContent.trim() }; }

                // Strategy 2: common class / data attribute selectors
                const byAttr = document.querySelector(
                    '.execute-button, [data-testid="execute-button"], .start-button, [data-action="execute"]'
                );
                if (byAttr) { byAttr.click(); return { clicked: true, method: 'attribute selector' }; }

                // Strategy 3: first non-disabled button that is visible
                const firstVisible = buttons.find(b => !b.disabled && b.offsetParent !== null);
                if (firstVisible) { firstVisible.click(); return { clicked: true, method: 'first visible button', text: firstVisible.textContent.trim() }; }

                return { clicked: false, buttonCount: buttons.length };
            });

            if (clickResult.clicked) {
                console.log(`  ✓ Execute button clicked (method: ${clickResult.method}${clickResult.text ? `, text: "${clickResult.text}"` : ''})`);
            } else {
                console.warn(`  ⚠️  Could not find execute button (${clickResult.buttonCount} buttons present) — workflow may start automatically`);
            }

            // Give the extension a moment to register the click
            await this.sleep(5000);

            // Verify workflow is running
            const isRunning = await livePage.evaluate((wfId) => {
                return new Promise((resolve) => {
                    try {
                        chrome.storage.local.get(['runningWorkflow'], (data) => {
                            resolve(data.runningWorkflow === wfId || !!data.runningWorkflow);
                        });
                    } catch {
                        resolve(false);
                    }
                });
            }, this.workflowId).catch(() => false);

            console.log(isRunning
                ? '  ✓ Workflow is running!'
                : '  ⚠️  Could not confirm workflow is running — check manually');

        } catch (error) {
            console.warn(`  ⚠️  Error clicking execute button: ${error.message}`);
            // Do NOT rethrow — allow the rest of the orchestration to continue
        }

        // ── Step 5: Bring the active workflow tab to front ─────────────────────
        console.log('\nStep 5: Finding active workflow tab for video recording...');

        let targetTab     = null;
        const maxAttempts = 5;

        for (let attempt = 1; attempt <= maxAttempts; attempt++) {
            console.log(`  Attempt ${attempt}/${maxAttempts} to find active tab...`);

            try {
                const allPages = await browser.pages();
                console.log(`    Total open tabs: ${allPages.length}`);

                for (let i = 0; i < allPages.length; i++) {
                    try {
                        const url = allPages[i].url();
                        console.log(`    Tab ${i}: ${url.substring(0, 100)}`);
                    } catch { console.log(`    Tab ${i}: <unable to get URL>`); }
                }

                // Priority 1: an http/https tab (real web page opened by workflow)
                for (const tab of allPages) {
                    try {
                        const url = tab.url();
                        if (url.startsWith('http://') || url.startsWith('https://')) {
                            targetTab = tab;
                            console.log(`  ✓ Found active web tab: ${url.substring(0, 100)}`);
                            break;
                        }
                    } catch { /* ignore closed/detached tabs */ }
                }

                // Priority 2: any non-extension tab
                if (!targetTab) {
                    for (const tab of allPages) {
                        try {
                            const url = tab.url();
                            if (url && !url.includes('chrome-extension://') && url !== 'about:blank') {
                                targetTab = tab;
                                console.log(`  ✓ Found non-extension tab: ${url.substring(0, 100)}`);
                                break;
                            }
                        } catch { /* ignore */ }
                    }
                }

                if (targetTab) {
                    try { await targetTab.bringToFront(); } catch { /* tab may have closed */ }
                    try {
                        console.log(`  ✓ Now showing: ${targetTab.url().substring(0, 100)}`);
                    } catch { console.log('  ✓ Tab brought to front'); }
                    break;
                }
            } catch (error) {
                console.log(`    Error checking tabs: ${error.message}`);
            }

            if (attempt < maxAttempts) {
                console.log(`    Waiting 3s for workflow to open tabs...`);
                await this.sleep(3000);
            }
        }

        if (!targetTab) {
            console.warn('  ⚠️  Could not find active workflow tab — showing popup instead');
            try {
                const popup = await this._getStablePage(browser, `chrome-extension://${this.extensionId}`);
                if (popup) await popup.bringToFront();
            } catch { /* ignore */ }
        }

        console.log('█'.repeat(80));
        console.log('');

        return {
            success:        true,
            workflowId:     this.workflowId,
            subWorkflowIds,
        };
    }

    /**
     * Ensure Automa logging is enabled (logsLimit > 0)
     */
    async ensureLoggingEnabled(page) {
        try {
            const result = await page.evaluate(() => {
                return new Promise((resolve) => {
                    try {
                        chrome.storage.local.get('settings', (data) => {
                            const settings = data.settings || {};
                            if (!settings.logsLimit) {
                                settings.logsLimit = 100;
                                settings.deleteLogAutomatically = false;
                                chrome.storage.local.set({ settings }, () => {
                                    resolve({ success: true, changed: true, logsLimit: settings.logsLimit });
                                });
                            } else {
                                resolve({ success: true, changed: false, logsLimit: settings.logsLimit });
                            }
                        });
                    } catch (error) {
                        resolve({ success: false, error: error.message });
                    }
                });
            });

            if (result.success) {
                console.log(result.changed
                    ? `✓ Enabled workflow logging (limit: ${result.logsLimit})`
                    : `✓ Workflow logging already enabled (limit: ${result.logsLimit})`
                );
            } else {
                console.warn(`⚠️  Could not enable logging: ${result.error}`);
            }
        } catch (error) {
            console.warn(`⚠️  Error checking logging settings: ${error.message}`);
        }
    }

    /**
     * Import a single workflow into Automa's chrome.storage.
     *
     * KEY CHANGE vs v25: No longer calls page.reload() at the end.
     * Reloading was the root cause of detached frame errors because it
     * invalidated the page reference held by executeWorkflow(). Instead,
     * we write directly to storage and let the CALLER decide when to
     * re-navigate (which it now does via _openPopup() before each import).
     */
    async importWorkflow(page, workflow, browser) {
        console.log('  Injecting workflow into extension storage...');

        const importResult = await page.evaluate((wf) => {
            return new Promise((resolve) => {
                try {
                    const generateId = () => {
                        const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
                        let r = '';
                        for (let i = 0; i < 21; i++) r += chars[Math.floor(Math.random() * chars.length)];
                        return r;
                    };

                    const newId = generateId();
                    const now   = Date.now();

                    const toSave = {
                        ...wf,
                        id:         newId,
                        createdAt:  now,
                        updatedAt:  now,
                        folderId:   null,
                        isDisabled: false,
                    };

                    if (typeof toSave.drawflow === 'string') {
                        toSave.drawflow = JSON.parse(toSave.drawflow);
                    }

                    chrome.storage.local.get('workflows', (storage) => {
                        let workflows = storage.workflows || {};
                        if (Array.isArray(workflows)) {
                            const obj = {};
                            workflows.forEach(w => { obj[w.id] = w; });
                            workflows = obj;
                        }
                        workflows[newId] = toSave;
                        chrome.storage.local.set({ workflows }, () => {
                            if (chrome.runtime.lastError) {
                                resolve({ success: false, error: chrome.runtime.lastError.message });
                            } else {
                                resolve({ success: true, workflowId: newId });
                            }
                        });
                    });
                } catch (e) {
                    resolve({ success: false, error: e.message });
                }
            });
        }, workflow);

        if (!importResult.success) {
            throw new Error(`Workflow import failed: ${importResult.error}`);
        }

        console.log(`  ✓ Saved with ID: ${importResult.workflowId}`);

        // Small pause so chrome.storage write is fully flushed before we continue
        await this.sleep(500);

        return importResult.workflowId;

        // ─── REMOVED: page.reload() was here in v25 ───────────────────────────
        // That reload destroyed the frame and caused every subsequent
        // page.evaluate() / page.click() to throw "Attempted to use detached Frame".
        // The caller (_openPopup) now handles re-navigation explicitly before
        // each import, which is the correct pattern.
    }

    // ─── Log fetching (unchanged from v25) ────────────────────────────────────

    extractScreenshotsFromLog(logData) {
        const screenshots = [];
        if (!logData?.history) return screenshots;

        for (const entry of logData.history) {
            if (entry.type === 'block' && entry.name === 'take-screenshot') {
                const screenshotData = entry.data?.screenshot;
                if (screenshotData) {
                    screenshots.push({
                        blockId:   entry.id,
                        blockName: entry.data?.description || entry.description || 'Screenshot',
                        timestamp: entry.timestamp || Date.now(),
                        data:      screenshotData,
                        url:       entry.activeTabUrl || null,
                    });
                }
            }
            if (entry.data?.blocks) {
                for (const block of entry.data.blocks) {
                    if (block.type === 'take-screenshot' || block.id === 'take-screenshot') {
                        const screenshotData = block.data?.screenshot;
                        if (screenshotData) {
                            screenshots.push({
                                blockId:   block.id,
                                blockName: block.data?.description || 'Screenshot',
                                timestamp: block.timestamp || Date.now(),
                                data:      screenshotData,
                                url:       block.activeTabUrl || null,
                            });
                        }
                    }
                }
            }
        }
        return screenshots;
    }

    async getWorkflowLogsFromIndexedDB(browser, workflowId, maxAttempts = 5, delayMs = 5000) {
        console.log(`\n📋 Fetching IndexedDB logs for workflow: ${workflowId}...`);

        try {
            const logPage = await browser.newPage();
            await logPage.goto(`chrome-extension://${this.extensionId}/popup.html`, {
                waitUntil: 'domcontentloaded',
                timeout: 30000,
            });
            await this.sleep(2000);

            for (let attempt = 1; attempt <= maxAttempts; attempt++) {
                console.log(`  Attempt ${attempt}/${maxAttempts}...`);

                const logsResult = await logPage.evaluate((wfId) => {
                    return new Promise(async (resolve) => {
                        try {
                            const request = indexedDB.open('logs');
                            request.onerror = () => resolve({ success: false, error: 'Failed to open IndexedDB' });
                            request.onsuccess = async (event) => {
                                const db = event.target.result;
                                try {
                                    const tx     = db.transaction(['items'], 'readonly');
                                    const store  = tx.objectStore('items');
                                    const getAll = store.getAll();

                                    getAll.onsuccess = async () => {
                                        const allItems = getAll.result;
                                        const wfLogs   = allItems
                                            .filter(l => l.workflowId === wfId)
                                            .sort((a, b) => (b.endedAt || b.startedAt || 0) - (a.endedAt || a.startedAt || 0));

                                        if (!wfLogs.length) {
                                            resolve({
                                                success: false,
                                                error: 'No logs found for workflow',
                                                totalLogs: allItems.length,
                                                allWorkflowIds: [...new Set(allItems.map(l => l.workflowId))].slice(0, 5),
                                            });
                                            return;
                                        }

                                        const latest = wfLogs[0];
                                        const logId  = latest.id;
                                        const data   = { item: latest, history: null, ctxData: null, logsData: null };

                                        try {
                                            const h = db.transaction(['histories'], 'readonly').objectStore('histories').index('logId').get(logId);
                                            await new Promise(r => { h.onsuccess = () => { if (h.result) data.history  = h.result; r(); }; h.onerror = r; });
                                        } catch (_) {}
                                        try {
                                            const c = db.transaction(['ctxData'], 'readonly').objectStore('ctxData').index('logId').get(logId);
                                            await new Promise(r => { c.onsuccess = () => { if (c.result) data.ctxData  = c.result; r(); }; c.onerror = r; });
                                        } catch (_) {}
                                        try {
                                            const d = db.transaction(['logsData'], 'readonly').objectStore('logsData').index('logId').get(logId);
                                            await new Promise(r => { d.onsuccess = () => { if (d.result) data.logsData = d.result; r(); }; d.onerror = r; });
                                        } catch (_) {}

                                        resolve({ success: true, logId, log: data, totalWorkflowLogs: wfLogs.length });
                                    };
                                    getAll.onerror = () => resolve({ success: false, error: 'getAll failed' });
                                } catch (e) {
                                    resolve({ success: false, error: `Transaction error: ${e.message}` });
                                }
                            };
                        } catch (e) {
                            resolve({ success: false, error: e.message });
                        }
                    });
                }, workflowId);

                if (logsResult.success && logsResult.log) {
                    await logPage.close();
                    console.log(`✓ Retrieved log (ID: ${logsResult.logId}, total runs: ${logsResult.totalWorkflowLogs})`);

                    const logData     = logsResult.log;
                    const screenshots = this.extractScreenshotsFromLog({
                        history:     logData.history?.data || [],
                        logMetadata: logData.item,
                    });
                    if (screenshots.length) console.log(`  📸 Found ${screenshots.length} screenshot(s) in workflow logs`);

                    return {
                        logId:       logsResult.logId,
                        logItem:     logData.item,
                        history:     logData.history?.data  || [],
                        ctxData:     logData.ctxData?.data  || null,
                        logsData:    logData.logsData?.data || null,
                        logMetadata: {
                            name:       logData.item.name,
                            status:     logData.item.status,
                            startedAt:  logData.item.startedAt,
                            endedAt:    logData.item.endedAt,
                            workflowId: logData.item.workflowId,
                            message:    logData.item.message || null,
                        },
                        screenshots,
                        source: 'indexeddb',
                    };
                }

                console.log(`  ⚠️  ${logsResult.error}`);
                if (logsResult.allWorkflowIds?.length) {
                    console.log(`  Recent IDs in DB: ${logsResult.allWorkflowIds.join(', ')}`);
                }
                if (attempt < maxAttempts) {
                    console.log(`  Retrying in ${delayMs / 1000}s...`);
                    await this.sleep(delayMs);
                }
            }

            await logPage.close();
            console.log(`  ⚠️  No logs found after ${maxAttempts} attempts`);
            return null;

        } catch (error) {
            console.warn(`  ⚠️  IndexedDB fetch failed: ${error.message}`);
            return null;
        }
    }

    async getAllWorkflowLogs(browser, orchestratorId, subWorkflowIds = []) {
        console.log('\n📋 Fetching logs for orchestrator + extracting sub-workflow results...');
        console.log(`  Orchestrator ID        : ${orchestratorId}`);
        console.log(`  Expected sub-workflows : ${subWorkflowIds.length}`);

        const results = [];
        const orchLog = await this.getWorkflowLogsFromIndexedDB(browser, orchestratorId, 5, 5000);

        if (orchLog) {
            results.push({ role: 'orchestrator', workflowId: orchestratorId, workflowName: 'Orchestrator', logData: orchLog, status: orchLog.logMetadata?.status });

            const subResults = this.extractSubWorkflowResults(orchLog);
            for (const sub of subWorkflowIds) {
                const matchingResult = subResults.find(r => r.workflowId === sub.id) || subResults.find(r => r.name.includes(sub.name));
                if (matchingResult) {
                    console.log(`\n  ✓ Found sub-workflow result: "${sub.name}" → ${matchingResult.status}`);
                    results.push({
                        role:         'sub_workflow',
                        workflowId:   sub.id,
                        workflowName: sub.name,
                        logData:      { logMetadata: { status: matchingResult.status, workflowId: sub.id, name: sub.name, error: matchingResult.error }, history: [matchingResult], screenshots: orchLog.screenshots?.filter(s => s.blockId === matchingResult.blockId) || [], source: 'extracted_from_orchestrator' },
                        status:       matchingResult.status,
                    });
                } else {
                    console.log(`\n  ⚠️  No result found for sub-workflow: "${sub.name}"`);
                    results.push({ role: 'sub_workflow', workflowId: sub.id, workflowName: sub.name, logData: null, status: 'unknown' });
                }
            }
        } else {
            console.log('\n⚠️  Could not fetch orchestrator log');
            results.push({ role: 'orchestrator', workflowId: orchestratorId, workflowName: 'Orchestrator', logData: null, status: 'unknown' });
            for (const sub of subWorkflowIds) {
                results.push({ role: 'sub_workflow', workflowId: sub.id, workflowName: sub.name, logData: null, status: 'unknown' });
            }
        }

        const orchOk = results[0]?.logData ? 1 : 0;
        const subOk  = results.filter(r => r.logData && r.role === 'sub_workflow').length;
        console.log(`\n✓ Log extraction complete — Orchestrator: ${orchOk ? '✅' : '❌'}, Sub-workflows: ${subOk}/${subWorkflowIds.length} found`);
        return results;
    }

    extractSubWorkflowResults(orchestratorLog) {
        if (!orchestratorLog?.history) return [];
        const results = [];
        for (const entry of orchestratorLog.history) {
            if (entry.type === 'block' && entry.name === 'execute-workflow') {
                results.push({ workflowId: entry.data?.workflowId || entry.blockId, status: entry.status || entry.type, name: entry.data?.description || entry.description || 'Unknown workflow', blockId: entry.id, error: entry.error || entry.message || null, timestamp: entry.timestamp });
            }
            if (entry.data?.blocks) {
                for (const block of entry.data.blocks) {
                    if (block.type === 'execute-workflow' || block.id === 'execute-workflow') {
                        results.push({ workflowId: block.data?.workflowId, status: block.status || 'unknown', name: block.data?.description || 'Nested workflow', blockId: block.id, error: block.error || null, timestamp: block.timestamp });
                    }
                }
            }
        }
        return results;
    }

    async exportLogsAsJSON(logData) {
        if (!logData?.history) { console.warn('No log data to export'); return null; }
        return {
            metadata:  logData.logMetadata,
            logs:      logData.history.map(item => ({
                timestamp:   item.timestamp   || null,
                status:      item.type?.toUpperCase() || 'UNKNOWN',
                name:        item.name,
                description: item.description || 'NULL',
                message:     item.message     || 'NULL',
                duration:    item.duration    || 0,
                blockId:     item.blockId     || null,
                data:        logData.ctxData?.ctxData?.[item.id] || null,
            })),
            tableData: logData.logsData?.table     || [],
            variables: logData.logsData?.variables || {},
        };
    }

    async getWorkflowLogsFromStorage(browser, workflowId) {
        console.log('\n📋 Attempting fallback log fetch from chrome.storage...');
        try {
            const logPage = await browser.newPage();
            await logPage.goto(`chrome-extension://${this.extensionId}/popup.html`, { waitUntil: 'domcontentloaded', timeout: 30000 });
            await this.sleep(2000);

            const logsResult = await logPage.evaluate(() => {
                return new Promise((resolve) => {
                    try {
                        if (typeof chrome === 'undefined' || !chrome.storage?.local) { resolve({ success: false, error: 'chrome.storage not available' }); return; }
                        chrome.storage.local.get('logs', (data) => {
                            if (chrome.runtime.lastError) { resolve({ success: false, error: chrome.runtime.lastError.message }); return; }
                            const entries = Object.entries(data.logs || {});
                            if (!entries.length) { resolve({ success: false, error: 'No logs in storage' }); return; }
                            entries.sort((a, b) => (parseInt(b[0]) || 0) - (parseInt(a[0]) || 0));
                            const [logId, logData] = entries[0];
                            resolve({ success: true, logId, logs: logData, totalLogEntries: entries.length });
                        });
                    } catch (e) { resolve({ success: false, error: e.message }); }
                });
            });

            await logPage.close();
            if (logsResult.success && logsResult.logs) {
                const logArray = Array.isArray(logsResult.logs) ? logsResult.logs : logsResult.logs.history || [];
                console.log(`✓ Retrieved ${logArray.length} entries from storage`);
                return { logs: logArray, logId: logsResult.logId, source: 'storage' };
            }
            console.log('  ⚠️  Storage logs not available');
            return null;
        } catch (error) {
            console.warn(`  ⚠️  Storage fetch failed: ${error.message}`);
            return null;
        }
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
