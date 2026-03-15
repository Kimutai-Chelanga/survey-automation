// modules/orchestrator/OrchestratorBuilder.js

/**
 * FIX HISTORY (2026-02-19 v11) — Inject screenshot_prefix into includedWorkflows globalData:
 *   Sub-workflows loaded into Automa via includedWorkflows need screenshot_prefix in their
 *   OWN globalData so that {{screenshot_prefix}} resolves in take-screenshot block filenames.
 *   The orchestrator execute-workflow block already has insertAllGlobalData=true, but Automa
 *   resolves filename templates from the sub-workflow's own variable scope.
 *   Fix: after Step 6 builds screenshotPrefixes, patch each includedWorkflows[docId].globalData
 *   with the prefix for that doc. For multi-combination runs with the same docId, the last
 *   prefix wins (acceptable — each combination gets its own execute-workflow block with its
 *   own globalData injection anyway).
 *
 *   Also adds downloads directory cleanup at session start (pre-run stale file removal) —
 *   see dynamic-orchestrator.js for that part.
 *
 * FIX HISTORY (2026-02-19 v10) — Removed total_execution_budget_ms override:
 *   Bottom-up block analysis is now ALWAYS performed — the user override path
 *   has been removed entirely. Users configure wait_margin_ms in Streamlit
 *   instead, which is passed in via executionSettings.wait_margin_ms and
 *   stored in _timing.wait_margin_ms for dynamic-orchestrator.js to use.
 *
 *   Estimated total = analysed_total_workflow_ms (always)
 *                   + sum of all inter-workflow delay block ms
 *
 *   dynamic-orchestrator.js adds wait_margin_ms (user-configured, default 300 s).
 *
 * FIX HISTORY (2026-02-19 v9) — Bottom-up workflow time analysis
 * FIX HISTORY (2026-02-19 v8) — Configurable total execution budget (user-set) [REMOVED]
 * FIX HISTORY (2026-02-19 v7) — Per-workflow budget × N (removed)
 * FIX HISTORY (2026-02-17 v6) — insertAllGlobalData must be true
 * FIX HISTORY (2026-02-17 v5) — per-workflow globalData injection
 * FIX HISTORY (2026-02-17 v4) — delay block time in MILLISECONDS
 * FIX HISTORY (2026-02-17 v3) — resolveDelayRange reads delay_min/max_milliseconds
 * FIX HISTORY (2026-02-15)    — execution_sessions created before run
 */

export class OrchestratorBuilder {
    constructor(mongoDBService) {
        if (!mongoDBService || !mongoDBService.db || !mongoDBService.client) {
            throw new Error(
                'OrchestratorBuilder requires mongoDBService with .db and .client.'
            );
        }
        this.mongoDBService = mongoDBService;

        // Fixed time budgets for block types that don't have an explicit timeout
        this.BLOCK_BUDGETS_MS = {
            'new-tab':         5_000,   // page load + waitTabLoaded
            'take-screenshot': 2_000,   // capture + save
            'press-key':         500,   // typing / keypress
            'active-tab':        500,
            'close-tab':         300,
            'go-back':         1_000,
            'reload-tab':      3_000,
            'scroll-element':    500,
            'attribute-value':   500,
            'get-text':          500,
            'forms':           1_000,
            'javascript-code': 2_000,
            'conditions':        200,
            'loop-data':         200,
            'loop-elements':     500,
            'default':         1_000,   // catch-all
        };

        // Default wait margin if none provided via executionSettings
        this.DEFAULT_WAIT_MARGIN_MS = 300_000; // 300 s
    }

    // ─── Utilities ────────────────────────────────────────────────────────────

    getRandomDelay(minMs, maxMs) {
        if (minMs > maxMs) throw new Error(`getRandomDelay: minMs (${minMs}) must be ≤ maxMs (${maxMs})`);
        return Math.floor(Math.random() * (maxMs - minMs + 1)) + minMs;
    }

    generateAutomaId(length = 21) {
        const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_';
        return Array.from({ length }, () => chars[Math.floor(Math.random() * chars.length)]).join('');
    }

    // ─── Bottom-up block time analyser ───────────────────────────────────────
    _analyseBlocks(blocks) {
        if (!Array.isArray(blocks) || blocks.length === 0) return 0;

        let totalMs = 0;

        for (const block of blocks) {
            const id   = block.id    || block.label || '';
            const data = block.data  || {};

            if (data.disableBlock) continue;

            switch (id) {
                case 'delay': {
                    const ms = (data.timeout != null && data.timeout > 0)
                        ? Number(data.timeout)
                        : Number(data.time || 0);
                    totalMs += ms;
                    break;
                }

                case 'event-click': {
                    const ms = Number(data.waitSelectorTimeout || data.timeout || 0);
                    totalMs += ms;
                    break;
                }

                case 'element-exists': {
                    const timeoutMs = Number(data.timeout  || 0);
                    const tryCount  = Number(data.tryCount || 1);
                    totalMs += timeoutMs * tryCount;
                    break;
                }

                case 'blocks-group': {
                    const nested = data.blocks || [];
                    totalMs += this._analyseBlocks(nested);
                    break;
                }

                default: {
                    totalMs += this.BLOCK_BUDGETS_MS[id] ?? this.BLOCK_BUDGETS_MS['default'];
                    break;
                }
            }
        }

        return totalMs;
    }

    /**
     * Analyse a full Automa workflow document and return an estimated duration in ms.
     *
     * @param {Object} workflowDoc  — MongoDB workflow document
     * @returns {{ estimatedMs: number, breakdown: Object }}
     */
    analyzeWorkflowDuration(workflowDoc) {
        const drawflow = workflowDoc.drawflow
            || workflowDoc.workflow_data?.drawflow
            || workflowDoc.template_data?.drawflow
            || null;

        if (!drawflow) {
            console.warn(`    ⚠️  analyzeWorkflowDuration: no drawflow found — using default 60 000 ms`);
            return { estimatedMs: 60_000, breakdown: { reason: 'no_drawflow' } };
        }

        const nodes = drawflow.nodes || [];
        let totalMs = 0;

        const breakdown = {
            node_count:    nodes.length,
            delay_ms:      0,
            click_ms:      0,
            other_ms:      0,
            block_counts:  {},
        };

        for (const node of nodes) {
            const label = node.label || '';
            const data  = node.data  || {};

            if (data.disableBlock) continue;

            if (label === 'blocks-group') {
                const nestedBlocks = data.blocks || [];
                const nestedMs     = this._analyseBlocks(nestedBlocks);
                totalMs           += nestedMs;

                for (const b of nestedBlocks) {
                    const bid = b.id || 'unknown';
                    breakdown.block_counts[bid] = (breakdown.block_counts[bid] || 0) + 1;

                    if (bid === 'delay')              breakdown.delay_ms += Number(b.data?.timeout || b.data?.time || 0);
                    else if (bid === 'element-exists') breakdown.other_ms += Number(b.data?.timeout || 0) * Number(b.data?.tryCount || 1);
                    else if (bid === 'event-click')   breakdown.click_ms += Number(b.data?.waitSelectorTimeout || 0);
                    else                              breakdown.other_ms += this.BLOCK_BUDGETS_MS[bid] ?? this.BLOCK_BUDGETS_MS['default'];
                }

            } else {
                const nodeMs = this._analyseBlocks([{ id: label, data }]);
                totalMs     += nodeMs;
                breakdown.block_counts[label] = (breakdown.block_counts[label] || 0) + 1;
            }
        }

        return { estimatedMs: totalMs, breakdown };
    }

    // ─── Delay range resolution ───────────────────────────────────────────────

    resolveDelayRange(executionSettings) {
        const d = executionSettings?.delays;

        if (
            d?.min_milliseconds != null && d?.max_milliseconds != null &&
            Number.isFinite(d.min_milliseconds) && Number.isFinite(d.max_milliseconds) &&
            d.min_milliseconds >= 0 && d.min_milliseconds <= d.max_milliseconds
        ) {
            console.log(`  Delay source : Streamlit execution_config`);
            console.log(`  Delay range  : ${d.min_milliseconds.toLocaleString()}–${d.max_milliseconds.toLocaleString()} ms  (${d.min_milliseconds / 1000}s–${d.max_milliseconds / 1000}s)`);
            return { min_milliseconds: d.min_milliseconds, max_milliseconds: d.max_milliseconds };
        }

        if (d?.gap_seconds != null && Number.isFinite(d.gap_seconds) && d.gap_seconds > 0) {
            const gapMs = d.gap_seconds * 1000;
            const min   = Math.max(gapMs - 5_000, 5_000);
            const max   = gapMs + 5_000;
            console.warn(`⚠️  Using legacy gap_seconds=${d.gap_seconds} → ${min.toLocaleString()}–${max.toLocaleString()} ms`);
            return { min_milliseconds: min, max_milliseconds: max };
        }

        console.warn('⚠️  No delay config — using hard-coded fallback: 10 000–20 000 ms');
        return { min_milliseconds: 10_000, max_milliseconds: 20_000 };
    }

    // ─── Per-workflow variable injection ──────────────────────────────────────

    buildWorkflowGlobalData(combination, screenshotPrefix) {
        const vars = { screenshot_prefix: screenshotPrefix };

        if (combination.link_url)                    vars.link_url            = combination.link_url;
        if (combination.tweet_content)               vars.tweet_content       = combination.tweet_content;
        if (combination.reply_text)                  vars.reply_text          = combination.reply_text;
        if (combination.content)                     vars.content             = combination.content;
        if (combination.tweet_text)                  vars.tweet_text          = combination.tweet_text;
        if (combination.account_id != null)          vars.account_id          = combination.account_id;
        if (combination.postgres_content_id != null) vars.postgres_content_id = combination.postgres_content_id;
        if (combination.tweet_id)                    vars.tweet_id            = combination.tweet_id;
        if (combination.tweet_url)                   vars.tweet_url           = combination.tweet_url;

        if (combination.extra_vars && typeof combination.extra_vars === 'object') {
            Object.assign(vars, combination.extra_vars);
        }

        return JSON.stringify(vars);
    }

    // ─── Template ─────────────────────────────────────────────────────────────

    async fetchOrchestratorTemplate() {
        const settingsDoc = await this.mongoDBService.db
            .collection('settings')
            .findOne({ category: 'system' });

        if (!settingsDoc?.settings?.execution_orchestrator_template) {
            throw new Error(
                'No orchestrator template found in MongoDB.\n' +
                'Fix: Streamlit → Settings → Execution Configuration → Upload Template.'
            );
        }

        const template = settingsDoc.settings.execution_orchestrator_template;
        console.log(`✓ Template loaded: ${template.template_name || '(unnamed)'}`);
        return template.template_data;
    }

    // ─── Workflow document fetching ────────────────────────────────────────────

    async fetchWorkflowDocuments(workflowIds, databaseName, collectionName) {
        if (!workflowIds || workflowIds.length === 0) {
            console.warn('⚠️  fetchWorkflowDocuments called with empty workflowIds');
            return [];
        }

        console.log(`\n  📦 Fetching ${workflowIds.length} doc(s) from ${databaseName}.${collectionName}`);

        const { ObjectId } = await import('mongodb');
        const targetDb     = this.mongoDBService.client.db(databaseName);
        const collection   = targetDb.collection(collectionName);

        const objectIds = [];
        const plainIds  = [];

        for (const id of workflowIds) {
            if (/^[a-f\d]{24}$/i.test(id)) {
                objectIds.push(new ObjectId(id));
            } else {
                plainIds.push(id);
                console.warn(`  ⚠️  Non-ObjectId workflow ID "${id}" — querying as string`);
            }
        }

        const results = [];
        if (objectIds.length > 0) results.push(...await collection.find({ _id: { $in: objectIds } }).toArray());
        if (plainIds.length  > 0) results.push(...await collection.find({ _id: { $in: plainIds  } }).toArray());

        if (results.length === 0) {
            throw new Error(`No workflow documents found in ${databaseName}.${collectionName} for IDs: ${workflowIds.join(', ')}`);
        }

        if (results.length !== workflowIds.length) {
            console.warn(`  ⚠️  Expected ${workflowIds.length}, found ${results.length}`);
        } else {
            console.log(`  ✓ ${results.length} doc(s) fetched`);
        }

        return results;
    }

    // ─── Total execution time estimator ──────────────────────────────────────
    //
    // Always performs bottom-up analysis — no user override path.
    //
    // Total = Σ(per-workflow block analysis across all combinations)
    //       + Σ(inter-workflow delay block values already randomised into the graph)
    //
    // dynamic-orchestrator.js adds wait_margin_ms on top (user-configured in Streamlit).
    // ─────────────────────────────────────────────────────────────────────────
    _estimateTotalExecutionMs({ nodes, validCombinations, docById }) {
        // Sum of all inter-workflow delay blocks (already randomised)
        const totalInterDelayMs = nodes
            .filter(n => n.label === 'delay')
            .reduce((sum, n) => sum + (n.data.time || 0), 0);

        console.log('\n  ── Time estimation (bottom-up block analysis) ───────────');
        console.log(`  Combinations              : ${validCombinations.length}`);
        console.log(`  Inter-workflow delays     : ${(totalInterDelayMs / 1000).toFixed(1)}s  (${totalInterDelayMs.toLocaleString()} ms)`);

        let totalWorkflowExecMs = 0;
        const perWorkflowAnalysis = [];

        for (const combo of validCombinations) {
            const docId = combo.automa_workflow_id;
            const doc   = docById.get(docId);

            if (!doc) {
                const fallback = 60_000;
                console.warn(`    ⚠️  Doc ${docId} missing from docById — using 60 s fallback`);
                totalWorkflowExecMs += fallback;
                perWorkflowAnalysis.push({ docId, name: '(missing)', estimatedMs: fallback, breakdown: { reason: 'doc_missing' } });
                continue;
            }

            const { estimatedMs, breakdown } = this.analyzeWorkflowDuration(doc);
            totalWorkflowExecMs += estimatedMs;

            perWorkflowAnalysis.push({
                docId,
                name:        doc.name || docId,
                estimatedMs,
                breakdown,
            });

            console.log(
                `    [wf] "${doc.name || docId}" → ${(estimatedMs / 1000).toFixed(1)}s` +
                `  (delays: ${(breakdown.delay_ms / 1000).toFixed(1)}s,` +
                ` clicks: ${(breakdown.click_ms / 1000).toFixed(1)}s,` +
                ` other: ${(breakdown.other_ms / 1000).toFixed(1)}s)`
            );
        }

        const totalMs = totalWorkflowExecMs + totalInterDelayMs;

        console.log(`  Total workflow exec (all) : ${(totalWorkflowExecMs / 1000).toFixed(1)}s  (${totalWorkflowExecMs.toLocaleString()} ms)`);
        console.log(`  ─────────────────────────────────────────────────────`);
        console.log(`  Estimated total           : ${(totalMs / 1000).toFixed(1)}s  (${totalMs.toLocaleString()} ms)`);
        console.log(`  (wait_margin_ms added by caller)`);

        return {
            totalMs,
            totalInterDelayMs,
            workflowExecMs:      totalWorkflowExecMs,
            estimationMethod:    'bottom_up_analysis',
            perWorkflowAnalysis,
        };
    }

    // ─── Core builder ─────────────────────────────────────────────────────────

    /**
     * @param {Array}  eligibleWorkflows   — from WorkflowFetcher
     * @param {Object} executionSettings   — {
     *                                         delays:          { min_milliseconds, max_milliseconds },
     *                                         wait_margin_ms:  number   (user-configured in Streamlit, default 300 000)
     *                                       }
     */
    async buildOrchestrator(eligibleWorkflows, executionSettings) {
        console.log('\n' + '='.repeat(80));
        console.log('BUILDING DYNAMIC ORCHESTRATOR WORKFLOW');
        console.log('='.repeat(80));

        if (!eligibleWorkflows || eligibleWorkflows.length === 0) {
            throw new Error('buildOrchestrator: eligibleWorkflows is empty.');
        }

        // Read user-configured wait margin (falls back to class default if absent)
        const waitMarginMs = (
            executionSettings?.wait_margin_ms != null &&
            Number.isFinite(executionSettings.wait_margin_ms) &&
            executionSettings.wait_margin_ms >= 0
        )
            ? executionSettings.wait_margin_ms
            : this.DEFAULT_WAIT_MARGIN_MS;

        console.log(`\n  🔍 Bottom-up block analysis will run for all workflows`);
        console.log(`  ⏱️  Wait margin : ${(waitMarginMs / 1000).toFixed(0)}s (${waitMarginMs.toLocaleString()} ms)`);

        // ── Step 1: Load template ────────────────────────────────────────────
        console.log('\nStep 1: Loading orchestrator template...');
        const template = await this.fetchOrchestratorTemplate();

        // ── Step 2: Resolve delay range ──────────────────────────────────────
        console.log('\nStep 2: Resolving delay range...');
        const delays = this.resolveDelayRange(executionSettings);

        // ── Step 3: Validate combinations ────────────────────────────────────
        console.log('\nStep 3: Validating combinations...');
        const validCombinations = eligibleWorkflows.filter(wf => {
            if (!wf.automa_workflow_id) {
                console.warn(`  ⚠️  No automa_workflow_id: ${wf.workflow_name || '(unnamed)'} — skipping`);
                return false;
            }
            if (!wf.link_url) {
                console.warn(`  ⚠️  No link_url: ${wf.workflow_name || wf.automa_workflow_id} — skipping`);
                return false;
            }
            return true;
        });

        console.log(`  ✓ ${validCombinations.length} valid combination(s) (${eligibleWorkflows.length} total)`);

        if (validCombinations.length === 0) {
            throw new Error('All combinations are missing automa_workflow_id or link_url.');
        }

        // ── Step 4: Fetch unique workflow documents ──────────────────────────
        console.log('\nStep 4: Fetching workflow documents from MongoDB...');

        const uniqueDocMap = new Map();
        for (const wf of validCombinations) {
            if (!uniqueDocMap.has(wf.automa_workflow_id)) {
                uniqueDocMap.set(wf.automa_workflow_id, {
                    id:         wf.automa_workflow_id,
                    database:   wf.database_name   || 'execution_workflows',
                    collection: wf.collection_name || 'automa_workflows',
                });
            }
        }

        const groups = {};
        for (const entry of uniqueDocMap.values()) {
            const key = `${entry.database}||${entry.collection}`;
            if (!groups[key]) groups[key] = { database: entry.database, collection: entry.collection, ids: [] };
            groups[key].ids.push(entry.id);
        }

        const allWorkflowDocs = [];
        for (const group of Object.values(groups)) {
            const docs = await this.fetchWorkflowDocuments(group.ids, group.database, group.collection);
            allWorkflowDocs.push(...docs);
        }

        if (allWorkflowDocs.length === 0) {
            throw new Error('fetchWorkflowDocuments returned 0 documents.');
        }

        const docById = new Map(allWorkflowDocs.map(d => [d._id.toString(), d]));

        const missingIds = [...uniqueDocMap.keys()].filter(id => !docById.has(id));
        if (missingIds.length > 0) {
            throw new Error(
                `${missingIds.length} workflow doc(s) not found: ${missingIds.join(', ')}`
            );
        }

        console.log(`\n  ✓ ALL ${allWorkflowDocs.length} unique workflow doc(s) fetched`);

        // ── Step 5: Build includedWorkflows map ──────────────────────────────
        // NOTE: We intentionally do NOT inject screenshot_prefix here yet — we don't
        // have the per-combination prefixes until Step 6. The patch happens in Step 6.5.
        console.log('\nStep 5: Building includedWorkflows map...');
        const includedWorkflows = {};
        for (const doc of allWorkflowDocs) {
            const docId = doc._id.toString();
            const { _id, ...docWithoutId } = doc;
            includedWorkflows[docId] = { ...docWithoutId, id: docId };
        }
        console.log(`  ✓ ${Object.keys(includedWorkflows).length} unique workflow(s) embedded`);

        // ── Step 6: Build nodes + edges ──────────────────────────────────────
        console.log('\nStep 6: Building node graph...');

        const nodes              = [];
        const edges              = [];
        const screenshotPrefixes = {};
        let   xPos    = 0;
        const yPos    = 200;
        const spacing = 300;

        // Trigger block
        const triggerId = this.generateAutomaId();
        nodes.push({
            id: triggerId, type: 'BlockBasic', initialized: false,
            position: { x: xPos, y: yPos },
            data: {
                activeInInput: false, contextMenuName: '', contextTypes: [],
                date: '', days: [], delay: 5,
                description: 'Orchestrator Trigger', disableBlock: false,
                interval: 60, isUrlRegex: false,
                observeElement: {
                    baseElOptions: { attributeFilter: [], attributes: false, characterData: false, childList: true, subtree: false },
                    baseSelector: '', matchPattern: '', selector: '',
                    targetOptions: { attributeFilter: [], attributes: false, characterData: false, childList: true, subtree: false },
                },
                parameters: [], preferParamsInTab: false, shortcut: '',
                time: '00:00', type: 'manual', url: '',
            },
            label: 'trigger',
        });

        let prevId = triggerId;
        xPos      += spacing;

        for (let i = 0; i < validCombinations.length; i++) {
            const combination = validCombinations[i];
            const docId       = combination.automa_workflow_id;
            const doc         = docById.get(docId);
            const isLast      = i === validCombinations.length - 1;

            const screenshotPrefix = `wf${i + 1}_${this.generateAutomaId(5)}_${Date.now()}`;

            screenshotPrefixes[`${docId}_${i}`] = {
                prefix:     screenshotPrefix,
                workflowId: docId,
                link_url:   combination.link_url,
                index:      i,
            };

            const globalData = this.buildWorkflowGlobalData(combination, screenshotPrefix);

            console.log(
                `  [${i + 1}/${validCombinations.length}] "${doc.name || docId}"` +
                ` link="${combination.link_url?.slice(0, 60)}..."` +
                (isLast ? ' (final)' : '')
            );

            const execBlockId = this.generateAutomaId();
            nodes.push({
                id: execBlockId, type: 'BlockBasic', initialized: false,
                position: { x: xPos, y: yPos },
                data: {
                    disableBlock:        false,
                    executeId:           '',
                    workflowId:          docId,
                    globalData,
                    description:         `Execute: ${doc.name || docId}`,
                    insertAllVars:       false,
                    insertAllGlobalData: true,
                },
                label: 'execute-workflow',
            });

            edges.push({
                id: `vueflow__edge-${prevId}-${execBlockId}`, type: 'custom',
                source: prevId, target: execBlockId,
                sourceHandle: `${prevId}-output-1`, targetHandle: `${execBlockId}-input-1`,
                updatable: true, selectable: true, data: {}, label: '', markerEnd: 'arrowclosed',
            });

            prevId  = execBlockId;
            xPos   += spacing;

            if (!isLast) {
                const delayMs      = this.getRandomDelay(delays.min_milliseconds, delays.max_milliseconds);
                const delayBlockId = this.generateAutomaId();

                nodes.push({
                    id: delayBlockId, type: 'BlockDelay', initialized: false,
                    position: { x: xPos, y: yPos },
                    data: { disableBlock: false, time: delayMs, description: `Wait ${(delayMs / 1000).toFixed(1)}s` },
                    label: 'delay',
                });

                edges.push({
                    id: `vueflow__edge-${execBlockId}-${delayBlockId}`, type: 'custom',
                    source: execBlockId, target: delayBlockId,
                    sourceHandle: `${execBlockId}-output-1`, targetHandle: `${delayBlockId}-input-1`,
                    updatable: true, selectable: true, data: {}, label: '', markerEnd: 'arrowclosed',
                });

                prevId  = delayBlockId;
                xPos   += spacing;

                console.log(`         → delay ${(delayMs / 1000).toFixed(1)}s (${delayMs.toLocaleString()} ms)`);
            }
        }

        // ── Step 6.5: Patch includedWorkflows with screenshot_prefix ─────────
        //
        // WHY THIS IS NEEDED:
        //   Automa's execute-workflow block passes the orchestrator's globalData into
        //   the sub-workflow via insertAllGlobalData=true. However, {{screenshot_prefix}}
        //   in take-screenshot filename fields is resolved from the sub-workflow's OWN
        //   variable scope (its globalData), not the injected parent scope.
        //   Without this patch, the filename renders literally as:
        //     tweet_{{screenshot_prefix}}.png
        //   With this patch, the sub-workflow's own globalData contains the correct prefix
        //   so Automa resolves it properly:
        //     tweet_wf1_Co4FM_1771516245590.png
        //
        //   For multi-combination runs where the same docId appears more than once, the
        //   last iteration's prefix wins in includedWorkflows. This is acceptable because
        //   each combination's execute-workflow block also injects its own prefix via its
        //   own globalData field — the sub-workflow's globalData is just the fallback scope.
        //
        console.log('\nStep 6.5: Patching includedWorkflows with screenshot_prefix...');
        for (const [key, entry] of Object.entries(screenshotPrefixes)) {
            const docId = entry.workflowId;
            if (!includedWorkflows[docId]) {
                console.warn(`  ⚠️  Cannot patch docId ${docId} — not in includedWorkflows`);
                continue;
            }

            let existingGlobalData = {};
            try {
                const raw = includedWorkflows[docId].globalData;
                if (raw) {
                    existingGlobalData = typeof raw === 'string' ? JSON.parse(raw) : raw;
                }
            } catch (parseErr) {
                console.warn(`  ⚠️  Could not parse globalData for ${docId}: ${parseErr.message} — starting fresh`);
                existingGlobalData = {};
            }

            existingGlobalData.screenshot_prefix = entry.prefix;
            includedWorkflows[docId].globalData   = JSON.stringify(existingGlobalData);

            console.log(`  ✓ Patched "${includedWorkflows[docId].name || docId}" → prefix: ${entry.prefix}`);
        }

        // ── Step 6.6: Estimate total execution time (always bottom-up) ────────
        console.log('\nStep 6.6: Estimating total execution time (bottom-up)...');
        const timingResult = this._estimateTotalExecutionMs({
            nodes,
            validCombinations,
            docById,
        });

        // ── Step 7: Assemble orchestrator ────────────────────────────────────
        const orchestrator = {
            extVersion: template.extVersion || '1.30.00',
            name:       `orchestrator_${new Date().toISOString().split('T')[0]}`,
            icon:       template.icon || 'riGlobalLine',
            table:      [],
            version:    template.version || '1.30.00',
            drawflow: {
                nodes, edges,
                position: [-252.625, 9], zoom: 1.3,
                viewport: { x: -252.625, y: 9, zoom: 1.3 },
            },
            settings: template.settings || {
                publicId: '', aipowerToken: '', blockDelay: 0,
                saveLog: true, debugMode: false, restartTimes: 3,
                notification: true, execContext: 'popup',
                reuseLastState: false, inputAutocomplete: true,
                onError: 'stop-workflow', executedBlockOnWeb: false,
                insertDefaultColumn: false, defaultColumnName: 'column',
            },
            globalData: JSON.stringify({
                execution_date:         new Date().toISOString(),
                total_combinations:     validCombinations.length,
                unique_workflows:       Object.keys(includedWorkflows).length,
                delay_min_ms:           delays.min_milliseconds,
                delay_max_ms:           delays.max_milliseconds,
                delay_range_display:    `${delays.min_milliseconds / 1000}s–${delays.max_milliseconds / 1000}s`,
                estimated_total_ms:     timingResult.totalMs,
                estimated_total_s:      Math.round(timingResult.totalMs / 1000),
                total_inter_delay_ms:   timingResult.totalInterDelayMs,
                workflow_exec_ms:       timingResult.workflowExecMs,
                estimation_method:      timingResult.estimationMethod,
                wait_margin_ms:         waitMarginMs,
            }, null, '\t'),
            description: `Auto-generated orchestrator for ${validCombinations.length} combination(s)`,
            includedWorkflows,
        };

        // Runtime-only — read by dynamic-orchestrator.js, not sent to Automa
        orchestrator._screenshotPrefixes = screenshotPrefixes;
        orchestrator._timing = {
            estimated_total_ms:      timingResult.totalMs,
            total_inter_delay_ms:    timingResult.totalInterDelayMs,
            workflow_exec_ms:        timingResult.workflowExecMs,
            estimation_method:       timingResult.estimationMethod,
            per_workflow_analysis:   timingResult.perWorkflowAnalysis,
            wait_margin_ms:          waitMarginMs,         // user-configured, passed to resolveWaitTime()
            workflow_count:          validCombinations.length,
            delay_range:             delays,
        };

        // ── Step 8: Final completeness check ─────────────────────────────────
        console.log('\nStep 8: Final completeness verification...');

        const execBlocks     = orchestrator.drawflow.nodes.filter(n => n.label === 'execute-workflow');
        const missingFromMap = execBlocks.filter(n => !orchestrator.includedWorkflows[n.data.workflowId]);

        if (missingFromMap.length > 0) {
            throw new Error(`CRITICAL: ${missingFromMap.length} execute-workflow block(s) not in includedWorkflows`);
        }

        const missingFlag = execBlocks.filter(n => n.data.insertAllGlobalData !== true);
        if (missingFlag.length > 0) {
            throw new Error(`CRITICAL: ${missingFlag.length} execute-workflow block(s) have insertAllGlobalData !== true`);
        }

        const missingLinkUrl = execBlocks.filter(n => {
            try { return !JSON.parse(n.data.globalData)?.link_url; } catch { return true; }
        });
        if (missingLinkUrl.length > 0) {
            console.warn(`  ⚠️  ${missingLinkUrl.length} execute-workflow block(s) have no link_url in globalData`);
        }

        // Verify screenshot_prefix was successfully patched into every includedWorkflow
        const unpatchedDocs = Object.entries(includedWorkflows).filter(([, doc]) => {
            try {
                const gd = typeof doc.globalData === 'string' ? JSON.parse(doc.globalData) : doc.globalData;
                return !gd?.screenshot_prefix;
            } catch { return true; }
        });
        if (unpatchedDocs.length > 0) {
            console.warn(`  ⚠️  ${unpatchedDocs.length} includedWorkflow(s) missing screenshot_prefix in globalData:`);
            unpatchedDocs.forEach(([id]) => console.warn(`       - ${id}`));
        } else {
            console.log(`  ✓ All ${Object.keys(includedWorkflows).length} includedWorkflow(s) have screenshot_prefix patched`);
        }

        const delayNodes = orchestrator.drawflow.nodes.filter(n => n.label === 'delay');

        console.log(`  ✓ All ${execBlocks.length} execute-workflow blocks verified`);
        console.log(`  ✓ All execute blocks have insertAllGlobalData=true`);

        console.log('\n' + '='.repeat(80));
        console.log('✓ ORCHESTRATOR BUILT SUCCESSFULLY');
        console.log(`  Combinations         : ${validCombinations.length}`);
        console.log(`  Unique workflows     : ${Object.keys(includedWorkflows).length}`);
        console.log(`  Execute blocks       : ${execBlocks.length}`);
        console.log(`  Delay blocks         : ${delayNodes.length}`);
        console.log(`  Delay range          : ${delays.min_milliseconds}ms–${delays.max_milliseconds}ms`);
        if (delayNodes.length > 0) {
            const times = delayNodes.map(n => n.data.time);
            console.log(`  Actual delays        : ${Math.min(...times)}ms–${Math.max(...times)}ms`);
        }
        console.log(`  Estimation method    : ${timingResult.estimationMethod}`);
        console.log(`  Workflow exec total  : ${(timingResult.workflowExecMs / 1000).toFixed(1)}s`);
        console.log(`  Inter-workflow delays: ${(timingResult.totalInterDelayMs / 1000).toFixed(1)}s`);
        console.log(`  Estimated total      : ${(timingResult.totalMs / 1000).toFixed(0)}s`);
        console.log(`  Wait margin          : ${(waitMarginMs / 1000).toFixed(0)}s  (user-configured)`);
        console.log(`  Configured wait      : ${((timingResult.totalMs + waitMarginMs) / 1000).toFixed(0)}s  (total + margin)`);
        console.log('='.repeat(80) + '\n');

        return orchestrator;
    }

    // ─── Persistence ──────────────────────────────────────────────────────────

    async saveOrchestrator(orchestrator, executionId) {
        try {
            const timing = orchestrator._timing || {};
            const result = await this.mongoDBService.db
                .collection('generated_orchestrators')
                .insertOne({
                    execution_id:           executionId,
                    orchestrator_data:      orchestrator,
                    workflow_count:         Object.keys(orchestrator.includedWorkflows || {}).length,
                    node_count:             orchestrator.drawflow?.nodes?.length || 0,
                    estimated_total_ms:     timing.estimated_total_ms    ?? null,
                    total_inter_delay_ms:   timing.total_inter_delay_ms  ?? null,
                    workflow_exec_ms:       timing.workflow_exec_ms       ?? null,
                    estimation_method:      timing.estimation_method      ?? null,
                    wait_margin_ms:         timing.wait_margin_ms         ?? null,
                    per_workflow_analysis:  timing.per_workflow_analysis  ?? null,
                    delay_range:            timing.delay_range            ?? null,
                    created_at:             new Date(),
                    status:                 'pending',
                });
            console.log(`✓ Orchestrator saved to generated_orchestrators: ${result.insertedId}`);
            return result.insertedId.toString();
        } catch (err) {
            console.warn(`⚠️  Could not save orchestrator to MongoDB: ${err.message}`);
            return null;
        }
    }
}
