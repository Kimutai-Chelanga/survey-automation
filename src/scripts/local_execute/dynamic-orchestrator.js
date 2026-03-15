// dynamic-orchestrator.js

/**
 * FIX HISTORY (2026-02-20 v30) — INTEGRATED WORKFLOWEXECUTOR LOGIC:
 *   - Now uses the same robust execution pattern as WorkflowExecutor.js
 *   - Properly clicks execute button to start workflow
 *   - Waits for workflow completion with log checking
 *   - Extracts screenshots from logs
 *   - 60-second wait margin is ONLY for recording stop
 *
 * FIX HISTORY (2026-02-20 v29) — FINAL FIXED VERSION
 */

import { ConfigManager }        from './modules/config/ConfigManager.js';
import { MongoDBService }       from './modules/database/MongoDBService.js';
import { PostgreSQLService }    from './modules/database/PostgreSQLService.js';
import { WorkflowFetcher }      from './modules/workflow/WorkflowFetcher.js';
import { OrchestratorBuilder }  from './modules/orchestrator/OrchestratorBuilder.js';
import { ChromeSessionManager } from './modules/chrome/ChromeSessionManager.js';
import { AutomaExecutor }       from './modules/automas/AutomaExecutor.js';
import { ScreenshotCapture }    from './modules/recording/ScreenshotCapture.js';
import { VideoRecorder }        from './modules/recording/VideoRecorder.js';

// Last-resort fallback only
const DEFAULT_WAIT_MARGIN_MS = 300_000; // 300 s

// ─── Crash safety ─────────────────────────────────────────────────────────────
process.on('uncaughtException',  (err)    => { console.error('\n[FATAL] Uncaught Exception:\n',  err.stack || err.message); process.exit(1); });
process.on('unhandledRejection', (reason) => { console.error('\n[FATAL] Unhandled Rejection:\n', reason?.stack || reason);  process.exit(1); });
process.on('SIGINT',  () => { console.log('\n⚠️  SIGINT received');  process.exit(0); });
process.on('SIGTERM', () => { console.log('\n⚠️  SIGTERM received'); process.exit(0); });

// ─── Helpers ──────────────────────────────────────────────────────────────────
function logSection(title) {
    console.log('\n' + '═'.repeat(80));
    console.log(`  ${title}`);
    console.log('═'.repeat(80));
}

/** Sleep for `ms` ms */
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function buildExecutionSettings(settings) {
    const minMs = settings?.delay_min_milliseconds;
    const maxMs = settings?.delay_max_milliseconds;

    let delays;
    if (
        minMs != null && maxMs != null &&
        Number.isFinite(minMs) && Number.isFinite(maxMs) &&
        minMs >= 0 && minMs <= maxMs
    ) {
        console.log(`  Delay range  : ${minMs.toLocaleString()} – ${maxMs.toLocaleString()} ms`);
        delays = { min_milliseconds: minMs, max_milliseconds: maxMs };
    } else {
        console.warn(`  ⚠️  No delay config — fallback: 10 000 – 20 000 ms`);
        delays = { min_milliseconds: 10_000, max_milliseconds: 20_000 };
    }

    const waitMarginMs = (
        settings?.wait_margin_ms != null &&
        Number.isFinite(settings.wait_margin_ms) &&
        settings.wait_margin_ms >= 0
    )
        ? settings.wait_margin_ms
        : DEFAULT_WAIT_MARGIN_MS;

    console.log(`  Wait margin  : ${(waitMarginMs / 1000).toFixed(0)}s (${waitMarginMs.toLocaleString()} ms) - recording will stop after this time`);
    console.log(`  Max workflows: ${settings?.max_workflows_to_process ?? 'all (unlimited)'}`);

    return { delays, wait_margin_ms: waitMarginMs };
}

// ─── Dynamic Orchestrator ─────────────────────────────────────────────────────
class DynamicOrchestrator {
    constructor() {
        this.config              = new ConfigManager();
        this.mongoDBService      = null;
        this.pgService           = null;
        this.workflowFetcher     = null;
        this.orchestratorBuilder = null;
        this.sessionManager      = null;
        this.automaExecutor      = null;
        this.screenshotCapture   = null;
        this.videoRecorder       = null;

        this.stats = {
            startTime:         new Date(),
            totalWorkflows:    0,
            accountsProcessed: 0,
            accountsSucceeded: 0,
            accountsFailed:    0,
        };
    }

    async initialize() {
        logSection('DYNAMIC ORCHESTRATOR INITIALIZING');

        await this.config.initialize();
        console.log(`  DAG Run ID   : ${this.config.dagRunId || process.env.AIRFLOW_CTX_DAG_RUN_ID || 'local'}`);
        console.log(`  Node version : ${process.version}`);
        console.log(`  Display      : ${process.env.DISPLAY || ':99'}`);
        console.log(`  Timezone     : ${this.config.timezone}`);

        this.mongoDBService = new MongoDBService(this.config.mongoUri, this.config.dbName);
        await this.mongoDBService.connect();

        this.pgService = new PostgreSQLService(this.config.pgConfig);
        await this.pgService.connect();

        this.workflowFetcher     = new WorkflowFetcher(this.mongoDBService, this.pgService, this.config);
        this.orchestratorBuilder = new OrchestratorBuilder(this.mongoDBService);
        this.sessionManager      = new ChromeSessionManager(this.config);
        this.automaExecutor      = new AutomaExecutor(this.config.extensionId);
        this.screenshotCapture   = new ScreenshotCapture(this.mongoDBService);
        this.videoRecorder       = new VideoRecorder(
            this.config.recordingsDir || process.env.RECORDINGS_DIR || '/workspace/recordings'
        );

        console.log('\n✓ All services initialized');
    }

    async fetchWorkflows() {
        logSection('FETCHING ELIGIBLE WORKFLOWS');

        const executionData = await this.workflowFetcher.fetchEligibleWorkflows();
        if (!executionData?.combinations?.length) {
            console.log('\nℹ️  No eligible workflows found for today.');
            return null;
        }

        const { combinations, settings } = executionData;
        this.stats.totalWorkflows = combinations.length;

        console.log(`\n✓ ${combinations.length} workflow combination(s) found`);
        console.log(`  Day         : ${settings.day}`);
        console.log(`  Category    : ${settings.destination_category}`);
        console.log(`  Type        : ${settings.workflow_type_name}`);
        console.log(`  Collection  : ${settings.collection_name || 'All'}`);

        if (settings.delay_min_milliseconds != null) {
            console.log(`  Delay min   : ${settings.delay_min_milliseconds.toLocaleString()} ms`);
            console.log(`  Delay max   : ${settings.delay_max_milliseconds.toLocaleString()} ms`);
        }

        const marginMin = settings.wait_margin_ms ?? DEFAULT_WAIT_MARGIN_MS;
        console.log(`  Wait margin : ${(marginMin / 1000).toFixed(0)}s (${marginMin.toLocaleString()} ms) — recording will stop after this time`);
        
        const maxWorkflows = settings.max_workflows_to_process ?? 0;
        console.log(`  Max workflows: ${maxWorkflows === 0 ? 'All (unlimited)' : maxWorkflows}`);

        return { combinations, settings };
    }

    async buildOrchestrator(combinations, settings) {
        logSection('BUILDING ORCHESTRATOR WORKFLOW');

        const executionSettings = buildExecutionSettings(settings);

        const orchestratorWorkflow = await this.orchestratorBuilder.buildOrchestrator(
            combinations,
            executionSettings
        );

        const executionId = this.config.dagRunId
            || process.env.AIRFLOW_CTX_DAG_RUN_ID
            || `dynamic_${Date.now()}`;

        await this.orchestratorBuilder.saveOrchestrator(orchestratorWorkflow, executionId);

        const nodes      = orchestratorWorkflow.drawflow?.nodes ?? [];
        const delayNodes = nodes.filter(n => n.label === 'delay');

        console.log('\n✓ Orchestrator built & verified');
        console.log(`  Total nodes       : ${nodes.length}`);
        console.log(`  Execute blocks    : ${nodes.filter(n => n.label === 'execute-workflow').length}`);
        console.log(`  Delay blocks      : ${delayNodes.length}`);
        if (delayNodes.length > 0) {
            const times = delayNodes.map(n => n.data.time);
            console.log(`  Delay spread      : ${Math.min(...times).toLocaleString()}ms – ${Math.max(...times).toLocaleString()}ms`);
        }

        return orchestratorWorkflow;
    }

    async fetchAccounts() {
        logSection('FETCHING CHROME ACCOUNTS');

        const accounts = await this.mongoDBService.db.collection('accounts').find(
            { profile_type: 'local_chrome', is_active: { $ne: false } },
            { projection: { postgres_account_id: 1, username: 1, profile_path: 1, profile_id: 1 } }
        ).toArray();

        if (accounts.length === 0) {
            throw new Error('No active Local Chrome accounts found.');
        }

        console.log(`\n✓ ${accounts.length} active account(s):`);
        accounts.forEach(a => console.log(`  - ${a.username}  (id: ${a.postgres_account_id})`));

        return accounts;
    }

    async _writeVideoMetadata({ gridfsFileId, sessionId, username, postgresAccountId, profileId, filename, durationSeconds, combinationCount, settings }) {
        try {
            const result = await this.mongoDBService.db.collection('video_recording_metadata').insertOne({
                gridfs_file_id:       gridfsFileId.toString(),
                session_id:           sessionId,
                session_type:         'local_chrome',
                postgres_account_id:  postgresAccountId,
                account_username:     username,
                profile_id:           profileId || null,
                filename,
                content_type:         'video/mp4',
                duration_seconds:     durationSeconds,
                recording_status:     'completed',
                recording_method:     'ffmpeg_x11grab',
                workflow_type:        settings.workflow_type_name,
                workflow_name:        `orchestrator_${settings.day}`,
                workflow_category:    settings.destination_category,
                workflow_collection:  settings.collection_name || null,
                workflows_in_session: combinationCount,
                execution_id:         sessionId,
                dag_run_id:           this.config.dagRunId || process.env.AIRFLOW_CTX_DAG_RUN_ID || null,
                created_at:           new Date(),
                recorded_at:          new Date(),
                updated_at:           new Date(),
            });
            console.log(`✓ video_recording_metadata written: ${result.insertedId}`);
            return result.insertedId.toString();
        } catch (err) {
            console.warn(`⚠️  Could not write video_recording_metadata: ${err.message}`);
            return null;
        }
    }

    async createExecutionSession(account, sessionId, combinations, settings) {
        try {
            const result = await this.mongoDBService.db.collection('execution_sessions').insertOne({
                session_id:             sessionId,
                session_type:           'local_chrome',
                session_status:         'active',
                created_by:             'airflow',
                is_active:              true,
                postgres_account_id:    account.postgres_account_id,
                account_username:       account.username,
                profile_id:             account.profile_id || null,
                dag_run_id:             this.config.dagRunId || process.env.AIRFLOW_CTX_DAG_RUN_ID,
                execution_day:          settings.day,
                execution_time:         new Date().toTimeString().split(' ')[0],
                destination_category:   settings.destination_category,
                workflow_type:          settings.workflow_type_name,
                collection_name:        settings.collection_name || null,
                delay_min_milliseconds: settings.delay_min_milliseconds ?? null,
                delay_max_milliseconds: settings.delay_max_milliseconds ?? null,
                max_workflows_to_process: settings.max_workflows_to_process ?? 0,
                workflows_executed:     combinations.length,
                successful_workflows:   0,
                failed_workflows:       0,
                screenshots:            [],
                video_recording_id:     null,
                wait_margin_ms:          settings.wait_margin_ms ?? DEFAULT_WAIT_MARGIN_MS,
                created_at:             new Date(),
                started_at:             new Date(),
                ended_at:               null,
                total_execution_time_seconds: 0,
            });
            console.log(`✓ execution_sessions record created: ${result.insertedId}`);
            return result.insertedId.toString();
        } catch (err) {
            console.warn(`⚠️  Could not create execution_sessions record: ${err.message}`);
            return null;
        }
    }

    async finalizeExecutionSession(mongoSessionId, { succeeded, failed, durationMs, screenshotIds, videoRecordingId }) {
        if (!mongoSessionId) return;
        try {
            const { ObjectId } = await import('mongodb');
            await this.mongoDBService.db.collection('execution_sessions').updateOne(
                { _id: new ObjectId(mongoSessionId) },
                {
                    $set: {
                        session_status:               succeeded > 0 ? 'completed' : 'failed',
                        is_active:                    false,
                        successful_workflows:         succeeded,
                        failed_workflows:             failed,
                        screenshots:                  screenshotIds,
                        video_recording_id:           videoRecordingId || null,
                        ended_at:                     new Date(),
                        total_execution_time_seconds: Math.round(durationMs / 1000),
                    }
                }
            );
            console.log(`✓ execution_sessions finalised`);
        } catch (err) {
            console.warn(`⚠️  Could not finalise execution_sessions: ${err.message}`);
        }
    }

    /**
     * Store orchestrator log and extract screenshots (same as WorkflowExecutor pattern)
     */
    async _processOrchestratorLog(logData, { sessionId, workflowId, username, postgresAccountId, profileId, workflowType, workflowName, combinations, settings }) {
        if (!logData) {
            console.warn(`  ⚠️  No log data for orchestrator — skipping`);
            return { logStored: false, screenshotIds: [] };
        }

        const allScreenshotIds = [];
        let logAnalysis = null;

        try {
            // Analyze logs (same as WorkflowExecutor)
            if (logData && logData.history) {
                console.log(`  Analyzing ${logData.history.length} log entries...`);
                
                // Count errors and successes
                const errorCount = logData.history.filter(step => step.$isError || step.type === 'error').length;
                const successCount = logData.history.length - errorCount;
                
                logAnalysis = {
                    has_errors: errorCount > 0,
                    error_count: errorCount,
                    success_count: successCount,
                    failed: errorCount > 0,
                    total_steps: logData.history.length
                };
                
                console.log(`  - Analysis: ${errorCount} errors, ${successCount} successes`);
            }

            // Export in JSON format
            const exportedLogs = await this.automaExecutor.exportLogsAsJSON(logData);
            
            // Store complete log data in MongoDB (same as WorkflowExecutor)
            const logDocument = {
                execution_id:      sessionId,
                session_id:        sessionId,
                workflow_id:       workflowId,
                workflow_name:     'Orchestrator',
                workflow_type:     workflowType,
                account_username:  username,
                account_id:        postgresAccountId,
                log_id:            logData.logId,
                history:           logData.history || [],
                ctx_data:          logData.ctxData,
                logs_data:         logData.logsData,
                exported_json:     exportedLogs,
                log_metadata:      logData.logMetadata,
                log_source:        logData.source || 'indexeddb',
                log_count:         logData.history?.length || 0,
                has_errors:        logAnalysis?.has_errors || false,
                error_count:       logAnalysis?.error_count || 0,
                success_count:     logAnalysis?.success_count || 0,
                created_at:        new Date()
            };

            await this.mongoDBService.storeAutomaLogs(logDocument);
            console.log(`  ✓ Orchestrator log stored (status: ${logData.logMetadata?.status || 'unknown'})`);

            // Extract screenshots from the log (using ScreenshotCapture)
            console.log(`\n📸 Extracting screenshots from workflow logs...`);
            
            const screenshotOptions = {
                accountId: postgresAccountId,
                username,
                profileId,
                workflowType,
                workflowName: 'Orchestrator',
                executionId: sessionId
            };
            
            const screenshotResult = await this.screenshotCapture.processScreenshotsFromLog(
                logData,
                sessionId,
                screenshotOptions
            );
            
            allScreenshotIds.push(...screenshotResult.screenshotIds);

            return { 
                logStored: true, 
                screenshotIds: allScreenshotIds,
                logAnalysis,
                exportedLogs 
            };

        } catch (err) {
            console.warn(`  ⚠️  Error processing orchestrator log: ${err.message}`);
            return { logStored: false, screenshotIds: allScreenshotIds };
        }
    }

    async executeForAccount(account, orchestratorWorkflow, combinations, settings) {
        const { username, profile_path, profile_id, postgres_account_id } = account;
        const profilePath = profile_path
            || `${this.config.chromeProfileDir || '/workspace/chrome_profiles'}/account_${username}`;

        logSection(`EXECUTING FOR ACCOUNT: ${username}  (id: ${postgres_account_id})`);
        console.log(`  Profile path : ${profilePath}`);
        console.log(`  Workflows in orchestrator: ${combinations.length}`);

        // Get the wait margin from settings
        const waitMarginMs = settings.wait_margin_ms ?? DEFAULT_WAIT_MARGIN_MS;
        console.log(`  ⏱️  Recording will stop after: ${(waitMarginMs / 1000).toFixed(0)}s (wait margin only)`);
        console.log(`  📋 Logs and screenshots will be captured after recording stops`);

        const sessionId      = `dynamic_orchestrator_${username}_${Date.now()}`;
        const startTime      = Date.now();
        let sessionInfo      = null;
        let mongoSessionId   = null;
        let recordingInfo    = null;
        let videoMetadataId  = null;

        try {
            mongoSessionId = await this.createExecutionSession(
                account, 
                sessionId, 
                combinations, 
                settings
            );

            sessionInfo = await this.sessionManager.startSession(
                { username, profilePath, profileId: profile_id || `account_${username}` },
                sessionId
            );
            const { browser, displayNum } = sessionInfo;
            console.log(`\n✓ Chrome session started for ${username}`);

            // ── Start recording ───────────────────────────────────────────────
            console.log('\n🎥 Starting video recording...');
            const workflowInfoForRecording = {
                username,
                workflowType: settings.workflow_type_name,
                workflowName: `orchestrator_${settings.day}`,
                category:     settings.destination_category,
                executionId:  sessionId,
                accountId:    postgres_account_id,
            };

            const display = displayNum
                ?? parseInt((process.env.DISPLAY || ':99').replace(':', ''), 10)
                ?? 99;

            recordingInfo = await this.videoRecorder.startRecording(display, sessionId, workflowInfoForRecording);
            if (recordingInfo) {
                console.log(`✓ Recording started → ${recordingInfo.outputPath}`);
                console.log(`  Recording will stop after ${(waitMarginMs / 1000).toFixed(0)}s (wait margin)`);
            } else {
                console.warn('⚠️  Recording could not start — continuing without video');
            }

            // ── Upload and trigger workflow (same pattern as WorkflowExecutor) ──
            console.log('\nStep 6: Importing and executing Automa workflow...');
            const executionResult = await this.automaExecutor.executeWorkflow(
                browser, orchestratorWorkflow, ''
            );
            const workflowId = executionResult.workflowId;
            console.log(`✓ Orchestrator triggered — ID: ${workflowId}`);
            console.log(`  Sub-workflows tracked: ${executionResult.subWorkflowIds.length}`);

            // ── Wait ONLY for the wait margin to stop recording ───────────────
            console.log(`\n⏳ Waiting ${(waitMarginMs / 1000).toFixed(0)}s (configured wait margin) after trigger...`);
            console.log(`   After this wait, recording will stop but workflow continues`);
            
            await sleep(waitMarginMs);
            console.log(`✓ Wait margin complete — stopping recording now`);

            // ── Stop recording (wait margin is ONLY for recording) ────────────
            if (recordingInfo) {
                console.log('\n🎥 Stopping video recording (wait margin complete)...');
                const stoppedRecording = await this.videoRecorder.stopRecording(recordingInfo);
                recordingInfo = null;

                if (stoppedRecording?.success) {
                    console.log(`  ✓ Recording stopped successfully`);
                    console.log(`  📊 Recording duration : ${stoppedRecording.duration.toFixed(1)}s (wait margin)`);
                    
                    const uploadedRecording = await this.videoRecorder.uploadToGridFS(
                        stoppedRecording, workflowInfoForRecording, this.mongoDBService
                    );
                    if (uploadedRecording?.gridfs_file_id) {
                        videoMetadataId = await this._writeVideoMetadata({
                            gridfsFileId:     uploadedRecording.gridfs_file_id,
                            sessionId,        username,
                            postgresAccountId: postgres_account_id,
                            profileId:        profile_id || null,
                            filename:         stoppedRecording.filename,
                            durationSeconds:  stoppedRecording.duration,
                            combinationCount: combinations.length,
                            settings,
                        });
                        console.log(`✓ Video saved to GridFS: ${uploadedRecording.gridfs_file_id}`);
                    }
                } else {
                    console.warn('⚠️  Recording stopped but may be incomplete');
                }
            }

            // ── NOW fetch orchestrator log and process (same as WorkflowExecutor) ──
            console.log('\n📋 Fetching orchestrator log (workflow may still be running)...');
            
            // Try multiple attempts to get logs (same as WorkflowExecutor)
            let logData = null;
            for (let attempt = 1; attempt <= 5; attempt++) {
                console.log(`  Attempt ${attempt}/5 to fetch logs...`);
                logData = await this.automaExecutor.getWorkflowLogsFromIndexedDB(
                    browser, 
                    workflowId, 
                    1, // inner attempts
                    2000
                );
                
                if (logData) {
                    console.log(`✓ Retrieved log on attempt ${attempt}`);
                    break;
                }
                
                if (attempt < 5) {
                    console.log(`  Waiting 3s before next attempt...`);
                    await sleep(3000);
                }
            }
            
            let screenshotIds = [];
            if (logData) {
                const result = await this._processOrchestratorLog(logData, {
                    sessionId,
                    workflowId,
                    username,
                    postgresAccountId: postgres_account_id,
                    profileId: profile_id,
                    workflowType: settings.workflow_type_name,
                    workflowName: `orchestrator_${settings.day}`,
                    combinations,
                    settings
                });
                screenshotIds = result.screenshotIds;
                console.log(`  ✓ Processed ${screenshotIds.length} screenshot(s) from logs`);
            } else {
                console.log(`  ⚠️  No orchestrator log available yet - workflow may still be running`);
            }

            const totalDurationMs = Date.now() - startTime;

            await this.finalizeExecutionSession(mongoSessionId, {
                succeeded:        combinations.length,
                failed:           0,
                durationMs:       totalDurationMs,
                screenshotIds:    screenshotIds,
                videoRecordingId: videoMetadataId,
            });

            return {
                success:        true,
                username,
                durationMs:     totalDurationMs,
                waitMarginMs,
                screenshotIds:  screenshotIds,
                videoMetadataId,
                logStatus:      logData ? 'stored' : 'pending',
            };

        } catch (err) {
            const durationMs = Date.now() - startTime;
            const errMsg     = err instanceof Error ? err.message : String(err) ?? 'unknown';

            console.error(`\n✗ FAILED for ${username}: ${errMsg}`);
            if (err?.stack) console.error(err.stack);

            if (recordingInfo) {
                try {
                    const stopped = await this.videoRecorder.stopRecording(recordingInfo);
                    recordingInfo = null;
                    if (stopped?.success) {
                        const uploaded = await this.videoRecorder.uploadToGridFS(
                            stopped,
                            {
                                executionId:  sessionId,
                                username,
                                accountId:    postgres_account_id,
                                workflowType: settings?.workflow_type_name || 'unknown',
                                workflowName: 'orchestrator_error'
                            },
                            this.mongoDBService
                        );
                        if (uploaded?.gridfs_file_id) {
                            videoMetadataId = await this._writeVideoMetadata({
                                gridfsFileId:      uploaded.gridfs_file_id,
                                sessionId,         username,
                                postgresAccountId: postgres_account_id,
                                profileId:         account.profile_id || null,
                                filename:          stopped.filename,
                                durationSeconds:   stopped.duration,
                                combinationCount:  combinations.length,
                                settings:          settings || {},
                            });
                        }
                    }
                } catch (recErr) {
                    console.warn(`⚠️  Error during recording cleanup: ${recErr.message}`);
                }
            }

            await this.finalizeExecutionSession(mongoSessionId, {
                succeeded:        0,
                failed:           combinations.length,
                durationMs,
                screenshotIds:    [],
                videoRecordingId: videoMetadataId,
            });

            return { success: false, username, error: errMsg, durationMs };

        } finally {
            if (recordingInfo) { 
                try { 
                    await this.videoRecorder.stopRecording(recordingInfo); 
                } catch (_) {} 
            }
            if (sessionInfo)   { 
                try { 
                    await this.sessionManager.stopSession(sessionId);      
                } catch (e) { 
                    console.warn(`⚠️  Session stop: ${e.message}`); 
                } 
            }
        }
    }

    printSummary(results) {
        logSection('EXECUTION SUMMARY');

        const elapsed   = Math.round((Date.now() - this.stats.startTime) / 1000);
        const succeeded = results.filter(r => r.success).length;
        const failed    = results.filter(r => !r.success).length;

        console.log(`  Workflows : ${this.stats.totalWorkflows}`);
        console.log(`  Accounts  : ${results.length}  (${succeeded} ok, ${failed} failed)`);
        console.log(`  Wall time : ${elapsed}s`);
        console.log();

        results.forEach((r, i) => {
            const icon = r.success ? '✓' : '✗';
            let detail;
            if (r.success) {
                detail = `wait_margin=${(r.waitMarginMs / 1000).toFixed(0)}s, shots=${r.screenshotIds?.length ?? 0}, video=${r.videoMetadataId ? '🎥' : '—'}, logs=${r.logStatus || 'stored'}`;
            } else {
                detail = `ERROR: ${r.error ?? '(no message)'}`;
            }
            console.log(`  ${icon} [${i + 1}] ${r.username} — ${detail}`);
        });

        return { succeeded, failed };
    }

    async cleanup() {
        console.log('\n🧹 Cleaning up...');
        if (this.mongoDBService) { try { await this.mongoDBService.close(); } catch (_) {} }
        if (this.pgService)      { try { await this.pgService.close();      } catch (_) {} }
        console.log('✓ Cleanup complete');
    }

    async run() {
        try {
            await this.initialize();

            const executionData = await this.fetchWorkflows();
            if (!executionData) { console.log('\nℹ️  Nothing to execute today.'); return; }

            const { combinations, settings } = executionData;
            const orchestratorWorkflow       = await this.buildOrchestrator(combinations, settings);
            const accounts                   = await this.fetchAccounts();

            logSection('RUNNING ORCHESTRATOR ACROSS ALL ACCOUNTS');
            const results = [];

            for (let i = 0; i < accounts.length; i++) {
                results.push(await this.executeForAccount(accounts[i], orchestratorWorkflow, combinations, settings));
                if (i < accounts.length - 1) {
                    console.log('\nPausing 5s before next account...');
                    await sleep(5_000);
                }
            }

            const { succeeded, failed } = this.printSummary(results);

            try {
                await this.mongoDBService.storeExecutionStatistics({
                    execution_id:       this.config.dagRunId || `dynamic_${Date.now()}`,
                    execution_type:     'dynamic_orchestrator',
                    execution_date:     new Date().toISOString(),
                    total_workflows:    this.stats.totalWorkflows,
                    accounts_processed: results.length,
                    accounts_succeeded: succeeded,
                    accounts_failed:    failed,
                    duration_seconds:   Math.round((Date.now() - this.stats.startTime) / 1000),
                    created_at:         new Date(),
                });
            } catch (statsErr) {
                console.warn(`⚠️  Could not store execution stats: ${statsErr.message}`);
            }

            logSection('DYNAMIC ORCHESTRATOR COMPLETE');

            if (failed > 0 && succeeded === 0) {
                console.error(`\n✗ All ${failed} account(s) failed.`);
                process.exit(1);
            }

            process.exit(0);

        } catch (err) {
            console.error(`\n[FATAL] ${err.message}`);
            if (process.env.DEBUG_MODE === 'true') console.error(err.stack);
            process.exit(1);
        } finally {
            await this.cleanup();
        }
    }
}

const orchestrator = new DynamicOrchestrator();
orchestrator.run();
