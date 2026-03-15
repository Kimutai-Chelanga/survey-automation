// modules/workflow/WorkflowExecutor.js

import { ObjectId } from 'mongodb';

/**
 * Executes individual workflows with all the steps
 */
export class WorkflowExecutor {
    constructor(config, mongoDBService, pgService, sessionManager, cookieManager, 
                automaExecutor, videoRecorder, screenshotCapture, profileManager) {
        this.config = config;
        this.mongoDBService = mongoDBService;
        this.pgService = pgService;
        this.sessionManager = sessionManager;
        this.cookieManager = cookieManager;
        this.automaExecutor = automaExecutor;
        this.videoRecorder = videoRecorder;
        this.screenshotCapture = screenshotCapture;
        this.profileManager = profileManager;
    }

    /**
     * Execute a single workflow
     */
    async execute(linkData, workflowMetadata, workflowNumber, totalWorkflows) {
        let sessionId = null;
        let executionSessionId = null;
        let browser = null;
        let sessionInfo = null;
        let recordingInfo = null;
        let workflowId = null;

        const accountProfile = this.profileManager.getProfile(workflowMetadata.postgres_account_id);

        const workflowInfo = {
            linkId: linkData.links_id,
            linkUrl: linkData.link,
            accountId: accountProfile.accountId,
            username: accountProfile.username,
            profileId: accountProfile.profileId,
            profilePath: accountProfile.profilePath,
            workflowName: workflowMetadata.workflow_name,
            workflowType: workflowMetadata.workflow_type,
            metadataId: workflowMetadata.metadata_id,
            executionId: `airflow_${this.config.dagRunId}_link${linkData.links_id}_wf${workflowNumber}`
        };

        console.log('\n' + '█'.repeat(80));
        console.log(`EXECUTING WORKFLOW ${workflowNumber}/${totalWorkflows}`);
        console.log('█'.repeat(80));
        console.log(`Link ID: ${workflowInfo.linkId}`);
        console.log(`URL: ${workflowInfo.linkUrl}`);
        console.log(`Account: ${workflowInfo.username}`);
        console.log(`Workflow: ${workflowInfo.workflowName}`);
        console.log('█'.repeat(80) + '\n');

        const executionStartTime = new Date();

        try {
            // Step 1: Load Automa workflow
            console.log('Step 1: Loading Automa workflow...');
            const automaWorkflow = await this.mongoDBService.fetchAutomaWorkflow(workflowMetadata.automa_workflow_id);
            console.log(`✓ Workflow loaded: ${automaWorkflow.name}`);

            // Step 2: Start Chrome session
            sessionId = `airflow_${workflowInfo.username}_${Date.now()}`;
            console.log('\nStep 2: Starting Chrome session...');
            sessionInfo = await this.sessionManager.startSession(accountProfile, sessionId);
            browser = sessionInfo.browser;
            console.log(`✓ Session started: ${sessionId}`);

            // Step 3: Start video recording
            console.log('\nStep 3: Starting video recording...');
            recordingInfo = await this.videoRecorder.startRecording(
                sessionInfo.displayNum,
                sessionId,
                workflowInfo
            );

            if (recordingInfo) {
                const session = this.sessionManager.getSession(sessionId);
                if (session) {
                    session.recording = recordingInfo;
                    session.workflowInfo = workflowInfo;
                }
                console.log('✓ Video recording started');
            }

            // Step 4: Create execution session record
            console.log('\nStep 4: Creating execution session record...');
            executionSessionId = await this.createExecutionSession(workflowInfo, sessionId, executionStartTime);

            // Step 5: Load cookies
            console.log('\nStep 5: Loading cookies from session_data.json...');
            const pages = await browser.pages();
            const page = pages.length > 0 ? pages[0] : await browser.newPage();

            const cookieResult = await this.cookieManager.loadCookies(page, workflowInfo.profilePath);

            if (!cookieResult.success) {
                console.warn('⚠️ WARNING: Failed to load cookies!');
            } else {
                console.log(`✓ Loaded ${cookieResult.cookiesLoaded} cookies`);
            }

            // Step 6: Import & execute workflow
            console.log('\nStep 6: Importing and executing Automa workflow...');
            const executionResult = await this.automaExecutor.executeWorkflow(browser, automaWorkflow, workflowInfo.linkUrl);
            workflowId = executionResult.workflowId;
            console.log('✓ Workflow execution initiated');

            // Step 7: Pre-execution screenshot
            console.log('\nStep 7: Capturing pre-execution screenshot...');
            const preScreenshots = await this.screenshotCapture.capture(browser, sessionId, workflowInfo, 'preExecution');
            console.log(`✓ Captured ${preScreenshots.length} screenshot(s)`);

            // Step 8: Wait for workflow execution
            const waitTime = 1200000; // 1200s (20 minutes)
            console.log(`\nStep 8: Waiting ${waitTime / 1000}s for workflow execution...`);
            await this.sleep(waitTime);

            // Step 8.1: Fetch Automa logs from IndexedDB
            console.log('\nStep 8.1: Fetching Automa workflow logs...');
            let logData     = null;
            let logAnalysis = null;
            let exportedLogs = null;

            try {
                // PRIMARY: Get complete logs from IndexedDB with 5 polling attempts
                logData = await this.automaExecutor.getWorkflowLogsFromIndexedDB(
                    browser,
                    workflowId,
                    5,    // maxAttempts
                    5000  // delayMs between attempts
                );

                if (logData && logData.history) {
                    console.log(`✓ Retrieved ${logData.history.length} log entries from IndexedDB`);

                    // Analyze logs
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

                    // Export in JSON format
                    try {
                        exportedLogs = await this.automaExecutor.exportLogsAsJSON(logData);
                        if (exportedLogs) {
                            console.log(`  - Exported: ${exportedLogs.logs.length} formatted entries`);
                        }
                    } catch (exportError) {
                        console.warn(`  ⚠️  Log export failed: ${exportError.message}`);
                        exportedLogs = null;
                    }

                    // Store complete log data in MongoDB
                    try {
                        const logDocument = {
                            execution_id:      workflowInfo.executionId,
                            session_id:        sessionId,
                            workflow_id:       workflowId,
                            workflow_name:     workflowInfo.workflowName,
                            workflow_type:     workflowInfo.workflowType,
                            workflow_version:  automaWorkflow.version || 'unknown',
                            link_id:           workflowInfo.linkId,
                            account_username:  workflowInfo.username,
                            account_id:        workflowInfo.accountId,
                            log_id:            logData.logId,
                            log_item:          logData.logItem,
                            history:           logData.history,
                            ctx_data:          logData.ctxData,
                            logs_data:         logData.logsData,
                            exported_json:     exportedLogs,
                            log_metadata:      logData.logMetadata,
                            log_source:        logData.source,
                            log_count:         logData.history.length,
                            has_errors:        logAnalysis.has_errors,
                            error_count:       logAnalysis.error_count,
                            success_count:     logAnalysis.success_count,
                            created_at:        new Date()
                        };

                        await this.mongoDBService.storeAutomaLogs(logDocument);
                        console.log(`✓ Automa logs stored in MongoDB`);

                    } catch (storageError) {
                        console.error(`❌ Failed to store logs in MongoDB: ${storageError.message}`);
                    }

                    // Extract screenshots from logs
                    console.log('\n📸 Extracting screenshots from workflow logs...');
                    const screenshotOptions = {
                        accountId: workflowInfo.accountId,
                        username: workflowInfo.username,
                        profileId: workflowInfo.profileId,
                        workflowType: workflowInfo.workflowType,
                        workflowName: workflowInfo.workflowName,
                        linkId: workflowInfo.linkId,
                        linkUrl: workflowInfo.linkUrl,
                        executionId: workflowInfo.executionId
                    };
                    
                    const screenshotResult = await this.screenshotCapture.processScreenshotsFromLog(
                        logData,
                        sessionId,
                        screenshotOptions
                    );
                    
                    console.log(`  ✓ Processed ${screenshotResult.count} screenshot(s) from logs`);

                } else {
                    console.log('  ⚠️  No IndexedDB logs found');
                }
            } catch (logError) {
                console.error(`⚠️  Failed to fetch Automa logs: ${logError.message}`);
            }

            // Step 9: Post-execution screenshot
            console.log('\nStep 9: Capturing post-execution screenshot...');
            const postScreenshots = await this.screenshotCapture.capture(browser, sessionId, workflowInfo, 'postExecution');
            console.log(`✓ Captured ${postScreenshots.length} screenshot(s)`);

            const allScreenshots = [...preScreenshots, ...postScreenshots];

            // Step 10: Stop video recording
            console.log('\nStep 10: Stopping video recording...');
            let recordingResult = await this.videoRecorder.stopRecording(recordingInfo);

            if (recordingResult && recordingResult.success) {
                recordingResult = await this.videoRecorder.uploadToGridFS(
                    recordingResult,
                    workflowInfo,
                    this.mongoDBService
                );
            }

            const executionTime = new Date() - executionStartTime;

            // Determine final success based on logs (if available)
            const finalSuccess = logAnalysis ? !logAnalysis.failed : true;

            // Step 11: Create video metadata
            if (recordingResult && recordingResult.gridfs_file_id) {
                console.log('\nStep 11: Creating video recording metadata...');
                await this.createVideoMetadata(recordingResult, workflowInfo, executionTime);
            }

            // Step 12: Update workflow_metadata
            console.log('\nStep 12: Updating workflow_metadata...');
            await this.markAsExecuted(
                linkData.links_id,
                new ObjectId(workflowMetadata.metadata_id),
                finalSuccess,
                recordingResult,
                logAnalysis ? (logAnalysis.failed ? 'Workflow execution had errors' : null) : null,
                executionStartTime,
                {
                    automa_logs_captured:  logData !== null,
                    automa_log_count:      logData ? logData.history.length : 0,
                    automa_error_count:    logAnalysis?.error_count || 0,
                    automa_success_count:  logAnalysis?.success_count || 0,
                    automa_log_status:     logData?.logMetadata?.status || 'unknown',
                    automa_log_source:     logData?.source || 'none'
                }
            );

            if (executionSessionId) {
                await this.updateExecutionSession(
                    executionSessionId,
                    finalSuccess,
                    executionTime,
                    allScreenshots,
                    recordingResult,
                    {
                        automa_logs_captured: logData !== null,
                        automa_log_count:     logData ? logData.history.length : 0,
                        automa_workflow_id:   workflowId,
                        automa_log_id:        logData?.logId || null,
                        automa_log_status:    logData?.logMetadata?.status || 'unknown',
                        automa_log_source:    logData?.source || 'none'
                    }
                );
            }

            console.log('\n' + '✓'.repeat(40));
            console.log(`WORKFLOW EXECUTION COMPLETED (${Math.round(executionTime / 1000)}s)`);
            console.log(`Status: ${finalSuccess ? '✅ SUCCESS' : '⚠️  WITH ERRORS'}`);
            if (logData) {
                console.log(`Logs: ${logData.history.length} entries captured from ${logData.source}`);
                if (logData.screenshots?.length) {
                    console.log(`Screenshots: ${logData.screenshots.length} extracted from logs`);
                }
            }
            console.log('✓'.repeat(40) + '\n');

            return {
                success:               finalSuccess,
                linkId:                workflowInfo.linkId,
                sessionId,
                executionTime,
                automaLogsAvailable:   logData !== null,
                automaLogStatus:       logData?.logMetadata?.status || 'unknown',
                automaLogCount:        logData ? logData.history.length : 0
            };

        } catch (error) {
            console.error('\n❌ WORKFLOW EXECUTION FAILED:', error.message);
            console.error(error.stack);

            const executionTime = new Date() - executionStartTime;

            let recordingResult = null;
            if (recordingInfo) {
                recordingResult = await this.videoRecorder.stopRecording(recordingInfo);
                if (recordingResult) {
                    recordingResult = await this.videoRecorder.uploadToGridFS(
                        recordingResult,
                        workflowInfo,
                        this.mongoDBService
                    );
                }
            }

            await this.markAsExecuted(
                linkData.links_id,
                new ObjectId(workflowMetadata.metadata_id),
                false,
                recordingResult,
                error.message,
                executionStartTime
            );

            if (executionSessionId) {
                await this.updateExecutionSession(
                    executionSessionId,
                    false,
                    executionTime,
                    [],
                    recordingResult
                );
            }

            return { success: false, linkId: workflowInfo.linkId, error: error.message };

        } finally {
            console.log('\nCleaning up session resources...');
            if (sessionId) {
                await this.sessionManager.stopSession(sessionId);
            }
            console.log('✓ Cleanup completed\n');
        }
    }

    /**
     * Create execution session record
     */
    async createExecutionSession(workflowInfo, sessionId, startTime) {
        const sessionRecord = {
            session_id:              sessionId,
            session_type:            'local_chrome_with_recording',
            postgres_account_id:     workflowInfo.accountId,
            account_username:        workflowInfo.username,
            profile_id:              workflowInfo.profileId,
            profile_path:            workflowInfo.profilePath,
            session_status:          'active',
            is_active:               true,
            workflows_executed:      0,
            successful_workflows:    0,
            failed_workflows:        0,
            created_at:              startTime,
            started_at:              startTime,
            ended_at:                null,
            execution_day:           new Date().toLocaleDateString('en-US', { weekday: 'long' }),
            execution_time:          new Date().toLocaleTimeString('en-US'),
            dag_run_id:              this.config.dagRunId,
            execution_date:          this.config.executionDate,
            screenshots:             [],
            screenshot_file_ids:     [],
            video_recording_id:      null,
            video_recording_status:  'pending',
            workflow_type:           workflowInfo.workflowType,
            session_purpose:         'local_workflow_execution_with_video',
            session_metadata: {
                link_id:          workflowInfo.linkId,
                workflow_name:    workflowInfo.workflowName,
                execution_id:     workflowInfo.executionId,
                chrome_mode:      'local_persistent',
                recording_method: 'ffmpeg_x11grab'
            }
        };

        return await this.mongoDBService.createExecutionSession(sessionRecord);
    }

    /**
     * Update execution session
     */
    async updateExecutionSession(sessionId, success, executionTime, screenshotIds, recordingResult, additionalData = {}) {
        const updateData = {
            session_status:               success ? 'completed' : 'failed',
            is_active:                    false,
            ended_at:                     new Date(),
            total_execution_time_seconds: Math.round(executionTime / 1000),
            updated_at:                   new Date(),
            workflows_executed:           1,
            successful_workflows:         success ? 1 : 0,
            failed_workflows:             success ? 0 : 1,
            ...additionalData
        };

        if (screenshotIds && screenshotIds.length > 0) {
            updateData.screenshots        = screenshotIds.map(id => id.toString());
            updateData.screenshot_file_ids = screenshotIds;
        }

        if (recordingResult && recordingResult.gridfs_file_id) {
            updateData.video_recording_id      = recordingResult.gridfs_file_id;
            updateData.video_recording_status  = 'completed';
            updateData.video_duration_seconds  = recordingResult.duration;
        } else if (!success) {
            updateData.video_recording_status = 'failed';
        }

        await this.mongoDBService.updateExecutionSession(sessionId, updateData);
    }

    /**
     * Create video metadata
     */
    async createVideoMetadata(recordingResult, workflowInfo, executionTime) {
        const videoRecord = {
            gridfs_file_id:             recordingResult.gridfs_file_id,
            session_id:                 workflowInfo.executionId,
            session_type:               'local_chrome',
            postgres_account_id:        workflowInfo.accountId,
            account_username:           workflowInfo.username,
            profile_path:               workflowInfo.profilePath,
            filename:                   recordingResult.filename,
            content_type:               'video/mp4',
            duration_seconds:           recordingResult.duration,
            recording_status:           'completed',
            recording_method:           'ffmpeg_x11grab',
            workflow_type:              workflowInfo.workflowType,
            workflow_name:              workflowInfo.workflowName,
            link_id:                    workflowInfo.linkId,
            execution_id:               workflowInfo.executionId,
            execution_duration_seconds: Math.round(executionTime / 1000),
            created_at:                 new Date(),
            recorded_at:                new Date(),
            updated_at:                 new Date()
        };

        await this.mongoDBService.createVideoMetadata(videoRecord);
    }

    /**
     * Mark workflow as executed
     */
    async markAsExecuted(linkId, metadataId, success, recordingResult, errorMessage, startTime, logMetadata = {}) {
        try {
            const executionTime = startTime ? new Date() - startTime : 0;

            await this.pgService.markLinkExecuted(linkId, success);

            const mongoUpdate = {
                executed:       true,
                success:        success,
                executed_at:    new Date().toISOString(),
                status:         success ? 'completed' : 'failed',
                updated_at:     new Date().toISOString(),
                execution_time_ms: executionTime,
                execution_mode: 'local_chrome_with_video',
                ...logMetadata
            };

            if (errorMessage) {
                mongoUpdate.error_message       = errorMessage;
                mongoUpdate.last_error_message  = errorMessage;
                mongoUpdate.last_error_timestamp = new Date().toISOString();
            }

            if (recordingResult && recordingResult.gridfs_file_id) {
                mongoUpdate.video_recording = {
                    gridfs_file_id: recordingResult.gridfs_file_id,
                    filename:       recordingResult.filename,
                    duration_seconds: recordingResult.duration,
                    recorded_at:    new Date().toISOString()
                };
            }

            await this.mongoDBService.markWorkflowExecuted(metadataId, mongoUpdate);

        } catch (error) {
            console.error(`  ⚠ Error marking as executed: ${error.message}`);
        }
    }

    /**
     * Sleep helper
     */
    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}
