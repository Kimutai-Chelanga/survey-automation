// Smart Workflow Orchestrator with Daily Settings Integration
// Processes workflows based on daily configuration, time limits, and link matching

import HyperbrowserService from './hyperbrowser.js';
import AccountProfileManager from './account-profile-manager.js';
import { VideoRecordingManager } from './video-recording-manager.js';
import { MongoClient, ObjectId } from 'mongodb';
import pg from 'pg';
const { Pool } = pg;

class SmartWorkflowOrchestrator {
    constructor() {
        // MongoDB Configuration
        this.mongoUri = process.env.MONGODB_URI || 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin';
        this.dbName = process.env.MONGODB_DB_NAME || 'messages_db';
        
        // Hyperbrowser Configuration
        this.apiKey = process.env.HYPERBROWSER_API_KEY;
        this.extensionId = process.env.AUTOMA_EXTENSION_ID || 'infppggnoaenmfagbfknfkancpbljcca';
        
        // PostgreSQL Configuration
        this.pgPool = new Pool({
            host: process.env.POSTGRES_HOST || 'postgres',
            port: parseInt(process.env.POSTGRES_PORT || '5432'),
            database: process.env.POSTGRES_DB || 'messages',
            user: process.env.POSTGRES_USER || 'airflow',
            password: process.env.POSTGRES_PASSWORD || 'airflow'
        });
        
        // Airflow Context
        this.dagRunId = process.env.AIRFLOW_CTX_DAG_RUN_ID || 'manual_run';
        this.executionDate = process.env.AIRFLOW_CTX_EXECUTION_DATE || new Date().toISOString();
        this.taskId = process.env.AIRFLOW_CTX_TASK_ID || 'workflow_executor';
        
        // Services
        this.profileManager = null;
        this.hyperbrowserService = null;
        this.mongoClient = null;
        this.mongodb = null;
        
        // Daily Settings
        this.dailySettings = null;
        this.workflowType = null;
        this.gapBetweenWorkflows = null;
        this.timeLimit = null;
        this.maxWorkflows = null;
        
        // Execution tracking
        this.executionStartTime = null;
        this.executionEndTime = null;
        
        // Statistics
        this.stats = {
            linksFound: 0,
            workflowsMatched: 0,
            workflowsExecuted: 0,
            successfulExecutions: 0,
            failedExecutions: 0,
            timeLimitReached: false,
            maxWorkflowsReached: false,
            startTime: new Date()
        };
    }

    async initialize() {
        console.log('\n' + '='.repeat(80));
        console.log('SMART WORKFLOW ORCHESTRATOR INITIALIZING');
        console.log('='.repeat(80));
        console.log(`DAG Run ID: ${this.dagRunId}`);
        console.log(`Execution Date: ${this.executionDate}`);
        console.log(`Task ID: ${this.taskId}`);
        console.log('='.repeat(80) + '\n');
        
        // MongoDB connection
        this.mongoClient = new MongoClient(this.mongoUri);
        await this.mongoClient.connect();
        this.mongodb = this.mongoClient.db(this.dbName);
        console.log('✓ MongoDB connected');

        // Profile Manager
        this.profileManager = new AccountProfileManager(this.mongoUri, this.dbName);
        await this.profileManager.connect();
        console.log('✓ Profile Manager initialized');

        // Hyperbrowser Service
        this.hyperbrowserService = new HyperbrowserService({
            hyperbrowser: {
                apiKey: this.apiKey,
                baseUrl: 'https://api.hyperbrowser.ai',
                sessionConfig: {
                    screen: { width: 1920, height: 1080 },
                    use_stealth: true,
                    browser_type: 'chrome',
                    enableWebRecording: true,
                    enableVideoWebRecording: true
                }
            }
        });
        console.log('✓ Hyperbrowser Service initialized');

        // Test PostgreSQL connection
        const pgClient = await this.pgPool.connect();
        console.log('✓ PostgreSQL connected');
        pgClient.release();
        
        // Load daily settings
        await this.loadDailySettings();
        
        console.log('\n✓ All systems initialized successfully\n');
    }

    /**
     * Load daily settings from MongoDB based on current day
     */
    async loadDailySettings() {
        console.log('\n' + '='.repeat(80));
        console.log('LOADING DAILY SETTINGS');
        console.log('='.repeat(80) + '\n');
        
        try {
            // Get current day name
            const days = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
            const today = days[new Date().getDay()];
            
            console.log(`Current day: ${today}`);
            
            // Fetch settings from MongoDB
            const settingsDoc = await this.mongodb.collection('weekly_workflow_settings').findOne({
                category: 'system'
            });

            if (!settingsDoc) {
                throw new Error('No system settings found in database');
            }

            const todaySettings = settingsDoc.settings?.weekly_workflow_settings?.[today];
            
            if (!todaySettings) {
                throw new Error(`No settings found for ${today}`);
            }

            // Extract key settings
            this.dailySettings = todaySettings;
            this.workflowType = todaySettings.workflow_type || 
                               (todaySettings.content_types && todaySettings.content_types[0]) || 
                               'messages';
            this.gapBetweenWorkflows = todaySettings.gap_between_workflows || 
                                      todaySettings.gap_between_workflows_seconds || 
                                      300; // Default 5 minutes
            this.timeLimit = todaySettings.time_limit || 2; // Default 2 hours
            this.maxWorkflows = todaySettings.workflows_to_process || 
                               todaySettings.content_to_process || 
                               15; // Default 15 workflows

            console.log('✓ Daily Settings Loaded:');
            console.log(`  Day: ${today}`);
            console.log(`  Workflow Type: ${this.workflowType}`);
            console.log(`  Gap Between Workflows: ${this.gapBetweenWorkflows}s`);
            console.log(`  Time Limit: ${this.timeLimit} hours`);
            console.log(`  Max Workflows: ${this.maxWorkflows}`);
            console.log(`  Enabled: ${todaySettings.enabled !== false}`);

            if (todaySettings.enabled === false) {
                console.log('\n⚠ WARNING: Today is marked as DISABLED in settings');
                console.log('  Orchestrator will proceed but may want to exit early\n');
            }

        } catch (error) {
            console.error('❌ Error loading daily settings:', error.message);
            console.log('⚠ Using default settings as fallback');
            
            // Fallback defaults
            this.workflowType = 'messages';
            this.gapBetweenWorkflows = 300;
            this.timeLimit = 2;
            this.maxWorkflows = 15;
        }
    }

    /**
     * Check if we should stop execution based on time limit or max workflows
     */
    shouldStopExecution() {
        // Check time limit
        if (this.executionStartTime) {
            const elapsedHours = (new Date() - this.executionStartTime) / (1000 * 60 * 60);
            if (elapsedHours >= this.timeLimit) {
                this.stats.timeLimitReached = true;
                return { stop: true, reason: 'time_limit', elapsedHours };
            }
        }

        // Check max workflows
        if (this.stats.workflowsExecuted >= this.maxWorkflows) {
            this.stats.maxWorkflowsReached = true;
            return { stop: true, reason: 'max_workflows', workflowsExecuted: this.stats.workflowsExecuted };
        }

        return { stop: false };
    }

    /**
     * Fetch eligible links from PostgreSQL
     */
    /**
 * Fetch eligible links from PostgreSQL that have been assigned workflows
 * but not yet executed
 */
    async fetchEligibleLinks() {
        console.log('\n' + '='.repeat(80));
        console.log('FETCHING ELIGIBLE LINKS FROM POSTGRESQL & MONGODB');
        console.log('='.repeat(80) + '\n');
        
        // Query for links that:
        // 1. Have been assigned workflows (used = TRUE, processed_by_workflow = TRUE)
        // 2. Have NOT been executed yet (executed = FALSE or NULL)
        // 3. Are within the time limit (within_limit = TRUE)
        // 4. Have been filtered (filtered = TRUE)
        const linksQuery = `
            SELECT 
                l.links_id,
                l.link,
                l.tweet_id,
                l.tweeted_date,
                l.tweeted_time,
                l.workflow_type,
                l.within_limit,
                l.account_id,
                l.used,
                l.processed_by_workflow,
                l.executed,
                l.workflow_status
            FROM links l
            WHERE l.within_limit = TRUE
                AND l.filtered = TRUE
                AND l.used = TRUE
                AND l.processed_by_workflow = TRUE
                AND COALESCE(l.executed, FALSE) = FALSE
                AND l.workflow_status = 'completed'
            ORDER BY l.tweeted_date DESC, l.links_id ASC
        `;
        
        try {
            const linksResult = await this.pgPool.query(linksQuery);
            console.log(`✓ Found ${linksResult.rows.length} eligible links from PostgreSQL`);
            
            if (linksResult.rows.length === 0) {
                this.stats.linksFound = 0;
                console.log('\n⚠️  No links found matching criteria:');
                console.log('   - within_limit = TRUE');
                console.log('   - filtered = TRUE');
                console.log('   - used = TRUE (assigned to workflows)');
                console.log('   - processed_by_workflow = TRUE');
                console.log('   - executed = FALSE or NULL');
                console.log('   - workflow_status = completed');
                console.log('\n💡 Tip: Check if the Python DAG has run and assigned workflows to links\n');
                return [];
            }
            
            // Log sample of found links
            console.log('\nSample of found links:');
            linksResult.rows.slice(0, 3).forEach((row, idx) => {
                console.log(`  ${idx + 1}. Link ${row.links_id}:`);
                console.log(`     URL: ${row.link.substring(0, 60)}...`);
                console.log(`     Account: ${row.account_id}, Status: ${row.workflow_status}`);
                console.log(`     Used: ${row.used}, Processed: ${row.processed_by_workflow}, Executed: ${row.executed}`);
            });
            if (linksResult.rows.length > 3) {
                console.log(`  ... and ${linksResult.rows.length - 3} more links`);
            }
            
            // Get link IDs for MongoDB query
            const linkIds = linksResult.rows.map(row => row.links_id);
            
            // Now fetch workflow assignments from MongoDB
            const workflowAssignments = await this.getWorkflowAssignmentsFromMongo(linkIds);
            
            console.log(`✓ Found ${workflowAssignments.length} workflow assignments from MongoDB`);
            
            if (workflowAssignments.length === 0) {
                console.log('\n⚠️  No workflow assignments found in MongoDB for these links');
                console.log('💡 This might indicate a sync issue between PostgreSQL and MongoDB\n');
                this.stats.linksFound = 0;
                return [];
            }
            
            // Combine PostgreSQL links with MongoDB workflow assignments
            const eligibleCombinations = [];
            
            for (const link of linksResult.rows) {
                const assignments = workflowAssignments.filter(
                    a => a.postgres_content_id === link.links_id
                );
                
                if (assignments.length === 0) {
                    console.warn(`⚠️  Link ${link.links_id} has no workflow assignments in MongoDB`);
                    continue;
                }
                
                // Create one row per workflow assignment (which includes account)
                for (const assignment of assignments) {
                    eligibleCombinations.push({
                        links_id: link.links_id,
                        link: link.link,
                        tweet_id: link.tweet_id,
                        tweeted_date: link.tweeted_date,
                        tweeted_time: link.tweeted_time,
                        workflow_type: assignment.workflow_type,
                        workflow_name: assignment.workflow_name,
                        automa_workflow_id: assignment.automa_workflow_id,
                        account_id: assignment.account_id,
                        metadata_id: assignment.metadata_id,
                        has_link: assignment.has_link,
                        status: assignment.status,
                        postgres_status: link.workflow_status
                    });
                }
            }
            
            this.stats.linksFound = eligibleCombinations.length;
            
            const uniqueLinks = new Set(eligibleCombinations.map(r => r.links_id)).size;
            const uniqueAccounts = new Set(eligibleCombinations.map(r => r.account_id)).size;
            const uniqueWorkflows = new Set(eligibleCombinations.map(r => r.automa_workflow_id)).size;
            
            console.log(`\n📊 Link-Workflow-Account Combinations Summary:`);
            console.log(`   Total combinations: ${eligibleCombinations.length}`);
            console.log(`   Unique links: ${uniqueLinks}`);
            console.log(`   Unique accounts: ${uniqueAccounts}`);
            console.log(`   Unique workflows: ${uniqueWorkflows}`);
            console.log(`   Avg assignments per link: ${(eligibleCombinations.length / uniqueLinks).toFixed(1)}`);
            
            if (eligibleCombinations.length > 0) {
                console.log('\n📋 Sample combinations:');
                eligibleCombinations.slice(0, 5).forEach((combo, idx) => {
                    console.log(`  ${idx + 1}. Link ${combo.links_id} → Workflow ${combo.workflow_name} → Account ${combo.account_id}`);
                    console.log(`     URL: ${combo.link.substring(0, 60)}...`);
                    console.log(`     Type: ${combo.workflow_type}, Status: ${combo.status}`);
                });
                if (eligibleCombinations.length > 5) {
                    console.log(`  ... and ${eligibleCombinations.length - 5} more combinations`);
                }
            }
            
            return eligibleCombinations;
            
        } catch (error) {
            console.error('❌ Error fetching eligible links:', error.message);
            console.error('Stack trace:', error.stack);
            throw error;
        }
    }

    async getWorkflowAssignmentsFromMongo(linkIds) {
        try {
            const db = this.mongoClient.db('messages_db');
            const workflowMetadata = db.collection('workflow_metadata');
            
            const assignments = await workflowMetadata.find({
                postgres_content_id: { $in: linkIds },
                has_link: true,
                has_content: true,
                status: 'ready_to_execute',
                executed: false
            }).toArray();
            
            return assignments.map(doc => ({
                metadata_id: doc._id.toString(),
                postgres_content_id: doc.postgres_content_id,
                automa_workflow_id: doc.automa_workflow_id.toString(),
                workflow_type: doc.workflow_type,
                workflow_name: doc.workflow_name,
                account_id: doc.account_id,
                has_link: doc.has_link,
                status: doc.status,
                link_url: doc.link_url
            }));
            
        } catch (error) {
            console.error('❌ Error fetching workflow assignments from MongoDB:', error.message);
            throw error;
        }
    }


    /**
     * Find matching workflows in MongoDB for a given link URL
     */
   
    /**
     * Fetch the actual Automa workflow from automa_workflows collection
     */
    async fetchAutomaWorkflow(automaWorkflowId) {
        try {
            const workflow = await this.mongodb.collection('automa_workflows').findOne({
                _id: new ObjectId(automaWorkflowId)
            });

            if (!workflow) {
                throw new Error(`Automa workflow not found: ${automaWorkflowId}`);
            }

            return workflow;
        } catch (error) {
            console.error(`  ❌ Error fetching Automa workflow: ${error.message}`);
            throw error;
        }
    }

    /**
     * Execute a single workflow with its own dedicated session and recording
     */
    async createExecutionSessionRecord(workflowInfo, sessionId, executionStartTime) {
        try {
            const sessionRecord = {
                session_id: sessionId,
                postgres_account_id: workflowInfo.accountId,
                account_username: workflowInfo.username,
                profile_id: workflowInfo.profileId,
                
                // Session status
                session_status: 'active',
                is_active: true,
                
                // Execution tracking
                workflows_executed: 0,
                successful_workflows: 0,
                failed_workflows: 0,
                
                // Timestamps
                created_at: executionStartTime,
                started_at: executionStartTime,
                ended_at: null,
                
                // Execution context
                execution_day: new Date().toLocaleDateString('en-US', { weekday: 'long' }),
                execution_time: new Date().toLocaleTimeString('en-US'),
                dag_run_id: this.dagRunId,
                execution_date: this.executionDate,
                
                // Media tracking
                screenshots: [],
                screenshot_file_ids: [],
                video_recording_url: null,
                web_recording_url: null,
                video_recording_status: 'pending',
                web_recording_status: 'pending',
                
                // Metadata
                workflow_type: workflowInfo.workflowType,
                session_purpose: 'workflow_execution',
                session_metadata: {
                    link_id: workflowInfo.linkId,
                    workflow_name: workflowInfo.workflowName,
                    execution_id: workflowInfo.executionId
                }
            };

            const result = await this.mongodb.collection('execution_sessions').insertOne(sessionRecord);
            console.log(`✓ Created execution session record: ${result.insertedId}`);
            return result.insertedId.toString();

        } catch (error) {
            console.error('Failed to create execution session record:', error.message);
            return null;
        }
    }
    async captureScreenshots(browser, sessionId, workflowInfo, category = 'execution') {
        const screenshotIds = [];
        
        try {
            const pages = await browser.pages();
            if (!pages || pages.length === 0) {
                console.log('No pages available for screenshots');
                return screenshotIds;
            }

            const page = pages[0];
            const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
            const filename = `${sessionId}_${category}_${timestamp}.png`;

            // Capture screenshot
            const screenshotBuffer = await page.screenshot({
                fullPage: true,
                type: 'png'
            });

            // Store in GridFS
            const GridFSBucket = (await import('mongodb')).GridFSBucket;
            const bucket = new GridFSBucket(this.mongodb, {
                bucketName: 'screenshots'
            });

            const uploadStream = bucket.openUploadStream(filename, {
                contentType: 'image/png',
                metadata: {
                    session_id: sessionId,
                    postgres_account_id: workflowInfo.accountId,
                    account_username: workflowInfo.username,
                    category: category,
                    workflow_type: workflowInfo.workflowType,
                    link_id: workflowInfo.linkId,
                    captured_at: new Date()
                }
            });

            await new Promise((resolve, reject) => {
                uploadStream.on('finish', resolve);
                uploadStream.on('error', reject);
                uploadStream.end(screenshotBuffer);
            });

            const gridfsFileId = uploadStream.id;
            console.log(`✓ Screenshot uploaded to GridFS: ${gridfsFileId}`);

            // Create metadata record
            const screenshotMetadata = {
                gridfs_file_id: gridfsFileId,
                session_id: sessionId,
                postgres_account_id: workflowInfo.accountId,
                account_username: workflowInfo.username,
                profile_id: workflowInfo.profileId,
                
                // File info
                filename: filename,
                content_type: 'image/png',
                size: screenshotBuffer.length,
                
                // Context
                category: category,
                workflow_type: workflowInfo.workflowType,
                workflow_name: workflowInfo.workflowName,
                link_id: workflowInfo.linkId,
                execution_id: workflowInfo.executionId,
                
                // Timestamps
                created_at: new Date(),
                captured_at: new Date()
            };

            const result = await this.mongodb.collection('screenshot_metadata').insertOne(screenshotMetadata);
            screenshotIds.push(gridfsFileId);
            
            console.log(`✓ Screenshot metadata created: ${result.insertedId}`);
            return screenshotIds;

        } catch (error) {
            console.error(`Failed to capture screenshot: ${error.message}`);
            return screenshotIds;
        }
    }

    /**
     * Create video recording record
     */
    async createVideoRecordingRecord(sessionId, workflowInfo, recordingStatus, executionTime, screenshotIds = []) {
        try {
            if (!recordingStatus || !recordingStatus.success) {
                console.log('No successful recording to record');
                return null;
            }

            const videoRecord = {
                session_id: sessionId,
                postgres_account_id: workflowInfo.accountId,
                account_username: workflowInfo.username,
                profile_id: workflowInfo.profileId,
                
                // Recording URLs
                video_recording_url: recordingStatus.videoRecordingUrl,
                web_recording_url: recordingStatus.webRecordingUrl,
                
                // Status
                video_recording_status: 'completed',
                web_recording_status: 'completed',
                
                // Execution info
                workflows_executed: 1,
                execution_duration_seconds: Math.round(executionTime / 1000),
                
                // Metadata
                workflow_type: workflowInfo.workflowType,
                workflow_name: workflowInfo.workflowName,
                link_id: workflowInfo.linkId,
                execution_id: workflowInfo.executionId,
                
                // Screenshots
                screenshot_file_ids: screenshotIds,
                
                // Timestamps
                created_at: new Date(),
                updated_at: new Date()
            };

            const result = await this.mongodb.collection('video_recordings').insertOne(videoRecord);
            console.log(`✓ Created video recording record: ${result.insertedId}`);
            return result.insertedId.toString();

        } catch (error) {
            console.error('Failed to create video recording record:', error.message);
            return null;
        }
    }
    async updateExecutionSessionRecord(mongoSessionId, success, recordingStatus, executionTime, screenshotIds = []) {
        try {
            const updateData = {
                session_status: success ? 'completed' : 'failed',
                is_active: false,
                ended_at: new Date(),
                total_execution_time_seconds: Math.round(executionTime / 1000),
                updated_at: new Date()
            };

            // Update workflow counts
            if (success) {
                updateData.workflows_executed = 1;
                updateData.successful_workflows = 1;
                updateData.failed_workflows = 0;
            } else {
                updateData.workflows_executed = 1;
                updateData.successful_workflows = 0;
                updateData.failed_workflows = 1;
            }

            // Add screenshots
            if (screenshotIds && screenshotIds.length > 0) {
                updateData.screenshots = screenshotIds.map(id => id.toString());
                updateData.screenshot_file_ids = screenshotIds;
            }

            // Add recording URLs if available
            if (recordingStatus && recordingStatus.success) {
                updateData.video_recording_url = recordingStatus.videoRecordingUrl;
                updateData.web_recording_url = recordingStatus.webRecordingUrl;
                updateData.video_recording_status = 'completed';
                updateData.web_recording_status = 'completed';
            } else if (!success) {
                updateData.video_recording_status = 'failed';
                updateData.web_recording_status = 'failed';
                updateData.recording_error = recordingStatus?.error || 'Unknown error';
            }

            await this.mongodb.collection('execution_sessions').updateOne(
                { _id: new ObjectId(mongoSessionId) },
                { $set: updateData }
            );

            console.log(`✓ Updated execution session record: ${mongoSessionId}`);
            return true;

        } catch (error) {
            console.error('Failed to update execution session:', error.message);
            return false;
        }
    }
    async executeWorkflow(linkData, workflowMetadata, workflowNumber, totalWorkflows) {
        let sessionId = null;
        let mongoSessionId = null;
        let executionSessionId = null;  // NEW: For dashboard tracking
        let browser = null;
        let recordingManager = null;

        const workflowInfo = {
            linkId: linkData.links_id,
            linkUrl: linkData.link,
            accountId: workflowMetadata.postgres_account_id,
            username: workflowMetadata.username || linkData.username,
            profileId: workflowMetadata.profile_id || linkData.profile_id,
            workflowName: workflowMetadata.workflow_name,
            workflowType: workflowMetadata.workflow_type,
            metadataId: workflowMetadata._id,
            executionId: `${this.dagRunId}_link${linkData.links_id}_wf${workflowNumber}`
        };

        console.log('\n' + '█'.repeat(80));
        console.log(`EXECUTING WORKFLOW ${workflowNumber}/${totalWorkflows} (Type: ${this.workflowType})`);
        console.log('█'.repeat(80));
        console.log(`Link ID: ${workflowInfo.linkId}`);
        console.log(`URL: ${workflowInfo.linkUrl}`);
        console.log(`Account: ${workflowInfo.username} (ID: ${workflowInfo.accountId})`);
        console.log(`Workflow: ${workflowInfo.workflowName}`);
        console.log(`Type: ${workflowInfo.workflowType}`);
        console.log(`Profile: ${workflowInfo.profileId}`);
        console.log(`Execution ID: ${workflowInfo.executionId}`);
        console.log('█'.repeat(80) + '\n');

        const executionStartTime = new Date();

        try {
            // Step 1: Get account profile
            console.log('Step 1: Loading account profile...');
            const accountProfile = await this.profileManager.getAccountProfile(workflowInfo.accountId);
            console.log(`✓ Account profile loaded`);
            console.log(`  Profile ID: ${accountProfile.profileId}`);
            console.log(`  Extension ID: ${accountProfile.extensionId || 'Not configured'}`);

            // Step 2: Fetch the actual Automa workflow
            console.log('\nStep 2: Loading Automa workflow from MongoDB...');
            const automaWorkflow = await this.fetchAutomaWorkflow(workflowMetadata.automa_workflow_id);
            console.log(`✓ Automa workflow loaded: ${automaWorkflow.name}`);

            // Step 3: Create dedicated Hyperbrowser session
            console.log('\nStep 3: Creating dedicated Hyperbrowser session...');
            const sessionResult = await this.hyperbrowserService.createSession({
                profileId: accountProfile.profileId,
                extensionId: this.extensionId,
                enableWebRecording: true,
                enableVideoRecording: true,
                persistChanges: true
            });

            sessionId = sessionResult.sessionId;
            console.log(`✓ Session created: ${sessionId}`);

            // Step 4: Initialize recording manager
            console.log('\nStep 4: Initializing recording manager...');
            recordingManager = new VideoRecordingManager(this.apiKey);
            console.log('✓ Recording manager initialized');

            // Step 5: Connect Puppeteer to browser
            console.log('\nStep 5: Connecting Puppeteer to browser...');
            browser = await this.hyperbrowserService.connectToBrowser(sessionResult);
            console.log('✓ Puppeteer connected to browser');

            // Step 6: Create session record in MongoDB (AccountProfileManager)
            console.log('\nStep 6: Creating session record in MongoDB...');
            mongoSessionId = await this.profileManager.createAccountSession({
                accountId: workflowInfo.accountId,
                profileId: accountProfile.profileId,
                extensionId: accountProfile.extensionId,
                dagRunId: this.dagRunId,
                executionDate: this.executionDate,
                username: workflowInfo.username,
                sessionPurpose: `${workflowInfo.workflowType}_execution`
            });
            console.log(`✓ Session record created: ${mongoSessionId}`);

            // NEW Step 6.5: Create execution session record for dashboard
            console.log('\nStep 6.5: Creating execution session for dashboard...');
            executionSessionId = await this.createExecutionSessionRecord(
                workflowInfo, 
                sessionId, 
                executionStartTime
            );

            // Step 7: Start recording monitor
            console.log('\nStep 7: Starting recording monitor...');
            recordingManager.startRecordingMonitor(sessionId, {
                pollInterval: 10000,
                maxWaitTime: 600000,
                onProgress: (progress) => {
                    console.log(`  Recording status: ${progress.status} (${Math.round(progress.elapsedTime/1000)}s elapsed)`);
                },
                onComplete: (recording) => {
                    console.log('  ✓ Recording completed successfully!');
                },
                onError: (error) => {
                    console.error(`  ⚠ Recording error: ${error}`);
                }
            });
            console.log('✓ Recording monitor started');

            // NEW Step 7.5: Capture pre-execution screenshot
            console.log('\nStep 7.5: Capturing pre-execution screenshot...');
            const preScreenshots = await this.captureScreenshots(browser, sessionId, workflowInfo, 'preExecution');
            console.log(`✓ Captured ${preScreenshots.length} pre-execution screenshot(s)`);

            // Step 8: Execute the workflow
            console.log('\nStep 8: Executing Automa workflow...');
            await this.executeAutomaWorkflow(browser, automaWorkflow, workflowInfo.linkUrl, workflowInfo);
            console.log('✓ Workflow execution initiated');

            // Step 9: Wait for workflow completion
            const waitTime = 120000; // 2 minutes
            console.log(`\nStep 9: Waiting ${waitTime/1000}s for workflow completion...`);
            await this.sleep(waitTime);
            console.log('✓ Wait period completed');

            // NEW Step 9.5: Capture post-execution screenshot
            console.log('\nStep 9.5: Capturing post-execution screenshot...');
            const postScreenshots = await this.captureScreenshots(browser, sessionId, workflowInfo, 'postExecution');
            console.log(`✓ Captured ${postScreenshots.length} post-execution screenshot(s)`);
            
            const allScreenshots = [...preScreenshots, ...postScreenshots];

            // Step 10: Get final recording status
            console.log('\nStep 10: Retrieving recording URLs...');
            const recordingStatus = await recordingManager.getRecordingUrls(sessionId);
            
            if (recordingStatus.success) {
                console.log('✓ Recordings available');
                console.log(`  Video URL: ${recordingStatus.videoRecordingUrl}`);
                console.log(`  Web URL: ${recordingStatus.webRecordingUrl}`);
            } else {
                console.log('⚠ Recordings not yet ready');
            }

            const executionTime = new Date() - executionStartTime;

            // Step 11: Mark as executed in workflow_metadata
            console.log('\nStep 11: Updating workflow_metadata...');
            await this.markAsExecuted(
                linkData.links_id, 
                workflowMetadata._id, 
                true, 
                recordingStatus, 
                null, 
                executionStartTime
            );
            console.log('✓ workflow_metadata updated');

            // NEW Step 11.5: Update execution session for dashboard (with screenshots)
            if (executionSessionId) {
                console.log('\nStep 11.5: Updating execution session for dashboard...');
                await this.updateExecutionSessionRecord(
                    executionSessionId,
                    true,
                    recordingStatus,
                    executionTime,
                    allScreenshots
                );
                console.log('✓ Execution session updated');
            }

            // NEW Step 11.6: Create video recording record if successful (with screenshots)
            if (recordingStatus && recordingStatus.success) {
                console.log('\nStep 11.6: Creating video recording record...');
                await this.createVideoRecordingRecord(
                    sessionId,
                    workflowInfo,
                    recordingStatus,
                    executionTime,
                    allScreenshots
                );
                console.log('✓ Video recording record created');
            }

            // Step 12: Update session stats
            await this.profileManager.updateAccountSessionStats(mongoSessionId, {
                workflowCount: 1,
                successCount: 1,
                failedCount: 0
            });

            console.log('\n' + '✓'.repeat(40));
            console.log(`WORKFLOW EXECUTION COMPLETED (${Math.round(executionTime/1000)}s)`);
            console.log('✓'.repeat(40) + '\n');
            
            this.stats.workflowsExecuted++;
            this.stats.successfulExecutions++;

            return { success: true, linkId: workflowInfo.linkId, sessionId, executionTime };

        } catch (error) {
            console.error('\n❌ WORKFLOW EXECUTION FAILED:', error.message);

            const executionTime = new Date() - executionStartTime;

            await this.markAsExecuted(
                linkData.links_id, 
                workflowMetadata._id, 
                false, 
                null, 
                error.message, 
                executionStartTime
            );

            // Update execution session as failed
            if (executionSessionId) {
                await this.updateExecutionSessionRecord(
                    executionSessionId,
                    false,
                    null,
                    executionTime
                );
            }

            if (mongoSessionId) {
                await this.profileManager.updateAccountSessionStats(mongoSessionId, {
                    workflowCount: 1, successCount: 0, failedCount: 1
                });
            }

            this.stats.workflowsExecuted++;
            this.stats.failedExecutions++;

            return { success: false, linkId: workflowInfo.linkId, error: error.message };

        } finally {
            console.log('\nCleaning up session resources...');
            
            if (browser) {
                try {
                    await browser.disconnect();
                    console.log('  ✓ Browser disconnected');
                } catch (e) {}
            }

            if (sessionId) {
                try {
                    await this.hyperbrowserService.stopSession(sessionId);
                    console.log('  ✓ Session stopped');
                } catch (e) {}
            }

            if (mongoSessionId) {
                await this.profileManager.closeAccountSession(mongoSessionId);
            }

            if (recordingManager && sessionId) {
                recordingManager.stopRecordingMonitor(sessionId);
            }

            console.log('✓ Cleanup completed\n');
        }
    }

    /**
     * Execute Automa workflow in the browser
     */
    async executeAutomaWorkflow(browser, automaWorkflow, linkUrl, workflowInfo) {
        const pages = await browser.pages();
        let page = pages.length > 0 ? pages[0] : await browser.newPage();

        // Import the workflow
        const popupUrl = `chrome-extension://${this.extensionId}/popup.html`;
        await page.goto(popupUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
        await this.sleep(3000);

        const workflowId = await this.importWorkflowToExtension(page, automaWorkflow);
        
        // NEW: Execute directly via URL instead of clicking
        const executeUrl = `chrome-extension://${this.extensionId}/execute.html#/${workflowId}`;
        console.log(`Executing workflow via URL: ${executeUrl}`);
        await page.goto(executeUrl, { waitUntil: 'domcontentloaded', timeout: 30000 });
        
        await this.sleep(3000); // Wait for execution to start

        return { success: true, workflowId };
    }

    async importWorkflowToExtension(page, workflow) {
        const importResult = await page.evaluate((wf) => {
            return new Promise((resolve) => {
                try {
                    const generateId = () => {
                        const chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
                        let result = '';
                        for (let i = 0; i < 21; i++) {
                            result += chars.charAt(Math.floor(Math.random() * chars.length));
                        }
                        return result;
                    };

                    const newId = generateId();
                    const now = Date.now();

                    const workflowToSave = {
                        ...wf,
                        id: newId,
                        createdAt: now,
                        updatedAt: now,
                        folderId: null,
                        isDisabled: false
                    };

                    if (typeof workflowToSave.drawflow === 'string') {
                        workflowToSave.drawflow = JSON.parse(workflowToSave.drawflow);
                    }

                    chrome.storage.local.get('workflows', (storage) => {
                        let workflows = storage.workflows || {};
                        
                        if (Array.isArray(workflows)) {
                            const workflowsObj = {};
                            workflows.forEach(w => { workflowsObj[w.id] = w; });
                            workflows = workflowsObj;
                        }

                        workflows[newId] = workflowToSave;

                        chrome.storage.local.set({ workflows }, () => {
                            if (chrome.runtime.lastError) {
                                resolve({ success: false, error: chrome.runtime.lastError.message });
                            } else {
                                resolve({ success: true, workflowId: newId });
                            }
                        });
                    });

                } catch (error) {
                    resolve({ success: false, error: error.message });
                }
            });
        }, workflow);

        if (!importResult.success) {
            throw new Error(`Workflow import failed: ${importResult.error}`);
        }

        await page.reload({ waitUntil: 'domcontentloaded', timeout: 30000 });
        await this.sleep(3000);

        return importResult.workflowId;
    }

    async triggerWorkflowExecution(page) {
        const executed = await page.evaluate(() => {
            const executeButton = document.querySelector('button[title="Execute"]');
            if (!executeButton) {
                return { success: false, error: 'No Execute button found' };
            }
            executeButton.click();
            return { success: true };
        });

        if (!executed.success) {
            throw new Error(executed.error);
        }

        await this.sleep(3000);
    }

    /**
     * Mark link and workflow as executed in both databases
     */
    async markAsExecuted(linkId, metadataId, success, recordingStatus = null, errorMessage = null, startTime = null) {
        try {
            const executionTime = startTime ? new Date() - startTime : 0;
            
            // Update PostgreSQL links table
            const pgQuery = `
                UPDATE links 
                SET 
                    executed = TRUE,
                    processed_by_workflow = TRUE,
                    workflow_status = $1,
                    workflow_processed_time = CURRENT_TIMESTAMP
                WHERE links_id = $2
            `;
            await this.pgPool.query(pgQuery, [success ? 'completed' : 'failed', linkId]);

            // Update MongoDB workflow_metadata
            const mongoUpdate = {
                executed: true,
                success: success,
                executed_at: new Date().toISOString(),
                status: success ? 'completed' : 'failed',
                updated_at: new Date().toISOString(),
                execution_time_ms: executionTime
            };

            if (errorMessage) {
                mongoUpdate.error_message = errorMessage;
                mongoUpdate.last_error_message = errorMessage;
                mongoUpdate.last_error_timestamp = new Date().toISOString();
            }

            if (recordingStatus && recordingStatus.success) {
                mongoUpdate.recording_urls = {
                    web: recordingStatus.webRecordingUrl,
                    video: recordingStatus.videoRecordingUrl,
                    retrieved_at: new Date().toISOString()
                };
            }

            await this.mongodb.collection('workflow_metadata').updateOne(
                { _id: metadataId },
                { 
                    $set: mongoUpdate,
                    ...(errorMessage && { $inc: { execution_attempts: 1 } })
                }
            );

        } catch (error) {
            console.error(`  ⚠ Error marking as executed: ${error.message}`);
        }
    }

    /**
     * Main orchestration method
     */
    /**
     * Main orchestration method
     */
    async orchestrate() {
        console.log('\n' + '='.repeat(80));
        console.log('SMART WORKFLOW ORCHESTRATION STARTED');
        console.log('='.repeat(80));
        console.log(`Start Time: ${this.stats.startTime.toISOString()}`);
        console.log(`Workflow Type: ${this.workflowType}`);
        console.log(`Time Limit: ${this.timeLimit} hours`);
        console.log(`Max Workflows: ${this.maxWorkflows}`);
        console.log(`Gap Between Workflows: ${this.gapBetweenWorkflows}s`);
        console.log('='.repeat(80) + '\n');

        this.executionStartTime = new Date();

        try {
            // Fetch eligible link-workflow-account combinations
            const combinations = await this.fetchEligibleLinks();

            if (combinations.length === 0) {
                console.log('✓ No eligible combinations found. Orchestration complete.');
                return { success: true, processed: 0 };
            }

            console.log(`\n→ Processing ${combinations.length} link-workflow-account combinations\n`);

            // Track workflows matched (all combinations found)
            this.stats.workflowsMatched = combinations.length;

            // Process each combination directly
            for (let i = 0; i < combinations.length; i++) {
                // Check if we should stop
                const stopCheck = this.shouldStopExecution();
                if (stopCheck.stop) {
                    console.log('\n' + '⚠'.repeat(40));
                    console.log(`STOPPING EXECUTION: ${stopCheck.reason}`);
                    if (stopCheck.reason === 'time_limit') {
                        console.log(`Time elapsed: ${stopCheck.elapsedHours.toFixed(2)} hours (limit: ${this.timeLimit} hours)`);
                    } else if (stopCheck.reason === 'max_workflows') {
                        console.log(`Workflows executed: ${stopCheck.workflowsExecuted} (limit: ${this.maxWorkflows})`);
                    }
                    console.log('⚠'.repeat(40) + '\n');
                    break;
                }

                const combo = combinations[i];
                
                console.log('\n' + '='.repeat(80));
                console.log(`PROCESSING COMBINATION ${i + 1}/${combinations.length}`);
                console.log('='.repeat(80));
                console.log(`Link ID: ${combo.links_id}`);
                console.log(`URL: ${combo.link.substring(0, 60)}...`);
                console.log(`Workflow: ${combo.workflow_name}`);
                console.log(`Account: ${combo.account_id}`);
                console.log('='.repeat(80));

                // Create workflow metadata object from combination data
                const workflowMetadata = {
                    _id: new ObjectId(combo.metadata_id),
                    automa_workflow_id: combo.automa_workflow_id,
                    workflow_name: combo.workflow_name,
                    workflow_type: combo.workflow_type,
                    postgres_account_id: combo.account_id,
                    postgres_content_id: combo.links_id
                };

                // Create link data object
                const linkData = {
                    links_id: combo.links_id,
                    link: combo.link,
                    tweet_id: combo.tweet_id,
                    tweeted_date: combo.tweeted_date,
                    tweeted_time: combo.tweeted_time
                };

                // Execute the workflow
                await this.executeWorkflow(linkData, workflowMetadata, i + 1, combinations.length);

                // Gap between workflows (skip for last one)
                if (i < combinations.length - 1) {
                    const remainingWorkflows = this.maxWorkflows - this.stats.workflowsExecuted;
                    if (remainingWorkflows > 0) {
                        console.log(`⏱ Waiting ${this.gapBetweenWorkflows}s before next workflow...`);
                        await this.sleep(this.gapBetweenWorkflows * 1000);
                    }
                }
            }

            this.executionEndTime = new Date();
            const duration = this.executionEndTime - this.executionStartTime;
            const durationHours = duration / (1000 * 60 * 60);
            
            console.log('\n' + '='.repeat(80));
            console.log('ORCHESTRATION COMPLETED');
            console.log('='.repeat(80));
            console.log(`Duration: ${Math.round(duration/1000)}s (${durationHours.toFixed(2)} hours)`);
            console.log(`Combinations Found: ${this.stats.linksFound}`);
            console.log(`Workflows Matched: ${this.stats.workflowsMatched}`);
            console.log(`Workflows Executed: ${this.stats.workflowsExecuted}`);
            console.log(`Successful: ${this.stats.successfulExecutions}`);
            console.log(`Failed: ${this.stats.failedExecutions}`);
            console.log(`Success Rate: ${this.stats.workflowsExecuted > 0 ? Math.round((this.stats.successfulExecutions/this.stats.workflowsExecuted)*100) : 0}%`);
            console.log(`Time Limit Reached: ${this.stats.timeLimitReached ? 'Yes' : 'No'}`);
            console.log(`Max Workflows Reached: ${this.stats.maxWorkflowsReached ? 'Yes' : 'No'}`);
            console.log('='.repeat(80) + '\n');

            return {
                success: true,
                ...this.stats,
                duration,
                durationHours
            };

        } catch (error) {
            console.error('\n' + '❌'.repeat(40));
            console.error('ORCHESTRATION FAILED');
            console.error('❌'.repeat(40));
            console.error(`Error: ${error.message}`);
            console.error(`Stack: ${error.stack}`);
            throw error;
        }
    }

    async cleanup() {
        console.log('\n' + '─'.repeat(80));
        console.log('Final cleanup...');
        
        if (this.hyperbrowserService) {
            await this.hyperbrowserService.cleanup();
            console.log('  ✓ Hyperbrowser service cleaned up');
        }
        
        if (this.profileManager) {
            await this.profileManager.disconnect();
            console.log('  ✓ Profile manager disconnected');
        }
        
        if (this.mongoClient) {
            await this.mongoClient.close();
            console.log('  ✓ MongoDB connection closed');
        }

        if (this.pgPool) {
            await this.pgPool.end();
            console.log('  ✓ PostgreSQL pool closed');
        }
        
        console.log('✓ Cleanup completed');
        console.log('─'.repeat(80) + '\n');
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }
}

// Main execution
async function main() {
    const orchestrator = new SmartWorkflowOrchestrator();
    
    try {
        await orchestrator.initialize();
        const result = await orchestrator.orchestrate();
        
        console.log('\n✓ Program completed successfully');
        console.log(`Total execution time: ${Math.round(result.duration/1000)}s`);
        console.log(`Workflows executed: ${result.workflowsExecuted}/${result.workflowsMatched} matched`);
        
        process.exit(0);
    } catch (error) {
        console.error('\n❌ Fatal error:', error.message);
        console.error(error.stack);
        process.exit(1);
    } finally {
        await orchestrator.cleanup();
    }
}

// Signal handlers
process.on('SIGINT', async () => {
    console.log('\n\n⚠ Received SIGINT, shutting down gracefully...');
    process.exit(0);
});

process.on('SIGTERM', async () => {
    console.log('\n\n⚠ Received SIGTERM, shutting down gracefully...');
    process.exit(0);
});

// Start the orchestrator
main();