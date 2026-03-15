#!/usr/bin/env node
/**
 * Main Workflow Orchestrator with Success/Failure Tracking
 * Executes workflows based on Streamlit UI configuration
 */

import { ConfigManager } from './modules/config/ConfigManager.js';
import { MongoDBService } from './modules/database/MongoDBService.js';
import { PostgreSQLService } from './modules/database/PostgreSQLService.js';
import { WorkflowFetcher } from './modules/workflow/WorkflowFetcher.js';
import { WorkflowExecutor } from './modules/workflow/WorkflowExecutor.js';
import { ChromeSessionManager } from './modules/chrome/ChromeSessionManager.js';
import { CookieManager } from './modules/chrome/CookieManager.js';
import { ProfileManager } from './modules/chrome/ProfileManager.js';
import { AutomaExecutor } from './modules/automas/AutomaExecutor.js';
import { VideoRecorder } from './modules/recording/VideoRecorder.js';
import { ScreenshotCapture } from './modules/recording/ScreenshotCapture.js';

class WorkflowOrchestrator {
    constructor() {
        this.config = new ConfigManager();
        this.mongoDBService = null;
        this.pgService = null;
        this.workflowFetcher = null;
        this.workflowExecutor = null;
        
        // Additional services required by WorkflowExecutor
        this.sessionManager = null;
        this.cookieManager = null;
        this.profileManager = null;
        this.automaExecutor = null;
        this.videoRecorder = null;
        this.screenshotCapture = null;

        // Success/Failure statistics
        this.stats = {
            startTime: new Date(),
            totalWorkflows: 0,
            successfulWorkflows: 0,
            failedWorkflows: 0,
            postgresUpdates: {
                success: 0,
                failure: 0,
                errors: 0
            },
            mongoUpdates: {
                success: 0,
                failure: 0,
                errors: 0
            }
        };
    }

    async initialize() {
        console.log('\n' + '='.repeat(80));
        console.log('WORKFLOW ORCHESTRATOR INITIALIZING');
        console.log('='.repeat(80));

        try {
            // Initialize configuration
            await this.config.initialize();

            // Initialize database services
            this.mongoDBService = new MongoDBService(
                this.config.mongoUri,
                this.config.dbName
            );
            await this.mongoDBService.connect();

            this.pgService = new PostgreSQLService(this.config.pgConfig);
            await this.pgService.connect();

            // Initialize Chrome-related services
            this.sessionManager = new ChromeSessionManager(this.config);
            this.cookieManager = new CookieManager();
            
            // Initialize ProfileManager and load profiles
            this.profileManager = new ProfileManager(this.mongoDBService, this.config);
            await this.profileManager.loadProfiles();

            // ✅ FIXED: Use this.config.extensionId instead of this.config.automaExtensionId
            this.automaExecutor = new AutomaExecutor(this.config.extensionId);

            // Initialize recording services
            this.videoRecorder = new VideoRecorder(this.config.recordingsDir);
            this.screenshotCapture = new ScreenshotCapture(this.mongoDBService);

            // Initialize workflow modules
            this.workflowFetcher = new WorkflowFetcher(
                this.mongoDBService,
                this.pgService,
                this.config
            );

            // ✅ NOW pass all required dependencies to WorkflowExecutor
            this.workflowExecutor = new WorkflowExecutor(
                this.config,
                this.mongoDBService,
                this.pgService,
                this.sessionManager,
                this.cookieManager,
                this.automaExecutor,
                this.videoRecorder,
                this.screenshotCapture,
                this.profileManager
            );

            console.log('✅ All systems initialized successfully');
            return true;

        } catch (error) {
            console.error('❌ Initialization failed:', error.message);
            throw error;
        }
    }

    async execute() {
        console.log('\n' + '='.repeat(80));
        console.log('WORKFLOW EXECUTION STARTING');
        console.log('='.repeat(80));
        console.log(`Start Time: ${this.stats.startTime.toISOString()}`);
        console.log(`Timezone: ${this.config.timezone}`);
        console.log(`Success/Failure Tracking: ${this.config.trackSuccessFailure ? 'ENABLED' : 'DISABLED'}`);
        console.log('='.repeat(80) + '\n');

        try {
            // Step 1: Fetch eligible workflows based on execution settings
            console.log('📋 Step 1: Fetching execution configuration...');
            const executionData = await this.workflowFetcher.fetchEligibleWorkflows();

            if (!executionData || executionData.combinations.length === 0) {
                console.log('ℹ️ No eligible workflows found for execution');
                return this.stats;
            }

            const { combinations, settings } = executionData;
            this.stats.totalWorkflows = combinations.length;

            console.log(`✅ Found ${combinations.length} workflows to execute`);
            console.log(`   Category: ${settings.destination_category}`);
            console.log(`   Type: ${settings.workflow_type_name}`);
            console.log(`   Collection: ${settings.collection_name || 'All'}`);
            console.log(`   Max Workflows: ${settings.max_workflows}`);
            console.log(`   Gap: ${settings.gap_seconds}s\n`);

            // Step 2: Execute each workflow
            console.log('🚀 Step 2: Executing workflows...');
            for (let i = 0; i < combinations.length; i++) {
                const combo = combinations[i];
                const workflowNumber = i + 1;

                console.log(`\n🔷 Workflow ${workflowNumber}/${combinations.length}`);
                console.log(`   Link ID: ${combo.links_id}`);
                console.log(`   Workflow: ${combo.workflow_name}`);
                console.log(`   Account: ${combo.account_id}`);

                try {
                    // Prepare linkData and workflowMetadata from combo
                    const linkData = {
                        links_id: combo.links_id,
                        link: combo.link,
                        tweet_id: combo.tweet_id,
                        tweeted_date: combo.tweeted_date,
                        tweeted_time: combo.tweeted_time
                    };

                    const workflowMetadata = {
                        metadata_id: combo.metadata_id,
                        workflow_name: combo.workflow_name,
                        workflow_type: combo.workflow_type,
                        automa_workflow_id: combo.automa_workflow_id,
                        postgres_account_id: combo.account_id,
                        category: combo.category,
                        collection_name: combo.collection_name,
                        database_name: combo.database_name || 'execution_workflows'
                    };

                    // Execute the workflow using execute() method
                    const result = await this.workflowExecutor.execute(
                        linkData,
                        workflowMetadata,
                        workflowNumber,
                        combinations.length
                    );

                    // Update statistics based on result
                    if (result.success) {
                        this.stats.successfulWorkflows++;
                        console.log(`   ✅ SUCCESS - Execution completed`);

                        // Update PostgreSQL success flag
                        if (this.config.updatePostgresSuccess) {
                            const pgUpdate = await this.pgService.updateLinkSuccessStatus(
                                combo.links_id,
                                true  // success = true
                            );
                            if (pgUpdate) {
                                this.stats.postgresUpdates.success++;
                                console.log(`   📊 PostgreSQL: success=TRUE, failure=FALSE`);
                            }
                        }
                    } else {
                        this.stats.failedWorkflows++;
                        console.log(`   ❌ FAILURE - ${result.error || 'Unknown error'}`);

                        // Update PostgreSQL failure flag
                        if (this.config.updatePostgresFailure) {
                            const pgUpdate = await this.pgService.updateLinkFailureStatus(
                                combo.links_id,
                                result.error || 'Workflow execution failed'
                            );
                            if (pgUpdate) {
                                this.stats.postgresUpdates.failure++;
                                console.log(`   📊 PostgreSQL: success=FALSE, failure=TRUE`);
                            }
                        }
                    }

                    // Wait between workflows (if not the last one)
                    if (i < combinations.length - 1) {
                        const delaySeconds = settings.gap_seconds || 30;
                        console.log(`   ⏱️  Waiting ${delaySeconds}s before next workflow...`);
                        await this.sleep(delaySeconds * 1000);
                    }

                } catch (executionError) {
                    this.stats.failedWorkflows++;
                    console.error(`   💥 EXECUTION ERROR: ${executionError.message}`);

                    // Still try to update PostgreSQL
                    try {
                        if (this.config.updatePostgresFailure) {
                            await this.pgService.updateLinkFailureStatus(
                                combo.links_id,
                                `Execution error: ${executionError.message}`
                            );
                            this.stats.postgresUpdates.failure++;
                        }
                    } catch (pgError) {
                        this.stats.postgresUpdates.errors++;
                        console.error(`   📊 PostgreSQL update failed: ${pgError.message}`);
                    }
                }
            }

            // Step 3: Generate execution summary
            console.log('\n' + '='.repeat(80));
            console.log('EXECUTION COMPLETED');
            console.log('='.repeat(80));
            this.printFinalStatistics();
            console.log('='.repeat(80));

            // Step 4: Store statistics in MongoDB
            if (this.config.sendSuccessStatsToMongo) {
                await this.storeExecutionStatistics();
            }

            // Step 5: Validate data consistency
            if (this.config.validateDataConsistency) {
                await this.validateDataConsistency();
            }

            return this.stats;

        } catch (error) {
            console.error('\n❌ ORCHESTRATION FAILED:', error.message);
            throw error;
        }
    }

    printFinalStatistics() {
        const duration = new Date() - this.stats.startTime;
        const successRate = this.stats.totalWorkflows > 0
            ? (this.stats.successfulWorkflows / this.stats.totalWorkflows * 100).toFixed(1)
            : 0;

        console.log(`Duration: ${Math.round(duration/1000)}s (${(duration/1000/60).toFixed(1)} minutes)`);
        console.log(`Total Workflows: ${this.stats.totalWorkflows}`);
        console.log(`✅ Successful: ${this.stats.successfulWorkflows}`);
        console.log(`❌ Failed: ${this.stats.failedWorkflows}`);
        console.log(`📈 Success Rate: ${successRate}%`);
        console.log('');
        console.log('📊 PostgreSQL Updates:');
        console.log(`   Success flags: ${this.stats.postgresUpdates.success}`);
        console.log(`   Failure flags: ${this.stats.postgresUpdates.failure}`);
        console.log(`   Update errors: ${this.stats.postgresUpdates.errors}`);

        // Check for data consistency issues
        const totalUpdated = this.stats.postgresUpdates.success + this.stats.postgresUpdates.failure;
        if (totalUpdated !== this.stats.totalWorkflows) {
            console.log(`⚠️  WARNING: Only ${totalUpdated}/${this.stats.totalWorkflows} PostgreSQL records were updated`);
        }
    }

    async storeExecutionStatistics() {
        try {
            const statsDocument = {
                execution_id: `airflow_${this.config.dagRunId}_${Date.now()}`,
                execution_date: new Date().toISOString(),
                dag_run_id: this.config.dagRunId,
                duration_seconds: Math.round((new Date() - this.stats.startTime) / 1000),
                total_workflows: this.stats.totalWorkflows,
                successful_workflows: this.stats.successfulWorkflows,
                failed_workflows: this.stats.failedWorkflows,
                success_rate: this.stats.totalWorkflows > 0
                    ? (this.stats.successfulWorkflows / this.stats.totalWorkflows * 100)
                    : 0,
                postgres_updates: this.stats.postgresUpdates,
                mongo_updates: this.stats.mongoUpdates,
                created_at: new Date(),
                updated_at: new Date()
            };

            await this.mongoDBService.storeExecutionStatistics(statsDocument);
            console.log('✅ Execution statistics stored in MongoDB');

        } catch (error) {
            console.error('❌ Failed to store execution statistics:', error.message);
        }
    }

    async validateDataConsistency() {
        try {
            console.log('\n🔍 Validating data consistency...');

            // Check that we don't have links with both success and failure = TRUE
            const inconsistentLinks = await this.pgService.findInconsistentSuccessFailure();

            if (inconsistentLinks.length > 0) {
                console.log(`⚠️  Found ${inconsistentLinks.length} links with inconsistent success/failure flags:`);
                inconsistentLinks.forEach(link => {
                    console.log(`   Link ${link.links_id}: success=${link.success}, failure=${link.failure}`);
                });

                // Log warning to MongoDB
                await this.mongoDBService.logDataInconsistency({
                    inconsistent_count: inconsistentLinks.length,
                    links: inconsistentLinks,
                    execution_id: this.config.dagRunId,
                    checked_at: new Date()
                });
            } else {
                console.log('✅ All success/failure flags are consistent');
            }

            // Check that executed count matches success + failure
            const executionStats = await this.pgService.getExecutionSummary();
            const totalMarked = executionStats.success_count + executionStats.failure_count;

            if (totalMarked !== executionStats.executed_count) {
                console.log(`⚠️  Execution count mismatch:`);
                console.log(`   Executed: ${executionStats.executed_count}`);
                console.log(`   Marked (success+failure): ${totalMarked}`);
                console.log(`   Difference: ${executionStats.executed_count - totalMarked}`);
            } else {
                console.log('✅ Execution counts match success+failure totals');
            }

        } catch (error) {
            console.error('❌ Data consistency check failed:', error.message);
        }
    }

    sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, ms));
    }

    async cleanup() {
        console.log('\n🧹 Cleaning up...');

        if (this.mongoDBService) {
            await this.mongoDBService.close();
        }

        if (this.pgService) {
            await this.pgService.close();
        }

        console.log('✅ Cleanup completed');
    }
}

// Main execution
async function main() {
    const orchestrator = new WorkflowOrchestrator();

    try {
        await orchestrator.initialize();
        const stats = await orchestrator.execute();

        console.log('\n' + '='.repeat(80));
        console.log('🎉 EXECUTION COMPLETED SUCCESSFULLY');
        console.log('='.repeat(80));
        console.log(`Total workflows: ${stats.totalWorkflows}`);
        console.log(`Success rate: ${stats.successfulWorkflows}/${stats.totalWorkflows} (${((stats.successfulWorkflows/stats.totalWorkflows)*100).toFixed(1)}%)`);
        console.log('='.repeat(80));

        process.exit(0);

    } catch (error) {
        console.error('\n💥 FATAL ERROR:', error.message);
        console.error(error.stack);
        process.exit(1);

    } finally {
        await orchestrator.cleanup();
    }
}

// Signal handlers
process.on('SIGINT', () => {
    console.log('\n\n⚠️ Received SIGINT, shutting down gracefully...');
    process.exit(0);
});

process.on('SIGTERM', () => {
    console.log('\n\n⚠️ Received SIGTERM, shutting down gracefully...');
    process.exit(0);
});

// Start execution
main();
