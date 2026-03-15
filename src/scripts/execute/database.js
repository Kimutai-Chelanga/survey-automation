// Complete MongoDB database operations manager with updated schema
// UPDATED: Modified to work with workflow_metadata and automa_workflows collections

import { MongoClient, ObjectId } from 'mongodb';

export class DatabaseManager {
    constructor(config) {
        this.uri = config.mongodb.uri;
        this.dbName = config.mongodb.dbName;
        this.client = null;
        this.db = null;
        this.isConnected = false;
    }

    async connect() {
        if (this.isConnected) {
            return this.db;
        }

        try {
            this.client = new MongoClient(this.uri);
            await this.client.connect();
            this.db = this.client.db(this.dbName);
            this.isConnected = true;
            console.log('MongoDB connected successfully');
            return this.db;
        } catch (error) {
            console.error('MongoDB connection failed:', error.message);
            throw error;
        }
    }

    async disconnect() {
        if (this.client && this.isConnected) {
            try {
                await this.client.close();
                this.isConnected = false;
                console.log('MongoDB disconnected');
            } catch (error) {
                console.error('Error disconnecting from MongoDB:', error.message);
            }
        }
    }

    // ==============================================================================
    // WEEKLY WORKFLOW SETTINGS
    // ==============================================================================

    /**
     * Get weekly workflow configuration for current day
     */
    async getWeeklyWorkflowConfig() {
        await this.ensureConnected();

        try {
            const daysOfWeek = ['sunday', 'monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday'];
            const currentDay = daysOfWeek[new Date().getDay()];
            
            console.log(`Getting weekly workflow config for: ${currentDay}`);

            const settingsDoc = await this.db.collection('settings').findOne({ 
                category: 'system' 
            });

            if (!settingsDoc?.settings?.weekly_workflow_settings) {
                console.warn('No weekly_workflow_settings found, using default configuration');
                return this.getDefaultDayConfig(currentDay);
            }

            const weeklySettings = settingsDoc.settings.weekly_workflow_settings;
            const dayConfig = weeklySettings[currentDay];

            if (!dayConfig) {
                console.warn(`No configuration found for ${currentDay}, using defaults`);
                return this.getDefaultDayConfig(currentDay);
            }

            if (!dayConfig.enabled) {
                console.log(`Workflows are DISABLED for ${currentDay}`);
                return {
                    enabled: false,
                    day: currentDay,
                    links_to_filter: 0,
                    workflows_to_process: 0,
                    workflow_types: {},
                    gap_between_workflows: 0
                };
            }

            const workflowTypes = {
                replies: dayConfig.content_types?.includes('replies') ?? true,
                messages: dayConfig.content_types?.includes('messages') ?? true,
                retweets: dayConfig.content_types?.includes('retweets') ?? false
            };

            const config = {
                enabled: dayConfig.enabled,
                day: currentDay,
                links_to_filter: dayConfig.links_to_filter || 10,
                workflows_to_process: dayConfig.workflows_to_process || 15,
                morning_time: dayConfig.morning_time || '09:00',
                evening_time: dayConfig.evening_time || '18:00',
                workflow_types: workflowTypes,
                content_types: dayConfig.content_types || ['messages', 'replies'],
                gap_between_workflows: dayConfig.gap_between_workflows || 300,
                time_limit: dayConfig.time_limit || 2,
                priority_order: this.getPriorityOrderFromContentTypes(dayConfig.content_types)
            };

            console.log(`Weekly config for ${currentDay}:`, JSON.stringify(config, null, 2));
            return config;

        } catch (error) {
            console.error('Error getting weekly workflow config:', error.message);
            return this.getDefaultDayConfig();
        }
    }

    /**
     * Get default configuration for a day
     */
    getDefaultDayConfig(day = 'monday') {
        const isWeekend = day === 'saturday' || day === 'sunday';
        
        return {
            enabled: true,
            day: day,
            links_to_filter: isWeekend ? 8 : 10,
            workflows_to_process: isWeekend ? 12 : 15,
            morning_time: isWeekend ? '10:00' : '09:00',
            evening_time: isWeekend ? '17:00' : '18:00',
            workflow_types: {
                replies: true,
                messages: true,
                retweets: false
            },
            content_types: ['messages', 'replies'],
            gap_between_workflows: isWeekend ? 600 : 300,
            time_limit: 2,
            priority_order: ['replies', 'messages']
        };
    }

    /**
     * Convert content_types array to priority order
     */
    getPriorityOrderFromContentTypes(contentTypes) {
        if (!contentTypes || !Array.isArray(contentTypes)) {
            return ['replies', 'messages', 'retweets'];
        }
        
        const validTypes = contentTypes.filter(type => 
            ['replies', 'messages', 'retweets'].includes(type)
        );
        
        return validTypes.length > 0 ? validTypes : ['replies', 'messages', 'retweets'];
    }

    // ==============================================================================
    // DAILY WORKFLOW LIMITS - Uses workflow_metadata collection
    // ==============================================================================

    /**
     * Get today's workflow execution count from workflow_metadata
     */
    async getTodaysWorkflowCount() {
        await this.ensureConnected();

        try {
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            const todayISO = today.toISOString();
            
            const tomorrow = new Date(today);
            tomorrow.setDate(tomorrow.getDate() + 1);
            const tomorrowISO = tomorrow.toISOString();

            const count = await this.db.collection('workflow_metadata').countDocuments({
                executed: true,
                executed_at: {
                    $gte: todayISO,
                    $lt: tomorrowISO
                }
            });

            console.log(`Today's workflow execution count: ${count}`);
            return count;

        } catch (error) {
            console.error('Error getting today\'s workflow count:', error.message);
            return 0;
        }
    }

    /**
     * Check if daily workflow limit is reached
     */
    async isDailyWorkflowLimitReached() {
        await this.ensureConnected();

        try {
            const todaysCount = await this.getTodaysWorkflowCount();
            const weeklyConfig = await this.getWeeklyWorkflowConfig();
            
            const dailyLimit = weeklyConfig.workflows_to_process || 15;
            const limitReached = todaysCount >= dailyLimit;

            console.log(`Daily workflow limit check: ${todaysCount}/${dailyLimit} (limit reached: ${limitReached})`);
            
            return {
                limitReached,
                currentCount: todaysCount,
                dailyLimit,
                remaining: Math.max(0, dailyLimit - todaysCount)
            };

        } catch (error) {
            console.error('Error checking daily workflow limit:', error.message);
            return {
                limitReached: false,
                currentCount: 0,
                dailyLimit: 15,
                remaining: 15
            };
        }
    }

    /**
     * Get remaining workflows available for today
     */
    async getRemainingWorkflowsForToday() {
        const limitStatus = await this.isDailyWorkflowLimitReached();
        return limitStatus.remaining;
    }

    /**
     * Create daily workflow limit analytics
     */
    async createDailyWorkflowLimitAnalytics() {
        await this.ensureConnected();

        try {
            const today = new Date();
            today.setHours(0, 0, 0, 0);
            
            const weeklyConfig = await this.getWeeklyWorkflowConfig();
            const limitStatus = await this.isDailyWorkflowLimitReached();

            const analyticsRecord = {
                date: today,
                day_of_week: weeklyConfig.day,
                daily_limit: weeklyConfig.workflows_to_process,
                workflows_executed: limitStatus.currentCount,
                workflows_remaining: limitStatus.remaining,
                limit_reached: limitStatus.limitReached,
                limit_utilization_percentage: weeklyConfig.workflows_to_process > 0 
                    ? Math.round((limitStatus.currentCount / weeklyConfig.workflows_to_process) * 100) 
                    : 0,
                weekly_config_snapshot: weeklyConfig,
                created_at: new Date(),
                updated_at: new Date()
            };

            // Upsert daily record
            await this.db.collection('daily_workflow_analytics').updateOne(
                { date: today },
                { $set: analyticsRecord },
                { upsert: true }
            );

            console.log(`Daily workflow analytics updated: ${limitStatus.currentCount}/${weeklyConfig.workflows_to_process} workflows`);
            return analyticsRecord;

        } catch (error) {
            console.error('Error creating daily workflow analytics:', error.message);
            return null;
        }
    }

    // ==============================================================================
    // WORKFLOW QUERIES WITH NEW SCHEMA (workflow_metadata + automa_workflows)
    // ==============================================================================

    /**
     * UPDATED: Get workflows to execute using new schema with daily limit enforcement
     * Fetches from workflow_metadata (executed: false, has_link: true) and joins with automa_workflows
     */
    async getWorkflowsToExecuteByAccount(requestedLimit = 100) {
        await this.ensureConnected();

        try {
            // Check daily workflow limit first
            const limitStatus = await this.isDailyWorkflowLimitReached();
            
            if (limitStatus.limitReached) {
                console.log(`Daily workflow limit reached (${limitStatus.currentCount}/${limitStatus.dailyLimit}). No more workflows will be processed today.`);
                return [];
            }

            // Adjust the limit to not exceed daily remaining workflows
            const effectiveLimit = Math.min(requestedLimit, limitStatus.remaining);
            
            console.log(`Daily workflow status: ${limitStatus.currentCount}/${limitStatus.dailyLimit} executed`);
            console.log(`Remaining workflows for today: ${limitStatus.remaining}`);
            console.log(`Effective query limit: ${effectiveLimit} (requested: ${requestedLimit})`);

            if (effectiveLimit <= 0) {
                console.log('No workflows available within daily limits');
                return [];
            }

            // Query filter for workflow_metadata
            const queryFilter = {
                has_link: true,
                executed: false,
                execute: true, // Must be ready for execution
                has_content: true, // Must have content
                postgres_account_id: { $exists: true, $ne: null },
                automa_workflow_id: { $exists: true, $ne: null }
            };

            console.log('Using query filter:', JSON.stringify(queryFilter, null, 2));

            // Count eligible workflows
            const eligibleCount = await this.db.collection('workflow_metadata')
                .countDocuments(queryFilter);

            console.log(`Found ${eligibleCount} eligible workflows across all accounts`);

            if (eligibleCount === 0) {
                console.log('No workflows found matching the criteria:');
                console.log('- executed: false');
                console.log('- has_link: true');
                console.log('- execute: true');
                console.log('- has_content: true');
                console.log('- postgres_account_id: exists and not null');
                console.log('- automa_workflow_id: exists and not null');
                
                return [];
            }

            // Aggregation pipeline to join workflow_metadata with automa_workflows
            const pipeline = [
                { $match: queryFilter },
                {
                    $lookup: {
                        from: 'accounts',
                        localField: 'postgres_account_id',
                        foreignField: 'postgres_account_id',
                        as: 'account_info'
                    }
                },
                {
                    $lookup: {
                        from: 'automa_workflows',
                        localField: 'automa_workflow_id',
                        foreignField: '_id',
                        as: 'workflow_info'
                    }
                },
                {
                    $addFields: {
                        account_profile_id: { 
                            $cond: [
                                { $gt: [{ $size: '$account_info' }, 0] },
                                { $arrayElemAt: ['$account_info.profile_id', 0] },
                                null
                            ]
                        },
                        account_username: { 
                            $cond: [
                                { $gt: [{ $size: '$account_info' }, 0] },
                                { $arrayElemAt: ['$account_info.username', 0] },
                                { $ifNull: ['$username', 'Unknown'] }
                            ]
                        },
                        workflow_name: { 
                            $cond: [
                                { $gt: [{ $size: '$workflow_info' }, 0] },
                                { $arrayElemAt: ['$workflow_info.name', 0] },
                                { $ifNull: ['$workflow_name', 'Unknown Workflow'] }
                            ]
                        },
                        workflow_data: { 
                            $cond: [
                                { $gt: [{ $size: '$workflow_info' }, 0] },
                                { $arrayElemAt: ['$workflow_info', 0] },
                                null
                            ]
                        },
                        has_account_info: { $gt: [{ $size: '$account_info' }, 0] },
                        has_workflow_info: { $gt: [{ $size: '$workflow_info' }, 0] }
                    }
                },
                { 
                    $sort: { 
                        processing_priority: 1, // Lower number = higher priority
                        created_at: 1 
                    } 
                },
                { $limit: effectiveLimit }
            ];

            const workflows = await this.db.collection('workflow_metadata')
                .aggregate(pipeline).toArray();

            console.log(`Retrieved ${workflows.length} workflows from aggregation pipeline`);

            const workflowsToProcess = [];

            for (const workflow of workflows) {
                const workflowToProcess = {
                    linkId: workflow._id.toString(),
                    executionId: workflow.execution_id,
                    automaWorkflowId: workflow.automa_workflow_id.toString(),
                    workflowName: workflow.workflow_name || 'Unknown',
                    workflowData: this.convertObjectIdsToStrings(workflow.workflow_data),
                    postgresContentId: workflow.postgres_content_id || null,
                    postgresAccountId: workflow.postgres_account_id,
                    workflowType: workflow.workflow_type || 'unknown',
                    hasLink: workflow.has_link || false,
                    linkUrl: workflow.link_url || null,
                    executed: workflow.executed || false,
                    accountUsername: workflow.account_username || 'Unknown',
                    accountProfileId: workflow.account_profile_id || workflow.profile_id || null,
                    hasValidAccount: workflow.has_account_info,
                    hasValidWorkflow: workflow.has_workflow_info,
                    status: workflow.status || 'unknown',
                    processingPriority: workflow.processing_priority || 1,
                    contentPreview: workflow.content_text_preview || null
                };

                workflowsToProcess.push(workflowToProcess);
            }

            console.log(`Selected ${workflowsToProcess.length} workflow(s) for execution (within daily limit of ${limitStatus.dailyLimit})`);
            
            // Log account distribution
            const accountDistribution = {};
            workflowsToProcess.forEach(workflow => {
                const accountId = workflow.postgresAccountId;
                if (!accountDistribution[accountId]) {
                    accountDistribution[accountId] = {
                        username: workflow.accountUsername,
                        count: 0,
                        hasValidAccount: workflow.hasValidAccount
                    };
                }
                accountDistribution[accountId].count++;
            });

            console.log('Workflow distribution by account:');
            Object.entries(accountDistribution).forEach(([accountId, info]) => {
                const status = info.hasValidAccount ? '✓' : '⚠️';
                console.log(`  ${status} Account ${info.username} (${accountId}): ${info.count} workflows`);
            });

            // Log workflow type distribution
            const typeDistribution = {};
            workflowsToProcess.forEach(workflow => {
                typeDistribution[workflow.workflowType] = (typeDistribution[workflow.workflowType] || 0) + 1;
            });

            console.log('Workflow distribution by type:');
            Object.entries(typeDistribution).forEach(([type, count]) => {
                console.log(`  - ${type}: ${count} workflows`);
            });

            return workflowsToProcess;

        } catch (error) {
            console.error('Error getting workflows by account:', error.message);
            console.error('Stack trace:', error.stack);
            throw error;
        }
    }

    // ==============================================================================
    // SETTINGS MANAGEMENT
    // ==============================================================================

    /**
     * Get extraction settings with weekly workflow configuration support
     */
    async getExtractionSettings() {
        await this.ensureConnected();
        
        try {
            const weeklyConfig = await this.getWeeklyWorkflowConfig();
            
            if (weeklyConfig && weeklyConfig.enabled) {
                console.log(`Using weekly workflow config for ${weeklyConfig.day}`);
                return {
                    workflowTypes: weeklyConfig.workflow_types,
                    priorityOrder: weeklyConfig.priority_order,
                    dailyWorkflowLimit: weeklyConfig.workflows_to_process,
                    source: 'weekly_config',
                    day: weeklyConfig.day
                };
            }
            
            if (weeklyConfig && !weeklyConfig.enabled) {
                console.log(`Workflows disabled for ${weeklyConfig.day}`);
                return {
                    workflowTypes: {
                        replies: false,
                        messages: false,
                        retweets: false
                    },
                    priorityOrder: [],
                    dailyWorkflowLimit: 0,
                    source: 'weekly_config_disabled',
                    day: weeklyConfig.day
                };
            }
            
            // Fallback to legacy settings
            const settings = await this.db.collection('settings').findOne({
                'category': 'system'
            });
            
            if (settings && settings.settings && settings.settings.extraction_workflow_types) {
                console.log('Using legacy extraction_workflow_types settings');
                return {
                    workflowTypes: settings.settings.extraction_workflow_types,
                    priorityOrder: settings.settings.extraction_priority_order || ['replies', 'messages', 'retweets'],
                    dailyWorkflowLimit: 15,
                    source: 'legacy_settings'
                };
            }
            
            console.log('No extraction settings found, using permissive defaults');
            return {
                workflowTypes: {
                    replies: true,
                    messages: true,
                    retweets: true
                },
                priorityOrder: ['replies', 'messages', 'retweets'],
                dailyWorkflowLimit: 15,
                source: 'defaults'
            };
        } catch (error) {
            console.error('Error getting extraction settings:', error.message);
            return {
                workflowTypes: {
                    replies: true,
                    messages: true,
                    retweets: true
                },
                priorityOrder: ['replies', 'messages', 'retweets'],
                dailyWorkflowLimit: 15,
                source: 'error_fallback'
            };
        }
    }

    /**
     * Get workflow gap setting with weekly configuration support
     */
    async getWorkflowGapSetting() {
        await this.ensureConnected();

        try {
            const weeklyConfig = await this.getWeeklyWorkflowConfig();
            
            if (weeklyConfig && weeklyConfig.enabled && weeklyConfig.gap_between_workflows) {
                const gapSeconds = weeklyConfig.gap_between_workflows;
                console.log(`Using weekly gap setting for ${weeklyConfig.day}: ${gapSeconds} seconds`);
                return Math.max(gapSeconds, 5);
            }
            
            // Fallback to legacy settings
            const settingsDoc = await this.db.collection('settings').findOne({ category: 'system' });
            
            if (settingsDoc?.settings) {
                const extractionSettings = settingsDoc.settings.extraction_processing_settings || {};
                let gapMinutes = extractionSettings.gap_between_workflows || 0.25;
                let gapSeconds = typeof gapMinutes === 'number' ? parseInt(gapMinutes * 60) : 15;

                if (gapSeconds <= 0) {
                    const strategySettings = settingsDoc.settings.workflow_strategy_settings || {};
                    gapSeconds = parseInt(strategySettings.workflow_gap_seconds || 15);
                }

                console.log(`Using legacy gap setting: ${gapSeconds} seconds`);
                return Math.max(gapSeconds, 5);
            }
            
            console.log('Using default gap setting: 15 seconds');
            return 15;
        } catch (error) {
            console.error(`Error retrieving workflow gap: ${error.message}`);
            return 15;
        }
    }

    // ==============================================================================
    // WORKFLOW EXECUTION TRACKING - UPDATED for workflow_metadata
    // ==============================================================================

    /**
     * UPDATED: Update workflow execution in workflow_metadata collection
     */
    async updateWorkflowExecution(linkId, executionData) {
        await this.ensureConnected();

        if (linkId.startsWith('direct_') || linkId.startsWith('test_')) {
            console.log(`Skipping database update for test workflow: ${linkId}`);
            return { success: true, modified: false };
        }

        try {
            const now = new Date();
            const updateData = {
                executed: true,
                executed_at: executionData.executedAt || now.toISOString(),
                success: executionData.success,
                status: executionData.success ? 'completed' : 'failed',
                updated_at: now.toISOString()
            };

            // Add account-specific data
            if (executionData.accountId) {
                updateData.postgres_account_id = executionData.accountId;
            }
            if (executionData.accountUsername) {
                updateData.username = executionData.accountUsername;
            }
            if (executionData.profileId) {
                updateData.profile_id = executionData.profileId;
            }

            // Add execution results and metrics
            if (executionData.success) {
                updateData.execution_end = now.toISOString();
                if (executionData.executionTime) {
                    updateData.execution_time_ms = executionData.executionTime;
                }
                if (executionData.finalResult) {
                    updateData['performance_metrics.final_result'] = executionData.finalResult;
                }
            } else {
                updateData.error_message = executionData.error || 'Unknown error';
                updateData.last_error_message = executionData.error || 'Unknown error';
                updateData.last_error_timestamp = now.toISOString();
                updateData.execution_attempts = { $inc: 1 };
            }

            // Update the workflow_metadata document
            const result = await this.db.collection('workflow_metadata').updateOne(
                { _id: new ObjectId(linkId) },
                { 
                    $set: updateData,
                    ...(updateData.execution_attempts && { $inc: { execution_attempts: 1 } })
                }
            );

            // Track account profile assignment if this is the first execution for the account
            if (result.modifiedCount > 0 && executionData.accountId && executionData.profileId) {
                await this.trackAccountProfileAssignment(
                    executionData.accountId, 
                    executionData.profileId, 
                    executionData.accountUsername
                );
            }

            // Log daily progress after successful execution
            if (result.modifiedCount > 0 && executionData.success) {
                const updatedLimitStatus = await this.isDailyWorkflowLimitReached();
                console.log(`Workflow execution updated for ${linkId} - Account: ${executionData.accountUsername}`);
                console.log(`Daily progress: ${updatedLimitStatus.currentCount}/${updatedLimitStatus.dailyLimit} workflows executed`);
                
                if (updatedLimitStatus.limitReached) {
                    console.log(`Daily workflow limit reached! No more workflows will be processed today.`);
                }
            }

            return { success: true, modified: result.modifiedCount > 0 };

        } catch (error) {
            console.error(`Error updating workflow execution for ${linkId}:`, error.message);
            return { success: false, error: error.message };
        }
    }

    /**
     * Track account profile assignments for monitoring
     */
    async trackAccountProfileAssignment(accountId, profileId, username) {
        try {
            const existingAssignment = await this.db.collection('account_profile_assignments').findOne({
                postgres_account_id: accountId,
                profile_id: profileId,
                is_active: true
            });

            if (!existingAssignment) {
                await this.db.collection('account_profile_assignments').insertOne({
                    postgres_account_id: accountId,
                    username: username,
                    profile_id: profileId,
                    assignment_date: new Date(),
                    assigned_by: 'system',
                    assignment_reason: 'workflow_execution_tracking',
                    is_active: true,
                    validation_status: 'validated',
                    validation_date: new Date(),
                    usage_stats: {
                        workflows_executed: 1,
                        last_workflow_date: new Date(),
                        success_rate: 0,
                        total_sessions: 0
                    },
                    created_at: new Date(),
                    updated_at: new Date()
                });
                console.log(`Tracked new account profile assignment: ${username} -> ${profileId}`);
            } else {
                await this.db.collection('account_profile_assignments').updateOne(
                    { _id: existingAssignment._id },
                    { 
                        $inc: { 'usage_stats.workflows_executed': 1 },
                        $set: { 
                            'usage_stats.last_workflow_date': new Date(),
                            'updated_at': new Date()
                        }
                    }
                );
            }
        } catch (error) {
            console.warn(`Warning: Could not track account profile assignment: ${error.message}`);
        }
    }

    // ==============================================================================
    // UTILITY METHODS
    // ==============================================================================

    convertObjectIdsToStrings(obj) {
        if (obj && typeof obj === 'object') {
            if (obj._bsontype === 'ObjectID' || ObjectId.isValid(obj)) {
                return obj.toString();
            } else if (Array.isArray(obj)) {
                return obj.map(item => this.convertObjectIdsToStrings(item));
            } else {
                const result = {};
                for (const [key, value] of Object.entries(obj)) {
                    result[key] = this.convertObjectIdsToStrings(value);
                }
                return result;
            }
        }
        return obj;
    }

    async ensureConnected() {
        if (!this.isConnected) {
            await this.connect();
        }
    }
}

export default DatabaseManager;