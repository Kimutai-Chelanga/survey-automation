// Account-Specific Profile Manager for JavaScript execution
// Handles account-specific Hyperbrowser profiles, extensions, and sessions

import { MongoClient, ObjectId } from 'mongodb';

export class AccountProfileManager {
    constructor(mongoUri, dbName) {
        this.mongoUri = mongoUri;
        this.dbName = dbName;
        this.client = null;
        this.db = null;
        this.isConnected = false;
        this.accountProfileCache = new Map();
    }

    async connect() {
        if (this.isConnected) {
            return this.db;
        }

        try {
            this.client = new MongoClient(this.mongoUri);
            await this.client.connect();
            this.db = this.client.db(this.dbName);
            this.isConnected = true;
            console.log('AccountProfileManager MongoDB connected');
            return this.db;
        } catch (error) {
            console.error('AccountProfileManager MongoDB connection failed:', error.message);
            throw error;
        }
    }

    async disconnect() {
        if (this.client && this.isConnected) {
            try {
                await this.client.close();
                this.isConnected = false;
                console.log('AccountProfileManager MongoDB disconnected');
            } catch (error) {
                console.error('Error disconnecting AccountProfileManager MongoDB:', error.message);
            }
        }
    }

    async ensureConnected() {
        if (!this.isConnected) {
            await this.connect();
        }
    }

    // ==============================================================================
    // ACCOUNT PROFILE RETRIEVAL METHODS
    // ==============================================================================

    /**
     * Get all accounts with their associated profiles
     * @returns {Array} Array of account objects with profile information
     */
    async getAllAccountProfiles() {
        await this.ensureConnected();
        
        try {
            console.log('Loading all account profiles...');
            
            // FIXED: Get accounts that have profile_id set and postgres_account_id
            const accounts = await this.db.collection('accounts').find({
                'postgres_account_id': { $exists: true, $ne: null },
                'profile_id': { $exists: true, $ne: null }
            }).toArray();

            console.log(`Found ${accounts.length} accounts with profiles`);

            const accountProfiles = [];

            for (const account of accounts) {
                try {
                    // FIXED: The profile data is in the same document since we store everything in 'accounts'
                    const profile = account; // Profile data is in the same document
                    
                    // Get extension details for this account
                    const extension = await this.db.collection('extension_instances').findOne({
                        'postgres_account_id': account.postgres_account_id,
                        'is_enabled': true,
                        'installation_status': 'active'
                    });

                    const accountProfile = {
                        accountId: account.postgres_account_id,
                        username: account.username,
                        profileId: account.profile_id,
                        extensionId: extension ? extension.extension_id : null,
                        lastWorkflowSync: account.last_workflow_sync,
                        totalRepliesProcessed: account.total_replies_processed || 0,
                        totalMessagesProcessed: account.total_messages_processed || 0,
                        totalRetweetsProcessed: account.total_retweets_processed || 0,
                        profileValidated: true, // Profile exists in our collection
                        extensionAvailable: !!extension,
                        profileData: profile,
                        extensionData: extension,
                        mongoAccountId: account._id
                    };

                    accountProfiles.push(accountProfile);

                    // Cache for quick access
                    this.accountProfileCache.set(account.postgres_account_id, accountProfile);

                } catch (error) {
                    console.error(`Error processing account ${account.username || 'unknown'}:`, error.message);
                    continue;
                }
            }

            console.log(`Successfully loaded ${accountProfiles.length} account profiles`);
            return accountProfiles;

        } catch (error) {
            console.error('Error getting all account profiles:', error.message);
            throw error;
        }
    }

    /**
     * Get profile information for a specific account
     * @param {number} accountId - The PostgreSQL account ID
     * @returns {Object} Account profile information
     */
    async getAccountProfile(accountId) {
        await this.ensureConnected();

        // Check cache first
        if (this.accountProfileCache.has(accountId)) {
            console.log(`Retrieved account ${accountId} profile from cache`);
            return this.accountProfileCache.get(accountId);
        }

        try {
            // Get account from MongoDB
            const account = await this.db.collection('accounts').findOne({
                'postgres_account_id': accountId
            });

            if (!account) {
                throw new Error(`Account with ID ${accountId} not found in MongoDB`);
            }

            if (!account.profile_id) {
                throw new Error(`Account ${accountId} does not have a profile_id configured`);
            }

            // Profile data is in the account document itself
            const profile = account;

            // Try to get extension for this account - now OPTIONAL
            const extension = await this.db.collection('extension_instances').findOne({
                'postgres_account_id': accountId,
                'is_enabled': true,
                'installation_status': 'active',
                'linked_to_postgres': true
            });

            // Log if no extension found, but don't throw error
            if (!extension) {
                console.log(`No extension configured for account ${accountId}. Extension-based features will be unavailable.`);
            }

            const accountProfile = {
                accountId: accountId,
                username: account.username,
                profileId: account.profile_id,
                extensionId: extension ? extension.extension_id : null,  // Null if no extension
                lastWorkflowSync: account.last_workflow_sync,
                totalRepliesProcessed: account.total_replies_processed || 0,
                totalMessagesProcessed: account.total_messages_processed || 0,
                totalRetweetsProcessed: account.total_retweets_processed || 0,
                profileValidated: true,  // Profile exists in accounts collection
                extensionAvailable: extension !== null,  // True only if extension exists
                extensionVerified: extension ? extension.linked_to_postgres === true : false,
                profileData: profile,
                extensionData: extension || null,  // Null if no extension
                mongoAccountId: account._id
            };

            // Cache the result
            this.accountProfileCache.set(accountId, accountProfile);

            const extensionInfo = extension 
                ? `Extension: ${extension.extension_id}` 
                : 'No extension configured';
            console.log(`Retrieved profile for account ${account.username}: ${account.profile_id}, ${extensionInfo}`);
            
            return accountProfile;

        } catch (error) {
            console.error(`Error getting profile for account ${accountId}:`, error.message);
            throw error;
        }
    }
    /**
     * Get profile ID for a specific account
     * @param {number} accountId - The PostgreSQL account ID
     * @returns {string} Profile ID
     */
    async getAccountProfileId(accountId) {
        const accountProfile = await this.getAccountProfile(accountId);
        return accountProfile.profileId;
    }

    /**
     * Get extension ID for a specific account
     * @param {number} accountId - The PostgreSQL account ID
     * @returns {string|null} Extension ID or null if not available
     */
    async getAccountExtensionId(accountId) {
        const accountProfile = await this.getAccountProfile(accountId);
        return accountProfile.extensionId;
    }

    // ==============================================================================
    // ACCOUNT PROFILE VALIDATION METHODS
    // ==============================================================================

    /**
     * Validate that an account's profile exists and is accessible
     * @param {number} accountId - The PostgreSQL account ID
     * @param {Object} hyperbrowserService - Hyperbrowser service instance
     * @returns {Object} Validation result
     */
    async validateAccountProfile(accountId, hyperbrowserService) {
        try {
            const accountProfile = await this.getAccountProfile(accountId);
            
            if (!accountProfile.profileValidated) {
                return {
                    isValid: false,
                    error: 'Profile not found in chrome_profiles collection',
                    accountProfile
                };
            }

            // Validate with Hyperbrowser service if provided
            if (hyperbrowserService) {
                const profileValidation = await hyperbrowserService.validateProfile(accountProfile.profileId);
                
                if (!profileValidation.isValid) {
                    return {
                        isValid: false,
                        error: `Profile validation failed with Hyperbrowser: ${profileValidation.error}`,
                        accountProfile
                    };
                }
            }

            return {
                isValid: true,
                accountProfile
            };

        } catch (error) {
            return {
                isValid: false,
                error: error.message,
                accountProfile: null
            };
        }
    }

    /**
     * Validate all account profiles
     * @param {Object} hyperbrowserService - Hyperbrowser service instance
     * @returns {Array} Array of validation results
     */
    async validateAllAccountProfiles(hyperbrowserService) {
        const accountProfiles = await this.getAllAccountProfiles();
        const validationResults = [];

        console.log(`Validating ${accountProfiles.length} account profiles...`);

        for (const accountProfile of accountProfiles) {
            const validationResult = await this.validateAccountProfile(
                accountProfile.accountId, 
                hyperbrowserService
            );
            
            validationResults.push({
                accountId: accountProfile.accountId,
                username: accountProfile.username,
                profileId: accountProfile.profileId,
                ...validationResult
            });
        }

        const validProfiles = validationResults.filter(result => result.isValid).length;
        console.log(`Profile validation complete: ${validProfiles}/${validationResults.length} profiles valid`);

        return validationResults;
    }

    // ==============================================================================
    // ACCOUNT SESSION MANAGEMENT
    // ==============================================================================

    /**
     * Create a session for a specific account
     * @param {Object} sessionData - Session creation data
     * @returns {string} MongoDB session ID
     */
    async createAccountSession(sessionData) {
        await this.ensureConnected();

        const {
            accountId,
            profileId,
            extensionId,
            dagRunId,
            executionDate,
            username,
            sessionPurpose = 'account_workflow_execution'
        } = sessionData;

        try {
            const sessionRecord = {
                session_id: new ObjectId().toString(),
                postgres_account_id: accountId,
                account_username: username,
                profile_id: profileId,
                extension_id: extensionId,
                browser_type: 'chrome',
                session_status: 'active',
                is_active: true,
                created_at: new Date(),
                started_at: new Date(),
                ended_at: null,
                session_purpose: sessionPurpose,
                workflow_type: 'account_specific_execution',
                workflow_count: 0,
                success_count: 0,
                failed_count: 0,
                automa_integration_errors: 0,
                dag_run_id: dagRunId,
                execution_date: executionDate,
                session_metadata: {
                    created_for: 'account_specific_workflow',
                    created_via: 'javascript_account_profile_manager',
                    account_id: accountId,
                    account_username: username,
                    parent_profile_id: profileId,
                    extension_loaded: Boolean(extensionId),
                    stealth_enabled: true,
                    screen_resolution: '1920x1080'
                }
            };

            const result = await this.db.collection('browser_sessions').insertOne(sessionRecord);
            console.log(`Created account session for ${username} (${accountId}): ${result.insertedId}`);
            return result.insertedId.toString();

        } catch (error) {
            console.error(`Error creating session for account ${accountId}:`, error.message);
            throw error;
        }
    }

    /**
     * Update session statistics for an account
     * @param {string} sessionMongoId - MongoDB session ID
     * @param {Object} stats - Statistics to update
     * @returns {boolean} Success status
     */
    async updateAccountSessionStats(sessionMongoId, stats) {
        await this.ensureConnected();

        try {
            const updateData = {
                last_activity_at: new Date(),
                updated_at: new Date()
            };

            if (stats.workflowCount !== undefined) {
                updateData.workflow_count = stats.workflowCount;
            }
            if (stats.successCount !== undefined) {
                updateData.success_count = stats.successCount;
            }
            if (stats.failedCount !== undefined) {
                updateData.failed_count = stats.failedCount;
            }
            if (stats.automaIntegrationErrors !== undefined) {
                updateData.automa_integration_errors = stats.automaIntegrationErrors;
            }

            // Calculate success rate if we have both counts
            if (stats.successCount !== undefined && stats.workflowCount !== undefined && stats.workflowCount > 0) {
                updateData.success_rate = Math.round((stats.successCount / stats.workflowCount) * 100 * 100) / 100;
            }

            const result = await this.db.collection('browser_sessions').updateOne(
                { _id: new ObjectId(sessionMongoId) },
                { $set: updateData }
            );

            return result.modifiedCount > 0;

        } catch (error) {
            console.error(`Error updating session stats for ${sessionMongoId}:`, error.message);
            return false;
        }
    }

    /**
     * Close a session for an account
     * @param {string} sessionMongoId - MongoDB session ID
     * @returns {boolean} Success status
     */
    async closeAccountSession(sessionMongoId) {
        await this.ensureConnected();

        try {
            const result = await this.db.collection('browser_sessions').updateOne(
                { _id: new ObjectId(sessionMongoId) },
                { 
                    $set: {
                        is_active: false,
                        session_status: 'completed',
                        ended_at: new Date(),
                        updated_at: new Date()
                    }
                }
            );

            return result.modifiedCount > 0;

        } catch (error) {
            console.error(`Error closing session ${sessionMongoId}:`, error.message);
            return false;
        }
    }

    // ==============================================================================
    // ACCOUNT STATISTICS AND REPORTING
    // ==============================================================================

    /**
     * Update account workflow statistics
     * @param {number} accountId - PostgreSQL account ID
     * @param {Object} statsUpdate - Statistics to update
     * @returns {boolean} Success status
     */
    async updateAccountWorkflowStats(accountId, statsUpdate) {
        await this.ensureConnected();

        try {
            const updateData = {
                last_workflow_sync: new Date(),
                updated_at: new Date()
            };

            const incData = {};
            if (statsUpdate.repliesProcessed > 0) {
                incData.total_replies_processed = statsUpdate.repliesProcessed;
            }
            if (statsUpdate.messagesProcessed > 0) {
                incData.total_messages_processed = statsUpdate.messagesProcessed;
            }
            if (statsUpdate.retweetsProcessed > 0) {
                incData.total_retweets_processed = statsUpdate.retweetsProcessed;
            }

            const updateQuery = { $set: updateData };
            if (Object.keys(incData).length > 0) {
                updateQuery.$inc = incData;
            }

            const result = await this.db.collection('accounts').updateOne(
                { postgres_account_id: accountId },
                updateQuery
            );

            // Update cache if it exists
            if (this.accountProfileCache.has(accountId)) {
                const cachedProfile = this.accountProfileCache.get(accountId);
                cachedProfile.lastWorkflowSync = updateData.last_workflow_sync;
                if (statsUpdate.repliesProcessed > 0) {
                    cachedProfile.totalRepliesProcessed += statsUpdate.repliesProcessed;
                }
                if (statsUpdate.messagesProcessed > 0) {
                    cachedProfile.totalMessagesProcessed += statsUpdate.messagesProcessed;
                }
                if (statsUpdate.retweetsProcessed > 0) {
                    cachedProfile.totalRetweetsProcessed += statsUpdate.retweetsProcessed;
                }
            }

            return result.modifiedCount > 0;

        } catch (error) {
            console.error(`Error updating account stats for ${accountId}:`, error.message);
            return false;
        }
    }

    /**
     * Get workflow analytics for a specific account
     * @param {number} accountId - PostgreSQL account ID
     * @param {number} days - Number of days to look back
     * @returns {Array} Analytics data
     */
    async getAccountWorkflowAnalytics(accountId, days = 7) {
        await this.ensureConnected();

        try {
            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - days);

            const pipeline = [
                {
                    $match: {
                        postgres_account_id: accountId,
                        executed_at: { $gte: cutoffDate },
                        executed: true
                    }
                },
                {
                    $group: {
                        _id: {
                            date: { $dateToString: { format: '%Y-%m-%d', date: '$executed_at' } },
                            content_type: '$content_type'
                        },
                        total_executions: { $sum: 1 },
                        successful_executions: { $sum: { $cond: [{ $eq: ['$execution_success', true] }, 1, 0] } },
                        failed_executions: { $sum: { $cond: [{ $eq: ['$execution_success', false] }, 1, 0] } },
                        avg_execution_time: { $avg: '$execution_time' }
                    }
                },
                {
                    $sort: { '_id.date': -1, '_id.content_type': 1 }
                }
            ];

            const analytics = await this.db.collection('workflow_executions')
                .aggregate(pipeline).toArray();

            return analytics;

        } catch (error) {
            console.error(`Error getting analytics for account ${accountId}:`, error.message);
            return [];
        }
    }

    /**
     * Get summary statistics for all accounts
     * @returns {Array} Account summaries
     */
    async getAllAccountSummaries() {
        await this.ensureConnected();

        try {
            const pipeline = [
                {
                    $match: {
                        postgres_account_id: { $exists: true }
                    }
                },
                {
                    $lookup: {
                        from: 'chrome_profiles',
                        localField: 'profile_id',
                        foreignField: 'profile_id',
                        as: 'profile_info'
                    }
                },
                {
                    $lookup: {
                        from: 'extension_instances',
                        localField: 'postgres_account_id',
                        foreignField: 'postgres_account_id',
                        as: 'extension_info'
                    }
                },
                {
                    $project: {
                        postgres_account_id: 1,
                        username: 1,
                        profile_id: 1,
                        total_replies_processed: 1,
                        total_messages_processed: 1,
                        total_retweets_processed: 1,
                        last_workflow_sync: 1,
                        has_profile: { $gt: [{ $size: '$profile_info' }, 0] },
                        has_extension: { $gt: [{ $size: '$extension_info' }, 0] },
                        profile_active: { 
                            $cond: [
                                { $gt: [{ $size: '$profile_info' }, 0] },
                                { $arrayElemAt: ['$profile_info.is_active', 0] },
                                false
                            ]
                        },
                        extension_enabled: {
                            $cond: [
                                { $gt: [{ $size: '$extension_info' }, 0] },
                                { $arrayElemAt: ['$extension_info.is_enabled', 0] },
                                false
                            ]
                        }
                    }
                }
            ];

            const summaries = await this.db.collection('accounts')
                .aggregate(pipeline).toArray();

            return summaries;

        } catch (error) {
            console.error('Error getting account summaries:', error.message);
            return [];
        }
    }

    // ==============================================================================
    // UTILITY METHODS
    // ==============================================================================

    /**
     * Clear the account profile cache
     */
    clearCache() {
        this.accountProfileCache.clear();
        console.log('Account profile cache cleared');
    }

    /**
     * Get cache statistics
     * @returns {Object} Cache statistics
     */
    getCacheStats() {
        return {
            cachedAccounts: this.accountProfileCache.size,
            cachedAccountIds: Array.from(this.accountProfileCache.keys())
        };
    }

    /**
     * Refresh cache for a specific account
     * @param {number} accountId - PostgreSQL account ID
     * @returns {Object} Updated account profile
     */
    async refreshAccountCache(accountId) {
        this.accountProfileCache.delete(accountId);
        return await this.getAccountProfile(accountId);
    }
}

export default AccountProfileManager;