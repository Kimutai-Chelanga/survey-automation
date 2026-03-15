// modules/database/MongoDBService.js
import { MongoClient, ObjectId, GridFSBucket } from 'mongodb';

/**
 * Handles all MongoDB operations
 * ✅ ADDED: storeExecutionStatistics() and logDataInconsistency() methods
 */
export class MongoDBService {
    constructor(mongoUri, dbName) {
        this.mongoUri = mongoUri;
        this.dbName = dbName;
        this.client = null;
        this.db = null;
    }

    /**
     * Connect to MongoDB
     */
    async connect() {
        this.client = new MongoClient(this.mongoUri);
        await this.client.connect();
        this.db = this.client.db(this.dbName);
        console.log('✓ MongoDB connected');
    }

    /**
     * Close MongoDB connection
     */
    async close() {
        if (this.client) {
            await this.client.close();
            console.log('✓ MongoDB closed');
        }
    }

    /**
     * ✅ Analyze Automa logs for errors and success
     */
    analyzeAutomaLogs(logs) {
        if (!Array.isArray(logs) || logs.length === 0) {
            return {
                has_errors: false,
                error_count: 0,
                success_count: 0,
                failed: false,
                total_steps: 0
            };
        }

        let errorCount = 0;
        let successCount = 0;

        for (const step of logs) {
            // Check for errors
            if (step.$isError === true || step.type === 'error' || step.status === 'error') {
                errorCount++;
            } else if (step.type === 'success' || step.status === 'success') {
                successCount++;
            }
        }

        return {
            has_errors: errorCount > 0,
            error_count: errorCount,
            success_count: successCount,
            failed: errorCount > 0,
            total_steps: logs.length
        };
    }

    /**
     * ✅ Store Automa execution logs in MongoDB
     */
    async storeAutomaLogs(logData) {
        try {
            const collection = this.db.collection('automa_execution_logs');

            const logDocument = {
                ...logData,
                created_at: logData.created_at || new Date(),
                updated_at: new Date()
            };

            const result = await collection.insertOne(logDocument);

            console.log(`✓ Automa logs stored in MongoDB: ${result.insertedId}`);
            return result.insertedId;
        } catch (error) {
            console.error(`❌ Failed to store Automa logs: ${error.message}`);
            throw error;
        }
    }

    /**
     * ✅ NEW: Store execution statistics in MongoDB
     * FIXES: "this.mongoDBService.storeExecutionStatistics is not a function" error
     */
    async storeExecutionStatistics(statsDocument) {
        try {
            const collection = this.db.collection('execution_statistics');
            
            const statRecord = {
                ...statsDocument,
                created_at: new Date(),
                updated_at: new Date()
            };

            const result = await collection.insertOne(statRecord);
            console.log(`✓ Execution statistics stored: ${result.insertedId}`);
            return result.insertedId;
        } catch (error) {
            console.error(`❌ Failed to store execution statistics: ${error.message}`);
            throw error;
        }
    }

    /**
     * ✅ NEW: Log data inconsistency issues
     * Used by validateDataConsistency in orchestrator
     */
    async logDataInconsistency(inconsistencyData) {
        try {
            const collection = this.db.collection('data_consistency_logs');
            
            const logRecord = {
                ...inconsistencyData,
                logged_at: new Date(),
                created_at: new Date()
            };

            const result = await collection.insertOne(logRecord);
            console.log(`✓ Data inconsistency logged: ${result.insertedId}`);
            return result.insertedId;
        } catch (error) {
            console.error(`❌ Failed to log data inconsistency: ${error.message}`);
            throw error;
        }
    }

    /**
     * ✅ Get Automa logs by execution ID
     */
    async getAutomaLogsByExecutionId(executionId) {
        try {
            const collection = this.db.collection('automa_execution_logs');
            const logs = await collection.findOne({ execution_id: executionId });
            return logs;
        } catch (error) {
            console.error(`❌ Failed to retrieve Automa logs: ${error.message}`);
            return null;
        }
    }

    /**
     * ✅ Get all Automa logs with optional filters
     */
    async getAllAutomaLogs(filters = {}, limit = 50) {
        try {
            const collection = this.db.collection('automa_execution_logs');
            const logs = await collection
                .find(filters)
                .sort({ created_at: -1 })
                .limit(limit)
                .toArray();
            return logs;
        } catch (error) {
            console.error(`❌ Failed to retrieve Automa logs: ${error.message}`);
            return [];
        }
    }

    /**
     * ✅ FIXED: Fetch Automa workflow from CORRECT collection
     * @param {string} workflowId - Workflow ObjectId
     * @param {string} databaseName - Database name (default: 'messages_db')
     * @param {string} collectionName - Collection name (default: 'automa_workflows')
     */
    async fetchAutomaWorkflow(workflowId, databaseName = 'messages_db', collectionName = 'automa_workflows') {
        console.log(`📁 Fetching workflow ${workflowId} from ${databaseName}.${collectionName}`);

        try {
            // Get the target database and collection
            const targetDb = this.client.db(databaseName);
            const workflow = await targetDb.collection(collectionName).findOne({
                _id: new ObjectId(workflowId)
            });

            if (!workflow) {
                throw new Error(
                    `Automa workflow not found: ${workflowId} in ${databaseName}.${collectionName}`
                );
            }

            console.log(`✓ Found workflow: ${workflow.name || 'Unknown'}`);
            console.log(`  Version: ${workflow.version || 'N/A'}`);
            console.log(`  Nodes: ${workflow.drawflow?.nodes?.length || 0}`);

            return workflow;
        } catch (error) {
            console.error(`❌ Error fetching workflow from ${databaseName}.${collectionName}:`, error.message);
            throw error;
        }
    }

    /**
     * ✅ FIXED: Get workflow assignments with database and collection info
     * @param {Array} linkIds - Array of link IDs to fetch
     * @param {Object} additionalFilters - Additional MongoDB filters
     */
    async getWorkflowAssignments(linkIds, additionalFilters = {}) {
        // Base filter
        const filter = {
            postgres_content_id: { $in: linkIds },
            has_link: true,
            has_content: true,
            status: 'ready_to_execute',
            executed: false,
            ...additionalFilters  // Merge additional filters from execution settings
        };

        console.log('🔍 Querying workflow_metadata with filter:', JSON.stringify(filter, null, 2));

        const assignments = await this.db.collection('workflow_metadata')
            .find(filter)
            .toArray();

        console.log(`✓ Retrieved ${assignments.length} workflow assignments`);

        return assignments.map(doc => ({
            metadata_id: doc._id.toString(),
            postgres_content_id: doc.postgres_content_id,
            automa_workflow_id: doc.automa_workflow_id.toString(),
            workflow_type: doc.workflow_type,
            workflow_name: doc.workflow_name,
            account_id: doc.account_id,
            category: doc.category,
            collection_name: doc.collection_name,
            database_name: doc.database_name || 'execution_workflows',  // ✅ RETURN THIS
        }));
    }

    /**
     * Create execution session record
     */
    async createExecutionSession(sessionData) {
        const result = await this.db.collection('execution_sessions').insertOne(sessionData);
        console.log(`  Execution session created: ${result.insertedId}`);
        return result.insertedId.toString();
    }

    /**
     * Update execution session record
     */
    async updateExecutionSession(sessionId, updateData) {
        await this.db.collection('execution_sessions').updateOne(
            { _id: new ObjectId(sessionId) },
            { $set: updateData }
        );
        console.log(`  Execution session updated: ${sessionId}`);
    }

    /**
     * Create video recording metadata
     */
    async createVideoMetadata(metadataRecord) {
        const result = await this.db.collection('video_recording_metadata').insertOne(metadataRecord);
        console.log(`✓ Video recording metadata created: ${result.insertedId}`);
        return result.insertedId.toString();
    }

    /**
     * Create screenshot metadata
     */
    async createScreenshotMetadata(metadata) {
        const result = await this.db.collection('screenshot_metadata').insertOne(metadata);
        return result.insertedId;
    }

    /**
     * Update workflow metadata as executed
     */
    async markWorkflowExecuted(metadataId, updateData) {
        await this.db.collection('workflow_metadata').updateOne(
            { _id: metadataId },
            {
                $set: updateData,
                ...(updateData.error_message && { $inc: { execution_attempts: 1 } })
            }
        );
    }

    /**
     * Upload file to GridFS
     */
    async uploadToGridFS(buffer, filename, metadata, bucketName = 'video_recordings') {
        const bucket = new GridFSBucket(this.db, { bucketName });

        const uploadStream = bucket.openUploadStream(filename, {
            contentType: metadata.contentType,
            metadata: metadata.metadata
        });

        await new Promise((resolve, reject) => {
            uploadStream.on('finish', resolve);
            uploadStream.on('error', reject);
            uploadStream.end(buffer);
        });

        return uploadStream.id;
    }

    /**
     * Get GridFS bucket for screenshots
     */
    getScreenshotBucket() {
        return new GridFSBucket(this.db, { bucketName: 'screenshots' });
    }
}
