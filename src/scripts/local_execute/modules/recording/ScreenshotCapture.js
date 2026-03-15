// modules/recording/ScreenshotCapture.js

/**
 * Captures screenshots from Automa workflow logs
 * 
 * FIX: Instead of looking for files in downloads directory,
 * extract screenshots from the workflow logs where Automa stores them
 */

export class ScreenshotCapture {
    constructor(mongoDBService) {
        this.mongoDBService = mongoDBService;
    }

    /**
     * Extract screenshots from workflow log data
     */
    extractScreenshotsFromLog(logData, sessionId, options = {}) {
        const screenshots = [];
        
        if (!logData?.history) {
            return screenshots;
        }

        const { 
            accountId, 
            username, 
            profileId, 
            workflowType, 
            workflowName, 
            linkId, 
            linkUrl, 
            executionId,
            screenshotPrefix 
        } = options;

        for (const entry of logData.history) {
            // Check for screenshot blocks
            if (entry.type === 'block' && entry.name === 'take-screenshot') {
                const screenshotData = entry.data?.screenshot;
                const blockName = entry.data?.description || entry.description || 'Screenshot';
                const timestamp = entry.timestamp || Date.now();
                const category = this._determineCategory(blockName, screenshotPrefix);
                
                if (screenshotData) {
                    // Remove data URL prefix if present
                    const base64Data = screenshotData.split(',').pop() || screenshotData;
                    const buffer = Buffer.from(base64Data, 'base64');
                    
                    const filename = this._generateFilename(blockName, category, timestamp, screenshotPrefix);
                    
                    screenshots.push({
                        buffer,
                        metadata: {
                            session_id: sessionId,
                            execution_id: executionId || sessionId,
                            postgres_account_id: accountId,
                            account_username: username,
                            profile_id: profileId,
                            workflow_type: workflowType,
                            workflow_name: workflowName,
                            link_id: linkId,
                            link_url: linkUrl,
                            block_name: blockName,
                            category: category,
                            filename: filename,
                            timestamp: timestamp,
                            captured_by: 'automa_workflow',
                            source: 'workflow_log',
                            screenshot_prefix: screenshotPrefix
                        }
                    });
                    
                    console.log(`  📸 Found screenshot: ${filename} (${category})`);
                }
            }
            
            // Check nested blocks
            if (entry.data?.blocks) {
                for (const block of entry.data.blocks) {
                    if (block.type === 'take-screenshot' || block.id === 'take-screenshot') {
                        const screenshotData = block.data?.screenshot;
                        const blockName = block.data?.description || 'Screenshot';
                        const timestamp = block.timestamp || Date.now();
                        const category = this._determineCategory(blockName, screenshotPrefix);
                        
                        if (screenshotData) {
                            const base64Data = screenshotData.split(',').pop() || screenshotData;
                            const buffer = Buffer.from(base64Data, 'base64');
                            
                            const filename = this._generateFilename(blockName, category, timestamp, screenshotPrefix);
                            
                            screenshots.push({
                                buffer,
                                metadata: {
                                    session_id: sessionId,
                                    execution_id: executionId || sessionId,
                                    postgres_account_id: accountId,
                                    account_username: username,
                                    profile_id: profileId,
                                    workflow_type: workflowType,
                                    workflow_name: workflowName,
                                    link_id: linkId,
                                    link_url: linkUrl,
                                    block_name: blockName,
                                    category: category,
                                    filename: filename,
                                    timestamp: timestamp,
                                    captured_by: 'automa_workflow',
                                    source: 'workflow_log',
                                    screenshot_prefix: screenshotPrefix
                                }
                            });
                            
                            console.log(`  📸 Found screenshot: ${filename} (${category})`);
                        }
                    }
                }
            }
        }

        return screenshots;
    }

    _determineCategory(blockName, screenshotPrefix) {
        const name = blockName.toLowerCase();
        const prefix = screenshotPrefix ? screenshotPrefix.toLowerCase() : '';
        
        if (name.includes('tweet') || name.includes('reply') || prefix.includes('tweet')) {
            return 'reply';
        }
        if (name.includes('retweet') || prefix.includes('retweet')) {
            return 'retweet';
        }
        if (name.includes('message') || prefix.includes('message')) {
            return 'message';
        }
        if (name.includes('error') || prefix.includes('error')) {
            return 'errors';
        }
        return 'debug';
    }

    _generateFilename(blockName, category, timestamp, screenshotPrefix) {
        const safe = blockName.replace(/[^a-z0-9]/gi, '_').toLowerCase();
        if (screenshotPrefix) {
            return `${category}_${screenshotPrefix}_${timestamp}.png`;
        }
        return `${category}_${safe}_${timestamp}.png`;
    }

    /**
     * Process screenshots from orchestrator log and save to GridFS
     */
    async processScreenshotsFromLog(orchLog, sessionId, options) {
        console.log(`\n📸 Extracting screenshots from workflow logs...`);
        
        const screenshots = this.extractScreenshotsFromLog(orchLog, sessionId, options);
        const screenshotIds = [];

        if (screenshots.length === 0) {
            console.log(`  No screenshots found in workflow logs`);
            return { screenshotIds, count: 0 };
        }

        console.log(`  Found ${screenshots.length} screenshot(s) in logs`);

        for (const screenshot of screenshots) {
            try {
                // Upload to GridFS
                const gridfsId = await this.mongoDBService.uploadToGridFS(
                    screenshot.buffer,
                    screenshot.metadata.filename,
                    { 
                        contentType: 'image/png',
                        metadata: screenshot.metadata 
                    },
                    'screenshots'
                );

                // Create metadata entry
                const metaResult = await this.mongoDBService.db.collection('screenshot_metadata').insertOne({
                    ...screenshot.metadata,
                    gridfs_file_id: gridfsId,
                    size: screenshot.buffer.length,
                    created_at: new Date(),
                });

                screenshotIds.push(metaResult.insertedId.toString());
                console.log(`  ✓ Saved: ${screenshot.metadata.filename}`);
                
            } catch (err) {
                console.warn(`  ⚠️  Failed to save screenshot: ${err.message}`);
            }
        }

        // Update execution session with screenshot IDs
        if (screenshotIds.length > 0) {
            try {
                await this.mongoDBService.db.collection('execution_sessions').updateOne(
                    { session_id: sessionId },
                    { $push: { screenshots: { $each: screenshotIds } } }
                );
            } catch (err) {
                console.warn(`  ⚠️  Failed to update session with screenshot IDs: ${err.message}`);
            }
        }

        // Log summary by category
        const categories = {};
        for (const s of screenshots) {
            const cat = s.metadata.category;
            categories[cat] = (categories[cat] || 0) + 1;
        }
        
        console.log(`  📊 Screenshot summary: ${Object.entries(categories).map(([cat, count]) => `${cat}=${count}`).join(', ')}, total=${screenshots.length}`);

        return { screenshotIds, count: screenshots.length };
    }

    /**
     * Legacy method kept for backward compatibility
     */
    async captureAll(browser, sessionId, options) {
        // This method is now deprecated - screenshots should be extracted from logs
        console.log(`\n⚠️  captureAll() is deprecated - use processScreenshotsFromLog() instead`);
        return { all: [] };
    }
}
