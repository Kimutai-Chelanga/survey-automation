import { MongoClient, ObjectId } from 'mongodb';
import { Client } from 'pg';

// Function to create/update comprehensive workflow execution record
export async function createOrUpdateWorkflowExecution(db, workflow, workflowType, linkData, pgLinkId, accountId) {
    try {
        const workflowExecutionsCollection = db.collection('workflow_executions');

        const workflowId = workflow.name || workflow._id.toString();
        const mongoWorkflowId = workflow._id.toString();

        const executionRecord = {
            workflow_id: workflowId,
            workflow_type: workflowType,
            postgres_content_id: null,
            postgres_account_id: accountId,
            workflow_mongo_id: mongoWorkflowId,
            postgres_link_id: pgLinkId,
            associated_link_url: linkData.url,
            link_tweet_id: linkData.tweetId || null,
            link_source_page: linkData.sourcePage || null,
            success: false,
            has_content: true,
            has_link: true,
            executed: false,
            execution_time: null,
            started_at: null,
            completed_at: null,
            created_at: new Date(),
            updated_at: new Date(),
            last_link_update: new Date(),
            error_message: null,
            workflow_name: workflow.name || `${workflowType}_${mongoWorkflowId}`,
            content_length: null,
            blocks_generated: workflow.drawflow?.nodes?.length || 0
        };

        const updateResult = await workflowExecutionsCollection.updateOne(
            {
                workflow_mongo_id: mongoWorkflowId,
                workflow_type: workflowType,
                postgres_account_id: accountId
            },
            {
                $set: {
                    ...executionRecord,
                    updated_at: new Date()
                }
            },
            { upsert: true }
        );

        if (updateResult.upsertedCount > 0) {
            console.log(`   - ✅ Created new workflow_executions record for ${workflowType} workflow ${workflowId} with link: ${linkData.url} (account_id: ${accountId})`);
        } else if (updateResult.modifiedCount > 0) {
            console.log(`   - ✅ Updated workflow_executions record for ${workflowType} workflow ${workflowId} with link: ${linkData.url} (account_id: ${accountId})`);
        } else {
            console.log(`   - ℹ️ No changes needed for workflow_executions record ${workflowId} (account_id: ${accountId})`);
        }

        return true;
    } catch (error) {
        console.error(`   - ❌ Error creating/updating workflow_executions for ${workflowType} workflow:`, error.message);
        return false;
    }
}

// Function to update PostgreSQL link with proper workflow association and account
export async function updatePostgreSQLLinkWithWorkflow(pgClient, linkUrl, pgLinkId, workflow, workflowType, accountId) {
    try {
        const workflowId = workflow.name || workflow._id.toString();
        const mongoWorkflowId = workflow._id.toString();

        const updateQuery = `
            UPDATE links
            SET filtered = TRUE,
                filtered_time = $1,
                mongo_object_id = $2,
                workflow_id = $3,
                mongo_workflow_id = $4,
                account_id = COALESCE($5, account_id),
                processed_by_workflow = TRUE,
                workflow_processed_time = $1,
                workflow_status = 'assigned'
            WHERE links_id = $6 AND link = $7
            RETURNING links_id, link, filtered, workflow_id, account_id;
        `;

        const updateResult = await pgClient.query(updateQuery, [
            new Date(),
            mongoWorkflowId,
            `${workflowType}_${workflowId}`,
            mongoWorkflowId,
            accountId,
            pgLinkId,
            linkUrl
        ]);

        if (updateResult.rowCount > 0) {
            const updatedLink = updateResult.rows[0];
            console.log(`   - ✅ PostgreSQL link updated: ID=${updatedLink.links_id}, filtered=${updatedLink.filtered}, workflow=${updatedLink.workflow_id}, account_id=${updatedLink.account_id}`);
            return {
                success: true,
                linkId: updatedLink.links_id,
                workflowId: updatedLink.workflow_id,
                accountId: updatedLink.account_id
            };
        } else {
            console.warn(`   - ⚠️ Failed to update PostgreSQL link: ${linkUrl} (ID: ${pgLinkId}, account_id: ${accountId})`);
            return { success: false };
        }
    } catch (error) {
        console.error(`   - ❌ Error updating PostgreSQL link ${linkUrl}:`, error.message);
        return { success: false, error: error.message };
    }
}

// Function to update MongoDB links_updated collection with proper associations and account info
export async function updateMongoLinksCollection(db, insertedLinks, workflowLinkAssociations) {
    console.log('💾 Updating MongoDB links_updated collection with associations and account info...');

    const linksUpdatedCollection = db.collection('links_updated');
    let updateCount = 0;
    let errorCount = 0;

    for (const [linkUrl, linkInfo] of insertedLinks) {
        try {
            const association = workflowLinkAssociations.find(assoc =>
                assoc.linkUrl === linkUrl && assoc.pgLinkId === linkInfo.linkId
            );

            const updateData = {
                link: linkUrl,
                postgres_links_id: linkInfo.linkId,
                postgres_account_id: linkInfo.accountId,
                created_at: new Date(),
                ...(association && {
                    associated_workflow_id: association.workflowId,
                    associated_workflow_type: association.workflowType,
                    mongo_workflow_id: association.mongoWorkflowId,
                    workflow_association_created: new Date()
                })
            };

            await linksUpdatedCollection.updateOne(
                { link: linkUrl },
                { $set: updateData },
                { upsert: true }
            );

            updateCount++;
            console.log(`   - ✅ MongoDB links_updated updated for link: ${linkUrl} (account_id: ${linkInfo.accountId})${association ? ` (associated with ${association.workflowType} workflow ${association.workflowId})` : ''}`);
        } catch (mongoError) {
            errorCount++;
            console.error(`   - ❌ Error updating MongoDB links_updated for link ${linkUrl}:`, mongoError.message);
        }
    }

    console.log(`📊 MongoDB links_updated summary: ${updateCount} updated, ${errorCount} errors`);
    return { updateCount, errorCount };
}

// Function to get or create default account
export async function getOrCreateDefaultAccount(pgClient, mongoClient, defaultUsername = 'default_extraction_account') {
    try {
        console.log('👤 Checking for default account...');

        // First, check if default account exists in PostgreSQL
        const accountQuery = `
            SELECT account_id, username, profile_id, mongo_object_id
            FROM accounts
            WHERE username = $1
            ORDER BY account_id
            LIMIT 1;
        `;

        const accountResult = await pgClient.query(accountQuery, [defaultUsername]);

        if (accountResult.rows.length > 0) {
            const account = accountResult.rows[0];
            console.log(`✅ Found existing default account: account_id=${account.account_id}, username=${account.username}`);
            return account.account_id;
        }

        // Account doesn't exist, create it
        console.log(`🔧 Creating default account: ${defaultUsername}`);

        const insertAccountQuery = `
            INSERT INTO accounts (username, profile_id, created_time, updated_time)
            VALUES ($1, $2, $3, $4)
            RETURNING account_id, username;
        `;

        const profileId = `profile_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
        const now = new Date();

        const insertResult = await pgClient.query(insertAccountQuery, [
            defaultUsername,
            profileId,
            now,
            now
        ]);

        if (insertResult.rows.length > 0) {
            const newAccount = insertResult.rows[0];
            console.log(`✅ Created default account: account_id=${newAccount.account_id}, username=${newAccount.username}`);

            // Optionally create corresponding MongoDB record
            try {
                if (mongoClient) {
                    const db = mongoClient.db('messages_db');
                    const accountsCollection = db.collection('accounts');

                    const mongoAccountDoc = {
                        username: defaultUsername,
                        profile_id: profileId,
                        postgres_account_id: newAccount.account_id,
                        created_at: now,
                        account_type: 'extraction_default'
                    };

                    const mongoResult = await accountsCollection.insertOne(mongoAccountDoc);
                    console.log(`✅ Created corresponding MongoDB account: ${mongoResult.insertedId}`);

                    // Update PostgreSQL with MongoDB ObjectId
                    await pgClient.query(
                        'UPDATE accounts SET mongo_object_id = $1 WHERE account_id = $2',
                        [mongoResult.insertedId.toString(), newAccount.account_id]
                    );
                    console.log(`✅ Updated PostgreSQL account with MongoDB ObjectId`);
                }
            } catch (mongoAccountError) {
                console.warn(`⚠️ Failed to create MongoDB account record:`, mongoAccountError.message);
            }

            return newAccount.account_id;
        } else {
            throw new Error('Failed to create default account');
        }

    } catch (error) {
        console.error('❌ Error getting/creating default account:', error.message);
        return null;
    }
}

// Function to validate account exists
export async function validateAccountExists(pgClient, accountId) {
    try {
        if (!accountId) {
            console.warn('⚠️ No account_id provided for validation');
            return false;
        }

        const accountQuery = 'SELECT account_id, username FROM accounts WHERE account_id = $1';
        const result = await pgClient.query(accountQuery, [accountId]);

        if (result.rows.length > 0) {
            console.log(`✅ Validated account exists: account_id=${accountId}, username=${result.rows[0].username}`);
            return true;
        } else {
            console.warn(`⚠️ Account does not exist: account_id=${accountId}`);
            return false;
        }
    } catch (error) {
        console.error('❌ Error validating account:', error.message);
        return false;
    }
}

// Function to update account statistics
export async function updateAccountStatistics(pgClient, accountId, statisticType) {
    try {
        if (!accountId) {
            console.warn('⚠️ Cannot update statistics - no account_id provided');
            return false;
        }

        const validTypes = ['links', 'replies', 'messages', 'retweets'];
        if (!validTypes.includes(statisticType)) {
            console.warn(`⚠️ Invalid statistic type: ${statisticType}`);
            return false;
        }

        const columnName = `total_${statisticType}_processed`;
        const updateQuery = `
            UPDATE accounts
            SET ${columnName} = ${columnName} + 1,
                last_workflow_sync = CURRENT_TIMESTAMP
            WHERE account_id = $1
            RETURNING ${columnName};
        `;

        const result = await pgClient.query(updateQuery, [accountId]);

        if (result.rows.length > 0) {
            console.log(`✅ Updated account statistics: account_id=${accountId}, ${columnName}=${result.rows[0][columnName]}`);
            return true;
        } else {
            console.warn(`⚠️ Failed to update statistics for account_id=${accountId}`);
            return false;
        }
    } catch (error) {
        console.error(`❌ Error updating account statistics:`, error.message);
        return false;
    }
}

// NEW: Function to get extraction statistics from extracted_urls table
export async function getExtractionStatistics(pgClient, accountId) {
    try {
        const result = await pgClient.query(
            'SELECT * FROM get_extraction_statistics($1)',
            [accountId]
        );

        if (result.rows.length > 0) {
            return result.rows[0];
        }

        return {
            total_extracted: 0,
            total_replies: 0,
            total_regular: 0,
            pending_parent_extraction: 0,
            parents_found: 0,
            parents_not_found: 0,
            moved_to_links: 0,
            pending_move_to_links: 0
        };
    } catch (error) {
        console.error('❌ Error getting extraction statistics:', error.message);
        return null;
    }
}

// NEW: Function to check if URL already exists in extracted_urls
export async function urlExistsInExtractedUrls(pgClient, accountId, url) {
    try {
        const result = await pgClient.query(
            'SELECT extracted_url_id FROM extracted_urls WHERE account_id = $1 AND url = $2',
            [accountId, url]
        );

        return result.rows.length > 0;
    } catch (error) {
        console.error('❌ Error checking if URL exists:', error.message);
        return false;
    }
}

// NEW: Function to get URLs that need parent extraction
export async function getUrlsNeedingParentExtraction(pgClient, accountId, limit = 100) {
    try {
        const result = await pgClient.query(
            'SELECT * FROM get_urls_needing_parent_extraction($1, $2)',
            [accountId, limit]
        );

        return result.rows;
    } catch (error) {
        console.error('❌ Error getting URLs needing parent extraction:', error.message);
        return [];
    }
}

// NEW: Function to mark parent extraction as attempted
export async function markParentExtractionAttempted(
    pgClient,
    extractedUrlId,
    parentFound,
    parentTweetId = null,
    parentTweetUrl = null,
    parentUrlId = null
) {
    try {
        const result = await pgClient.query(
            'SELECT mark_parent_extraction_attempted($1, $2, $3, $4, $5)',
            [extractedUrlId, parentFound, parentTweetId, parentTweetUrl, parentUrlId]
        );

        return result.rows[0].mark_parent_extraction_attempted;
    } catch (error) {
        console.error('❌ Error marking parent extraction attempted:', error.message);
        return false;
    }
}

// NEW: Function to batch move extracted URLs to links table
export async function batchMoveExtractedUrlsToLinks(pgClient, accountId, limit = 100) {
    try {
        const result = await pgClient.query(
            'SELECT * FROM batch_move_extracted_urls_to_links($1, $2)',
            [accountId, limit]
        );

        return result.rows;
    } catch (error) {
        console.error('❌ Error batch moving URLs to links table:', error.message);
        return [];
    }
}
