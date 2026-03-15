// modules/workflow/WorkflowFetcher.js

/**
 * FIX HISTORY (2026-02-20 v5) — Added max_workflows_to_process support
 *   - Reads max_workflows_to_process from execution_config (0 = all workflows)
 *   - When > 0, fetches only the latest X workflows based on _id timestamp
 *   - Preserves MongoDB natural order (oldest first) unless overridden
 *
 * FIX HISTORY (2026-02-19 v4) — Replaced total_execution_budget_ms with wait_margin_ms:
 *   total_execution_budget_ms has been removed. Bottom-up block analysis now always runs.
 *   getExecutionSettings() reads and returns wait_margin_ms (the safety buffer added on top
 *   of the analysed estimate). Stored as execution_config.wait_margin_ms in MongoDB.
 *   Default is 300 000 ms (5 min) if not set.
 *
 * FIX HISTORY (2026-02-19 v3) — Pass total_execution_budget_ms [REPLACED by v4]
 *
 * FIX HISTORY (2026-02-17 v2) — CRITICAL: link_url missing from combinations
 *
 *   ROOT CAUSE:
 *     Step 5 built combination objects with `link: link.link` but never set
 *     `link_url`. OrchestratorBuilder.buildOrchestrator() Step 3 checks
 *     `wf.link_url` — it was always undefined → ALL combinations skipped →
 *     "All combinations are missing automa_workflow_id or link_url" fatal error.
 *
 *   FIX:
 *     Added `link_url: link.link` to every combination pushed in Step 5.
 *     Also added all content/text fields the sub-workflows need at runtime:
 *       - content, tweet_content, reply_text, tweet_text  (generated text)
 *       - postgres_content_id   (= links_id, needed for DB tracking)
 *       - tweet_url             (full tweet URL, if present on the link row)
 *     These flow into OrchestratorBuilder.buildWorkflowGlobalData() which
 *     injects them into each execute-workflow block's globalData so that
 *     Automa's {{link_url}}, {{tweet_content}} etc. resolve correctly.
 *
 * FIX HISTORY (2026-02-17 v1):
 *   getExecutionSettings() now includes delay_min_milliseconds and
 *   delay_max_milliseconds in the returned execSettings object so that
 *   dynamic-orchestrator.js → OrchestratorBuilder can use the exact values
 *   saved by the Streamlit UI.
 *
 * FIX (2026-02-15):
 *   Field name mismatch between Streamlit and WorkflowFetcher.
 *   Streamlit saves workflow_category / workflow_type; old code read
 *   destination_category / workflow_type_name. Both variants now accepted.
 */

export class WorkflowFetcher {
    constructor(mongoDBService, pgService, config = null) {
        this.mongoDBService = mongoDBService;
        this.pgService      = pgService;
        this.config         = config;
    }

    /**
     * Get execution settings from MongoDB for the current day.
     * Returns all fields from execution_config, including delay_min/max_milliseconds,
     * wait_margin_ms, and max_workflows_to_process.
     */
    async getExecutionSettings() {
        try {
            let executionDate;
            if (process.env.AIRFLOW_CTX_EXECUTION_DATE) {
                executionDate = new Date(process.env.AIRFLOW_CTX_EXECUTION_DATE);
                console.log(`📅 Using Airflow execution date: ${executionDate.toISOString()}`);
            } else {
                executionDate = new Date();
                console.log(`📅 Using current system date: ${executionDate.toISOString()}`);
            }

            const timezone   = this.config?.timezone || 'Africa/Nairobi';
            const currentDay = executionDate.toLocaleDateString('en-US', {
                weekday:  'long',
                timeZone: timezone,
            }).toLowerCase();

            console.log(`📅 Fetching execution settings for: ${currentDay} (timezone: ${timezone})`);

            const settingsDoc = await this.mongoDBService.db
                .collection('settings')
                .findOne({ category: 'system' });

            if (!settingsDoc?.settings) {
                console.error('❌ No system settings found in database');
                console.error('   SOLUTION: Configure settings in Streamlit UI → Settings → Execution Configuration');
                return null;
            }

            const weeklySettings = settingsDoc.settings.weekly_workflow_settings;
            if (!weeklySettings) {
                console.error('❌ No weekly_workflow_settings found in system settings');
                console.error('   Available settings keys:', Object.keys(settingsDoc.settings));
                console.error('   SOLUTION: Go to Streamlit UI → Settings → Execution Configuration');
                return null;
            }

            const dayConfig = weeklySettings[currentDay];
            if (!dayConfig) {
                console.warn(`⚠️ No configuration found for ${currentDay}`);
                console.warn('   Available days:', Object.keys(weeklySettings));
                console.warn('   SOLUTION: Configure this day in Streamlit UI → Settings → Execution Configuration');
                return null;
            }

            const execConfig = dayConfig.execution_config;
            if (!execConfig) {
                console.warn(`⚠️ No execution_config found for ${currentDay}`);
                console.warn('   Day config keys:', Object.keys(dayConfig));
                console.warn('   SOLUTION: Go to Streamlit UI → Settings → Execution Configuration');
                console.warn(`   Select ${currentDay} and save execution configuration`);
                return null;
            }

            const DEFAULT_WAIT_MARGIN_MS = 300_000; // 5 min fallback
            const DEFAULT_MAX_WORKFLOWS  = 0; // 0 = process all available

            const execSettings = {
                // ── Target identification ────────────────────────────────────
                destination_category: execConfig.destination_category
                                   || execConfig.workflow_category
                                   || '',
                workflow_type_name:   execConfig.workflow_type_name
                                   || execConfig.workflow_type
                                   || '',
                collection_name:      execConfig.collection_name  || '',

                // ── NEW: Workflow limit (0 = process all available) ─────────
                max_workflows_to_process: execConfig.max_workflows_to_process != null
                    ? parseInt(execConfig.max_workflows_to_process, 10)
                    : DEFAULT_MAX_WORKFLOWS,

                // ── Delay range (passed through to OrchestratorBuilder) ──────
                delay_min_milliseconds: execConfig.delay_min_milliseconds != null
                    ? parseInt(execConfig.delay_min_milliseconds, 10)
                    : null,
                delay_max_milliseconds: execConfig.delay_max_milliseconds != null
                    ? parseInt(execConfig.delay_max_milliseconds, 10)
                    : null,

                // ── Wait margin (safety buffer added on top of bottom-up estimate) ──
                // User sets this in Streamlit → Execution Configuration → ⚙️ Execution Settings.
                // OrchestratorBuilder passes it through to _timing.wait_margin_ms.
                // dynamic-orchestrator.js reads it from there and adds it to the
                // bottom-up estimated_total_ms to get the final sleep duration.
                // Bottom-up block analysis is ALWAYS performed — this is not an override.
                wait_margin_ms: execConfig.wait_margin_ms != null
                    ? parseInt(execConfig.wait_margin_ms, 10)
                    : DEFAULT_WAIT_MARGIN_MS,

                // ── Legacy field kept for backward compatibility ──────────────
                gap_seconds: execConfig.gap_seconds || null,

                // ── Metadata ─────────────────────────────────────────────────
                day:         currentDay,
                config_date: execConfig.config_date || '',
                enabled:     execConfig.enabled !== false,
            };

            if (!execSettings.destination_category || !execSettings.workflow_type_name) {
                console.error('❌ Invalid execution settings — missing category or workflow_type_name');
                console.error('   Execution config (raw):', JSON.stringify(execConfig, null, 2));
                console.error('   Resolved category     :', execSettings.destination_category || 'MISSING');
                console.error('   Resolved workflow type:', execSettings.workflow_type_name   || 'MISSING');
                console.error('   Current day checked   :', currentDay);
                console.error('   Timezone used         :', timezone);
                console.error('   SOLUTION: Go to Streamlit → Settings → Execution Configuration');
                return null;
            }

            if (execSettings.delay_min_milliseconds == null || execSettings.delay_max_milliseconds == null) {
                console.warn('⚠️  delay_min/max_milliseconds not set in execution_config.');
                if (execSettings.gap_seconds) {
                    console.warn(`   Legacy gap_seconds=${execSettings.gap_seconds} will be used as fallback.`);
                } else {
                    console.warn('   OrchestratorBuilder will use its hardcoded default (10 000 – 20 000 ms).');
                }
            }

            console.log('✅ Execution settings loaded:');
            console.log(`   Day              : ${currentDay}`);
            console.log(`   Config Date      : ${execSettings.config_date}`);
            console.log(`   Category         : ${execSettings.destination_category}`);
            console.log(`   Type             : ${execSettings.workflow_type_name}`);
            console.log(`   Collection       : ${execSettings.collection_name || 'All'}`);
            console.log(`   Max Workflows    : ${execSettings.max_workflows_to_process === 0 ? 'All (unlimited)' : execSettings.max_workflows_to_process}`);
            console.log(`   Delay min        : ${execSettings.delay_min_milliseconds != null ? execSettings.delay_min_milliseconds + ' ms' : '(not set)'}`);
            console.log(`   Delay max        : ${execSettings.delay_max_milliseconds != null ? execSettings.delay_max_milliseconds + ' ms' : '(not set)'}`);
            console.log(`   Wait margin      : ${(execSettings.wait_margin_ms / 1000).toFixed(0)}s (${execSettings.wait_margin_ms.toLocaleString()} ms) — block analysis always runs`);
            if (execSettings.gap_seconds) {
                console.log(`   Gap (legacy)     : ${execSettings.gap_seconds}s`);
            }

            return execSettings;

        } catch (error) {
            console.error('❌ Error fetching execution settings:', error.message);
            console.error(error.stack);
            return null;
        }
    }

    /**
     * Fetch eligible workflows based on execution settings.
     * Returns { combinations, settings } or { combinations: [], settings: null }.
     */
    async fetchEligibleWorkflows() {
        console.log('\n' + '='.repeat(80));
        console.log('FETCHING ELIGIBLE WORKFLOWS FOR EXECUTION');
        console.log('='.repeat(80) + '\n');

        try {
            // Step 1: Get execution settings
            const execSettings = await this.getExecutionSettings();
            if (!execSettings) {
                console.log('❌ No valid execution settings found for today');
                return { combinations: [], settings: null };
            }

            console.log('📋 Execution Settings:');
            console.log(`   Day              : ${execSettings.day}`);
            console.log(`   Config Date      : ${execSettings.config_date}`);
            console.log(`   Category         : ${execSettings.destination_category}`);
            console.log(`   Workflow Type    : ${execSettings.workflow_type_name}`);
            console.log(`   Collection       : ${execSettings.collection_name || 'All'}`);
            console.log(`   Max Workflows    : ${execSettings.max_workflows_to_process === 0 ? 'All (unlimited)' : execSettings.max_workflows_to_process}`);
            console.log(`   Delay Range      : ${
                execSettings.delay_min_milliseconds != null
                    ? `${execSettings.delay_min_milliseconds}–${execSettings.delay_max_milliseconds} ms`
                    : execSettings.gap_seconds
                        ? `${execSettings.gap_seconds}s (legacy gap_seconds)`
                        : '(not set — will use fallback)'
            }`);
            console.log(`   Wait margin      : ${(execSettings.wait_margin_ms / 1000).toFixed(0)}s — added on top of bottom-up analysis`);
            console.log('');

            // Step 2: Build MongoDB filter
            const mongoFilter = {
                category:      execSettings.destination_category.toLowerCase(),
                workflow_type: execSettings.workflow_type_name.toLowerCase(),
                has_link:      true,
                has_content:   true,
                status:        'ready_to_execute',
                executed:      false,
            };

            if (execSettings.collection_name) {
                mongoFilter.collection_name = execSettings.collection_name;
            }

            console.log('🔍 MongoDB Query Filter:', JSON.stringify(mongoFilter, null, 2));
            console.log('');

            // Step 3: Fetch eligible links from PostgreSQL
            console.log('📊 Querying PostgreSQL for eligible links...');
            const links = await this.pgService.fetchEligibleLinks();

            if (links.length === 0) {
                console.log('ℹ️ No eligible links found in PostgreSQL');
                console.log('   Run the filter_links DAG first to assign workflows to links.');
                return { combinations: [], settings: execSettings };
            }

            console.log(`✓ Found ${links.length} eligible links`);

            // Step 4: Get workflow assignments from MongoDB
            const linkIds = links.map(row => row.links_id);
            console.log(`🔍 Searching for workflow assignments for ${linkIds.length} link(s)...`);

            const workflowAssignments = await this.mongoDBService.getWorkflowAssignments(
                linkIds,
                mongoFilter
            );

            console.log(`✓ Retrieved ${workflowAssignments.length} workflow assignments`);

            if (workflowAssignments.length === 0) {
                console.log('');
                console.log('⚠️ WARNING: No workflow assignments found matching your filters!');
                console.log('');
                console.log('Possible reasons:');
                console.log('1. Wrong category/type/collection in execution config');
                console.log('2. Workflows exist but don\'t match execution filters');
                console.log('3. All matching workflows already executed');
                console.log('4. Filter config and execution config have different settings');
                console.log('');
                return { combinations: [], settings: execSettings };
            }

            // Step 5: Combine links with workflow assignments
            console.log('🔗 Combining links with workflow assignments...');
            const eligibleCombinations = [];

            for (const link of links) {
                const assignments = workflowAssignments.filter(
                    a => a.postgres_content_id === link.links_id
                );
                for (const assignment of assignments) {
                    eligibleCombinations.push({
                        // ── Link identity ──────────────────────────────────
                        links_id:            link.links_id,
                        postgres_content_id: link.links_id,   // alias used by OrchestratorBuilder

                        // ── THE FIX: map link.link → link_url ──────────────
                        link_url:            link.link,        // ← was missing; caused all combos to be skipped
                        link:                link.link,        // kept for backward compatibility

                        // ── Content fields (injected into sub-workflow globalData) ──
                        tweet_content:       link.tweet_content || link.content || null,
                        content:             link.content       || link.tweet_content || null,
                        reply_text:          link.reply_text    || null,
                        tweet_text:          link.tweet_text    || null,
                        tweet_url:           link.tweet_url     || null,

                        // ── Source tweet metadata ──────────────────────────
                        tweet_id:            link.tweet_id      || null,
                        tweeted_date:        link.tweeted_date  || null,
                        tweeted_time:        link.tweeted_time  || null,

                        // ── Workflow assignment fields ──────────────────────
                        workflow_type:       assignment.workflow_type,
                        workflow_name:       assignment.workflow_name,
                        automa_workflow_id:  assignment.automa_workflow_id,
                        account_id:          assignment.account_id         || null,
                        metadata_id:         assignment.metadata_id        || null,
                        category:            assignment.category,
                        collection_name:     assignment.collection_name,
                        database_name:       assignment.database_name      || 'execution_workflows',
                        
                        // ── Timestamp for sorting (latest first) ──────────
                        _id:                 assignment._id,                // MongoDB ObjectId contains timestamp
                    });
                }
            }

            console.log(`✓ Created ${eligibleCombinations.length} link-workflow combination(s)`);

            // Step 6: Apply max_workflows limit - get LATEST workflows first
            let limitedCombinations = eligibleCombinations;
            
            if (execSettings.max_workflows_to_process > 0 && eligibleCombinations.length > execSettings.max_workflows_to_process) {
                // Sort by _id descending to get latest first (ObjectId contains timestamp)
                // This ensures we execute the most recent workflows when limiting
                const sortedByLatest = [...eligibleCombinations].sort((a, b) => {
                    // If _id is available, compare ObjectId timestamps (newer first)
                    if (a._id && b._id) {
                        return b._id.getTimestamp() - a._id.getTimestamp();
                    }
                    // Fallback to string comparison
                    return String(b._id || '').localeCompare(String(a._id || ''));
                });
                
                limitedCombinations = sortedByLatest.slice(0, execSettings.max_workflows_to_process);
                console.log(`⚠️ Limited to ${execSettings.max_workflows_to_process} LATEST workflows (from ${eligibleCombinations.length} total)`);
            } else if (execSettings.max_workflows_to_process === 0) {
                console.log(`📊 Processing ALL ${eligibleCombinations.length} workflows (unlimited)`);
            } else {
                console.log(`📊 Processing all ${eligibleCombinations.length} workflows (less than limit ${execSettings.max_workflows_to_process})`);
            }

            console.log('');
            console.log('='.repeat(80));
            console.log(`📊 FINAL RESULT: ${limitedCombinations.length} workflows will be executed`);
            if (execSettings.max_workflows_to_process > 0) {
                console.log(`   (limited to ${execSettings.max_workflows_to_process} latest workflows)`);
            }
            console.log('='.repeat(80));
            console.log('');

            return {
                combinations: limitedCombinations,
                settings:     execSettings,
            };

        } catch (error) {
            console.error('❌ Error fetching eligible workflows:', error.message);
            console.error(error.stack);
            throw error;
        }
    }
}
