// modules/database/PostgreSQLService.js
import pg from 'pg';
const { Pool } = pg;

/**
 * Handles all PostgreSQL operations
 */
export class PostgreSQLService {
    constructor(config) {
        this.pool = new Pool(config);
    }

    async connect() {
        const client = await this.pool.connect();
        console.log('✓ PostgreSQL connected');
        client.release();
    }

    async close() {
        await this.pool.end();
        console.log('✓ PostgreSQL closed');
    }

    async fetchEligibleLinks() {
        const query = `
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
                AND COALESCE(l.executed, FALSE) = FALSE
            ORDER BY l.tweeted_date DESC, l.links_id ASC
        `;

        const result = await this.pool.query(query);
        console.log(`✓ Found ${result.rows.length} eligible links`);
        return result.rows;
    }

    async markLinkExecuted(linkId, success) {
        const query = `
            UPDATE links
            SET
                executed = TRUE,
                processed_by_workflow = TRUE,
                workflow_status = $1,
                workflow_processed_time = CURRENT_TIMESTAMP,
                success = $2,
                failure = $3
            WHERE links_id = $4
            RETURNING links_id, workflow_status, success, failure
        `;

        const result = await this.pool.query(query, [
            success ? 'completed' : 'failed',
            success,
            !success,
            linkId
        ]);

        console.log(`  ✅ Link ${linkId}: ${success ? 'SUCCESS' : 'FAILURE'}`);
        return result.rows[0];
    }

    async updateLinkSuccessStatus(linkId, success) {
        const query = `
            UPDATE links
            SET
                success = $1,
                failure = $2,
                workflow_status = $3,
                workflow_processed_time = CURRENT_TIMESTAMP
            WHERE links_id = $4
            RETURNING links_id, success, failure
        `;

        const result = await this.pool.query(query, [
            success,
            !success,
            success ? 'completed' : 'failed',
            linkId
        ]);

        if (result.rows.length > 0) {
            console.log(`  ✅ PostgreSQL: Link ${linkId} - success=${result.rows[0].success}, failure=${result.rows[0].failure}`);
            return result.rows[0];
        }
        return null;
    }

    async updateLinkFailureStatus(linkId, errorMessage) {
        const query = `
            UPDATE links
            SET
                success = FALSE,
                failure = TRUE,
                workflow_status = 'failed',
                workflow_processed_time = CURRENT_TIMESTAMP
            WHERE links_id = $1
            RETURNING links_id, success, failure
        `;

        const result = await this.pool.query(query, [linkId]);

        if (result.rows.length > 0) {
            console.log(`  ❌ PostgreSQL: Link ${linkId} - marked as failure`);
            return result.rows[0];
        }
        return null;
    }

    async findInconsistentSuccessFailure() {
        const query = `
            SELECT links_id, link, success, failure
            FROM links
            WHERE (success = TRUE AND failure = TRUE)
            OR (success = FALSE AND failure = FALSE AND executed = TRUE)
            ORDER BY links_id
        `;

        const result = await this.pool.query(query);
        return result.rows;
    }

    async getExecutionStatistics() {
        const query = `
            SELECT
                COUNT(*) as total_links,
                COUNT(CASE WHEN executed = TRUE THEN 1 END) as executed_links,
                COUNT(CASE WHEN success = TRUE THEN 1 END) as successful_links,
                COUNT(CASE WHEN failure = TRUE THEN 1 END) as failed_links,
                ROUND(
                    COUNT(CASE WHEN success = TRUE THEN 1 END)::DECIMAL /
                    NULLIF(COUNT(CASE WHEN executed = TRUE THEN 1 END), 0) * 100, 2
                ) as success_rate
            FROM links
            WHERE executed = TRUE
        `;

        const result = await this.pool.query(query);
        return result.rows[0];
    }

    async getExecutionSummary() {
        const query = `
            SELECT
                COUNT(*) as total_links,
                SUM(CASE WHEN executed = TRUE THEN 1 ELSE 0 END) as executed_count,
                SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END) as success_count,
                SUM(CASE WHEN failure = TRUE THEN 1 ELSE 0 END) as failure_count,
                ROUND(
                    SUM(CASE WHEN success = TRUE THEN 1 ELSE 0 END)::DECIMAL /
                    NULLIF(SUM(CASE WHEN executed = TRUE THEN 1 ELSE 0 END), 0) * 100, 2
                ) as success_rate
            FROM links
        `;

        const result = await this.pool.query(query);
        return result.rows[0] || {};
    }
}
