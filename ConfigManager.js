// modules/config/ConfigManager.js
import { promises as fsp } from 'fs';
import path from 'path';
import { promisify } from 'util';
import { exec } from 'child_process';

const execAsync = promisify(exec);

/**
 * Manages configuration, environment variables, and system requirements
 */
class ConfigManager {
    constructor() {
        // MongoDB Configuration
        this.mongoUri = process.env.MONGODB_URI || 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin';
        this.dbName = process.env.MONGODB_DB_NAME || 'messages_db';

        // PostgreSQL Configuration
        this.pgConfig = {
            host: process.env.POSTGRES_HOST || 'postgres',
            port: parseInt(process.env.POSTGRES_PORT || '5432'),
            database: process.env.POSTGRES_DB || 'messages',
            user: process.env.POSTGRES_USER || 'airflow',
            password: process.env.POSTGRES_PASSWORD || 'airflow'
        };

        // Chrome Configuration
        this.baseProfileDir = process.env.CHROME_PROFILE_DIR || '/workspace/chrome_profiles';
        this.chromeExecutable = process.env.CHROME_EXECUTABLE || '/usr/bin/google-chrome-stable';
        this.recordingsDir = process.env.RECORDINGS_DIR || '/workspace/recordings';
        
        // ✅ FIXED: Use workspace downloads (Docker-safe, never /root)
        this.downloadsDir = process.env.DOWNLOADS_DIR || '/workspace/downloads';
        
        this.debugPort = 9222;
        this.displayNum = 99;
        this.extensionId = 'infppggnoaenmfagbfknfkancpbljcca';

        // Timezone Configuration
        this.timezone = process.env.TZ || 'Africa/Nairobi';

        // Airflow Context
        this.dagRunId = process.env.AIRFLOW_CTX_DAG_RUN_ID || 'manual_run';
        this.executionDate = process.env.AIRFLOW_CTX_EXECUTION_DATE || new Date().toISOString();
        this.taskId = process.env.AIRFLOW_CTX_TASK_ID || 'local_workflow_executor';

        // Feature Flags - Success/Failure Tracking
        this.trackSuccessFailure = process.env.TRACK_SUCCESS_FAILURE !== 'false';
        this.updatePostgresSuccess = process.env.UPDATE_POSTGRES_SUCCESS !== 'false';
        this.updatePostgresFailure = process.env.UPDATE_POSTGRES_FAILURE !== 'false';
        this.sendSuccessStatsToMongo = process.env.SEND_SUCCESS_STATS_TO_MONGO !== 'false';
        this.validateDataConsistency = process.env.VALIDATE_DATA_CONSISTENCY !== 'false';

        // Debug mode
        this.debugMode = process.env.DEBUG_MODE === 'true';
    }

    /**
     * Initialize directories and verify system requirements
     */
    async initialize() {
        console.log('\n' + '='.repeat(80));
        console.log('INITIALIZING CONFIGURATION');
        console.log('='.repeat(80));

        await this.ensureDirectories();
        await this.checkFFmpeg();

        this.logConfiguration();

        console.log('✓ Configuration initialized');
        console.log('='.repeat(80) + '\n');
    }

    /**
     * Ensure required directories exist
     */
    async ensureDirectories() {
        await fsp.mkdir(this.baseProfileDir, { recursive: true });
        await fsp.mkdir(this.recordingsDir, { recursive: true });
        await fsp.mkdir(this.downloadsDir, { recursive: true });
        console.log('✓ Directories ready');
    }

    /**
     * Check if FFmpeg is available
     */
    async checkFFmpeg() {
        try {
            const { stdout } = await execAsync('ffmpeg -version');
            console.log('✓ FFmpeg available:', stdout.split('\n')[0]);
        } catch (error) {
            console.warn('⚠ FFmpeg not found - video recording will be disabled');
        }
    }

    /**
     * Log current configuration
     */
    logConfiguration() {
        console.log('Configuration:');
        console.log(`  DAG Run ID: ${this.dagRunId}`);
        console.log(`  Chrome Profile Dir: ${this.baseProfileDir}`);
        console.log(`  Recordings Dir: ${this.recordingsDir}`);
        console.log(`  Downloads Dir: ${this.downloadsDir}`);
        console.log(`  Debug Port: ${this.debugPort}`);
        console.log(`  Display: :${this.displayNum}`);
        console.log(`  Timezone: ${this.timezone}`);
        console.log(`  Track Success/Failure: ${this.trackSuccessFailure}`);
        console.log(`  Update Postgres Success: ${this.updatePostgresSuccess}`);
        console.log(`  Update Postgres Failure: ${this.updatePostgresFailure}`);
        console.log(`  Send Stats to Mongo: ${this.sendSuccessStatsToMongo}`);
        console.log(`  Validate Consistency: ${this.validateDataConsistency}`);
    }

    /**
     * Get profile path for a username
     */
    getProfilePath(username) {
        return path.join(this.baseProfileDir, `account_${username}`);
    }

    /**
     * Get current day name in the configured timezone
     * This ensures consistency with Streamlit UI day detection
     */
    getCurrentDay() {
        const now = new Date();
        return now.toLocaleDateString('en-US', {
            weekday: 'long',
            timeZone: this.timezone
        }).toLowerCase();
    }

    /**
     * Get formatted date in the configured timezone
     */
    getFormattedDate(date = new Date()) {
        return date.toLocaleDateString('en-US', {
            year: 'numeric',
            month: '2-digit',
            day: '2-digit',
            timeZone: this.timezone
        });
    }

    /**
     * Get formatted time in the configured timezone
     */
    getFormattedTime(date = new Date()) {
        return date.toLocaleTimeString('en-US', {
            hour: '2-digit',
            minute: '2-digit',
            second: '2-digit',
            timeZone: this.timezone
        });
    }
}

export { ConfigManager };
