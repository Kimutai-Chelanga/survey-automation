// config/environment.js
// Environment configuration loader from MongoDB

import DatabaseManager from './database.js';

export class Environment {
    constructor() {
        this.config = null;
        this.dbManager = null;
    }

    async loadConfiguration() {
        try {
            console.log('Loading configuration from MongoDB...');
            
            // Initialize database connection
            this.dbManager = new DatabaseManager({
                mongodb: {
                    uri: process.env.MONGODB_URI || 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin',
                    dbName: process.env.MONGODB_DB_NAME || 'messages_db'
                }
            });
            
            await this.dbManager.connect();
            console.log('✓ Database connected for configuration loading');

            // Load configuration from MongoDB settings
            const config = await this.loadFromMongoDB();
            
            // Merge with default configuration
            const finalConfig = {
                ...this.getDefaultConfig(),
                ...config
            };

            console.log('Configuration loaded successfully from MongoDB');
            return finalConfig;

        } catch (error) {
            console.error('Failed to load configuration from MongoDB:', error.message);
            // Fall back to default configuration
            console.log('Falling back to default configuration');
            return this.getDefaultConfig();
        } finally {
            if (this.dbManager) {
                await this.dbManager.disconnect();
            }
        }
    }

    async loadFromMongoDB() {
        try {
            // Get hyperbrowser configuration from settings collection
            const settings = await this.dbManager.db.collection('settings').findOne({
                'category': 'hyperbrowser_configuration'
            });

            if (!settings || !settings.settings) {
                console.log('No hyperbrowser configuration found in MongoDB, using defaults');
                return {};
            }

            const config = {
                hyperbrowser: {
                    apiKey: process.env.HYPERBROWSER_API_KEY || '', // Still need API key from env
                    baseUrl: settings.settings.base_url || 'https://api.hyperbrowser.ai', // Added base URL :cite[2]
                    maxSteps: settings.settings.max_steps || 25,
                    defaultTimeout: settings.settings.default_timeout || 30000,
                    sessionConfig: {
                        screen: settings.settings.screen_config || { width: 1920, height: 1080 },
                        use_stealth: settings.settings.use_stealth !== false,
                        browser_type: settings.settings.browser_type || 'chrome',
                        start_url: settings.settings.start_url || 'chrome://newtab/',
                        enableWebRecording: settings.settings.enable_web_recording !== false,
                        enableVideoWebRecording: settings.settings.enable_video_recording !== false
                    }
                },
                execution: {
                    dagRunId: process.env.AIRFLOW_CTX_DAG_RUN_ID || 'manual_run',
                    executionDate: process.env.AIRFLOW_CTX_EXECUTION_DATE || new Date().toISOString(),
                    taskInstanceKey: process.env.AIRFLOW_CTX_TASK_ID || 'manual_execution',
                    workflowGapSeconds: settings.settings.workflow_gap_seconds || 15,
                    maxConcurrentWorkflows: settings.settings.max_concurrent_workflows || 1
                },
                logging: {
                    level: process.env.LOG_LEVEL || 'info',
                    enableDebug: process.env.NODE_ENV === 'development'
                }
            };

            // Add active profile and extension from settings
            if (settings.settings.active_profile_id) {
                config.hyperbrowser.activeProfileId = settings.settings.active_profile_id;
            }
            
            if (settings.settings.active_extension_id) {
                config.hyperbrowser.activeExtensionId = settings.settings.active_extension_id;
            }

            return config;

        } catch (error) {
            console.error('Error loading from MongoDB:', error.message);
            return {};
        }
    }

    getDefaultConfig() {
        return {
            hyperbrowser: {
                apiKey: process.env.HYPERBROWSER_API_KEY || '',
                baseUrl: 'https://api.hyperbrowser.ai', // Default base URL :cite[2]
                maxSteps: 25,
                defaultTimeout: 30000,
                sessionConfig: {
                    screen: { width: 1920, height: 1080 },
                    use_stealth: true,
                    browser_type: 'chrome',
                    start_url: 'chrome://newtab/',
                    enableWebRecording: true,
                    enableVideoWebRecording: true
                }
            },
            mongodb: {
                uri: process.env.MONGODB_URI || 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin',
                dbName: process.env.MONGODB_DB_NAME || 'messages_db'
            },
            execution: {
                dagRunId: process.env.AIRFLOW_CTX_DAG_RUN_ID || 'manual_run',
                executionDate: process.env.AIRFLOW_CTX_EXECUTION_DATE || new Date().toISOString(),
                taskInstanceKey: process.env.AIRFLOW_CTX_TASK_ID || 'manual_execution',
                workflowGapSeconds: 15,
                maxConcurrentWorkflows: 1
            },
            logging: {
                level: process.env.LOG_LEVEL || 'info',
                enableDebug: process.env.NODE_ENV === 'development'
            }
        };
    }

    validateConfiguration(config) {
        const required = [
            { key: 'hyperbrowser.apiKey', name: 'HYPERBROWSER_API_KEY' },
            { key: 'mongodb.uri', name: 'MONGODB_URI' }
        ];

        const missing = [];

        for (const { key, name } of required) {
            const value = this.getNestedValue(config, key);
            if (!value) {
                missing.push(name);
            }
        }

        if (missing.length > 0) {
            throw new Error(`Missing required configuration: ${missing.join(', ')}`);
        }

        console.log('Configuration validation passed');
    }

    getNestedValue(obj, path) {
        return path.split('.').reduce((current, key) => {
            return current && typeof current === 'object' ? current[key] : undefined;
        }, obj);
    }

    async get() {
        if (!this.config) {
            this.config = await this.loadConfiguration();
            this.validateConfiguration(this.config);
        }
        return this.config;
    }
}

export default Environment;