#!/usr/bin/env python3
from pymongo import MongoClient
from streamlit.ui.settings.settings import DEFAULT_SETTINGS
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import json
from typing import Dict, Any  # Added missing imports
import logging  # Added missing import

# Initialize logger
logger = logging.getLogger(__name__)

class DAGConfig:
    @staticmethod
    def get_dag_config(dag_id):
        """Retrieve configuration for a specific DAG from MongoDB or defaults."""
        client = None
        try:
            client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']), 
                               serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db = client['messages_db']
            settings_doc = db['settings'].find_one({'category': 'system'})
            if settings_doc and 'settings' in settings_doc:
                config = settings_doc['settings'].get(dag_id, {})
                if config:
                    return config
            return DEFAULT_SETTINGS.get(dag_id, {})
        except Exception as e:
            print(f"Error retrieving DAG config for {dag_id} from MongoDB: {e}")
            return DEFAULT_SETTINGS.get(dag_id, {})
        finally:
            if client:
                client.close()
    
    

    

    @staticmethod
    def get_workflow_strategy():
        """Get workflow strategy from MongoDB settings."""
        client = None
        try:
            client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']), 
                               serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db = client['messages_db']
            settings_doc = db['settings'].find_one({'category': 'system'})
            if settings_doc and 'settings' in settings_doc:
                strategy_settings = settings_doc['settings'].get('workflow_strategy_settings', {})
                return (
                    strategy_settings.get('strategy', 'all'),
                    strategy_settings.get('custom_order', 'messages,replies,retweets')
                )
            return 'all', 'messages,replies,retweets'
        except Exception as e:
            print(f"Error retrieving workflow strategy from MongoDB: {e}")
            return 'all', 'messages,replies,retweets'
        finally:
            if client:
                client.close()

    @staticmethod
    def get_execution_config():
        """Get execution configuration from MongoDB settings."""
        client = None
        try:
            client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']), 
                               serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            db = client['messages_db']
            settings_doc = db['settings'].find_one({'category': 'system'})
            if settings_doc and 'settings' in settings_doc:
                strategy_settings = settings_doc['settings'].get('workflow_strategy_settings', {})
                return {
                    'trigger_after_upload': strategy_settings.get('trigger_after_upload', False),
                    'trigger_delay_seconds': float(strategy_settings.get('trigger_delay_seconds', 5.0)),
                    'batch_size': int(strategy_settings.get('batch_size', 1)),
                    'interval_between_batches': float(strategy_settings.get('interval_between_batches', 3.0)),
                    'max_workflows_per_run': int(strategy_settings.get('max_workflows_per_run', 50))
                }
            return {
                'trigger_after_upload': False,
                'trigger_delay_seconds': 5.0,
                'batch_size': 1,
                'interval_between_batches': 3.0,
                'max_workflows_per_run': 50
            }
        except Exception as e:
            print(f"Error retrieving execution config from MongoDB: {e}")
            return {
                'trigger_after_upload': False,
                'trigger_delay_seconds': 5.0,
                'batch_size': 1,
                'interval_between_batches': 3.0,
                'max_workflows_per_run': 50
            }
        finally:
            if client:
                client.close()

    
   
    
    @staticmethod
    def get_processing_config() -> Dict[str, Any]:
        """Get processing configuration based on daily content config"""
        try:
            from streamlit.ui.settings.settings_manager import get_daily_content_config
            
            # Get daily configuration
            daily_config = get_daily_content_config()
            
            workflow_type = daily_config.get('workflow_type', 'messages')
            content_amount = daily_config.get('content_amount', 5)
            enabled_types = daily_config.get('enabled_types', {'messages': True, 'replies': False, 'retweets': False})
            
            config = {
                # Daily configuration
                'workflow_type': workflow_type,
                'content_amount': content_amount,
                'enabled_types': enabled_types,
                'day_name': daily_config.get('day_name', 'unknown'),
                'day_enabled': daily_config.get('enabled', True),
                
                # Individual amounts based on workflow type
                'num_messages': content_amount if workflow_type == 'messages' else 0,
                'num_replies': content_amount if workflow_type == 'replies' else 0,
                'num_retweets': content_amount if workflow_type == 'retweets' else 0,
                
                # Account-based settings
                'enable_account_based_generation': True,
                'require_account_prompts': True,
                'skip_accounts_without_prompts': True,
                'create_default_prompts_if_missing': False,
                
                # Content generation settings
                'batch_size_per_account': 50,
                'max_accounts_per_run': 20,
                'enable_parallel_account_processing': False,
                
                # Quality control
                'min_content_length': 10,
                'max_content_length': 500,
                'content_validation_enabled': True,
                'duplicate_detection_enabled': True,
                
                # Retry settings
                'retry_failed_generations': True,
                'max_retries_per_account': 2,
                'retry_delay_seconds': 30,
                
                # Logging and monitoring
                'detailed_logging_enabled': True,
                'track_generation_metrics': True,
                'log_prompt_usage': True,
            }
            
            logger.info(f"Processing config loaded - Daily: {workflow_type} x {content_amount}")
            return config
            
        except Exception as e:
            logger.error(f"Error loading processing config: {e}")
            # Return safe defaults
            return {
                'workflow_type': 'messages',
                'content_amount': 5,
                'enabled_types': {'messages': True, 'replies': False, 'retweets': False},
                'num_messages': 5,
                'num_replies': 0,
                'num_retweets': 0,
                'enable_account_based_generation': True,
                'require_account_prompts': True,
                'skip_accounts_without_prompts': True,
            }
    
    @staticmethod 
    def update_processing_config(config: Dict[str, Any]) -> bool:
        """Update processing configuration - now handles daily content config"""
        try:
            from streamlit.ui.settings.settings_manager import update_system_setting
            
            # This method now updates the weekly workflow settings
            # or create_content_settings instead of a unified processing config
            
            if 'workflow_type' in config and 'content_amount' in config:
                # Update create_content_settings
                create_content_settings = get_system_setting('create_content_settings', {})
                create_content_settings.update({
                    'workflow_type': config['workflow_type'],
                    f'number_of_{config["workflow_type"]}': config['content_amount'],
                    'enabled_types': config.get('enabled_types', {'messages': True, 'replies': False, 'retweets': False})
                })
                update_system_setting('create_content_settings', create_content_settings)
                logger.info(f"Updated create_content_settings: {config['workflow_type']} x {config['content_amount']}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error updating processing config: {e}")
            return False
    
    @staticmethod
    def get_account_generation_stats() -> Dict[str, Any]:
        """Get statistics about account-based content generation"""
        try:
            from core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor
            from streamlit.ui.settings.settings_manager import get_daily_content_config
            
            stats = {}
            daily_config = get_daily_content_config()
            
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Get account count
                    cursor.execute("SELECT COUNT(*) as total_accounts FROM accounts")
                    stats['total_accounts'] = cursor.fetchone()['total_accounts']
                    
                    # Get accounts with prompts for today's workflow type
                    workflow_type = daily_config.get('workflow_type', 'messages')
                    cursor.execute("""
                        SELECT COUNT(DISTINCT account_id) as accounts_with_prompts
                        FROM prompts 
                        WHERE is_active = TRUE AND prompt_type = %s
                    """, (workflow_type,))
                    stats['accounts_with_prompts'] = cursor.fetchone()['accounts_with_prompts']
                    
                    # Get prompt counts by type
                    cursor.execute("""
                        SELECT 
                            prompt_type,
                            COUNT(*) as count,
                            COUNT(DISTINCT account_id) as accounts_count
                        FROM prompts 
                        WHERE is_active = TRUE
                        GROUP BY prompt_type
                    """)
                    
                    prompt_stats = cursor.fetchall()
                    stats['prompt_breakdown'] = {row['prompt_type']: {
                        'total_prompts': row['count'],
                        'accounts_with_prompts': row['accounts_count']
                    } for row in prompt_stats}
                    
                    # Get today's expected generation
                    stats['daily_config'] = daily_config
                    stats['expected_generation_today'] = {
                        'workflow_type': daily_config.get('workflow_type'),
                        'content_amount': daily_config.get('content_amount', 0),
                        'total_expected': daily_config.get('content_amount', 0) * stats['accounts_with_prompts']
                    }
                    
                    # Calculate readiness metrics
                    stats['generation_readiness'] = {
                        'accounts_ready_for_generation': stats['accounts_with_prompts'],
                        'accounts_missing_prompts': stats['total_accounts'] - stats['accounts_with_prompts'],
                        'readiness_percentage': round((stats['accounts_with_prompts'] / max(stats['total_accounts'], 1)) * 100, 2),
                        'workflow_type': workflow_type
                    }
                    
                    logger.info("Account generation statistics retrieved successfully")
                    return stats
                    
        except Exception as e:
            logger.error(f"Error getting account generation stats: {e}")
            return {}

    def create_settings_refresh_task():
        """Create Airflow task to refresh settings before content generation"""
        from airflow.operators.python import PythonOperator
        from streamlit.ui.settings.settings_manager import get_daily_content_config
        
        def refresh_settings_task():
            """Refresh and validate settings before content generation"""
            logger.info("Refreshing content generation settings...")
            
            config = DAGConfig.get_processing_config()
            stats = DAGConfig.get_account_generation_stats()
            
            daily_config = get_daily_content_config()
            
            logger.info(f"Today's configuration: {daily_config.get('day_name')} - {daily_config.get('workflow_type')} x {daily_config.get('content_amount')}")
            logger.info(f"Accounts ready for generation: {stats.get('generation_readiness', {}).get('accounts_ready_for_generation', 0)}")
            
            # Log any issues
            total_accounts = stats.get('total_accounts', 0)
            ready_accounts = stats.get('generation_readiness', {}).get('accounts_ready_for_generation', 0)
            
            if ready_accounts < total_accounts:
                logger.warning(f"Only {ready_accounts}/{total_accounts} accounts are ready for {daily_config.get('workflow_type')} generation")
                
            if not daily_config.get('enabled', True):
                logger.warning(f"Today ({daily_config.get('day_name')}) is disabled in weekly settings")
                
            logger.info("Settings refresh completed")
            return f"Settings refreshed - {ready_accounts} accounts ready for {daily_config.get('workflow_type')} generation"
        
        return PythonOperator(
            task_id="refresh_generation_settings",
            python_callable=refresh_settings_task
        )
    @staticmethod
    def validate_account_readiness(account_id: int) -> Dict[str, Any]:
        """Validate if an account is ready for content generation"""
        try:
            from core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor
            
            readiness = {
                'account_id': account_id,
                'is_ready': False,
                'missing_prompts': [],
                'existing_prompts': [],
                'issues': [],
                'recommendations': []
            }
            
            with get_postgres_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                    # Check if account exists
                    cursor.execute("SELECT username FROM accounts WHERE account_id = %s", (account_id,))
                    account = cursor.fetchone()
                    
                    if not account:
                        readiness['issues'].append(f"Account {account_id} does not exist")
                        return readiness
                    
                    readiness['username'] = account['username']
                    
                    # Check prompts for each content type
                    content_types = ['replies', 'messages', 'retweets']
                    
                    for content_type in content_types:
                        cursor.execute("""
                            SELECT prompt_id, name, content, is_active
                            FROM prompts
                            WHERE account_id = %s AND prompt_type = %s
                            ORDER BY updated_time DESC
                            LIMIT 1
                        """, (account_id, content_type))
                        
                        prompt = cursor.fetchone()
                        
                        if prompt and prompt['is_active']:
                            readiness['existing_prompts'].append({
                                'content_type': content_type,
                                'prompt_name': prompt['name'],
                                'prompt_id': prompt['prompt_id'],
                                'content_length': len(prompt['content'])
                            })
                        else:
                            readiness['missing_prompts'].append(content_type)
                            if prompt and not prompt['is_active']:
                                readiness['issues'].append(f"Prompt for {content_type} exists but is inactive")
                            else:
                                readiness['issues'].append(f"No prompt found for {content_type}")
                    
                    # Determine readiness
                    readiness['is_ready'] = len(readiness['missing_prompts']) == 0
                    
                    # Generate recommendations
                    if readiness['missing_prompts']:
                        config = DAGConfig.get_processing_config()
                        if config.get('create_default_prompts_if_missing', False):
                            readiness['recommendations'].append("Auto-create default prompts for missing types")
                        else:
                            readiness['recommendations'].append(f"Create prompts for: {', '.join(readiness['missing_prompts'])}")
                    
                    if readiness['is_ready']:
                        readiness['recommendations'].append("Account is ready for content generation")
                    
                    return readiness
                    
        except Exception as e:
            logger.error(f"Error validating account readiness for {account_id}: {e}")
            return {
                'account_id': account_id,
                'is_ready': False,
                'error': str(e),
                'issues': [f"Error during validation: {str(e)}"]
            }
