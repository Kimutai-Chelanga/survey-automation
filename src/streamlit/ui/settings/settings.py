#!/usr/bin/env python3
from datetime import datetime, timedelta

# Default settings for all DAGs
DEFAULT_SETTINGS = {
    'automa': {
        'schedule_interval': '0 8 * * *',  # Daily at 8 AM
        'timezone': 'Africa/Nairobi',
        'default_args': {
            'owner': 'airflow',
            'start_date': datetime(2025, 6, 27),
            'retries': 2,
            'retry_delay': timedelta(seconds=30),
            'max_active_tis_per_dag': 1,
        },
    },
    'create_content': {
        'schedule_interval': '0 16 * * 0',  # Weekly on Sunday at 4 PM
        'timezone': 'Africa/Nairobi',
        'default_args': {
            'owner': 'airflow',
            'start_date': datetime(2025, 6, 27),
            'retries': 3,
            'retry_delay': timedelta(minutes=5),
        },
    },
    'tweet_timestamp': {
        'schedule_interval': '0 * * * *',  # Hourly
        'timezone': 'Africa/Nairobi',
        'default_args': {
            'owner': 'airflow',
            'start_date': datetime(2025, 7, 31),
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    },
    'inject_workflow': {
        'schedule_interval': '*/5 * * * *',  # Every 5 minutes for single workflow processing
        'timezone': 'Africa/Nairobi',
        'default_args': {
            'owner': 'airflow',
            'start_date': datetime(2025, 6, 27, 3, 5),
            'retries': 1,
            'retry_delay': timedelta(minutes=2),
            'max_active_tis_per_dag': 1,
            'execution_timeout': timedelta(minutes=30),  # Reduced timeout for single workflow
        },
    },
    'system': {
        'mongodb_uri': 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin',
        'database_url': '',
        'twitter_consumer_key': '5Q3CFnvq02nKj6kI9gRpGNHXH',
        'twitter_consumer_secret': '4OBnuBjedjwZUZmtslwzzPmWxeQtN7LHUeYHf4jsqZjQkEyW4v',
        'twitter_access_token_key': '907341293717737473-for4ikiKhPAHxD54pnRqhJSPpr1QmNB',
        'twitter_access_token_secret': 'jV6TplxXfCQOu8C8zArB2wzlwGisq2Y0kRHUtrvuKYQNr',
    },
    
    # UPDATED: Single Workflow Strategy Settings
    'workflow_strategy_settings': {
        'trigger_delay_seconds': 5.0,  # Delay between injection and triggering (configurable)
        'interval_between_batches': 120.0,  # Wait time after workflow completion (2 minutes like example)
        'batch_size': 1,  # Always 1 for single workflow processing
        'max_workflows_per_run': 1,  # Always 1 for single workflow processing
        'enable_sequential_triggering': True,  # Enable the inject-and-trigger functionality
        'timeout_per_workflow': 10.0,  # Timeout for each workflow trigger in seconds
        'retry_failed_triggers': True,  # Whether to retry failed workflow triggers
        'log_detailed_execution': True,  # Whether to log detailed execution information
        'single_workflow_mode': True,  # NEW: Enable single workflow processing mode
        'workflow_execution_timeout': 300.0,  # NEW: Total timeout for workflow execution (5 minutes)
        'clear_storage_before_injection': True,  # NEW: Clear storage before injecting new workflow
        'wait_after_storage_clear': 2.0,  # NEW: Wait time after clearing storage
        'wait_before_trigger': 1.0,  # NEW: Additional wait before triggering after injection
    },
    
    # NEW: Workflow Type Selection Settings
    'extraction_workflow_types': {
        'replies': True,    # Enable replies workflow processing
        'messages': True,   # Enable messages workflow processing
        'retweets': True,   # Enable retweets workflow processing
    },
    
    # NEW: Workflow Priority Order Settings
    'extraction_priority_order': ['replies', 'messages', 'retweets'],  # Processing priority order
    
    # Extraction time settings (updated for more frequent single workflow processing)
    'extraction_time_settings': {
        'morning': {
            'start_times': ['0 8 * * *'],  # 8:00 AM
            'number_of_batches': 1,
            'gap_between_batches': 30
        },
        'afternoon': {
            'start_times': ['0 14 * * *'],  # 2:00 PM
            'number_of_batches': 1,
            'gap_between_batches': 30
        },
        'evening': {
            'start_times': ['0 18 * * *'],  # 6:00 PM
            'number_of_batches': 1,
            'gap_between_batches': 45
        },
        # NEW: Continuous processing schedule for single workflows
        'continuous': {
            'start_times': ['*/5 * * * *'],  # Every 5 minutes
            'number_of_batches': 1,
            'gap_between_batches': 5,
            'single_workflow_mode': True
        },
        # NEW: Workflow types configuration
        'workflow_types': {
            'replies': True,
            'messages': True,
            'retweets': True
        },
        'priority_order': ['replies', 'messages', 'retweets']
    },
    
    # NEW: Single Workflow Processing Configuration
    'single_workflow_processing': {
        'enabled': True,  # Enable single workflow processing
        'schedule_interval': '*/5 * * * *',  # Process every 5 minutes
        'max_concurrent_executions': 1,  # Only one workflow at a time
        'workflow_timeout': 300,  # 5 minutes timeout per workflow
        'retry_attempts': 2,  # Retry failed workflows
        'retry_delay': 60,  # Wait 60 seconds before retry
        'priority_order': ['replies', 'messages', 'retweets'],  # Process order preference
        'log_file': '/tmp/single_workflow_execution.log',  # Dedicated log file
        'chrome_startup_timeout': 45,  # Chrome startup timeout
        'chrome_health_check_retries': 5,  # Health check retries
        
        # NEW: Workflow type filtering
        'enabled_workflow_types': {
            'replies': True,
            'messages': True,
            'retweets': True
        },
        
        # NEW: Workflow type specific settings
        'workflow_type_settings': {
            'replies': {
                'timeout': 300,
                'retry_attempts': 2,
                'priority_weight': 1
            },
            'messages': {
                'timeout': 300,
                'retry_attempts': 2,
                'priority_weight': 2
            },
            'retweets': {
                'timeout': 300,
                'retry_attempts': 2,
                'priority_weight': 3
            }
        }
    },
    
    # NEW: Workflow Type Statistics and Monitoring
    'workflow_type_monitoring': {
        'track_type_performance': True,
        'track_type_success_rates': True,
        'generate_type_reports': True,
        'type_specific_logging': True,
        'performance_thresholds': {
            'replies': {
                'max_execution_time': 300,
                'min_success_rate': 0.90
            },
            'messages': {
                'max_execution_time': 300,
                'min_success_rate': 0.90
            },
            'retweets': {
                'max_execution_time': 300,
                'min_success_rate': 0.90
            }
        }
    }
}

# NEW: Validation function for workflow type settings
def validate_workflow_type_settings(settings):
    """Validate workflow type selection settings"""
    required_fields = ['replies', 'messages', 'retweets']
    
    # Check that at least one workflow type is enabled
    enabled_types = [wf_type for wf_type in required_fields if settings.get(wf_type, False)]
    if not enabled_types:
        raise ValueError("At least one workflow type must be enabled (replies, messages, or retweets)")
    
    # Validate each field is boolean
    for field in required_fields:
        if field in settings and not isinstance(settings[field], bool):
            raise ValueError(f"{field} must be a boolean value")
    
    return True

# NEW: Validation function for priority order settings
def validate_priority_order_settings(priority_order, enabled_types):
    """Validate priority order against enabled workflow types"""
    if not isinstance(priority_order, list):
        raise ValueError("Priority order must be a list")
    
    if not priority_order:
        raise ValueError("Priority order cannot be empty if workflow types are enabled")
    
    # Check that all items in priority order are valid workflow types
    valid_types = ['replies', 'messages', 'retweets']
    for wf_type in priority_order:
        if wf_type not in valid_types:
            raise ValueError(f"Invalid workflow type in priority order: {wf_type}")
    
    # Check that all enabled types are in priority order
    enabled_type_names = [wf_type for wf_type, enabled in enabled_types.items() if enabled]
    for enabled_type in enabled_type_names:
        if enabled_type not in priority_order:
            raise ValueError(f"Enabled workflow type '{enabled_type}' not found in priority order")
    
    # Check that all priority order items are enabled
    for wf_type in priority_order:
        if not enabled_types.get(wf_type, False):
            raise ValueError(f"Workflow type '{wf_type}' in priority order is not enabled")
    
    return True

# NEW: Validation function for single workflow settings (updated)
def validate_single_workflow_settings(settings):
    """Validate single workflow processing settings with workflow type support"""
    required_fields = [
        'trigger_delay_seconds',
        'interval_between_batches', 
        'timeout_per_workflow',
        'single_workflow_mode'
    ]
    
    for field in required_fields:
        if field not in settings:
            raise ValueError(f"Missing required field: {field}")
    
    # Type validations
    if not isinstance(settings.get('trigger_delay_seconds'), (int, float)):
        raise ValueError("trigger_delay_seconds must be a number")
    
    if not isinstance(settings.get('interval_between_batches'), (int, float)):
        raise ValueError("interval_between_batches must be a number")
    
    if not isinstance(settings.get('timeout_per_workflow'), (int, float)):
        raise ValueError("timeout_per_workflow must be a number")
    
    if not isinstance(settings.get('single_workflow_mode'), bool):
        raise ValueError("single_workflow_mode must be a boolean")
    
    # Range validations
    if settings.get('trigger_delay_seconds', 0) < 0:
        raise ValueError("trigger_delay_seconds cannot be negative")
    
    if settings.get('interval_between_batches', 0) < 0:
        raise ValueError("interval_between_batches cannot be negative")
    
    if settings.get('timeout_per_workflow', 0) <= 0:
        raise ValueError("timeout_per_workflow must be positive")
    
    # NEW: Validate workflow type settings if present
    if 'enabled_workflow_types' in settings:
        validate_workflow_type_settings(settings['enabled_workflow_types'])
    
    if 'priority_order' in settings and 'enabled_workflow_types' in settings:
        validate_priority_order_settings(settings['priority_order'], settings['enabled_workflow_types'])
    
    return True

# NEW: Helper function to get single workflow settings with workflow type support
def get_single_workflow_settings():
    """Get single workflow processing settings with proper defaults and workflow type support"""
    base_settings = DEFAULT_SETTINGS['workflow_strategy_settings'].copy()
    
    # Ensure single workflow mode settings
    base_settings.update({
        'batch_size': 1,
        'max_workflows_per_run': 1,
        'single_workflow_mode': True,
    })
    
    # Add workflow type settings
    base_settings.update({
        'enabled_workflow_types': DEFAULT_SETTINGS['extraction_workflow_types'].copy(),
        'priority_order': DEFAULT_SETTINGS['extraction_priority_order'].copy(),
    })
    
    return base_settings

# NEW: Helper function to get enabled workflow types
def get_enabled_workflow_types(settings=None):
    """Get list of enabled workflow types from settings"""
    if settings is None:
        settings = DEFAULT_SETTINGS
    
    workflow_types = settings.get('extraction_workflow_types', {
        'replies': True,
        'messages': True,
        'retweets': True
    })
    
    return [wf_type for wf_type, enabled in workflow_types.items() if enabled]

# NEW: Helper function to get workflow type priority order
def get_workflow_type_priority_order(settings=None):
    """Get workflow type priority order from settings"""
    if settings is None:
        settings = DEFAULT_SETTINGS
    
    return settings.get('extraction_priority_order', ['replies', 'messages', 'retweets'])

# NEW: Helper function to filter workflows by enabled types
def filter_workflows_by_enabled_types(workflows, enabled_types=None):
    """Filter workflow list to only include enabled types"""
    if enabled_types is None:
        enabled_types = get_enabled_workflow_types()
    
    if not enabled_types:
        return []
    
    # Filter workflows based on their type
    filtered_workflows = []
    for workflow in workflows:
        workflow_type = workflow.get('workflow_type') or workflow.get('content_type')
        if workflow_type in enabled_types:
            filtered_workflows.append(workflow)
    
    return filtered_workflows

# NEW: Helper function to sort workflows by priority order
def sort_workflows_by_priority(workflows, priority_order=None):
    """Sort workflow list based on priority order"""
    if priority_order is None:
        priority_order = get_workflow_type_priority_order()
    
    # Create priority mapping
    priority_map = {wf_type: idx for idx, wf_type in enumerate(priority_order)}
    
    def get_priority(workflow):
        workflow_type = workflow.get('workflow_type') or workflow.get('content_type')
        return priority_map.get(workflow_type, 999)  # Unknown types get lowest priority
    
    return sorted(workflows, key=get_priority)

# NEW: Helper function to get workflow type statistics
def get_workflow_type_statistics():
    """Get statistics for each workflow type (placeholder - implement with actual data)"""
    stats = {}
    enabled_types = get_enabled_workflow_types()
    
    for wf_type in enabled_types:
        stats[wf_type] = {
            'pending': 0,      # To be filled with actual data
            'executed': 0,     # To be filled with actual data
            'success_rate': 0.0, # To be filled with actual data
            'avg_execution_time': 0.0, # To be filled with actual data
            'last_execution': None, # To be filled with actual data
        }
    
    return stats

# NEW: Helper function to validate extraction settings with workflow types
def validate_extraction_settings_with_workflow_types(extraction_settings):
    """Validate extraction settings include proper workflow type configuration"""
    
    # Check for workflow type configuration
    if 'workflow_types' not in extraction_settings:
        extraction_settings['workflow_types'] = DEFAULT_SETTINGS['extraction_workflow_types'].copy()
    
    if 'priority_order' not in extraction_settings:
        extraction_settings['priority_order'] = DEFAULT_SETTINGS['extraction_priority_order'].copy()
    
    # Validate workflow type settings
    validate_workflow_type_settings(extraction_settings['workflow_types'])
    validate_priority_order_settings(extraction_settings['priority_order'], extraction_settings['workflow_types'])
    
    return extraction_settings 