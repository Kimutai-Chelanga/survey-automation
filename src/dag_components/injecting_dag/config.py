import os
from enum import Enum
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import logging
from airflow.models import Variable

# Default args for DAG (from your initial config)
DEFAULT_ARGS = {
    'owner': 'airflow',
    'start_date': datetime(2025, 6, 27, 3, 5),
    'retries': 2,  # Increased retries
    'retry_delay': timedelta(minutes=5),  # Increased retry delay
    'max_active_tis_per_dag': 1,
    'execution_timeout': timedelta(hours=3),  # Increased timeout
}

# Database configuration (from your initial config)
MONGO_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin')
POSTGRES_URI = os.getenv('DATABASE_URL', 'postgresql://airflow:airflow@postgres:5432/messages')
CHROME_DEBUG_URL = "http://localhost:9222/json"

# Chrome configuration (from your initial config + enhanced)
CHROME_STARTUP_TIMEOUT = 90  # seconds
CHROME_HEALTH_CHECK_RETRIES = 5
CHROME_HEALTH_CHECK_DELAY = 5  # seconds

# WebSocket configuration (from your initial config + enhanced)
WEBSOCKET_CONNECTION_TIMEOUT = 30  # seconds
WEBSOCKET_OPERATION_TIMEOUT = 15  # seconds
WEBSOCKET_RETRY_ATTEMPTS = 3
WEBSOCKET_RETRY_DELAY = 2  # seconds

# Workflow strategies (from your initial config)
class WorkflowStrategy(Enum):
    SINGLE = "single"
    MULTIPLE = "multiple"

# Workflow collections mapping (from your initial config)
WORKFLOW_COLLECTIONS = {
    'messages': {'name': 'messages_workflows', 'type': 'messages', 'priority': 1},
    'replies': {'name': 'replies_workflows', 'type': 'replies', 'priority': 2},
    'retweets': {'name': 'retweets_workflows', 'type': 'retweets', 'priority': 3}
}

def get_workflow_strategy():
    """Get workflow strategy and configuration from Airflow Variables (from your initial config)"""
    strategy = Variable.get('workflow_strategy', default_var='single')
    selected_types = Variable.get('workflow_types', default_var='messages')
    type_order = Variable.get('workflow_type_order', default_var='messages')
    
    if strategy == WorkflowStrategy.MULTIPLE.value:
        selected_types = [t.strip() for t in selected_types.split(',') if t.strip() in WORKFLOW_COLLECTIONS]
        if len(selected_types) == 3:
            type_order = selected_types
        else:
            type_order = [t.strip() for t in type_order.split(',') if t.strip() in selected_types]
            # Add any missing selected types to the end
            type_order.extend([t for t in selected_types if t not in type_order])
    else:
        selected_types = [selected_types] if selected_types in WORKFLOW_COLLECTIONS else ['messages']
        type_order = selected_types

    return strategy, selected_types, type_order

def get_execution_config():
    """Get execution configuration from Airflow Variables (from your initial config)"""
    return {
        'workflow_gap_seconds': float(Variable.get('workflow_gap_seconds', default_var=15.0)),
        'max_connection_retries': int(Variable.get('max_connection_retries', default_var=3)),
        'connection_retry_delay': float(Variable.get('connection_retry_delay', default_var=30.0)),
        'enable_websocket_pooling': Variable.get('enable_websocket_pooling', default_var='true').lower() == 'true',
        'chrome_health_check_interval': float(Variable.get('chrome_health_check_interval', default_var=10.0))
    }

# Enhanced logging configuration
ENHANCED_LOGGING_CONFIG = {
    'debug_mode': True,
    'log_level': logging.INFO,
    'console_log_capture': True,
    'automa_state_monitoring': True,
    'performance_monitoring': True,
    'websocket_debugging': True
}

# Enhanced WebSocket configuration
ENHANCED_WEBSOCKET_CONFIG = {
    'connection_timeout': 30,
    'response_timeout': 20,
    'max_response_attempts': 15,
    'health_check_interval': 60,
    'reconnection_attempts': 3,
    'message_buffer_size': 1000
}

# Enhanced Chrome configuration
ENHANCED_CHROME_CONFIG = {
    'startup_timeout': 180,
    'debugging_port': 9222,
    'profile_cleanup': True,
    'extension_load_timeout': 60,
    'enhanced_logging': True,
    'performance_monitoring': True
}

# Enhanced Automa integration configuration
ENHANCED_AUTOMA_CONFIG = {
    'injection_debugging': True,
    'storage_monitoring': True,
    'console_capture': True,
    'workflow_state_tracking': True,
    'execution_logging': True,
    'error_handling': 'comprehensive'
}

# Database configuration for enhanced logging
ENHANCED_DB_CONFIG = {
    'collections': {
        'workflow_logs_enhanced': 'workflow_logs_enhanced',
        'automa_debug_logs': 'automa_debug_logs',
        'automa_console_logs': 'automa_console_logs',
        'enhanced_execution_summaries': 'enhanced_dag_execution_summaries',
        'performance_metrics': 'workflow_performance_metrics',
        'websocket_stats': 'websocket_connection_stats'
    },
    'indexes': {
        'workflow_id_index': True,
        'timestamp_index': True,
        'dag_run_index': True,
        'performance_index': True
    },
    'retention_days': 30,
    'auto_cleanup': True
}

class EnhancedConfigManager:
    """Manages enhanced configuration without affecting existing modules"""
    
    def __init__(self):
        self.config = {
            'logging': ENHANCED_LOGGING_CONFIG,
            'websocket': ENHANCED_WEBSOCKET_CONFIG,
            'chrome': ENHANCED_CHROME_CONFIG,
            'automa': ENHANCED_AUTOMA_CONFIG,
            'database': ENHANCED_DB_CONFIG
        }
        self.environment_overrides = self._load_environment_overrides()
    
    def _load_environment_overrides(self) -> Dict[str, Any]:
        """Load configuration overrides from environment variables"""
        overrides = {}
        
        # Enhanced logging overrides
        if os.getenv('ENHANCED_DEBUG_MODE'):
            overrides['logging.debug_mode'] = os.getenv('ENHANCED_DEBUG_MODE').lower() == 'true'
        
        if os.getenv('ENHANCED_LOG_LEVEL'):
            log_level = os.getenv('ENHANCED_LOG_LEVEL').upper()
            if hasattr(logging, log_level):
                overrides['logging.log_level'] = getattr(logging, log_level)
        
        # WebSocket overrides
        if os.getenv('ENHANCED_WS_TIMEOUT'):
            overrides['websocket.connection_timeout'] = int(os.getenv('ENHANCED_WS_TIMEOUT'))
        
        if os.getenv('ENHANCED_WS_RESPONSE_TIMEOUT'):
            overrides['websocket.response_timeout'] = int(os.getenv('ENHANCED_WS_RESPONSE_TIMEOUT'))
        
        # Chrome overrides
        if os.getenv('ENHANCED_CHROME_TIMEOUT'):
            overrides['chrome.startup_timeout'] = int(os.getenv('ENHANCED_CHROME_TIMEOUT'))
        
        if os.getenv('ENHANCED_CHROME_DEBUG_PORT'):
            overrides['chrome.debugging_port'] = int(os.getenv('ENHANCED_CHROME_DEBUG_PORT'))
        
        # Database overrides
        if os.getenv('ENHANCED_DB_RETENTION_DAYS'):
            overrides['database.retention_days'] = int(os.getenv('ENHANCED_DB_RETENTION_DAYS'))
        
        return overrides
    
    def get_config(self, section: str, key: str, default: Any = None) -> Any:
        """Get configuration value with environment override support"""
        override_key = f"{section}.{key}"
        if override_key in self.environment_overrides:
            return self.environment_overrides[override_key]
        
        section_config = self.config.get(section, {})
        return section_config.get(key, default)
    
    def get_section_config(self, section: str) -> Dict[str, Any]:
        """Get entire section configuration with overrides applied"""
        base_config = self.config.get(section, {}).copy()
        
        # Apply environment overrides
        for override_key, override_value in self.environment_overrides.items():
            if override_key.startswith(f"{section}."):
                key = override_key.split('.', 1)[1]
                base_config[key] = override_value
        
        return base_config
    
    def is_enhanced_mode_enabled(self) -> bool:
        """Check if enhanced mode is enabled"""
        return self.get_config('logging', 'debug_mode', True)
    
    def get_database_collections(self) -> Dict[str, str]:
        """Get database collection names"""
        return self.get_config('database', 'collections', {})
    
    def get_websocket_config(self) -> Dict[str, Any]:
        """Get WebSocket configuration"""
        return self.get_section_config('websocket')
    
    def get_chrome_config(self) -> Dict[str, Any]:
        """Get Chrome configuration"""
        return self.get_section_config('chrome')
    
    def get_automa_config(self) -> Dict[str, Any]:
        """Get Automa configuration"""
        return self.get_section_config('automa')
    
    def validate_config(self) -> Dict[str, Any]:
        """Validate configuration and return validation results"""
        validation_results = {
            'valid': True,
            'warnings': [],
            'errors': []
        }
        
        # Validate timeouts
        ws_timeout = self.get_config('websocket', 'connection_timeout')
        if ws_timeout < 10:
            validation_results['warnings'].append(f"WebSocket timeout ({ws_timeout}s) is very low")
        
        chrome_timeout = self.get_config('chrome', 'startup_timeout')
        if chrome_timeout < 60:
            validation_results['warnings'].append(f"Chrome startup timeout ({chrome_timeout}s) might be insufficient")
        
        # Validate retention settings
        retention_days = self.get_config('database', 'retention_days')
        if retention_days < 7:
            validation_results['warnings'].append(f"Database retention ({retention_days} days) is very short")
        
        return validation_results

# Global configuration instance
enhanced_config = EnhancedConfigManager()

def get_enhanced_config() -> EnhancedConfigManager:
    """Get the enhanced configuration manager instance"""
    return enhanced_config

def is_enhanced_logging_enabled() -> bool:
    """Quick check if enhanced logging is enabled"""
    return enhanced_config.get_config('logging', 'debug_mode', True)

def get_enhanced_collections() -> Dict[str, str]:
    """Get enhanced database collection names"""
    return enhanced_config.get_database_collections()

def get_enhanced_websocket_settings() -> Dict[str, Any]:
    """Get enhanced WebSocket settings"""
    return enhanced_config.get_websocket_config()

def get_enhanced_chrome_settings() -> Dict[str, Any]:
    """Get enhanced Chrome settings"""
    return enhanced_config.get_chrome_config()

def get_enhanced_automa_settings() -> Dict[str, Any]:
    """Get enhanced Automa settings"""
    return enhanced_config.get_automa_config()

# Feature flags for enhanced functionality
ENHANCED_FEATURES = {
    'comprehensive_logging': True,
    'debug_mode': True,
    'console_capture': True,
    'performance_monitoring': True,
    'websocket_debugging': True,
    'automa_state_tracking': True,
    'enhanced_error_handling': True,
    'plug_and_play_mode': True
}

def is_feature_enabled(feature_name: str) -> bool:
    """Check if a specific enhanced feature is enabled"""
    return ENHANCED_FEATURES.get(feature_name, False)

# Enhanced mode detection
def detect_enhanced_mode() -> Dict[str, bool]:
    """Detect what enhanced features are available and enabled"""
    return {
        'enhanced_logging': is_enhanced_logging_enabled(),
        'debug_mode': enhanced_config.get_config('logging', 'debug_mode', False),
        'console_capture': enhanced_config.get_config('logging', 'console_log_capture', False),
        'websocket_debugging': enhanced_config.get_config('logging', 'websocket_debugging', False),
        'performance_monitoring': enhanced_config.get_config('logging', 'performance_monitoring', False),
        'automa_integration': enhanced_config.get_config('automa', 'injection_debugging', False)
    }

# Configuration validation on import
def validate_enhanced_setup():
    """Validate enhanced setup and warn about issues"""
    validation = enhanced_config.validate_config()
    
    if validation['warnings']:
        import logging
        logger = logging.getLogger(__name__)
        logger.warning("Enhanced configuration warnings:")
        for warning in validation['warnings']:
            logger.warning(f"  - {warning}")
    
    if validation['errors']:
        import logging
        logger = logging.getLogger(__name__)
        logger.error("Enhanced configuration errors:")
        for error in validation['errors']:
            logger.error(f"  - {error}")
    
    return validation

# Auto-validate on import if in enhanced mode
if enhanced_config.is_enhanced_mode_enabled():
    validate_enhanced_setup()

# Export all enhanced configurations
__all__ = [
    'enhanced_config',
    'get_enhanced_config',
    'is_enhanced_logging_enabled',
    'get_enhanced_collections',
    'get_enhanced_websocket_settings',
    'get_enhanced_chrome_settings',
    'get_enhanced_automa_settings',
    'is_feature_enabled',
    'detect_enhanced_mode',
    'validate_enhanced_setup',
    'ENHANCED_FEATURES',
    'ENHANCED_LOGGING_CONFIG',
    'ENHANCED_WEBSOCKET_CONFIG',
    'ENHANCED_CHROME_CONFIG',
    'ENHANCED_AUTOMA_CONFIG',
    'ENHANCED_DB_CONFIG'
]