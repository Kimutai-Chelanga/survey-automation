import json
import time
import logging
from datetime import datetime, timedelta
from bson import ObjectId
from typing import Dict, List, Optional, Any
from enum import Enum

from .db_utils import get_mongo_db

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WorkflowStatus(Enum):
    """Enum for workflow execution status tracking"""
    PENDING = "pending"
    INJECTION_STARTED = "injection_started"
    INJECTION_COMPLETED = "injection_completed"
    INJECTION_FAILED = "injection_failed"
    EXECUTION_STARTED = "execution_started"
    EXECUTION_IN_PROGRESS = "execution_in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    ERROR = "error"
    TIMEOUT = "timeout"
    CANCELLED = "cancelled"

class AutomaLogCapture:
    """Handles Automa-specific log capture and debugging"""
    
    def __init__(self):
        self.debug_mode = True
        self.console_logs = []
        self.storage_snapshots = []
    
    def capture_console_logs_script(self, workflow_id: str = None) -> str:
        """Generate JavaScript to capture console logs with enhanced debugging"""
        return f"""
        (() => {{
            console.log('[AUTOMA-DEBUG] Starting enhanced console capture for workflow: {workflow_id or "all"}');
            
            // Capture existing console history if available
            const logs = {{
                timestamp: new Date().toISOString(),
                workflow_id: '{workflow_id or "all"}',
                console_messages: [],
                storage_data: null,
                debug_info: {{
                    chrome_available: typeof chrome !== 'undefined',
                    storage_available: typeof chrome !== 'undefined' && !!chrome.storage,
                    extension_context: typeof chrome !== 'undefined' && !!chrome.runtime?.id
                }}
            }};
            
            // Override console methods to capture logs
            const originalLog = console.log;
            const originalError = console.error;
            const originalWarn = console.warn;
            
            const logCapture = [];
            
            console.log = function(...args) {{
                logCapture.push({{
                    level: 'info',
                    timestamp: new Date().toISOString(),
                    message: args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : String(arg)).join(' ')
                }});
                originalLog.apply(console, args);
            }};
            
            console.error = function(...args) {{
                logCapture.push({{
                    level: 'error',
                    timestamp: new Date().toISOString(),
                    message: args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : String(arg)).join(' ')
                }});
                originalError.apply(console, args);
            }};
            
            console.warn = function(...args) {{
                logCapture.push({{
                    level: 'warn',
                    timestamp: new Date().toISOString(),
                    message: args.map(arg => typeof arg === 'object' ? JSON.stringify(arg) : String(arg)).join(' ')
                }});
                originalWarn.apply(console, args);
            }};
            
            // Capture storage data
            if (logs.debug_info.storage_available) {{
                let storageCompleted = false;
                chrome.storage.local.get(null, (data) => {{
                    if (chrome.runtime.lastError) {{
                        logs.storage_error = chrome.runtime.lastError.message;
                    }} else {{
                        logs.storage_data = data;
                        logs.workflow_logs = data.workflowLogs || [];
                        logs.execution_history = data.executionHistory || [];
                        logs.workflows = data.workflows || [];
                    }}
                    storageCompleted = true;
                }});
                
                // Wait for storage operation
                const startTime = Date.now();
                while (!storageCompleted && (Date.now() - startTime) < 5000) {{
                    // Wait
                }}
            }}
            
            // Wait a moment to capture any immediate console output
            setTimeout(() => {{
                logs.console_messages = logCapture;
                console.log('[AUTOMA-DEBUG] Captured', logCapture.length, 'console messages');
            }}, 1000);
            
            logs.console_messages = logCapture;
            return JSON.stringify(logs);
        }})();
        """
    
    def capture_workflow_state_script(self) -> str:
        """Generate JavaScript to capture current workflow execution state"""
        return """
        (() => {
            console.log('[AUTOMA-DEBUG] Capturing workflow execution state');
            
            const state = {
                timestamp: new Date().toISOString(),
                automa_state: {
                    available: typeof window.automa !== 'undefined',
                    version: typeof window.automa !== 'undefined' ? window.automa.version : null,
                    active_workflows: []
                },
                dom_state: {
                    active_elements: document.querySelectorAll('[data-automa]').length,
                    automa_ui_present: !!document.querySelector('.automa-ui, #automa-root')
                },
                performance: {
                    memory_usage: performance.memory ? {
                        used: performance.memory.usedJSHeapSize,
                        total: performance.memory.totalJSHeapSize,
                        limit: performance.memory.jsHeapSizeLimit
                    } : null,
                    timing: performance.timing
                }
            };
            
            // Try to get workflow execution status from Automa
            if (typeof window.automa !== 'undefined' && window.automa.getWorkflowStatus) {
                try {
                    state.automa_state.active_workflows = window.automa.getWorkflowStatus();
                } catch (e) {
                    state.automa_state.error = e.message;
                }
            }
            
            console.log('[AUTOMA-DEBUG] Workflow state captured:', state);
            return JSON.stringify(state);
        })();
        """

class EnhancedWorkflowLogger:
    """Enhanced workflow logger with comprehensive Automa integration"""
    
    def __init__(self):
        self.log_collection_name = "workflow_logs_enhanced"
        self.execution_logs_collection = "workflow_execution_logs_enhanced"
        self.automa_logs_collection = "automa_raw_logs"
        self.automa_console_logs = "automa_console_logs"
        self.automa_debug_logs = "automa_debug_logs"
        self.performance_metrics_collection = "workflow_performance_metrics"
        self.workflow_status_collection = "workflow_status_tracking"
        self.active_logs: Dict[str, str] = {}
        self.log_capture = AutomaLogCapture()
    
    def initialize_collections(self):
        """Initialize MongoDB collections with proper indexes"""
        try:
            db, client = get_mongo_db()
            
            collections_to_index = [
                (self.log_collection_name, [("workflow_id", 1), ("created_at", -1)]),
                (self.execution_logs_collection, [("workflow_id", 1), ("execution_timestamp", -1)]),
                (self.automa_logs_collection, [("workflow_id", 1), ("captured_at", -1)]),
                (self.automa_console_logs, [("workflow_id", 1), ("captured_at", -1)]),
                (self.automa_debug_logs, [("workflow_id", 1), ("debug_timestamp", -1)]),
                (self.workflow_status_collection, [("workflow_id", 1), ("updated_at", -1)]),
                (self.performance_metrics_collection, [("dag_run_id", 1), ("created_at", -1)])
            ]
            
            for collection_name, indexes in collections_to_index:
                collection = db[collection_name]
                for index_spec in indexes:
                    try:
                        collection.create_index([index_spec])
                    except Exception as e:
                        logger.warning(f"Index creation failed for {collection_name}: {e}")
            
            client.close()
            logger.info("✅ Collections initialized with indexes")
            
        except Exception as e:
            logger.error(f"❌ Failed to initialize collections: {e}")
            if 'client' in locals():
                client.close()
    
    def start_workflow_logging(self, workflow_data: dict, dag_context: dict = None) -> str:
        """Start logging for a workflow execution with enhanced debugging"""
        try:
            db, client = get_mongo_db()
            log_collection = db[self.log_collection_name]
            
            workflow_info = workflow_data.get('automaWf', {})
            workflow_id = workflow_info.get('id', '')
            workflow_name = workflow_info.get('name', '')
            
            log_entry = {
                'workflow_id': workflow_id,
                'workflow_name': workflow_name,
                'workflow_data': workflow_data,
                'dag_context': dag_context or {},
                'created_at': datetime.now(),
                'status': WorkflowStatus.PENDING.value,
                'log_events': [],
                'debug_enabled': True,
                'console_logs_captured': False,
                'automa_state_captured': False,
                'metadata': {
                    'workflow_type': workflow_data.get('workflowType'),
                    'collection_name': workflow_data.get('collectionName'),
                    'mongo_id': str(workflow_data.get('mongoDoc', {}).get('_id', '')),
                    'debug_mode': self.log_capture.debug_mode
                }
            }
            
            result = log_collection.insert_one(log_entry)
            log_entry_id = str(result.inserted_id)
            
            self.active_logs[workflow_id] = log_entry_id
            
            self.update_workflow_status(
                workflow_id, 
                WorkflowStatus.PENDING, 
                "Enhanced workflow logging started with debug mode enabled"
            )
            
            client.close()
            logger.info(f"✅ Started enhanced logging for workflow: {workflow_name}")
            return log_entry_id
            
        except Exception as e:
            logger.error(f"❌ Failed to start workflow logging: {e}")
            if 'client' in locals():
                client.close()
            return ""
    
    def update_workflow_status(self, workflow_id: str, status: WorkflowStatus, message: str = "", additional_data: dict = None):
        """Update workflow status with detailed tracking"""
        try:
            db, client = get_mongo_db()
            status_collection = db[self.workflow_status_collection]
            
            status_update = {
                'workflow_id': workflow_id,
                'status': status.value,
                'message': message,
                'updated_at': datetime.now(),
                'additional_data': additional_data or {}
            }
            
            status_collection.update_one(
                {'workflow_id': workflow_id},
                {
                    '$set': status_update,
                    '$push': {
                        'status_history': {
                            'status': status.value,
                            'message': message,
                            'timestamp': datetime.now(),
                            'additional_data': additional_data or {}
                        }
                    }
                },
                upsert=True
            )
            
            log_entry_id = self.active_logs.get(workflow_id)
            if log_entry_id:
                log_collection = db[self.log_collection_name]
                log_collection.update_one(
                    {'_id': ObjectId(log_entry_id)},
                    {
                        '$set': {'status': status.value, 'last_updated': datetime.now()},
                        '$push': {
                            'log_events': {
                                'timestamp': datetime.now(),
                                'status': status.value,
                                'message': message,
                                'additional_data': additional_data or {}
                            }
                        }
                    }
                )
            
            client.close()
            logger.debug(f"Updated status for workflow {workflow_id}: {status.value}")
            
        except Exception as e:
            logger.error(f"❌ Failed to update workflow status: {e}")
            if 'client' in locals():
                client.close()
    
    def capture_automa_debug_logs(self, ws_url: str, workflow_id: str = None) -> dict:
        """Capture comprehensive Automa debug logs"""
        from .websocket_handler import WebSocketResponseHandler
        from .chrome_automa import ws_manager
        import websocket
        
        try:
            ws = ws_manager.get_connection(ws_url)
            response_handler = WebSocketResponseHandler()
            
            # Clear any pending messages
            ws.settimeout(1)
            try:
                while True:
                    ws.recv()
            except websocket.WebSocketTimeoutException:
                pass
            
            # Capture console logs
            console_script = self.log_capture.capture_console_logs_script(workflow_id)
            message_id = int(time.time() * 1000) % 100000
            
            console_message = {
                "id": message_id,
                "method": "Runtime.evaluate",
                "params": {
                    "expression": console_script,
                    "returnByValue": True
                }
            }
            
            ws.settimeout(30)
            ws.send(json.dumps(console_message))
            
            console_logs = {}
            for attempt in range(10):
                try:
                    response = ws.recv()
                    response_data = json.loads(response)
                    
                    if response_data.get('id') == message_id and 'result' in response_data:
                        if 'value' in response_data['result']:
                            try:
                                console_logs = json.loads(response_data['result']['value'])
                                break
                            except json.JSONDecodeError:
                                continue
                                
                except websocket.WebSocketTimeoutException:
                    break
                except Exception as e:
                    logger.error(f"Error capturing console logs: {e}")
                    continue
            
            # Capture workflow state
            state_script = self.log_capture.capture_workflow_state_script()
            state_message = {
                "id": message_id + 1,
                "method": "Runtime.evaluate",
                "params": {
                    "expression": state_script,
                    "returnByValue": True
                }
            }
            
            ws.send(json.dumps(state_message))
            
            workflow_state = {}
            for attempt in range(10):
                try:
                    response = ws.recv()
                    response_data = json.loads(response)
                    
                    if response_data.get('id') == message_id + 1 and 'result' in response_data:
                        if 'value' in response_data['result']:
                            try:
                                workflow_state = json.loads(response_data['result']['value'])
                                break
                            except json.JSONDecodeError:
                                continue
                                
                except websocket.WebSocketTimeoutException:
                    break
                except Exception as e:
                    logger.error(f"Error capturing workflow state: {e}")
                    continue
            
            return {
                'success': True,
                'timestamp': datetime.now().isoformat(),
                'console_logs': console_logs,
                'workflow_state': workflow_state,
                'debug_enabled': True
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to capture Automa debug logs: {e}")
            return {
                'success': False,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def get_automa_logs_enhanced(self, ws_url: str, workflow_id: str = None) -> dict:
        """Enhanced log capture with debug capabilities"""
        debug_logs = self.capture_automa_debug_logs(ws_url, workflow_id)
        
        # Store debug logs if successful
        if debug_logs.get('success') and workflow_id:
            self.store_automa_debug_logs(workflow_id, debug_logs)
        
        # Also capture regular storage logs for compatibility
        from .websocket_handler import WebSocketResponseHandler
        from .chrome_automa import ws_manager
        import websocket
        
        max_retries = 3
        response_handler = WebSocketResponseHandler()
        
        for attempt in range(max_retries):
            try:
                ws = ws_manager.get_connection(ws_url)
                
                if not ws_manager._test_connection_health(ws_url):
                    logger.warning(f"WebSocket unhealthy, attempt {attempt + 1}/{max_retries}")
                    ws_manager._close_connection(ws_url)
                    time.sleep(2)
                    continue
                
                ws.settimeout(60)
                try:
                    while True:
                        ws.recv()
                except websocket.WebSocketTimeoutException:
                    pass
                
                log_capture_script = f"""
                (() => {{
                    console.log('[LOG-CAPTURE-ENHANCED] Starting enhanced log capture for: {workflow_id or "all"}');
                    
                    const logs = {{
                        timestamp: new Date().toISOString(),
                        workflow_id: '{workflow_id or "all"}',
                        success: false,
                        method: 'enhanced_capture_with_debug',
                        debug_enabled: true
                    }};
                    
                    if (typeof chrome === 'undefined' || !chrome.storage || !chrome.storage.local) {{
                        logs.error = 'Chrome storage not available';
                        console.log('[LOG-CAPTURE-ENHANCED] Chrome storage not available');
                        return JSON.stringify(logs);
                    }}
                    
                    let completed = false;
                    
                    chrome.storage.local.get(null, (data) => {{
                        if (chrome.runtime.lastError) {{
                            logs.storage_error = chrome.runtime.lastError.message;
                            console.log('[LOG-CAPTURE-ENHANCED] Storage error:', chrome.runtime.lastError.message);
                        }} else {{
                            logs.chrome_storage = data;
                            logs.workflowLogs = data.workflowLogs || [];
                            logs.executionHistory = data.executionHistory || [];
                            logs.workflows = data.workflows || [];
                            logs.debugLogs = data.debugLogs || [];
                            logs.success = true;
                            
                            if ('{workflow_id}' && '{workflow_id}' !== 'all') {{
                                logs.filtered_workflow_logs = logs.workflowLogs.filter(log => 
                                    log.workflowId === '{workflow_id}' || log.workflow_id === '{workflow_id}'
                                );
                                logs.filtered_execution_history = logs.executionHistory.filter(entry =>
                                    entry.workflowId === '{workflow_id}' || entry.workflow_id === '{workflow_id}'
                                );
                                logs.filtered_debug_logs = logs.debugLogs.filter(log =>
                                    log.workflowId === '{workflow_id}' || log.workflow_id === '{workflow_id}'
                                );
                            }}
                            
                            console.log('[LOG-CAPTURE-ENHANCED] Captured', Object.keys(data).length, 'storage keys');
                            console.log('[LOG-CAPTURE-ENHANCED] Found', logs.workflowLogs.length, 'workflow logs');
                            console.log('[LOG-CAPTURE-ENHANCED] Found', logs.executionHistory.length, 'execution entries');
                            console.log('[LOG-CAPTURE-ENHANCED] Found', logs.debugLogs.length, 'debug logs');
                            console.log('[LOG-CAPTURE-ENHANCED] Found', logs.workflows.length, 'workflows');
                        }}
                        completed = true;
                    }});
                    
                    const startTime = Date.now();
                    while (!completed && (Date.now() - startTime) < 8000) {{
                    }}
                    
                    if (!completed) {{
                        logs.error = 'Log capture timeout';
                        console.log('[LOG-CAPTURE-ENHANCED] Timeout waiting for storage');
                    }}
                    
                    console.log('[LOG-CAPTURE-ENHANCED] Enhanced log capture completed, success:', logs.success);
                    return JSON.stringify(logs);
                }})();
                """
                
                message_id = int(time.time() * 1000) % 100000
                log_message = {
                    "id": message_id,
                    "method": "Runtime.evaluate", 
                    "params": {
                        "expression": log_capture_script,
                        "returnByValue": True
                    }
                }
                
                ws.settimeout(60)
                ws.send(json.dumps(log_message))
                ws_manager.record_message(ws_url)
                
                log_responses = []
                log_data = None
                
                for response_attempt in range(20):
                    try:
                        response = ws.recv()
                        response_data = json.loads(response)
                        log_responses.append(response_data)
                        
                        if response_data.get('id') == message_id and 'result' in response_data:
                            if 'value' in response_data['result']:
                                try:
                                    result_value = response_data['result']['value']
                                    if isinstance(result_value, str) and result_value.startswith('{'):
                                        log_data = json.loads(result_value)
                                        break
                                except json.JSONDecodeError:
                                    continue
                        
                        if 'method' in response_data and 'LOG-CAPTURE-ENHANCED' in str(response_data.get('params', {})):
                            logger.debug(f"Enhanced log capture console message: {response_data}")
                            
                    except websocket.WebSocketTimeoutException:
                        logger.debug(f"Log response timeout on attempt {response_attempt + 1}")
                        break
                    except Exception as e:
                        logger.error(f"Error collecting log response: {e}")
                        continue
                
                if log_data:
                    # Merge with debug logs
                    if debug_logs.get('success'):
                        log_data['debug_logs'] = debug_logs
                    
                    logger.info(f"Successfully captured enhanced logs: {log_data.get('success', False)}")
                    return log_data
                else:
                    logger.warning(f"Failed to capture logs on attempt {attempt + 1}")
                    return {
                        "error": "Failed to parse log data",
                        "timestamp": datetime.now().isoformat(),
                        "responses_collected": len(log_responses),
                        "debug_logs": debug_logs if debug_logs.get('success') else None
                    }
                    
            except Exception as e:
                logger.error(f"Enhanced log capture failed (attempt {attempt + 1}/{max_retries}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return {
            "error": "Max retries exceeded for enhanced log capture", 
            "timestamp": datetime.now().isoformat(),
            "debug_logs": debug_logs if debug_logs.get('success') else None
        }
    
    def store_automa_debug_logs(self, workflow_id: str, debug_data: dict) -> str:
        """Store Automa debug logs separately"""
        try:
            db, client = get_mongo_db()
            debug_logs_collection = db[self.automa_debug_logs]
            
            debug_entry = {
                'workflow_id': workflow_id,
                'debug_timestamp': datetime.now(),
                'debug_data': debug_data,
                'console_logs_count': len(debug_data.get('console_logs', {}).get('console_messages', [])),
                'workflow_state': debug_data.get('workflow_state', {}),
                'metadata': {
                    'debug_enabled': True,
                    'capture_method': 'enhanced_debug_capture'
                }
            }
            
            result = debug_logs_collection.insert_one(debug_entry)
            debug_log_id = str(result.inserted_id)
            
            client.close()
            logger.info(f"✅ Stored debug logs for workflow: {workflow_id}")
            return debug_log_id
            
        except Exception as e:
            logger.error(f"❌ Failed to store debug logs: {e}")
            if 'client' in locals():
                client.close()
            return ""
    
    def store_automa_logs(self, workflow_data: dict, log_data: dict, log_entry_id: str) -> str:
        """Store Automa logs captured from Chrome extension with enhanced debug info"""
        try:
            db, client = get_mongo_db()
            automa_logs_collection = db[self.automa_logs_collection]
            
            workflow_info = workflow_data.get('automaWf', {})
            workflow_id = workflow_info.get('id', '')
            workflow_name = workflow_info.get('name', '')
            
            automa_log_entry = {
                'workflow_id': workflow_id,
                'workflow_name': workflow_name,
                'log_entry_id': log_entry_id,
                'captured_at': datetime.now(),
                'log_data': log_data,
                'success': log_data.get('success', False),
                'debug_enabled': log_data.get('debug_enabled', False),
                'metadata': {
                    'capture_method': log_data.get('method', 'unknown'),
                    'workflow_logs_count': len(log_data.get('workflowLogs', [])),
                    'execution_history_count': len(log_data.get('executionHistory', [])),
                    'debug_logs_count': len(log_data.get('debugLogs', [])),
                    'chrome_storage_keys': len(log_data.get('chrome_storage', {})),
                    'has_filtered_logs': bool(log_data.get('filtered_workflow_logs')),
                    'has_debug_data': bool(log_data.get('debug_logs'))
                }
            }
            
            result = automa_logs_collection.insert_one(automa_log_entry)
            automa_log_id = str(result.inserted_id)
            
            log_collection = db[self.log_collection_name]
            log_collection.update_one(
                {'_id': ObjectId(log_entry_id)},
                {
                    '$set': {
                        'automa_log_id': automa_log_id,
                        'automa_logs_captured': True,
                        'automa_logs_captured_at': datetime.now(),
                        'debug_logs_available': bool(log_data.get('debug_logs'))
                    },
                    '$push': {
                        'log_events': {
                            'timestamp': datetime.now(),
                            'event_type': 'automa_logs_captured',
                            'message': 'Enhanced Automa logs successfully captured and stored',
                            'automa_log_id': automa_log_id,
                            'debug_enabled': log_data.get('debug_enabled', False)
                        }
                    }
                }
            )
            
            client.close()
            logger.info(f"✅ Stored enhanced Automa logs for workflow: {workflow_name}")
            return automa_log_id
            
        except Exception as e:
            logger.error(f"❌ Failed to store Automa logs: {e}")
            if 'client' in locals():
                client.close()
            return ""
    
    def store_execution_logs(self, workflow_data: dict, execution_data: dict, log_entry_id: str) -> str:
        """Store detailed workflow execution logs"""
        try:
            db, client = get_mongo_db()
            execution_logs_collection = db[self.execution_logs_collection]
            
            workflow_info = workflow_data.get('automaWf', {})
            workflow_id = workflow_info.get('id', '')
            workflow_name = workflow_info.get('name', '')
            
            execution_log_entry = {
                'workflow_id': workflow_id,
                'workflow_name': workflow_name,
                'log_entry_id': log_entry_id,
                'execution_timestamp': datetime.now(),
                'execution_data': execution_data,
                'execution_status': execution_data.get('status', 'unknown'),
                'metadata': {
                    'execution_duration': execution_data.get('duration'),
                    'steps_completed': execution_data.get('steps_completed', 0),
                    'errors_encountered': len(execution_data.get('errors', [])),
                    'success': execution_data.get('success', False),
                    'debug_info_available': bool(execution_data.get('debug_info'))
                }
            }
            
            result = execution_logs_collection.insert_one(execution_log_entry)
            execution_log_id = str(result.inserted_id)
            
            log_collection = db[self.log_collection_name]
            log_collection.update_one(
                {'_id': ObjectId(log_entry_id)},
                {
                    '$set': {
                        'execution_log_id': execution_log_id,
                        'execution_logs_stored': True,
                        'execution_logs_stored_at': datetime.now()
                    },
                    '$push': {
                        'log_events': {
                            'timestamp': datetime.now(),
                            'event_type': 'execution_logs_stored',
                            'message': 'Execution logs successfully stored',
                            'execution_log_id': execution_log_id
                        }
                    }
                }
            )
            
            client.close()
            logger.info(f"✅ Stored execution logs for workflow: {workflow_name}")
            return execution_log_id
            
        except Exception as e:
            logger.error(f"❌ Failed to store execution logs: {e}")
            if 'client' in locals():
                client.close()
            return ""
    
    def store_console_logs(self, workflow_id: str, console_messages: List[dict], log_entry_id: str) -> str:
        """Store console messages captured during workflow execution"""
        try:
            db, client = get_mongo_db()
            console_logs_collection = db[self.automa_console_logs]
            
            console_log_entry = {
                'workflow_id': workflow_id,
                'log_entry_id': log_entry_id,
                'captured_at': datetime.now(),
                'console_messages': console_messages,
                'message_count': len(console_messages),
                'metadata': {
                    'error_messages': [msg for msg in console_messages if msg.get('level') == 'error'],
                    'warning_messages': [msg for msg in console_messages if msg.get('level') == 'warning'],
                    'info_messages': [msg for msg in console_messages if msg.get('level') == 'info'],
                    'debug_messages': [msg for msg in console_messages if '[AUTOMA-DEBUG]' in str(msg.get('message', ''))]
                }
            }
            
            result = console_logs_collection.insert_one(console_log_entry)
            console_log_id = str(result.inserted_id)
            
            client.close()
            logger.info(f"✅ Stored {len(console_messages)} console messages for workflow: {workflow_id}")
            return console_log_id
            
        except Exception as e:
            logger.error(f"❌ Failed to store console logs: {e}")
            if 'client' in locals():
                client.close()
            return ""
    
    def calculate_performance_metrics(self, dag_run_id: str = None) -> dict:
        """Calculate performance metrics for workflow executions"""
        try:
            db, client = get_mongo_db()
            
            query_filter = {}
            if dag_run_id:
                query_filter['dag_context.dag_run_id'] = dag_run_id
            
            log_collection = db[self.log_collection_name]
            workflows = list(log_collection.find(query_filter))
            
            if not workflows:
                client.close()
                return {}
            
            total_workflows = len(workflows)
            successful_workflows = len([wf for wf in workflows if wf.get('status') == WorkflowStatus.COMPLETED.value])
            failed_workflows = len([wf for wf in workflows if wf.get('status') in [WorkflowStatus.FAILED.value, WorkflowStatus.ERROR.value]])
            workflows_with_debug = len([wf for wf in workflows if wf.get('debug_logs_available', False)])
            
            execution_times = []
            for workflow in workflows:
                created_at = workflow.get('created_at')
                last_updated = workflow.get('last_updated')
                if created_at and last_updated:
                    duration = (last_updated - created_at).total_seconds()
                    execution_times.append(duration)
            
            metrics = {
                'total_workflows': total_workflows,
                'successful_workflows': successful_workflows,
                'failed_workflows': failed_workflows,
                'workflows_with_debug': workflows_with_debug,
                'success_rate': round((successful_workflows / total_workflows) * 100, 2) if total_workflows > 0 else 0,
                'debug_coverage': round((workflows_with_debug / total_workflows) * 100, 2) if total_workflows > 0 else 0,
                'average_execution_time': round(sum(execution_times) / len(execution_times), 2) if execution_times else 0,
                'min_execution_time': min(execution_times) if execution_times else 0,
                'max_execution_time': max(execution_times) if execution_times else 0,
                'workflows_with_logs': len([wf for wf in workflows if wf.get('automa_logs_captured', False)]),
                'calculated_at': datetime.now().isoformat()
            }
            
            performance_collection = db[self.performance_metrics_collection]
            metrics_entry = {
                'dag_run_id': dag_run_id,
                'created_at': datetime.now(),
                'metrics': metrics
            }
            performance_collection.insert_one(metrics_entry)
            
            client.close()
            logger.info(f"✅ Calculated enhanced performance metrics: {metrics['success_rate']}% success rate, {metrics['debug_coverage']}% debug coverage")
            return metrics
            
        except Exception as e:
            logger.error(f"❌ Failed to calculate performance metrics: {e}")
            if 'client' in locals():
                client.close()
            return {}
    
    def get_workflow_logs(self, workflow_id: str, include_automa_logs: bool = True, include_debug: bool = True) -> dict:
        """Retrieve all logs for a specific workflow including debug information"""
        try:
            db, client = get_mongo_db()
            
            log_collection = db[self.log_collection_name]
            main_log = log_collection.find_one({'workflow_id': workflow_id})
            
            if not main_log:
                client.close()
                return {'error': 'Workflow logs not found'}
            
            result = {
                'workflow_id': workflow_id,
                'main_log': main_log,
                'automa_logs': None,
                'execution_logs': None,
                'console_logs': None,
                'debug_logs': None,
                'performance_metrics': None
            }
            
            if include_automa_logs and main_log.get('automa_log_id'):
                automa_logs_collection = db[self.automa_logs_collection]
                result['automa_logs'] = automa_logs_collection.find_one(
                    {'_id': ObjectId(main_log['automa_log_id'])}
                )
            
            if main_log.get('execution_log_id'):
                execution_logs_collection = db[self.execution_logs_collection]
                result['execution_logs'] = execution_logs_collection.find_one(
                    {'_id': ObjectId(main_log['execution_log_id'])}
                )
            
            console_logs_collection = db[self.automa_console_logs]
            console_logs = list(console_logs_collection.find({'workflow_id': workflow_id}))
            result['console_logs'] = console_logs
            
            if include_debug:
                debug_logs_collection = db[self.automa_debug_logs]
                debug_logs = list(debug_logs_collection.find({'workflow_id': workflow_id}))
                result['debug_logs'] = debug_logs
            
            client.close()
            logger.info(f"✅ Retrieved comprehensive logs for workflow: {workflow_id}")
            return result
            
        except Exception as e:
            logger.error(f"❌ Failed to retrieve workflow logs: {e}")
            if 'client' in locals():
                client.close()
            return {'error': str(e)}
    
    def get_debug_summary(self, dag_run_id: str = None) -> dict:
        """Get a summary of debug information captured"""
        try:
            db, client = get_mongo_db()
            
            query_filter = {}
            if dag_run_id:
                query_filter['dag_context.dag_run_id'] = dag_run_id
            
            log_collection = db[self.log_collection_name]
            debug_logs_collection = db[self.automa_debug_logs]
            
            workflows = list(log_collection.find(query_filter))
            debug_entries = list(debug_logs_collection.find())
            
            summary = {
                'total_workflows': len(workflows),
                'workflows_with_debug': len([wf for wf in workflows if wf.get('debug_logs_available', False)]),
                'total_debug_entries': len(debug_entries),
                'debug_statistics': {
                    'console_logs_captured': 0,
                    'workflow_states_captured': 0,
                    'performance_data_captured': 0
                },
                'common_issues': [],
                'generated_at': datetime.now().isoformat()
            }
            
            for debug_entry in debug_entries:
                debug_data = debug_entry.get('debug_data', {})
                if debug_data.get('console_logs', {}).get('console_messages'):
                    summary['debug_statistics']['console_logs_captured'] += 1
                if debug_data.get('workflow_state', {}).get('automa_state'):
                    summary['debug_statistics']['workflow_states_captured'] += 1
                if debug_data.get('workflow_state', {}).get('performance'):
                    summary['debug_statistics']['performance_data_captured'] += 1
            
            client.close()
            return summary
            
        except Exception as e:
            logger.error(f"❌ Failed to get debug summary: {e}")
            if 'client' in locals():
                client.close()
            return {'error': str(e)}
    
    def cleanup_old_logs(self, days_to_keep: int = 30) -> dict:
        """Clean up old log entries to prevent database bloat"""
        try:
            db, client = get_mongo_db()
            cutoff_date = datetime.now() - timedelta(days=days_to_keep)
            
            collections_to_clean = [
                self.log_collection_name,
                self.execution_logs_collection,
                self.automa_logs_collection,
                self.automa_console_logs,
                self.automa_debug_logs,
                self.performance_metrics_collection
            ]
            
            cleanup_results = {}
            
            for collection_name in collections_to_clean:
                collection = db[collection_name]
                result = collection.delete_many({'created_at': {'$lt': cutoff_date}})
                cleanup_results[collection_name] = {
                    'deleted_count': result.deleted_count
                }
                logger.info(f"Cleaned up {result.deleted_count} old entries from {collection_name}")
            
            client.close()
            return {
                'cleanup_completed': True,
                'cutoff_date': cutoff_date.isoformat(),
                'results': cleanup_results
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to cleanup old logs: {e}")
            if 'client' in locals():
                client.close()
            return {'error': str(e)}