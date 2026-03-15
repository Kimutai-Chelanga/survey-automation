import json
import time
import logging
import websocket
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebSocketResponseHandler:
    """Enhanced WebSocket response handler with comprehensive debugging"""
    
    def __init__(self):
        self.debug_mode = True
        self.response_patterns = {
            'workflow_injection_success': [
                'success',
                'workflows: workflows',
                'storage successful',
                'injection_success',
                'workflows injected'
            ],
            'workflow_injection_error': [
                'chrome_storage_unavailable',
                'storage_error',
                'storage_timeout',
                'injection_exception',
                'runtime_exception'
            ],
            'console_workflow_log': [
                '[WORKFLOW-INJECT]',
                '[LOG-CAPTURE]',
                '[AUTOMA-DEBUG]'
            ]
        }
        self.captured_responses = []
        self.debug_info = {
            'total_responses_processed': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'console_messages_captured': 0
        }
    
    def log_debug(self, message: str, data: Any = None):
        """Debug logging with optional data"""
        if self.debug_mode:
            timestamp = datetime.now().isoformat()
            debug_msg = f"[WS-DEBUG {timestamp}] {message}"
            if data:
                debug_msg += f" | Data: {json.dumps(data) if not isinstance(data, str) else data}"
            logger.debug(debug_msg)
    
    def extract_workflows_from_response(self, response_data: dict) -> Optional[List[dict]]:
        """Enhanced workflow extraction with comprehensive debugging and robust error handling"""
        self.debug_info['total_responses_processed'] += 1
        self.log_debug("Processing response", {
            "response_type": str(type(response_data)),  # Convert type to string for safe serialization
            "keys": list(response_data.keys()) if isinstance(response_data, dict) else "not_dict",
            "preview": str(response_data)[:200] if response_data else "empty"  # Safe preview
        })

        try:
            # Validate input is a dictionary
            if not isinstance(response_data, dict):
                self.log_debug("Invalid response format", {
                    "preview": str(response_data)[:200] if response_data else "empty"
                })
                self.debug_info['failed_extractions'] += 1
                return None

            # Method 1: Direct result extraction
            if 'result' in response_data and 'value' in response_data['result']:
                result_value = response_data['result']['value']
                self.log_debug("Found result.value", {
                    "type": str(type(result_value)),  # Convert type to string
                    "length": len(str(result_value)) if result_value else 0,
                    "preview": str(result_value)[:200] if result_value else "empty"
                })

                if isinstance(result_value, str):
                    try:
                        # Try to parse as JSON
                        if result_value.startswith('[') or result_value.startswith('{'):
                            parsed_data = json.loads(result_value)
                            workflows = self._extract_workflows_from_parsed_data(parsed_data)
                            if workflows:
                                self.debug_info['successful_extractions'] += 1
                                self.log_debug("Successfully extracted workflows", {
                                    "count": len(workflows),
                                    "source": "result_value_json"
                                })
                                return workflows
                        # Check for workflow count in console messages
                        if '[WORKFLOW-INJECT]' in result_value:
                            workflow_count = self._extract_workflow_count_from_console(result_value)
                            if workflow_count is not None:
                                self.debug_info['console_messages_captured'] += 1
                                self.log_debug("Extracted workflow count from console", {
                                    "count": workflow_count,
                                    "source": "console_message"
                                })
                                return [{'console_reported_count': workflow_count, 'source': 'console_message'}]
                    except json.JSONDecodeError as e:
                        self.log_debug("JSON decode error", {
                            "error": str(e),
                            "value_preview": result_value[:200]
                        })
                        self.debug_info['failed_extractions'] += 1
                        return None

                # Handle numeric result (e.g., Response 4: 72938 from log)
                if isinstance(result_value, (int, float)):
                    self.log_debug("Found numeric result, treating as workflow count", {
                        "value": result_value,
                        "source": "numeric_result"
                    })
                    self.debug_info['successful_extractions'] += 1
                    return [{'console_reported_count': int(result_value), 'source': 'numeric_result'}]

            # Method 2: Console API messages
            if response_data.get('method') == 'Runtime.consoleAPICalled':
                console_message = self._process_console_message(response_data)
                if console_message:
                    self.debug_info['console_messages_captured'] += 1
                    self.log_debug("Extracted console message", {
                        "message_preview": str(console_message)[:200],
                        "source": "console_api"
                    })
                    return console_message

            # Method 3: Check params for console messages
            if 'params' in response_data:
                params = response_data['params']
                if isinstance(params, dict):
                    console_result = self._extract_from_console_params(params)
                    if console_result:
                        self.debug_info['console_messages_captured'] += 1
                        self.log_debug("Extracted from console params", {
                            "result_preview": str(console_result)[:200],
                            "source": "console_params"
                        })
                        return console_result

            # Method 4: Check for error responses
            if 'error' in response_data:
                error_info = response_data['error']
                error_message = error_info.get('message', 'Unknown error') if isinstance(error_info, dict) else str(error_info)
                self.log_debug("Error response detected", {
                    "error_message": error_message[:200],
                    "source": "error_response"
                })
                self.debug_info['failed_extractions'] += 1
                return None

            # Method 5: Exception detection in result
            if 'result' in response_data and 'exceptionDetails' in response_data['result']:
                exception_details = response_data['result']['exceptionDetails']
                exception_text = exception_details.get('text', 'Unknown exception')
                self.log_debug("Exception details found", {
                    "exception": exception_text[:200],
                    "source": "exception_details"
                })
                self.debug_info['failed_extractions'] += 1
                return None

            self.log_debug("No workflows extracted from response", {
                "reason": "no_matching_extraction_method"
            })
            self.debug_info['failed_extractions'] += 1
            return None

        except Exception as e:
            self.debug_info['failed_extractions'] += 1
            self.log_debug("Exception during extraction", {
                "error": str(e),
                "error_type": str(type(e)),  # Convert type to string
                "response_preview": str(response_data)[:200] if response_data else "empty"
            })
            return None
    
    def _extract_workflows_from_parsed_data(self, parsed_data: Any) -> Optional[List[dict]]:
        """Extract workflows from parsed JSON data"""
        self.log_debug("Extracting workflows from parsed data", {"type": type(parsed_data)})
        
        if isinstance(parsed_data, list):
            # Direct list of workflows
            if all(isinstance(item, dict) and ('id' in item or 'name' in item) for item in parsed_data):
                self.log_debug("Found direct workflow list", {"count": len(parsed_data)})
                return parsed_data
        
        elif isinstance(parsed_data, dict):
            # Check for workflows in various keys
            possible_keys = ['workflows', 'workflowLogs', 'data', 'result']
            for key in possible_keys:
                if key in parsed_data and isinstance(parsed_data[key], list):
                    workflows = parsed_data[key]
                    if workflows and all(isinstance(wf, dict) for wf in workflows):
                        self.log_debug(f"Found workflows in key '{key}'", {"count": len(workflows)})
                        return workflows
            
            # Check if the dict itself represents workflow data
            if 'workflows' in parsed_data:
                workflows_data = parsed_data['workflows']
                if isinstance(workflows_data, list):
                    self.log_debug("Found workflows key with list", {"count": len(workflows_data)})
                    return workflows_data
                elif isinstance(workflows_data, dict):
                    # Convert dict to list
                    workflows_list = list(workflows_data.values()) if workflows_data else []
                    self.log_debug("Converted workflows dict to list", {"count": len(workflows_list)})
                    return workflows_list
        
        return None
    
    def _extract_workflow_count_from_console(self, console_text: str) -> Optional[int]:
        """Extract workflow count from console messages"""
        patterns = [
            r'\[WORKFLOW-INJECT\].*?(\d+)\s+workflows',
            r'Found (\d+) workflows',
            r'Returning (\d+) workflows',
            r'injected (\d+) workflows',
            r'Storage successful.*?(\d+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, console_text, re.IGNORECASE)
            if match:
                count = int(match.group(1))
                self.log_debug("Extracted workflow count from console", {"count": count, "pattern": pattern})
                return count
        
        return None
    
    def _process_console_message(self, response_data: dict) -> Optional[List[dict]]:
        """Process console API messages for workflow information"""
        params = response_data.get('params', {})
        if not params:
            return None
        
        args = params.get('args', [])
        if not args:
            return None
        
        message_text = ' '.join(str(arg.get('value', '')) for arg in args if isinstance(arg, dict))
        
        # Check for workflow injection messages
        for pattern_type, patterns in self.response_patterns.items():
            for pattern in patterns:
                if pattern in message_text:
                    self.log_debug(f"Found console pattern: {pattern_type}", {"pattern": pattern, "message": message_text[:200]})
                    
                    if pattern_type == 'console_workflow_log':
                        workflow_count = self._extract_workflow_count_from_console(message_text)
                        if workflow_count is not None:
                            return [{'console_reported_count': workflow_count, 'source': 'console_api'}]
        
        return None
    
    def _extract_from_console_params(self, params: dict) -> Optional[List[dict]]:
        """Extract workflow information from console parameters"""
        # Check for console messages in params
        if 'args' in params:
            args = params['args']
            if isinstance(args, list):
                for arg in args:
                    if isinstance(arg, dict) and 'value' in arg:
                        value = arg['value']
                        if isinstance(value, str) and any(pattern in value for pattern in self.response_patterns['console_workflow_log']):
                            workflow_count = self._extract_workflow_count_from_console(value)
                            if workflow_count is not None:
                                return [{'console_reported_count': workflow_count, 'source': 'console_params'}]
        
        return None
    
    def is_injection_success(self, response_data: dict) -> Tuple[bool, str]:
        """Enhanced injection success detection with detailed messaging"""
        self.log_debug("Checking injection success", {"response_keys": list(response_data.keys()) if isinstance(response_data, dict) else "not_dict"})
        
        try:
            # Method 1: Check result value
            if 'result' in response_data and 'value' in response_data['result']:
                result_value = response_data['result']['value']
                self.log_debug("Checking result value", {"type": type(result_value), "value": str(result_value)[:100]})
                
                if isinstance(result_value, str):
                    # Check for success patterns
                    for pattern in self.response_patterns['workflow_injection_success']:
                        if pattern in result_value.lower():
                            self.log_debug("Found success pattern", {"pattern": pattern})
                            return True, f"injection_success_{pattern.replace(' ', '_')}"
                    
                    # Check for error patterns
                    for pattern in self.response_patterns['workflow_injection_error']:
                        if pattern in result_value.lower():
                            self.log_debug("Found error pattern", {"pattern": pattern})
                            return False, f"injection_error_{pattern.replace(' ', '_')}"
            
            # Method 2: Console API messages
            if response_data.get('method') == 'Runtime.consoleAPICalled':
                params = response_data.get('params', {})
                args = params.get('args', [])
                
                message_text = ' '.join(str(arg.get('value', '')) for arg in args if isinstance(arg, dict))
                self.log_debug("Processing console API message", {"message": message_text[:200]})
                
                # Check for success in console messages
                if any(pattern in message_text.lower() for pattern in self.response_patterns['workflow_injection_success']):
                    return True, "console_injection_success"
                
                # Check for errors in console messages
                if any(pattern in message_text.lower() for pattern in self.response_patterns['workflow_injection_error']):
                    return False, "console_injection_error"
            
            # Method 3: Error responses
            if 'error' in response_data:
                error_info = response_data['error']
                error_message = error_info.get('message', '') if isinstance(error_info, dict) else str(error_info)
                self.log_debug("Found error response", {"error": error_message})
                return False, f"runtime_error: {error_message}"
            
            # Method 4: Exception detection in result
            if 'result' in response_data:
                result = response_data['result']
                if isinstance(result, dict):
                    if 'exceptionDetails' in result:
                        exception_details = result['exceptionDetails']
                        exception_text = exception_details.get('text', 'Unknown exception')
                        self.log_debug("Found exception details", {"exception": exception_text})
                        return False, f"runtime_exception: {exception_text}"
            
            self.log_debug("No clear success/failure indicators found")
            return False, "no_clear_response"
            
        except Exception as e:
            self.log_debug("Exception in injection success check", {"error": str(e)})
            return False, f"success_check_exception: {str(e)}"
    
    def get_debug_summary(self) -> dict:
        """Get debug summary of WebSocket response processing"""
        return {
            'debug_info': self.debug_info,
            'response_patterns': self.response_patterns,
            'captured_responses_count': len(self.captured_responses),
            'debug_mode': self.debug_mode
        }
    
    def reset_debug_info(self):
        """Reset debug information counters"""
        self.debug_info = {
            'total_responses_processed': 0,
            'successful_extractions': 0,
            'failed_extractions': 0,
            'console_messages_captured': 0
        }
        self.captured_responses = []

def verify_websocket_connection(**kwargs) -> bool:
    """Enhanced WebSocket connection verification with better timeout handling and fallback strategies"""
    try:
        from .chrome_automa import find_automa_context, create_automa_page, ws_manager
        
        logger.info("🔌 Starting enhanced WebSocket connection verification...")
        
        # Try to find existing context first
        debugger_url = find_automa_context()
        if not debugger_url:
            logger.info("📄 No existing context found, creating new Automa page...")
            debugger_url = create_automa_page()
        
        if not debugger_url:
            logger.error("❌ Failed to establish debugger connection")
            return False
        
        logger.info(f"🔗 Testing WebSocket connection to: {debugger_url}")
        
        # Test connection with multiple strategies
        verification_strategies = [
            _verify_simple_eval,
            _verify_chrome_runtime,
            _verify_basic_ping
        ]
        
        for i, strategy in enumerate(verification_strategies, 1):
            try:
                logger.info(f"🧪 Trying verification strategy {i}/{len(verification_strategies)}: {strategy.__name__}")
                if strategy(debugger_url):
                    logger.info(f"✅ Verification successful with strategy: {strategy.__name__}")
                    return True
                else:
                    logger.warning(f"⚠️ Strategy {strategy.__name__} failed, trying next...")
            except Exception as e:
                logger.warning(f"⚠️ Strategy {strategy.__name__} threw exception: {e}")
                continue
        
        logger.error("❌ All verification strategies failed")
        return False
            
    except Exception as e:
        logger.error(f"❌ WebSocket verification failed with exception: {e}")
        return False

def _verify_simple_eval(ws_url: str) -> bool:
    """Simple evaluation test - most basic verification"""
    try:
        ws = websocket.create_connection(ws_url, timeout=10)
        
        test_script = "2 + 2"
        message_id = int(time.time() * 1000) % 100000
        
        test_message = {
            "id": message_id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": test_script,
                "returnByValue": True
            }
        }
        
        # Clear any pending messages
        ws.settimeout(1)
        try:
            while True:
                ws.recv()
        except websocket.WebSocketTimeoutException:
            pass
        
        # Send test message
        ws.settimeout(5)  # Shorter timeout for basic test
        ws.send(json.dumps(test_message))
        
        # Wait for response
        for attempt in range(3):
            try:
                response = ws.recv()
                response_data = json.loads(response)
                
                if response_data.get('id') == message_id:
                    result = response_data.get('result', {})
                    if result.get('result', {}).get('value') == 4:
                        logger.info("✅ Simple evaluation test passed (2+2=4)")
                        ws.close()
                        return True
                
            except websocket.WebSocketTimeoutException:
                logger.debug(f"⏰ Simple eval timeout on attempt {attempt + 1}")
                continue
        
        ws.close()
        return False
        
    except Exception as e:
        logger.debug(f"❌ Simple eval test failed: {e}")
        return False

def _verify_chrome_runtime(ws_url: str) -> bool:
    """Chrome runtime availability test"""
    try:
        ws = websocket.create_connection(ws_url, timeout=10)
        
        test_script = """
        (() => {
            try {
                return {
                    chrome_available: typeof chrome !== 'undefined',
                    timestamp: Date.now(),
                    user_agent: navigator.userAgent.substring(0, 50)
                };
            } catch (e) {
                return { error: e.message };
            }
        })();
        """
        
        message_id = int(time.time() * 1000) % 100000
        test_message = {
            "id": message_id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": test_script,
                "returnByValue": True
            }
        }
        
        # Clear pending messages
        ws.settimeout(1)
        try:
            while True:
                ws.recv()
        except websocket.WebSocketTimeoutException:
            pass
        
        # Send and wait for response
        ws.settimeout(8)
        ws.send(json.dumps(test_message))
        
        for attempt in range(5):
            try:
                response = ws.recv()
                response_data = json.loads(response)
                
                if response_data.get('id') == message_id:
                    result = response_data.get('result', {}).get('result', {})
                    value = result.get('value')
                    
                    if isinstance(value, dict) and 'timestamp' in value:
                        chrome_available = value.get('chrome_available', False)
                        logger.info(f"✅ Chrome runtime test passed - Chrome API: {'Available' if chrome_available else 'Not Available'}")
                        ws.close()
                        return True
                
            except websocket.WebSocketTimeoutException:
                logger.debug(f"⏰ Chrome runtime timeout on attempt {attempt + 1}")
                continue
        
        ws.close()
        return False
        
    except Exception as e:
        logger.debug(f"❌ Chrome runtime test failed: {e}")
        return False

def _verify_basic_ping(ws_url: str) -> bool:
    """Basic WebSocket ping test"""
    try:
        ws = websocket.create_connection(ws_url, timeout=10)
        
        # Try WebSocket ping/pong if supported
        try:
            ws.ping("test")
            logger.info("✅ WebSocket ping/pong test passed")
            ws.close()
            return True
        except Exception as ping_error:
            logger.debug(f"WebSocket ping failed: {ping_error}")
        
        # Fallback: try to enable runtime and check response
        enable_runtime_message = {
            "id": 1,
            "method": "Runtime.enable"
        }
        
        ws.settimeout(5)
        ws.send(json.dumps(enable_runtime_message))
        
        try:
            response = ws.recv()
            response_data = json.loads(response)
            
            if response_data.get('id') == 1:
                logger.info("✅ Runtime.enable test passed")
                ws.close()
                return True
        except websocket.WebSocketTimeoutException:
            logger.debug("⏰ Runtime.enable timeout")
        
        ws.close()
        return False
        
    except Exception as e:
        logger.debug(f"❌ Basic ping test failed: {e}")
        return False

class EnhancedWebSocketManager:
    """Enhanced WebSocket manager with better connection handling and fallback strategies"""
    
    def __init__(self):
        self.connections = {}
        self.connection_stats = {}
        self.debug_mode = True
        self.max_connection_attempts = 3
        self.connection_timeout = 15
    
    def get_connection(self, ws_url: str, force_new: bool = False):
        """Get or create WebSocket connection with enhanced error handling and retry logic"""
        try:
            if not force_new and ws_url in self.connections:
                ws = self.connections[ws_url]
                if self._test_connection_health(ws_url):
                    return ws
                else:
                    logger.info(f"🔄 Refreshing unhealthy connection: {ws_url[:50]}...")
                    self._close_connection(ws_url)
            
            # Try multiple connection attempts with progressive timeout
            for attempt in range(self.max_connection_attempts):
                try:
                    timeout = self.connection_timeout + (attempt * 5)  # Progressive timeout
                    logger.info(f"🔌 Creating WebSocket connection (attempt {attempt + 1}/{self.max_connection_attempts}, timeout: {timeout}s)")
                    
                    ws = websocket.create_connection(
                        ws_url, 
                        timeout=timeout,
                        header=["Origin: http://localhost:9222"],
                        enable_multithread=True
                    )
                    
                    # Test the connection immediately
                    ws.settimeout(5)
                    test_msg = {"id": 999, "method": "Runtime.enable"}
                    ws.send(json.dumps(test_msg))
                    
                    # Try to receive response (don't wait too long)
                    try:
                        response = ws.recv()
                        logger.debug("✅ Connection test successful")
                    except websocket.WebSocketTimeoutException:
                        logger.debug("⚠️ Connection test timeout, but connection seems stable")
                    
                    # Store connection
                    self.connections[ws_url] = ws
                    self.connection_stats[ws_url] = {
                        'created_at': datetime.now(),
                        'messages_sent': 0,
                        'health_checks': 0,
                        'last_activity': datetime.now(),
                        'attempt_used': attempt + 1
                    }
                    
                    logger.info(f"✅ WebSocket connection established on attempt {attempt + 1}")
                    return ws
                    
                except Exception as e:
                    logger.warning(f"❌ Connection attempt {attempt + 1} failed: {e}")
                    if attempt == self.max_connection_attempts - 1:
                        raise
                    time.sleep(2 ** attempt)  # Exponential backoff
            
            return None
            
        except Exception as e:
            logger.error(f"❌ Failed to create WebSocket connection after all attempts: {e}")
            return None
    
    def _test_connection_health(self, ws_url: str) -> bool:
        """Enhanced connection health testing with multiple strategies"""
        if ws_url not in self.connections:
            return False
        
        try:
            ws = self.connections[ws_url]
            
            # Strategy 1: Try ping if supported
            try:
                ws.ping("health")
                self.connection_stats[ws_url]['health_checks'] += 1
                return True
            except:
                pass
            
            # Strategy 2: Try a simple runtime command
            try:
                test_message = {
                    "id": 999998,
                    "method": "Runtime.evaluate",
                    "params": {
                        "expression": "1",
                        "returnByValue": True
                    }
                }
                
                original_timeout = ws.gettimeout()
                ws.settimeout(3)
                ws.send(json.dumps(test_message))
                
                # Don't wait for response, just test if send works
                ws.settimeout(original_timeout)
                self.connection_stats[ws_url]['health_checks'] += 1
                self.connection_stats[ws_url]['last_activity'] = datetime.now()
                return True
                
            except Exception as e:
                logger.debug(f"Health check failed for {ws_url[:50]}: {e}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Health check failed for {ws_url[:50]}: {e}")
            return False
    
    def record_message(self, ws_url: str):
        """Record message activity for connection statistics"""
        if ws_url in self.connection_stats:
            self.connection_stats[ws_url]['messages_sent'] += 1
            self.connection_stats[ws_url]['last_activity'] = datetime.now()
    
    def _close_connection(self, ws_url: str):
        """Close and clean up WebSocket connection"""
        if ws_url in self.connections:
            try:
                self.connections[ws_url].close()
            except:
                pass
            del self.connections[ws_url]
            logger.info(f"🔌 Closed WebSocket connection: {ws_url[:50]}...")
    
    def close_all(self):
        """Close all WebSocket connections"""
        for ws_url in list(self.connections.keys()):
            self._close_connection(ws_url)
        logger.info("🔌 All WebSocket connections closed")
    
    def get_connection_stats(self) -> dict:
        """Get statistics for all connections"""
        return {
            'total_connections': len(self.connections),
            'connection_details': self.connection_stats
        }

# Create enhanced WebSocket manager instance
enhanced_ws_manager = EnhancedWebSocketManager()