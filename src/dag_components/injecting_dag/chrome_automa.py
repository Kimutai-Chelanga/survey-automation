import requests
import websocket
import json
import time
import socket
import threading
from queue import Queue, Empty
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from .config import CHROME_DEBUG_URL

class WebSocketManager:
    """Enhanced WebSocket manager with connection pooling, health monitoring, and better error handling"""
    
    def __init__(self, max_connections=3):
        self.connections = {}
        self.connection_health = {}
        self.max_connections = max_connections
        self.lock = threading.Lock()
        self.last_health_check = 0
        self.health_check_interval = 30  # seconds
    
    def get_connection(self, ws_url, force_new=False, max_attempts=3):
        """Get a WebSocket connection with improved health checking and retry logic"""
        with self.lock:
            # Check if we need to refresh connections
            current_time = time.time()
            if current_time - self.last_health_check > self.health_check_interval:
                self._cleanup_dead_connections()
                self.last_health_check = current_time
            
            # Return existing healthy connection
            if not force_new and ws_url in self.connections:
                if self._test_connection_health(ws_url):
                    return self.connections[ws_url]
                else:
                    self._close_connection(ws_url)
            
            # Create new connection with retry logic
            last_error = None
            for attempt in range(max_attempts):
                try:
                    timeout = 30 + (attempt * 10)  # Progressive timeout
                    print(f"🔌 Creating WebSocket connection (attempt {attempt + 1}/{max_attempts}, timeout: {timeout}s)")
                    
                    connection = websocket.create_connection(
                        ws_url, 
                        timeout=timeout,
                        header=["Origin: http://localhost:9222"],
                        sockopt=[(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)],
                        enable_multithread=True
                    )
                    
                    # Test the connection immediately
                    self._test_new_connection(connection)
                    
                    self.connections[ws_url] = connection
                    self.connection_health[ws_url] = {
                        'created_at': current_time,
                        'last_used': current_time,
                        'message_count': 0,
                        'attempt_used': attempt + 1
                    }
                    
                    print(f"✅ WebSocket connection established on attempt {attempt + 1}")
                    return connection
                    
                except Exception as e:
                    last_error = e
                    print(f"❌ Connection attempt {attempt + 1} failed: {e}")
                    if attempt < max_attempts - 1:
                        wait_time = 2 ** attempt  # Exponential backoff
                        print(f"⏳ Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
            
            print(f"❌ Failed to create WebSocket connection after {max_attempts} attempts")
            raise Exception(f"WebSocket connection failed after {max_attempts} attempts: {last_error}")
    
    def _test_new_connection(self, connection):
        """Test a newly created connection"""
        try:
            # Send a simple enable runtime command
            test_msg = {"id": 999, "method": "Runtime.enable"}
            connection.settimeout(5)
            connection.send(json.dumps(test_msg))
            
            # Don't wait for response, just ensure send works
            print("✅ Connection test message sent successfully")
            
        except Exception as e:
            print(f"⚠️ Connection test failed: {e}")
            # Don't fail on test - connection might still work
    
    def _test_connection_health(self, ws_url):
        """Test if a WebSocket connection is still alive with multiple strategies"""
        if ws_url not in self.connections:
            return False
            
        connection = self.connections[ws_url]
        
        # Strategy 1: Try ping frame if supported
        try:
            connection.ping("health_check")
            return True
        except Exception:
            pass
        
        # Strategy 2: Try sending a simple message
        try:
            test_msg = {"id": 999997, "method": "Runtime.enable"}
            original_timeout = connection.gettimeout()
            connection.settimeout(3)
            connection.send(json.dumps(test_msg))
            connection.settimeout(original_timeout)
            return True
        except Exception:
            pass
        
        return False
    
    def _cleanup_dead_connections(self):
        """Remove dead connections from the pool"""
        dead_connections = []
        for ws_url in list(self.connections.keys()):
            if not self._test_connection_health(ws_url):
                dead_connections.append(ws_url)
        
        for ws_url in dead_connections:
            self._close_connection(ws_url)
            print(f"🧹 Cleaned up dead connection: {ws_url[:50]}...")
    
   
    def _close_connection(self, ws_url):
        """Close and remove a specific connection"""
        if ws_url in self.connections:
            try:
                self.connections[ws_url].close()
            except:
                pass
            del self.connections[ws_url]
            
        if ws_url in self.connection_health:
            del self.connection_health[ws_url]
    
    def record_message(self, ws_url):
        """Record that a message was sent through this connection"""
        if ws_url in self.connection_health:
            self.connection_health[ws_url]['message_count'] += 1
            self.connection_health[ws_url]['last_used'] = time.time()
    
    def close_all(self):
        """Close all WebSocket connections"""
        with self.lock:
            for ws_url in list(self.connections.keys()):
                self._close_connection(ws_url)

class WorkflowQueue:
    """Queue system for managing workflow processing with retry logic"""
    
    def __init__(self, max_retries=3, retry_delay=5):
        self.queue = Queue()
        self.retry_queue = Queue()
        self.failed_queue = Queue()
        self.processed = []
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.lock = threading.Lock()
    
    def add_workflow(self, workflow_data):
        """Add workflow to the processing queue"""
        workflow_item = {
            'workflow_data': workflow_data,
            'retry_count': 0,
            'added_at': time.time()
        }
        self.queue.put(workflow_item)
    
    def add_workflows_batch(self, workflows_batch):
        """Add multiple workflows to the queue"""
        for workflow_data in workflows_batch:
            self.add_workflow(workflow_data)
    
    def get_next_workflow(self, timeout=None):
        """Get the next workflow to process"""
        try:
            # First try the retry queue
            try:
                item = self.retry_queue.get_nowait()
                return item
            except Empty:
                pass
            
            # Then try the main queue
            return self.queue.get(timeout=timeout)
            
        except Empty:
            return None
    
    def mark_success(self, workflow_item):
        """Mark workflow as successfully processed"""
        with self.lock:
            self.processed.append({
                'workflow_name': workflow_item['workflow_data']['automaWf']['name'],
                'status': 'success',
                'processed_at': time.time(),
                'retry_count': workflow_item['retry_count']
            })
    
    def mark_failure(self, workflow_item, error_message):
        """Mark workflow as failed and decide on retry"""
        workflow_item['retry_count'] += 1
        workflow_item['last_error'] = error_message
        workflow_item['failed_at'] = time.time()
        
        if workflow_item['retry_count'] < self.max_retries:
            # Add back to retry queue with delay
            print(f"🔄 Queuing workflow for retry {workflow_item['retry_count']}/{self.max_retries}: {workflow_item['workflow_data']['automaWf']['name']}")
            time.sleep(self.retry_delay)
            self.retry_queue.put(workflow_item)
        else:
            # Move to failed queue
            print(f"❌ Workflow failed permanently after {self.max_retries} retries: {workflow_item['workflow_data']['automaWf']['name']}")
            self.failed_queue.put(workflow_item)
    
    def get_queue_stats(self):
        """Get statistics about the queue processing"""
        with self.lock:
            return {
                'pending': self.queue.qsize(),
                'retry_pending': self.retry_queue.qsize(),
                'failed': self.failed_queue.qsize(),
                'processed_success': len([p for p in self.processed if p['status'] == 'success']),
                'total_processed': len(self.processed)
            }

# Global instances
ws_manager = WebSocketManager()
workflow_queue = WorkflowQueue()

def check_chrome_and_automa_health():
    """Enhanced Chrome health check with better error handling"""
    max_retries = 5
    for attempt in range(max_retries):
        try:
            response = requests.get('http://localhost:9222/json/version', timeout=10)
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Chrome is healthy: {data.get('Product', 'Unknown version')}")
                break
        except Exception as e:
            print(f"Chrome health attempt {attempt + 1}/{max_retries} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
            else:
                raise Exception("Chrome health check failed after all retries")
    
    try:
        tabs_response = requests.get('http://localhost:9222/json', timeout=10)
        if tabs_response.status_code == 200:
            tabs = tabs_response.json()
            print(f"✅ Chrome has {len(tabs)} tab(s) available")
            
            # Check for WebSocket availability
            websocket_tabs = [tab for tab in tabs if tab.get('webSocketDebuggerUrl')]
            print(f"✅ Found {len(websocket_tabs)} tabs with WebSocket support")
            
            automa_tabs = [tab for tab in tabs if 'chrome-extension' in tab.get('url', '') or 'newtab' in tab.get('url', '')]
            if automa_tabs:
                print(f"✅ Found {len(automa_tabs)} extension tab(s)")
            else:
                print("ℹ️ No extension tabs found yet, but Chrome is ready")
        else:
            raise Exception(f"Failed to get Chrome tabs: {tabs_response.status_code}")
    except Exception as e:
        print(f"Warning: Could not check Chrome tabs: {e}")
    
    return True

def monitor_chrome_health():
    """Monitor Chrome process health"""
    try:
        response = requests.get('http://localhost:9222/json/version', timeout=5)
        if response.status_code == 200:
            return True
        else:
            print(f"⚠️ Chrome health check failed with status: {response.status_code}")
            return False
    except Exception as e:
        print(f"⚠️ Chrome health check failed: {e}")
        return False

def get_chrome_tabs():
    """Get all Chrome tabs and pages with better error handling"""
    try:
        response = requests.get(CHROME_DEBUG_URL, timeout=600)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"❌ Failed to get Chrome targets, status: {response.status_code}")
            return []
    except Exception as e:
        print(f"❌ Failed to get Chrome targets: {e}")
        return []

def find_automa_context():
    """Find Automa extension context with improved logic"""
    tabs = get_chrome_tabs()
    
    if not tabs:
        print("❌ No Chrome tabs available")
        return None
    
    print("🔍 Available Chrome contexts:")
    for i, tab in enumerate(tabs):
        title = tab.get('title', 'Unknown')
        url = tab.get('url', 'Unknown')
        tab_type = tab.get('type', 'Unknown')
        ws_url = tab.get('webSocketDebuggerUrl', 'No WebSocket')
        print(f"  {i+1}. {title} ({tab_type}) - {url[:60]}... - WS: {'✅' if ws_url != 'No WebSocket' else '❌'}")
    
    # Priority 1: Background pages with Automa
    for tab in tabs:
        if (tab.get('type') == 'background_page' and 
            ('automa' in tab.get('title', '').lower() or 
             'chrome-extension' in tab.get('url', '')) and
            tab.get('webSocketDebuggerUrl')):
            print(f"✅ Found background page: {tab.get('title')}")
            return tab.get('webSocketDebuggerUrl')
    
    # Priority 2: Extension pages with Automa
    for tab in tabs:
        url = tab.get('url', '').lower()
        if ('chrome-extension' in url and 'automa' in url and 
            tab.get('webSocketDebuggerUrl')):
            print(f"✅ Found extension page: {tab.get('title')}")
            return tab.get('webSocketDebuggerUrl')
    
    # Priority 3: New tab pages or Automa-related pages
    for tab in tabs:
        url = tab.get('url', '').lower()
        title = tab.get('title', '').lower()
        if (('newtab.html' in url or 'automa' in title) and 
            tab.get('webSocketDebuggerUrl')):
            print(f"✅ Found Automa-related page: {tab.get('title')}")
            return tab.get('webSocketDebuggerUrl')
    
    # Priority 4: Any page with WebSocket support
    for tab in tabs:
        if tab.get('webSocketDebuggerUrl') and tab.get('type') == 'page':
            print(f"✅ Found page with WebSocket support: {tab.get('title')}")
            return tab.get('webSocketDebuggerUrl')
    
    print("❌ No suitable context found")
    return None

def create_automa_page():
    """Create a new tab and navigate to Automa extension with better error handling"""
    try:
        print("🔧 Creating new Automa page...")
        response = requests.get(f"{CHROME_DEBUG_URL}/new", timeout=10)
        
        if response.status_code != 200:
            print(f"❌ Failed to create new tab, status: {response.status_code}")
            return None
        
        new_tab = response.json()
        
        if 'webSocketDebuggerUrl' not in new_tab:
            print("❌ New tab doesn't have WebSocket support")
            return None
        
        ws_url = new_tab['webSocketDebuggerUrl']
        print(f"🔗 Created new tab with WebSocket: {ws_url[:50]}...")
        
        try:
            ws = websocket.create_connection(ws_url, timeout=15)
            
            # Navigate to Automa extension
            navigate_cmd = {
                "id": 1,
                "method": "Page.navigate",
                "params": {"url": "chrome-extension://infppggnoaenmfagbfknfkancpbljcca/newtab.html"}
            }
            ws.send(json.dumps(navigate_cmd))
            result = ws.recv()
            
            print("✅ Successfully navigated to Automa extension")
            time.sleep(5)  # Allow page to load
            ws.close()
            return ws_url
            
        except Exception as e:
            print(f"⚠️ Failed to navigate to Automa page: {e}")
            return ws_url  # Return the WebSocket URL anyway, might still work
        
    except Exception as e:
        print(f"❌ Failed to create Automa page: {e}")
        return None

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=2, min=4, max=30),
    retry=retry_if_exception_type((websocket.WebSocketException, ConnectionError, TimeoutError))
)
def inject_workflows_batch_with_retry(ws_url, workflows_batch, batch_number=1):
    """Enhanced workflow injection using queue system"""
    if not workflows_batch:
        print("⚠️ No workflows to inject")
        return False, 0
    
    # Add workflows to queue
    workflow_queue.add_workflows_batch(workflows_batch)
    
    total_injected = 0
    connection_failures = 0
    max_connection_failures = 3
    
    while True:
        # Check queue stats
        stats = workflow_queue.get_queue_stats()
        if stats['pending'] == 0 and stats['retry_pending'] == 0:
            break
        
        # Get next workflow
        workflow_item = workflow_queue.get_next_workflow(timeout=5)
        if not workflow_item:
            break
        
        workflow_data = workflow_item['workflow_data']
        workflow_name = workflow_data['automaWf']['name']
        
        try:
            # Check connection health before each workflow
            if connection_failures >= max_connection_failures:
                print(f"❌ Too many connection failures, attempting to find new context...")
                new_ws_url = find_automa_context() or create_automa_page()
                if new_ws_url:
                    ws_url = new_ws_url
                    connection_failures = 0
                else:
                    workflow_queue.mark_failure(workflow_item, "Could not establish connection")
                    continue
            
            print(f"📤 Processing workflow: {workflow_name} (Attempt {workflow_item['retry_count'] + 1})")
            
            # Inject single workflow with enhanced error handling
            success = inject_single_workflow_safe(ws_url, workflow_data)
            
            if success:
                total_injected += 1
                workflow_queue.mark_success(workflow_item)
                connection_failures = 0  # Reset on success
                print(f"✅ Successfully injected: {workflow_name}")
            else:
                connection_failures += 1
                workflow_queue.mark_failure(workflow_item, "Injection failed")
        
        except Exception as e:
            connection_failures += 1
            error_msg = str(e)
            print(f"❌ Error processing {workflow_name}: {error_msg}")
            workflow_queue.mark_failure(workflow_item, error_msg)
    
    # Final statistics
    final_stats = workflow_queue.get_queue_stats()
    print(f"\n📊 QUEUE PROCESSING SUMMARY:")
    print(f"   - Successfully processed: {final_stats['processed_success']}")
    print(f"   - Failed permanently: {final_stats['failed']}")
    print(f"   - Total injected: {total_injected}")
    
    return total_injected > 0, total_injected

def inject_single_workflow_safe(ws_url, workflow_data):
    """Inject a single workflow with enhanced safety and error handling"""
    try:
        ws = ws_manager.get_connection(ws_url)
        workflow = workflow_data['automaWf']
        
        # Check existing workflows first
        get_existing_script = """
        new Promise((resolve) => {
            const timeout = setTimeout(() => resolve('[]'), 3000);
            
            if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {
                chrome.storage.local.get('workflows', (result) => {
                    clearTimeout(timeout);
                    resolve(JSON.stringify(result.workflows || []));
                });
            } else {
                clearTimeout(timeout);
                resolve('[]');
            }
        });
        """
        
        get_message = {
            "id": int(time.time() * 1000) % 10000,
            "method": "Runtime.evaluate",
            "params": {"expression": get_existing_script, "awaitPromise": True}
        }
        
        ws.settimeout(10)
        ws.send(json.dumps(get_message))
        ws_manager.record_message(ws_url)
        
        get_result = json.loads(ws.recv())
        
        existing_workflows = []
        if "result" in get_result and "result" in get_result["result"]:
            try:
                existing_workflows = json.loads(get_result["result"]["result"]["value"])
                if not isinstance(existing_workflows, list):
                    existing_workflows = []
            except (json.JSONDecodeError, KeyError):
                existing_workflows = []
        
        # Check if workflow already exists
        existing_ids = {wf.get('id') for wf in existing_workflows if isinstance(wf, dict) and wf.get('id')}
        if workflow.get('id') in existing_ids:
            print(f"ℹ️ Workflow {workflow['name']} already exists, skipping")
            return True
        
        # Add new workflow
        all_workflows = existing_workflows + [workflow]
        
        # Inject with smaller timeout
        storage_script = f"""
        new Promise((resolve, reject) => {{
            const timeout = setTimeout(() => reject('timeout'), 5000);
            
            if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {{
                chrome.storage.local.set({{workflows: {json.dumps(all_workflows)}}}, () => {{
                    clearTimeout(timeout);
                    if (chrome.runtime.lastError) {{
                        reject(chrome.runtime.lastError.message);
                    }} else {{
                        resolve('success');
                    }}
                }});
            }} else {{
                clearTimeout(timeout);
                reject('chrome_storage_unavailable');
            }}
        }});
        """
        
        inject_message = {
            "id": int(time.time() * 1000) % 10000 + 1,
            "method": "Runtime.evaluate",
            "params": {"expression": storage_script, "awaitPromise": True}
        }
        
        ws.send(json.dumps(inject_message))
        ws_manager.record_message(ws_url)
        
        inject_result = json.loads(ws.recv())
        
        if "result" in inject_result and "result" in inject_result["result"]:
            result_value = inject_result["result"]["result"].get("value", "")
            return "success" in result_value
        
        return False
        
    except Exception as e:
        print(f"❌ Single workflow injection failed: {e}")
        # Force connection refresh on certain errors
        if "403" in str(e) or "WebSocket" in str(e) or "timeout" in str(e).lower():
            ws_manager._close_connection(ws_url)
        return False

def inject_workflows_batch(ws_url, workflows_batch, batch_number=1):
    """Legacy batch injection - maintained for compatibility"""
    if not workflows_batch:
        print("⚠️ No workflows to inject")
        return False, 0
    
    try:
        # Use the WebSocket manager for better connection handling
        ws = ws_manager.get_connection(ws_url)
        
        workflows_data = []
        for workflow in workflows_batch:
            workflows_data.append(workflow['automaWf'])
        
        print(f"📥 Injecting batch {batch_number} - {len(workflows_data)} workflows into chrome.storage.local")
        
        # Get existing workflows with timeout and error handling
        get_existing_script = """
        new Promise((resolve) => {
            const timeout = setTimeout(() => resolve('[]'), 5000);
            
            if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {
                chrome.storage.local.get('workflows', (result) => {
                    clearTimeout(timeout);
                    resolve(JSON.stringify(result.workflows || []));
                });
            } else {
                clearTimeout(timeout);
                resolve('[]');
            }
        });
        """
        
        get_message = {
            "id": batch_number * 100 + 1,
            "method": "Runtime.evaluate",
            "params": {"expression": get_existing_script, "awaitPromise": True}
        }
        
        ws.settimeout(15)
        ws.send(json.dumps(get_message))
        get_result = json.loads(ws.recv())
        
        existing_workflows = []
        if "result" in get_result and "result" in get_result["result"]:
            try:
                existing_workflows = json.loads(get_result["result"]["result"]["value"])
                if not isinstance(existing_workflows, list):
                    existing_workflows = []
            except (json.JSONDecodeError, KeyError):
                print("⚠️ Could not parse existing workflows, starting fresh")
                existing_workflows = []
        
        # Filter out workflows that already exist
        existing_ids = {wf.get('id') for wf in existing_workflows if isinstance(wf, dict) and wf.get('id')}
        new_workflows = [wf for wf in workflows_data if wf.get('id') not in existing_ids]
        all_workflows = existing_workflows + new_workflows
        
        print(f"📊 Existing: {len(existing_workflows)}, New: {len(new_workflows)}, Total: {len(all_workflows)}")
        
        if not new_workflows:
            print(f"ℹ️ All workflows in batch {batch_number} already exist, skipping injection")
            return True, 0
        
        # Inject workflows with enhanced error handling
        storage_script = f"""
        new Promise((resolve, reject) => {{
            const timeout = setTimeout(() => reject('Storage operation timed out'), 10000);
            
            if (typeof chrome !== 'undefined' && chrome.storage && chrome.storage.local) {{
                try {{
                    chrome.storage.local.set({{workflows: {json.dumps(all_workflows)}}}, () => {{
                        clearTimeout(timeout);
                        if (chrome.runtime.lastError) {{
                            reject('Chrome storage error: ' + chrome.runtime.lastError.message);
                        }} else {{
                            console.log('Workflows batch {batch_number} saved: {len(new_workflows)} new workflows');
                            resolve('workflows_injected_success');
                        }}
                    }});
                }} catch (error) {{
                    clearTimeout(timeout);
                    reject('Storage operation failed: ' + error.message);
                }}
            }} else {{
                clearTimeout(timeout);
                reject('chrome_storage_unavailable');
            }}
        }});
        """
        
        inject_message = {
            "id": batch_number * 100 + 2,
            "method": "Runtime.evaluate",
            "params": {"expression": storage_script, "awaitPromise": True}
        }
        
        ws.send(json.dumps(inject_message))
        inject_result = json.loads(ws.recv())
        
        success = False
        if "result" in inject_result and "result" in inject_result["result"]:
            result_value = inject_result["result"]["result"].get("value", "")
            if "workflows_injected_success" in result_value:
                print(f"✅ Successfully injected batch {batch_number}: {len(new_workflows)} workflows")
                success = True
            else:
                print(f"⚠️ Injection result for batch {batch_number}: {result_value}")
        elif "exceptionDetails" in inject_result.get("result", {}):
            error_details = inject_result["result"]["exceptionDetails"]
            print(f"❌ Chrome runtime error for batch {batch_number}: {error_details}")
        
        return success, len(new_workflows)
        
    except websocket.WebSocketTimeoutException:
        print(f"❌ WebSocket timeout for batch {batch_number}")
        return False, 0
    except websocket.WebSocketConnectionClosedException:
        print(f"❌ WebSocket connection closed for batch {batch_number}")
        raise websocket.WebSocketException("Connection closed unexpectedly")
    except json.JSONDecodeError as e:
        print(f"❌ JSON decode error for batch {batch_number}: {e}")
        return False, 0
    except Exception as e:
        print(f"❌ Failed to inject batch {batch_number}: {e}")
        if "403" in str(e) or "Forbidden" in str(e):
            raise websocket.WebSocketException(f"WebSocket connection forbidden: {e}")
        return False, 0

@retry(
    stop=stop_after_attempt(2),
    wait=wait_exponential(multiplier=1, min=2, max=10)
)
def trigger_automa_workflows(ws_url, workflows_batch, variables_map=None):
    """
    Legacy trigger function - maintained for compatibility.
    variables_map: Optional dict mapping workflow_id to variables dict.
    """
    if not workflows_batch:
        print("⚠️ No workflows to trigger")
        return False

    print(f"🚀 Triggering execution of {len(workflows_batch)} workflows...")
    
    try:
        ws = ws_manager.get_connection(ws_url)
        ws.settimeout(10)
    except Exception as e:
        print(f"❌ Connection failed for workflow triggering: {e}")
        return False

    triggered_count = 0
    for workflow_data in workflows_batch:
        wf = workflow_data['automaWf']
        wf_id = wf.get('id')
        wf_name = wf.get('name', 'Unknown')
        vars_for_wf = (variables_map or {}).get(wf_id, {})

        # Build JS to dispatch event with better error handling
        detail = {'id': wf_id}
        if vars_for_wf:
            detail['data'] = {'variables': vars_for_wf}

        js = f"""
        new Promise((resolve, reject) => {{
            try {{
                const timeout = setTimeout(() => reject('Event dispatch timed out'), 5000);
                
                window.dispatchEvent(new CustomEvent('automa:execute-workflow', {{
                    detail: {json.dumps(detail)}
                }}));
                
                setTimeout(() => {{
                    clearTimeout(timeout);
                    resolve('dispatched_successfully');
                }}, 200);
            }} catch (error) {{
                reject('Event dispatch failed: ' + error.message);
            }}
        }});
        """

        payload = {
            "id": triggered_count + 200,
            "method": "Runtime.evaluate",
            "params": {"expression": js, "awaitPromise": True}
        }

        try:
            ws.send(json.dumps(payload))
            resp = json.loads(ws.recv())
            
            if "result" in resp and "result" in resp["result"]:
                val = resp["result"]["result"].get("value", "")
                if "dispatched_successfully" in val:
                    print(f"✅ Dispatched workflow event: {wf_name}")
                    triggered_count += 1
                else:
                    print(f"⚠️ Unexpected result for {wf_name}: {val}")
            elif "exceptionDetails" in resp.get("result", {}):
                error_details = resp["result"]["exceptionDetails"]
                print(f"❌ Chrome runtime error triggering {wf_name}: {error_details}")
            else:
                print(f"⚠️ Unexpected response format for {wf_name}")
                
        except Exception as err:
            print(f"❌ Error triggering {wf_name}: {err}")

        time.sleep(0.5)  # Small delay between triggers

    print(f"🎯 Successfully dispatched {triggered_count}/{len(workflows_batch)} workflows")
    return triggered_count > 0

def trigger_automa_workflows_batch(ws_url, workflows_batch, variables_map=None):
    """Enhanced batch workflow triggering with better error handling"""
    if not workflows_batch:
        return False
    
    print(f"🚀 Batch triggering {len(workflows_batch)} workflows...")
    successful_triggers = 0
    
    for workflow_data in workflows_batch:
        try:
            success = trigger_single_workflow_safe(ws_url, workflow_data, variables_map)
            if success:
                successful_triggers += 1
            time.sleep(0.5)  # Small delay between triggers
        except Exception as e:
            print(f"❌ Failed to trigger workflow {workflow_data['automaWf']['name']}: {e}")
    
    print(f"🎯 Successfully triggered {successful_triggers}/{len(workflows_batch)} workflows")
    return successful_triggers > 0


def trigger_single_workflow_safe(ws_url, workflow_data, variables_map=None):
    """Trigger single workflow with enhanced safety"""
    try:
        ws = ws_manager.get_connection(ws_url)
        wf = workflow_data['automaWf']
        wf_id = wf.get('id')
        wf_name = wf.get('name', 'Unknown')
        vars_for_wf = (variables_map or {}).get(wf_id, {})

        detail = {'id': wf_id}
        if vars_for_wf:
            detail['data'] = {'variables': vars_for_wf}

        js = f"""
        new Promise((resolve, reject) => {{
            try {{
                const timeout = setTimeout(() => reject('timeout'), 3000);
                
                window.dispatchEvent(new CustomEvent('automa:execute-workflow', {{
                    detail: {json.dumps(detail)}
                }}));
                
                setTimeout(() => {{
                    clearTimeout(timeout);
                    resolve('dispatched');
                }}, 100);
            }} catch (error) {{
                reject(error.message);
            }}
        }});
        """

        payload = {
            "id": int(time.time() * 1000) % 10000 + 500,
            "method": "Runtime.evaluate",
            "params": {"expression": js, "awaitPromise": True}
        }

        ws.settimeout(5)
        ws.send(json.dumps(payload))
        ws_manager.record_message(ws_url)
        
        resp = json.loads(ws.recv())
        
        if "result" in resp and "result" in resp["result"]:
            val = resp["result"]["result"].get("value", "")
            success = "dispatched" in val
            if success:
                print(f"✅ Triggered: {wf_name}")
            return success
        
        return False
        
    except Exception as e:
        print(f"❌ Failed to trigger {workflow_data['automaWf']['name']}: {e}")
        return False