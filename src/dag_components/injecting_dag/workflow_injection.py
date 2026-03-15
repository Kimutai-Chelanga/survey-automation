import json
import time
import logging
from .websocket_handler import verify_websocket_connection
import websocket
from datetime import datetime, timedelta
import traceback

from .config import get_workflow_strategy, get_execution_config
from .chrome_automa import find_automa_context, create_automa_page, trigger_automa_workflows_batch, monitor_chrome_health, ws_manager
from .workflow_fetch import fetch_workflows_by_strategy
from .tracking import update_execution_tracking
from .db_utils import get_system_setting, get_mongo_db
from .workflow_logging import EnhancedWorkflowLogger, WorkflowStatus
from .websocket_handler import WebSocketResponseHandler, verify_websocket_connection

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def inject_single_workflow_enhanced(ws_url: str, workflow_data: dict, log_entry_id: str) -> bool:
    """Fixed version with proper variable initialization"""
    workflow_id = None  # Initialize early to prevent reference errors
    
    try:
        response_handler = WebSocketResponseHandler()
        
        # Extract workflow info safely
        workflow = workflow_data.get('automaWf', {})
        workflow_name = workflow.get('name', 'Unknown')
        workflow_id = workflow.get('id', '')  # Proper assignment here
        
        if not workflow_id:
            print(f"❌ No workflow ID found for {workflow_name}")
            return False
        
        ws = ws_manager.get_connection(ws_url)
        
        print(f"🔧 Injecting workflow: {workflow_name} (ID: {workflow_id})")
        
        enhanced_logger = EnhancedWorkflowLogger()
        enhanced_logger.update_workflow_status(
            workflow_id,  # Now safely defined
            WorkflowStatus.INJECTION_STARTED,
            "Starting enhanced injection process"
        )
        print("📡 Clearing WebSocket event queue...")
        ws.settimeout(1)
        cleared_events = 0
        try:
            while True:
                event = ws.recv()
                cleared_events += 1
                if cleared_events > 20:
                    break
        except websocket.WebSocketTimeoutException:
            pass
        
        print(f"📡 Cleared {cleared_events} pending events")
        
        get_existing_script = """
        (() => {
            console.log('[WORKFLOW-INJECT] Starting workflow retrieval');
            
            if (typeof chrome === 'undefined' || !chrome.storage || !chrome.storage.local) {
                console.log('[WORKFLOW-INJECT] Chrome storage not available');
                return JSON.stringify([]);
            }
            
            let workflows = null;
            let completed = false;
            
            chrome.storage.local.get('workflows', (result) => {
                console.log('[WORKFLOW-INJECT] Storage result type:', typeof result.workflows);
                
                if (chrome.runtime.lastError) {
                    console.log('[WORKFLOW-INJECT] Storage error:', chrome.runtime.lastError.message);
                    workflows = [];
                } else {
                    const stored = result.workflows;
                    if (Array.isArray(stored)) {
                        workflows = stored;
                        console.log('[WORKFLOW-INJECT] Found', stored.length, 'workflows in array format');
                    } else if (stored && typeof stored === 'object') {
                        workflows = Object.values(stored);
                        console.log('[WORKFLOW-INJECT] Converted object to array:', workflows.length, 'workflows');
                    } else {
                        workflows = [];
                        console.log('[WORKFLOW-INJECT] No workflows or invalid format');
                    }
                }
                completed = true;
            });
            
            const startTime = Date.now();
            while (!completed && (Date.now() - startTime) < 6000) {
            }
            
            if (!completed) {
                console.log('[WORKFLOW-INJECT] Timeout getting workflows');
                workflows = [];
            }
            
            console.log('[WORKFLOW-INJECT] Returning', workflows ? workflows.length : 0, 'workflows');
            return JSON.stringify(workflows || []);
        })();
        """
        
        message_id = int(time.time() * 1000) % 100000
        get_message = {
            "id": message_id,
            "method": "Runtime.evaluate",
            "params": {
                "expression": get_existing_script,
                "returnByValue": True
            }
        }
        
        ws.settimeout(15)
        ws.send(json.dumps(get_message))
        ws_manager.record_message(ws_url)
        
        existing_workflows = []
        responses_collected = []
        max_response_attempts = 15
        
        print("📥 Collecting responses for workflow retrieval...")
        
        for attempt in range(max_response_attempts):
            try:
                response = ws.recv()
                response_data = json.loads(response)
                responses_collected.append(response_data)
                
                print(f"📥 Response {attempt + 1}: {response_data.get('method', response_data.get('id', 'unknown'))}")
                
                if response_data.get('id') == message_id:
                    print(f"✅ Found target response for workflow retrieval")
                    workflows = response_handler.extract_workflows_from_response(response_data)
                    if workflows and not all('console_reported_count' in wf for wf in workflows):
                        existing_workflows = workflows
                        break
                
                workflows = response_handler.extract_workflows_from_response(response_data)
                if workflows and 'console_reported_count' in workflows[0]:
                    print(f"📊 Console reported workflow count: {workflows[0]['console_reported_count']}")
                
            except websocket.WebSocketTimeoutException:
                print(f"⏰ Response timeout on attempt {attempt + 1}")
                break
            except json.JSONDecodeError as e:
                print(f"❌ JSON decode error: {e}")
                continue
            except Exception as e:
                print(f"❌ Error collecting response: {e}")
                continue
        
        if not existing_workflows:
            print("🔍 Attempting to extract workflows from all collected responses...")
            for response_data in responses_collected:
                workflows = response_handler.extract_workflows_from_response(response_data)
                if workflows and not all('console_reported_count' in wf for wf in workflows):
                    existing_workflows = workflows
                    print(f"✅ Extracted {len(existing_workflows)} workflows from response")
                    break
        
        if not isinstance(existing_workflows, list):
            existing_workflows = []
        
        print(f"📊 Found {len(existing_workflows)} existing workflows")
        
        existing_ids = {wf.get('id') for wf in existing_workflows if isinstance(wf, dict) and wf.get('id')}
        
        if workflow.get('id') in existing_ids:
            print(f"ℹ️ Workflow {workflow_name} already exists, marking as successful")
            enhanced_logger.update_workflow_status(
                workflow_id,
                WorkflowStatus.INJECTION_COMPLETED,
                "Workflow already exists - skipped"
            )
            return True
        
        all_workflows = existing_workflows + [workflow]
        print(f"💉 Preparing to inject: {len(existing_workflows)} existing + 1 new = {len(all_workflows)} total")
        
        injection_script = f"""
        (() => {{
            console.log('[WORKFLOW-INJECT] Starting injection of {len(all_workflows)} workflows');
            
            if (typeof chrome === 'undefined' || !chrome.storage || !chrome.storage.local) {{
                console.log('[WORKFLOW-INJECT] Chrome storage not available for injection');
                return 'chrome_storage_unavailable';
            }}
            
            const workflows = {json.dumps(all_workflows)};
            let result = null;
            let completed = false;
            
            try {{
                chrome.storage.local.set({{'workflows': workflows}}, () => {{
                    if (chrome.runtime.lastError) {{
                        const error = chrome.runtime.lastError.message;
                        console.log('[WORKFLOW-INJECT] Storage error:', error);
                        result = 'storage_error: ' + error;
                    }} else {{
                        console.log('[WORKFLOW-INJECT] Storage successful - injected', workflows.length, 'workflows');
                        result = 'success';
                    }}
                    completed = true;
                }});
                
                const startTime = Date.now();
                while (!completed && (Date.now() - startTime) < 10000) {{
                }}
                
                if (!completed) {{
                    console.log('[WORKFLOW-INJECT] Storage operation timeout');
                    return 'storage_timeout';
                }}
                
                console.log('[WORKFLOW-INJECT] Final result:', result);
                return result;
                
            }} catch (error) {{
                console.log('[WORKFLOW-INJECT] Exception during injection:', error.message);
                return 'injection_exception: ' + error.message;
            }}
        }})();
        """
        
        ws.settimeout(1)
        try:
            while True:
                ws.recv()
        except websocket.WebSocketTimeoutException:
            pass
        
        inject_message = {
            "id": message_id + 1,
            "method": "Runtime.evaluate",
            "params": {
                "expression": injection_script,
                "returnByValue": True
            }
        }
        
        ws.settimeout(20)
        ws.send(json.dumps(inject_message))
        ws_manager.record_message(ws_url)
        
        injection_responses = []
        injection_success = False
        injection_result = "no_response"
        
        print("📥 Collecting injection responses...")
        
        for attempt in range(max_response_attempts):
            try:
                response = ws.recv()
                response_data = json.loads(response)
                injection_responses.append(response_data)
                
                print(f"📥 Injection response {attempt + 1}: {response_data.get('method', response_data.get('id', 'unknown'))}")
                
                success, result_msg = response_handler.is_injection_success(response_data)
                if success:
                    injection_success = True
                    injection_result = result_msg
                    print(f"✅ Injection success detected: {result_msg}")
                    break
                elif result_msg.startswith(('console_error', 'runtime_console_error')):
                    injection_success = False
                    injection_result = result_msg
                    print(f"❌ Injection error detected: {result_msg}")
                    break
                
                if response_data.get('id') == message_id + 1:
                    success, result_msg = response_handler.is_injection_success(response_data)
                    injection_success = success
                    injection_result = result_msg
                    print(f"🎯 Target injection response: success={success}, result={result_msg}")
                    break
                    
            except websocket.WebSocketTimeoutException:
                print(f"⏰ Injection response timeout on attempt {attempt + 1}")
                break
            except Exception as e:
                print(f"❌ Error collecting injection response: {e}")
                continue
        
        if not injection_success and injection_result == "no_response":
            print("🔍 Analyzing all injection responses for success indicators...")
            for response_data in injection_responses:
                success, result_msg = response_handler.is_injection_success(response_data)
                if success:
                    injection_success = True
                    injection_result = result_msg
                    print(f"✅ Found success in collected responses: {result_msg}")
                    break
        
        enhanced_logger.update_workflow_status(
            workflow_id,
            WorkflowStatus.INJECTION_COMPLETED if injection_success else WorkflowStatus.INJECTION_FAILED,
            f"Enhanced injection completed: {injection_result}",
            additional_data={
                'injection_success': injection_success,
                'injection_result': injection_result,
                'existing_workflows_count': len(existing_workflows),
                'total_workflows_after': len(all_workflows) if injection_success else len(existing_workflows),
                'responses_collected': len(responses_collected),
                'injection_responses_collected': len(injection_responses)
            }
        )
        
        if injection_success:
            print(f"✅ Successfully injected workflow: {workflow_name}")
        else:
            print(f"❌ Failed to inject workflow: {workflow_name} - {injection_result}")
        
        return injection_success
        
    except Exception as e:
        print(f"❌ Enhanced workflow injection failed: {e}")
        print(f"📋 Full traceback: {traceback.format_exc()}")
        
        # Safe error handling even if workflow_id is None
        if workflow_id:
            enhanced_logger = EnhancedWorkflowLogger()
            enhanced_logger.update_workflow_status(
                workflow_id,
                WorkflowStatus.ERROR,
                f"Enhanced injection error: {str(e)}",
                additional_data={
                    'exception': str(e),
                    'exception_type': type(e).__name__
                }
            )
        
        return False

def process_workflows_group_with_logging(debugger_url, group_workflows, batch_number, dag_run_id, task_id, enhanced_logger):
    """Process workflows with comprehensive logging including actual Automa execution logs"""
    group_logs = {}
    
    for workflow_data in group_workflows:
        workflow_name = workflow_data['automaWf']['name']
        workflow_id = workflow_data['automaWf']['id']
        
        log_entry_id = enhanced_logger.start_workflow_logging(
            workflow_data, 
            dag_context={'dag_run_id': dag_run_id, 'task_id': task_id, 'batch_number': batch_number}
        )
        
        group_logs[workflow_name] = {
            'log_entry_id': log_entry_id,
            'started_at': datetime.now(),
            'injection_success': False,
            'comprehensive_logs_captured': False
        }
        
        try:
            enhanced_logger.update_workflow_status(
                workflow_id,
                WorkflowStatus.INJECTION_STARTED,
                "Starting workflow injection with comprehensive logging",
                additional_data={'start_time': datetime.now().isoformat()}
            )
            
            print(f"📤 Injecting workflow: {workflow_name}")
            
            injection_success = inject_single_workflow_enhanced(debugger_url, workflow_data, log_entry_id)
            
            group_logs[workflow_name]['injection_success'] = injection_success
            group_logs[workflow_name]['injected_at'] = datetime.now()
            
            if injection_success:
                enhanced_logger.update_workflow_status(
                    workflow_id,
                    WorkflowStatus.INJECTION_COMPLETED,
                    "Workflow injection completed successfully",
                    additional_data={'injection_completed_time': datetime.now().isoformat()}
                )
                
                post_injection_logs = enhanced_logger.get_automa_logs_enhanced(debugger_url, workflow_id)
                
                automa_log_id = enhanced_logger.store_automa_logs(workflow_data, post_injection_logs, log_entry_id)
                group_logs[workflow_name]['comprehensive_logs_captured'] = bool(automa_log_id)
                group_logs[workflow_name]['automa_log_id'] = automa_log_id
                
            else:
                enhanced_logger.update_workflow_status(
                    workflow_id,
                    WorkflowStatus.INJECTION_FAILED,
                    "Workflow injection failed",
                    additional_data={'injection_failure': True, 'failed_at': datetime.now().isoformat()}
                )
        
        except Exception as e:
            print(f"❌ Error processing workflow {workflow_name}: {e}")
            group_logs[workflow_name]['error'] = str(e)
            enhanced_logger.update_workflow_status(
                workflow_id,
                WorkflowStatus.ERROR,
                f"Error processing workflow: {str(e)}",
                additional_data={'exception': str(e), 'type': type(e).__name__, 'failed_at': datetime.now().isoformat()}
            )
    
    return group_logs

def enhanced_workflow_injection_with_logging(**kwargs):
    """Enhanced workflow injection with comprehensive Automa log capture"""
    print('🚀 Starting Enhanced MongoDB Workflow Injection Process...')
    
    dag_run_id = kwargs.get('dag_run').dag_id if kwargs.get('dag_run') else None
    task_id = kwargs.get('task_instance').task_id if kwargs.get('task_instance') else None
    
    enhanced_logger = EnhancedWorkflowLogger()
    enhanced_logger.initialize_collections()
    
    if not verify_websocket_connection():
        raise Exception("❌ WebSocket connection verification failed")
    
    strategy, selected_types, type_order = get_workflow_strategy()
    config = get_execution_config()
    
    extraction_settings = get_system_setting('extraction_processing_settings', {
        'content_to_filter': 5,
        'gap_between_workflows': 15
    })
    gap_between_workflows = config.get('workflow_gap_seconds', extraction_settings.get('gap_between_workflows', 15) * 60)
    
    print(f"📋 Configuration: Strategy: {strategy}, Types: {selected_types}, Gap: {gap_between_workflows}s")
    
    all_workflows, execution_stats = fetch_workflows_by_strategy(strategy, selected_types, type_order)
    
    if not all_workflows:
        print('⚠️ No workflows found')
        return {'totalInjected': 0, 'executionStats': execution_stats}
    
    print(f"📊 Found {len(all_workflows)} workflows to process")
    
    debugger_url = find_automa_context() or create_automa_page()
    if not debugger_url or not monitor_chrome_health():
        raise Exception("❌ Could not establish Chrome connection")
    
    link_groups = {}
    for workflow_data in all_workflows:
        link_group = workflow_data['automaWf'].get('_linkGroup', 'no_link')
        if link_group not in link_groups:
            link_groups[link_group] = []
        link_groups[link_group].append(workflow_data)
    
    total_injected = 0
    total_triggered = 0
    failed_workflows = []
    comprehensive_workflow_logs = {}
    
    print(f"📦 Processing {len(link_groups)} link groups...")
    
    for group_index, (link_group, group_workflows) in enumerate(link_groups.items()):
        print(f"\n🔗 Processing link group {group_index + 1}/{len(link_groups)}: {link_group}")
        
        group_logs = process_workflows_group_with_logging(
            debugger_url, group_workflows, group_index + 1, dag_run_id, task_id, enhanced_logger
        )
        
        comprehensive_workflow_logs[link_group] = group_logs
        
        successful_workflows = [wf for wf in group_workflows if group_logs.get(wf['automaWf']['name'], {}).get('injection_success', False)]
        
        if successful_workflows:
            total_injected += len(successful_workflows)
            
            trigger_success = trigger_automa_workflows_batch(debugger_url, successful_workflows)
            if trigger_success:
                total_triggered += len(successful_workflows)
                
                time.sleep(10)
                
                print("📊 Capturing comprehensive post-execution logs...")
                for workflow_data in successful_workflows:
                    workflow_name = workflow_data['automaWf']['name']
                    workflow_id = workflow_data['automaWf']['id']
                    log_entry_id = group_logs.get(workflow_name, {}).get('log_entry_id')
                    
                    if log_entry_id:
                        comprehensive_logs = enhanced_logger.get_automa_logs_enhanced(debugger_url, workflow_id)
                        
                        automa_log_id = enhanced_logger.store_automa_logs(workflow_data, comprehensive_logs, log_entry_id)
                        
                        enhanced_logger.update_workflow_status(
                            workflow_id,
                            WorkflowStatus.COMPLETED,
                            "Comprehensive workflow execution completed with logs captured",
                            additional_data={
                                'completion_time': datetime.now().isoformat(),
                                'automa_log_id': automa_log_id,
                                'comprehensive_logs_captured': True
                            }
                        )
            else:
                print(f"⚠️ Failed to trigger workflows in group: {link_group}")
            
            for workflow_data in successful_workflows:
                update_execution_tracking(workflow_data, True)
        else:
            print(f"❌ Failed to inject workflows in group: {link_group}")
            for workflow_data in group_workflows:
                failed_workflows.append(workflow_data['automaWf']['name'])
                update_execution_tracking(workflow_data, False)
        
        if group_index < len(link_groups) - 1:
            print(f"⏱️ Waiting {gap_between_workflows} seconds...")
            time.sleep(gap_between_workflows)
    
    ws_manager.close_all()
    
    store_comprehensive_summary(all_workflows, comprehensive_workflow_logs, total_injected, total_triggered, dag_run_id, task_id, enhanced_logger)
    
    print(f"\n📊 FINAL INJECTION SUMMARY:")
    print(f"   - Total workflows injected: {total_injected}/{len(all_workflows)}")
    print(f"   - Success rate: {(total_injected/len(all_workflows)*100):.1f}%")
    if failed_workflows:
        print(f"   - Failed workflows: {len(failed_workflows)}")
    
    return {
        'totalInjected': total_injected,
        'totalTriggered': total_triggered,
        'failedWorkflows': failed_workflows,
        'successRate': round(total_injected/len(all_workflows)*100, 1) if all_workflows else 0,
        'executionStats': execution_stats,
        'comprehensiveWorkflowLogs': comprehensive_workflow_logs
    }

def store_comprehensive_summary(all_workflows, comprehensive_logs, total_injected, total_triggered, dag_run_id, task_id, enhanced_logger):
    """Store comprehensive execution summary with detailed logging information"""
    try:
        db, client = get_mongo_db()
        summary_collection = db["dag_execution_summaries_comprehensive"]
        
        summary = {
            'dag_run_id': dag_run_id,
            'task_id': task_id,
            'execution_timestamp': datetime.now(),
            'total_workflows_processed': len(all_workflows),
            'total_workflows_injected': total_injected,
            'total_workflows_triggered': total_triggered,
            'success_rate': round((total_injected / len(all_workflows)) * 100, 2) if all_workflows else 0,
            'comprehensive_logging_enabled': True,
            'workflow_details': {},
            'execution_statistics': {
                'workflows_with_logs': 0,
                'workflows_with_errors': 0,
                'workflows_with_timeouts': 0,
                'total_log_entries': 0
            }
        }
        
        for link_group, group_logs in comprehensive_logs.items():
            for workflow_name, logs in group_logs.items():
                summary['workflow_details'][workflow_name] = {
                    'injection_success': logs.get('injection_success', False),
                    'comprehensive_logs_captured': logs.get('comprehensive_logs_captured', False),
                    'started_at': logs.get('started_at').isoformat() if logs.get('started_at') else None,
                    'log_entry_id': logs.get('log_entry_id'),
                    'automa_log_id': logs.get('automa_log_id'),
                    'link_group': link_group
                }
                
                if logs.get('comprehensive_logs_captured'):
                    summary['execution_statistics']['workflows_with_logs'] += 1
                if logs.get('error'):
                    summary['execution_statistics']['workflows_with_errors'] += 1
                
                summary['execution_statistics']['total_log_entries'] += 1
        
        performance_metrics = enhanced_logger.calculate_performance_metrics(dag_run_id=dag_run_id)
        summary['performance_metrics'] = performance_metrics
        
        summary_collection.insert_one(summary)
        client.close()
        
        logger.info(f"✅ Stored comprehensive execution summary for DAG run: {dag_run_id}")
        
    except Exception as e:
        logger.error(f"❌ Failed to store comprehensive execution summary: {e}")
        if 'client' in locals():
            client.close()