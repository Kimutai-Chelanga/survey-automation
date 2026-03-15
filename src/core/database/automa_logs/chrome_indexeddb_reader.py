import os
import json
import struct
from pathlib import Path
from datetime import datetime
import logging

class ChromeIndexedDBReader:
    def __init__(self, chrome_profile_path="/app/chrome_persistent_profile"):
        self.chrome_profile_path = Path(chrome_profile_path)
        self.indexeddb_path = self.chrome_profile_path / "Default" / "IndexedDB"
        self.logger = logging.getLogger(__name__)
    
    def find_automa_extension_db(self):
        """Find Automa extension's IndexedDB directory"""
        if not self.indexeddb_path.exists():
            return None
        
        # Look for chrome-extension directories
        for item in self.indexeddb_path.iterdir():
            if item.is_dir() and "chrome-extension_" in item.name:
                # This could be Automa's extension
                self.logger.info(f"Found extension directory: {item.name}")
                return item
        
        return None
    
    def read_leveldb_file(self, file_path):
        """Read data from LevelDB files used by Chrome's IndexedDB"""
        try:
            with open(file_path, 'rb') as f:
                content = f.read()
            
            # Try to extract JSON-like data
            workflow_data = []
            
            # Convert to string, ignoring binary data
            try:
                text_content = content.decode('utf-8', errors='ignore')
                
                # Look for JSON objects that might be workflows
                import re
                
                # Find JSON objects
                json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                potential_json = re.findall(json_pattern, text_content)
                
                for json_str in potential_json:
                    try:
                        data = json.loads(json_str)
                        if self.looks_like_automa_workflow(data):
                            workflow_data.append(data)
                    except json.JSONDecodeError:
                        continue
                
                # Also look for workflow indicators in the raw text
                if any(keyword in text_content.lower() for keyword in ['workflow', 'automa', 'drawflow']):
                    # Try to extract more complex nested JSON
                    lines = text_content.split('\n')
                    for line in lines:
                        if '{' in line and any(keyword in line.lower() for keyword in ['workflow', 'automa']):
                            # Try to extract JSON from this line
                            start_idx = line.find('{')
                            if start_idx != -1:
                                json_part = line[start_idx:]
                                # Find the end of JSON
                                brace_count = 0
                                end_idx = 0
                                for i, char in enumerate(json_part):
                                    if char == '{':
                                        brace_count += 1
                                    elif char == '}':
                                        brace_count -= 1
                                        if brace_count == 0:
                                            end_idx = i + 1
                                            break
                                
                                if end_idx > 0:
                                    try:
                                        potential_workflow = json.loads(json_part[:end_idx])
                                        if self.looks_like_automa_workflow(potential_workflow):
                                            workflow_data.append(potential_workflow)
                                    except:
                                        continue
                
            except UnicodeDecodeError:
                # File is purely binary, try different approach
                pass
            
            return workflow_data
            
        except Exception as e:
            self.logger.error(f"Error reading {file_path}: {e}")
            return []
    
    def looks_like_automa_workflow(self, data):
        """Check if data looks like an Automa workflow"""
        if not isinstance(data, dict):
            return False
        
        # Check for Automa-specific indicators
        automa_indicators = [
            'name', 'workflowId', 'id', 'drawflow', 'blocks', 'version',
            'description', 'createdAt', 'updatedAt', 'icon', 'category'
        ]
        
        execution_indicators = [
            'status', 'endedAt', 'startedAt', 'logs', 'execId', 'isDisabled'
        ]
        
        # Count matches
        matches = sum(1 for key in automa_indicators + execution_indicators if key in data)
        
        # Special checks for Automa structure
        if 'drawflow' in data and isinstance(data['drawflow'], dict):
            if 'Home' in data['drawflow']:
                return True
        
        if 'blocks' in data and isinstance(data['blocks'], list):
            return True
        
        # Check for workflow-like name patterns
        if 'name' in data and isinstance(data['name'], str):
            name_lower = data['name'].lower()
            if any(keyword in name_lower for keyword in ['workflow', 'automation', 'bot', 'script']):
                return True
        
        return matches >= 2
    
    def read_automa_workflows(self):
        """Read Automa workflows from Chrome's IndexedDB"""
        workflows = []
        
        extension_db = self.find_automa_extension_db()
        if not extension_db:
            self.logger.info("No Automa extension database found")
            return workflows
        
        self.logger.info(f"Reading from extension database: {extension_db}")
        
        # Read all files in the extension directory
        for file_path in extension_db.rglob('*'):
            if file_path.is_file():
                # Read different file types
                if file_path.suffix in ['.ldb', '.log']:
                    file_workflows = self.read_leveldb_file(file_path)
                    workflows.extend(file_workflows)
                    if file_workflows:
                        self.logger.info(f"Found {len(file_workflows)} workflows in {file_path.name}")
        
        return workflows
    
    def read_automa_logs(self, limit=50):
        """Read Automa execution logs"""
        workflows = self.read_automa_workflows()
        
        # Convert workflows to log format
        logs = []
        for workflow in workflows:
            log_entry = {
                'workflow_name': workflow.get('name', 'Unknown'),
                'workflow_id': workflow.get('workflowId', workflow.get('id', 'unknown')),
                'status': self._determine_status(workflow),
                'captured_at': self._parse_timestamp(
                    workflow.get('endedAt') or 
                    workflow.get('updatedAt') or 
                    workflow.get('createdAt')
                ),
                'execution_id': f"chrome_{workflow.get('id', len(logs))}",
                'raw_logs': workflow,
                'processed_logs': self._process_workflow_data(workflow),
                'table_source': 'chrome_indexeddb',
                'db_file': 'chrome_extension_indexeddb',
                'metadata': self._extract_metadata(workflow)
            }
            logs.append(log_entry)
        
        # Sort by timestamp
        return sorted(logs, key=lambda x: x.get('captured_at', datetime.min), reverse=True)[:limit]
    
    def _determine_status(self, workflow):
        """Determine workflow status"""
        if 'status' in workflow:
            return workflow['status']
        
        if workflow.get('isDisabled'):
            return 'disabled'
        
        if workflow.get('endedAt'):
            return 'completed'
        
        return 'unknown'
    
    def _parse_timestamp(self, timestamp):
        """Parse timestamp"""
        if not timestamp:
            return datetime.now()
        
        try:
            if isinstance(timestamp, (int, float)):
                if timestamp > 1e10:
                    return datetime.fromtimestamp(timestamp / 1000)
                else:
                    return datetime.fromtimestamp(timestamp)
            elif isinstance(timestamp, str):
                return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except:
            pass
        
        return datetime.now()
    
    def _process_workflow_data(self, workflow):
        """Process workflow data"""
        # Count nodes/blocks
        node_count = 0
        if 'drawflow' in workflow:
            nodes = workflow.get('drawflow', {}).get('Home', {}).get('data', {})
            node_count = len(nodes) if isinstance(nodes, dict) else 0
        elif 'blocks' in workflow:
            node_count = len(workflow['blocks']) if isinstance(workflow['blocks'], list) else 0
        
        has_error = self._determine_status(workflow) in ['error', 'failed']
        
        return {
            'summary': {
                'total_workflow_logs': 1,
                'total_errors': 1 if has_error else 0,
                'total_console_messages': 0,
                'total_execution_entries': 1,
                'has_timeouts_or_failures': has_error,
                'node_count': node_count
            },
            'error_messages': [],
            'timeouts_and_failures': [],
            'workflow_steps': self._extract_steps(workflow),
            'console_messages': [],
            'execution_timeline': [workflow]
        }
    
    def _extract_steps(self, workflow):
        """Extract workflow steps"""
        steps = []
        
        # From drawflow
        if 'drawflow' in workflow:
            drawflow = workflow.get('drawflow', {}).get('Home', {}).get('data', {})
            for node_id, node_data in drawflow.items():
                if isinstance(node_data, dict):
                    steps.append({
                        'id': node_id,
                        'name': node_data.get('name', f'Step {node_id}'),
                        'type': node_data.get('data', {}).get('name', 'unknown'),
                        'status': 'unknown'
                    })
        
        # From blocks
        elif 'blocks' in workflow and isinstance(workflow['blocks'], list):
            for i, block in enumerate(workflow['blocks']):
                if isinstance(block, dict):
                    steps.append({
                        'id': block.get('id', f'block_{i}'),
                        'name': block.get('name', f'Block {i}'),
                        'type': block.get('type', 'unknown'),
                        'status': 'unknown'
                    })
        
        return steps
    
    def _extract_metadata(self, workflow):
        """Extract metadata"""
        return {
            'workflow_keys': list(workflow.keys()),
            'has_drawflow': 'drawflow' in workflow,
            'has_blocks': 'blocks' in workflow,
            'is_disabled': workflow.get('isDisabled', False),
            'category': workflow.get('category', 'Unknown'),
            'version': workflow.get('version', 'Unknown')
        }
    
    def get_debug_info(self):
        """Get debug information about the IndexedDB structure"""
        debug_info = {
            'indexeddb_exists': self.indexeddb_path.exists(),
            'extension_directories': [],
            'file_analysis': []
        }
        
        if self.indexeddb_path.exists():
            for item in self.indexeddb_path.iterdir():
                if item.is_dir():
                    debug_info['extension_directories'].append({
                        'name': item.name,
                        'is_extension': 'chrome-extension_' in item.name,
                        'file_count': len(list(item.rglob('*'))) if item.is_dir() else 0
                    })
                    
                    # Analyze files in extension directories
                    if 'chrome-extension_' in item.name:
                        for file_path in item.rglob('*'):
                            if file_path.is_file():
                                size = file_path.stat().st_size
                                debug_info['file_analysis'].append({
                                    'file': str(file_path.relative_to(self.indexeddb_path)),
                                    'size': size,
                                    'extension': file_path.suffix,
                                    'might_contain_workflows': size > 100 and file_path.suffix in ['.ldb', '.log']
                                })
        
        return debug_info