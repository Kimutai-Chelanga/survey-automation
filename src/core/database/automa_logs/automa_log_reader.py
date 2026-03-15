import sqlite3
import json
import os
from datetime import datetime
from pathlib import Path
import glob
import struct
import logging
import re
from typing import List, Dict, Any, Optional, Tuple

# LevelDB support
try:
    import plyvel
    PLYVEL_AVAILABLE = True
except ImportError:
    PLYVEL_AVAILABLE = False

class AutomaLogReader:
    def __init__(self, chrome_profile_path=None):
        """
        Initialize AutomaLogReader with auto-detection of Chrome profile path
        
        Args:
            chrome_profile_path: Optional path to Chrome profile. If None, will auto-detect.
        """
        self.logger = logging.getLogger(__name__)
        
        # Configure logging for container environment
        if not self.logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        
        # Check LevelDB support
        if not PLYVEL_AVAILABLE:
            self.logger.warning("plyvel not available. Install with 'pip install plyvel' for better LevelDB support.")
            self.logger.warning("Falling back to basic binary parsing (less reliable).")
        
        # Auto-detect Chrome profile path if not provided
        if chrome_profile_path is None:
            self.chrome_profile_path = self._auto_detect_chrome_profile_path()
        else:
            self.chrome_profile_path = Path(chrome_profile_path)
        
        self.logger.info(f"Using Chrome profile path: {self.chrome_profile_path}")
        
        # Set IndexedDB path
        self.indexeddb_path = self.chrome_profile_path / "Default" / "IndexedDB"
        self.logger.info(f"IndexedDB path: {self.indexeddb_path}")
        
        # Automa detection keywords - these indicate Automa-related content
        self.automa_indicators = [
            'automa', 'workflow', 'drawflow', 'automation', 'blocks',
            'trigger', 'executeWorkflow', 'workflowId', 'blocksDetail'
        ]
        
        # Cache for better performance
        self._extension_paths_cache = None
        self._cache_timestamp = None
        self._cache_ttl = 300  # 5 minutes cache TTL
    
    def _auto_detect_chrome_profile_path(self):
        """Auto-detect Chrome profile path based on common container mount points"""
        possible_paths = [
            # Airflow container paths
            "/opt/airflow/chrome_persistent_profile",
            "/opt/airflow/chrome_profile",
            
            # Workspace paths (devcontainer)
            "/workspace/chrome_profile",
            "/workspace/chrome_persistent_profile",
            
            # App container paths
            "/app/chrome_profile",
            "/app/chrome_persistent_profile",
            
            # Data volume paths
            "/data/chrome_profile",
            "/data/chrome_persistent_profile",
            
            # Root level paths
            "/chrome_profile",
            "/chrome_persistent_profile",
            
            # Current working directory
            "./chrome_profile",
            "./chrome_persistent_profile",
        ]
        
        self.logger.info("Auto-detecting Chrome profile path...")
        
        for path_str in possible_paths:
            path = Path(path_str)
            self.logger.debug(f"Checking path: {path}")
            
            # Check if path exists and has the expected structure
            if path.exists() and path.is_dir():
                default_path = path / "Default"
                indexeddb_path = default_path / "IndexedDB"
                
                if default_path.exists() and indexeddb_path.exists():
                    # Check if it contains any extension IndexedDB data
                    extension_dirs = list(indexeddb_path.glob("chrome-extension_*"))
                    if extension_dirs:
                        self.logger.info(f"Found Chrome profile with extensions at: {path}")
                        self.logger.info(f"Found {len(extension_dirs)} extension(s)")
                        return path
                    else:
                        self.logger.debug(f"Path {path} exists but no extensions found")
                else:
                    self.logger.debug(f"Path {path} exists but missing Default/IndexedDB structure")
        
        # Fallback to default Airflow path even if it doesn't exist
        fallback_path = Path("/opt/airflow/chrome_persistent_profile")
        self.logger.warning(f"Could not auto-detect Chrome profile path. Using fallback: {fallback_path}")
        return fallback_path
    
    def verify_setup(self):
        """Verify that the Chrome profile setup is correct and accessible"""
        issues = []
        
        # Check Chrome profile path
        if not self.chrome_profile_path.exists():
            issues.append(f"Chrome profile path does not exist: {self.chrome_profile_path}")
        elif not self.chrome_profile_path.is_dir():
            issues.append(f"Chrome profile path is not a directory: {self.chrome_profile_path}")
        
        # Check Default directory
        default_path = self.chrome_profile_path / "Default"
        if not default_path.exists():
            issues.append(f"Default profile directory does not exist: {default_path}")
        elif not default_path.is_dir():
            issues.append(f"Default profile path is not a directory: {default_path}")
        
        # Check IndexedDB
        if not self.indexeddb_path.exists():
            issues.append(f"IndexedDB directory does not exist: {self.indexeddb_path}")
        elif not self.indexeddb_path.is_dir():
            issues.append(f"IndexedDB path is not a directory: {self.indexeddb_path}")
        
        # Check for extensions
        if self.indexeddb_path.exists():
            extension_dirs = list(self.indexeddb_path.glob("chrome-extension_*"))
            automa_dirs = self._find_automa_extensions()
            
            if not extension_dirs:
                issues.append("No Chrome extensions found in IndexedDB")
            else:
                self.logger.info(f"Found {len(extension_dirs)} extension(s)")
                
            if not automa_dirs:
                issues.append("No Automa extension found")
            else:
                self.logger.info(f"Found {len(automa_dirs)} potential Automa extension(s)")
        
        return {
            'is_valid': len(issues) == 0,
            'issues': issues,
            'chrome_profile_path': str(self.chrome_profile_path),
            'indexeddb_path': str(self.indexeddb_path),
            'extensions_found': len(list(self.indexeddb_path.glob("chrome-extension_*"))) if self.indexeddb_path.exists() else 0,
            'automa_extensions_found': len(self._find_automa_extensions())
        }
    
    def _find_automa_extensions(self):
        """Find Automa extensions in the IndexedDB directory"""
        if not self.indexeddb_path.exists():
            return []
        
        automa_extensions = []
        
        try:
            for item in self.indexeddb_path.iterdir():
                if item.is_dir() and item.name.startswith("chrome-extension_"):
                    # Extract extension ID from directory name
                    match = re.match(r"chrome-extension_([a-z]+)_\d+\.indexeddb\.leveldb", item.name)
                    if match:
                        extension_id = match.group(1)
                        
                        # Check if directory contains Automa-like data (content-based detection)
                        is_automa_like = self._check_if_automa_extension(item, extension_id)
                        
                        if is_automa_like:
                            automa_extensions.append({
                                'path': item,
                                'extension_id': extension_id,
                                'directory_name': item.name,
                                'is_automa': True,
                                'confidence': self._calculate_automa_confidence(item, extension_id)
                            })
                            
                            self.logger.info(f"Found potential Automa extension: {extension_id}")
        
        except Exception as e:
            self.logger.error(f"Error finding Automa extensions: {e}")
        
        # Sort by confidence score (higher confidence first)
        automa_extensions.sort(key=lambda x: x.get('confidence', 0), reverse=True)
    
    def _calculate_automa_confidence(self, db_path, extension_id):
        """Calculate confidence score that this is an Automa extension"""
        confidence_score = 0
        
        try:
            # Check multiple files for Automa indicators
            db_files = list(db_path.glob("*.db")) + list(db_path.glob("*.ldb")) + list(db_path.glob("LOG"))
            
            total_indicators = 0
            files_checked = 0
            
            for db_file in db_files[:5]:  # Check up to 5 files
                try:
                    files_checked += 1
                    indicators_found = self._count_automa_indicators(db_file)
                    total_indicators += indicators_found
                    
                    # Bonus for finding specific Automa structures
                    if self._has_drawflow_structure(db_file):
                        confidence_score += 30
                    
                    if self._has_workflow_structure(db_file):
                        confidence_score += 20
                        
                except Exception:
                    continue
            
            # Base score from indicator frequency
            if files_checked > 0:
                indicator_density = total_indicators / files_checked
                confidence_score += min(indicator_density * 10, 50)  # Max 50 points from indicators
            
            # Bonus for having multiple database files (typical of active extensions)
            if len(db_files) > 2:
                confidence_score += 10
                
        except Exception:
            pass
        
        return min(confidence_score, 100)  # Cap at 100
    
    def _count_automa_indicators(self, db_file):
        """Count Automa-related indicators in a database file"""
        try:
            with open(db_file, 'rb') as f:
                content = f.read(16384)  # Read first 16KB
                content_str = content.decode('utf-8', errors='ignore').lower()
                
                indicator_count = 0
                for indicator in self.automa_indicators:
                    # Count occurrences of each indicator
                    indicator_count += content_str.count(indicator.lower())
                
                return indicator_count
                
        except Exception:
            return 0
    
    def _has_drawflow_structure(self, db_file):
        """Check if file contains drawflow structure (very specific to Automa)"""
        try:
            with open(db_file, 'rb') as f:
                content = f.read(32768)  # Read first 32KB
                content_str = content.decode('utf-8', errors='ignore')
                
                # Look for drawflow JSON structure
                return ('drawflow' in content_str.lower() and 
                        '"home"' in content_str.lower() and 
                        '"data"' in content_str.lower())
                        
        except Exception:
            return False
    
    def _has_workflow_structure(self, db_file):
        """Check if file contains workflow-like JSON structures"""
        try:
            with open(db_file, 'rb') as f:
                content = f.read(32768)
                content_str = content.decode('utf-8', errors='ignore')
                
                # Look for workflow patterns
                workflow_patterns = [
                    '"workflowId"',
                    '"blocks"',
                    '"executeWorkflow"',
                    '"blocksDetail"'
                ]
                
                return any(pattern.lower() in content_str.lower() for pattern in workflow_patterns)
                
        except Exception:
            return False
    
    def _check_if_automa_extension(self, db_path, extension_id):
        """Check if an extension directory likely contains Automa data"""
        try:
            confidence = self._calculate_automa_confidence(db_path, extension_id)
            # Consider it Automa if confidence is above threshold
            return confidence >= 20  # Adjustable threshold
            
        except Exception:
            return False
    
    def find_extension_indexeddb_paths(self):
        """Find Chrome extension IndexedDB paths for Automa with caching"""
        current_time = datetime.now().timestamp()
        
        if (self._extension_paths_cache is None or 
            self._cache_timestamp is None or 
            current_time - self._cache_timestamp > self._cache_ttl):
            
            self._extension_paths_cache = self._find_automa_extensions()
            self._cache_timestamp = current_time
            self.logger.debug("Refreshed extension paths cache")
        
        return self._extension_paths_cache
    
    def find_logs_database(self):
        """Find the logs IndexedDB database files with auto-detection"""
        db_files = []
        
        # Get Automa extension paths
        automa_extensions = self.find_extension_indexeddb_paths()
        
        self.logger.info(f"Found {len(automa_extensions)} Automa extension(s)")
        
        # Process each Automa extension
        for ext_info in automa_extensions:
            ext_path = ext_info['path']
            try:
                self.logger.info(f"Processing extension: {ext_info['extension_id']} at {ext_path}")
                
                # Look for database files in extension directory
                for pattern in ["*.db", "*.ldb", "LOG", "MANIFEST-*", "*.log"]:
                    for db_file in ext_path.glob(pattern):
                        if db_file.is_file() and db_file.stat().st_size > 0:
                            db_files.append(db_file)
                            self.logger.debug(f"Found database file: {db_file}")
                        
            except Exception as e:
                self.logger.warning(f"Error processing extension path {ext_path}: {e}")
        
        # If no Automa extensions found, check all extensions
        if not db_files and self.indexeddb_path.exists():
            self.logger.info("No Automa extensions found, checking all extensions...")
            try:
                for ext_dir in self.indexeddb_path.glob("chrome-extension_*"):
                    if ext_dir.is_dir():
                        for pattern in ["*.db", "*.ldb", "LOG"]:
                            for db_file in ext_dir.glob(pattern):
                                if db_file.is_file() and db_file.stat().st_size > 0:
                                    db_files.append(db_file)
                                    
            except Exception as e:
                self.logger.error(f"Error in fallback search: {e}")
        
        # Remove duplicates
        unique_files = list(set(db_files))
        self.logger.info(f"Found {len(unique_files)} unique database files")
        
        return unique_files
    
    def list_available_extensions(self):
        """List all available Chrome extensions in IndexedDB"""
        if not self.indexeddb_path.exists():
            return []
        
        extensions = []
        
        try:
            for item in self.indexeddb_path.iterdir():
                if item.is_dir():
                    if item.name.startswith("chrome-extension_"):
                        # Extract extension ID
                        match = re.match(r"chrome-extension_([a-z]+)_\d+\.indexeddb\.leveldb", item.name)
                        extension_id = match.group(1) if match else "unknown"
                        
                        extensions.append({
                            'type': 'extension',
                            'id': extension_id,
                            'directory': item.name,
                            'path': str(item),
                            'is_automa': self._check_if_automa_extension(item, extension_id),
                            'size': sum(f.stat().st_size for f in item.rglob('*') if f.is_file())
                        })
                    else:
                        # Other IndexedDB (websites, etc.)
                        extensions.append({
                            'type': 'website',
                            'id': item.name,
                            'directory': item.name,
                            'path': str(item),
                            'is_automa': False,
                            'size': sum(f.stat().st_size for f in item.rglob('*') if f.is_file())
                        })
        
        except Exception as e:
            self.logger.error(f"Error listing extensions: {e}")
        
        return sorted(extensions, key=lambda x: (not x['is_automa'], x['size']), reverse=True)
    
    def read_automa_logs(self, limit=50):
        """Read Automa logs with auto-detection"""
        try:
            # Verify setup first
            setup_status = self.verify_setup()
            if not setup_status['is_valid']:
                self.logger.error("Setup verification failed:")
                for issue in setup_status['issues']:
                    self.logger.error(f"  - {issue}")
                return []
            
            logs = []
            db_files = self.find_logs_database()
            
            if not db_files:
                self.logger.warning("No database files found")
                return []
            
            self.logger.info(f"Processing {len(db_files)} database files")
            
            # Get extension info for context
            automa_extensions = self.find_extension_indexeddb_paths()
            ext_info_map = {str(ext['path']): ext for ext in automa_extensions}
            
            # Process each database file
            for db_file in db_files:
                try:
                    # Find matching extension info
                    ext_info = None
                    for ext_path, ext_data in ext_info_map.items():
                        if str(db_file).startswith(ext_path):
                            ext_info = ext_data
                            break
                    
                    file_logs = self._process_database_file(db_file, ext_info, limit)
                    logs.extend(file_logs)
                    
                    if file_logs:
                        self.logger.info(f"Found {len(file_logs)} logs in {db_file}")
                    
                except Exception as e:
                    self.logger.error(f"Error processing {db_file}: {e}")
            
            # Sort by timestamp and limit results
            sorted_logs = sorted(logs, key=lambda x: x.get('captured_at', datetime.min), reverse=True)
            final_logs = sorted_logs[:limit]
            
            self.logger.info(f"Returning {len(final_logs)} total logs")
            return final_logs
            
        except Exception as e:
            self.logger.error(f"Error reading Automa logs: {e}")
            return []
    
    def _process_database_file(self, db_file, ext_info, limit):
        """Process a single database file with proper LevelDB support"""
        logs = []
        
        try:
            self.logger.debug(f"Processing database file: {db_file}")
            
            # Determine if this is a LevelDB directory or single file
            if db_file.is_dir() or str(db_file).endswith('.leveldb'):
                # This is a LevelDB database
                if PLYVEL_AVAILABLE:
                    logs.extend(self._process_leveldb_database_proper(db_file, ext_info, limit))
                else:
                    logs.extend(self._process_leveldb_database_fallback(db_file, ext_info, limit))
            else:
                # Try as SQLite first, then fallback to binary parsing
                try:
                    conn = sqlite3.connect(str(db_file))
                    conn.close()
                    logs.extend(self._process_sqlite_database(db_file, {}, ext_info, limit))
                except sqlite3.DatabaseError:
                    # Not SQLite, try as binary file
                    if PLYVEL_AVAILABLE:
                        logs.extend(self._process_leveldb_database_proper(db_file, ext_info, limit))
                    else:
                        logs.extend(self._process_leveldb_database_fallback(db_file, ext_info, limit))
                        
        except Exception as e:
            self.logger.error(f"Error processing database file {db_file}: {e}")
        
        return logs
    
    def _process_leveldb_database_proper(self, db_file, ext_info, limit):
        """Process LevelDB database using plyvel (proper method)"""
        logs = []
        
        try:
            # Determine the database path
            if db_file.is_dir():
                db_path = str(db_file)
            else:
                # For .leveldb files, the parent directory is usually the database
                db_path = str(db_file.parent)
            
            self.logger.info(f"Opening LevelDB at: {db_path}")
            
            # Open LevelDB database
            db = plyvel.DB(db_path, create_if_missing=False)
            
            processed_count = 0
            
            try:
                # Iterate through all key-value pairs
                for key, value in db.iterator():
                    if processed_count >= limit:
                        break
                    
                    try:
                        # Try to decode key and value
                        key_str = key.decode('utf-8', errors='ignore')
                        
                        # Try different value decoding approaches
                        value_data = self._decode_leveldb_value(value)
                        
                        if value_data and self._looks_like_workflow_data(value_data):
                            log_entry = self._create_log_entry(
                                value_data, f"leveldb_key_{key_str[:50]}", db_file, {}, ext_info, {}
                            )
                            logs.append(log_entry)
                            processed_count += 1
                            
                        # Also check if the key itself contains workflow data (some IndexedDB implementations)
                        elif isinstance(value_data, dict) and any(
                            indicator in str(value_data).lower() 
                            for indicator in self.automa_indicators
                        ):
                            # This might be workflow-related even if not perfect structure
                            log_entry = self._create_log_entry(
                                value_data, f"leveldb_key_{key_str[:50]}", db_file, {}, ext_info, {}
                            )
                            logs.append(log_entry)
                            processed_count += 1
                            
                    except Exception as decode_error:
                        self.logger.debug(f"Error decoding LevelDB entry: {decode_error}")
                        continue
                        
            finally:
                db.close()
                
            self.logger.info(f"Processed LevelDB, found {len(logs)} workflow entries")
            
        except Exception as e:
            self.logger.error(f"Error processing LevelDB database {db_file}: {e}")
            # Fallback to binary parsing if LevelDB fails
            if not logs:
                logs = self._process_leveldb_database_fallback(db_file, ext_info, limit)
        
        return logs
    
    def _process_sqlite_database(self, db_file, structure, ext_info, limit):
        """Process SQLite database file"""
        logs = []
        
        try:
            conn = sqlite3.connect(str(db_file))
            conn.row_factory = sqlite3.Row
            
            # Get table names
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                try:
                    self.logger.debug(f"Checking table: {table}")
                    
                    # Try to read table contents
                    cursor = conn.execute(f"SELECT * FROM `{table}` LIMIT ?", (limit * 2,))
                    rows = cursor.fetchall()
                    
                    for row in rows:
                        try:
                            row_dict = dict(row)
                            
                            # Look for Automa-specific data patterns
                            workflow_data = self._extract_workflow_data(row_dict)
                            
                            if workflow_data:
                                log_entry = self._create_log_entry(
                                    workflow_data, table, db_file, structure, ext_info, row_dict
                                )
                                logs.append(log_entry)
                                
                        except Exception as row_error:
                            self.logger.debug(f"Error processing row in {table}: {row_error}")
                            continue
                            
                except Exception as table_error:
                    self.logger.debug(f"Error processing table {table}: {table_error}")
                    continue
            
            conn.close()
            
        except Exception as e:
            self.logger.error(f"Error processing SQLite database {db_file}: {e}")
        
        return logs
    
    def _extract_workflow_data(self, row_dict):
        """Extract workflow data from various row formats"""
        workflow_data = None
        
        # Try different column names that might contain workflow data
        possible_columns = ['value', 'data', 'object_data', 'blob_data', 'json_data', 'workflow_data']
        
        for column in possible_columns:
            if column in row_dict and row_dict[column]:
                try:
                    value = row_dict[column]
                    
                    # Handle different data types
                    if isinstance(value, bytes):
                        # Try to decode bytes
                        try:
                            value = value.decode('utf-8')
                        except:
                            continue
                    
                    if isinstance(value, str) and value.strip():
                        # Try to parse as JSON
                        if value.strip().startswith('{') or value.strip().startswith('['):
                            try:
                                parsed = json.loads(value)
                                if self._looks_like_workflow_data(parsed):
                                    workflow_data = parsed
                                    break
                            except json.JSONDecodeError:
                                continue
                        
                        # Check if it contains workflow-related keywords
                        if any(keyword in value.lower() for keyword in self.automa_indicators):
                            # Try to extract JSON from string
                            json_matches = re.findall(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', value)
                            for match in json_matches:
                                try:
                                    parsed = json.loads(match)
                                    if self._looks_like_workflow_data(parsed):
                                        workflow_data = parsed
                                        break
                                except:
                                    continue
                            if workflow_data:
                                break
                    
                except Exception:
                    continue
        
        # Also check if the entire row looks like workflow data
        if not workflow_data and self._looks_like_workflow_data(row_dict):
            workflow_data = row_dict
        
        return workflow_data
    
    def _decode_leveldb_value(self, value_bytes):
        """Decode LevelDB value with multiple strategies"""
        if not value_bytes:
            return None
        
        # Strategy 1: Direct UTF-8 decode and JSON parse
        try:
            value_str = value_bytes.decode('utf-8')
            if value_str.strip().startswith('{') or value_str.strip().startswith('['):
                return json.loads(value_str)
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        
        # Strategy 2: Skip potential headers/prefixes and try JSON
        try:
            value_str = value_bytes.decode('utf-8', errors='ignore')
            
            # Look for JSON objects in the string
            json_start = value_str.find('{')
            if json_start != -1:
                json_candidate = value_str[json_start:]
                # Find the matching closing brace
                brace_count = 0
                json_end = 0
                for i, char in enumerate(json_candidate):
                    if char == '{':
                        brace_count += 1
                    elif char == '}':
                        brace_count -= 1
                        if brace_count == 0:
                            json_end = i + 1
                            break
                
                if json_end > 0:
                    json_str = json_candidate[:json_end]
                    return json.loads(json_str)
                    
        except (UnicodeDecodeError, json.JSONDecodeError):
            pass
        
        # Strategy 3: Handle potential binary prefixes (Chrome sometimes adds metadata)
        try:
            # Skip first few bytes that might be metadata and try JSON
            for skip_bytes in [1, 2, 4, 8, 16]:
                if len(value_bytes) > skip_bytes:
                    try:
                        value_str = value_bytes[skip_bytes:].decode('utf-8')
                        if value_str.strip().startswith('{'):
                            return json.loads(value_str.strip())
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        continue
        except:
            pass
        
        # Strategy 4: Return as dictionary if it contains Automa indicators
        try:
            value_str = value_bytes.decode('utf-8', errors='ignore')
            if any(indicator in value_str.lower() for indicator in self.automa_indicators):
                # Create a simple structure to capture the content
                return {
                    'raw_content': value_str[:1000],  # Limit to prevent huge entries
                    'contains_automa_data': True,
                    'detected_indicators': [
                        indicator for indicator in self.automa_indicators 
                        if indicator in value_str.lower()
                    ]
                }
        except:
            pass
        
        return None
    
    def _process_leveldb_database_fallback(self, db_file, ext_info, limit):
        """Fallback LevelDB processing when plyvel is not available"""
        logs = []
        
        try:
            # If it's a directory, look for database files within it
            if db_file.is_dir():
                db_files = list(db_file.glob("*.ldb")) + list(db_file.glob("*.log")) + list(db_file.glob("LOG"))
            else:
                db_files = [db_file]
            
            processed_count = 0
            
            for file_path in db_files:
                if processed_count >= limit:
                    break
                    
                try:
                    with open(file_path, 'rb') as f:
                        # Read file in chunks to handle large files
                        chunk_size = 32768  # 32KB chunks
                        content = b""
                        
                        while len(content) < 2097152:  # Max 2MB to prevent memory issues
                            chunk = f.read(chunk_size)
                            if not chunk:
                                break
                            content += chunk
                        
                        # Decode and look for JSON patterns
                        content_str = content.decode('utf-8', errors='ignore')
                        
                        # Look for JSON objects that might be workflows
                        json_pattern = r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}'
                        json_matches = re.findall(json_pattern, content_str)
                        
                        for i, match in enumerate(json_matches):
                            if processed_count >= limit:
                                break
                            
                            try:
                                data = json.loads(match)
                                if self._looks_like_workflow_data(data):
                                    log_entry = self._create_log_entry(
                                        data, f"fallback_entry_{i}", db_file, {}, ext_info, {}
                                    )
                                    logs.append(log_entry)
                                    processed_count += 1
                            except json.JSONDecodeError:
                                continue
                                
                except Exception as file_error:
                    self.logger.debug(f"Error processing file {file_path}: {file_error}")
                    continue
                    
        except Exception as e:
            self.logger.error(f"Error in fallback LevelDB processing {db_file}: {e}")
        
        return logs
    
    def _looks_like_workflow_data(self, data):
        """Check if data structure looks like Automa workflow data"""
        if not isinstance(data, dict):
            return False
        
        # Check for common Automa workflow properties
        workflow_indicators = [
            'name', 'workflowId', 'id', 'drawflow', 'blocks', 
            'nodes', 'edges', 'version', 'description', 'createdAt', 'updatedAt'
        ]
        
        # Check for execution-related properties
        execution_indicators = [
            'status', 'endedAt', 'startedAt', 'logs', 'errors', 'execId'
        ]
        
        # Special case: check for nested drawflow structure (very specific to Automa)
        if 'drawflow' in data and isinstance(data['drawflow'], dict):
            if 'Home' in data['drawflow'] and isinstance(data['drawflow']['Home'], dict):
                if 'data' in data['drawflow']['Home']:
                    return True
        
        # Count matches
        workflow_matches = sum(1 for key in workflow_indicators if key in data)
        execution_matches = sum(1 for key in execution_indicators if key in data)
        
        total_matches = workflow_matches + execution_matches
        
        # Consider it workflow data if it has enough indicators
        return total_matches >= 2 or workflow_matches >= 1
    
    def _create_log_entry(self, workflow_data, table, db_file, structure, ext_info, row_dict):
        """Create a standardized log entry"""
        return {
            'workflow_name': workflow_data.get('name', 'Unknown'),
            'workflow_id': workflow_data.get('workflowId', workflow_data.get('id', 'unknown')),
            'status': self._determine_status(workflow_data),
            'captured_at': self._parse_timestamp(
                workflow_data.get('endedAt') or 
                workflow_data.get('timestamp') or 
                workflow_data.get('createdAt') or
                workflow_data.get('updatedAt')
            ),
            'execution_id': f"{table}_{workflow_data.get('id', 'unknown')}",
            'raw_logs': workflow_data,
            'processed_logs': self._process_log_data(workflow_data),
            'table_source': table,
            'db_file': str(db_file),
            'extension_info': ext_info,
            'metadata': self._extract_metadata(row_dict, workflow_data, ext_info)
        }
    
    def _determine_status(self, workflow_data):
        """Determine workflow execution status"""
        if 'status' in workflow_data:
            return workflow_data['status']
        
        # Try to infer status from other fields
        if workflow_data.get('endedAt'):
            if workflow_data.get('errors') or workflow_data.get('error'):
                return 'error'
            else:
                return 'completed'
        elif workflow_data.get('startedAt'):
            return 'running'
        else:
            return 'unknown'
    
    def _parse_timestamp(self, timestamp):
        """Parse various timestamp formats"""
        if not timestamp:
            return datetime.now()
        
        try:
            if isinstance(timestamp, (int, float)):
                # Assume milliseconds if > 1e10, otherwise seconds
                if timestamp > 1e10:
                    return datetime.fromtimestamp(timestamp / 1000)
                else:
                    return datetime.fromtimestamp(timestamp)
            elif isinstance(timestamp, str):
                # Try ISO format
                try:
                    return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
                except:
                    pass
        except Exception as e:
            self.logger.debug(f"Error parsing timestamp {timestamp}: {e}")
        
        return datetime.now()
    
    def _process_log_data(self, log_data):
        """Process raw log data into structured format"""
        if not log_data:
            return {}
        
        has_error = self._determine_status(log_data) in ['error', 'failed', 'stopped']
        
        # Extract console logs
        console_logs = []
        if 'logs' in log_data and isinstance(log_data['logs'], list):
            console_logs = log_data['logs']
        elif 'console' in log_data and isinstance(log_data['console'], list):
            console_logs = log_data['console']
        
        # Extract error messages
        error_messages = []
        if has_error:
            if 'errors' in log_data:
                if isinstance(log_data['errors'], list):
                    error_messages.extend(log_data['errors'])
                else:
                    error_messages.append(str(log_data['errors']))
            if 'error' in log_data:
                error_messages.append(str(log_data['error']))
            if 'message' in log_data and has_error:
                error_messages.append(log_data['message'])
        
        return {
            'summary': {
                'total_workflow_logs': 1,
                'total_errors': len(error_messages),
                'total_console_messages': len(console_logs),
                'total_execution_entries': 1,
                'has_timeouts_or_failures': has_error
            },
            'error_messages': error_messages,
            'timeouts_and_failures': [],
            'workflow_steps': self._extract_steps(log_data),
            'console_messages': console_logs,
            'execution_timeline': [log_data]
        }
    
    def _extract_steps(self, log_data):
        """Extract workflow steps from log data"""
        steps = []
        
        # Try to extract from drawflow data
        if 'drawflow' in log_data and isinstance(log_data['drawflow'], dict):
            drawflow = log_data.get('drawflow', {}).get('Home', {}).get('data', {})
            for node_id, node_data in drawflow.items():
                if isinstance(node_data, dict):
                    steps.append({
                        'id': node_id,
                        'name': node_data.get('name', f'Step {node_id}'),
                        'type': node_data.get('data', {}).get('name', 'unknown'),
                        'status': 'unknown',
                        'duration': 'unknown'
                    })
        
        # Try to extract from blocks data (alternative format)
        elif 'blocks' in log_data and isinstance(log_data['blocks'], list):
            for i, block in enumerate(log_data['blocks']):
                if isinstance(block, dict):
                    steps.append({
                        'id': block.get('id', f'block_{i}'),
                        'name': block.get('name', f'Block {i}'),
                        'type': block.get('type', 'unknown'),
                        'status': 'unknown',
                        'duration': 'unknown'
                    })
        
        return steps
    
    def _extract_metadata(self, row_dict, workflow_data, ext_info):
        """Extract additional metadata"""
        metadata = {
            'row_columns': list(row_dict.keys()) if row_dict else [],
            'workflow_keys': list(workflow_data.keys()) if workflow_data else [],
            'has_drawflow': 'drawflow' in workflow_data if workflow_data else False,
            'has_logs': 'logs' in workflow_data if workflow_data else False,
            'has_errors': bool(workflow_data.get('errors') or workflow_data.get('error')) if workflow_data else False,
            'extension_id': ext_info['extension_id'] if ext_info else None,
            'is_extension_data': ext_info is not None,
            'extension_directory': ext_info['directory_name'] if ext_info else None
        }
        
        # Count nodes if drawflow exists
        if workflow_data and 'drawflow' in workflow_data:
            try:
                nodes = workflow_data.get('drawflow', {}).get('Home', {}).get('data', {})
                metadata['node_count'] = len(nodes) if isinstance(nodes, dict) else 0
            except:
                metadata['node_count'] = 0
        
        return metadata


# Convenience function for easy usage
def create_automa_reader(chrome_profile_path=None):
    """Create an AutomaLogReader with auto-detection
    
    Usage:
        reader = create_automa_reader()  # Auto-detect
        # or
        reader = create_automa_reader("/custom/chrome/path")  # Custom path
    """
    return AutomaLogReader(chrome_profile_path)


# Example usage and testing
if __name__ == "__main__":
    # Create reader with auto-detection
    reader = create_automa_reader()
    
    # Verify setup
    setup_status = reader.verify_setup()
    print("Setup Status:", setup_status)
    
    # List available extensions
    extensions = reader.list_available_extensions()
    print(f"\nFound {len(extensions)} extensions:")
    for ext in extensions:
        print(f"  - {ext['type']}: {ext['id']} ({'AUTOMA' if ext['is_automa'] else 'other'})")
    
    # Read logs
    logs = reader.read_automa_logs(limit=10)
    print(f"\nFound {len(logs)} workflow logs")
    
    for log in logs[:3]:  # Show first 3
        print(f"  - {log['workflow_name']} ({log['status']}) - {log['captured_at']}")