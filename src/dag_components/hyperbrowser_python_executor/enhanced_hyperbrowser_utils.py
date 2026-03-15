# enhanced_hyperbrowser_utils.py
"""
Enhanced Hyperbrowser Utilities for Python Workflow Execution

This module extends the base hyperbrowser_utils.py with additional functionality
specifically for Python-based workflow execution, video recording management,
and advanced session handling.
"""

import os
import logging
import time
import asyncio
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple, Union
from pymongo import MongoClient
from bson import ObjectId

from hyperbrowser import Hyperbrowser
from hyperbrowser.models import CreateSessionParams

# Import base utilities
from .hyperbrowser_utils import (
    get_mongodb_client,
    store_in_mongodb,
    get_from_mongodb,
    update_in_mongodb,
    get_hyperbrowser_profile_id,
    get_hyperbrowser_extension_id
)

# Configuration
HYPERBROWSER_API_KEY = os.environ.get('HYPERBROWSER_API_KEY')

class PythonSessionManager:
    """Enhanced session manager for Python workflow execution"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.hb_client = Hyperbrowser(api_key=api_key) if api_key else None
        self.active_sessions = {}
        
    def create_python_execution_session(
        self, 
        profile_id: str,
        extension_id: Optional[str] = None,
        enable_video_recording: bool = True,
        session_purpose: str = 'python_workflow_execution'
    ) -> Dict[str, Any]:
        """Create a session optimized for Python workflow execution"""
        
        if not self.hb_client:
            raise ValueError("Hyperbrowser API key not configured")
        
        try:
            # Enhanced session parameters for Python execution
            session_params = CreateSessionParams(
                screen={'width': 1920, 'height': 1080},
                use_stealth=True,
                profile={'id': profile_id, 'persist_changes': True},
                start_url='chrome://newtab/',
                browser_type='chrome'
            )
            
            # Enable recordings if requested
            if enable_video_recording:
                session_params.enableWebRecording = True
                session_params.enableVideoWebRecording = True
            
            # Add extension if provided
            if extension_id:
                session_params.extension_ids = [extension_id]
            
            # Create session
            session_response = self.hb_client.sessions.create(session_params)
            session_id = session_response.id
            
            # Store session info
            session_info = {
                'session_id': session_id,
                'profile_id': profile_id,
                'extension_id': extension_id,
                'created_at': datetime.now(),
                'session_purpose': session_purpose,
                'video_recording_enabled': enable_video_recording,
                'status': 'active',
                'python_executor': True
            }
            
            self.active_sessions[session_id] = session_info
            
            logging.info(f"Created Python execution session: {session_id}")
            return session_info
            
        except Exception as e:
            raise Exception(f"Failed to create Python execution session: {str(e)}")
    
    def get_session_websocket_url(self, session_id: str) -> str:
        """Get WebSocket URL for session"""
        try:
            session_details = self.hb_client.sessions.get(session_id)
            return session_details.ws_endpoint
        except Exception as e:
            raise Exception(f"Failed to get WebSocket URL for session {session_id}: {str(e)}")
    
    def stop_session(self, session_id: str) -> bool:
        """Stop a session and clean up"""
        try:
            if session_id in self.active_sessions:
                self.hb_client.sessions.stop(session_id)
                self.active_sessions[session_id]['status'] = 'stopped'
                self.active_sessions[session_id]['stopped_at'] = datetime.now()
                logging.info(f"Stopped session: {session_id}")
                return True
        except Exception as e:
            logging.error(f"Error stopping session {session_id}: {e}")
            return False
    
    def cleanup_all_sessions(self) -> int:
        """Cleanup all active sessions"""
        cleaned_up = 0
        for session_id in list(self.active_sessions.keys()):
            if self.stop_session(session_id):
                cleaned_up += 1
        return cleaned_up


class PythonWorkflowSessionManager:
    """Manages workflow sessions specifically for Python execution"""
    
    def __init__(self):
        self.session_manager = None
        if HYPERBROWSER_API_KEY:
            self.session_manager = PythonSessionManager(HYPERBROWSER_API_KEY)
    
    def create_python_workflow_session(
        self, 
        workflow_type: str = 'python_execution',
        enable_video_recording: bool = True,
        **context
    ) -> Dict[str, Any]:
        """Create a workflow session optimized for Python execution"""
        
        try:
            # Get profile and extension IDs
            profile_id = get_hyperbrowser_profile_id(**context)
            extension_id = get_hyperbrowser_extension_id(**context)
            
            if not profile_id:
                raise ValueError("No active Hyperbrowser profile found")
            
            if not self.session_manager:
                raise ValueError("Session manager not initialized - check API key")
            
            # Create session
            session_info = self.session_manager.create_python_execution_session(
                profile_id=profile_id,
                extension_id=extension_id,
                enable_video_recording=enable_video_recording,
                session_purpose=f'python_{workflow_type}'
            )
            
            # Enhanced session data for MongoDB
            enhanced_session_data = {
                'session_id': session_info['session_id'],
                'profile_id': profile_id,
                'extension_id': extension_id,
                'browser_type': 'chrome',
                'session_status': 'active',
                'is_active': True,
                'created_at': datetime.now(),
                'started_at': datetime.now(),
                'ended_at': None,
                'session_purpose': f'python_{workflow_type}',
                'workflow_type': workflow_type,
                'workflow_count': 0,
                'success_count': 0,
                'execution_mode': 'python',
                'video_recording_enabled': enable_video_recording,
                'session_metadata': {
                    'created_for': 'python_automated_workflow',
                    'dag_run_id': context.get('dag_run').run_id if context.get('dag_run') else None,
                    'parent_profile_id': profile_id,
                    'extension_loaded': bool(extension_id),
                    'stealth_enabled': True,
                    'screen_resolution': '1920x1080',
                    'python_executor': True,
                    'sdk_version': 'python',
                    'enhanced_features': {
                        'video_recording': enable_video_recording,
                        'async_execution': True,
                        'multi_approach_execution': True
                    }
                }
            }
            
            # Store in MongoDB
            mongodb_id = store_in_mongodb('browser_sessions', enhanced_session_data, **context)
            
            # Update profile usage
            try:
                client, db = get_mongodb_client()
                db.chrome_profiles.update_one(
                    {'profile_id': profile_id},
                    {
                        '$inc': {'usage_count': 1},
                        '$set': {'last_used_at': datetime.now()},
                        '$addToSet': {'execution_modes': 'python'}
                    }
                )
                client.close()
            except Exception as e:
                logging.warning(f"Could not update profile usage: {e}")
            
            session_info.update({
                'mongodb_id': mongodb_id,
                'browser_url': f"https://app.hyperbrowser.ai/sessions/{session_info['session_id']}"
            })
            
            logging.info(f"Created Python workflow session: {session_info['session_id']}")
            return session_info
            
        except Exception as e:
            logging.error(f"Failed to create Python workflow session: {e}")
            raise
    
    def close_python_workflow_session(self, session_id: str, **context) -> bool:
        """Close a Python workflow session"""
        try:
            if not session_id:
                logging.warning("No session ID provided for closing")
                return False
            
            # Stop session via session manager
            if self.session_manager:
                self.session_manager.stop_session(session_id)
            
            # Update MongoDB
            end_time = datetime.now()
            session = get_from_mongodb('browser_sessions', {'session_id': session_id}, **context)
            session_duration = None
            
            if session and session.get('started_at'):
                start_time = session['started_at']
                if isinstance(start_time, str):
                    start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                session_duration = (end_time - start_time).total_seconds()
            
            update_data = {
                'is_active': False,
                'ended_at': end_time,
                'session_status': 'completed_python'
            }
            
            if session_duration:
                update_data['session_duration'] = session_duration
            
            update_in_mongodb(
                'browser_sessions',
                {'session_id': session_id},
                update_data,
                **context
            )
            
            logging.info(f"Closed Python workflow session: {session_id}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to close Python workflow session {session_id}: {e}")
            return False


def create_python_workflow_session(workflow_type: str = 'general_execution', **context) -> Dict[str, Any]:
    """Create workflow session optimized for Python execution"""
    manager = PythonWorkflowSessionManager()
    return manager.create_python_workflow_session(
        workflow_type=workflow_type,
        enable_video_recording=True,
        **context
    )


def close_python_workflow_session(session_id: str, **context) -> bool:
    """Close Python workflow session"""
    manager = PythonWorkflowSessionManager()
    return manager.close_python_workflow_session(session_id, **context)


def update_python_workflow_session_stats(
    session_id: str,
    workflow_count: int = 0,
    success_count: int = 0,
    failed_count: int = 0,
    **context
) -> bool:
    """Update Python workflow session statistics"""
    try:
        update_data = {
            'workflow_count': workflow_count,
            'success_count': success_count,
            'failed_count': failed_count,
            'last_activity_at': datetime.now(),
            'execution_mode': 'python_enhanced'
        }
        
        success = update_in_mongodb(
            'browser_sessions',
            {'session_id': session_id},
            update_data,
            **context
        )
        
        if success:
            logging.info(f"Updated Python session stats for {session_id}: {workflow_count} total, {success_count} success")
        
        return success
        
    except Exception as e:
        logging.error(f"Failed to update Python session stats for {session_id}: {e}")
        return False


class PythonVideoRecordingManager:
    """Enhanced video recording manager for Python workflows"""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.hb_client = Hyperbrowser(api_key=api_key) if api_key else None
        self.recording_sessions = {}
    
    async def start_recording_session(
        self,
        session_id: str,
        workflow_name: str,
        metadata: Dict[str, Any] = None
    ) -> Dict[str, Any]:
        """Start a recording session with enhanced metadata"""
        if not self.hb_client:
            raise ValueError("Hyperbrowser client not initialized")
        
        recording_info = {
            'session_id': session_id,
            'workflow_name': workflow_name,
            'start_time': datetime.now(),
            'status': 'recording',
            'metadata': metadata or {}
        }
        
        self.recording_sessions[session_id] = recording_info
        
        logging.info(f"Started recording session for {workflow_name}")
        return recording_info
    
    async def get_recording_status(self, session_id: str) -> Dict[str, Any]:
        """Get current recording status with enhanced details"""
        try:
            if session_id not in self.recording_sessions:
                return {'status': 'not_found', 'error': 'Session not found'}
            
            # Get session details from Hyperbrowser
            session_details = self.hb_client.sessions.get(session_id)
            recording_info = self.recording_sessions[session_id]
            
            # Check for recording completion
            status = {
                'session_id': session_id,
                'workflow_name': recording_info.get('workflow_name'),
                'recording_status': 'processing',
                'start_time': recording_info.get('start_time'),
                'duration': (datetime.now() - recording_info.get('start_time', datetime.now())).total_seconds(),
                'video_url': None,
                'web_recording_url': None
            }
            
            # Extract recording information from session details
            if hasattr(session_details, 'recording') and session_details.recording:
                recording_data = session_details.recording
                if hasattr(recording_data, 'status'):
                    status['recording_status'] = recording_data.status
                if hasattr(recording_data, 'url'):
                    status['video_url'] = recording_data.url
                if hasattr(recording_data, 'web_url'):
                    status['web_recording_url'] = recording_data.web_url
            
            # If session is stopped/completed, assume recording is completed
            if hasattr(session_details, 'status') and session_details.status in ['stopped', 'completed']:
                if status['recording_status'] == 'processing':
                    status['recording_status'] = 'completed'
            
            return status
            
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'session_id': session_id
            }
    
    def store_recording_metadata(
        self,
        session_id: str,
        workflow_execution_data: Dict[str, Any],
        **context
    ) -> bool:
        """Store recording metadata in MongoDB"""
        try:
            recording_data = {
                'session_id': session_id,
                'workflow_name': workflow_execution_data.get('workflow_name'),
                'recording_created_at': datetime.now(),
                'execution_success': workflow_execution_data.get('success', False),
                'execution_time': workflow_execution_data.get('execution_time'),
                'steps_taken': workflow_execution_data.get('steps_taken', 0),
                'video_url': workflow_execution_data.get('video_url'),
                'profile_id': workflow_execution_data.get('profile_id'),
                'extension_id': workflow_execution_data.get('extension_id'),
                'execution_mode': 'python_video_recording',
                'metadata': {
                    'dag_run_id': context.get('dag_run').run_id if context.get('dag_run') else None,
                    'workflow_type': workflow_execution_data.get('workflow_type', 'python_execution'),
                    'recorded_via': 'hyperbrowser_python_sdk'
                }
            }
            
            store_in_mongodb('workflow_recordings', recording_data, **context)
            logging.info(f"Stored recording metadata for session: {session_id}")
            return True
            
        except Exception as e:
            logging.error(f"Failed to store recording metadata: {e}")
            return False
    
    def cleanup_recording_session(self, session_id: str) -> None:
        """Cleanup recording session"""
        if session_id in self.recording_sessions:
            del self.recording_sessions[session_id]
            logging.info(f"Cleaned up recording session: {session_id}")


class PythonWorkflowExecutionMonitor:
    """Monitor and track Python workflow execution"""
    
    def __init__(self):
        self.execution_stats = {}
        self.start_time = None
    
    def start_monitoring(self, execution_id: str, workflow_name: str) -> None:
        """Start monitoring workflow execution"""
        self.start_time = time.time()
        self.execution_stats[execution_id] = {
            'workflow_name': workflow_name,
            'start_time': self.start_time,
            'steps_completed': 0,
            'status': 'running',
            'errors': []
        }
        logging.info(f"Started monitoring execution: {execution_id}")
    
    def update_step(self, execution_id: str, step_name: str, success: bool = True, error: str = None) -> None:
        """Update execution step"""
        if execution_id in self.execution_stats:
            stats = self.execution_stats[execution_id]
            stats['steps_completed'] += 1
            stats['last_step'] = step_name
            stats['last_step_time'] = time.time()
            
            if not success and error:
                stats['errors'].append({
                    'step': step_name,
                    'error': error,
                    'timestamp': datetime.now()
                })
                stats['status'] = 'error'
                logging.error(f"Step {step_name} failed for {execution_id}: {error}")
            else:
                logging.info(f"Step {step_name} completed for {execution_id}")
    
    def finish_monitoring(self, execution_id: str, success: bool = True) -> Dict[str, Any]:
        """Finish monitoring and return stats"""
        if execution_id in self.execution_stats:
            stats = self.execution_stats[execution_id]
            stats['end_time'] = time.time()
            stats['total_duration'] = stats['end_time'] - stats['start_time']
            stats['status'] = 'completed' if success else 'failed'
            
            result = dict(stats)
            del self.execution_stats[execution_id]
            
            logging.info(f"Finished monitoring {execution_id}: {result['status']} in {result['total_duration']:.2f}s")
            return result
        
        return {'status': 'not_found'}


def get_python_workflow_settings(**context) -> Dict[str, Any]:
    """Get settings specific to Python workflow execution"""
    try:
        client, db = get_mongodb_client()
        
        try:
            settings_doc = db.settings.find_one({'category': 'python_workflow_execution'})
            if settings_doc and 'settings' in settings_doc:
                return settings_doc['settings']
            
            # Return default settings if not found
            default_settings = {
                'max_execution_time': 300,  # 5 minutes
                'video_recording_enabled': True,
                'retry_attempts': 3,
                'workflow_gap_seconds': 15,
                'session_timeout': 600,  # 10 minutes
                'enable_stealth_mode': True,
                'screen_resolution': {'width': 1920, 'height': 1080},
                'execution_approaches': [
                    'extension_api',
                    'chrome_messaging',
                    'storage_injection',
                    'http_api'
                ],
                'recording_options': {
                    'poll_interval': 15,
                    'max_wait_time': 600,
                    'enable_web_recording': True
                }
            }
            
            return default_settings
            
        finally:
            client.close()
            
    except Exception as e:
        logging.error(f"Error getting Python workflow settings: {e}")
        return {}


def validate_python_workflow_environment(**context) -> Dict[str, Any]:
    """Validate that the environment is ready for Python workflow execution"""
    validation_results = {
        'valid': True,
        'errors': [],
        'warnings': [],
        'components': {}
    }
    
    # Check Hyperbrowser API key
    if not HYPERBROWSER_API_KEY:
        validation_results['valid'] = False
        validation_results['errors'].append("HYPERBROWSER_API_KEY environment variable not set")
    else:
        validation_results['components']['hyperbrowser_api_key'] = 'configured'
    
    # Check MongoDB connection
    try:
        client, db = get_mongodb_client()
        # Test connection
        db.command('ping')
        client.close()
        validation_results['components']['mongodb'] = 'connected'
    except Exception as e:
        validation_results['valid'] = False
        validation_results['errors'].append(f"MongoDB connection failed: {str(e)}")
    
    # Check for active profile
    try:
        profile_id = get_hyperbrowser_profile_id(**context)
        if profile_id:
            validation_results['components']['hyperbrowser_profile'] = profile_id
        else:
            validation_results['warnings'].append("No active Hyperbrowser profile found")
    except Exception as e:
        validation_results['warnings'].append(f"Could not check Hyperbrowser profile: {str(e)}")
    
    # Check for extension
    try:
        extension_id = get_hyperbrowser_extension_id(**context)
        if extension_id:
            validation_results['components']['hyperbrowser_extension'] = extension_id
        else:
            validation_results['warnings'].append("No Hyperbrowser extension found - will proceed without extensions")
    except Exception as e:
        validation_results['warnings'].append(f"Could not check Hyperbrowser extension: {str(e)}")
    
    # Test Hyperbrowser SDK
    if HYPERBROWSER_API_KEY:
        try:
            hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
            # Test API connectivity by trying to list profiles or similar
            validation_results['components']['hyperbrowser_sdk'] = 'initialized'
        except Exception as e:
            validation_results['valid'] = False
            validation_results['errors'].append(f"Hyperbrowser SDK initialization failed: {str(e)}")
    
    return validation_results


def cleanup_python_workflow_resources(session_ids: List[str] = None, **context) -> Dict[str, Any]:
    """Cleanup resources from Python workflow execution"""
    cleanup_results = {
        'sessions_cleaned': 0,
        'recordings_processed': 0,
        'errors': []
    }
    
    try:
        if session_ids:
            # Clean up specific sessions
            for session_id in session_ids:
                try:
                    if HYPERBROWSER_API_KEY:
                        session_manager = PythonSessionManager(HYPERBROWSER_API_KEY)
                        if session_manager.stop_session(session_id):
                            cleanup_results['sessions_cleaned'] += 1
                    
                    # Update MongoDB
                    update_in_mongodb(
                        'browser_sessions',
                        {'session_id': session_id},
                        {
                            'is_active': False,
                            'session_status': 'cleanup_completed',
                            'ended_at': datetime.now()
                        },
                        **context
                    )
                    
                except Exception as e:
                    cleanup_results['errors'].append(f"Error cleaning session {session_id}: {str(e)}")
        
        # Clean up old inactive sessions
        try:
            client, db = get_mongodb_client()
            
            # Find old Python sessions that are still marked as active
            cutoff_time = datetime.now() - timedelta(hours=2)
            old_sessions = db.browser_sessions.find({
                'execution_mode': 'python',
                'is_active': True,
                'created_at': {'$lt': cutoff_time}
            })
            
            for session in old_sessions:
                try:
                    session_id = session.get('session_id')
                    if session_id and HYPERBROWSER_API_KEY:
                        session_manager = PythonSessionManager(HYPERBROWSER_API_KEY)
                        session_manager.stop_session(session_id)
                    
                    db.browser_sessions.update_one(
                        {'_id': session['_id']},
                        {
                            '$set': {
                                'is_active': False,
                                'session_status': 'auto_cleanup',
                                'ended_at': datetime.now(),
                                'cleanup_reason': 'automatic_cleanup_old_sessions'
                            }
                        }
                    )
                    cleanup_results['sessions_cleaned'] += 1
                    
                except Exception as e:
                    cleanup_results['errors'].append(f"Error auto-cleaning session: {str(e)}")
            
            client.close()
            
        except Exception as e:
            cleanup_results['errors'].append(f"Error during auto-cleanup: {str(e)}")
    
    except Exception as e:
        cleanup_results['errors'].append(f"General cleanup error: {str(e)}")
    
    logging.info(f"Python workflow cleanup completed: {cleanup_results}")
    return cleanup_results


