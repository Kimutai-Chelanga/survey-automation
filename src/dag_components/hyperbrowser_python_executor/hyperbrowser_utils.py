# hyperbrowser_utils.py
"""
Base Hyperbrowser Utilities for Airflow DAGs

This module provides core utilities for managing Hyperbrowser profiles, extensions,
MongoDB operations, and session management for workflow automation.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Union
from pymongo import MongoClient
from bson import ObjectId
import uuid

# Configuration
MONGODB_CONNECTION_STRING = os.environ.get('MONGODB_CONNECTION_STRING', 'mongodb://localhost:27017')
MONGODB_DATABASE_NAME = os.environ.get('MONGODB_DATABASE_NAME', 'workflow_automation')
HYPERBROWSER_API_KEY = os.environ.get('HYPERBROWSER_API_KEY')


def get_mongodb_client():
    """Get MongoDB client and database"""
    try:
        client = MongoClient(MONGODB_CONNECTION_STRING)
        db = client[MONGODB_DATABASE_NAME]
        return client, db
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        raise


def store_in_mongodb(collection_name: str, data: Dict[str, Any], **context) -> str:
    """Store data in MongoDB and return the document ID"""
    client, db = get_mongodb_client()
    
    try:
        # Add metadata
        data['created_at'] = datetime.now()
        if context.get('dag_run'):
            data['dag_run_id'] = context['dag_run'].run_id
        
        result = db[collection_name].insert_one(data)
        logging.info(f"Stored document in {collection_name}: {result.inserted_id}")
        return str(result.inserted_id)
        
    except Exception as e:
        logging.error(f"Error storing data in {collection_name}: {e}")
        raise
    finally:
        client.close()


def get_from_mongodb(collection_name: str, query: Dict[str, Any], **context) -> Optional[Dict[str, Any]]:
    """Get a document from MongoDB"""
    client, db = get_mongodb_client()
    
    try:
        result = db[collection_name].find_one(query)
        return result
    except Exception as e:
        logging.error(f"Error getting data from {collection_name}: {e}")
        return None
    finally:
        client.close()


def update_in_mongodb(
    collection_name: str, 
    query: Dict[str, Any], 
    update_data: Dict[str, Any], 
    **context
) -> bool:
    """Update a document in MongoDB"""
    client, db = get_mongodb_client()
    
    try:
        # Add update metadata
        update_data['updated_at'] = datetime.now()
        
        result = db[collection_name].update_one(query, {'$set': update_data})
        success = result.modified_count > 0
        
        if success:
            logging.info(f"Updated document in {collection_name}")
        else:
            logging.warning(f"No document updated in {collection_name} with query: {query}")
        
        return success
        
    except Exception as e:
        logging.error(f"Error updating data in {collection_name}: {e}")
        return False
    finally:
        client.close()


def get_hyperbrowser_profile_id(**context) -> Optional[str]:
    """Get the active Hyperbrowser profile ID"""
    try:
        # First check for active session with a profile
        active_session = get_from_mongodb(
            'browser_sessions',
            {'is_active': True, 'session_status': 'active'},
            **context
        )
        
        if active_session and active_session.get('profile_id'):
            logging.info(f"Found active profile from session: {active_session['profile_id']}")
            return active_session['profile_id']
        
        # Otherwise get the most recently used profile
        client, db = get_mongodb_client()
        
        try:
            # Look for profiles sorted by last_used_at or created_at
            profile = db.chrome_profiles.find_one(
                {'status': {'$ne': 'deleted'}},
                sort=[('last_used_at', -1), ('created_at', -1)]
            )
            
            if profile and profile.get('profile_id'):
                logging.info(f"Found most recent profile: {profile['profile_id']}")
                return profile['profile_id']
            
            logging.warning("No Hyperbrowser profile found")
            return None
            
        finally:
            client.close()
            
    except Exception as e:
        logging.error(f"Error getting Hyperbrowser profile ID: {e}")
        return None


def get_hyperbrowser_extension_id(**context) -> Optional[str]:
    """Get the active Hyperbrowser extension ID"""
    try:
        # Check for active extensions
        client, db = get_mongodb_client()
        
        try:
            extension = db.browser_extensions.find_one(
                {'status': 'active', 'extension_type': {'$in': ['automa', 'workflow']}},
                sort=[('last_used_at', -1), ('created_at', -1)]
            )
            
            if extension and extension.get('extension_id'):
                logging.info(f"Found active extension: {extension['extension_id']}")
                return extension['extension_id']
            
            # Try to get from profile settings
            profile_id = get_hyperbrowser_profile_id(**context)
            if profile_id:
                profile = db.chrome_profiles.find_one({'profile_id': profile_id})
                if profile and profile.get('extensions'):
                    extensions = profile['extensions']
                    if isinstance(extensions, list) and extensions:
                        return extensions[0]  # Return first extension
                    elif isinstance(extensions, dict) and extensions.get('automa'):
                        return extensions['automa']
            
            logging.info("No Hyperbrowser extension found - will proceed without extensions")
            return None
            
        finally:
            client.close()
            
    except Exception as e:
        logging.error(f"Error getting Hyperbrowser extension ID: {e}")
        return None


def create_workflow_session(workflow_type: str = 'general_execution', **context) -> Dict[str, Any]:
    """Create a new workflow session in MongoDB"""
    try:
        profile_id = get_hyperbrowser_profile_id(**context)
        extension_id = get_hyperbrowser_extension_id(**context)
        
        if not profile_id:
            raise ValueError("No active Hyperbrowser profile found")
        
        session_id = f"session_{uuid.uuid4().hex[:8]}_{int(datetime.now().timestamp())}"
        
        session_data = {
            'session_id': session_id,
            'profile_id': profile_id,
            'extension_id': extension_id,
            'browser_type': 'chrome',
            'session_status': 'active',
            'is_active': True,
            'created_at': datetime.now(),
            'started_at': datetime.now(),
            'ended_at': None,
            'session_purpose': workflow_type,
            'workflow_type': workflow_type,
            'workflow_count': 0,
            'success_count': 0,
            'session_metadata': {
                'created_for': 'automated_workflow',
                'dag_run_id': context.get('dag_run').run_id if context.get('dag_run') else None,
                'parent_profile_id': profile_id,
                'extension_loaded': bool(extension_id)
            }
        }
        
        mongodb_id = store_in_mongodb('browser_sessions', session_data, **context)
        
        session_info = {
            'session_id': session_id,
            'profile_id': profile_id,
            'extension_id': extension_id,
            'mongodb_id': mongodb_id,
            'browser_url': f"https://app.hyperbrowser.ai/sessions/{session_id}"
        }
        
        logging.info(f"Created workflow session: {session_id}")
        return session_info
        
    except Exception as e:
        logging.error(f"Failed to create workflow session: {e}")
        raise


def close_workflow_session(session_id: str, **context) -> bool:
    """Close a workflow session"""
    try:
        if not session_id:
            logging.warning("No session ID provided for closing")
            return False
        
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
            'session_status': 'completed'
        }
        
        if session_duration:
            update_data['session_duration'] = session_duration
        
        success = update_in_mongodb(
            'browser_sessions',
            {'session_id': session_id},
            update_data,
            **context
        )
        
        if success:
            logging.info(f"Closed workflow session: {session_id}")
        else:
            logging.warning(f"Could not update session {session_id} in database")
        
        return success
        
    except Exception as e:
        logging.error(f"Failed to close workflow session {session_id}: {e}")
        return False


def update_workflow_session_stats(
    session_id: str,
    workflow_count: int = 0,
    success_count: int = 0,
    failed_count: int = 0,
    **context
) -> bool:
    """Update workflow session statistics"""
    try:
        update_data = {
            'workflow_count': workflow_count,
            'success_count': success_count,
            'failed_count': failed_count,
            'last_activity_at': datetime.now()
        }
        
        success = update_in_mongodb(
            'browser_sessions',
            {'session_id': session_id},
            update_data,
            **context
        )
        
        if success:
            logging.info(f"Updated session stats for {session_id}: {workflow_count} total, {success_count} success")
        
        return success
        
    except Exception as e:
        logging.error(f"Failed to update session stats for {session_id}: {e}")
        return False


def get_active_workflow_sessions(**context) -> List[Dict[str, Any]]:
    """Get all active workflow sessions"""
    client, db = get_mongodb_client()
    
    try:
        sessions = list(db.browser_sessions.find({
            'is_active': True,
            'session_status': 'active'
        }).sort('created_at', -1))
        
        logging.info(f"Found {len(sessions)} active workflow sessions")
        return sessions
        
    except Exception as e:
        logging.error(f"Error getting active workflow sessions: {e}")
        return []
    finally:
        client.close()


def cleanup_old_sessions(max_age_hours: int = 24, **context) -> int:
    """Clean up old inactive sessions"""
    client, db = get_mongodb_client()
    
    try:
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        # Update old sessions to inactive
        result = db.browser_sessions.update_many(
            {
                'is_active': True,
                'created_at': {'$lt': cutoff_time}
            },
            {
                '$set': {
                    'is_active': False,
                    'session_status': 'expired',
                    'ended_at': datetime.now(),
                    'cleanup_reason': f'Auto-cleanup after {max_age_hours} hours'
                }
            }
        )
        
        cleaned_count = result.modified_count
        logging.info(f"Cleaned up {cleaned_count} old sessions")
        return cleaned_count
        
    except Exception as e:
        logging.error(f"Error cleaning up old sessions: {e}")
        return 0
    finally:
        client.close()


def get_system_settings(category: str = 'system') -> Dict[str, Any]:
    """Get system settings from MongoDB"""
    client, db = get_mongodb_client()
    
    try:
        settings_doc = db.settings.find_one({'category': category})
        if settings_doc and 'settings' in settings_doc:
            return settings_doc['settings']
        return {}
        
    except Exception as e:
        logging.error(f"Error getting system settings: {e}")
        return {}
    finally:
        client.close()


def ensure_profile_exists(**context) -> str:
    """Ensure a Hyperbrowser profile exists, create one if needed"""
    try:
        # Check for existing profile
        profile_id = get_hyperbrowser_profile_id(**context)
        if profile_id:
            return profile_id
        
        # Create a new profile entry in MongoDB
        profile_data = {
            'profile_id': f"profile_{uuid.uuid4().hex[:8]}",
            'profile_name': f"Auto Profile {datetime.now().strftime('%Y%m%d_%H%M%S')}",
            'created_at': datetime.now(),
            'status': 'active',
            'usage_count': 0,
            'last_used_at': datetime.now(),
            'profile_metadata': {
                'created_by': 'airflow_dag',
                'auto_created': True,
                'dag_run_id': context.get('dag_run').run_id if context.get('dag_run') else None
            }
        }
        
        store_in_mongodb('chrome_profiles', profile_data, **context)
        
        logging.info(f"Created new profile: {profile_data['profile_id']}")
        return profile_data['profile_id']
        
    except Exception as e:
        logging.error(f"Failed to ensure profile exists: {e}")
        raise


def convert_objectids_to_strings(obj):
    """Recursively convert ObjectIds to strings in nested dictionaries/lists"""
    if isinstance(obj, dict):
        return {key: convert_objectids_to_strings(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_objectids_to_strings(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj


def log_execution_event(event_type: str, message: str, details: Dict[str, Any] = None, **context):
    """Log execution events to MongoDB for debugging and monitoring"""
    try:
        event_data = {
            'event_type': event_type,
            'message': message,
            'details': details or {},
            'timestamp': datetime.now(),
            'dag_run_id': context.get('dag_run').run_id if context.get('dag_run') else None,
            'task_id': context.get('task_instance').task_id if context.get('task_instance') else None
        }
        
        store_in_mongodb('execution_events', event_data, **context)
        logging.info(f"Logged execution event: {event_type} - {message}")
        
    except Exception as e:
        logging.warning(f"Failed to log execution event: {e}")


# Utility classes for better organization
class HyperbrowserProfileManager:
    """Manager class for Hyperbrowser profiles"""
    
    @staticmethod
    def get_active_profile(**context) -> Optional[str]:
        return get_hyperbrowser_profile_id(**context)
    
    @staticmethod
    def ensure_profile(**context) -> str:
        return ensure_profile_exists(**context)
    
    @staticmethod
    def update_profile_usage(profile_id: str, **context) -> bool:
        return update_in_mongodb(
            'chrome_profiles',
            {'profile_id': profile_id},
            {
                'last_used_at': datetime.now(),
                '$inc': {'usage_count': 1}
            },
            **context
        )


class WorkflowSessionManager:
    """Manager class for workflow sessions"""
    
    @staticmethod
    def create_session(workflow_type: str = 'general_execution', **context) -> Dict[str, Any]:
        return create_workflow_session(workflow_type, **context)
    
    @staticmethod
    def close_session(session_id: str, **context) -> bool:
        return close_workflow_session(session_id, **context)
    
    @staticmethod
    def update_stats(session_id: str, **kwargs) -> bool:
        return update_workflow_session_stats(session_id, **kwargs)
    
    @staticmethod
    def get_active_sessions(**context) -> List[Dict[str, Any]]:
        return get_active_workflow_sessions(**context)
    
    @staticmethod
    def cleanup_old(max_age_hours: int = 24, **context) -> int:
        return cleanup_old_sessions(max_age_hours, **context)