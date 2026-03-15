# hyperbrowser_utils.py
"""
Hyperbrowser MongoDB Utilities

Reusable helper functions for managing Hyperbrowser profiles, extensions, and sessions
in MongoDB. This module provides a consistent interface for storing and retrieving
Hyperbrowser configuration data across multiple Airflow DAGs.

Usage:
    from hyperbrowser_utils import (
        get_hyperbrowser_profile_id,
        get_hyperbrowser_extension_id,
        create_workflow_session,
        close_workflow_session
    )
"""

import os
import logging
import time
from datetime import datetime, timedelta
from pymongo import MongoClient
from bson import ObjectId
from hyperbrowser import Hyperbrowser

# Configuration
HYPERBROWSER_API_KEY = os.environ.get('HYPERBROWSER_API_KEY')
MONGODB_URI = os.environ.get('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin')

def get_mongodb_client():
    """Get MongoDB client and database"""
    client = MongoClient(MONGODB_URI)
    db = client.get_default_database()
    return client, db

def store_in_mongodb(collection_name, data, **context):
    """Generic function to store data in MongoDB"""
    try:
        client, db = get_mongodb_client()
        collection = db[collection_name]
        
        # Add timestamp
        data['created_at'] = datetime.now()
        if context and context.get('dag_run'):
            data['dag_run_id'] = context.get('dag_run').run_id
        
        result = collection.insert_one(data)
        client.close()
        
        logging.info(f"Successfully stored data in {collection_name}: {result.inserted_id}")
        return str(result.inserted_id)
        
    except Exception as e:
        logging.error(f"Failed to store data in {collection_name}: {e}")
        if 'client' in locals():
            client.close()
        raise

def get_from_mongodb(collection_name, query, **context):
    """Generic function to retrieve data from MongoDB"""
    try:
        client, db = get_mongodb_client()
        collection = db[collection_name]
        
        result = collection.find_one(query, sort=[('created_at', -1)])
        client.close()
        
        return result
        
    except Exception as e:
        logging.error(f"Failed to retrieve data from {collection_name}: {e}")
        if 'client' in locals():
            client.close()
        return None

def update_in_mongodb(collection_name, query, update_data, **context):
    """Generic function to update data in MongoDB"""
    try:
        client, db = get_mongodb_client()
        collection = db[collection_name]
        
        update_data['updated_at'] = datetime.now()
        
        result = collection.update_one(query, {'$set': update_data}, upsert=True)
        client.close()
        
        logging.info(f"Successfully updated data in {collection_name}: {result.matched_count} matched, {result.modified_count} modified")
        return result.upserted_id or result.matched_count
        
    except Exception as e:
        logging.error(f"Failed to update data in {collection_name}: {e}")
        if 'client' in locals():
            client.close()
        raise

# ==============================================================================
# HYPERBROWSER CONFIGURATION RETRIEVAL FUNCTIONS
# ==============================================================================

def get_hyperbrowser_profile_id(**context):
    """Retrieve active Hyperbrowser profile ID from MongoDB"""
    try:
        # First try settings collection for quick access
        settings = get_from_mongodb('settings', {'category': 'hyperbrowser_configuration'}, **context)
        if settings and settings.get('settings', {}).get('active_profile_id'):
            return settings['settings']['active_profile_id']
        
        # Fallback to chrome_profiles collection
        profile = get_from_mongodb('chrome_profiles', {
            'profile_type': 'hyperbrowser_automa',
            'is_active': True
        }, **context)
        
        if profile:
            return profile['profile_id']
        
        logging.warning("No active Hyperbrowser profile found in MongoDB")
        return None
        
    except Exception as e:
        logging.error(f"Failed to retrieve profile ID: {e}")
        return None

def get_hyperbrowser_extension_id(**context):
    """Retrieve active Hyperbrowser extension ID from MongoDB"""
    try:
        # First try settings collection
        settings = get_from_mongodb('settings', {'category': 'hyperbrowser_configuration'}, **context)
        if settings and settings.get('settings', {}).get('active_extension_id'):
            return settings['settings']['active_extension_id']
        
        # Fallback to extension_instances collection
        extension = get_from_mongodb('extension_instances', {
            'extension_name': 'automa-extension',
            'is_enabled': True,
            'installation_status': 'active'
        }, **context)
        
        if extension:
            return extension['extension_id']
        
        logging.warning("No active Hyperbrowser extension found in MongoDB")
        return None
        
    except Exception as e:
        logging.error(f"Failed to retrieve extension ID: {e}")
        return None

def get_hyperbrowser_session_template(**context):
    """Retrieve session template information from MongoDB"""
    try:
        setup_info = get_from_mongodb('hyperbrowser_setup', {
            'setup_type': 'hyperbrowser_profile_mongodb',
            'automated_setup_success': True
        }, **context)
        
        if setup_info:
            return {
                'profile_id': setup_info['profile_id'],
                'extension_id': setup_info['extension_id'],
                'template_session_id': setup_info['setup_session_id'],
                'browser_type': 'chrome',
                'configuration_ready': setup_info.get('configuration_for_workflows', {}).get('ready_for_automation', False)
            }
        
        logging.warning("No session template found in MongoDB")
        return None
        
    except Exception as e:
        logging.error(f"Failed to retrieve session template: {e}")
        return None

def get_setup_status(**context):
    """Get current setup status from MongoDB"""
    try:
        settings = get_from_mongodb('settings', {'category': 'hyperbrowser_configuration'}, **context)
        if settings:
            return settings.get('settings', {})
        return None
    except Exception as e:
        logging.error(f"Failed to get setup status: {e}")
        return None

def is_hyperbrowser_ready(**context):
    """Check if Hyperbrowser is fully configured and ready for use"""
    try:
        status = get_setup_status(**context)
        if not status:
            return False
        
        return (
            status.get('setup_completed', False) and
            not status.get('manual_configuration_required', True) and
            status.get('ready_for_production', False)
        )
    except Exception as e:
        logging.error(f"Failed to check if Hyperbrowser is ready: {e}")
        return False

# ==============================================================================
# SESSION MANAGEMENT FUNCTIONS
# ==============================================================================

def create_workflow_session(workflow_type=None, **context):
    """Create a new session using stored profile/extension for workflow execution"""
    try:
        profile_id = get_hyperbrowser_profile_id(**context)
        extension_id = get_hyperbrowser_extension_id(**context)
        
        if not profile_id:
            raise ValueError("No active Hyperbrowser profile found in MongoDB")
        
        if not HYPERBROWSER_API_KEY:
            raise ValueError("HYPERBROWSER_API_KEY not configured")
        
        hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
        
        # Create session for workflow execution
        from hyperbrowser.models import CreateSessionParams
        
        session_params = CreateSessionParams(
            screen={'width': 1920, 'height': 1080},
            use_stealth=True,
            profile={'id': profile_id, 'persist_changes': True},
            start_url='chrome://newtab/',
            browser_type='chrome'
        )
        
        if extension_id:
            session_params.extension_ids = [extension_id]
        
        session_response = hb_client.sessions.create(session_params)
        workflow_session_id = session_response.id
        
        # Store workflow session in MongoDB
        session_data = {
            'session_id': workflow_session_id,
            'profile_id': profile_id,
            'extension_id': extension_id,
            'browser_type': 'chrome',
            'session_status': 'active',
            'is_active': True,
            'created_at': datetime.now(),
            'started_at': datetime.now(),
            'ended_at': None,
            'session_purpose': 'workflow_execution',
            'workflow_type': workflow_type,
            'workflow_count': 0,
            'success_count': 0,
            'session_metadata': {
                'created_for': 'automated_workflow',
                'dag_run_id': context.get('dag_run').run_id if context.get('dag_run') else None,
                'parent_profile_id': profile_id,
                'extension_loaded': bool(extension_id),
                'stealth_enabled': True,
                'screen_resolution': '1920x1080'
            }
        }
        
        session_mongodb_id = store_in_mongodb('browser_sessions', session_data, **context)
        
        # Update profile usage
        try:
            client, db = get_mongodb_client()
            db.chrome_profiles.update_one(
                {'profile_id': profile_id},
                {
                    '$inc': {'usage_count': 1},
                    '$set': {'last_used_at': datetime.now()}
                }
            )
            client.close()
        except Exception as e:
            logging.warning(f"Could not update profile usage: {e}")
        
        logging.info(f"Created workflow session: {workflow_session_id}")
        
        return {
            'session_id': workflow_session_id,
            'profile_id': profile_id,
            'extension_id': extension_id,
            'mongodb_id': session_mongodb_id,
            'browser_url': f"https://app.hyperbrowser.ai/sessions/{workflow_session_id}"
        }
        
    except Exception as e:
        logging.error(f"Failed to create workflow session: {e}")
        raise

def close_workflow_session(session_id, **context):
    """Close a workflow session and update MongoDB"""
    try:
        if not session_id:
            logging.warning("No session ID provided for closing")
            return False
        
        # Update session in MongoDB
        end_time = datetime.now()
        
        # Get session to calculate duration
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
        
        update_in_mongodb('browser_sessions',
            {'session_id': session_id},
            update_data,
            **context)
        
        logging.info(f"Closed workflow session: {session_id}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to close workflow session {session_id}: {e}")
        return False

def update_workflow_session_stats(session_id, workflow_count=None, success_count=None, failed_count=None, **context):
    """Update session statistics after workflow execution"""
    try:
        if not session_id:
            logging.warning("No session ID provided for stats update")
            return False
        
        update_data = {'last_activity_at': datetime.now()}
        
        if workflow_count is not None:
            update_data['workflow_count'] = workflow_count
        if success_count is not None:
            update_data['success_count'] = success_count
        if failed_count is not None:
            update_data['failed_count'] = failed_count
            
        # Calculate success rate if we have both counts
        if success_count is not None and workflow_count is not None and workflow_count > 0:
            update_data['success_rate'] = round((success_count / workflow_count) * 100, 2)
            
        update_in_mongodb('browser_sessions',
            {'session_id': session_id},
            update_data,
            **context)
        
        logging.info(f"Updated session stats for {session_id}: workflows={workflow_count}, success={success_count}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to update session stats for {session_id}: {e}")
        return False

def get_active_sessions(**context):
    """Get all active sessions from MongoDB"""
    try:
        client, db = get_mongodb_client()
        sessions = list(db.browser_sessions.find({
            'is_active': True,
            'session_status': 'active'
        }).sort('created_at', -1))
        client.close()
        
        return sessions
        
    except Exception as e:
        logging.error(f"Failed to get active sessions: {e}")
        return []

def cleanup_stale_sessions(max_age_hours=24, **context):
    """Mark old active sessions as stale"""
    try:
        cutoff_time = datetime.now() - timedelta(hours=max_age_hours)
        
        client, db = get_mongodb_client()
        result = db.browser_sessions.update_many(
            {
                'is_active': True,
                'created_at': {'$lt': cutoff_time}
            },
            {
                '$set': {
                    'is_active': False,
                    'session_status': 'stale',
                    'ended_at': datetime.now(),
                    'cleanup_reason': f'auto_cleanup_after_{max_age_hours}h'
                }
            }
        )
        client.close()
        
        logging.info(f"Cleaned up {result.modified_count} stale sessions older than {max_age_hours} hours")
        return result.modified_count
        
    except Exception as e:
        logging.error(f"Failed to cleanup stale sessions: {e}")
        return 0

# ==============================================================================
# SETUP MANAGEMENT FUNCTIONS
# ==============================================================================

def mark_manual_setup_complete(**context):
    """Mark manual setup as completed - call this after manual configuration"""
    try:
        profile_id = get_hyperbrowser_profile_id(**context)
        if not profile_id:
            raise ValueError("No active profile found")
        
        # Update hyperbrowser_setup collection
        update_in_mongodb('hyperbrowser_setup',
            {'profile_id': profile_id, 'setup_type': 'hyperbrowser_profile_mongodb'},
            {
                'manual_steps_completed': True,
                'manual_completion_date': datetime.now(),
                'status': 'fully_configured',
                'ready_for_automation': True,
                'configuration_for_workflows.ready_for_automation': True
            },
            **context)
        
        # Update settings
        update_in_mongodb('settings',
            {'category': 'hyperbrowser_configuration'},
            {
                'settings.manual_configuration_required': False,
                'settings.configuration_status': 'completed',
                'settings.ready_for_production': True,
                'settings.manual_completion_date': datetime.now().isoformat()
            },
            **context)
        
        logging.info(f"Manual setup marked as complete for profile: {profile_id}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to mark manual setup complete: {e}")
        return False

def reset_hyperbrowser_configuration(**context):
    """Reset Hyperbrowser configuration - marks current setup as inactive"""
    try:
        # Mark current profile as inactive
        client, db = get_mongodb_client()
        
        # Deactivate current profile
        db.chrome_profiles.update_many(
            {'profile_type': 'hyperbrowser_automa', 'is_active': True},
            {'$set': {'is_active': False, 'deactivated_at': datetime.now()}}
        )
        
        # Deactivate current extensions
        db.extension_instances.update_many(
            {'extension_name': 'automa-extension', 'is_enabled': True},
            {'$set': {'is_enabled': False, 'deactivated_at': datetime.now()}}
        )
        
        # Close active sessions
        db.browser_sessions.update_many(
            {'is_active': True},
            {'$set': {'is_active': False, 'session_status': 'reset', 'ended_at': datetime.now()}}
        )
        
        # Update settings
        db.settings.update_one(
            {'category': 'hyperbrowser_configuration'},
            {
                '$set': {
                    'settings.configuration_status': 'reset',
                    'settings.ready_for_production': False,
                    'settings.reset_date': datetime.now().isoformat()
                }
            }
        )
        
        client.close()
        
        logging.info("Hyperbrowser configuration has been reset")
        return True
        
    except Exception as e:
        logging.error(f"Failed to reset Hyperbrowser configuration: {e}")
        if 'client' in locals():
            client.close()
        return False

# ==============================================================================
# ANALYTICS AND REPORTING FUNCTIONS
# ==============================================================================

def get_session_analytics(days=7, **context):
    """Get session analytics for the past N days"""
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        client, db = get_mongodb_client()
        
        pipeline = [
            {
                '$match': {
                    'created_at': {'$gte': cutoff_date},
                    'session_purpose': 'workflow_execution'
                }
            },
            {
                '$group': {
                    '_id': {
                        'date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$created_at'}},
                        'workflow_type': '$workflow_type'
                    },
                    'total_sessions': {'$sum': 1},
                    'total_workflows': {'$sum': '$workflow_count'},
                    'total_successes': {'$sum': '$success_count'},
                    'avg_session_duration': {'$avg': '$session_duration'}
                }
            },
            {
                '$sort': {'_id.date': -1, '_id.workflow_type': 1}
            }
        ]
        
        analytics = list(db.browser_sessions.aggregate(pipeline))
        client.close()
        
        return analytics
        
    except Exception as e:
        logging.error(f"Failed to get session analytics: {e}")
        if 'client' in locals():
            client.close()
        return []

def get_profile_usage_stats(**context):
    """Get profile usage statistics"""
    try:
        client, db = get_mongodb_client()
        
        stats = db.chrome_profiles.find_one(
            {'profile_type': 'hyperbrowser_automa', 'is_active': True},
            {
                'profile_id': 1,
                'usage_count': 1,
                'last_used_at': 1,
                'assigned_workflows': 1,
                'created_at': 1
            }
        )
        
        if stats:
            # Get session count for this profile
            session_count = db.browser_sessions.count_documents({
                'profile_id': stats['profile_id']
            })
            stats['total_sessions'] = session_count
        
        client.close()
        return stats
        
    except Exception as e:
        logging.error(f"Failed to get profile usage stats: {e}")
        if 'client' in locals():
            client.close()
        return None

# ==============================================================================
# UTILITY FUNCTIONS FOR WORKFLOW INTEGRATION
# ==============================================================================

def store_workflow_session_link(session_id, workflow_type, workflow_id, **context):
    """Store a link between a session and workflow execution"""
    try:
        link_data = {
            'session_id': session_id,
            'workflow_type': workflow_type,
            'workflow_id': workflow_id,
            'linked_at': datetime.now(),
            'execution_started_at': datetime.now(),
            'execution_status': 'running'
        }
        
        mongodb_id = store_in_mongodb('session_workflow_links', link_data, **context)
        logging.info(f"Created session-workflow link: {mongodb_id}")
        return mongodb_id
        
    except Exception as e:
        logging.error(f"Failed to store session-workflow link: {e}")
        return None

def update_workflow_session_link(link_id, status, **context):
    """Update a session-workflow link with execution results"""
    try:
        update_data = {
            'execution_status': status,
            'execution_completed_at': datetime.now()
        }
        
        client, db = get_mongodb_client()
        result = db.session_workflow_links.update_one(
            {'_id': ObjectId(link_id)},
            {'$set': update_data}
        )
        client.close()
        
        return result.modified_count > 0
        
    except Exception as e:
        logging.error(f"Failed to update session-workflow link: {e}")
        return False

# ==============================================================================
# EXAMPLE USAGE AND INTEGRATION PATTERNS
# ==============================================================================

"""
Example usage in Airflow DAGs:

# Basic setup check
if not is_hyperbrowser_ready():
    raise ValueError("Hyperbrowser not configured. Run setup DAG first.")

# Create session for workflow execution
session_info = create_workflow_session(workflow_type='replies')
session_id = session_info['session_id']

# Execute workflows using session_id...
# Your workflow execution code here

# Update session stats
update_workflow_session_stats(
    session_id, 
    workflow_count=10, 
    success_count=8, 
    failed_count=2
)

# Close session when done
close_workflow_session(session_id)

# Cleanup old sessions (can be scheduled)
cleanup_stale_sessions(max_age_hours=24)

# Get analytics
analytics = get_session_analytics(days=7)
profile_stats = get_profile_usage_stats()
"""