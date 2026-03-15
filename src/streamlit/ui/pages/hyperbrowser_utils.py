"""
Updated Hyperbrowser MongoDB Utilities with Fixed Extension Management

This updated version includes proper extension upload and session creation
based on the official Hyperbrowser documentation and API patterns.

Key improvements:
1. Correct extension upload using file_path parameter
2. Proper session creation with extensionIds parameter
3. Better validation and error handling
4. Extension lifecycle management
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
AUTOMA_EXTENSION_PATH = os.environ.get('AUTOMA_EXTENSION_PATH', '/app/automa-extension/automa-extension.zip')

def get_mongodb_client():
    """Get MongoDB client and database"""
    try:
        client = MongoClient(MONGODB_URI)
        db = client.get_default_database()
        return client, db
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        raise

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
# UPDATED EXTENSION MANAGEMENT FUNCTIONS
# ==============================================================================



def close_workflow_session(session_id, **context):
    """Close a workflow session and update MongoDB"""
    try:
        if not session_id:
            logging.warning("No session ID provided for closing")
            return False
        
        # Close session via API if possible
        if HYPERBROWSER_API_KEY:
            try:
                hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
                hb_client.sessions.close(session_id)
                logging.info(f"Closed session via API: {session_id}")
            except Exception as e:
                logging.warning(f"Could not close session via API: {e}")
        
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
            'session_status': 'completed',
            'session_closure_type': 'api_close'
        }
        
        if session_duration:
            update_data['session_duration'] = session_duration
            update_data['session_duration_hours'] = round(session_duration / 3600, 2)
        
        update_in_mongodb('browser_sessions',
            {'session_id': session_id},
            update_data,
            **context)
        
        logging.info(f"Closed workflow session: {session_id}")
        return True
        
    except Exception as e:
        logging.error(f"Failed to close workflow session {session_id}: {e}")
        return False

# ==============================================================================
# HYPERBROWSER CONFIGURATION RETRIEVAL FUNCTIONS (UPDATED)
# ==============================================================================

def get_hyperbrowser_profile_id(**context):
    """Retrieve active Hyperbrowser profile ID from MongoDB"""
    try:
        # Check accounts collection for active profile
        profile = get_from_mongodb('accounts', {
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




def is_hyperbrowser_ready(**context):
    """Check if Hyperbrowser is fully configured and ready for use"""
    try:
        # Check if we have an active profile
        profile_id = get_hyperbrowser_profile_id(**context)
        if not profile_id:
            logging.warning("No active profile found")
            return False
        
        # Check if we have an API key
        if not HYPERBROWSER_API_KEY:
            logging.warning("HYPERBROWSER_API_KEY not configured")
            return False
        
        # Check if we have an active extension (optional but recommended)
        extension_id = get_hyperbrowser_extension_id(**context)
        if not extension_id:
            logging.warning("No active extension found - workflows may not work properly")
            # Don't return False here as extension might be optional
        
        # All checks passed
        logging.info(f"Hyperbrowser is ready - Profile: {profile_id}, Extension: {extension_id or 'None'}")
        return True
        
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
            start_url='chrome://popup/',
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
            db.accounts.update_one(
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
        db.accounts.update_many(
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
        
        stats = db.accounts.find_one(
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
"""
Updated methods for hyperbrowser_utils.py that need to be modified to support
the new extension-to-account linking functionality.

These methods should replace the corresponding methods in the existing hyperbrowser_utils.py file.
"""

def upload_automa_extension(**context):
    """
    UPDATED: Upload Automa extension using the correct API approach with account linking support
    """
    try:
        if not HYPERBROWSER_API_KEY:
            raise ValueError("HYPERBROWSER_API_KEY is required")
        
        if not os.path.exists(AUTOMA_EXTENSION_PATH):
            raise FileNotFoundError(f"Extension file not found: {AUTOMA_EXTENSION_PATH}")
        
        # Check if extension already exists and is NOT linked to any account
        existing_extension = get_from_mongodb('extension_instances', {
            'extension_name': 'automa-extension',
            'is_enabled': True,
            'installation_status': 'active',
            'postgres_account_id': None  # Only reuse if not linked to specific account
        }, **context)
        
        if existing_extension:
            extension_id = existing_extension['extension_id']
            logging.info(f"Using existing unlinked extension: {extension_id}")
            return {
                'extension_id': extension_id,
                'extension_uploaded': False,
                'action': 'reused_existing_unlinked',
                'mongodb_id': str(existing_extension['_id'])
            }
        
        # Upload new extension
        hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
        
        # FIXED: Use correct parameters as per documentation
        extension_response = hb_client.extensions.create(
            name="automa-extension",
            file_path=AUTOMA_EXTENSION_PATH
        )
        
        extension_id = extension_response.id
        logging.info(f"Successfully uploaded extension with ID: {extension_id}")
        
        # Store in MongoDB with account linking fields
        extension_data = {
            'extension_id': extension_id,
            'extension_name': 'automa-extension',
            'extension_version': '2.0.0',
            'manifest_version': 3,
            'installation_status': 'active',
            'is_enabled': True,
            'installation_path': AUTOMA_EXTENSION_PATH,
            'installed_at': datetime.now(),
            'last_updated_at': datetime.now(),
            'usage_count': 0,
            'associated_profiles': [],
            'file_size': os.path.getsize(AUTOMA_EXTENSION_PATH),
            
            # ADDED: Account linking fields (similar to profiles)
            'postgres_account_id': None,  # Will be set when linked to account
            'account_username': None,     # Will be set when linked to account  
            'linked_to_postgres': False,  # Will be set to True when linked
            
            'extension_metadata': {
                'permissions': ['activeTab', 'storage', 'scripting'],
                'content_scripts': True,
                'background_service_worker': True,
                'uploaded_via': 'fixed_hyperbrowser_utils',
                'hyperbrowser_response': {
                    'id': extension_id,
                    'upload_timestamp': datetime.now().isoformat()
                },
                'supports_account_linking': True  # Flag to indicate this extension supports account linking
            }
        }
        
        mongodb_id = store_in_mongodb('extension_instances', extension_data, **context)
        
        return {
            'extension_id': extension_id,
            'extension_uploaded': True,
            'action': 'uploaded_new',
            'mongodb_id': mongodb_id,
            'supports_account_linking': True
        }
        
    except Exception as e:
        logging.error(f"Error uploading Automa extension: {str(e)}")
        raise

def create_workflow_session(workflow_type=None, postgres_account_id=None, account_username=None, **context):
    """
    UPDATED: Create a new session with proper extension handling and account-specific extension lookup
    """
    try:
        profile_id = get_hyperbrowser_profile_id(**context)
        if not profile_id:
            raise ValueError("No active Hyperbrowser profile found in MongoDB")
        
        if not HYPERBROWSER_API_KEY:
            raise ValueError("HYPERBROWSER_API_KEY not configured")
        
        extension_id = None
        
        # NEW: Try to get account-specific extension first
        if postgres_account_id:
            logging.info(f"Looking for account-specific extension for account {postgres_account_id}")
            account_extension = get_from_mongodb('extension_instances', {
                'postgres_account_id': postgres_account_id,
                'extension_name': 'automa-extension',
                'is_enabled': True,
                'installation_status': 'active'
            }, **context)
            
            if account_extension:
                extension_id = account_extension['extension_id']
                logging.info(f"Found account-specific extension: {extension_id}")
            else:
                logging.info(f"No account-specific extension found for {postgres_account_id}")
        
        # Fallback: Get or create general extension
        if not extension_id:
            logging.info("Getting or creating general extension...")
            extension_result = upload_automa_extension(**context)
            extension_id = extension_result['extension_id']
            
            # If we have account info, try to link this extension to the account
            if postgres_account_id and account_username:
                try:
                    # Import the linking function
                    from .hyperbrowser_extensions import link_extension_to_postgres_account
                    
                    link_success = link_extension_to_postgres_account(
                        extension_id=extension_id,
                        postgres_account_id=postgres_account_id,
                        username=account_username,
                        **context
                    )
                    
                    if link_success:
                        logging.info(f"Successfully linked extension {extension_id} to account {postgres_account_id}")
                    else:
                        logging.warning(f"Could not link extension to account {postgres_account_id}")
                        
                except Exception as link_error:
                    logging.warning(f"Extension linking failed: {link_error}")
        
        hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
        
        # FIXED: Create session with proper extension handling
        from hyperbrowser.models import CreateSessionParams
        
        session_params = CreateSessionParams(
            screen={'width': 1920, 'height': 1080},
            use_stealth=True,
            profile={'id': profile_id, 'persist_changes': True},
            start_url='https://twitter.com',
            browser_type='chrome',
            timeout_minutes=120
        )
        
        # FIXED: Use the correct parameter name for extensions
        if extension_id:
            session_params.extension_ids = [extension_id]
            logging.info(f"Session will include extension: {extension_id}")
        else:
            logging.warning("No extension available, creating session without extensions")
        
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
            'extension_loaded': bool(extension_id),
            
            # ADDED: Account context for better tracking
            'postgres_account_id': postgres_account_id,
            'account_username': account_username,
            'account_specific_session': bool(postgres_account_id),
            
            'session_metadata': {
                'created_for': 'automated_workflow_with_extension',
                'dag_run_id': context.get('dag_run').run_id if context.get('dag_run') else None,
                'parent_profile_id': profile_id,
                'extension_loaded': bool(extension_id),
                'extension_ids_used': [extension_id] if extension_id else [],
                'stealth_enabled': True,
                'screen_resolution': '1920x1080',
                'created_via': 'fixed_workflow_session_with_account_context',
                'account_specific': bool(postgres_account_id)
            }
        }
        
        session_mongodb_id = store_in_mongodb('browser_sessions', session_data, **context)
        
        # Update extension usage
        if extension_id:
            try:
                client, db = get_mongodb_client()
                db.extension_instances.update_one(
                    {'extension_id': extension_id},
                    {
                        '$addToSet': {'associated_profiles': profile_id},
                        '$inc': {'usage_count': 1},
                        '$set': {'last_used_at': datetime.now()}
                    }
                )
                client.close()
                logging.info(f"Updated extension {extension_id} usage statistics")
            except Exception as e:
                logging.warning(f"Could not update extension usage: {e}")
        
        # Update profile usage
        try:
            client, db = get_mongodb_client()
            db.accounts.update_one(
                {'profile_id': profile_id},
                {
                    '$inc': {'usage_count': 1},
                    '$set': {'last_used_at': datetime.now()}
                }
            )
            client.close()
        except Exception as e:
            logging.warning(f"Could not update profile usage: {e}")
        
        logging.info(f"Created workflow session: {workflow_session_id} with extension: {extension_id}")
        
        return {
            'session_id': workflow_session_id,
            'profile_id': profile_id,
            'extension_id': extension_id,
            'mongodb_id': session_mongodb_id,
            'browser_url': f"https://app.hyperbrowser.ai/sessions/{workflow_session_id}",
            'extension_loaded': bool(extension_id),
            'account_specific': bool(postgres_account_id),
            'postgres_account_id': postgres_account_id
        }
        
    except Exception as e:
        logging.error(f"Failed to create workflow session: {e}")
        raise

def get_account_profiles_compatible_data(**context):
    """
    UPDATED: Get profile data in format compatible with getAllAccountProfiles() function
    with proper extension linking support
    """
    try:
        client, db = get_mongodb_client()
        
        # Get accounts with profiles (matching getAllAccountProfiles logic)
        accounts = list(db.accounts.find({
            'postgres_account_id': {'$exists': True},
            'profile_id': {'$exists': True, '$ne': None}
        }))
        
        account_profiles = []
        for account in accounts:
            try:
                # The profile data is in the same document now
                profile_data = account  # Since we store everything in accounts collection
                
                # UPDATED: Get extension details for this specific account
                extension = db.extension_instances.find_one({
                    'postgres_account_id': account.get('postgres_account_id'),
                    'is_enabled': True,
                    'installation_status': 'active'
                })
                
                # FALLBACK: If no account-specific extension, look for general extension
                if not extension:
                    extension = db.extension_instances.find_one({
                        'extension_name': 'automa-extension',
                        'is_enabled': True,
                        'installation_status': 'active',
                        'postgres_account_id': None  # General extension not linked to any account
                    })
                
                account_profile = {
                    'accountId': account.get('postgres_account_id'),
                    'username': account.get('username'),
                    'profileId': account.get('profile_id'),
                    'extensionId': extension.get('extension_id') if extension else None,
                    'lastWorkflowSync': account.get('last_workflow_sync'),
                    'totalRepliesProcessed': account.get('total_replies_processed', 0),
                    'totalMessagesProcessed': account.get('total_messages_processed', 0),
                    'totalRetweetsProcessed': account.get('total_retweets_processed', 0),
                    'profileValidated': True,  # Profile exists in our collection
                    'extensionAvailable': bool(extension),
                    'extensionAccountSpecific': bool(extension and extension.get('postgres_account_id')),  # NEW field
                    'profileData': profile_data,
                    'extensionData': extension,
                    'mongoAccountId': account.get('_id')
                }
                account_profiles.append(account_profile)
                
            except Exception as e:
                logging.error(f"Error processing account {account.get('username', 'unknown')}: {e}")
                continue
        
        client.close()
        logging.info(f"Successfully loaded {len(account_profiles)} account profiles with extension info")
        return account_profiles
        
    except Exception as e:
        logging.error(f"Failed to get account profiles compatible data: {e}")
        return []

def cleanup_unlinked_extensions(**context):
    """
    NEW METHOD: Clean up extensions that are not linked to any account after a certain period
    This helps prevent accumulation of unused extensions
    """
    try:
        cutoff_time = datetime.now() - timedelta(days=7)  # Extensions unused for 7 days
        
        client, db = get_mongodb_client()
        
        # Find extensions that are not linked to any account and haven't been used recently
        stale_extensions = db.extension_instances.find({
            'postgres_account_id': None,
            'linked_to_postgres': False,
            'is_enabled': True,
            'last_used_at': {'$lt': cutoff_time}
        })
        
        cleanup_count = 0
        for extension in stale_extensions:
            try:
                # Deactivate the extension
                db.extension_instances.update_one(
                    {'_id': extension['_id']},
                    {
                        '$set': {
                            'is_enabled': False,
                            'installation_status': 'cleaned_up',
                            'cleaned_up_at': datetime.now(),
                            'cleanup_reason': 'unused_unlinked_extension'
                        }
                    }
                )
                cleanup_count += 1
                logging.info(f"Cleaned up unlinked extension: {extension['extension_id']}")
                
            except Exception as e:
                logging.warning(f"Could not clean up extension {extension.get('extension_id')}: {e}")
        
        client.close()
        
        logging.info(f"Cleaned up {cleanup_count} unlinked extensions")
        return cleanup_count
        
    except Exception as e:
        logging.error(f"Failed to cleanup unlinked extensions: {e}")
        return 0

def get_extension_usage_analytics(days=30, **context):
    """
    NEW METHOD: Get analytics on extension usage by account
    """
    try:
        cutoff_date = datetime.now() - timedelta(days=days)
        
        client, db = get_mongodb_client()
        
        pipeline = [
            {
                '$match': {
                    'last_used_at': {'$gte': cutoff_date},
                    'is_enabled': True
                }
            },
            {
                '$group': {
                    '_id': {
                        'extension_id': '$extension_id',
                        'account_id': '$postgres_account_id',
                        'account_username': '$account_username'
                    },
                    'usage_count': {'$first': '$usage_count'},
                    'last_used': {'$first': '$last_used_at'},
                    'linked_to_account': {'$first': '$linked_to_postgres'}
                }
            },
            {
                '$sort': {'usage_count': -1}
            }
        ]
        
        analytics = list(db.extension_instances.aggregate(pipeline))
        client.close()
        
        return analytics
        
    except Exception as e:
        logging.error(f"Failed to get extension usage analytics: {e}")
        return []
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