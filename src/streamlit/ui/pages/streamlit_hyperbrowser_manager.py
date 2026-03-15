"""
Hyperbrowser Profile Management Functions

This module contains the core logic for managing Hyperbrowser profiles,
extensions, and sessions. These functions can be reused across multiple
scripts that need to interact with Hyperbrowser profiles.

Usage:
    from hyperbrowser_profile_manager import (
        check_existing_profile,
        create_hyperbrowser_profile,
        setup_profile_with_automa
    )
"""

import os
import logging
import time
from datetime import datetime
from hyperbrowser import Hyperbrowser

# Import the base utilities
from .hyperbrowser_utils import (
    get_mongodb_client,
    store_in_mongodb,
    get_from_mongodb,
    update_in_mongodb
)

# Configuration
HYPERBROWSER_API_KEY = os.environ.get('HYPERBROWSER_API_KEY')

def create_hyperbrowser_profile(profile_type='hyperbrowser_automa', profile_name=None, skip_if_exists=True, previous_check_result=None):
    """
    Create a new Hyperbrowser profile and store in MongoDB - UPDATED for correct collection
    
    Args:
        profile_type (str): Type of profile to create
        profile_name (str): Custom profile name (optional)
        skip_if_exists (bool): Whether to skip creation if profile exists
        previous_check_result (dict): Result from a previous check_existing_profile call
    
    Returns:
        dict: Profile creation results
    """
    
    # Check if we should skip creation based on existing profile
    skip_creation = False
    existing_profile_id = None
    existing_mongodb_id = None
    
    if skip_if_exists:
        if previous_check_result:
            # Use the result from a previous check
            skip_creation = previous_check_result.get('skip_creation', False)
            existing_profile_id = previous_check_result.get('profile_id')
            existing_mongodb_id = previous_check_result.get('mongodb_id')
        else:
            # Check directly if no previous result available
            check_result = check_existing_profile(profile_type=profile_type)
            skip_creation = check_result.get('skip_creation', False)
            existing_profile_id = check_result.get('profile_id')
            existing_mongodb_id = check_result.get('mongodb_id')
    
    if skip_creation and existing_profile_id:
        logging.info(f"Skipping profile creation - using existing profile: {existing_profile_id}")
        
        return {
            'profile_id': existing_profile_id,
            'profile_created': False,
            'action': 'reused_existing',
            'mongodb_id': existing_mongodb_id
        }
    
    logging.info(f"Creating new Hyperbrowser profile of type: {profile_type}")
    
    if not HYPERBROWSER_API_KEY:
        error_msg = "HYPERBROWSER_API_KEY is required to create profiles"
        logging.error(error_msg)
        raise ValueError(error_msg)
    
    try:
        hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
        
        # Create new profile
        profile = hb_client.profiles.create()
        profile_id = profile.id
        
        logging.info(f"Profile created successfully with ID: {profile_id}")
        
        # Generate profile name if not provided
        if not profile_name:
            profile_name = f'Hyperbrowser {profile_type.replace("_", " ").title()} Profile - {datetime.now().strftime("%Y%m%d_%H%M%S")}'
        
        # UPDATED: Store profile information in 'accounts' collection
        profile_data = {
            'profile_id': profile_id,
            'profile_name': profile_name,
            'profile_path': f'/hyperbrowser/profiles/{profile_id}',
            'profile_type': profile_type,
            'is_default': profile_type == 'hyperbrowser_automa',
            'is_active': True,
            'created_at': datetime.now(),
            'last_used_at': datetime.now(),
            'usage_count': 0,
            'assigned_workflows': ['replies', 'messages', 'retweets'],
            'profile_settings': {
                'disable_images': False,
                'disable_javascript': False,
                'user_agent': None,
                'window_size': '1920,1080',
                'created_via': 'hyperbrowser_profile_manager',
                'hyperbrowser_metadata': {
                    'profile_id': profile_id,
                    'creation_timestamp': datetime.now().isoformat()
                }
            },
            # Add fields for linking to PostgreSQL later
            'postgres_account_id': None,  # Will be set when linked
            'username': None,             # Will be set when linked
            'linked_to_postgres': False
        }
        
        # Store in accounts collection (UPDATED: not chrome_profiles)
        mongodb_id = store_in_mongodb('accounts', profile_data)
        
        return {
            'profile_id': profile_id,
            'profile_created': True,
            'action': 'created_new',
            'mongodb_id': mongodb_id,
            'stored_in_mongodb': True,
            'profile_name': profile_name
        }
        
    except Exception as e:
        error_msg = f"Failed to create profile: {e}"
        logging.error(error_msg)
        raise Exception(error_msg)
    
def check_existing_profile(profile_type='hyperbrowser_automa'):
    """
    Check if a profile already exists in MongoDB - UPDATED for correct collection
    
    Args:
        profile_type (str): Type of profile to check for
    
    Returns:
        dict: Profile information and action to take
    """
    try:
        # UPDATED: Check in 'accounts' collection instead of separate collections
        existing_profile = get_from_mongodb('accounts', {
            'is_active': True,
            'profile_type': profile_type
        })
        
        if existing_profile:
            profile_id = existing_profile['profile_id']
            logging.info(f"Found existing profile ID in MongoDB accounts collection: {profile_id}")
            
            # Verify the profile still exists in Hyperbrowser
            if not HYPERBROWSER_API_KEY:
                logging.warning("HYPERBROWSER_API_KEY not available, cannot verify profile existence")
                return {
                    'profile_id': profile_id,
                    'profile_exists': True,
                    'skip_creation': True,
                    'action': 'verify_existing_no_api_check',
                    'mongodb_id': str(existing_profile['_id']),
                    'warning': 'Could not verify profile exists in Hyperbrowser'
                }
            
            hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
            try:
                profile = hb_client.profiles.get(profile_id)
                logging.info(f"Profile {profile_id} exists and is accessible in Hyperbrowser")
                
                return {
                    'profile_id': profile_id,
                    'profile_exists': True,
                    'skip_creation': True,
                    'action': 'verify_existing',
                    'mongodb_id': str(existing_profile['_id'])
                }
            except Exception as e:
                logging.warning(f"Stored profile {profile_id} no longer exists in Hyperbrowser: {e}")
                # Mark as inactive in MongoDB instead of deleting
                update_in_mongodb('accounts', 
                    {'_id': existing_profile['_id']}, 
                    {'is_active': False, 'status': 'not_found_in_hyperbrowser'})
                
    except Exception as e:
        logging.info(f"No existing profile found in MongoDB: {e}")
    
    # No valid existing profile found
    return {
        'profile_exists': False,
        'skip_creation': False,
        'action': 'create_new'
    }



# The rest of the file remains the same...
def setup_profile_with_automa(profile_id=None, extension_id=None, session_purpose='setup', previous_profile_result=None, previous_extension_result=None):
    """
    Setup the profile with Automa extension and store session in MongoDB
    
    Args:
        profile_id (str): Profile ID to use (optional, will use previous result if not provided)
        extension_id (str): Extension ID to use (optional, will use previous result if not provided)
        session_purpose (str): Purpose of the session ('setup', 'workflow_execution', etc.)
        previous_profile_result (dict): Result from a previous create_hyperbrowser_profile call
        previous_extension_result (dict): Result from a previous upload_automa_extension call
    
    Returns:
        dict: Session setup results
    """
    
    # Get profile_id from previous result if not provided
    if not profile_id and previous_profile_result:
        profile_id = previous_profile_result.get('profile_id')
    
    # Get extension_id from previous result if not provided
    if not extension_id and previous_extension_result:
        extension_id = previous_extension_result.get('extension_id')
    
    if not profile_id:
        raise ValueError("No profile ID provided or found from previous operation")
    
    if not HYPERBROWSER_API_KEY:
        raise ValueError("HYPERBROWSER_API_KEY is required to create sessions")
    
    # Handle case where extension ID is None
    if not extension_id:
        logging.info("No extension ID available - proceeding without extensions")
        extension_ids = []
    else:
        extension_ids = [extension_id]
    
    logging.info(f"Setting up profile {profile_id} with extensions: {extension_ids}")
    
    try:
        from hyperbrowser.models import CreateSessionParams
        
        hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
        
        # Create session with proper Chrome configuration
        session_params = CreateSessionParams(
            screen={'width': 1920, 'height': 1080},
            use_stealth=True,
            profile={'id': profile_id, 'persist_changes': True},
            start_url='chrome://popup/',
            browser_type='chrome'
        )
        
        if extension_ids:
            session_params.extension_ids = extension_ids
        
        session_response = hb_client.sessions.create(session_params)
        setup_session_id = session_response.id
        
        logging.info(f"Setup session created: {setup_session_id}")
        
        # Store session information in MongoDB
        session_data = {
            'session_id': setup_session_id,
            'profile_id': profile_id,
            'extension_id': extension_ids[0] if extension_ids else None,
            'browser_type': 'chrome',
            'session_status': 'active',
            'is_active': True,
            'created_at': datetime.now(),
            'started_at': datetime.now(),
            'ended_at': None,
            'session_duration': None,
            'session_purpose': session_purpose,
            'workflow_count': 0,
            'browser_version': '120.0.6099.109',
            'user_data_dir': f'/hyperbrowser/profiles/{profile_id}',
            'session_metadata': {
                'headless': False,
                'debugging_enabled': True,
                'extension_loaded': bool(extension_ids),
                'startup_time': 2.5,
                'screen_resolution': '1920x1080',
                'stealth_enabled': True,
                'created_via': 'hyperbrowser_profile_manager',
                'hyperbrowser_session_data': {
                    'session_id': setup_session_id,
                    'creation_timestamp': datetime.now().isoformat()
                }
            }
        }
        
        # Store in browser_sessions collection
        session_mongodb_id = store_in_mongodb('browser_sessions', session_data)
        
        # Update extension_instances with associated profile
        if extension_ids:
            try:
                client, db = get_mongodb_client()
                db.extension_instances.update_one(
                    {'extension_id': extension_ids[0]},
                    {
                        '$addToSet': {'associated_profiles': profile_id},
                        '$inc': {'usage_count': 1},
                        '$set': {'last_updated_at': datetime.now()}
                    }
                )
                client.close()
                logging.info(f"Updated extension {extension_ids[0]} usage count")
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
            logging.info(f"Updated profile {profile_id} usage count")
        except Exception as e:
            logging.warning(f"Could not update profile usage: {e}")
        
        # Wait a moment for session to fully initialize
        time.sleep(3)
        
        return {
            'profile_id': profile_id,
            'extension_id': extension_ids[0] if extension_ids else None,
            'setup_session_id': setup_session_id,
            'setup_success': True,
            'browser_configured': True,
            'session_mongodb_id': session_mongodb_id,
            'stored_in_mongodb': True,
            'session_purpose': session_purpose,
            'browser_url': f"https://app.hyperbrowser.ai/sessions/{setup_session_id}"
        }
        
    except Exception as e:
        logging.error(f"Failed to setup profile with Automa: {e}")
        raise

# ==============================================================================
# ADDITIONAL HELPER FUNCTIONS FOR PROFILE MANAGEMENT
# ==============================================================================

def get_profile_by_id(profile_id):
    """Get profile information by profile ID"""
    try:
        profile = get_from_mongodb('accounts', {
            'profile_id': profile_id,
            'is_active': True
        })
        return profile
    except Exception as e:
        logging.error(f"Failed to get profile {profile_id}: {e}")
        return None

def get_profiles_by_type(profile_type='hyperbrowser_automa'):
    """Get all profiles of a specific type"""
    try:
        client, db = get_mongodb_client()
        profiles = list(db.accounts.find({
            'profile_type': profile_type,
            'is_active': True
        }).sort('created_at', -1))
        client.close()
        return profiles
    except Exception as e:
        logging.error(f"Failed to get profiles of type {profile_type}: {e}")
        return []

def deactivate_profile(profile_id, reason='manual_deactivation'):
    """Deactivate a profile"""
    try:
        update_result = update_in_mongodb('accounts',
            {'profile_id': profile_id},
            {
                'is_active': False,
                'deactivated_at': datetime.now(),
                'deactivation_reason': reason
            })
        
        # Also close any active sessions for this profile
        client, db = get_mongodb_client()
        db.browser_sessions.update_many(
            {'profile_id': profile_id, 'is_active': True},
            {
                '$set': {
                    'is_active': False,
                    'session_status': 'profile_deactivated',
                    'ended_at': datetime.now()
                }
            }
        )
        client.close()
        
        logging.info(f"Deactivated profile {profile_id} and closed associated sessions")
        return update_result
        
    except Exception as e:
        logging.error(f"Failed to deactivate profile {profile_id}: {e}")
        return False

def create_workflow_session_from_profile(profile_id, workflow_type=None):
    """Create a new session for workflow execution using an existing profile"""
    try:
        # Get the profile's extension
        profile = get_profile_by_id(profile_id)
        if not profile:
            raise ValueError(f"Profile {profile_id} not found")
        
        # Try to get the extension ID from existing sessions or setup data
        extension_id = None
        try:
            client, db = get_mongodb_client()
            recent_session = db.browser_sessions.find_one(
                {'profile_id': profile_id, 'extension_id': {'$exists': True, '$ne': None}},
                sort=[('created_at', -1)]
            )
            if recent_session:
                extension_id = recent_session['extension_id']
            client.close()
        except Exception as e:
            logging.warning(f"Could not get extension ID from recent sessions: {e}")
        
        # Create the session
        return setup_profile_with_automa(
            profile_id=profile_id,
            extension_id=extension_id,
            session_purpose='workflow_execution'
        )
        
    except Exception as e:
        logging.error(f"Failed to create workflow session for profile {profile_id}: {e}")
        raise

# ==============================================================================
# EXAMPLE USAGE PATTERNS
# ==============================================================================

"""
Example usage:

from hyperbrowser_profile_manager import (
    check_existing_profile,
    create_hyperbrowser_profile,
    setup_profile_with_automa,
    create_workflow_session_from_profile
)

# In a setup script:
check_result = check_existing_profile(profile_type='custom_profile_type')
if not check_result['skip_creation']:
    create_result = create_hyperbrowser_profile(
        profile_type='custom_profile_type',
        profile_name='My Custom Profile',
        previous_check_result=check_result
    )
    setup_result = setup_profile_with_automa(
        profile_id=create_result['profile_id']
    )

# In a workflow script:
session_info = create_workflow_session_from_profile(
    profile_id='existing_profile_id',
    workflow_type='replies'
)

# Direct usage:
profiles = get_profiles_by_type('hyperbrowser_automa')
for profile in profiles:
    print(f"Profile: {profile['profile_id']}, Usage: {profile['usage_count']}")
"""