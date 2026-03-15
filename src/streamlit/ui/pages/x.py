"""
Fixed Hyperbrowser Extension Management with Multi-URL Navigation

This module provides corrected functions for uploading Automa extensions
and creating sessions with extensions properly attached, with proper 
PostgreSQL account linking and multi-URL navigation support.

Key features:
1. Proper extension upload using file path
2. Correct session creation with extensionIds parameter
3. Multi-URL navigation in same tab
4. Better error handling and validation
5. Extension-to-account linking
"""

import os
import logging
import time
from datetime import datetime
from hyperbrowser import Hyperbrowser
from pathlib import Path

# Import the base utilities
from .hyperbrowser_utils import (
    get_mongodb_client,
    store_in_mongodb,
    get_from_mongodb,
    update_in_mongodb
)

# Configuration
HYPERBROWSER_API_KEY = os.environ.get('HYPERBROWSER_API_KEY')
AUTOMA_EXTENSION_PATH = os.environ.get('AUTOMA_EXTENSION_PATH', '/app/automa-extension/automa-extension.zip')

def validate_extension_file(extension_path):
    """Validate that the extension file exists and is accessible"""
    try:
        if not os.path.exists(extension_path):
            raise FileNotFoundError(f"Extension file not found at: {extension_path}")
        
        if not extension_path.endswith('.zip'):
            raise ValueError(f"Extension file must be a ZIP archive: {extension_path}")
        
        # Check file size (reasonable limit)
        file_size = os.path.getsize(extension_path)
        max_size = 50 * 1024 * 1024  # 50MB limit
        if file_size > max_size:
            raise ValueError(f"Extension file too large: {file_size} bytes (max: {max_size})")
        
        if file_size == 0:
            raise ValueError(f"Extension file is empty: {extension_path}")
        
        logging.info(f"Extension file validated: {extension_path} ({file_size} bytes)")
        return True
        
    except Exception as e:
        logging.error(f"Extension file validation failed: {e}")
        raise


def create_session_with_extension(profile_id, extension_id=None, session_config=None, **context):
    """
    Create a Hyperbrowser session with extension and navigate to multiple URLs
    
    Args:
        profile_id (str): Profile ID to use for the session
        extension_id (str): Extension ID to attach (optional)
        session_config (dict): Additional session configuration including URLs
        **context: Additional context
    
    Returns:
        dict: Session creation results
    """
    try:
        if not HYPERBROWSER_API_KEY:
            raise ValueError("HYPERBROWSER_API_KEY is required for session creation")
        
        if not profile_id:
            raise ValueError("Profile ID is required for session creation")
        
        # Get extension ID if not provided
        if not extension_id:
            extension_data = get_from_mongodb('extension_instances', {
                'extension_name': 'automa-extension',
                'is_enabled': True,
                'installation_status': 'active'
            }, **context)
            
            if extension_data:
                extension_id = extension_data['extension_id']
                logging.info(f"Using extension from MongoDB: {extension_id}")
            else:
                logging.warning("No active extension found, creating session without extension")
        
        # Default session configuration
        config = session_config or {}
        
        # Get URLs to open - UPDATED to support multiple URLs
        urls_to_open = config.get('urls_to_open', [])
        if not urls_to_open:
            # Default URLs for extension installation and Twitter
            urls_to_open = [
                'https://chromewebstore.google.com/detail/automa/infppggnoaenmfagbfknfkancpbljcca',
                'https://x.com/'
            ]
        
        # Use first URL as start_url
        start_url = urls_to_open[0] if urls_to_open else config.get('start_url', 'https://twitter.com')
        
        logging.info(f"Creating session with profile: {profile_id}, extension: {extension_id}")
        logging.info(f"Will open URLs: {urls_to_open}")
        
        hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
        
        # Create session parameters
        from hyperbrowser.models import CreateSessionParams
        
        session_params = CreateSessionParams(
            screen={
                'width': config.get('screen_width', 1920),
                'height': config.get('screen_height', 1080)
            },
            use_stealth=config.get('use_stealth', True),
            profile={
                'id': profile_id,
                'persist_changes': config.get('persist_changes', True)
            },
            start_url=start_url,  # Start with first URL
            browser_type='chrome',
            timeout_minutes=config.get('timeout_minutes', 120)
        )
        
        # Add extension IDs
        if extension_id:
            session_params.extension_ids = [extension_id]
            logging.info(f"Session will include extension: {extension_id}")
        else:
            logging.info("Session will be created without extensions")
        
        # Create the session
        logging.info("Creating Hyperbrowser session...")
        session_response = hb_client.sessions.create(session_params)
        session_id = session_response.id
        
        logging.info(f"Session created successfully: {session_id}")
        
        # Wait for session to initialize
        time.sleep(3)
        
        # Navigate to additional URLs if provided
        if len(urls_to_open) > 1:
            logging.info(f"Navigating to {len(urls_to_open) - 1} additional URLs...")
            try:
                session = hb_client.sessions.get(session_id)
                
                # Navigate to remaining URLs in the same tab
                for url in urls_to_open[1:]:
                    logging.info(f"Navigating to: {url}")
                    session.goto(url)
                    time.sleep(2)  # Wait between navigations
                
                logging.info("Successfully navigated to all URLs")
                
            except Exception as nav_error:
                logging.warning(f"Could not navigate to additional URLs: {nav_error}")
                # Don't fail the session creation if navigation fails
        
        # Store session information in MongoDB
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
            'session_purpose': config.get('session_purpose', 'workflow_execution'),
            'workflow_count': 0,
            'success_count': 0,
            'urls_opened': urls_to_open,
            'current_url': urls_to_open[-1] if urls_to_open else start_url,
            'session_config': {
                'screen_resolution': f"{config.get('screen_width', 1920)}x{config.get('screen_height', 1080)}",
                'stealth_enabled': config.get('use_stealth', True),
                'start_url': start_url,
                'urls_to_open': urls_to_open,
                'timeout_minutes': config.get('timeout_minutes', 120),
                'persist_changes': config.get('persist_changes', True),
                'extension_enabled': bool(extension_id)
            },
            'session_metadata': {
                'created_for': 'enhanced_session_with_extension_and_navigation',
                'parent_profile_id': profile_id,
                'extension_loaded': bool(extension_id),
                'stealth_enabled': config.get('use_stealth', True),
                'created_via': 'fixed_extension_manager_with_multi_url',
                'hyperbrowser_session_data': {
                    'session_id': session_id,
                    'creation_timestamp': datetime.now().isoformat(),
                    'extension_ids_used': [extension_id] if extension_id else [],
                    'navigation_sequence': urls_to_open
                }
            }
        }
        
        # Store in MongoDB
        session_mongodb_id = store_in_mongodb('browser_sessions', session_data, **context)
        
        # Update extension usage if extension was used
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
                    '$set': {'last_used_at': datetime.now()},
                    '$push': {
                        'recent_sessions': {
                            'session_id': session_id,
                            'started_at': datetime.now(),
                            'extension_used': bool(extension_id),
                            'urls_opened': urls_to_open
                        }
                    }
                }
            )
            client.close()
            logging.info(f"Updated profile {profile_id} usage statistics")
        except Exception as e:
            logging.warning(f"Could not update profile usage: {e}")
        
        return {
            'session_id': session_id,
            'profile_id': profile_id,
            'extension_id': extension_id,
            'mongodb_id': session_mongodb_id,
            'browser_url': f"https://app.hyperbrowser.ai/sessions/{session_id}",
            'session_created': True,
            'extension_loaded': bool(extension_id),
            'urls_opened': urls_to_open,
            'message': f"Session {session_id} created successfully" + (f" with extension {extension_id}" if extension_id else " without extensions") + f" and navigated to {len(urls_to_open)} URLs"
        }
        
    except Exception as e:
        error_msg = f"Failed to create session with extension: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)


def link_extension_to_postgres_account(extension_id, postgres_account_id, username, max_retries=3, **context):
    """
    Link extension to PostgreSQL account with robust error handling and retries
    """
    import time
    
    for attempt in range(max_retries):
        try:
            logging.info(f"Linking extension {extension_id} to account {postgres_account_id} (attempt {attempt + 1}/{max_retries})")
            
            # Import MongoDB utilities with multiple fallback patterns
            try:
                from .hyperbrowser_utils import get_mongodb_client
            except ImportError:
                try:
                    from streamlit_hyperbrowser_manager import get_mongodb_client
                except ImportError:
                    import streamlit_hyperbrowser_manager
                    get_mongodb_client = streamlit_hyperbrowser_manager.get_mongodb_client
            
            client, db = get_mongodb_client()
            
            # First verify extension exists
            extension = db.extension_instances.find_one({'extension_id': extension_id})
            if not extension:
                client.close()
                raise Exception(f"Extension {extension_id} not found in MongoDB")
            
            # Check if already linked to this account
            if (extension.get('postgres_account_id') == postgres_account_id and 
                extension.get('linked_to_postgres') == True):
                client.close()
                logging.info(f"Extension {extension_id} already linked to account {postgres_account_id}")
                return True
            
            # Check if linked to a different account
            if (extension.get('postgres_account_id') and 
                extension.get('postgres_account_id') != postgres_account_id):
                client.close()
                raise Exception(f"Extension {extension_id} already linked to account {extension.get('postgres_account_id')}")
            
            # Perform the update
            update_data = {
                'postgres_account_id': postgres_account_id,
                'account_username': username,
                'linked_to_postgres': True,
                'postgres_link_created_at': datetime.now(),
                'last_postgres_sync': datetime.now(),
                'link_attempt': attempt + 1
            }
            
            result = db.extension_instances.update_one(
                {'extension_id': extension_id},
                {'$set': update_data}
            )
            
            client.close()
            
            if result.modified_count > 0:
                logging.info(f"✅ Successfully linked extension {extension_id} to account {postgres_account_id}")
                time.sleep(1)
                return True
            else:
                logging.warning(f"⚠️ No documents modified in link attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    time.sleep(2)
                    continue
                else:
                    return False
                    
        except Exception as e:
            logging.error(f"❌ Link attempt {attempt + 1} failed: {e}")
            if 'client' in locals():
                try:
                    client.close()
                except:
                    pass
            
            if attempt < max_retries - 1:
                time.sleep(2)
                continue
            else:
                logging.error(f"❌ All {max_retries} link attempts failed for extension {extension_id}")
                return False
    
    return False


# Rest of the functions remain the same as in the original file
# (get_or_create_extension_for_account, upload_automa_extension_for_account, etc.)