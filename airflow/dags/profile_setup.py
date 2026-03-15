from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import os
import logging
import json
import time
import re
from datetime import datetime, timedelta
from bson import ObjectId

# Hyperbrowser imports
from hyperbrowser import Hyperbrowser

# Import our reusable utilities
from dag_components.hyperbrowser.hyperbrowser_utils import (
    get_mongodb_client,
    store_in_mongodb,
    get_from_mongodb,
    update_in_mongodb,
    get_hyperbrowser_profile_id,
    get_hyperbrowser_extension_id,
    mark_manual_setup_complete
)

# Import the extracted profile management functions
from dag_components.hyperbrowser.profile_manager import (
    check_existing_profile,
    create_hyperbrowser_profile,
    setup_profile_with_automa
)

# Default arguments for the setup DAG
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Initialize the setup DAG
setup_dag = DAG(
    'profile_setup',
    default_args=default_args,
    description='One-time setup of Hyperbrowser profile with Automa extension - MongoDB storage',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['setup', 'hyperbrowser', 'automa', 'profile', 'mongodb']
)

# Configuration
HYPERBROWSER_API_KEY = os.environ.get('HYPERBROWSER_API_KEY')
AUTOMA_EXTENSION_PATH = os.environ.get('AUTOMA_EXTENSION_PATH', '/opt/automa-extension/automa-extension.zip')

def validate_prerequisites(**context):
    """Validate all prerequisites are in place before setup"""
    logging.info("Validating prerequisites for Hyperbrowser profile setup...")
    
    prerequisites = {
        'HYPERBROWSER_API_KEY': HYPERBROWSER_API_KEY,
        'AUTOMA_EXTENSION_PATH': AUTOMA_EXTENSION_PATH
    }
    
    missing = []
    for key, value in prerequisites.items():
        if not value:
            missing.append(key)
            logging.error(f"Missing required environment variable: {key}")
        else:
            logging.info(f"Found {key}")
    
    if missing:
        raise ValueError(f"Missing required environment variables: {', '.join(missing)}")
    
    # Check if extension file exists
    if not os.path.exists(AUTOMA_EXTENSION_PATH):
        raise ValueError(f"Automa extension file not found at: {AUTOMA_EXTENSION_PATH}")
    
    logging.info(f"Found Automa extension file: {AUTOMA_EXTENSION_PATH}")
    
    # Test Hyperbrowser API connection
    try:
        hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
        profiles_response = hb_client.profiles.list()
        
        profile_count = 0
        if hasattr(profiles_response, 'profiles'):
            profile_count = len(profiles_response.profiles)
        elif isinstance(profiles_response, list):
            profile_count = len(profiles_response)
        
        logging.info(f"Hyperbrowser API connection successful. Found {profile_count} existing profiles.")
        
    except Exception as e:
        logging.error(f"Failed to connect to Hyperbrowser API: {e}")
        raise
    
    # Test MongoDB connection
    try:
        client, db = get_mongodb_client()
        collections = db.list_collection_names()
        client.close()
        logging.info(f"MongoDB connection successful. Found {len(collections)} collections.")
    except Exception as e:
        logging.error(f"Failed to connect to MongoDB: {e}")
        raise
    
    logging.info("All prerequisites validated successfully!")
    return {'status': 'prerequisites_valid'}

def upload_automa_extension(**context):
    """Upload Automa extension to Hyperbrowser and store in MongoDB"""
    try:
        from hyperbrowser.models.extension import CreateExtensionParams
        
        hb_client = Hyperbrowser(api_key=HYPERBROWSER_API_KEY)
        
        # Upload the extension using the SDK with proper parameters
        response = hb_client.extensions.create(
            CreateExtensionParams(
                name="automa-extension",
                file_path=AUTOMA_EXTENSION_PATH
            )
        )
        
        extension_id = response.id
        logging.info(f"Successfully uploaded Automa extension with ID: {extension_id}")
        
        # Store extension information in MongoDB
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
            'extension_metadata': {
                'permissions': ['activeTab', 'storage', 'scripting'],
                'content_scripts': True,
                'background_service_worker': True,
                'uploaded_via': 'airflow_dag',
                'hyperbrowser_response': {
                    'id': extension_id,
                    'upload_timestamp': datetime.now().isoformat()
                }
            }
        }
        
        # Store in extension_instances collection
        mongodb_id = store_in_mongodb('extension_instances', extension_data, **context)
        
        # Store in XCom for immediate use by following tasks
        context['task_instance'].xcom_push(key='extension_id', value=extension_id)
        context['task_instance'].xcom_push(key='extension_mongodb_id', value=mongodb_id)
        
        return {
            'extension_id': extension_id,
            'mongodb_id': mongodb_id,
            'stored_in_mongodb': True
        }
        
    except Exception as e:
        logging.error(f"Error uploading Automa extension: {str(e)}")
        if 'response' in locals():
            logging.error(f"Response type: {type(response)}")
            logging.error(f"Response attributes: {dir(response)}")
        raise

def provide_manual_instructions(**context):
    """Provide detailed manual setup instructions with MongoDB references"""
    
    profile_id = context['task_instance'].xcom_pull(
        task_ids='create_hyperbrowser_profile', 
        key='profile_id'
    )
    extension_id = context['task_instance'].xcom_pull(
        task_ids='upload_automa_extension', 
        key='extension_id'
    )
    setup_session_id = context['task_instance'].xcom_pull(
        task_ids='setup_profile_with_automa', 
        key='setup_session_id'
    )
    setup_success = context['task_instance'].xcom_pull(
        task_ids='setup_profile_with_automa', 
        key='setup_success'
    )
    
    logging.info("HYPERBROWSER + AUTOMA SETUP INSTRUCTIONS")
    logging.info("="*80)
    
    logging.info(f"Profile ID: {profile_id}")
    logging.info(f"Extension ID: {extension_id}")
    logging.info(f"Setup Session: {setup_session_id}")
    logging.info(f"Automated Setup: {'Success' if setup_success else 'Needs Attention'}")
    logging.info("Storage Method: MongoDB collections")
    
    logging.info("\nSTORED IN MONGODB COLLECTIONS:")
    logging.info("-" * 40)
    logging.info("• chrome_profiles - Profile information and settings")
    logging.info("• extension_instances - Extension metadata and usage")
    logging.info("• browser_sessions - Active session tracking")
    logging.info("• hyperbrowser_setup - Setup metadata and history")
    
    logging.info("\nMANUAL STEPS REQUIRED:")
    logging.info("-" * 40)
    
    logging.info("1. ACCESS THE CHROME BROWSER:")
    logging.info(f"   Go to: https://app.hyperbrowser.ai/sessions/{setup_session_id}")
    logging.info("   You should see a Chrome browser window with the new tab page")
    logging.info("   Look for the Automa extension icon in the browser toolbar")
    
    logging.info("\n2. VERIFY AUTOMA EXTENSION:")
    logging.info("   - Click the Automa extension icon in the browser toolbar")
    logging.info("   - If you don't see it, click the puzzle piece icon to view all extensions")
    logging.info("   - Pin the Automa extension for easy access")
    
    logging.info("\n3. COMPLETE LOGINS:")
    logging.info("   - Navigate to Twitter/X and login with your automation account")
    logging.info("   - Login to any other services your workflows need")
    logging.info("   - These login sessions will be saved to your profile")
    
    logging.info("\n4. CONFIGURE AUTOMA WORKFLOWS:")
    logging.info("   - Click the Automa extension icon")
    logging.info("   - Import your existing workflows or create new ones")
    logging.info("   - Test a few workflows manually to ensure they work")
    
    logging.info("\n5. MARK SETUP COMPLETE:")
    logging.info("   - After manual configuration is done, call:")
    logging.info("   - from hyperbrowser_utils import mark_manual_setup_complete")
    logging.info("   - mark_manual_setup_complete()")
    
    logging.info("\nMONGODB QUERIES TO CHECK SETUP:")
    logging.info("-" * 40)
    logging.info(f"db.chrome_profiles.findOne({{'profile_id': '{profile_id}'}})")
    if extension_id:
        logging.info(f"db.extension_instances.findOne({{'extension_id': '{extension_id}'}})")
    logging.info(f"db.browser_sessions.findOne({{'session_id': '{setup_session_id}'}})")
    
    # Store final instructions in XCom
    instructions = {
        'profile_id': profile_id,
        'extension_id': extension_id,
        'setup_session_id': setup_session_id,
        'browser_url': f"https://app.hyperbrowser.ai/sessions/{setup_session_id}",
        'storage_method': 'mongodb_collections',
        'manual_steps': [
            'Access Chrome browser and verify Automa extension',
            'Complete logins to required services', 
            'Configure Automa workflows',
            'Test workflow execution',
            'Call mark_manual_setup_complete() to finish'
        ]
    }
    
    context['task_instance'].xcom_push(key='setup_instructions', value=instructions)
    return instructions

def log_setup_completion(**context):
    """Log setup completion and store comprehensive metadata in MongoDB"""
    
    profile_id = context['task_instance'].xcom_pull(
        task_ids='create_hyperbrowser_profile', 
        key='profile_id'
    )
    extension_id = context['task_instance'].xcom_pull(
        task_ids='upload_automa_extension', 
        key='extension_id'
    )
    setup_session_id = context['task_instance'].xcom_pull(
        task_ids='setup_profile_with_automa', 
        key='setup_session_id'
    )
    setup_success = context['task_instance'].xcom_pull(
        task_ids='setup_profile_with_automa', 
        key='setup_success'
    )
    
    try:
        # Store comprehensive setup metadata in MongoDB
        setup_metadata = {
            'setup_type': 'hyperbrowser_profile_mongodb',
            'profile_id': profile_id,
            'extension_id': extension_id,
            'setup_session_id': setup_session_id,
            'automated_setup_success': setup_success,
            'storage_method': 'mongodb_only',
            'setup_completed_at': datetime.now(),
            'setup_dag_run_id': context.get('dag_run').run_id,
            'status': 'setup_completed_manual_steps_required',
            'manual_steps_required': True,
            'next_action': 'complete_manual_setup_then_call_mark_manual_setup_complete',
            'browser_configuration': 'chrome_with_automa_extension',
            
            # Configuration for other DAGs
            'configuration_for_workflows': {
                'profile_id': profile_id,
                'extension_id': extension_id,
                'session_template_id': setup_session_id,
                'browser_type': 'chrome',
                'extension_loaded': True,
                'ready_for_automation': False  # Will be True after mark_manual_setup_complete()
            }
        }
        
        # Store in hyperbrowser_setup collection
        setup_mongodb_id = store_in_mongodb('hyperbrowser_setup', setup_metadata, **context)
        
        # Create settings entry for easy access
        settings_data = {
            'category': 'hyperbrowser_configuration',
            'settings': {
                'active_profile_id': profile_id,
                'active_extension_id': extension_id,
                'template_session_id': setup_session_id,
                'setup_completed': True,
                'manual_configuration_required': True,
                'mongodb_storage_enabled': True,
                'last_setup_date': datetime.now().isoformat(),
                'configuration_status': 'pending_manual_setup'
            },
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        # Store/update in settings collection
        try:
            client, db = get_mongodb_client()
            db.settings.update_one(
                {'category': 'hyperbrowser_configuration'},
                {'$set': settings_data},
                upsert=True
            )
            client.close()
            logging.info("Updated hyperbrowser_configuration in settings collection")
        except Exception as e:
            logging.warning(f"Could not update settings collection: {e}")
        
        logging.info(f"Setup metadata stored in MongoDB: {setup_mongodb_id}")
        
    except Exception as e:
        logging.warning(f"Could not store setup metadata in MongoDB: {e}")
    
    # Final summary
    logging.info("\nHYPERBROWSER PROFILE SETUP COMPLETED!")
    logging.info("="*60)
    logging.info(f"Profile ID: {profile_id}")
    logging.info(f"Extension ID: {extension_id}")
    logging.info(f"Setup Session: {setup_session_id}")
    logging.info(f"Browser URL: https://app.hyperbrowser.ai/sessions/{setup_session_id}")
    logging.info("Configuration stored in MongoDB collections")
    logging.info("Manual setup steps are now required to complete configuration")
    logging.info("Use hyperbrowser_utils functions in other DAGs to access configuration")
    logging.info("="*60)
    
    return {
        'setup_status': 'completed',
        'profile_id': profile_id,
        'extension_id': extension_id,
        'requires_manual_steps': True,
        'setup_session_id': setup_session_id,
        'storage_method': 'mongodb_only',
        'mongodb_collections_used': ['chrome_profiles', 'extension_instances', 'browser_sessions', 'hyperbrowser_setup', 'settings'],
        'browser_type': 'chrome_with_extensions'
    }

# Define tasks using imported functions
validate_task = PythonOperator(
    task_id='validate_prerequisites',
    python_callable=validate_prerequisites,
    dag=setup_dag,
)

upload_extension_task = PythonOperator(
    task_id='upload_automa_extension',
    python_callable=upload_automa_extension,
    dag=setup_dag,
)

# Use imported functions for these three tasks
check_profile_task = PythonOperator(
    task_id='check_existing_profile',
    python_callable=check_existing_profile,
    op_kwargs={'profile_type': 'hyperbrowser_automa'},
    dag=setup_dag,
)

create_profile_task = PythonOperator(
    task_id='create_hyperbrowser_profile',
    python_callable=create_hyperbrowser_profile,
    op_kwargs={
        'profile_type': 'hyperbrowser_automa',
        'skip_if_exists': True
    },
    dag=setup_dag,
)

setup_automa_task = PythonOperator(
    task_id='setup_profile_with_automa',
    python_callable=setup_profile_with_automa,
    op_kwargs={'session_purpose': 'setup'},
    dag=setup_dag,
)

instructions_task = PythonOperator(
    task_id='provide_manual_instructions',
    python_callable=provide_manual_instructions,
    dag=setup_dag,
)

completion_task = PythonOperator(
    task_id='log_setup_completion',
    python_callable=log_setup_completion,
    dag=setup_dag,
)

# Define task dependencies
validate_task >> [upload_extension_task, check_profile_task]
check_profile_task >> create_profile_task
[upload_extension_task, create_profile_task] >> setup_automa_task >> instructions_task >> completion_task