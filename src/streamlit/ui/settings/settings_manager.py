#!/usr/bin/env python3
from pymongo import MongoClient
from .settings import (DEFAULT_SETTINGS, validate_single_workflow_settings, get_single_workflow_settings,
                       validate_workflow_type_settings, validate_priority_order_settings,
                       get_enabled_workflow_types, get_workflow_type_priority_order,
                       filter_workflows_by_enabled_types, sort_workflows_by_priority)
import re
from pymongo import MongoClient
from datetime import date, datetime
import logging
from datetime import datetime
from typing import Dict, Any, List
import os
import logging
from .settings import (
    DEFAULT_SETTINGS,
    validate_single_workflow_settings,
    get_single_workflow_settings,
    validate_workflow_type_settings,
    validate_priority_order_settings,
    get_enabled_workflow_types,
    get_workflow_type_priority_order,
    filter_workflows_by_enabled_types,
    sort_workflows_by_priority
)
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)
#!/usr/bin/env python3
# File: settings_manager.py - Backend Database Methods

from pymongo import MongoClient
from datetime import datetime
import os
import logging

logger = logging.getLogger(__name__)

def reset_workflow_execution_status_with_reverse(workflow_types=None, reset_all=False, reverse_order=False,
                                                  include_successful=True, include_failed=True):
    """Reset execution status for workflows with optional reverse processing order and selective filtering."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Get enabled workflow types if not specified
        if workflow_types is None and not reset_all:
            enabled_types = get_system_setting('extraction_workflow_types',
                                              {'replies': True, 'messages': True, 'retweets': True})
            workflow_types = [wf_type for wf_type, enabled in enabled_types.items() if enabled]

        # Reverse the order if requested
        if reverse_order and workflow_types:
            workflow_types = list(reversed(workflow_types))
            logger.info(f"Reversed workflow processing order: {workflow_types}")

        # FIXED: Build filter for BOTH collections
        reset_filter = {}

        if reset_all:
            reset_filter = {}  # No filter = all documents
        else:
            reset_filter = {}

            if workflow_types:
                if isinstance(workflow_types, str):
                    workflow_types = [workflow_types]
                reset_filter['content_type'] = {'$in': workflow_types}

            # Add execution status filters
            status_conditions = []

            if include_successful:
                status_conditions.append({'executed': True, 'execution_success': True})

            if include_failed:
                status_conditions.append({'executed': True, 'execution_success': False})
                status_conditions.append({'executed': True, 'execution_success': None})
                status_conditions.append({'executed': True, 'execution_success': {'$exists': False}})

            if status_conditions:
                reset_filter['$or'] = status_conditions
            else:
                reset_filter['executed'] = True  # Default: reset all executed

        # FIXED: Reset BOTH collections
        reset_data = {
            'executed': False,
            'execution_success': None,
            'executed_at': None,
            'execution_error': None,
            'error_category': None,
            'final_result': None,
            'execution_time': None,
            'steps_taken': None,
            'session_id': None,
            'video_recording_session_id': None,
            'video_recording_enabled': None,
            'recording_status': None,
            'dag_run_id': None,
            'postgres_account_id': None,
            'account_username': None,
            'profile_id': None,
            'extension_id': None,
            'profile_specific_execution': None,
            'execution_mode': None,
            'success': False,  # Additional field
            'status': 'pending',  # Reset status
            'reset_at': datetime.now(),
            'reset_reason': f'reverse_execution_reset_order_{reverse_order}',
            'processing_order_reversed': reverse_order,
            'reset_mode': 'selective' if not reset_all else 'complete',
            'reset_by': 'streamlit_ui',
            'updated_at': datetime.now()
        }

        # FIXED: Reset content_workflow_links (main collection)
        content_result = db.content_workflow_links.update_many(
            reset_filter if reset_filter else {},
            {'$set': reset_data}
        )

        # FIXED: Reset workflow_executions (secondary collection)
        workflow_result = db.workflow_executions.update_many(
            reset_filter if reset_filter else {},
            {'$set': reset_data}
        )

        total_reset = content_result.modified_count + workflow_result.modified_count

        logger.info(f"Reset {content_result.modified_count} content workflow links")
        logger.info(f"Reset {workflow_result.modified_count} workflow executions")
        logger.info(f"Total reset: {total_reset} workflows")

        # Get count by type
        workflows_by_type = {}
        if workflow_types:
            for wf_type in workflow_types:
                type_filter = {'content_type': wf_type}
                count = db.content_workflow_links.count_documents(type_filter)
                workflows_by_type[wf_type] = count

        # Update priority order if reverse was requested
        if reverse_order and workflow_types:
            try:
                update_system_setting('extraction_priority_order', workflow_types)
                logger.info(f"Updated priority order to reversed: {workflow_types}")
            except Exception as e:
                logger.warning(f"Failed to update priority order: {e}")

        # Clear daily limit tracking
        if total_reset > 0:
            try:
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                db.daily_workflow_analytics.delete_many({'date': today})
                logger.info("Cleared daily workflow analytics to reset daily limits")
            except Exception as e:
                logger.warning(f"Failed to clear daily analytics: {e}")

        return {
            'success': True,
            'reset_count': total_reset,
            'content_links_reset': content_result.modified_count,
            'workflow_executions_reset': workflow_result.modified_count,
            'filter_used': str(reset_filter),
            'workflow_types_reset': workflow_types if workflow_types else 'all',
            'workflows_by_type': workflows_by_type,
            'processing_order_reversed': reverse_order,
            'new_priority_order': workflow_types if reverse_order and workflow_types else None,
            'include_successful': include_successful,
            'include_failed': include_failed,
            'reset_at': datetime.now().isoformat(),
            'daily_limits_cleared': True
        }

    except Exception as e:
        logger.error(f"Error resetting workflow execution status with reverse: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            'success': False,
            'reset_count': 0,
            'error': str(e),
            'workflow_types_reset': workflow_types if workflow_types else 'all',
            'processing_order_reversed': reverse_order,
            'reset_at': datetime.now().isoformat()
        }
    finally:
        if client:
            client.close()
# File: streamlit/ui/settings/settings_manager.py
# ADD these functions at the end of your existing settings_manager.py

# ==============================================================================
# POSTGRES DATABASE FUNCTIONS - Centralized database access
# ==============================================================================

# File: streamlit/ui/settings/settings_manager.py
# FIXED: Changed import path to work in Airflow context

def get_postgres_prompt_types():
    """
    Get all unique prompt types from the prompts table via Postgres.
    GRACEFUL FALLBACK: Returns empty list if no prompts exist.
    """
    # Try multiple import paths to work in different contexts
    connection_module = None
    cursor_factory = None

    try:
        # Try Airflow/DAG context first (no src prefix)
        from core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor
        connection_module = get_postgres_connection
        cursor_factory = RealDictCursor
        logger.info("Using Airflow/DAG import path (core.database.postgres)")
    except ImportError:
        try:
            # Try Streamlit context (with src prefix)
            from src.core.database.postgres.connection import get_postgres_connection
            from psycopg2.extras import RealDictCursor
            connection_module = get_postgres_connection
            cursor_factory = RealDictCursor
            logger.info("Using Streamlit import path (src.core.database.postgres)")
        except ImportError as ie:
            error_msg = (
                f"Warning: Cannot import database connection from any known path: {ie}. "
                f"Tried: 'core.database.postgres.connection' and 'src.core.database.postgres.connection'"
            )
            logger.warning(error_msg)
            return []  # Return empty list instead of raising error

    try:
        with connection_module() as conn:
            logger.info(f"PostgreSQL connection status: closed={conn.closed}")

            with conn.cursor(cursor_factory=cursor_factory) as cursor:
                # Check if prompts table has any data
                cursor.execute("SELECT COUNT(*) as count FROM prompts")
                count_result = cursor.fetchone()
                total_prompts = count_result['count'] if count_result else 0
                logger.info(f"Total prompts in database: {total_prompts}")

                # If no prompts exist, return empty list
                if total_prompts == 0:
                    logger.warning("No prompts found in database. Please create prompts first.")
                    return []

                # Get the unique prompt types
                cursor.execute("""
                    SELECT DISTINCT prompt_type
                    FROM prompts
                    WHERE prompt_type IS NOT NULL
                    ORDER BY prompt_type
                """)

                results = cursor.fetchall()
                prompt_types = [row['prompt_type'] for row in results]

                logger.info(f"Retrieved {len(prompt_types)} prompt types from Postgres: {prompt_types}")

                # If prompts exist but no types found (data integrity issue)
                if not prompt_types:
                    logger.warning(f"{total_prompts} prompts exist but ALL have NULL prompt_type.")

                    # Show sample data for debugging
                    cursor.execute("SELECT prompt_id, name, prompt_type, is_active FROM prompts LIMIT 5")
                    samples = cursor.fetchall()
                    logger.warning(f"Sample prompts: {[dict(s) for s in samples]}")

                    return []

                return prompt_types

    except Exception as e:
        error_msg = f"Database error while fetching prompt types: {e}"
        logger.error(error_msg)
        return []  # Return empty list instead of raising error
# ADD THESE FUNCTIONS TO settings_manager.py

def get_execution_setting(setting_key=None, default=None):
    """
    Get execution settings from weekly_workflow_settings for the current day.
    SIMPLIFIED: Returns execution configuration.
    """
    try:
        from datetime import date

        # Get current day
        current_day = date.today().strftime('%A').lower()

        # Get weekly workflow settings
        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        if not weekly_settings:
            logger.warning(f"No weekly_workflow_settings found in database")
            return None

        # Get day-specific configuration
        day_config = weekly_settings.get(current_day, {})

        if not day_config:
            logger.warning(f"No configuration found for {current_day}")
            return None

        # If specific setting requested, return it
        if setting_key:
            return day_config.get(setting_key, default)

        # Return execution-related settings
        execution_settings = {
            'execution_date': date.today().isoformat(),
            'destination_category': day_config.get('destination_category', ''),
            'workflow_type_name': day_config.get('workflow_type_name', ''),
            'collection_name': day_config.get('collection_name', ''),
            'max_workflows': day_config.get('max_workflows', 50),
            'gap_seconds': day_config.get('gap_seconds', 30),
            'enabled': day_config.get('enabled', True),
            'day': current_day,
            'updated_at': day_config.get('updated_at', '')
        }

        return execution_settings

    except Exception as e:
        logger.error(f"Error getting execution setting: {e}")
        return None


# File: streamlit/ui/settings/settings_manager.py
# Replace the existing update_execution_config function with this

def update_execution_config(settings):
    """
    Update execution configuration in weekly_workflow_settings for current day.
    ✅ FIXED: Now saves ALL required fields for Node.js orchestrator.
    """
    try:
        from datetime import date

        # Get current day
        current_day = date.today().strftime('%A').lower()

        # Get existing weekly workflow settings
        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        if not weekly_settings:
            weekly_settings = {}

        # Get current day settings or create new
        day_settings = weekly_settings.get(current_day, {})

        # ✅ UPDATE: Save ALL execution configuration fields
        day_settings.update({
            # ✅ REQUIRED FIELDS FOR NODE.JS ORCHESTRATOR
            'destination_category': settings.get('destination_category', ''),
            'workflow_type_name': settings.get('workflow_type_name', ''),
            'collection_name': settings.get('collection_name', ''),
            'max_workflows': settings.get('max_workflows', 50),
            'gap_seconds': settings.get('gap_seconds', 30),

            # ✅ ALSO UPDATE LEGACY FIELDS (for backward compatibility)
            'workflow_type': settings.get('workflow_type_name', settings.get('workflow_type', 'messages')),
            'gap_between_workflows': settings.get('gap_seconds', 30),  # Store gap_seconds as gap_between_workflows too

            # Metadata
            'execution_date': settings.get('execution_date', date.today().isoformat()),
            'day_name': current_day.capitalize(),
            'day_key': current_day,
            'updated_at': settings.get('updated_at', datetime.now().isoformat())
        })

        # Keep existing settings that we don't update here
        if 'enabled' not in day_settings:
            day_settings['enabled'] = True

        # Keep other fields if they exist (for filtering config)
        for field in ['links_to_filter', 'workflows_to_process', 'morning_time',
                      'evening_time', 'content_amount', 'time_limit']:
            if field in weekly_settings.get(current_day, {}):
                if field not in day_settings:
                    day_settings[field] = weekly_settings[current_day][field]

        # Update weekly settings with modified day settings
        weekly_settings[current_day] = day_settings

        # Save back to system settings
        update_system_setting('weekly_workflow_settings', weekly_settings)

        logger.info(f"✅ Execution configuration updated for {current_day}")
        logger.info(f"   Category: {settings.get('destination_category')}")
        logger.info(f"   Type: {settings.get('workflow_type_name')}")
        logger.info(f"   Collection: {settings.get('collection_name')}")
        logger.info(f"   Max workflows: {settings.get('max_workflows')}")
        logger.info(f"   Gap: {settings.get('gap_seconds')}s")

        return True

    except Exception as e:
        logger.error(f"Error updating execution configuration: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False
def get_cached_prompt_types():
    """
    Get prompt types from MongoDB cache, with fallback to Postgres.
    NO FALLBACKS - fails if types cannot be retrieved.

    Raises:
        RuntimeError: If no prompt types can be retrieved
    """
    try:
        # Try to get from MongoDB cache first
        cached_types = get_system_setting('available_prompt_types', None)

        if cached_types and isinstance(cached_types, list) and cached_types:
            logger.info(f"Using {len(cached_types)} cached prompt types")
            return cached_types

        # Cache miss or empty - fetch from Postgres and cache
        logger.info("Cache miss - fetching prompt types from Postgres")
        fresh_types = sync_postgres_prompt_types_to_settings()

        if not fresh_types:
            raise RuntimeError("CRITICAL: No prompt types retrieved from database")

        return fresh_types

    except Exception as e:
        error_msg = f"CRITICAL: Cannot retrieve prompt types: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e


def get_enabled_workflow_types(settings=None):
    """
    Get list of enabled workflow types from custom prompt types.
    NO FALLBACKS - fails if no types found.
    """
    try:
        # First check daily config (which queries database)
        daily_config = get_daily_content_config()

        if 'all_custom_types' in daily_config and daily_config['all_custom_types']:
            # Return enabled custom types
            enabled = [wf_type for wf_type, is_enabled in daily_config['enabled_types'].items() if is_enabled]

            if not enabled:
                raise RuntimeError("CRITICAL: No workflow types are enabled in daily config")

            logger.info(f"Using custom workflow types: {enabled}")
            return enabled

        # Query database directly if no daily config
        if settings is None:
            custom_types = get_postgres_prompt_types()  # This will raise if fails

            if not custom_types:
                raise RuntimeError("CRITICAL: No prompt types found in database")

            # Convert list to dict format expected by rest of code
            settings = {ptype: True for ptype in custom_types}

        workflow_types = settings if isinstance(settings, dict) else {}

        enabled = [wf_type for wf_type, enabled in workflow_types.items() if enabled]

        if not enabled:
            raise RuntimeError("CRITICAL: No workflow types are enabled")

        return enabled

    except Exception as e:
        error_msg = f"CRITICAL: Cannot get enabled workflow types: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e


def get_workflow_type_priority_order(settings=None):
    """
    Get workflow type priority order from custom types.
    NO FALLBACKS - fails if no order found.
    """
    try:
        # Check daily config for custom types
        daily_config = get_daily_content_config()

        if 'all_custom_types' in daily_config and daily_config['all_custom_types']:
            order = daily_config['all_custom_types']

            if not order:
                raise RuntimeError("CRITICAL: Priority order is empty")

            logger.info(f"Using custom type priority order: {order}")
            return order

        # Query database directly
        if settings is None:
            custom_types = get_postgres_prompt_types()  # This will raise if fails

            if not custom_types:
                raise RuntimeError("CRITICAL: No prompt types found for priority order")

            return custom_types

        order = settings if isinstance(settings, list) else []

        if not order:
            raise RuntimeError("CRITICAL: Priority order is empty")

        return order

    except Exception as e:
        error_msg = f"CRITICAL: Cannot get priority order: {e}"
        logger.error(error_msg)
        raise RuntimeError(error_msg) from e


# Add this to your settings_manager.py file in the _get_default_for_key function




def get_daily_content_config() -> Dict[str, Any]:
    """
    Get today's content configuration from weekly settings.
    Returns configuration with prompt_name support.
    """
    try:
        today = datetime.now()
        day_name = today.strftime('%A')  # e.g., "Wednesday"
        day_key = day_name.lower()  # e.g., "wednesday"

        logger.info(f"Getting content config for {day_name} ({day_key})")

        # Get weekly settings
        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        if not isinstance(weekly_settings, dict):
            logger.error(f"Invalid weekly_settings type: {type(weekly_settings)}")
            return {
                'enabled': False,
                'day_name': day_name,
                'day_key': day_key,
                'content_types': [],
                'error': 'Invalid settings format'
            }

        # Check if today has configuration
        if day_key not in weekly_settings:
            logger.warning(f"No configuration found for {day_key}")
            logger.info(f"Available days: {list(weekly_settings.keys())}")
            return {
                'enabled': False,
                'day_name': day_name,
                'day_key': day_key,
                'content_types': [],
                'message': f'No configuration for {day_name}'
            }

        day_config = weekly_settings[day_key]

        # Validate day configuration structure
        if not isinstance(day_config, dict):
            logger.error(f"Invalid day_config type for {day_key}: {type(day_config)}")
            return {
                'enabled': False,
                'day_name': day_name,
                'day_key': day_key,
                'content_types': [],
                'error': 'Invalid day configuration format'
            }

        # Get content types
        content_types = day_config.get('content_types', [])

        if not isinstance(content_types, list):
            logger.error(f"Invalid content_types type: {type(content_types)}")
            return {
                'enabled': False,
                'day_name': day_name,
                'day_key': day_key,
                'content_types': [],
                'error': 'Invalid content_types format'
            }

        # Validate each content type configuration
        valid_content_types = []

        for idx, ct in enumerate(content_types):
            if not isinstance(ct, dict):
                logger.warning(f"Skipping invalid content type at index {idx}: {type(ct)}")
                continue

            # Check required fields
            workflow_type = ct.get('workflow_type')
            content_name = ct.get('content_name')
            content_amount = ct.get('content_amount')
            prompt_name = ct.get('prompt_name')  # NEW: Get prompt name

            if not workflow_type:
                logger.warning(f"Content type {idx} missing workflow_type")
                continue

            if not content_name:
                logger.warning(f"Content type {idx} missing content_name")
                continue

            if not content_amount:
                logger.warning(f"Content type {idx} missing content_amount")
                continue

            # Prompt name is now required (but we'll warn if missing for backward compatibility)
            if not prompt_name:
                logger.warning(f"Content type {idx} missing prompt_name (required for new configs)")
                # For backward compatibility, we'll still include it but flag it
                ct['_missing_prompt_name'] = True

            # Add validated content type
            valid_content_types.append({
                'workflow_type': workflow_type,
                'prompt_name': prompt_name,  # NEW: Include prompt name
                'content_name': content_name,
                'content_amount': int(content_amount),
                '_original_index': idx
            })

            logger.info(f"✅ Valid content type {idx + 1}: {workflow_type} | Prompt: {prompt_name} | Name: {content_name}")

        if not valid_content_types:
            logger.warning(f"No valid content types found for {day_key}")
            return {
                'enabled': False,
                'day_name': day_name,
                'day_key': day_key,
                'content_types': [],
                'message': 'No valid content types configured'
            }

        # Build response
        response = {
            'enabled': True,
            'day_name': day_name,
            'day_key': day_key,
            'content_types': valid_content_types,
            'config_date': day_config.get('config_date'),
            'created_at': day_config.get('created_at'),
            'updated_at': day_config.get('updated_at')
        }

        logger.info(f"✅ Successfully loaded config for {day_name}: {len(valid_content_types)} content type(s)")

        return response

    except Exception as e:
        logger.error(f"Error getting daily content config: {e}")
        import traceback
        logger.error(traceback.format_exc())

        return {
            'enabled': False,
            'day_name': datetime.now().strftime('%A'),
            'day_key': datetime.now().strftime('%A').lower(),
            'content_types': [],
            'error': str(e)
        }


def validate_daily_config_for_generation() -> tuple[bool, str]:
    """
    Validate that today's configuration is ready for content generation.
    Returns (is_valid, message)
    """
    try:
        config = get_daily_content_config()

        if not config.get('enabled'):
            return False, f"Configuration disabled: {config.get('message', 'Unknown reason')}"

        content_types = config.get('content_types', [])

        if not content_types:
            return False, "No content types configured"

        # Check for missing prompt names
        missing_prompt_names = []
        for idx, ct in enumerate(content_types):
            if ct.get('_missing_prompt_name'):
                missing_prompt_names.append(f"TYPE{idx + 1} ({ct['workflow_type']})")

        if missing_prompt_names:
            return False, f"Missing prompt names for: {', '.join(missing_prompt_names)}"

        # Check that prompts exist
        from core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                missing_prompts = []

                for ct in content_types:
                    cursor.execute("""
                        SELECT COUNT(*) as count
                        FROM prompts
                        WHERE prompt_type = %s
                          AND name = %s
                          AND is_active = TRUE
                    """, (ct['workflow_type'], ct['prompt_name']))

                    result = cursor.fetchone()
                    if result['count'] == 0:
                        missing_prompts.append(f"{ct['workflow_type']}/{ct['prompt_name']}")

                if missing_prompts:
                    return False, f"Prompts not found: {', '.join(missing_prompts)}"

        return True, f"Configuration valid: {len(content_types)} content type(s) ready"

    except Exception as e:
        return False, f"Validation error: {str(e)}"


def validate_workflow_type_settings(workflow_types):
    """
    Validate workflow type settings.
    NO FALLBACKS - raises error if validation fails.
    """
    # Get current types from database (will raise if fails)
    valid_types = get_postgres_prompt_types()

    if not valid_types:
        raise ValueError("CRITICAL: No prompt types found in database for validation")

    # Check that at least one type is enabled
    enabled_types = [wf_type for wf_type in workflow_types if workflow_types.get(wf_type, False)]

    if not enabled_types:
        raise ValueError("CRITICAL: At least one workflow type must be enabled")

    # Validate all types are in database
    invalid_types = [wf_type for wf_type in workflow_types if wf_type not in valid_types]

    if invalid_types:
        raise ValueError(
            f"CRITICAL: Invalid workflow types not found in database: {invalid_types}. "
            f"Valid types: {valid_types}"
        )

    return True


def validate_priority_order_settings(priority_order, enabled_types):
    """
    Validate priority order.
    NO FALLBACKS - raises error if validation fails.
    """
    if not isinstance(priority_order, list):
        raise ValueError("CRITICAL: Priority order must be a list")

    if not priority_order:
        raise ValueError("CRITICAL: Priority order cannot be empty")

    # Get valid types from database (will raise if fails)
    valid_types = get_postgres_prompt_types()

    if not valid_types:
        raise ValueError("CRITICAL: No prompt types found in database for validation")

    # Validate all items in priority order are valid
    invalid_types = [wf_type for wf_type in priority_order if wf_type not in valid_types]

    if invalid_types:
        raise ValueError(
            f"CRITICAL: Invalid workflow types in priority order: {invalid_types}. "
            f"Valid types: {valid_types}"
        )

    # Check enabled types are in priority order
    enabled_type_names = [wf_type for wf_type, enabled in enabled_types.items() if enabled]

    missing_types = [wf_type for wf_type in enabled_type_names if wf_type not in priority_order]

    if missing_types:
        raise ValueError(
            f"CRITICAL: Enabled types not in priority order: {missing_types}"
        )

    return True
def get_postgres_accounts_with_prompt_type(workflow_type: str) -> list:
    """
    Get accounts that have active prompts for a specific custom type.

    Args:
        workflow_type: The prompt type to filter by

    Returns:
        list: List of account dictionaries with prompt information
    """
    try:
        from core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT DISTINCT
                        a.account_id,
                        a.username,
                        a.profile_id,
                        p.prompt_id,
                        p.name as prompt_name,
                        p.prompt_type
                    FROM accounts a
                    INNER JOIN prompts p ON a.account_id = p.account_id
                    WHERE p.prompt_type = %s
                      AND p.is_active = TRUE
                    ORDER BY a.username
                """, (workflow_type,))

                results = cursor.fetchall()
                accounts = [dict(row) for row in results]

                logger.info(f"Found {len(accounts)} accounts with prompt type '{workflow_type}'")
                return accounts

    except Exception as e:
        logger.error(f"Error fetching accounts for prompt type '{workflow_type}': {e}")
        return []


def get_postgres_prompt_type_statistics() -> dict:
    """
    Get statistics about custom prompt types from Postgres.

    Returns:
        dict: Statistics per prompt type
    """
    try:
        from core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Get counts per custom type
                cursor.execute("""
                    SELECT
                        prompt_type,
                        COUNT(*) as total_prompts,
                        COUNT(CASE WHEN is_active = TRUE THEN 1 END) as active_prompts,
                        COUNT(DISTINCT account_id) as accounts_with_type
                    FROM prompts
                    WHERE prompt_type IS NOT NULL
                    GROUP BY prompt_type
                    ORDER BY prompt_type
                """)

                results = cursor.fetchall()

                stats = {}
                for row in results:
                    stats[row['prompt_type']] = {
                        'total_prompts': row['total_prompts'],
                        'active_prompts': row['active_prompts'],
                        'accounts_with_type': row['accounts_with_type']
                    }

                logger.info(f"Retrieved statistics for {len(stats)} prompt types")
                return stats

    except Exception as e:
        logger.error(f"Error getting prompt type statistics: {e}")
        return {}


def validate_postgres_prompt_type(workflow_type: str) -> bool:
    """
    Validate that a workflow type exists in the prompts table.

    Args:
        workflow_type: The prompt type to validate

    Returns:
        bool: True if valid, False otherwise
    """
    try:
        all_types = get_postgres_prompt_types()

        if workflow_type not in all_types:
            logger.warning(f"Prompt type '{workflow_type}' not found in database")
            return False

        return True

    except Exception as e:
        logger.error(f"Error validating prompt type: {e}")
        return False


def sync_postgres_prompt_types_to_settings():
    """
    Sync available prompt types from Postgres to MongoDB system settings.
    This allows easy access without repeated database queries.

    Returns:
        list: Synced prompt types
    """
    try:
        all_prompt_types = get_postgres_prompt_types()

        # Update system setting with current prompt types
        update_system_setting('available_prompt_types', all_prompt_types)

        logger.info(f"Synced {len(all_prompt_types)} prompt types to system settings")
        return all_prompt_types

    except Exception as e:
        logger.error(f"Error syncing prompt types to settings: {e}")
        return []





# ==============================================================================
# UPDATED VERSIONS OF EXISTING FUNCTIONS TO USE CENTRALIZED POSTGRES ACCESS
# ==============================================================================

def get_all_custom_prompt_types_from_db():
    """
    Get all unique custom prompt types from the prompts table.
    UPDATED: Now uses centralized Postgres function.
    """
    return get_postgres_prompt_types()


def get_accounts_with_custom_prompt_type(workflow_type: str) -> list:
    """
    Get accounts that have active prompts for a specific custom type.
    UPDATED: Now uses centralized Postgres function.
    """
    return get_postgres_accounts_with_prompt_type(workflow_type)


def get_custom_type_statistics() -> dict:
    """
    Get statistics about custom prompt types.
    UPDATED: Now uses centralized Postgres function.
    """
    return get_postgres_prompt_type_statistics()


def validate_custom_workflow_type(workflow_type: str) -> bool:
    """
    Validate that a workflow type exists in the prompts table.
    UPDATED: Now uses centralized Postgres function.
    """
    return validate_postgres_prompt_type(workflow_type)


def sync_custom_types_to_settings():
    """
    Sync available custom prompt types to system settings for easy access.
    UPDATED: Now uses centralized Postgres function.
    """
    return sync_postgres_prompt_types_to_settings()
def clear_all_workflow_data():
    """Completely clear all workflow execution data from all collections."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        db = client['messages_db']

        reset_operations = []

        # 1. Reset content_workflow_links
        content_reset = db.content_workflow_links.update_many(
            {},
            {'$set': {
                'executed': False,
                'execution_success': None,
                'executed_at': None,
                'success': False,
                'status': 'pending',
                'reset_at': datetime.now(),
                'reset_reason': 'complete_system_reset'
            }}
        )
        reset_operations.append(('content_workflow_links', content_reset.modified_count))

        # 2. Reset workflow_executions
        workflow_reset = db.workflow_executions.update_many(
            {},
            {'$set': {
                'executed': False,
                'execution_success': None,
                'executed_at': None,
                'success': False,
                'status': 'pending',
                'reset_at': datetime.now(),
                'reset_reason': 'complete_system_reset'
            }}
        )
        reset_operations.append(('workflow_executions', workflow_reset.modified_count))

        # 3. Clear all analytics
        daily_delete = db.daily_workflow_analytics.delete_many({})
        reset_operations.append(('daily_workflow_analytics', daily_delete.deleted_count))

        # 4. Clear sessions
        session_delete = db.browser_sessions.delete_many({})
        reset_operations.append(('browser_sessions', session_delete.deleted_count))

        total_affected = sum(count for _, count in reset_operations)

        return {
            'success': True,
            'operations': reset_operations,
            'total_affected': total_affected,
            'reset_at': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error in complete workflow reset: {e}")
        return {
            'success': False,
            'error': str(e),
            'reset_at': datetime.now().isoformat()
        }
    finally:
        if client:
            client.close()

def reset_workflow_execution_status(workflow_types=None, reset_all=False):
    """Reset execution status for workflows to make them eligible again - FIXED VERSION."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Get enabled workflow types if not specified
        if workflow_types is None and not reset_all:
            enabled_types = get_system_setting('extraction_workflow_types',
                                              {'replies': True, 'messages': True, 'retweets': True})
            workflow_types = [wf_type for wf_type, enabled in enabled_types.items() if enabled]

        # Build filter - FIXED to be more comprehensive
        reset_filter = {}

        if reset_all:
            reset_filter = {}  # Reset all workflows
        elif workflow_types:
            if isinstance(workflow_types, str):
                workflow_types = [workflow_types]
            reset_filter['content_type'] = {'$in': workflow_types}

        # FIXED: More comprehensive reset data
        reset_data = {
            'executed': False,
            'execution_success': None,
            'executed_at': None,
            'execution_error': None,
            'error_category': None,
            'final_result': None,
            'execution_time': None,
            'steps_taken': None,
            'session_id': None,
            'video_recording_session_id': None,
            'video_recording_enabled': None,
            'recording_status': None,
            'dag_run_id': None,
            'postgres_account_id': None,
            'account_username': None,
            'profile_id': None,
            'extension_id': None,
            'profile_specific_execution': None,
            'execution_mode': None,
            'automaIntegration': None,
            'extensionContextMethod': None,
            'extensionReady': None,
            # Reset tracking
            'reset_at': datetime.now(),
            'reset_reason': 'manual_reset_for_re_execution',
            'reset_by': 'streamlit_ui',
            'updated_at': datetime.now()
        }

        # FIXED: Use correct collection
        collection_name = 'workflow_executions'
        result = db[collection_name].update_many(reset_filter if reset_filter else {}, {'$set': reset_data})

        # FIXED: Clear daily limits
        if result.modified_count > 0:
            try:
                today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                db.daily_workflow_analytics.delete_many({'date': today})
                logger.info("Cleared daily workflow analytics to reset daily limits")
            except Exception as e:
                logger.warning(f"Failed to clear daily analytics: {e}")

        logger.info(f"Reset {result.modified_count} workflows for re-execution")
        logger.info(f"Reset applied to workflow types: {workflow_types if workflow_types else 'all'}")

        return {
            'success': True,
            'reset_count': result.modified_count,
            'filter_used': str(reset_filter),
            'workflow_types_reset': workflow_types if workflow_types else 'all',
            'reset_at': datetime.now().isoformat(),
            'daily_limits_cleared': True
        }

    except Exception as e:
        logger.error(f"Error resetting workflow execution status: {e}")
        return {
            'success': False,
            'reset_count': 0,
            'error': str(e),
            'workflow_types_reset': workflow_types if workflow_types else 'all',
            'reset_at': datetime.now().isoformat()
        }
    finally:
        if client:
            client.close()


def clear_all_execution_tracking():
    """Clear all execution tracking data to completely reset the system."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Collections to clear/reset
        reset_operations = []

        # 1. Reset all workflow executions
        workflow_reset = db.workflow_executions.update_many(
            {},  # All documents
            {'$set': {
                'executed': False,
                'execution_success': None,
                'executed_at': None,
                'execution_error': None,
                'error_category': None,
                'final_result': None,
                'execution_time': None,
                'steps_taken': None,
                'session_id': None,
                'video_recording_session_id': None,
                'video_recording_enabled': None,
                'recording_status': None,
                'dag_run_id': None,
                'postgres_account_id': None,
                'account_username': None,
                'profile_id': None,
                'extension_id': None,
                'profile_specific_execution': None,
                'execution_mode': None,
                'reset_at': datetime.now(),
                'reset_reason': 'complete_system_reset',
                'updated_at': datetime.now()
            }}
        )
        reset_operations.append(('workflow_executions', workflow_reset.modified_count))

        # 2. Clear daily analytics
        daily_analytics_delete = db.daily_workflow_analytics.delete_many({})
        reset_operations.append(('daily_workflow_analytics', daily_analytics_delete.deleted_count))

        # 3. Clear browser sessions
        session_delete = db.browser_sessions.delete_many({})
        reset_operations.append(('browser_sessions', session_delete.deleted_count))

        # 4. Clear account profile assignments (optional)
        assignment_delete = db.account_profile_assignments.delete_many({})
        reset_operations.append(('account_profile_assignments', assignment_delete.deleted_count))

        # 5. Reset account workflow statistics
        account_reset = db.accounts.update_many(
            {},
            {'$set': {
                'total_replies_processed': 0,
                'total_messages_processed': 0,
                'total_retweets_processed': 0,
                'last_workflow_sync': None,
                'updated_at': datetime.now()
            }}
        )
        reset_operations.append(('accounts_stats_reset', account_reset.modified_count))

        logger.info("Complete execution tracking reset completed")
        for collection, count in reset_operations:
            logger.info(f"  {collection}: {count} records affected")

        return {
            'success': True,
            'operations': reset_operations,
            'total_affected': sum(count for _, count in reset_operations),
            'reset_at': datetime.now().isoformat(),
            'reset_reason': 'complete_system_reset'
        }

    except Exception as e:
        logger.error(f"Error in complete execution tracking reset: {e}")
        return {
            'success': False,
            'error': str(e),
            'reset_at': datetime.now().isoformat()
        }
    finally:
        if client:
            client.close()


def get_workflow_execution_statistics():
    """Get workflow execution statistics with proper error handling - UPDATED VERSION."""
    client = None

    try:
        client = MongoClient(os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Get enabled workflow types
        enabled_types = get_system_setting('extraction_workflow_types',
                                          {'replies': True, 'messages': True, 'retweets': True})
        enabled_type_names = [wf_type for wf_type, enabled in enabled_types.items() if enabled]

        # FIXED: Use correct collection name based on your schema
        collection_name = 'workflow_executions'

        # Get execution statistics
        total_workflows = db[collection_name].count_documents({})

        # Filter for enabled workflow types
        type_filter = {'content_type': {'$in': enabled_type_names}} if enabled_type_names else {}

        # UPDATED: More accurate eligible count
        eligible_workflows = db[collection_name].count_documents({
            **type_filter,
            'has_link': True,
            'executed': False,
            'postgres_account_id': {'$exists': True, '$ne': None}  # Has account assignment
        })

        executed_workflows = db[collection_name].count_documents({
            **type_filter,
            'executed': True
        })

        successful_workflows = db[collection_name].count_documents({
            **type_filter,
            'executed': True,
            'execution_success': True
        })

        failed_workflows = db[collection_name].count_documents({
            **type_filter,
            'executed': True,
            '$or': [
                {'execution_success': False},
                {'execution_success': None},
                {'execution_success': {'$exists': False}}
            ]
        })

        # Get statistics by workflow type
        type_stats = {}
        for workflow_type in enabled_type_names:
            type_stats[workflow_type] = {
                'total': db[collection_name].count_documents({'content_type': workflow_type}),
                'eligible': db[collection_name].count_documents({
                    'content_type': workflow_type,
                    'has_link': True,
                    'executed': False,
                    'postgres_account_id': {'$exists': True, '$ne': None}
                }),
                'executed': db[collection_name].count_documents({
                    'content_type': workflow_type,
                    'executed': True
                }),
                'successful': db[collection_name].count_documents({
                    'content_type': workflow_type,
                    'executed': True,
                    'execution_success': True
                }),
                'failed': db[collection_name].count_documents({
                    'content_type': workflow_type,
                    'executed': True,
                    '$or': [
                        {'execution_success': False},
                        {'execution_success': None},
                        {'execution_success': {'$exists': False}}
                    ]
                }),
                'enabled': enabled_types.get(workflow_type, False)
            }

        # Check if any resets have occurred today
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        reset_count_today = db[collection_name].count_documents({
            'reset_at': {'$gte': today},
            'reset_reason': {'$exists': True}
        })

        statistics = {
            'total_workflows': total_workflows,
            'eligible_workflows': eligible_workflows,
            'executed_workflows': executed_workflows,
            'successful_workflows': successful_workflows,
            'failed_workflows': failed_workflows,
            'success_rate': round((successful_workflows / max(executed_workflows, 1)) * 100, 2),
            'eligibility_rate': round((eligible_workflows / max(total_workflows, 1)) * 100, 2),
            'type_statistics': type_stats,
            'enabled_workflow_types': enabled_types,
            'enabled_type_names': enabled_type_names,
            'reset_count_today': reset_count_today,
            'collection_used': collection_name,
            'updated_at': datetime.now().isoformat()
        }

        return statistics

    except Exception as e:
        logger.error(f"Error getting workflow execution statistics: {e}")
        return {
            'total_workflows': 0,
            'eligible_workflows': 0,
            'executed_workflows': 0,
            'successful_workflows': 0,
            'failed_workflows': 0,
            'success_rate': 0.0,
            'eligibility_rate': 0.0,
            'type_statistics': {},
            'enabled_workflow_types': {'replies': True, 'messages': True, 'retweets': True},
            'enabled_type_names': ['replies', 'messages', 'retweets'],
            'reset_count_today': 0,
            'error': str(e),
            'updated_at': datetime.now().isoformat()
        }
    finally:
        if client:
            client.close()

# File: streamlit/ui/settings/settings_manager.py
# FIXED VERSION - Removes all hardcoded 'replies', 'messages', 'retweets'

from datetime import datetime
import logging

logger = logging.getLogger(__name__)













def validate_workflow_type_settings(workflow_types):
    """
    Validate workflow type settings - FIXED to use database types.
    """
    # Get current types from database
    valid_types = get_postgres_prompt_types()

    if not valid_types:
        raise ValueError("No prompt types found in database")

    # Check that at least one type is enabled
    enabled_types = [wf_type for wf_type in workflow_types if workflow_types.get(wf_type, False)]

    if not enabled_types:
        raise ValueError("At least one workflow type must be enabled")

    # Validate all types are in database
    for wf_type in workflow_types:
        if wf_type not in valid_types:
            raise ValueError(f"Invalid workflow type '{wf_type}' - not found in database")

    return True





# SUMMARY OF CHANGES:
# 1. Removed ALL hardcoded ['replies', 'messages', 'retweets'] defaults
# 2. All functions now query database via get_postgres_prompt_types()
# 3. Functions return empty lists/dicts if no types found (no fake defaults)
# 4. Validation functions check against database types, not hardcoded list
# 5. Daily config properly validates workflow types exist in database

# settings_manager.py - ADD THESE METHODS

# ----------------------------------------------------------------------
# FILTER WORDS MANAGEMENT
# ----------------------------------------------------------------------

"""
FIXED METHODS FOR settings_manager.py
Replace these 3 methods in your existing settings_manager.py:
  1. _get_default_for_key()
  2. get_system_setting()  — only the extraction_processing_settings block changes
  3. get_filter_words_list()

Search for: advert,advertisement,sponsored
Replace all 3 occurrences with the correct words below.
"""

# ============================================================================
# CORRECT FILTER WORDS — single source of truth
# ============================================================================
CORRECT_FILTER_WORDS = (
    'touchofm_,Eileevalencia,Record_spot1,brill_writers,essayzpro,'
    'primewriters23a,essaygirl01,EssayNasrah,Sharifwriter1,'
    'EssaysAstute,queentinabrown,analytics'
)


# ============================================================================
# METHOD 1 — _get_default_for_key()
# ============================================================================

def _get_default_for_key(key, fallback_default):
    """
    Helper function to return appropriate defaults when settings don't exist.
    GRACEFUL FALLBACK: Uses fallback_default if database query fails.
    """
    try:
        # Try to get custom types from database, but handle gracefully if none exist
        try:
            all_custom_types = get_postgres_prompt_types()
        except Exception as db_error:
            logger.warning(f"Database query failed for default '{key}': {db_error}")
            # Return a sensible fallback instead of crashing
            if key in ['extraction_workflow_types', 'extraction_priority_order',
                      'available_custom_prompt_types', 'extraction_processing_settings']:
                # Return empty defaults for workflow-related settings
                if key == 'extraction_workflow_types':
                    return {}
                elif key == 'extraction_priority_order':
                    return []
                elif key == 'available_custom_prompt_types':
                    return []
                elif key == 'extraction_processing_settings':
                    return {
                        'content_to_filter': 19,
                        'words_to_filter': CORRECT_FILTER_WORDS,
                        'gap_between_workflows': 0.25,
                        'gap_between_workflows_seconds': 15,
                        'workflow_types': {},
                        'priority_order': []
                    }
                # ============================================================
                # ADD THIS BLOCK for extraction_schedule_settings
                # ============================================================
                elif key == 'extraction_schedule_settings':
                    return {
                        'schedule_interval': '0 * * * *',
                        'schedule_description': 'Every hour',
                        'updated_at': datetime.now().isoformat()
                    }
            # For unknown keys, return the fallback_default
            return fallback_default

        if not all_custom_types:
            # Database is accessible but no prompts exist
            logger.warning(f"No prompt types found in database for default '{key}'")

            # Return appropriate fallbacks for workflow-related settings
            if key in ['extraction_workflow_types', 'extraction_priority_order',
                      'available_custom_prompt_types']:
                return fallback_default if fallback_default is not None else []

            # ================================================================
            # ADD THIS BLOCK for extraction_schedule_settings
            # ================================================================
            elif key == 'extraction_schedule_settings':
                return {
                    'schedule_interval': '0 * * * *',
                    'schedule_description': 'Every hour',
                    'updated_at': datetime.now().isoformat()
                }

            # For weekly settings, use a generic fallback
            if key == 'weekly_workflow_settings':
                return {
                    'monday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                              'morning_time': '09:00', 'evening_time': '18:00',
                              'workflow_type': 'generic', 'content_amount': 5,
                              'gap_between_workflows': 300, 'time_limit': 2},
                    'tuesday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                               'morning_time': '09:00', 'evening_time': '18:00',
                               'workflow_type': 'generic', 'content_amount': 5,
                               'gap_between_workflows': 300, 'time_limit': 2},
                    'wednesday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                                 'morning_time': '09:00', 'evening_time': '18:00',
                                 'workflow_type': 'generic', 'content_amount': 5,
                                 'gap_between_workflows': 300, 'time_limit': 2},
                    'thursday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                                'morning_time': '09:00', 'evening_time': '18:00',
                                'workflow_type': 'generic', 'content_amount': 5,
                                'gap_between_workflows': 300, 'time_limit': 2},
                    'friday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                              'morning_time': '09:00', 'evening_time': '18:00',
                              'workflow_type': 'generic', 'content_amount': 5,
                              'gap_between_workflows': 300, 'time_limit': 2},
                    'saturday': {'enabled': True, 'links_to_filter': 8, 'workflows_to_process': 12,
                                'morning_time': '10:00', 'evening_time': '17:00',
                                'workflow_type': 'generic', 'content_amount': 5,
                                'gap_between_workflows': 600, 'time_limit': 2},
                    'sunday': {'enabled': True, 'links_to_filter': 8, 'workflows_to_process': 12,
                              'morning_time': '10:00', 'evening_time': '17:00',
                              'workflow_type': 'generic', 'content_amount': 5,
                              'gap_between_workflows': 600, 'time_limit': 2}
                }

            return fallback_default

        # If we have custom types, proceed with original logic
        default_workflow_type = all_custom_types[0]

        if key == 'weekly_workflow_settings':
            return {
                'monday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                          'morning_time': '09:00', 'evening_time': '18:00',
                          'workflow_type': default_workflow_type, 'content_amount': 5,
                          'gap_between_workflows': 300, 'time_limit': 2},
                'tuesday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                           'morning_time': '09:00', 'evening_time': '18:00',
                           'workflow_type': default_workflow_type, 'content_amount': 5,
                           'gap_between_workflows': 300, 'time_limit': 2},
                'wednesday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                             'morning_time': '09:00', 'evening_time': '18:00',
                             'workflow_type': default_workflow_type, 'content_amount': 5,
                             'gap_between_workflows': 300, 'time_limit': 2},
                'thursday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                            'morning_time': '09:00', 'evening_time': '18:00',
                            'workflow_type': default_workflow_type, 'content_amount': 5,
                            'gap_between_workflows': 300, 'time_limit': 2},
                'friday': {'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                          'morning_time': '09:00', 'evening_time': '18:00',
                          'workflow_type': default_workflow_type, 'content_amount': 5,
                          'gap_between_workflows': 300, 'time_limit': 2},
                'saturday': {'enabled': True, 'links_to_filter': 8, 'workflows_to_process': 12,
                            'morning_time': '10:00', 'evening_time': '17:00',
                            'workflow_type': default_workflow_type, 'content_amount': 5,
                            'gap_between_workflows': 600, 'time_limit': 2},
                'sunday': {'enabled': True, 'links_to_filter': 8, 'workflows_to_process': 12,
                          'morning_time': '10:00', 'evening_time': '17:00',
                          'workflow_type': default_workflow_type, 'content_amount': 5,
                          'gap_between_workflows': 600, 'time_limit': 2}
            }

        elif key == 'workflow_strategy_settings':
            return get_single_workflow_settings()

        # ================================================================
        # ADD THIS BLOCK for extraction_schedule_settings
        # ================================================================
        elif key == 'extraction_schedule_settings':
            return {
                'schedule_interval': '0 * * * *',
                'schedule_description': 'Every hour',
                'updated_at': datetime.now().isoformat()
            }

        elif key == 'extraction_workflow_types':
            return {ptype: True for ptype in all_custom_types}

        elif key == 'extraction_priority_order':
            return all_custom_types

        elif key == 'session_management_settings':
            return {
                'current_profile_id': 'profile_default_001',
                'current_session_id': None,
                'current_extension_id': 'ext_automa_v2_001',
                'auto_create_sessions': True,
                'auto_assign_profiles': True,
                'session_timeout_minutes': 30,
                'track_session_performance': True
            }

        elif key == 'execution_limits_settings':
            return {
                'workflow_execution_limit': 50,
                'execution_time_limit_hours': 2.0,
                'daily_execution_limit': 200,
                'hourly_execution_limit': 25,
                'reset_limits_daily': True,
                'reset_limits_hourly': True,
                'enforce_limits': True
            }

        elif key == 'extraction_processing_settings':
            return {
                'content_to_filter': 19,
                'words_to_filter': CORRECT_FILTER_WORDS,
                'gap_between_workflows': 0.25,
                'gap_between_workflows_seconds': 15,
                'workflow_types': {ptype: True for ptype in all_custom_types},
                'priority_order': all_custom_types
            }

        elif key == 'filter_links_execution_settings':
            return {
                'selected_dag': 'report_with_workflows',
                'trigger_executor': False,
                'trigger_report': False,
                'trigger_report_with_workflows': True,
                'updated_at': datetime.now().isoformat()
            }

        elif key == 'available_custom_prompt_types':
            return all_custom_types

        elif key == 'workflow_categories':
            logger.info("Initializing empty workflow_categories setting")
            return {}

        # For unknown keys, return the provided fallback
        return fallback_default

    except Exception as e:
        logger.error(f"Error getting default for '{key}': {e}")
        return fallback_default


# ============================================================================
# METHOD 2 — get_system_setting()
# Only the extraction_processing_settings block is changed.
# The rest of the method is identical to your original.
# ============================================================================

def get_system_setting(key, default=None):
    """Retrieve a system setting from MongoDB or return default."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']
        settings_doc = db['settings'].find_one({'category': 'system'})

        if settings_doc and 'settings' in settings_doc:
            retrieved_setting = settings_doc['settings'].get(key, default)

            # ================================================================
            # EXTRACTION SCHEDULE SETTINGS - NEW BLOCK TO ADD
            # ================================================================
            if key == 'extraction_schedule_settings':
                default_schedule = {
                    'schedule_interval': '0 * * * *',
                    'schedule_description': 'Every hour',
                    'updated_at': datetime.now().isoformat()
                }
                if isinstance(retrieved_setting, dict):
                    return retrieved_setting
                return default_schedule

            # ================================================================
            # WORKFLOW STRATEGY SETTINGS - Individual execution mode
            # ================================================================
            elif key == 'workflow_strategy_settings':
                merged_settings = get_single_workflow_settings()
                if isinstance(retrieved_setting, dict):
                    merged_settings.update(retrieved_setting)
                    merged_settings['batch_size'] = 1
                    merged_settings['max_workflows_per_run'] = 1
                    merged_settings['single_workflow_mode'] = True
                return merged_settings

            # ================================================================
            # WEEKLY WORKFLOW SETTINGS
            # ================================================================
            elif key == 'weekly_workflow_settings':
                all_custom_types = get_postgres_prompt_types()
                default_workflow_type = all_custom_types[0] if all_custom_types else 'messages'

                default_weekly_settings = {
                    'monday': {
                        'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                        'morning_time': '09:00', 'evening_time': '18:00',
                        'workflow_type': default_workflow_type, 'content_amount': 5,
                        'gap_between_workflows': 300, 'time_limit': 2
                    },
                    'tuesday': {
                        'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                        'morning_time': '09:00', 'evening_time': '18:00',
                        'workflow_type': default_workflow_type, 'content_amount': 5,
                        'gap_between_workflows': 300, 'time_limit': 2
                    },
                    'wednesday': {
                        'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                        'morning_time': '09:00', 'evening_time': '18:00',
                        'workflow_type': default_workflow_type, 'content_amount': 5,
                        'gap_between_workflows': 300, 'time_limit': 2
                    },
                    'thursday': {
                        'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                        'morning_time': '09:00', 'evening_time': '18:00',
                        'workflow_type': default_workflow_type, 'content_amount': 5,
                        'gap_between_workflows': 300, 'time_limit': 2
                    },
                    'friday': {
                        'enabled': True, 'links_to_filter': 10, 'workflows_to_process': 15,
                        'morning_time': '09:00', 'evening_time': '18:00',
                        'workflow_type': default_workflow_type, 'content_amount': 5,
                        'gap_between_workflows': 300, 'time_limit': 2
                    },
                    'saturday': {
                        'enabled': True, 'links_to_filter': 8, 'workflows_to_process': 12,
                        'morning_time': '10:00', 'evening_time': '17:00',
                        'workflow_type': default_workflow_type, 'content_amount': 5,
                        'gap_between_workflows': 600, 'time_limit': 2
                    },
                    'sunday': {
                        'enabled': True, 'links_to_filter': 8, 'workflows_to_process': 12,
                        'morning_time': '10:00', 'evening_time': '17:00',
                        'workflow_type': default_workflow_type, 'content_amount': 5,
                        'gap_between_workflows': 600, 'time_limit': 2
                    }
                }

                if isinstance(retrieved_setting, dict):
                    for day_key, day_settings in default_weekly_settings.items():
                        if day_key in retrieved_setting:
                            if 'workflows_to_process' not in retrieved_setting[day_key]:
                                retrieved_setting[day_key]['workflows_to_process'] = day_settings['workflows_to_process']
                            stored_type = retrieved_setting[day_key].get('workflow_type')
                            if not stored_type or (all_custom_types and stored_type not in all_custom_types):
                                retrieved_setting[day_key]['workflow_type'] = default_workflow_type
                                logger.warning(f"Invalid workflow_type for {day_key}, using '{default_workflow_type}'")
                            if day_key in ['saturday', 'sunday']:
                                retrieved_setting[day_key]['enabled'] = True
                        else:
                            retrieved_setting[day_key] = day_settings
                    return retrieved_setting

                return default_weekly_settings

            # ================================================================
            # EXTRACTION WORKFLOW TYPES
            # ================================================================
            elif key == 'extraction_workflow_types':
                all_custom_types = get_postgres_prompt_types()
                if not all_custom_types:
                    logger.warning("No prompt types found in database for extraction_workflow_types")
                    return {}
                default_types = {ptype: True for ptype in all_custom_types}
                if isinstance(retrieved_setting, dict):
                    valid_stored = {k: v for k, v in retrieved_setting.items() if k in all_custom_types}
                    for ptype in all_custom_types:
                        if ptype not in valid_stored:
                            valid_stored[ptype] = True
                    return valid_stored
                return default_types

            # ================================================================
            # EXTRACTION PRIORITY ORDER
            # ================================================================
            elif key == 'extraction_priority_order':
                all_custom_types = get_postgres_prompt_types()
                if not all_custom_types:
                    logger.warning("No prompt types found in database for priority order")
                    return []
                if isinstance(retrieved_setting, list) and retrieved_setting:
                    valid_order = [ptype for ptype in retrieved_setting if ptype in all_custom_types]
                    for ptype in all_custom_types:
                        if ptype not in valid_order:
                            valid_order.append(ptype)
                    return valid_order
                return all_custom_types

            # ================================================================
            # SESSION MANAGEMENT SETTINGS
            # ================================================================
            elif key == 'session_management_settings':
                default_session_settings = {
                    'current_profile_id': 'profile_default_001',
                    'current_session_id': None,
                    'current_extension_id': 'ext_automa_v2_001',
                    'auto_create_sessions': True,
                    'auto_assign_profiles': True,
                    'session_timeout_minutes': 30,
                    'track_session_performance': True
                }
                if isinstance(retrieved_setting, dict):
                    default_session_settings.update(retrieved_setting)
                return default_session_settings

            # ================================================================
            # EXECUTION LIMITS SETTINGS
            # ================================================================
            elif key == 'execution_limits_settings':
                default_limits_settings = {
                    'workflow_execution_limit': 50,
                    'execution_time_limit_hours': 2.0,
                    'daily_execution_limit': 200,
                    'hourly_execution_limit': 25,
                    'reset_limits_daily': True,
                    'reset_limits_hourly': True,
                    'enforce_limits': True
                }
                if isinstance(retrieved_setting, dict):
                    default_limits_settings.update(retrieved_setting)
                return default_limits_settings

            # ================================================================
            # EXTRACTION PROCESSING SETTINGS
            # ================================================================
            elif key == 'extraction_processing_settings':
                all_custom_types = get_postgres_prompt_types()

                default_extraction_settings = {
                    'content_to_filter': 19,
                    'words_to_filter': CORRECT_FILTER_WORDS,
                    'gap_between_workflows': 0.25,
                    'gap_between_workflows_seconds': 15,
                    'workflow_types': {ptype: True for ptype in all_custom_types} if all_custom_types else {},
                    'priority_order': all_custom_types if all_custom_types else []
                }

                if isinstance(retrieved_setting, dict):
                    merged_settings = default_extraction_settings.copy()
                    merged_settings.update(retrieved_setting)

                    if all_custom_types:
                        merged_settings['workflow_types'] = {ptype: True for ptype in all_custom_types}
                        merged_settings['priority_order'] = all_custom_types
                    else:
                        merged_settings['workflow_types'] = {}
                        merged_settings['priority_order'] = []

                    if 'gap_between_workflows' in retrieved_setting:
                        gap_minutes = retrieved_setting.get('gap_between_workflows', 0.25)
                        gap_seconds = int(gap_minutes * 60) if isinstance(gap_minutes, (int, float)) else 15
                        merged_settings['gap_between_workflows_seconds'] = max(gap_seconds, 5)
                        logger.info(f"Converted workflow gap: {gap_minutes} minutes = {gap_seconds} seconds")

                    if not merged_settings.get('words_to_filter', '').strip():
                        merged_settings['words_to_filter'] = CORRECT_FILTER_WORDS
                        logger.info("words_to_filter was empty — applying correct default filter words")

                    return merged_settings

                return default_extraction_settings

            # ================================================================
            # FILTER LINKS EXECUTION SETTINGS
            # ================================================================
            elif key == 'filter_links_execution_settings':
                default_filter_settings = {
                    'selected_dag': 'report_with_workflows',
                    'trigger_executor': False,
                    'trigger_report': False,
                    'trigger_report_with_workflows': True,
                    'updated_at': datetime.now().isoformat()
                }
                if isinstance(retrieved_setting, dict):
                    default_filter_settings.update(retrieved_setting)
                return default_filter_settings

            # ================================================================
            # AVAILABLE CUSTOM PROMPT TYPES (cached)
            # ================================================================
            elif key == 'available_custom_prompt_types':
                if isinstance(retrieved_setting, list) and retrieved_setting:
                    return retrieved_setting
                fresh_types = get_postgres_prompt_types()
                logger.info(f"Cache miss for available_custom_prompt_types, fetched {len(fresh_types)} from database")
                return fresh_types

            # ================================================================
            # Default case
            # ================================================================
            return retrieved_setting

        # Handle case with no settings document
        if key == 'extraction_schedule_settings':
            return {
                'schedule_interval': '0 * * * *',
                'schedule_description': 'Every hour',
                'updated_at': datetime.now().isoformat()
            }
        elif key == 'filter_links_execution_settings':
            return {
                'selected_dag': 'report_with_workflows',
                'trigger_executor': False,
                'trigger_report': False,
                'trigger_report_with_workflows': True,
                'updated_at': datetime.now().isoformat()
            }
        elif key == 'workflow_categories':
            return default if default is not None else {}

        return _get_default_for_key(key, default)

    except Exception as e:
        logger.warning(f"Error retrieving setting {key} from MongoDB: {e}")
        if key == 'extraction_schedule_settings':
            return {
                'schedule_interval': '0 * * * *',
                'schedule_description': 'Every hour',
                'updated_at': datetime.now().isoformat()
            }
        elif key == 'filter_links_execution_settings':
            return {
                'selected_dag': 'report_with_workflows',
                'trigger_executor': False,
                'trigger_report': False,
                'trigger_report_with_workflows': True,
                'updated_at': datetime.now().isoformat()
            }
        elif key == 'workflow_categories':
            return default if default is not None else {}
        return _get_default_for_key(key, default)

    finally:
        if client:
            client.close()



def update_extraction_setting(key, value):
    """
    Write an extraction setting under the **extraction** category.
    Used for extraction-specific settings separate from system settings.
    """
    client = None
    try:
        client = MongoClient(
            os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Validation for extraction_schedule_settings
        if key == 'extraction_schedule_settings':
            if not isinstance(value, dict):
                raise ValueError("extraction_schedule_settings must be a dict")
            if 'schedule_interval' not in value:
                raise ValueError("extraction_schedule_settings needs 'schedule_interval'")
            if 'schedule_description' not in value:
                value['schedule_description'] = 'Custom schedule'

        db.settings.update_one(
            {'category': 'extraction'},
            {'$set': {f'settings.{key}': value, 'updated_at': datetime.now()}},
            upsert=True
        )
        logger.info(f"✅ Extraction setting '{key}' saved successfully")
        return True
    except Exception as e:
        logger.error(f"❌ Failed to save extraction setting '{key}': {e}")
        return False
    finally:
        if client:
            client.close()


# ============================================================================
# METHOD 3 — get_filter_words_list()  ✅ FIXED
# ============================================================================
def get_extraction_setting(key, default=None):
    """
    Retrieve an extraction setting from the **extraction** category.
    Used for extraction-specific settings separate from system settings.
    """
    client = None
    try:
        client = MongoClient(
            os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        doc = db.settings.find_one({'category': 'extraction'})
        if doc and 'settings' in doc:
            val = doc['settings'].get(key, default)

            # Handle extraction_schedule_settings with defaults
            if key == 'extraction_schedule_settings':
                if isinstance(val, dict):
                    return val
                return {
                    'schedule_interval': '0 * * * *',
                    'schedule_description': 'Every hour',
                    'updated_at': datetime.now().isoformat()
                }

            return val

        # Default for extraction_schedule_settings
        if key == 'extraction_schedule_settings':
            return {
                'schedule_interval': '0 * * * *',
                'schedule_description': 'Every hour',
                'updated_at': datetime.now().isoformat()
            }
        return default

    except Exception as e:
        logger.warning(f"Error reading extraction setting {key}: {e}")
        if key == 'extraction_schedule_settings':
            return {
                'schedule_interval': '0 * * * *',
                'schedule_description': 'Every hour',
                'updated_at': datetime.now().isoformat()
            }
        return default
    finally:
        if client:
            client.close()

def get_filter_words_list():
    """
    Get filter words as a clean list (not comma-separated string).
    Returns list of strings.
    """
    try:
        processing_settings = get_system_setting('extraction_processing_settings', {})
        words_str = processing_settings.get('words_to_filter', '')

        if not words_str or not words_str.strip():
            # ✅ FIXED: Use correct default filter words
            return [w.strip().lower() for w in CORRECT_FILTER_WORDS.split(',') if w.strip()]

        word_list = [w.strip().lower() for w in words_str.split(',') if w.strip()]

        logger.info(f"Retrieved {len(word_list)} filter words from settings")
        return word_list

    except Exception as e:
        logger.error(f"Error getting filter words list: {e}")
        # ✅ FIXED: Use correct default filter words
        return [w.strip().lower() for w in CORRECT_FILTER_WORDS.split(',') if w.strip()]

def update_filtering_settings(settings):
    """
    Update filtering settings in weekly_workflow_settings for current day.
    FIXED: Now saves category, workflow_type, and collection_name correctly.
    """
    try:
        # Get current day name
        current_day = date.today().strftime('%A').lower()

        # Get existing weekly workflow settings
        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        # Get current day settings or create new
        day_settings = weekly_settings.get(current_day, {})

        # ✅ FIXED: Update with new filtering configuration structure
        day_settings.update({
            # Basic filtering settings
            'filter_amount': settings.get('filter_amount', 100),
            'enabled': settings.get('enabled', True),

            # ✅ NEW: Category and collection info
            'destination_category': settings.get('destination_category', ''),
            'workflow_type_name': settings.get('workflow_type_name', ''),
            'collection_name': settings.get('collection_name', ''),

            # Legacy fields (for backward compatibility)
            'workflow_type': settings.get('workflow_type_name', settings.get('workflow_type', 'messages')),
            'content_types': [settings.get('workflow_type_name', 'messages')],

            # Metadata
            'config_date': settings.get('filter_date', date.today().isoformat()),
            'day_name': current_day.capitalize(),
            'day_key': current_day,
            'last_updated': datetime.now().isoformat()
        })

        # Update weekly settings with modified day settings
        weekly_settings[current_day] = day_settings

        # Save back to system settings
        update_system_setting('weekly_workflow_settings', weekly_settings)

        logger.info(f"✅ Filtering settings updated for {current_day}:")
        logger.info(f"   Category: {settings.get('destination_category')}")
        logger.info(f"   Type: {settings.get('workflow_type_name')}")
        logger.info(f"   Collection: {settings.get('collection_name')}")
        logger.info(f"   Amount: {settings.get('filter_amount')}")

        return True

    except Exception as e:
        logger.error(f"Error updating filtering settings: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def get_filtering_settings():
    """
    Get filtering settings from weekly_workflow_settings based on current date.
    FIXED: Now returns category, workflow_type_name, and collection_name.
    """
    try:
        # Get current day name
        current_day = date.today().strftime('%A').lower()

        # Get system settings
        system_settings = get_system_setting('weekly_workflow_settings', {})

        # Get day-specific settings
        day_settings = system_settings.get(current_day, {})

        # ✅ FIXED: Return all filtering configuration fields
        filtering_settings = {
            'filter_date': date.today().isoformat(),
            'filter_amount': day_settings.get('filter_amount', 100),
            'enabled': day_settings.get('enabled', True),

            # ✅ NEW: Category and collection info
            'destination_category': day_settings.get('destination_category', ''),
            'workflow_type_name': day_settings.get('workflow_type_name', ''),
            'collection_name': day_settings.get('collection_name', ''),

            # Legacy field (for backward compatibility with old DAGs)
            'workflow_type': day_settings.get('workflow_type_name', day_settings.get('workflow_type', 'messages')),
        }

        return filtering_settings

    except Exception as e:
        logger.error(f"Error getting filtering settings: {e}")
        return {
            'filter_date': date.today().isoformat(),
            'filter_amount': 100,
            'enabled': True,
            'destination_category': '',
            'workflow_type_name': '',
            'collection_name': '',
            'workflow_type': 'messages'
        }
def update_filter_words(words):
    """
    Update filter words in extraction_processing_settings.

    Args:
        words: Can be list of strings OR comma-separated string

    Returns:
        dict with success status and word count
    """
    try:
        # Handle both list and string input
        if isinstance(words, list):
            words_str = ', '.join([str(w).strip() for w in words if str(w).strip()])
        else:
            words_str = str(words).strip()

        # Get current settings
        processing_settings = get_system_setting('extraction_processing_settings', {})

        # Update only the filter words
        processing_settings['words_to_filter'] = words_str

        # Save back
        update_system_setting('extraction_processing_settings', processing_settings)

        # Count words for confirmation
        word_count = len([w for w in words_str.split(',') if w.strip()])

        logger.info(f"Updated filter words: {word_count} words configured")

        return {
            'success': True,
            'word_count': word_count,
            'words_string': words_str,
            'updated_at': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error updating filter words: {e}")
        return {
            'success': False,
            'error': str(e),
            'word_count': 0,
            'updated_at': datetime.now().isoformat()
        }


def add_filter_word(word):
    """
    Add a single word to the filter list (if not already present).

    Args:
        word: String word to add

    Returns:
        dict with success status
    """
    try:
        word = str(word).strip().lower()

        if not word:
            return {'success': False, 'error': 'Empty word provided'}

        # Get current words
        current_words = get_filter_words_list()

        # Check if already exists
        if word in current_words:
            logger.info(f"Filter word '{word}' already exists")
            return {
                'success': True,
                'already_exists': True,
                'word': word,
                'total_words': len(current_words)
            }

        # Add new word
        current_words.append(word)

        # Update
        result = update_filter_words(current_words)

        if result['success']:
            logger.info(f"Added filter word: '{word}'")
            return {
                'success': True,
                'already_exists': False,
                'word': word,
                'total_words': result['word_count']
            }
        else:
            return result

    except Exception as e:
        logger.error(f"Error adding filter word: {e}")
        return {
            'success': False,
            'error': str(e),
            'word': word
        }


def remove_filter_word(word):
    """
    Remove a single word from the filter list.

    Args:
        word: String word to remove

    Returns:
        dict with success status
    """
    try:
        word = str(word).strip().lower()

        if not word:
            return {'success': False, 'error': 'Empty word provided'}

        # Get current words
        current_words = get_filter_words_list()

        # Check if exists
        if word not in current_words:
            logger.info(f"Filter word '{word}' not found in list")
            return {
                'success': True,
                'not_found': True,
                'word': word,
                'total_words': len(current_words)
            }

        # Remove word
        current_words.remove(word)

        # Update
        result = update_filter_words(current_words)

        if result['success']:
            logger.info(f"Removed filter word: '{word}'")
            return {
                'success': True,
                'not_found': False,
                'word': word,
                'total_words': result['word_count']
            }
        else:
            return result

    except Exception as e:
        logger.error(f"Error removing filter word: {e}")
        return {
            'success': False,
            'error': str(e),
            'word': word
        }


def clear_all_filter_words():
    """
    Remove all filter words (sets to empty string).

    Returns:
        dict with success status
    """
    try:
        result = update_filter_words('')

        if result['success']:
            logger.info("Cleared all filter words")
            return {
                'success': True,
                'message': 'All filter words cleared',
                'updated_at': result['updated_at']
            }
        else:
            return result

    except Exception as e:
        logger.error(f"Error clearing filter words: {e}")
        return {
            'success': False,
            'error': str(e)
        }


def get_filter_statistics():
    """
    Get statistics about current filter configuration.

    Returns:
        dict with filter stats
    """
    try:
        processing_settings = get_system_setting('extraction_processing_settings', {})
        filter_words = get_filter_words_list()

        # Analyze word lengths
        word_lengths = [len(w) for w in filter_words]
        avg_length = sum(word_lengths) / len(word_lengths) if word_lengths else 0

        stats = {
            'total_words': len(filter_words),
            'filter_words': filter_words,
            'words_string': processing_settings.get('words_to_filter', ''),
            'content_to_filter': int(processing_settings.get('content_to_filter', 19)),  # Integer digit count
            'gap_between_workflows': processing_settings.get('gap_between_workflows', 0.25),
            'gap_between_workflows_seconds': processing_settings.get('gap_between_workflows_seconds', 15),
            'statistics': {
                'average_word_length': round(avg_length, 2),
                'shortest_word': min(word_lengths) if word_lengths else 0,
                'longest_word': max(word_lengths) if word_lengths else 0,
                'total_characters': sum(word_lengths)
            },
            'updated_at': datetime.now().isoformat()
        }

        logger.info(f"Filter statistics retrieved: {stats['total_words']} words configured")
        return stats

    except Exception as e:
        logger.error(f"Error getting filter statistics: {e}")
        return {
            'total_words': 0,
            'filter_words': [],
            'error': str(e),
            'updated_at': datetime.now().isoformat()
        }


def validate_filter_words(words):
    """
    Validate filter words before saving.
    Checks for duplicates, empty words, and provides warnings.

    Args:
        words: List of words or comma-separated string

    Returns:
        dict with validation results
    """
    try:
        # Parse words
        if isinstance(words, list):
            word_list = [str(w).strip().lower() for w in words if str(w).strip()]
        else:
            word_list = [w.strip().lower() for w in str(words).split(',') if w.strip()]

        # Check for issues
        issues = []
        warnings = []

        # Check for duplicates
        duplicates = [w for w in set(word_list) if word_list.count(w) > 1]
        if duplicates:
            warnings.append(f"Duplicate words found: {', '.join(duplicates)}")

        # Check for very short words (might cause false positives)
        short_words = [w for w in word_list if len(w) <= 2]
        if short_words:
            warnings.append(f"Very short words (might cause false positives): {', '.join(short_words)}")

        # Check for very long words (typos?)
        long_words = [w for w in word_list if len(w) >= 30]
        if long_words:
            warnings.append(f"Very long words (possible typo?): {', '.join(long_words)}")

        # Check for special characters
        special_char_words = [w for w in word_list if not w.replace('-', '').replace('_', '').isalnum()]
        if special_char_words:
            warnings.append(f"Words with special characters: {', '.join(special_char_words)}")

        # Remove duplicates for final count
        unique_words = list(set(word_list))

        validation = {
            'valid': len(issues) == 0,
            'word_count': len(unique_words),
            'original_count': len(word_list),
            'duplicates_removed': len(word_list) - len(unique_words),
            'issues': issues,
            'warnings': warnings,
            'validated_words': unique_words,
            'validated_at': datetime.now().isoformat()
        }

        logger.info(f"Filter words validated: {validation['word_count']} unique words, {len(issues)} issues, {len(warnings)} warnings")
        return validation

    except Exception as e:
        logger.error(f"Error validating filter words: {e}")
        return {
            'valid': False,
            'word_count': 0,
            'issues': [str(e)],
            'warnings': [],
            'validated_at': datetime.now().isoformat()
        }




# ----------------------------------------------------------------------
# UTILITY FUNCTIONS FOR FILTER WORD TESTING
# ----------------------------------------------------------------------

def test_filter_word_match(text, word):
    """
    Test if a specific filter word would match against text.
    Useful for debugging filtering behavior.

    Args:
        text: Text to test
        word: Filter word to check

    Returns:
        dict with match results
    """
    try:
        text_lower = str(text).lower()
        word_lower = str(word).strip().lower()

        matches = word_lower in text_lower

        if matches:
            # Find all positions where word appears
            positions = []
            start = 0
            while True:
                pos = text_lower.find(word_lower, start)
                if pos == -1:
                    break
                positions.append(pos)
                start = pos + 1
        else:
            positions = []

        return {
            'matches': matches,
            'word': word_lower,
            'text_length': len(text),
            'match_count': len(positions),
            'positions': positions,
            'text_preview': text[:100] + ('...' if len(text) > 100 else '')
        }

    except Exception as e:
        logger.error(f"Error testing filter word match: {e}")
        return {
            'matches': False,
            'error': str(e)
        }


def batch_test_filter_words(urls_or_texts):
    """
    Test multiple URLs/texts against all filter words.
    Returns which would be filtered and why.

    Args:
        urls_or_texts: List of URLs or text strings to test

    Returns:
        dict with test results for each input
    """
    try:
        filter_words = get_filter_words_list()

        results = []

        for item in urls_or_texts:
            item_lower = str(item).lower()
            matched_words = []

            for word in filter_words:
                if word in item_lower:
                    matched_words.append(word)

            results.append({
                'input': item,
                'would_filter': len(matched_words) > 0,
                'matched_words': matched_words,
                'match_count': len(matched_words)
            })

        total_filtered = sum(1 for r in results if r['would_filter'])
        total_passed = len(results) - total_filtered

        return {
            'total_tested': len(results),
            'total_filtered': total_filtered,
            'total_passed': total_passed,
            'filter_rate': round((total_filtered / len(results) * 100), 2) if results else 0,
            'results': results,
            'filter_words_used': filter_words,
            'tested_at': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error in batch filter test: {e}")
        return {
            'total_tested': 0,
            'total_filtered': 0,
            'total_passed': 0,
            'error': str(e),
            'tested_at': datetime.now().isoformat()
        }

def update_system_setting(key, value):
    """Update a system setting in MongoDB with validation for individual workflow execution."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # NEW: Special handling for workflow type settings
        if key == 'extraction_workflow_types':
            # Validate workflow type settings
            validate_workflow_type_settings(value)
            logger.info("Workflow type settings validated successfully")

        elif key == 'extraction_priority_order':
            # Get current workflow types for validation
            current_types = get_system_setting('extraction_workflow_types',
                                              {'replies': True, 'messages': True, 'retweets': True})
            validate_priority_order_settings(value, current_types)
            logger.info("Priority order settings validated successfully")

        # Special handling for extraction time settings with workflow types
        elif key == 'extraction_time_settings':
            # Ensure workflow types are included
            if 'workflow_types' not in value:
                value['workflow_types'] = get_system_setting('extraction_workflow_types',
                                                           {'replies': True, 'messages': True, 'retweets': True})

            if 'priority_order' not in value:
                value['priority_order'] = get_system_setting('extraction_priority_order',
                                                           ['replies', 'messages', 'retweets'])

            # Validate workflow type settings
            validate_workflow_type_settings(value['workflow_types'])
            validate_priority_order_settings(value['priority_order'], value['workflow_types'])
            logger.info("Extraction time settings with workflow types validated successfully")
        elif key == 'weekly_workflow_settings':
            # Validate workflows_to_process field
            for day_key, day_settings in value.items():
                if 'workflows_to_process' in day_settings:
                    workflows_count = day_settings['workflows_to_process']
                    if not isinstance(workflows_count, int) or workflows_count < 1:
                        raise ValueError(f"workflows_to_process for {day_key} must be a positive integer")

                # Ensure weekends are always enabled
                if day_key in ['saturday', 'sunday']:
                    day_settings['enabled'] = True

                # Ensure workflows_to_process exists
                if 'workflows_to_process' not in day_settings:
                    day_settings['workflows_to_process'] = 12 if day_key in ['saturday', 'sunday'] else 15

            logger.info("Weekly workflow settings with workflows_to_process validated successfully")
        # Special handling for session management settings
        elif key == 'session_management_settings':
            # Validate UUID format for profile_id and session_id
            if 'current_profile_id' in value and value['current_profile_id']:
                profile_id = value['current_profile_id']
                if not (len(profile_id) == 36 and profile_id.count('-') == 4):
                    logger.warning(f"Profile ID should be in UUID format: {profile_id}")

            if 'current_session_id' in value and value['current_session_id']:
                session_id = value['current_session_id']
                if not (len(session_id) == 36 and session_id.count('-') == 4):
                    logger.warning(f"Session ID should be in UUID format: {session_id}")

            # Validate extension ID format (32-character Chrome extension ID)
            if 'current_extension_id' in value and value['current_extension_id']:
                extension_id = value['current_extension_id']
                if not (len(extension_id) == 32 and extension_id.isalnum()):
                    logger.warning(f"Extension ID should be 32-character alphanumeric: {extension_id}")

            # Type enforcement
            if 'session_timeout_minutes' in value:
                value['session_timeout_minutes'] = int(value['session_timeout_minutes'])

            bool_fields = ['auto_create_sessions', 'auto_assign_profiles', 'track_session_performance']
            for field in bool_fields:
                if field in value:
                    value[field] = bool(value[field])

        # Special handling for execution limits settings
        elif key == 'execution_limits_settings':
            # Type enforcement
            numeric_fields = ['workflow_execution_limit', 'daily_execution_limit', 'hourly_execution_limit']
            for field in numeric_fields:
                if field in value:
                    value[field] = int(value[field])

            if 'execution_time_limit_hours' in value:
                value['execution_time_limit_hours'] = float(value['execution_time_limit_hours'])

            bool_fields = ['reset_limits_daily', 'reset_limits_hourly', 'enforce_limits']
            for field in bool_fields:
                if field in value:
                    value[field] = bool(value[field])

        # Special handling for extraction_processing_settings
        elif key == 'extraction_processing_settings':
            # Ensure gap_between_workflows is properly converted
            if 'gap_between_workflows' in value:
                gap_minutes = float(value['gap_between_workflows'])
                gap_seconds = int(gap_minutes * 60)
                value['gap_between_workflows_seconds'] = max(gap_seconds, 5)  # Minimum 5 seconds
                logger.info(f"Updated workflow gap: {gap_minutes} minutes = {gap_seconds} seconds")

            # Ensure content_to_filter is an integer
            if 'content_to_filter' in value:
                value['content_to_filter'] = int(value['content_to_filter'])

            # Ensure words_to_filter is a string
            if 'words_to_filter' in value:
                if isinstance(value['words_to_filter'], list):
                    value['words_to_filter'] = ', '.join(value['words_to_filter'])
                else:
                    value['words_to_filter'] = str(value['words_to_filter'])

            # NEW: Handle workflow type settings
            if 'workflow_types' in value:
                validate_workflow_type_settings(value['workflow_types'])

            if 'priority_order' in value:
                workflow_types = value.get('workflow_types', get_system_setting('extraction_workflow_types'))
                validate_priority_order_settings(value['priority_order'], workflow_types)

        # Special handling for workflow strategy settings
        elif key == 'workflow_strategy_settings':
            # Type enforcement for individual workflow execution
            numeric_fields = ['trigger_delay_seconds', 'interval_between_batches',
                            'timeout_per_workflow', 'workflow_execution_timeout',
                            'wait_after_storage_clear', 'wait_before_trigger', 'workflow_gap_seconds']

            for field in numeric_fields:
                if field in value:
                    value[field] = float(value[field])

            integer_fields = ['batch_size', 'max_workflows_per_run']
            for field in integer_fields:
                if field in value:
                    value[field] = int(value[field])

            # Boolean enforcement
            bool_fields = ['enable_sequential_triggering', 'retry_failed_triggers',
                          'log_detailed_execution', 'single_workflow_mode',
                          'clear_storage_before_injection']
            for field in bool_fields:
                if field in value:
                    value[field] = bool(value[field])

            # Force individual workflow mode settings
            value['batch_size'] = 1
            value['max_workflows_per_run'] = 1
            value['single_workflow_mode'] = True

            # NEW: Handle workflow type settings in strategy
            if 'enabled_workflow_types' in value:
                validate_workflow_type_settings(value['enabled_workflow_types'])

            if 'priority_order' in value:
                enabled_types = value.get('enabled_workflow_types',
                                        get_system_setting('extraction_workflow_types'))
                validate_priority_order_settings(value['priority_order'], enabled_types)

            # Validate settings for individual execution
            try:
                validate_single_workflow_settings(value)
                logger.info("Individual workflow strategy settings validated successfully")
            except ValueError as ve:
                logger.error(f"Invalid individual workflow settings: {ve}")
                raise ve

        # Update the setting in MongoDB
        db.settings.update_one(
            {'category': 'system'},
            {'$set': {f'settings.{key}': value, 'updated_at': datetime.now()}},
            upsert=True
        )

        logger.info(f"Successfully updated system setting: {key}")

    except Exception as e:
        logger.error(f"Error updating setting {key} in MongoDB: {e}")
        raise e
    finally:
        if client:
            client.close()

def get_available_profiles():
    """Get list of available Chrome profiles from MongoDB."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        db = client['messages_db']

        profiles = list(db.chrome_profiles.find(
            {'is_active': True},
            {'profile_id': 1, 'profile_name': 1, 'profile_type': 1, '_id': 0}
        ).sort('profile_name', 1))

        return profiles
    except Exception as e:
        logger.error(f"Error getting available profiles: {e}")
        return []
    finally:
        if client:
            client.close()

def get_available_extensions():
    """Get list of available extensions from MongoDB."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        db = client['messages_db']

        extensions = list(db.extension_instances.find(
            {'is_enabled': True},
            {'extension_id': 1, 'extension_name': 1, 'extension_version': 1, '_id': 0}
        ).sort('extension_name', 1))

        return extensions
    except Exception as e:
        logger.error(f"Error getting available extensions: {e}")
        return []
    finally:
        if client:
            client.close()

def get_active_sessions():
    """Get list of active browser sessions from MongoDB."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        db = client['messages_db']

        sessions = list(db.browser_sessions.find(
            {'is_active': True},
            {'session_id': 1, 'profile_id': 1, 'extension_id': 1, 'browser_type': 1,
             'created_at': 1, 'workflow_count': 1, '_id': 0}
        ).sort('created_at', -1))

        return sessions
    except Exception as e:
        logger.error(f"Error getting active sessions: {e}")
        return []
    finally:
        if client:
            client.close()

def get_execution_limits_status():
    """Get current execution limits status and usage."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        db = client['messages_db']

        # Get current settings
        limits_settings = get_system_setting('execution_limits_settings')

        # Get current tracking data
        today = datetime.now().strftime('%Y-%m-%d')
        current_hour = datetime.now().hour

        daily_tracking = db.execution_limits_tracking.find_one({
            'tracking_id': f'daily_{today}',
            'period_type': 'daily'
        })

        hourly_tracking = db.execution_limits_tracking.find_one({
            'tracking_id': f'hourly_{today}_{current_hour}',
            'period_type': 'hourly'
        })

        # Calculate status
        status = {
            'settings': limits_settings,
            'daily': {
                'current_count': daily_tracking['current_execution_count'] if daily_tracking else 0,
                'current_time_hours': daily_tracking['current_execution_time_hours'] if daily_tracking else 0.0,
                'limit_reached': daily_tracking['limit_reached'] if daily_tracking else False,
                'time_limit_reached': daily_tracking['time_limit_reached'] if daily_tracking else False,
            },
            'hourly': {
                'current_count': hourly_tracking['current_execution_count'] if hourly_tracking else 0,
                'current_time_hours': hourly_tracking['current_execution_time_hours'] if hourly_tracking else 0.0,
                'limit_reached': hourly_tracking['limit_reached'] if hourly_tracking else False,
                'time_limit_reached': hourly_tracking['time_limit_reached'] if hourly_tracking else False,
            }
        }

        return status

    except Exception as e:
        logger.error(f"Error getting execution limits status: {e}")
        return {
            'settings': get_system_setting('execution_limits_settings'),
            'daily': {'current_count': 0, 'current_time_hours': 0.0, 'limit_reached': False, 'time_limit_reached': False},
            'hourly': {'current_count': 0, 'current_time_hours': 0.0, 'limit_reached': False, 'time_limit_reached': False}
        }
    finally:
        if client:
            client.close()

def create_browser_session(profile_id, extension_id):
    """Create a new browser session."""
    client = None
    try:
        from bson import ObjectId

        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        db = client['messages_db']

        session_id = f"sess_{ObjectId()}"

        session_data = {
            'session_id': session_id,
            'profile_id': profile_id,
            'extension_id': extension_id,
            'browser_type': 'chrome',
            'session_status': 'created',
            'is_active': False,  # Will be activated when browser starts
            'created_at': datetime.now(),
            'started_at': None,
            'ended_at': None,
            'session_duration': None,
            'workflow_count': 0,
            'browser_version': None,
            'user_data_dir': f'/tmp/chrome-profile-{profile_id}',
            'session_metadata': {
                'created_via': 'settings_ui',
                'headless': False,
                'debugging_enabled': True,
                'extension_loaded': False,
                'startup_time': None
            }
        }

        db.browser_sessions.insert_one(session_data)
        logger.info(f"Created new browser session: {session_id}")

        return session_id

    except Exception as e:
        logger.error(f"Error creating browser session: {e}")
        return None
    finally:
        if client:
            client.close()

def get_individual_workflow_gap_seconds():
    """Get the workflow gap specifically for individual execution in seconds."""
    try:
        # First try extraction_processing_settings
        extraction_settings = get_system_setting('extraction_processing_settings', {})

        if 'gap_between_workflows_seconds' in extraction_settings:
            gap_seconds = extraction_settings['gap_between_workflows_seconds']
        elif 'gap_between_workflows' in extraction_settings:
            gap_minutes = extraction_settings.get('gap_between_workflows', 0.25)
            gap_seconds = int(gap_minutes * 60)
        else:
            # Fallback to strategy settings
            strategy_settings = get_system_setting('workflow_strategy_settings', {})
            gap_seconds = int(strategy_settings.get('workflow_gap_seconds', 15))

        # Ensure minimum gap
        gap_seconds = max(gap_seconds, 5)

        logger.info(f"Retrieved individual workflow gap: {gap_seconds} seconds")
        return gap_seconds

    except Exception as e:
        logger.error(f"Error getting individual workflow gap: {e}")
        return 15  # Safe default

# For settings_manager.py - add these missing variables and functions

# Updated get_individual_workflow_processing_config function with proper variable definitions
def get_individual_workflow_processing_config():
    """Get configuration for individual workflow processing with all required variables."""
    from datetime import datetime
    import logging

    logger = logging.getLogger(__name__)

    try:
        # Get workflow strategy settings
        strategy_settings = get_system_setting('workflow_strategy_settings', get_single_workflow_settings())

        # Get extraction settings for gap configuration
        extraction_settings = get_system_setting('extraction_processing_settings', {})

        # Get single workflow processing settings
        processing_settings = get_system_setting('single_workflow_processing', {
            'enabled': True,
            'schedule_interval': '*/5 * * * *',
            'max_concurrent_executions': 1,
            'retry_attempts': 2,
            'retry_delay': 60,
            'log_file': '/tmp/individual_workflow_execution.log',
            'chrome_startup_timeout': 45,
            'chrome_health_check_retries': 5,
        })

        # Get the proper gap setting
        workflow_gap_seconds = get_individual_workflow_gap_seconds()

        # Get workflow type settings
        workflow_types = get_system_setting('extraction_workflow_types',
                                           {'replies': True, 'messages': True, 'retweets': True})
        priority_order = get_system_setting('extraction_priority_order',
                                          ['replies', 'messages', 'retweets'])

        # Get enabled type names for filtering
        enabled_type_names = [wf_type for wf_type, enabled in workflow_types.items() if enabled]

        # Merge and validate configuration for individual execution
        config = {
            'enabled': processing_settings.get('enabled', True),
            'schedule_interval': processing_settings.get('schedule_interval', '*/5 * * * *'),
            'trigger_delay_seconds': strategy_settings.get('trigger_delay_seconds', 5.0),
            'interval_between_batches': strategy_settings.get('interval_between_batches', 120.0),
            'timeout_per_workflow': strategy_settings.get('timeout_per_workflow', 10.0),
            'workflow_execution_timeout': strategy_settings.get('workflow_execution_timeout', 300.0),
            'retry_failed_triggers': strategy_settings.get('retry_failed_triggers', True),
            'log_detailed_execution': strategy_settings.get('log_detailed_execution', True),
            'clear_storage_before_injection': strategy_settings.get('clear_storage_before_injection', True),
            'wait_after_storage_clear': strategy_settings.get('wait_after_storage_clear', 2.0),
            'wait_before_trigger': strategy_settings.get('wait_before_trigger', 1.0),
            'max_concurrent_executions': processing_settings.get('max_concurrent_executions', 1),
            'retry_attempts': processing_settings.get('retry_attempts', 2),
            'retry_delay': processing_settings.get('retry_delay', 60),
            'priority_order': priority_order,
            'log_file': processing_settings.get('log_file', '/tmp/individual_workflow_execution.log'),
            'chrome_startup_timeout': processing_settings.get('chrome_startup_timeout', 45),
            'chrome_health_check_retries': processing_settings.get('chrome_health_check_retries', 5),

            # Individual workflow execution specific settings
            'execution_mode': 'individual',
            'workflow_gap_seconds': workflow_gap_seconds,
            'enabled_workflow_types': workflow_types,
            'enabled_type_names': enabled_type_names,  # Add this missing variable
            'filter_criteria': {
                'has_link': True,
                'executed': False,
                'workflow_types': enabled_type_names
            },
            'content_filtering': {
                'content_to_filter': extraction_settings.get('content_to_filter', 19),
                'words_to_filter': extraction_settings.get('words_to_filter', '')
            },
            'updated_at': datetime.now().isoformat()
        }

        # Get statistics if needed
        statistics = get_workflow_execution_statistics()
        eligible_workflows = statistics.get('eligible_workflows', 0)

        # Add statistics to config
        config.update({
            'statistics': statistics,
            'eligible_workflows': eligible_workflows
        })

        logger.info("Individual workflow processing configuration retrieved successfully")
        logger.info(f"Eligible workflows for next run: {eligible_workflows}")
        logger.info(f"Enabled workflow types: {enabled_type_names}")

        return config

    except Exception as e:
        logger.error(f"Error getting individual workflow processing config: {e}")
        # Return safe defaults
        return {
            'enabled': True,
            'schedule_interval': '*/5 * * * *',
            'execution_mode': 'individual',
            'workflow_gap_seconds': 15,
            'enabled_workflow_types': {'replies': True, 'messages': True, 'retweets': True},
            'enabled_type_names': ['replies', 'messages', 'retweets'],
            'eligible_workflows': 0,
            'filter_criteria': {'has_link': True, 'executed': False, 'workflow_types': ['replies', 'messages', 'retweets']},
            'error': str(e),
            'updated_at': datetime.now().isoformat()
        }


# File: streamlit/ui/settings/settings_manager.py
# ADD these functions to support custom prompt types







def validate_custom_workflow_type(workflow_type: str) -> bool:
    """Validate that a workflow type exists in the prompts table"""
    try:
        all_types = get_all_custom_prompt_types_from_db()

        if workflow_type not in all_types:
            logger.warning(f"Custom workflow type '{workflow_type}' not found in database")
            return False

        return True

    except Exception as e:
        logger.error(f"Error validating custom workflow type: {e}")
        return False







def sync_custom_types_to_settings():
    """Sync available custom prompt types to system settings for easy access"""
    try:
        all_custom_types = get_all_custom_prompt_types_from_db()

        # Update system setting with current custom types
        update_system_setting('available_custom_prompt_types', all_custom_types)

        logger.info(f"Synced {len(all_custom_types)} custom types to system settings")
        return all_custom_types

    except Exception as e:
        logger.error(f"Error syncing custom types to settings: {e}")
        return []


# UPDATE the existing workflow type functions to support custom types





def get_workflow_type_configuration():
    """Get current workflow type configuration with statistics."""
    try:
        # Get current workflow type settings
        workflow_types = get_system_setting('extraction_workflow_types',
                                           {'replies': True, 'messages': True, 'retweets': True})
        priority_order = get_system_setting('extraction_priority_order',
                                          ['replies', 'messages', 'retweets'])

        # Get statistics for each type
        statistics = get_workflow_execution_statistics()
        type_stats = statistics.get('type_statistics', {})

        # Build configuration with statistics
        configuration = {
            'workflow_types': workflow_types,
            'priority_order': priority_order,
            'enabled_types': [wf_type for wf_type, enabled in workflow_types.items() if enabled],
            'disabled_types': [wf_type for wf_type, enabled in workflow_types.items() if not enabled],
            'type_statistics': type_stats,
            'total_eligible': sum([stats.get('eligible', 0) for stats in type_stats.values()]),
            'configuration_valid': len([wf for wf, enabled in workflow_types.items() if enabled]) > 0,
            'priority_order_valid': all(wf_type in workflow_types for wf_type in priority_order),
            'updated_at': datetime.now().isoformat()
        }

        logger.info("Workflow type configuration retrieved successfully")
        logger.info(f"Enabled types: {configuration['enabled_types']}")
        logger.info(f"Priority order: {priority_order}")
        return configuration

    except Exception as e:
        logger.error(f"Error getting workflow type configuration: {e}")
        return {
            'workflow_types': {'replies': True, 'messages': True, 'retweets': True},
            'priority_order': ['replies', 'messages', 'retweets'],
            'enabled_types': ['replies', 'messages', 'retweets'],
            'disabled_types': [],
            'type_statistics': {},
            'total_eligible': 0,
            'configuration_valid': True,
            'priority_order_valid': True,
            'error': str(e),
            'updated_at': datetime.now().isoformat()
        }

def update_workflow_type_configuration(workflow_types, priority_order):
    """Update workflow type configuration with validation."""
    try:
        # Validate the configuration
        validate_workflow_type_settings(workflow_types)
        validate_priority_order_settings(priority_order, workflow_types)

        # Update the settings
        update_system_setting('extraction_workflow_types', workflow_types)
        update_system_setting('extraction_priority_order', priority_order)

        # Also update extraction_processing_settings to include workflow types
        extraction_settings = get_system_setting('extraction_processing_settings', {})
        extraction_settings['workflow_types'] = workflow_types
        extraction_settings['priority_order'] = priority_order
        update_system_setting('extraction_processing_settings', extraction_settings)

        logger.info("Workflow type configuration updated successfully")
        logger.info(f"Updated workflow types: {workflow_types}")
        logger.info(f"Updated priority order: {priority_order}")

        return {
            'success': True,
            'workflow_types': workflow_types,
            'priority_order': priority_order,
            'enabled_types': [wf_type for wf_type, enabled in workflow_types.items() if enabled],
            'updated_at': datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error updating workflow type configuration: {e}")
        return {
            'success': False,
            'error': str(e),
            'workflow_types': workflow_types,
            'priority_order': priority_order,
            'updated_at': datetime.now().isoformat()
        }

def get_next_eligible_workflow_by_type():
    """Get the next eligible workflow based on enabled types and priority order."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Get workflow type configuration
        workflow_types = get_system_setting('extraction_workflow_types',
                                           {'replies': True, 'messages': True, 'retweets': True})
        priority_order = get_system_setting('extraction_priority_order',
                                          ['replies', 'messages', 'retweets'])

        # Get enabled types
        enabled_types = [wf_type for wf_type, enabled in workflow_types.items() if enabled]

        if not enabled_types:
            logger.warning("No workflow types are enabled")
            return None

        # Filter priority order to only include enabled types
        filtered_priority = [wf_type for wf_type in priority_order if wf_type in enabled_types]

        # Try to find a workflow for each type in priority order
        for workflow_type in filtered_priority:
            workflow = db.content_workflow_links.find_one({
                'content_type': workflow_type,
                'has_link': True,
                'executed': False
            }, sort=[('created_at', 1)])  # FIFO order

            if workflow:
                logger.info(f"Found eligible workflow of type '{workflow_type}': {workflow.get('_id')}")
                return workflow

        # If no workflows found in priority order, check all enabled types
        for workflow_type in enabled_types:
            if workflow_type not in filtered_priority:  # Skip already checked types
                workflow = db.content_workflow_links.find_one({
                    'content_type': workflow_type,
                    'has_link': True,
                    'executed': False
                }, sort=[('created_at', 1)])

                if workflow:
                    logger.info(f"Found eligible workflow of type '{workflow_type}' (fallback): {workflow.get('_id')}")
                    return workflow

        logger.info("No eligible workflows found for any enabled type")
        return None

    except Exception as e:
        logger.error(f"Error getting next eligible workflow by type: {e}")
        return None
    finally:
        if client:
            client.close()

def get_workflows_by_type_and_status(workflow_type=None, status_filter=None, limit=10):
    """Get workflows filtered by type and status."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Build filter
        filter_criteria = {}

        if workflow_type:
            filter_criteria['content_type'] = workflow_type

        if status_filter == 'eligible':
            filter_criteria.update({'has_link': True, 'executed': False})
        elif status_filter == 'executed':
            filter_criteria['executed'] = True
        elif status_filter == 'successful':
            filter_criteria['execution_success'] = True
        elif status_filter == 'failed':
            filter_criteria.update({'executed': True, 'execution_success': False})

        # Get workflows
        workflows = list(db.content_workflow_links.find(
            filter_criteria,
            {
                'content_type': 1,
                'has_link': 1,
                'executed': 1,
                'execution_success': 1,
                'executed_at': 1,
                'created_at': 1,
                'execution_time': 1,
                'link_url': 1,
                '_id': 1
            }
        ).sort('created_at', -1).limit(limit))

        logger.info(f"Retrieved {len(workflows)} workflows for type='{workflow_type}', status='{status_filter}'")
        return workflows

    except Exception as e:
        logger.error(f"Error getting workflows by type and status: {e}")
        return []
    finally:
        if client:
            client.close()



def update_individual_workflow_processing_config(config):
    """Update individual workflow processing configuration with gap settings."""
    try:
        # Extract gap setting and convert to proper format
        workflow_gap_seconds = config.get('workflow_gap_seconds', 15)
        gap_minutes = workflow_gap_seconds / 60.0  # Convert back to minutes for storage

        # Extract workflow type settings
        enabled_workflow_types = config.get('enabled_workflow_types',
                                           {'replies': True, 'messages': True, 'retweets': True})
        priority_order = config.get('priority_order', ['replies', 'messages', 'retweets'])

        # Split config between different setting categories
        strategy_settings = {
            'trigger_delay_seconds': config.get('trigger_delay_seconds', 5.0),
            'interval_between_batches': config.get('interval_between_batches', 120.0),
            'timeout_per_workflow': config.get('timeout_per_workflow', 10.0),
            'workflow_execution_timeout': config.get('workflow_execution_timeout', 300.0),
            'retry_failed_triggers': config.get('retry_failed_triggers', True),
            'log_detailed_execution': config.get('log_detailed_execution', True),
            'clear_storage_before_injection': config.get('clear_storage_before_injection', True),
            'wait_after_storage_clear': config.get('wait_after_storage_clear', 2.0),
            'wait_before_trigger': config.get('wait_before_trigger', 1.0),
            'workflow_gap_seconds': workflow_gap_seconds,
            'enabled_workflow_types': enabled_workflow_types,
            'priority_order': priority_order,
            # Ensure individual workflow mode
            'batch_size': 1,
            'max_workflows_per_run': 1,
            'enable_sequential_triggering': True,
            'single_workflow_mode': True,
        }

        processing_settings = {
            'enabled': config.get('enabled', True),
            'schedule_interval': config.get('schedule_interval', '*/5 * * * *'),
            'max_concurrent_executions': config.get('max_concurrent_executions', 1),
            'retry_attempts': config.get('retry_attempts', 2),
            'retry_delay': config.get('retry_delay', 60),
            'priority_order': priority_order,
            'log_file': config.get('log_file', '/tmp/individual_workflow_execution.log'),
            'chrome_startup_timeout': config.get('chrome_startup_timeout', 45),
            'chrome_health_check_retries': config.get('chrome_health_check_retries', 5),
            'enabled_workflow_types': enabled_workflow_types,
        }

        # Extraction settings with gap configuration and workflow types
        content_filtering = config.get('content_filtering', {})
        extraction_settings = {
            'content_to_filter': content_filtering.get('content_to_filter', 19),
            'words_to_filter': content_filtering.get('words_to_filter', ''),
            'gap_between_workflows': gap_minutes,  # Store in minutes
            'gap_between_workflows_seconds': workflow_gap_seconds,  # Also store in seconds for direct access
            'workflow_types': enabled_workflow_types,
            'priority_order': priority_order
        }

        # Update all settings
        update_system_setting('workflow_strategy_settings', strategy_settings)
        update_system_setting('single_workflow_processing', processing_settings)
        update_system_setting('extraction_processing_settings', extraction_settings)
        update_system_setting('extraction_workflow_types', enabled_workflow_types)
        update_system_setting('extraction_priority_order', priority_order)

        logger.info("Individual workflow processing configuration updated successfully")
        logger.info(f"Updated workflow gap: {workflow_gap_seconds} seconds ({gap_minutes:.2f} minutes)")
        logger.info(f"Updated workflow types: {[wf for wf, enabled in enabled_workflow_types.items() if enabled]}")
        logger.info(f"Updated priority order: {priority_order}")
        return True

    except Exception as e:
        logger.error(f"Error updating individual workflow processing config: {e}")
        return False

def log_individual_workflow_execution(workflow_name, status, execution_time=None, error_message=None, workflow_gap=None, workflow_type=None):
    """Log individual workflow execution details with gap and type information."""
    try:
        config = get_individual_workflow_processing_config()
        log_file = config.get('log_file', '/tmp/individual_workflow_execution.log')

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"{timestamp} - INDIVIDUAL - {status} - Workflow: {workflow_name}"

        if workflow_type:
            log_entry += f" - Type: {workflow_type}"

        if execution_time:
            log_entry += f" - Duration: {execution_time:.2f}s"

        if workflow_gap:
            log_entry += f" - Gap: {workflow_gap}s"

        if error_message:
            log_entry += f" - Error: {error_message}"

        log_entry += "\n"

        # Write to log file
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(log_entry)

        # Also log to application logger
        if status == 'SUCCESS':
            logger.info(f"Individual workflow executed successfully: {workflow_name} [{workflow_type}] (gap: {workflow_gap}s)")
        elif status == 'FAILURE':
            logger.error(f"Individual workflow execution failed: {workflow_name} [{workflow_type}] - {error_message}")
        else:
            logger.info(f"Individual workflow status: {status} - {workflow_name} [{workflow_type}]")

    except Exception as e:
        logger.error(f"Error logging individual workflow execution: {e}")


def get_combined_config():
    """Get combined configuration (weekly + filtering settings)."""
    try:
        weekly_config = get_weekly_workflow_config()
        filtering_config = get_filtering_config()

        # Priority: Filtering settings override weekly settings if enabled
        if filtering_config.get('enabled', True):
            logger.info("Using filtering settings (overrides weekly config)")
            return {
                'enabled': True,
                'filter_amount': filtering_config['filter_amount'],
                'workflow_type': filtering_config['workflow_type'],
                'source': 'filtering_settings'
            }
        else:
            logger.info("Using weekly workflow settings")
            return {
                'enabled': weekly_config.get('enabled', True),
                'filter_amount': weekly_config.get('filter_amount', weekly_config.get('links_to_filter', 10)),
                'workflow_type': weekly_config.get('workflow_type', 'messages'),
                'source': 'weekly_settings'
            }

    except Exception as e:
        logger.error(f"Error getting combined config: {e}")
        # Fallback to defaults
        return {
            'enabled': True,
            'filter_amount': 100,
            'workflow_type': 'messages',
            'source': 'fallback'
        }

def get_filtering_config():
    """Get filtering configuration from MongoDB settings."""
    try:
        from streamlit.ui.settings.settings_manager import get_filtering_settings

        filtering_settings = get_filtering_settings()

        # Check if filtering is enabled
        if not filtering_settings.get('enabled', True):
            logger.info("Filtering is disabled in settings")
            return {
                'enabled': False,
                'filter_amount': 0,
                'workflow_type': 'messages'
            }

        config = {
            'enabled': filtering_settings.get('enabled', True),
            'filter_amount': filtering_settings.get('filter_amount', 100),
            'workflow_type': filtering_settings.get('workflow_type', 'messages'),
            'content_types': filtering_settings.get('content_types', ['messages', 'replies']),
            'extraction_window': filtering_settings.get('extraction_window', 24),
            'time_limit': filtering_settings.get('time_limit', 2.0),
            'filter_date': filtering_settings.get('filter_date', datetime.now().date().isoformat())
        }

        logger.info(f"Filtering config: amount={config['filter_amount']}, type={config['workflow_type']}, enabled={config['enabled']}")
        return config

    except Exception as e:
        logger.error(f"Error getting filtering config: {e}")
        return {
            'enabled': True,
            'filter_amount': 100,
            'workflow_type': 'messages',
            'content_types': ['messages', 'replies'],
            'extraction_window': 24,
            'time_limit': 2.0
        }
def get_link_setting(key, default=None):
    """Retrieve link filtering settings from MongoDB or return default."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']
        settings_doc = db['settings'].find_one({'category': 'link_filtering'})
        if settings_doc and 'settings' in settings_doc:
            retrieved_setting = settings_doc['settings'].get(key, default)
            return retrieved_setting
        return default
    except Exception as e:
        logger.warning(f"Error retrieving link setting {key} from MongoDB: {e}")
        return default
    finally:
        if client:
            client.close()

def update_link_setting(key, value):
    """Update a link filtering setting in MongoDB."""
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Update the setting in MongoDB under link_filtering category
        db.settings.update_one(
            {'category': 'link_filtering'},
            {'$set': {f'settings.{key}': value, 'updated_at': datetime.now()}},
            upsert=True
        )

        logger.info(f"Successfully updated link setting: {key}")

    except Exception as e:
        logger.error(f"Error updating link setting {key} in MongoDB: {e}")
        raise e
    finally:
        if client:
            client.close()

def get_weekly_workflow_config(day=None):
    """Get weekly workflow configuration for a specific day or current day."""
    try:
        # Get day name
        if day is None:
            day = date.today().strftime('%A').lower()
        else:
            day = day.lower()

        # Get weekly workflow settings
        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        # Get day-specific config
        day_config = weekly_settings.get(day, {})

        # Add day metadata
        day_config['day'] = day.capitalize()
        day_config['day_key'] = day

        return day_config

    except Exception as e:
        logger.error(f"Error getting weekly workflow config: {e}")
        return {}

def get_scheduled_content_config(date_str: str = None) -> dict:
    """Get scheduled content configuration for a specific date"""
    try:
        if not date_str:
            date_str = datetime.now().strftime('%Y-%m-%d')

        scheduled_configs = get_system_setting('scheduled_content_configs', {})
        return scheduled_configs.get(date_str, {})

    except Exception as e:
        logger.error(f"Error getting scheduled content config for {date_str}: {e}")
        return {}
def get_execution_setting(setting_key=None, default=None):
    """
    Get execution settings from weekly_workflow_settings for the current day.
    STRICT MODE: Returns None if settings don't exist - NO DEFAULTS.

    If setting_key is provided, returns that specific setting.
    Otherwise returns all execution settings for today.
    """
    try:
        from datetime import date

        # Get current day
        current_day = date.today().strftime('%A').lower()

        # Get weekly workflow settings
        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        if not weekly_settings:
            logger.error(f"❌ CRITICAL: No weekly_workflow_settings found in database!")
            return None

        # Get day-specific configuration
        day_config = weekly_settings.get(current_day, None)

        if day_config is None:
            logger.error(f"❌ CRITICAL: No configuration found for {current_day}!")
            return None

        # If specific setting requested, return it
        if setting_key:
            if setting_key not in day_config:
                logger.error(f"❌ CRITICAL: Setting '{setting_key}' not found for {current_day}!")
                return None
            return day_config.get(setting_key)

        # Validate all required fields exist
        required_fields = [
            'time_between_workflows',
            'daily_limit',
            'time_limit',
            'workflows_to_process',
            'gap_between_workflows'
        ]

        missing_fields = [field for field in required_fields if field not in day_config]
        if missing_fields:
            logger.error(f"❌ CRITICAL: Missing required fields for {current_day}: {missing_fields}")
            return None

        # Return all execution-related settings
        execution_settings = {
            'execution_date': date.today().isoformat(),
            'time_between_workflows': day_config['time_between_workflows'],
            'daily_limit': day_config['daily_limit'],
            'time_limit': day_config['time_limit'],
            'workflows_to_process': day_config['workflows_to_process'],
            'gap_between_workflows': day_config['gap_between_workflows'],
            'enabled': day_config.get('enabled', True),
            'day': current_day,
            'config_date': day_config.get('config_date', date.today().isoformat())
        }

        return execution_settings

    except Exception as e:
        logger.error(f"❌ Error getting execution setting: {e}")
        return None


def update_execution_setting(settings):
    """
    Update execution settings in weekly_workflow_settings for the current day.
    Updates: time_between_workflows, daily_limit, time_limit, workflows_to_process
    """
    try:
        from datetime import date

        # Get current day
        current_day = date.today().strftime('%A').lower()

        # Get existing weekly workflow settings
        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        if not weekly_settings:
            weekly_settings = {}

        # Get current day settings or create new
        day_settings = weekly_settings.get(current_day, {})

        # Update only execution-related settings
        if 'time_between_workflows' in settings:
            day_settings['time_between_workflows'] = settings['time_between_workflows']

        if 'daily_limit' in settings:
            day_settings['daily_limit'] = settings['daily_limit']

        if 'time_limit' in settings:
            day_settings['time_limit'] = settings['time_limit']

        if 'workflows_to_process' in settings:
            day_settings['workflows_to_process'] = settings['workflows_to_process']

        if 'gap_between_workflows' in settings:
            day_settings['gap_between_workflows'] = settings['gap_between_workflows']

        # Update metadata
        day_settings['config_date'] = settings.get('execution_date', date.today().isoformat())
        day_settings['day_name'] = current_day.capitalize()
        day_settings['day_key'] = current_day

        # Keep existing settings that we don't update here
        if 'enabled' not in day_settings:
            day_settings['enabled'] = True

        # Update weekly settings with modified day settings
        weekly_settings[current_day] = day_settings

        # Save back to system settings
        update_system_setting('weekly_workflow_settings', weekly_settings)

        logger.info(f"Execution settings updated successfully for {current_day}")
        return True

    except Exception as e:
        logger.error(f"Error updating execution settings: {e}")
        return False


def get_all_execution_settings():
    """
    Get execution settings for all days of the week.
    Returns a dictionary with day names as keys.
    """
    try:
        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        if not weekly_settings:
            return {}

        execution_settings_by_day = {}

        for day in ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']:
            day_config = weekly_settings.get(day, {})

            if day_config:
                execution_settings_by_day[day] = {
                    'time_between_workflows': day_config.get('time_between_workflows', 15),
                    'daily_limit': day_config.get('daily_limit', 200),
                    'time_limit': day_config.get('time_limit', 2.0),
                    'workflows_to_process': day_config.get('workflows_to_process', 15),
                    'gap_between_workflows': day_config.get('gap_between_workflows', 300),
                    'enabled': day_config.get('enabled', True),
                    'config_date': day_config.get('config_date', ''),
                    'workflow_type': day_config.get('workflow_type', 'messages'),
                    'filter_amount': day_config.get('filter_amount', 100)
                }

        return execution_settings_by_day

    except Exception as e:
        logger.error(f"Error getting all execution settings: {e}")
        return {}
def get_combined_config():
    """
    Get combined configuration prioritizing weekly_workflow_settings over filtering_settings.
    Returns config with source information.
    """
    try:
        from datetime import date

        # Get current day
        current_day = date.today().strftime('%A').lower()

        # Try weekly workflow settings first
        weekly_settings = get_system_setting('weekly_workflow_settings', {})
        day_config = weekly_settings.get(current_day, {})

        if day_config and day_config.get('enabled', False):
            # Weekly config exists and is enabled - use it
            filter_amount = day_config.get('filter_amount', 100)
            workflow_type = day_config.get('workflow_type', 'messages')

            return {
                'source': 'weekly_workflow_settings',
                'day': current_day,
                'enabled': True,
                'filter_amount': filter_amount,
                'workflow_type': workflow_type,
                'content_amount': day_config.get('content_amount', 5),
                'extraction_window': day_config.get('extraction_window', 24),
                'time_limit': day_config.get('time_limit', 2.0),
                'content_types': day_config.get('content_types', ['messages', 'replies']),
                'gap_between_workflows': day_config.get('gap_between_workflows', 300),
                'raw_config': day_config
            }

        # Fallback to filtering_settings if weekly config not available
        filtering_settings = get_system_setting('filtering_settings', {})
        if filtering_settings:
            logger.info("Using filtering_settings as fallback")
            return {
                'source': 'filtering_settings',
                'day': current_day,
                'enabled': filtering_settings.get('enabled', True),
                'filter_amount': filtering_settings.get('filter_amount', 100),
                'workflow_type': filtering_settings.get('workflow_type', 'messages'),
                'extraction_window': filtering_settings.get('extraction_window', 24),
                'time_limit': filtering_settings.get('time_limit', 2.0),
                'content_types': filtering_settings.get('content_types', ['messages', 'replies']),
                'raw_config': filtering_settings
            }

        # Ultimate fallback - default values
        logger.warning("No configuration found, using defaults")
        return {
            'source': 'defaults',
            'day': current_day,
            'enabled': True,
            'filter_amount': 100,
            'workflow_type': 'messages',
            'content_amount': 5,
            'extraction_window': 24,
            'time_limit': 2.0,
            'content_types': ['messages', 'replies'],
            'gap_between_workflows': 300
        }

    except Exception as e:
        logger.error(f"Error getting combined config: {e}")
        return {
            'source': 'error_defaults',
            'day': 'unknown',
            'enabled': True,
            'filter_amount': 100,
            'workflow_type': 'messages',
            'content_amount': 5,
            'extraction_window': 24,
            'time_limit': 2.0,
            'content_types': ['messages', 'replies']
        }

# --------------------------------------------------------------
#  NEW / UPDATED FUNCTIONS – keep everything else you already have
# --------------------------------------------------------------

# Add this to your settings_manager.py to REPLACE the existing get_extraction_setting

# Add this to your settings_manager.py to REPLACE the existing get_extraction_setting

def get_extraction_setting(key, default=None):
    """
    Retrieve an extraction setting from the **extraction** category.
    ✅ FIXED: Properly handles extraction_schedule with nested format
    """
    client = None
    try:
        client = MongoClient(
            os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        doc = db.settings.find_one({'category': 'extraction'})
        if doc and 'settings' in doc:
            val = doc['settings'].get(key, default)

            # Handle extraction_schedule → {global_url, dates: {date: {times: [...]}}}
            if key == 'extraction_schedule':
                if isinstance(val, dict):
                    # Ensure proper structure
                    if 'global_url' not in val:
                        val['global_url'] = ''
                    if 'dates' not in val:
                        val['dates'] = {}
                    return val
                # Return empty structure if not dict
                return {'global_url': '', 'dates': {}}

            return val

        # Default for extraction_schedule
        if key == 'extraction_schedule':
            return {'global_url': '', 'dates': {}}
        return default

    except Exception as e:
        logger.warning(f"Error reading extraction setting {key}: {e}")
        if key == 'extraction_schedule':
            return {'global_url': '', 'dates': {}}
        return default
    finally:
        if client:
            client.close()


def update_extraction_setting(key, value):
    """
    Write an extraction setting under the **extraction** category.
    ✅ FIXED: Validates extraction_schedule structure
    """
    client = None
    try:
        client = MongoClient(
            os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Validation for extraction_schedule
        if key == 'extraction_schedule':
            if not isinstance(value, dict):
                raise ValueError("extraction_schedule must be a dict")
            if 'global_url' not in value or 'dates' not in value:
                raise ValueError("extraction_schedule needs 'global_url' and 'dates'")

            # Validate each date entry
            for d, cfg in value['dates'].items():
                if not isinstance(cfg.get('times'), list):
                    raise ValueError(f"date {d} → 'times' must be a list")
                for t in cfg['times']:
                    if not isinstance(t, dict) or 'time' not in t:
                        raise ValueError(f"Invalid time entry under {d}")

        db.settings.update_one(
            {'category': 'extraction'},
            {'$set': {f'settings.{key}': value, 'updated_at': datetime.now()}},
            upsert=True
        )
        logger.info(f"✅ Extraction setting '{key}' saved successfully")
    except Exception as e:
        logger.error(f"❌ Failed to save extraction setting '{key}': {e}")
        raise
    finally:
        if client:
            client.close()

def update_extraction_setting(key, value):
    """
    Write an extraction setting under the **extraction** category.
    ✅ FIXED: Validates extraction_schedule structure
    """
    client = None
    try:
        client = MongoClient(
            os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'),
            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Validation for extraction_schedule
        if key == 'extraction_schedule':
            if not isinstance(value, dict):
                raise ValueError("extraction_schedule must be a dict")
            if 'global_url' not in value or 'dates' not in value:
                raise ValueError("extraction_schedule needs 'global_url' and 'dates'")

            # Validate each date entry
            for d, cfg in value['dates'].items():
                if not isinstance(cfg.get('times'), list):
                    raise ValueError(f"date {d} → 'times' must be a list")
                for t in cfg['times']:
                    if not isinstance(t, dict) or 'time' not in t:
                        raise ValueError(f"Invalid time entry under {d}")

        db.settings.update_one(
            {'category': 'extraction'},
            {'$set': {f'settings.{key}': value, 'updated_at': datetime.now()}},
            upsert=True
        )
        logger.info(f"✅ Extraction setting '{key}' saved successfully")
    except Exception as e:
        logger.error(f"❌ Failed to save extraction setting '{key}': {e}")
        raise
    finally:
        if client:
            client.close()

def update_extraction_setting(key, value):
    """Write an extraction setting under the **extraction** category."""
    client = None
    try:
        client = MongoClient(
            os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # simple validation for the schedule
        if key == 'extraction_schedule':
            if not isinstance(value, dict):
                raise ValueError("extraction_schedule must be a dict")
            if 'global_url' not in value or 'dates' not in value:
                raise ValueError("extraction_schedule needs 'global_url' and 'dates'")

            # ensure each date entry is valid
            for d, cfg in value['dates'].items():
                if not isinstance(cfg.get('times'), list):
                    raise ValueError(f"date {d} → 'times' must be a list")
                for t in cfg['times']:
                    if not isinstance(t, dict) or 'time' not in t:
                        raise ValueError(f"Invalid time entry under {d}")

        db.settings.update_one(
            {'category': 'extraction'},
            {'$set': {f'settings.{key}': value, 'updated_at': datetime.now()}},
            upsert=True
        )
        logger.info(f"Extraction setting {key} saved")
    except Exception as e:
        logger.error(f"Failed to save extraction setting {key}: {e}")
        raise
    finally:
        if client:
            client.close()


def _migrate_old_extraction_schedule(old):
    """
    Convert the legacy flat format
        {'number_of_times':2, 'schedule_times':[{'time':'09:00','link':''}, ...]}
    into the new nested format (one entry for *today*).
    """
    if not isinstance(old, dict):
        return {'global_url': '', 'dates': {}}

    times = old.get('schedule_times', [])
    today = date.today().isoformat()
    new_times = [{'time': t.get('time', '09:00')} for t in times]
    return {
        'global_url': old.get('schedule_times', [{}])[0].get('link', ''),
        'dates': {today: {'times': new_times}}
    }


def get_all_extraction_settings():
    """
    Get all extraction settings from MongoDB.
    Returns a dictionary with all extraction configuration.
    """
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        settings_doc = db['settings'].find_one({'category': 'extraction'})

        if settings_doc and 'settings' in settings_doc:
            return settings_doc['settings']

        # Return empty dict if no settings found
        return {}

    except Exception as e:
        logger.error(f"Error getting all extraction settings: {e}")
        return {}
    finally:
        if client:
            client.close()


def delete_extraction_setting(key):
    """
    Delete a specific extraction setting from MongoDB.
    """
    client = None
    try:
        client = MongoClient(os.getenv('MONGODB_URI', DEFAULT_SETTINGS['system']['mongodb_uri']),
                            serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        db = client['messages_db']

        # Remove the specific setting
        result = db.settings.update_one(
            {'category': 'extraction'},
            {'$unset': {f'settings.{key}': ''}, '$set': {'updated_at': datetime.now()}}
        )

        if result.modified_count > 0:
            logger.info(f"Successfully deleted extraction setting: {key}")
            return True
        else:
            logger.warning(f"Extraction setting not found: {key}")
            return False

    except Exception as e:
        logger.error(f"Error deleting extraction setting {key}: {e}")
        return False
    finally:
        if client:
            client.close()


def get_extraction_schedule_for_today():
    """
    Get today's extraction schedule with times and links.
    Returns list of scheduled extractions for today.
    """
    try:
        from datetime import datetime

        extraction_schedule = get_extraction_setting('extraction_schedule', {})

        if not extraction_schedule:
            logger.warning("No extraction schedule configured")
            return []

        schedule_times = extraction_schedule.get('schedule_times', [])

        # Add metadata for today
        today_date = datetime.now().date().isoformat()

        result = []
        for i, schedule in enumerate(schedule_times, 1):
            result.append({
                'extraction_number': i,
                'time': schedule.get('time', '09:00'),
                'link': schedule.get('link', ''),
                'scheduled_date': today_date,
                'status': 'pending'  # Can be: pending, running, completed, failed
            })

        return result

    except Exception as e:
        logger.error(f"Error getting extraction schedule for today: {e}")
        return []
