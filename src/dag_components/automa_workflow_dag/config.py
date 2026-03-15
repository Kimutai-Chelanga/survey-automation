import os
import re
import logging
from datetime import datetime, date
from dotenv import load_dotenv
from typing import Dict, Any, List, Tuple, Optional
from bson import ObjectId

# Load environment variables
load_dotenv()

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/opt/airflow/logs/automa_workflow.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# MongoDB URI
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://app_user:app_password@mongodb:27017/messages_db?authSource=admin")

# Workflow generation setting
WORKFLOW_GENERATION_ENABLED = os.getenv("WORKFLOW_GENERATION_ENABLED", "true").lower() == "true"

# Default args for DAG - NO RETRIES, NO DELAYS
DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 3, 19, 39),
    'retries': 0,
}

# ============================================================================
# DEFAULT TIMING CONFIGS
# ============================================================================

DEFAULT_PRESS_KEYS     = {'mode': 'range', 'min_milliseconds': 0, 'max_milliseconds': 10000}
DEFAULT_CLICK_ELEMENTS = {'mode': 'range', 'min_milliseconds': 0, 'max_milliseconds': 10000}
DEFAULT_TYPING_DELAY   = {'mode': 'range', 'min_milliseconds': 0, 'max_milliseconds': 10000}


def _normalise_timing_config(cfg: dict, default: dict) -> dict:
    """
    Ensure a timing config dict has a valid 'mode' key and required ms fields.

    Handles legacy configs saved before 'mode' was introduced:
        {'min_milliseconds': 0, 'max_milliseconds': 10000}
        → {'mode': 'range', 'min_milliseconds': 0, 'max_milliseconds': 10000}

    Also handles legacy microseconds keys.
    """
    if not cfg:
        return dict(default)

    # Migrate microseconds → milliseconds (legacy)
    if 'min_microseconds' in cfg and 'min_milliseconds' not in cfg:
        cfg['min_milliseconds'] = cfg['min_microseconds'] // 1000
    if 'max_microseconds' in cfg and 'max_milliseconds' not in cfg:
        cfg['max_milliseconds'] = cfg['max_microseconds'] // 1000

    # Fill missing ms keys with defaults
    cfg.setdefault('min_milliseconds', default['min_milliseconds'])
    cfg.setdefault('max_milliseconds', default['max_milliseconds'])

    # Inject default mode if absent (backward compat — treat old configs as 'range')
    if 'mode' not in cfg:
        cfg['mode'] = 'range'

    # Sanitise mode value
    if cfg['mode'] not in ('range', 'fixed'):
        logger.warning(f"Unknown timing mode '{cfg['mode']}', falling back to 'range'")
        cfg['mode'] = 'range'

    return cfg


# ============================================================================
# PERMANENT COLLECTION NAME HELPER
# ============================================================================

_DATE_SUFFIX_RE = re.compile(r'_\d{8}$')


def make_permanent_collection_name(name: str) -> str:
    """
    Strip any trailing _YYYYMMDD date stamp and replace with _permanent.

    Examples:
        x_x_automation_template_20260302  →  x_x_automation_template_permanent
        x_x_automation_template_permanent →  x_x_automation_template_permanent (unchanged)
        my_collection                     →  my_collection (unchanged)
    """
    if not name:
        return name
    cleaned = _DATE_SUFFIX_RE.sub('', name)
    if cleaned != name:
        logger.info(f"Collection name normalised: '{name}' → '{cleaned}_permanent'")
        return f"{cleaned}_permanent"
    return name


# ============================================================================
# DYNAMIC CONFIGURATION FUNCTIONS
# ============================================================================

def get_today_automa_config():
    """
    Get today's Automa workflow configuration from settings.

    - Normalises collection_name to permanent (removes date stamp).
    - Normalises press_keys / click_elements / typing_delay to include 'mode' field.
    - Handles old single-content format transparently.
    """
    try:
        from streamlit.ui.settings.settings_manager import get_system_setting
        import traceback

        today       = date.today()
        current_day = today.strftime('%A').lower()
        logger.info(f"🔍 Looking for automa config for: {current_day} ({today})")

        weekly_settings = get_system_setting('weekly_workflow_settings', {})

        if not weekly_settings:
            logger.error("❌ No weekly_workflow_settings found in database!")
            return None

        logger.info(f"📋 Available days in weekly_settings: {list(weekly_settings.keys())}")

        day_config = weekly_settings.get(current_day, None)
        if not day_config:
            logger.error(f"❌ No configuration found for {current_day}!")
            return None

        logger.info(f"✅ Found day config for {current_day}")

        automa_config = day_config.get('automa_workflow_config', None)
        if not automa_config:
            logger.error(f"❌ No automa_workflow_config found for {current_day}!")
            return None

        # ── Normalise collection_name ─────────────────────────────────────────
        if 'collection_name' in automa_config:
            automa_config['collection_name'] = make_permanent_collection_name(
                automa_config['collection_name']
            )

        # ── Handle multi-content vs single-content ────────────────────────────
        if 'content_items' not in automa_config:
            if 'content_type' in automa_config and 'content_name' in automa_config:
                logger.info("📝 Converting old single-content format to new multi-content format")
                automa_config['content_items'] = [{
                    'label':        'CONTENT1',
                    'content_type': automa_config['content_type'],
                    'content_name': automa_config['content_name']
                }]
            else:
                logger.error("❌ Config has neither content_items nor content_type/content_name!")
                return None
        else:
            if len(automa_config['content_items']) > 0:
                first_item = automa_config['content_items'][0]
                automa_config['content_type'] = first_item['content_type']
                automa_config['content_name'] = first_item['content_name']

        # ── Verify date (warning only) ────────────────────────────────────────
        config_date_str = automa_config.get('config_date')
        if config_date_str:
            try:
                config_date = date.fromisoformat(config_date_str)
                if config_date != today:
                    logger.warning(
                        f"⚠️ Config date {config_date} doesn't match today {today} — "
                        "collection_name is permanent so this will still work"
                    )
            except Exception as date_error:
                logger.error(f"Invalid config_date format: {config_date_str} - {date_error}")

        # ── Normalise press_keys / click_elements / typing_delay ─────────────
        automa_config['press_keys'] = _normalise_timing_config(
            automa_config.get('press_keys', {}),
            DEFAULT_PRESS_KEYS
        )
        automa_config['click_elements'] = _normalise_timing_config(
            automa_config.get('click_elements', {}),
            DEFAULT_CLICK_ELEMENTS
        )
        automa_config['typing_delay'] = _normalise_timing_config(
            automa_config.get('typing_delay', {}),
            DEFAULT_TYPING_DELAY
        )

        # ── Validate required fields ──────────────────────────────────────────
        required_fields = [
            'content_items', 'source_category', 'workflow_source_id',
            'workflow_source_name', 'destination_category', 'workflow_type',
            'collection_name', 'press_keys', 'click_elements'
            # typing_delay is optional (added gracefully above with defaults)
        ]

        missing_fields = [f for f in required_fields if f not in automa_config]
        if missing_fields:
            logger.error(f"❌ Automa config missing required fields: {missing_fields}")
            return None

        logger.info(f"✅ Retrieved complete automa config for {current_day}")
        logger.info(f"   Collection (permanent): {automa_config['collection_name']}")
        logger.info(f"   Press keys mode:   {automa_config['press_keys']['mode']}")
        logger.info(f"   Click keys mode:   {automa_config['click_elements']['mode']}")
        logger.info(f"   Typing delay mode: {automa_config['typing_delay']['mode']}")
        for item in automa_config.get('content_items', []):
            logger.info(f"   - {item['label']}: {item['content_type']} / {item['content_name']}")

        return automa_config

    except ImportError as e:
        logger.error(f"❌ Import error: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Error getting today's automa config: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def get_template_from_mongodb(template_id):
    """Get workflow template from MongoDB workflow_templates collection."""
    try:
        from core.database.mongodb.connection import get_mongo_collection

        templates_collection = get_mongo_collection("workflow_templates")
        if templates_collection is None:
            logger.error("Failed to get workflow_templates collection")
            return None

        if isinstance(template_id, str):
            template_id = ObjectId(template_id)

        template = templates_collection.find_one({"_id": template_id})
        if not template:
            logger.error(f"Template not found: {template_id}")
            return None

        logger.info(f"✅ Retrieved template: {template.get('template_name')} (Category: {template.get('category')})")
        return template

    except Exception as e:
        logger.error(f"Error getting template from MongoDB: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def get_content_by_name_and_type(content_name, content_type, account_id=None):
    """
    Get unused content items from the content table by name and type.
    When account_id is provided, results are scoped strictly to that account.
    """
    try:
        from core.database.postgres.content_handler import get_content_handler

        handler       = get_content_handler(content_type)
        content_items = handler.get_comprehensive_data(account_id=account_id, used=False)
        filtered      = [i for i in content_items if i.get('content_name') == content_name]

        if account_id is not None:
            filtered = [i for i in filtered if i.get('account_id') == account_id]
            logger.info(
                f"Found {len(filtered)} unused items for "
                f"account={account_id}, name='{content_name}', type='{content_type}'"
            )
        else:
            logger.info(
                f"Found {len(filtered)} unused items for "
                f"name='{content_name}', type='{content_type}' (all accounts)"
            )

        return filtered

    except Exception as e:
        logger.error(f"Error getting content by name and type: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return []


def get_workflow_names_from_config(automa_config):
    """Generate workflow names based on configuration."""
    destination_category = automa_config.get('destination_category', 'workflow')
    workflow_type        = automa_config.get('workflow_type', 'default')
    content_items        = automa_config.get('content_items', [])

    if content_items and len(content_items) > 1:
        labels_str = "_".join([item.get('label', f'CONTENT{i+1}') for i, item in enumerate(content_items)])
        base_name  = f"{destination_category}_{workflow_type}_{labels_str}"
    else:
        base_name = f"{destination_category}_{workflow_type}"

    names = [
        f"{base_name}_one",   f"{base_name}_two",
        f"{base_name}_three", f"{base_name}_four",
        f"{base_name}_five",  f"{base_name}_six"
    ]

    logger.info(f"Generated workflow names: {names[:2]}... (6 total)")
    return names


def get_collection_name(automa_config):
    """Get the MongoDB collection name (permanent variant)."""
    collection_name = automa_config.get('collection_name')
    if not collection_name:
        logger.error("No collection_name in automa config!")
        return None
    collection_name = make_permanent_collection_name(collection_name)
    logger.info(f"Using collection: {collection_name}")
    return collection_name


def get_database_name(automa_config):
    """Get the MongoDB database name (defaults to 'execution_workflows')."""
    database_name = automa_config.get('database', 'execution_workflows')
    logger.info(f"Using database: {database_name}")
    return database_name


# ============================================================================
# VALIDATION
# ============================================================================

def validate_automa_config(automa_config):
    """
    Validate the automa configuration.
    Also normalises press_keys / click_elements / typing_delay to include 'mode' if absent.

    Returns:
        tuple: (is_valid: bool, error_message: str or None)
    """
    if not automa_config:
        return False, "Configuration is empty"

    required_fields = [
        'content_items', 'source_category', 'workflow_source_id',
        'workflow_source_name', 'destination_category', 'workflow_type',
        'collection_name', 'database'
    ]
    for field in required_fields:
        if field not in automa_config:
            return False, f"Missing required field: {field}"

    content_items = automa_config.get('content_items', [])
    if not isinstance(content_items, list) or len(content_items) == 0:
        return False, "content_items must be a non-empty list"

    if len(content_items) > 3:
        return False, "content_items cannot exceed 3 items"

    for i, item in enumerate(content_items):
        if not isinstance(item, dict):
            return False, f"content_items[{i}] must be a dictionary"
        if 'label' not in item or 'content_type' not in item or 'content_name' not in item:
            return False, f"content_items[{i}] missing required fields (label, content_type, content_name)"
        expected_label = f"CONTENT{i+1}"
        if item['label'] != expected_label:
            return False, f"content_items[{i}] has invalid label '{item['label']}', expected '{expected_label}'"

    # Normalise all three timing configs in place
    automa_config['press_keys']     = _normalise_timing_config(
        automa_config.get('press_keys', {}), DEFAULT_PRESS_KEYS
    )
    automa_config['click_elements'] = _normalise_timing_config(
        automa_config.get('click_elements', {}), DEFAULT_CLICK_ELEMENTS
    )
    automa_config['typing_delay']   = _normalise_timing_config(
        automa_config.get('typing_delay', {}), DEFAULT_TYPING_DELAY
    )

    return True, "Configuration is valid"


def log_configuration_summary():
    """Log a summary of the current configuration for debugging."""
    logger.info("=" * 80)
    logger.info("AUTOMA WORKFLOW CONFIGURATION SUMMARY")
    logger.info("=" * 80)

    automa_config = get_today_automa_config()
    if not automa_config:
        logger.error("❌ No automa configuration available!")
        logger.info("=" * 80)
        return

    is_valid, error_msg = validate_automa_config(automa_config)
    if not is_valid:
        logger.error(f"❌ Configuration validation failed: {error_msg}")
        logger.info("=" * 80)
        return

    logger.info(f"📅 Day:  {automa_config.get('day_name', 'Unknown')}")
    logger.info(f"📅 Date: {automa_config.get('config_date', 'Unknown')}")

    content_items = automa_config.get('content_items', [])
    logger.info(f"📝 Content Items: {len(content_items)}")
    for item in content_items:
        logger.info(f"   - {item['label']}: {item['content_type']} / {item['content_name']}")

    logger.info(f"📂 Source Category: {automa_config.get('source_category')}")
    logger.info(f"📄 Template:        {automa_config.get('workflow_source_name')}")
    logger.info(f"💾 Dest Category:   {automa_config.get('destination_category')}")
    logger.info(f"💾 Workflow Type:   {automa_config.get('workflow_type')}")
    logger.info(f"📁 Collection:      {automa_config.get('collection_name')}")
    logger.info(f"🗄️  Database:        {automa_config.get('database', 'execution_workflows')}")

    press  = automa_config.get('press_keys', {})
    p_mode = press.get('mode', 'range')
    if p_mode == 'fixed':
        logger.info(f"⌨️  Press Keys:    fixed {press.get('max_milliseconds', 0)/1000:.2f}s")
    else:
        logger.info(
            f"⌨️  Press Keys:    range "
            f"{press.get('min_milliseconds', 0)/1000:.2f}s – "
            f"{press.get('max_milliseconds', 10000)/1000:.2f}s"
        )

    click  = automa_config.get('click_elements', {})
    c_mode = click.get('mode', 'range')
    if c_mode == 'fixed':
        logger.info(f"🖱️  Click Elems:   fixed {click.get('max_milliseconds', 0)/1000:.2f}s")
    else:
        logger.info(
            f"🖱️  Click Elems:   range "
            f"{click.get('min_milliseconds', 0)/1000:.2f}s – "
            f"{click.get('max_milliseconds', 10000)/1000:.2f}s"
        )

    typing  = automa_config.get('typing_delay', {})
    t_mode  = typing.get('mode', 'range')
    if t_mode == 'fixed':
        logger.info(f"⌚ Typing Delay:  fixed {typing.get('max_milliseconds', 0)}ms")
    else:
        logger.info(
            f"⌚ Typing Delay:  range "
            f"{typing.get('min_milliseconds', 0)}ms – "
            f"{typing.get('max_milliseconds', 10000)}ms"
        )

    logger.info("=" * 80)
