"""
FIXED Filter Links Report DAG - ROOT CAUSE FIXES:

[Previous bug fix comments remain the same...]

CRITICAL BUG 8 (ACCOUNT DEDUP FIX - 2026-03-09):
    The real bug was in _get_filtered_workflows_with_links() where we were 
    deduplicating by link_id only (seen_link_ids set), which discarded all but 
    one account's workflow for each link. Since filter_links correctly creates 
    one workflow_metadata doc per (account × link), we need to deduplicate by 
    (link_id, workflow_account_id) to keep all accounts.
    
    Fixed by:
    1. Creating a composite key (link_id, workflow_account_id) for deduplication
    2. Processing all assignments per link, not just the first one
    3. Adding each unique (link, account) combination to enriched_workflows
    4. Added diagnostic logging to show account distribution
"""

import sys
import os
import re
import logging
import io
import json
import random
import zipfile
from datetime import datetime, timedelta, timezone
from bson import ObjectId
from bson.errors import InvalidId

sys.path.append('/opt/airflow/src')

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from pymongo import MongoClient
from core.database.postgres.connection import get_postgres_connection
from streamlit.ui.settings.settings_manager import get_system_setting

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://app_user:app_password@mongodb:27017/messages_db')

DEFAULT_ARGS = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


# ============================================================================
# COLLECTION NAME NORMALISATION
# ============================================================================

_PERMANENT_DATE_RE = re.compile(r'_\d{8}$')


def _make_permanent_collection_name(name: str) -> str:
    """
    Strip trailing _YYYYMMDD date stamp and append _permanent.
    """
    if not name:
        return name
    cleaned = _PERMANENT_DATE_RE.sub('', name)
    return f"{cleaned}_permanent" if cleaned != name else name


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def get_mongo_connection():
    client = MongoClient(MONGODB_URI)
    return client['messages_db']


def safe_object_id(value):
    """
    Safely convert a value to ObjectId without double-wrapping.
    """
    if isinstance(value, ObjectId):
        return value
    try:
        return ObjectId(str(value))
    except (InvalidId, TypeError, ValueError) as e:
        logger.warning(f"Could not convert to ObjectId: {value!r} — {e}")
        return None


def get_system_setting_mongo(key, default=None):
    """Get system setting from MongoDB."""
    try:
        mongo_db = get_mongo_connection()
        result = mongo_db['system_settings'].find_one({'key': key})
        return result.get('value', default) if result else default
    except Exception as e:
        logger.error(f"Error getting system setting {key}: {e}")
        return default


def get_account_username_map():
    """Fetch account_id to username mapping from PostgreSQL."""
    account_map = {}
    try:
        from psycopg2.extras import RealDictCursor
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT account_id, username, x_account_id FROM accounts ORDER BY account_id")
                for row in cur.fetchall():
                    account_map[str(row['account_id'])] = {
                        'username': row.get('username') or f"account_{row['account_id']}",
                        'x_account_id': row.get('x_account_id'),
                    }
        logger.info(f"✅ Loaded {len(account_map)} account mappings from PostgreSQL")
    except Exception as e:
        logger.error(f"❌ Error loading account names: {e}")
    return account_map


def send_email_report(subject, text_body, html_body=None, attachments=None):
    """Send email with optional attachments. Returns True on success."""
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders

    try:
        email_host     = os.getenv('EMAIL_HOST', 'smtp.gmail.com')
        email_port     = int(os.getenv('EMAIL_PORT', '587'))
        email_username = os.getenv('EMAIL_USERNAME')
        email_password = os.getenv('EMAIL_PASSWORD')
        email_from     = os.getenv('EMAIL_FROM', email_username)
        email_to       = os.getenv('EMAIL_TO', email_username)

        if not email_username or not email_password:
            logger.warning("⚠️ Email credentials not configured")
            return False

        msg            = MIMEMultipart('mixed')
        msg['From']    = email_from
        msg['To']      = email_to
        msg['Subject'] = subject

        alt_part = MIMEMultipart('alternative')
        alt_part.attach(MIMEText(text_body, 'plain'))
        if html_body:
            alt_part.attach(MIMEText(html_body, 'html'))
        msg.attach(alt_part)

        if attachments:
            for filename, file_bytes in attachments:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(file_bytes)
                encoders.encode_base64(part)
                part.add_header('Content-Disposition', f'attachment; filename="{filename}"')
                msg.attach(part)

        use_tls = os.getenv('EMAIL_USE_TLS', 'true').lower() == 'true'
        if use_tls:
            server = smtplib.SMTP(email_host, email_port)
            server.starttls()
        else:
            server = smtplib.SMTP_SSL(email_host, email_port)

        server.login(email_username, email_password)
        server.send_message(msg)
        server.quit()

        logger.info(f"✅ Email sent to {email_to}")
        return True

    except Exception as e:
        logger.error(f"❌ Failed to send email: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


# ============================================================================
# get_weekly_workflow_config() - unchanged
# ============================================================================

def get_weekly_workflow_config():
    """
    Get today's workflow config for the report DAG.
    """
    HARDCODED_DEFAULTS = {
        'filter_amount':   5,
        'category':        'x',
        'workflow_type':   'x',
        'collection_name': '',
    }

    try:
        current_day     = datetime.now().strftime('%A').lower()
        weekly_settings = get_system_setting('weekly_workflow_settings', {})
        day_config      = weekly_settings.get(current_day, {})

        automa_config    = day_config.get('automa_workflow_config', {})
        filtering_config = day_config.get('filtering_config', {})

        category = (
            automa_config.get('destination_category')
            or filtering_config.get('destination_category')
            or HARDCODED_DEFAULTS['category']
        )

        workflow_type = (
            automa_config.get('workflow_type')
            or filtering_config.get('workflow_type_name')
            or filtering_config.get('workflow_type')
            or HARDCODED_DEFAULTS['workflow_type']
        )

        raw_collection_name = (
            automa_config.get('collection_name')
            or filtering_config.get('collection_name')
            or HARDCODED_DEFAULTS['collection_name']
        )
        collection_name = _make_permanent_collection_name(raw_collection_name)
        if collection_name != raw_collection_name:
            logger.info(
                f"   Collection normalised: '{raw_collection_name}' -> '{collection_name}'"
            )

        filter_amount = filtering_config.get('filter_amount') or HARDCODED_DEFAULTS['filter_amount']
        try:
            filter_amount = int(filter_amount)
        except (TypeError, ValueError):
            filter_amount = HARDCODED_DEFAULTS['filter_amount']

        routing_source = (
            'automa_workflow_config' if automa_config.get('destination_category') else
            'filtering_config'       if filtering_config.get('destination_category') else
            'hardcoded_defaults'
        )

        config = {
            'enabled':         True,
            'filter_amount':   filter_amount,
            'category':        category,
            'workflow_type':   workflow_type,
            'collection_name': collection_name,
            'day':             current_day,
            'filtering_config': {
                'destination_category': category,
                'workflow_type_name':   workflow_type,
                'collection_name':      collection_name,
                'filter_amount':        filter_amount,
            },
        }

        logger.info(f"📅 Report config for {current_day}:")
        logger.info(f"   Category:        {category}")
        logger.info(f"   Workflow type:   {workflow_type}")
        logger.info(f"   Collection:      {collection_name or '(any)'}")
        logger.info(f"   Filter amount:   {filter_amount}")
        logger.info(f"   Routing source:  {routing_source}")

        return config

    except Exception as e:
        logger.error(f"Error loading weekly config, using hardcoded defaults: {e}")
        return {
            'enabled':         True,
            'filter_amount':   HARDCODED_DEFAULTS['filter_amount'],
            'category':        HARDCODED_DEFAULTS['category'],
            'workflow_type':   HARDCODED_DEFAULTS['workflow_type'],
            'collection_name': HARDCODED_DEFAULTS['collection_name'],
            'day':             datetime.now().strftime('%A').lower(),
            'filtering_config': {
                'destination_category': HARDCODED_DEFAULTS['category'],
                'workflow_type_name':   HARDCODED_DEFAULTS['workflow_type'],
                'collection_name':      HARDCODED_DEFAULTS['collection_name'],
                'filter_amount':        HARDCODED_DEFAULTS['filter_amount'],
            },
        }


# ============================================================================
# CORE: FETCH ELIGIBLE WORKFLOWS
# CRITICAL BUG 8 FIX: Deduplicate by (link_id, account_id), not just link_id
# ============================================================================

def _get_filtered_workflows_with_links(category=None, workflow_type=None, collection_name=None):
    from psycopg2.extras import RealDictCursor

    if not category:
        logger.error("❌ No category — cannot select workflows.")
        return []
    if not workflow_type:
        logger.error("❌ No workflow_type — cannot select workflows.")
        return []

    mongo_db = get_mongo_connection()
    account_map = get_account_username_map()

    logger.info("=" * 60)
    logger.info("FETCHING ELIGIBLE WORKFLOWS FOR REPORT")
    logger.info(f"  category:        {category}")
    logger.info(f"  workflow_type:   {workflow_type}")
    logger.info(f"  collection_name: {collection_name or '(any)'}")
    logger.info("=" * 60)

    # ── Step 1: get eligible link IDs from PostgreSQL ─────────────────────────
    eligible_links_query = """
        SELECT
            l.links_id, l.link, l.tweet_id, l.tweeted_date, l.tweeted_time,
            l.workflow_type, l.within_limit, l.account_id as link_account_id,
            l.processed_by_workflow, l.executed, l.workflow_status,
            l.chat_link
        FROM links l
        WHERE l.within_limit = TRUE
          AND l.filtered = TRUE
          AND l.workflow_status = 'completed'
          AND COALESCE(l.executed, FALSE) = FALSE
        ORDER BY l.tweeted_date DESC, l.links_id ASC
    """

    with get_postgres_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(eligible_links_query)
            eligible_links = cursor.fetchall()

    if not eligible_links:
        logger.info("No eligible links found in PostgreSQL")
        return []

    logger.info(f"✅ Found {len(eligible_links)} eligible links in PostgreSQL")
    
    # Create a lookup map for link data keyed by links_id
    link_data_map = {}
    link_ids = []
    for link in eligible_links:
        link_id = link['links_id']
        link_ids.append(link_id)
        link_data_map[link_id] = {
            'link_account_id': link['link_account_id'],
            'link_url': link['link'],
            'tweet_id': link['tweet_id'],
            'tweeted_date': link['tweeted_date'],
            'tweeted_time': link['tweeted_time'],
            'chat_link': link.get('chat_link'),
        }
        
        # Add username for this link's account if available
        aid_str = str(link['link_account_id']) if link['link_account_id'] else 'unknown'
        if aid_str in account_map:
            link_data_map[link_id]['link_account_username'] = account_map[aid_str]['username']
        else:
            link_data_map[link_id]['link_account_username'] = f"account_{link['link_account_id']}"

    # ── Step 2: find matching workflow_metadata docs in MongoDB ───────────────
    mongo_filter = {
        "postgres_content_id": {"$in": link_ids},
        "has_content":         True,
        "has_link":            True,
        "executed":            {"$ne": True},
        "status":              {"$in": [
            "ready_to_execute",
            "generated",
            "content_confirmed",
        ]},
        "category":            category.lower(),
        "workflow_type":       workflow_type,
    }

    if collection_name:
        mongo_filter["collection_name"] = collection_name

    logger.info(f"MongoDB filter: {mongo_filter}")

    workflow_assignments = list(
        mongo_db.workflow_metadata.find(mongo_filter).sort("link_assigned_at", -1)
    )

    logger.info(f"MongoDB returned {len(workflow_assignments)} assignment docs")

    # ── Diagnostic: if still 0, log what IS in the collection ─────────────────
    if not workflow_assignments:
        logger.warning("⚠️ Zero results — running diagnostic query...")
        sample = list(mongo_db.workflow_metadata.find(
            {"postgres_content_id": {"$in": link_ids[:5]}},
            {"postgres_content_id": 1, "category": 1, "workflow_type": 1,
             "has_link": 1, "has_content": 1, "status": 1, "executed": 1,
             "collection_name": 1, "postgres_account_id": 1, "content_items": 1}
        ).limit(10))

        if sample:
            logger.warning(f"Sample docs for those link_ids (first {len(sample)}):")
            for doc in sample:
                content_types = [item.get('content_type') for item in doc.get('content_items', [])]
                logger.warning(
                    f"  link_id={doc.get('postgres_content_id')} "
                    f"account={doc.get('postgres_account_id')} "
                    f"cat={doc.get('category')} "
                    f"wf_type={doc.get('workflow_type')} "
                    f"has_link={doc.get('has_link')} "
                    f"has_content={doc.get('has_content')} "
                    f"status={doc.get('status')} "
                    f"executed={doc.get('executed')} "
                    f"collection={doc.get('collection_name')} "
                    f"content_types={content_types}"
                )
        else:
            logger.warning(f"No docs at all in workflow_metadata for these link_ids: {link_ids[:5]}")
            any_docs = list(mongo_db.workflow_metadata.find(
                {},
                {"postgres_content_id": 1, "category": 1, "workflow_type": 1, "status": 1}
            ).limit(5))
            logger.warning(f"Sample of ALL workflow_metadata docs: {any_docs}")

        return []

    # ── Step 3: CRITICAL BUG 8 FIX - Deduplicate by (link_id, account_id) ─────
    # We want to keep ALL account-specific workflows, not just one per link
    enriched_workflows = []
    
    # Track unique (link_id, account_id) combinations
    seen_combinations = set()
    
    # Diagnostic: Count assignments per link before dedup
    link_assignment_counts = {}
    for assignment in workflow_assignments:
        link_id = assignment.get('postgres_content_id')
        account_id = assignment.get('postgres_account_id')
        if link_id and account_id:
            link_assignment_counts[link_id] = link_assignment_counts.get(link_id, 0) + 1
    
    logger.info(f"📊 Raw MongoDB assignments per link (before dedup):")
    for link_id, count in list(link_assignment_counts.items())[:10]:  # First 10 only
        logger.info(f"  Link {link_id}: {count} account workflows")
    
    # Process each assignment
    for assignment in workflow_assignments:
        link_id = assignment.get('postgres_content_id')
        account_id = assignment.get('postgres_account_id')
        
        if not link_id or not account_id:
            logger.warning(f"Skipping assignment with missing link_id ({link_id}) or account_id ({account_id})")
            continue
        
        # Create composite key for deduplication
        composite_key = (link_id, account_id)
        
        if composite_key in seen_combinations:
            logger.debug(f"Already processed link {link_id} for account {account_id}, skipping duplicate")
            continue
        
        # Get link data from our map
        link_data = link_data_map.get(link_id, {})
        if not link_data:
            logger.warning(f"No link data found for link_id {link_id}, skipping")
            continue
        
        try:
            database_name     = assignment.get('database_name', 'execution_workflows')
            actual_collection = assignment.get('collection_name')
            automa_wf_id      = assignment.get('automa_workflow_id')
            
            if not actual_collection or not automa_wf_id:
                logger.warning(
                    f"Skipping assignment {assignment.get('_id')} — "
                    f"missing collection ({actual_collection!r}) or workflow ID ({automa_wf_id!r})"
                )
                continue
            
            workflow_doc = mongo_db.client[database_name][actual_collection].find_one(
                {'_id': automa_wf_id}
            )
            
            if workflow_doc:
                # Extract content types from metadata
                content_items = assignment.get('content_items', [])
                content_types = []
                content_previews = []
                
                for item in content_items:
                    content_type = item.get('content_type', 'unknown')
                    content_text = item.get('content_text', '')
                    preview = content_text[:50] + '...' if len(content_text) > 50 else content_text
                    
                    content_types.append({
                        'label': item.get('label', 'unknown'),
                        'type': content_type,
                        'preview': preview,
                        'full_text': content_text
                    })
                    content_previews.append(f"{item.get('label')}: {content_type}")
                
                enriched_workflows.append({
                    'metadata':                assignment,
                    'workflow':                workflow_doc,
                    'workflow_id':              str(automa_wf_id),
                    'workflow_name':            workflow_doc.get('name', assignment.get('workflow_name', 'Unknown')),
                    'link_url':                 link_data['link_url'],
                    'link_id':                   link_id,
                    'chat_link':                 link_data.get('chat_link'),
                    'assigned_at':               assignment.get('link_assigned_at'),
                    'category':                  assignment.get('category', ''),
                    'workflow_type':              assignment.get('workflow_type', ''),
                    'collection_name':            actual_collection,
                    'database_name':              database_name,
                    # CRITICAL: Use the workflow's account_id (executing account)
                    'link_account_id':            link_data.get('link_account_id'),  # Original extractor (informational only)
                    'link_account_username':      link_data.get('link_account_username', f"account_{link_data.get('link_account_id')}"),
                    'workflow_account_id':        account_id,  # The account that will execute the workflow
                    'workflow_account_username':  account_map.get(str(account_id), {}).get('username', f"account_{account_id}"),
                    'tweet_id':                    link_data['tweet_id'],
                    'tweeted_date':                link_data['tweeted_date'],
                    'content_types':               content_types,
                    'content_types_summary':       ', '.join(content_previews),
                })
                seen_combinations.add(composite_key)
                
                logger.debug(f"Added workflow for link {link_id} (account {account_id})")
            else:
                logger.warning(
                    f"Workflow document not found in {database_name}.{actual_collection} "
                    f"for automa_workflow_id={automa_wf_id}"
                )
        
        except Exception as e:
            logger.error(f"Error enriching workflow for link {link_id}, account {account_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            continue
    
    logger.info(f"✅ Final enriched workflows count: {len(enriched_workflows)} (unique link+account combinations)")
    
    # Final account distribution
    account_dist = {}
    for wf in enriched_workflows:
        aid = str(wf.get('workflow_account_id', 'unknown'))
        account_dist[aid] = account_dist.get(aid, 0) + 1
    
    logger.info(f"📊 Final workflow distribution by executing account: {account_dist}")
    
    return enriched_workflows


# ============================================================================
# CORE: BUILD MASTER WORKFLOW JSON
# ============================================================================

def _round_delay(min_ms, max_ms, step=1000):
    min_step = max(0, round(min_ms / step))
    max_step = max(min_step, round(max_ms / step))
    return random.randint(min_step, max_step) * step


def _create_master_workflow_with_delays(filtered_workflows, template_data, gap_config):
    """Build master workflow JSON with delay nodes between each sub-workflow."""
    min_gap = gap_config.get('min_milliseconds', 0)
    max_gap = gap_config.get('max_milliseconds', 10000)

    master_workflow = {
        "extVersion":        template_data.get("extVersion", "1.30.00"),
        "name":              "master_execution",
        "icon":              "riGlobalLine",
        "table":             [],
        "version":           template_data.get("version", "1.30.00"),
        "settings":          template_data.get("settings", {}),
        "globalData":        template_data.get("globalData", '{\n\t"key": "value"\n}'),
        "description":       f"Master workflow - {len(filtered_workflows)} workflows with delays",
        "includedWorkflows": {}
    }

    nodes = []
    edges = []

    nodes.append({
        "id":          "master_trigger",
        "type":        "BlockBasic",
        "initialized": False,
        "position":    {"x": 50, "y": 200},
        "data": {
            "disableBlock": False, "description": "", "type": "manual",
            "interval": 60, "delay": 5, "date": "", "time": "00:00",
            "url": "", "shortcut": "", "activeInInput": False, "isUrlRegex": False,
            "days": [], "contextMenuName": "", "contextTypes": [],
            "parameters": [], "preferParamsInTab": False,
            "observeElement": {
                "selector": "", "baseSelector": "", "matchPattern": "",
                "targetOptions": {"subtree": False, "childList": True, "attributes": False, "attributeFilter": [], "characterData": False},
                "baseElOptions": {"subtree": False, "childList": True, "attributes": False, "attributeFilter": [], "characterData": False}
            }
        },
        "label": "trigger"
    })

    x_position       = 350
    y_position       = 200
    previous_node_id = "master_trigger"

    for idx, wf in enumerate(filtered_workflows):
        exec_node_id = f"exec_{idx}"

        nodes.append({
            "id":          exec_node_id,
            "type":        "BlockBasic",
            "initialized": False,
            "position":    {"x": x_position, "y": y_position},
            "data": {
                "disableBlock":        False,
                "executeId":           "",
                "workflowId":          wf['workflow_id'],
                "globalData":          "",
                "description":         f"Execute {wf['workflow_name']}",
                "insertAllVars":       False,
                "insertAllGlobalData": False
            },
            "label": "execute-workflow"
        })

        edges.append({
            "id":           f"edge_{previous_node_id}_to_{exec_node_id}",
            "type":         "custom",
            "source":       previous_node_id,
            "target":       exec_node_id,
            "sourceHandle": f"{previous_node_id}-output-1",
            "targetHandle": f"{exec_node_id}-input-1",
            "updatable":    True,
            "selectable":   True,
            "data":         {},
            "label":        "",
            "markerEnd":    "arrowclosed",
            "class":        "connected-edges"
        })

        master_workflow["includedWorkflows"][wf['workflow_id']] = wf['workflow']

        if idx < len(filtered_workflows) - 1:
            delay_node_id = f"delay_{idx}"
            delay_time    = _round_delay(min_gap, max_gap, step=1000)

            nodes.append({
                "id":          delay_node_id,
                "type":        "BlockDelay",
                "initialized": False,
                "position":    {"x": x_position + 300, "y": y_position - 25},
                "data":        {"disableBlock": False, "time": delay_time},
                "label":       "delay"
            })

            edges.append({
                "id":           f"edge_{exec_node_id}_to_{delay_node_id}",
                "type":         "custom",
                "source":       exec_node_id,
                "target":       delay_node_id,
                "sourceHandle": f"{exec_node_id}-output-1",
                "targetHandle": f"{delay_node_id}-input-1",
                "updatable":    True,
                "selectable":   True,
                "data":         {},
                "label":        "",
                "markerEnd":    "arrowclosed",
                "class":        f"source-{exec_node_id}-output-1 target-{delay_node_id}-input-1"
            })

            x_position      += 600
            previous_node_id = delay_node_id
        else:
            previous_node_id = exec_node_id

        y_position += 100

    master_workflow["drawflow"] = {
        "nodes":    nodes,
        "edges":    edges,
        "position": [0, 0],
        "zoom":     1.0,
        "viewport": {"x": 0, "y": 0, "zoom": 1.0}
    }

    return master_workflow


def _create_zip_bytes(filtered_workflows, master_workflow):
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.writestr('master_execution.json', json.dumps(master_workflow, indent=2, default=str))
        for wf in filtered_workflows:
            name = wf['workflow_name'].replace(' ', '_').replace('/', '_')
            zf.writestr(f'{name}.json', json.dumps(wf['workflow'], indent=2, default=str))
    zip_buffer.seek(0)
    return zip_buffer.read()


# ============================================================================
# ATOMIC MARK-AS-EXECUTED — called only after email is confirmed sent
# ============================================================================

def _mark_workflows_as_executed_after_email(filtered_workflows):
    """
    Mark links as executed in BOTH PostgreSQL AND MongoDB.
    Called only after email is confirmed sent.
    """
    from psycopg2.extras import RealDictCursor

    mongo_db     = get_mongo_connection()
    link_ids     = []
    metadata_ids = []

    for wf in filtered_workflows:
        postgres_content_id = wf['metadata'].get('postgres_content_id')
        if postgres_content_id:
            link_ids.append(postgres_content_id)

        raw_id = wf['metadata'].get('_id')
        oid    = safe_object_id(raw_id)
        if oid:
            metadata_ids.append(oid)
        else:
            logger.warning(f"Skipping invalid metadata _id: {raw_id!r}")

    if not link_ids:
        logger.error("No link_ids found — nothing to mark as executed")
        return {'success': False, 'postgres_updated': 0, 'mongo_updated': 0, 'error': 'No link IDs'}

    logger.info(f"Marking {len(link_ids)} links as executed in PostgreSQL...")
    logger.info(f"Marking {len(metadata_ids)} metadata docs as executed in MongoDB...")

    # ---- PostgreSQL ----
    postgres_updated = 0
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = 'links' AND column_name IN ('success', 'failure')
                """)
                existing_cols = {r['column_name'] for r in cursor.fetchall()}
                if 'success' not in existing_cols:
                    cursor.execute("ALTER TABLE links ADD COLUMN success BOOLEAN DEFAULT FALSE")
                if 'failure' not in existing_cols:
                    cursor.execute("ALTER TABLE links ADD COLUMN failure BOOLEAN DEFAULT FALSE")

                cursor.execute("""
                    UPDATE links SET
                        executed = TRUE,
                        processed_by_workflow = TRUE,
                        workflow_status = 'completed',
                        workflow_processed_time = CURRENT_TIMESTAMP,
                        success = TRUE,
                        failure = FALSE
                    WHERE links_id = ANY(%s)
                      AND COALESCE(executed, FALSE) = FALSE
                    RETURNING links_id
                """, (link_ids,))

                updated_rows     = cursor.fetchall()
                postgres_updated = len(updated_rows)
                conn.commit()

        logger.info(f"✅ PostgreSQL: {postgres_updated} links marked as executed")
        skipped = len(link_ids) - postgres_updated
        if skipped > 0:
            logger.warning(f"⚠️ {skipped} links already marked executed in PostgreSQL — skipped")

    except Exception as e:
        logger.error(f"❌ PostgreSQL update failed: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {'success': False, 'postgres_updated': 0, 'mongo_updated': 0, 'error': str(e)}

    # ---- MongoDB ----
    mongo_updated = 0
    if metadata_ids:
        try:
            result = mongo_db.workflow_metadata.update_many(
                {
                    '_id':      {'$in': metadata_ids},
                    'executed': {'$ne': True}
                },
                {'$set': {
                    'executed':         True,
                    'success':          True,
                    'executed_at':      datetime.now().isoformat(),
                    'status':           'completed',
                    'updated_at':       datetime.now().isoformat(),
                    'execution_mode':   'dag_auto_download',
                    'execution_source': 'airflow_dag'
                }}
            )
            mongo_updated = result.modified_count
            logger.info(f"✅ MongoDB: {mongo_updated} metadata docs marked as executed")
        except Exception as e:
            logger.error(f"❌ MongoDB update failed: {e}")
    else:
        logger.warning("No valid MongoDB ObjectIds to update")

    return {
        'success':          True,
        'postgres_updated': postgres_updated,
        'mongo_updated':    mongo_updated,
        'link_ids':         link_ids,
        'timestamp':        datetime.now().isoformat()
    }


# ============================================================================
# STEP 1: GENERATE WORKFLOW ZIP
# ============================================================================

MANUAL_TEMPLATE_FILE_PATHS = [
    '/opt/airflow/src/templates/manual_orchestrator_template.automa.json',
    '/app/src/templates/manual_orchestrator_template.automa.json',
    os.path.join(os.path.dirname(__file__), '..', 'src', 'templates', 'manual_orchestrator_template.automa.json'),
    os.path.join(os.path.dirname(__file__), 'src', 'templates', 'manual_orchestrator_template.automa.json'),
]


def _load_manual_template_from_file_dag():
    for path in MANUAL_TEMPLATE_FILE_PATHS:
        normalized = os.path.normpath(path)
        if os.path.isfile(normalized):
            try:
                with open(normalized, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                logger.info(f"DAG: Loaded manual template from {normalized}")
                return data
            except Exception as e:
                logger.warning(f"DAG: Found file at {normalized} but failed to parse: {e}")
    return None


def _get_or_auto_save_manual_template_dag():
    saved = get_system_setting_mongo('manual_execution_template', None)
    if saved:
        return saved

    logger.info("No manual_execution_template in MongoDB — trying file auto-load...")
    file_data = _load_manual_template_from_file_dag()

    if not file_data:
        logger.warning("DAG: Template file not found.")
        return None

    template_to_save = {
        'template_data':         file_data,
        'uploaded_at':           datetime.now().isoformat(),
        'template_name':         file_data.get('name', 'manual_orchestrator_template'),
        'source':                'auto_loaded_from_file_dag',
        'gap_between_workflows': {
            'min_milliseconds': 0,
            'max_milliseconds': 10000
        }
    }

    try:
        mongo_db = get_mongo_connection()
        mongo_db['system_settings'].update_one(
            {'key': 'manual_execution_template'},
            {'$set': {'value': template_to_save, 'updated_at': datetime.now().isoformat()}},
            upsert=True
        )
        logger.info("DAG: Auto-saved manual template from file to MongoDB")
    except Exception as e:
        logger.warning(f"DAG: Could not save template to MongoDB: {e}")

    return template_to_save


# ============================================================================
# STEP 2: GENERATE WORKFLOW ZIP
# ============================================================================

def generate_workflow_zip(**kwargs):
    """
    Fetch workflows grouped by account, build one master JSON per account,
    create one ZIP per account, save all to /tmp.
    """
    try:
        config           = get_weekly_workflow_config()
        filtering_config = config.get('filtering_config', {})
        account_map      = get_account_username_map()

        logger.info(
            f"generate_workflow_zip using: "
            f"category={filtering_config.get('destination_category')}, "
            f"workflow_type={filtering_config.get('workflow_type_name')}, "
            f"collection={filtering_config.get('collection_name')}"
        )

        manual_template = _get_or_auto_save_manual_template_dag()

        if not manual_template:
            logger.warning("⚠️ No manual execution template — skipping ZIP generation")
            kwargs['ti'].xcom_push(key='zip_skipped',       value=True)
            kwargs['ti'].xcom_push(key='workflow_count',    value=0)
            kwargs['ti'].xcom_push(key='account_zip_index', value={})
            return

        template_data = manual_template.get('template_data', {})
        gap_config    = manual_template.get('gap_between_workflows', {
            'min_milliseconds': 0,
            'max_milliseconds': 10000
        })

        all_workflows = _get_filtered_workflows_with_links(
            category=filtering_config.get('destination_category'),
            workflow_type=filtering_config.get('workflow_type_name'),
            collection_name=filtering_config.get('collection_name'),
        )

        if not all_workflows:
            logger.info("No eligible workflows found — skipping ZIP")
            kwargs['ti'].xcom_push(key='zip_skipped',       value=True)
            kwargs['ti'].xcom_push(key='workflow_count',    value=0)
            kwargs['ti'].xcom_push(key='account_zip_index', value={})
            return

        # Group workflows by workflow_account_id (executing account)
        by_account: dict = {}
        for wf in all_workflows:
            aid = str(wf.get('workflow_account_id') or 'unknown')
            by_account.setdefault(aid, []).append(wf)

        logger.info(f"Found {len(all_workflows)} workflow(s) across {len(by_account)} account(s) (by executing account)")
        logger.info(f"Account distribution: { {aid: len(wfs) for aid, wfs in by_account.items()} }")

        now          = datetime.now()
        ts           = now.strftime('%Y-%m-%d_%H-%M-%S')
        day_str      = now.strftime('%A')

        account_zip_index = {}
        all_serialisable = []

        for account_id, acct_workflows in by_account.items():
            # Get username for this account
            username = account_map.get(account_id, {}).get('username', f"account_{account_id}")
            # Sanitize username for filename
            safe_username = re.sub(r'[^\w\-_]', '_', username)
            
            logger.info(f"  Building ZIP for account {account_id} ({username}) — {len(acct_workflows)} workflow(s)")

            master_workflow = _create_master_workflow_with_delays(
                acct_workflows, template_data, gap_config
            )
            zip_bytes    = _create_zip_bytes(acct_workflows, master_workflow)
            
            zip_filename = f"workflows_{safe_username}_{account_id}_{ts}_{day_str}.zip"
            tmp_path     = f"/tmp/{zip_filename}"

            with open(tmp_path, 'wb') as f:
                f.write(zip_bytes)

            logger.info(
                f"  ✅ ZIP for account {account_id} ({username}): {tmp_path} "
                f"({len(zip_bytes)} bytes, {len(acct_workflows)} workflows)"
            )

            account_zip_index[str(account_id)] = {
                'tmp_path':       tmp_path,
                'zip_filename':   zip_filename,
                'workflow_count': len(acct_workflows),
                'username':       username,
                'safe_username':  safe_username,
            }

            for wf in acct_workflows:
                meta = wf['metadata']
                all_serialisable.append({
                    'link_id':                wf['link_id'],
                    'workflow_id':            wf['workflow_id'],
                    'workflow_name':          wf['workflow_name'],
                    'link_url':               wf['link_url'],
                    'chat_link':              wf.get('chat_link'),
                    'category':               wf['category'],
                    'workflow_type':          wf['workflow_type'],
                    'tweeted_date':           str(wf['tweeted_date']) if wf['tweeted_date'] else None,
                    'assigned_at':            str(wf['assigned_at'])  if wf['assigned_at']  else None,
                    # CRITICAL: Store both account IDs but group by workflow_account_id
                    'link_account_id':        str(wf.get('link_account_id', 'unknown')),
                    'link_account_username':  wf.get('link_account_username', 'unknown'),
                    'workflow_account_id':    account_id,
                    'workflow_account_username': username,
                    'content_types':          wf.get('content_types', []),
                    'content_types_summary':  wf.get('content_types_summary', ''),
                    'metadata': {
                        '_id':                 str(meta.get('_id', '')),
                        'postgres_content_id': meta.get('postgres_content_id'),
                    }
                })

        kwargs['ti'].xcom_push(key='zip_skipped',            value=False)
        kwargs['ti'].xcom_push(key='workflow_count',         value=len(all_workflows))
        kwargs['ti'].xcom_push(key='account_zip_index',      value=account_zip_index)
        kwargs['ti'].xcom_push(key='serialisable_workflows', value=all_serialisable)

    except Exception as e:
        logger.error(f"❌ Error generating workflow ZIPs: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


# ============================================================================
# STEP 3: GENERATE TEXT REPORT
# ============================================================================

def collect_simple_filter_data(**kwargs):
    """Collect report data, now grouped by executing account."""
    try:
        ti                     = kwargs['ti']
        config                 = get_weekly_workflow_config()
        current_day            = config.get('day', datetime.now().strftime('%A').lower())
        filtering_config       = config.get('filtering_config', {})

        serialisable_workflows = ti.xcom_pull(key='serialisable_workflows', task_ids='generate_workflow_zip') or []
        zip_skipped            = ti.xcom_pull(key='zip_skipped',            task_ids='generate_workflow_zip') or False
        workflow_count         = ti.xcom_pull(key='workflow_count',         task_ids='generate_workflow_zip') or 0
        account_zip_index      = ti.xcom_pull(key='account_zip_index',      task_ids='generate_workflow_zip') or {}

        end_time   = datetime.now(timezone.utc)
        start_time = end_time - timedelta(hours=24)

        # Fetch account username map from PostgreSQL
        account_name_map = get_account_username_map()

        # Group serialisable workflows by workflow_account_id (executing account)
        by_account: dict = {}
        for wf in serialisable_workflows:
            aid = str(wf.get('workflow_account_id', 'unknown'))
            by_account.setdefault(aid, []).append({
                'link_id':                wf['metadata'].get('postgres_content_id'),
                'url':                    wf.get('link_url', 'N/A'),
                'chat_link':               wf.get('chat_link'),
                'tweeted_date':            wf.get('tweeted_date', 'N/A'),
                'assigned_at':             wf.get('assigned_at', 'N/A'),
                'workflow_name':           wf.get('workflow_name', 'N/A'),
                'workflow_type':           wf.get('workflow_type', 'N/A'),
                'category':                wf.get('category', 'N/A'),
                'status_badge':             '📦 Selected for ZIP',
                'link_account_id':          wf.get('link_account_id', 'unknown'),
                'link_account_username':    wf.get('link_account_username', 'unknown'),
                'workflow_account_id':      aid,
                'workflow_account_username': wf.get('workflow_account_username', f"account_{aid}"),
                'content_types':             wf.get('content_types', []),
                'content_types_summary':     wf.get('content_types_summary', ''),
            })

        accounts_summary = []
        for aid, links in by_account.items():
            zip_info = account_zip_index.get(aid, {})
            acct_info = account_name_map.get(aid, {})
            accounts_summary.append({
                'account_id':           aid,
                'username':             acct_info.get('username', f'account_{aid}'),
                'x_account_id':         acct_info.get('x_account_id'),
                'workflow_count':       len(links),
                'zip_filename':         zip_info.get('zip_filename'),
                'zip_username':         zip_info.get('username', acct_info.get('username')),
                'links':                links,
            })

        data = {
            'day':         current_day.title(),
            'report_time': end_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
            'time_range': {
                'start': start_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'end':   end_time.strftime('%Y-%m-%d %H:%M:%S UTC'),
                'hours': 24,
            },
            'filtering_config': {
                'category':        filtering_config.get('destination_category', 'N/A'),
                'workflow_type':   filtering_config.get('workflow_type_name',   'N/A'),
                'collection_name': filtering_config.get('collection_name',      'All'),
            },
            'zip_skipped':           zip_skipped,
            'workflow_count':        workflow_count,
            'total_links_selected':  len(serialisable_workflows),
            'total_accounts':        len(by_account),
            'accounts_summary':      accounts_summary,
            'links': [wf for acct in accounts_summary for wf in acct['links']],
        }

        logger.info(f"📊 Report data: {data['total_accounts']} executing accounts, {data['total_links_selected']} total workflows")
        logger.info(f"📊 Accounts in report: {[a['account_id'] for a in accounts_summary]}")

        kwargs['ti'].xcom_push(key='filter_data', value=data)
        return data

    except Exception as e:
        logger.error(f"❌ Error collecting filter data: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def generate_simple_text_report(**kwargs):
    """Generate text report with chat links and content types."""
    try:
        ti                = kwargs['ti']
        data              = ti.xcom_pull(key='filter_data',       task_ids='collect_simple_data')
        zip_skipped       = ti.xcom_pull(key='zip_skipped',       task_ids='generate_workflow_zip') or False
        workflow_count    = ti.xcom_pull(key='workflow_count',    task_ids='generate_workflow_zip') or 0
        account_zip_index = ti.xcom_pull(key='account_zip_index', task_ids='generate_workflow_zip') or {}

        if not data:
            raise Exception("No filter data found")

        fc = data.get('filtering_config', {})
        accounts_summary = data.get('accounts_summary', [])

        lines = [
            "=" * 80,
            "LINK FILTERING REPORT",
            "=" * 80,
            "",
            f"Day:                     {data.get('day', 'Unknown')}",
            f"Report Generated:        {data.get('report_time', 'Unknown')}",
            f"Period:                  Last {data.get('time_range', {}).get('hours', 24)} hours",
            f"                         {data['time_range']['start']} to {data['time_range']['end']}",
            "",
            "=" * 80,
            "SELECTION FILTERS APPLIED",
            "=" * 80,
            f"Category:                {fc.get('category', 'N/A')}",
            f"Workflow Type:           {fc.get('workflow_type', 'N/A')}",
            f"Collection:              {fc.get('collection_name', 'All')}",
            "",
            "=" * 80,
            "SUMMARY",
            "=" * 80,
            f"Total Executing Accounts: {data.get('total_accounts', 0)}",
            f"Total Workflows:          {workflow_count}",
            f"Total Links Selected:     {data.get('total_links_selected', 0)}",
            "",
        ]

        if zip_skipped:
            lines.append("⚠️  No ZIPs generated (no eligible workflows or template missing)")
        else:
            lines.append("ZIP PACKAGES (one per executing account):")
            for aid, zip_info in account_zip_index.items():
                acct = next((a for a in accounts_summary if str(a['account_id']) == str(aid)), {})
                username = zip_info.get('username', acct.get('username', f'account_{aid}'))
                lines.append(
                    f"  Account {aid} ({username}): "
                    f"{zip_info['zip_filename']} — {zip_info['workflow_count']} workflow(s)"
                )
            lines.append("")

        lines += ["=" * 80, "PER-ACCOUNT BREAKDOWN (by executing account)", "=" * 80, ""]

        for acct in accounts_summary:
            aid       = acct['account_id']
            username  = acct['username']
            x_acct_id = acct.get('x_account_id') or 'NOT SET'
            wf_count  = acct['workflow_count']
            zip_info  = account_zip_index.get(str(aid), {})

            lines += [
                f"Executing Account {aid}: {username}",
                f"  x_account_id (DM sender): {x_acct_id}",
                f"  Workflows assigned:       {wf_count}",
                f"  ZIP:                      {zip_info.get('zip_filename', 'N/A')}",
                "",
            ]

            for idx, link in enumerate(acct['links'], 1):
                # Format content types for easy copying
                content_types_display = ""
                if link.get('content_types'):
                    content_lines = []
                    for ct in link['content_types']:
                        label = ct.get('label', 'unknown')
                        ctype = ct.get('type', 'unknown')
                        content_lines.append(f"        {label}: {ctype}")
                    content_types_display = "\n" + "\n".join(content_lines)
                else:
                    content_types_display = " None"

                # Format chat link
                chat_link_display = link.get('chat_link', 'Not available')
                
                lines += [
                    f"  [{idx}] Link ID: {link.get('link_id')} — {link.get('status_badge', '')}",
                    f"       URL:          {link.get('url')}",
                    f"       Chat Link:    {chat_link_display}",
                    f"       Content Types:{content_types_display}",
                    f"       Tweeted Date: {link.get('tweeted_date')}",
                    f"       Assigned At:  {link.get('assigned_at')}",
                    f"       Workflow:     {link.get('workflow_name')}",
                    f"       Type:         {link.get('workflow_type')}",
                    f"       Link Owner:   {link.get('link_account_username', 'unknown')} (ID: {link.get('link_account_id', 'unknown')})",
                    "",
                ]

        lines += ["=" * 80, "END OF REPORT", "=" * 80]

        text_report = "\n".join(lines)
        kwargs['ti'].xcom_push(key='text_report', value=text_report)
        return text_report

    except Exception as e:
        logger.error(f"❌ Error generating text report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


# ============================================================================
# STEP 4: GENERATE HTML REPORT
# ============================================================================

def generate_simple_html_report(**kwargs):
    """Generate HTML report with copyable content types and chat links."""
    try:
        ti                = kwargs['ti']
        data              = ti.xcom_pull(key='filter_data',       task_ids='collect_simple_data')
        zip_skipped       = ti.xcom_pull(key='zip_skipped',       task_ids='generate_workflow_zip') or False
        workflow_count    = ti.xcom_pull(key='workflow_count',    task_ids='generate_workflow_zip') or 0
        account_zip_index = ti.xcom_pull(key='account_zip_index', task_ids='generate_workflow_zip') or {}

        if not data:
            raise Exception("No filter data found")

        fc               = data.get('filtering_config', {})
        accounts_summary = data.get('accounts_summary', [])
        total_accounts   = data.get('total_accounts', 0)

        # Build ZIP summary banner
        if zip_skipped:
            zip_section = '<div class="zip-banner zip-warn">⚠️ No ZIPs generated — no eligible workflows or template missing.</div>'
        else:
            zip_rows = ""
            for aid, zip_info in account_zip_index.items():
                acct = next((a for a in accounts_summary if str(a['account_id']) == str(aid)), {})
                username = zip_info.get('username', acct.get('username', f'account_{aid}'))
                zip_rows += (
                    f"<tr><td><strong>{username}</strong> (ID: {aid})</td>"
                    f"<td>{zip_info['zip_filename']}</td>"
                    f"<td>{zip_info['workflow_count']} workflow(s)</td></tr>"
                )
            zip_section = f"""
<div class="zip-banner zip-ok">
  📦 <strong>{len(account_zip_index)} ZIP(s) attached</strong> — one per executing account &nbsp;|&nbsp; {workflow_count} total workflows
  <table class="zip-table">
    <thead><tr><th>Account</th><th>Filename</th><th>Workflows</th></tr></thead>
    <tbody>{zip_rows}</tbody>
  </table>
</div>
"""

        # Build per-account cards
        account_cards = ""
        for acct in accounts_summary:
            aid       = acct['account_id']
            username  = acct['username']
            x_acct_id = acct.get('x_account_id')
            wf_count  = acct['workflow_count']
            zip_info  = account_zip_index.get(str(aid), {})

            x_id_badge = (
                f'<span class="badge badge-ok">✅ {x_acct_id}</span>'
                if x_acct_id
                else '<span class="badge badge-warn">❌ x_account_id not set</span>'
            )

            link_cards = ""
            for idx, link in enumerate(acct['links'], 1):
                # Build content types display with copy buttons
                content_types_html = ""
                if link.get('content_types'):
                    for ct in link['content_types']:
                        label = ct.get('label', 'unknown')
                        ctype = ct.get('type', 'unknown')
                        preview = ct.get('preview', '')
                        full_text = ct.get('full_text', '')
                        
                        # Create a copyable content type block
                        content_types_html += f"""
                        <div class="content-type-item">
                            <span class="content-type-label">{label}:</span>
                            <span class="content-type-value" onclick="copyToClipboard(this)" title="Click to copy">{ctype}</span>
                            <span class="content-type-preview" title="{full_text}">{preview}</span>
                        </div>
                        """
                else:
                    content_types_html = '<div class="content-type-item">No content types</div>'

                # Chat link display with copy button
                chat_link = link.get('chat_link', '')
                chat_link_display = f'<a href="{chat_link}" target="_blank">{chat_link}</a>' if chat_link else 'Not available'
                chat_link_copy = f'<button class="copy-btn" onclick="copyToClipboard(\'{chat_link}\')">📋 Copy</button>' if chat_link else ''

                link_cards += f"""
<div class="link-card">
  <div class="link-header">
    <div class="link-number">#{idx} — Link ID: {link['link_id']}</div>
    <div class="status-badge">{link.get('status_badge', '📦 Selected')}</div>
  </div>
  <div class="link-url">
    <span class="url-label">Tweet URL:</span> 
    <a href="{link['url']}" target="_blank">{link['url']}</a>
    <button class="copy-btn" onclick="copyToClipboard('{link['url']}')">📋 Copy</button>
  </div>
  <div class="link-chat">
    <span class="chat-label">Chat Link:</span> 
    {chat_link_display}
    {chat_link_copy}
  </div>
  <div class="link-content-types">
    <span class="content-types-label">Content Types:</span>
    <div class="content-types-container">
      {content_types_html}
    </div>
  </div>
  <div class="link-details">
    <div class="detail-item"><span class="detail-label">Tweeted Date:</span><br>{link['tweeted_date']}</div>
    <div class="detail-item"><span class="detail-label">Assigned At:</span><br>{link['assigned_at']}</div>
    <div class="detail-item"><span class="detail-label">Workflow:</span><br>{link['workflow_name']}</div>
    <div class="detail-item"><span class="detail-label">Type:</span><br>{link['workflow_type']}</div>
    <div class="detail-item"><span class="detail-label">Link Owner:</span><br>{link.get('link_account_username', 'unknown')} (ID: {link.get('link_account_id', 'unknown')})</div>
  </div>
</div>
"""

            account_cards += f"""
<div class="account-card">
  <div class="account-header">
    <div class="account-title">👤 {username} <span class="account-id">ID: {aid}</span></div>
    <div class="account-meta">
      {x_id_badge}
      <span class="badge badge-info">{wf_count} workflow(s)</span>
      <span class="badge badge-zip">📦 {zip_info.get('zip_filename', 'N/A')}</span>
    </div>
  </div>
  <div class="account-body">
    {link_cards if link_cards else '<p class="no-links">No links for this account.</p>'}
  </div>
</div>
"""

        html = f"""<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
<style>
body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; max-width: 1200px; margin: 0 auto; padding: 20px; background: #f5f7fa; }}
.header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 30px; border-radius: 10px; margin-bottom: 25px; }}
.header h1 {{ margin: 0 0 10px 0; font-size: 28px; }}
.header p {{ margin: 5px 0; opacity: 0.95; font-size: 14px; }}
.zip-banner {{ padding: 15px 20px; border-radius: 10px; margin-bottom: 20px; font-size: 14px; font-weight: 500; }}
.zip-ok {{ background: #d4edda; color: #155724; border-left: 5px solid #28a745; }}
.zip-warn {{ background: #fff3cd; color: #856404; border-left: 5px solid #ffc107; }}
.zip-table {{ width: 100%; margin-top: 10px; border-collapse: collapse; font-size: 13px; }}
.zip-table th {{ background: rgba(0,0,0,0.08); padding: 6px 10px; text-align: left; }}
.zip-table td {{ padding: 6px 10px; border-top: 1px solid rgba(0,0,0,0.08); }}
.summary {{ background: white; padding: 25px; margin-bottom: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.08); }}
.summary h2 {{ color: #667eea; margin-top: 0; border-bottom: 2px solid #667eea; padding-bottom: 10px; }}
.filter-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin: 20px 0; }}
.filter-item {{ background: #f8f9fa; padding: 16px; border-radius: 8px; border-left: 4px solid #667eea; }}
.filter-label {{ font-size: 11px; text-transform: uppercase; color: #6c757d; margin-bottom: 6px; font-weight: 600; }}
.filter-value {{ font-size: 16px; font-weight: bold; color: #343a40; }}
.metric-grid {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin: 20px 0; }}
.metric {{ background: #f8f9fa; padding: 20px; border-radius: 8px; border-left: 4px solid #667eea; }}
.metric-label {{ font-size: 12px; text-transform: uppercase; color: #6c757d; margin-bottom: 8px; font-weight: 600; }}
.metric-value {{ font-size: 32px; font-weight: bold; color: #667eea; }}
.account-card {{ background: white; margin-bottom: 20px; border-radius: 10px; box-shadow: 0 2px 4px rgba(0,0,0,0.08); overflow: hidden; }}
.account-header {{ background: #667eea; color: white; padding: 16px 20px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 10px; }}
.account-title {{ font-size: 18px; font-weight: bold; }}
.account-id {{ font-size: 13px; opacity: 0.8; margin-left: 8px; }}
.account-meta {{ display: flex; gap: 8px; flex-wrap: wrap; align-items: center; }}
.badge {{ display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; }}
.badge-ok {{ background: #d4edda; color: #155724; }}
.badge-warn {{ background: #fff3cd; color: #856404; }}
.badge-info {{ background: rgba(255,255,255,0.25); color: white; }}
.badge-zip {{ background: rgba(255,255,255,0.15); color: white; font-size: 11px; }}
.account-body {{ padding: 20px; }}
.link-card {{ background: #f8f9fa; padding: 16px; margin: 10px 0; border-radius: 8px; border-left: 4px solid #667eea; }}
.link-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }}
.link-number {{ font-size: 15px; font-weight: bold; color: #667eea; }}
.status-badge {{ display: inline-block; padding: 4px 10px; border-radius: 12px; font-size: 12px; font-weight: 600; background: #cce5ff; color: #004085; }}
.link-url, .link-chat {{ margin: 8px 0; padding: 6px 8px; background: white; border-radius: 4px; display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }}
.url-label, .chat-label, .content-types-label {{ font-weight: 600; color: #495057; min-width: 100px; }}
.link-url a, .link-chat a {{ color: #667eea; word-break: break-all; flex: 1; }}
.copy-btn {{ background: #667eea; color: white; border: none; border-radius: 4px; padding: 4px 8px; cursor: pointer; font-size: 12px; }}
.copy-btn:hover {{ background: #5a67d8; }}
.link-content-types {{ margin: 8px 0; padding: 8px; background: white; border-radius: 4px; }}
.content-types-container {{ margin-top: 5px; }}
.content-type-item {{ display: flex; align-items: center; gap: 10px; padding: 4px 0; border-bottom: 1px dashed #e2e8f0; }}
.content-type-label {{ font-weight: 600; color: #4a5568; min-width: 80px; }}
.content-type-value {{ color: #667eea; cursor: pointer; padding: 2px 6px; border-radius: 4px; background: #ebf4ff; }}
.content-type-value:hover {{ background: #c3dafe; }}
.content-type-preview {{ color: #718096; font-size: 11px; flex: 1; }}
.link-details {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(150px, 1fr)); gap: 8px; margin-top: 8px; font-size: 12px; }}
.detail-item {{ padding: 6px; background: white; border-radius: 4px; }}
.detail-label {{ font-weight: 600; color: #6c757d; display: block; margin-bottom: 2px; }}
.no-links {{ color: #6c757d; font-style: italic; text-align: center; padding: 20px; }}
</style>
<script>
function copyToClipboard(text) {{
    if (typeof text === 'object') {{
        // If called from onclick with element
        text = text.innerText;
    }}
    navigator.clipboard.writeText(text).then(function() {{
        alert('Copied to clipboard: ' + text);
    }}, function(err) {{
        console.error('Could not copy text: ', err);
    }});
}}
</script>
</head>
<body>
<div class="header">
  <h1>🔗 Link Filtering Report</h1>
  <p><strong>Day:</strong> {data['day']}</p>
  <p><strong>Generated:</strong> {data['report_time']}</p>
  <p><strong>Period:</strong> {data['time_range']['start']} → {data['time_range']['end']}</p>
</div>
{zip_section}
<div class="summary">
  <h2>Selection Filters Applied</h2>
  <div class="filter-grid">
    <div class="filter-item"><div class="filter-label">Category</div><div class="filter-value">{fc.get('category', 'N/A')}</div></div>
    <div class="filter-item"><div class="filter-label">Workflow Type</div><div class="filter-value">{fc.get('workflow_type', 'N/A')}</div></div>
    <div class="filter-item"><div class="filter-label">Collection</div><div class="filter-value">{fc.get('collection_name', 'All')}</div></div>
  </div>
  <h2>Summary</h2>
  <div class="metric-grid">
    <div class="metric"><div class="metric-label">Total Executing Accounts</div><div class="metric-value">{total_accounts}</div></div>
    <div class="metric"><div class="metric-label">Total Workflows</div><div class="metric-value">{workflow_count}</div></div>
    <div class="metric"><div class="metric-label">Total Workflows</div><div class="metric-value">{data['total_links_selected']}</div></div>
  </div>
</div>
<h2 style="color:#667eea; margin: 20px 0 10px 0;">Per-Account Breakdown (by executing account)</h2>
{account_cards if account_cards else '<div class="no-links">No accounts had workflows assigned.</div>'}
</body></html>"""

        kwargs['ti'].xcom_push(key='html_report', value=html)
        return html

    except Exception as e:
        logger.error(f"❌ Error generating HTML report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


# ============================================================================
# STEP 5: SEND EMAIL
# ============================================================================

def send_simple_report_email(**kwargs):
    try:
        ti                     = kwargs['ti']
        data                   = ti.xcom_pull(key='filter_data',            task_ids='collect_simple_data')
        text_report            = ti.xcom_pull(key='text_report',            task_ids='generate_simple_text_report')
        html_report            = ti.xcom_pull(key='html_report',            task_ids='generate_simple_html_report')
        zip_skipped            = ti.xcom_pull(key='zip_skipped',            task_ids='generate_workflow_zip') or False
        workflow_count         = ti.xcom_pull(key='workflow_count',         task_ids='generate_workflow_zip') or 0
        account_zip_index      = ti.xcom_pull(key='account_zip_index',      task_ids='generate_workflow_zip') or {}
        serialisable_workflows = ti.xcom_pull(key='serialisable_workflows', task_ids='generate_workflow_zip') or []

        if not text_report or not html_report:
            logger.warning("⚠️ No reports found to send")
            return False

        total_accounts = data.get('total_accounts', 0)
        subject = (
            f"🔗 Links Report - {data['day']} - "
            f"{total_accounts} executing account(s) | "
            f"{data['total_links_selected']} workflows"
            + (f" | 📦 {len(account_zip_index)} ZIP(s)" if not zip_skipped else " | ⚠️ No ZIP")
        )

        # Attach one ZIP per account
        attachments = []
        if not zip_skipped:
            for aid, zip_info in account_zip_index.items():
                tmp_path = zip_info.get('tmp_path')
                if tmp_path and os.path.exists(tmp_path):
                    with open(tmp_path, 'rb') as f:
                        attachments.append((zip_info['zip_filename'], f.read()))
                    logger.info(f"📎 Attaching ZIP for account {aid} ({zip_info.get('username', '?')}): {zip_info['zip_filename']}")
                else:
                    logger.warning(f"ZIP file not found for account {aid}: {tmp_path}")

        email_sent = send_email_report(
            subject, text_report, html_report,
            attachments=attachments if attachments else None
        )

        if email_sent and serialisable_workflows:
            logger.info(f"✅ Email sent — marking {len(serialisable_workflows)} workflows as executed...")
            workflows_for_marking = [
                {
                    'metadata': {
                        '_id':                 wf['metadata']['_id'],
                        'postgres_content_id': wf['metadata']['postgres_content_id'],
                    }
                }
                for wf in serialisable_workflows
            ]
            mark_result = _mark_workflows_as_executed_after_email(workflows_for_marking)
            if mark_result.get('success'):
                logger.info(
                    f"✅ Marked — PG: {mark_result['postgres_updated']}, "
                    f"Mongo: {mark_result['mongo_updated']}"
                )
            else:
                logger.error(f"❌ Failed to mark: {mark_result.get('error')}")
        elif not email_sent:
            logger.warning("⚠️ Email failed — NOT marking links as executed. Will retry next run.")

        # Clean up all tmp ZIPs
        for aid, zip_info in account_zip_index.items():
            tmp_path = zip_info.get('tmp_path')
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)
                logger.info(f"🗑️ Cleaned up: {tmp_path}")

        ti.xcom_push(key='email_sent', value=email_sent)
        return email_sent

    except Exception as e:
        logger.error(f"❌ Error sending report: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def store_simple_report_metadata(**kwargs):
    try:
        ti                = kwargs['ti']
        data              = ti.xcom_pull(key='filter_data',       task_ids='collect_simple_data')
        email_sent        = ti.xcom_pull(key='email_sent',        task_ids='send_simple_report_email')
        workflow_count    = ti.xcom_pull(key='workflow_count',    task_ids='generate_workflow_zip') or 0
        zip_skipped       = ti.xcom_pull(key='zip_skipped',       task_ids='generate_workflow_zip') or False
        account_zip_index = ti.xcom_pull(key='account_zip_index', task_ids='generate_workflow_zip') or {}

        mongo_db = get_mongo_connection()

        report_doc = {
            'report_type':          'simple_link_filtering',
            'generated_at':         datetime.now(timezone.utc),
            'day':                  data['day'],
            'time_range':           data['time_range'],
            'filtering_config':     data.get('filtering_config', {}),
            'total_accounts':       data.get('total_accounts', 0),
            'total_links_selected': data['total_links_selected'],
            'zip_generated':        not zip_skipped,
            'zip_count':            len(account_zip_index),
            'zip_workflow_count':   workflow_count,
            'zip_filenames':        [v['zip_filename'] for v in account_zip_index.values()],
            'accounts_summary': [
                {
                    'account_id':     a['account_id'],
                    'username':       a['username'],
                    'workflow_count': a['workflow_count'],
                    'zip_filename':   a.get('zip_filename'),
                }
                for a in data.get('accounts_summary', [])
            ],
            'email_sent':  email_sent,
            'dag_run_id':  kwargs.get('dag_run').run_id if kwargs.get('dag_run') else None,
            'created_by':  'filtering_report_with_workflows'
        }

        result = mongo_db.filter_reports.insert_one(report_doc)
        logger.info(f"✅ Report metadata stored: {result.inserted_id}")
        return str(result.inserted_id)

    except Exception as e:
        logger.error(f"❌ Error storing report metadata: {e}")
        return None


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    'filtering_report_with_workflows',
    default_args=DEFAULT_ARGS,
    description=(
        'Generate workflow ZIP + send filtering report. '
        'CRITICAL FIX: Now keeps ALL account workflows (dedup by link_id + account_id). '
        'Includes chat links and copyable content types.'
    ),
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['reporting', 'filtering', 'email', 'links', 'zip', 'chat', 'content']
) as dag:

    start_task = DummyOperator(task_id='start')

    zip_task = PythonOperator(
        task_id='generate_workflow_zip',
        python_callable=generate_workflow_zip,
        provide_context=True,
    )

    collect_data_task = PythonOperator(
        task_id='collect_simple_data',
        python_callable=collect_simple_filter_data,
        provide_context=True,
    )

    generate_text_task = PythonOperator(
        task_id='generate_simple_text_report',
        python_callable=generate_simple_text_report,
        provide_context=True,
    )

    generate_html_task = PythonOperator(
        task_id='generate_simple_html_report',
        python_callable=generate_simple_html_report,
        provide_context=True,
    )

    send_email_task = PythonOperator(
        task_id='send_simple_report_email',
        python_callable=send_simple_report_email,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    store_metadata_task = PythonOperator(
        task_id='store_simple_report_metadata',
        python_callable=store_simple_report_metadata,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    end_task = DummyOperator(task_id='end', trigger_rule=TriggerRule.ALL_DONE)

    start_task >> zip_task >> collect_data_task
    collect_data_task >> [generate_text_task, generate_html_task]
    [generate_text_task, generate_html_task] >> send_email_task
    send_email_task >> store_metadata_task >> end_task
