"""
Filter Links DAG with Independent Time Windows
==============================================
- Has its own configurable time windows (can be different from extraction)
- Even if triggered by extraction, only runs if within its windows
- Manual triggers bypass window checks
"""

import sys
import os
import logging
from datetime import datetime, timedelta, timezone
import re
from typing import List, Dict, Any, Optional
from bson import ObjectId

sys.path.append('/opt/airflow/src')

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from pymongo import MongoClient
from core.database.postgres.connection import get_postgres_connection
from streamlit.ui.settings.settings_manager import get_system_setting

# ============================================================================
# MODULE-LEVEL CONSTANTS
# ============================================================================

MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin')
REQUIRED_TWEET_ID_LENGTH = 19

# Default filtering windows (can be any duration)
DEFAULT_FILTERING_WINDOWS = {
    "enabled": True,
    "morning_window": "06:00-10:00",
    "evening_window": "18:00-22:00",
}

_PERMANENT_DATE_RE = re.compile(r'_\d{8}$')

def _make_permanent_collection_name(name: str) -> str:
    """Strip trailing _YYYYMMDD date stamp and append _permanent."""
    if not name:
        return name
    cleaned = _PERMANENT_DATE_RE.sub('', name)
    return f"{cleaned}_permanent" if cleaned != name else name

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ============================================================================
# HARDCODED FALLBACK FILTER WORDS
# ============================================================================
HARDCODED_FILTER_WORDS = [
    'touchofm_', 'eileevalencia', 'record_spot1', 'brill_writers',
    'essayzpro', 'primewriters23a', 'essaygirl01', 'essaynasrah',
    'sharifwriter1', 'essaysastute', 'queentinabrown', 'analytics',
]

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}


# ============================================================================
# DATABASE CONNECTIONS
# ============================================================================

def get_postgres_connection():
    import psycopg2
    from urllib.parse import urlparse
    DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres:5432/messages")
    r = urlparse(DATABASE_URL)
    return psycopg2.connect(database=r.path[1:], user=r.username, password=r.password, host=r.hostname, port=r.port)


def get_mongo_connection():
    try:
        client = MongoClient(MONGODB_URI)
        return client['messages_db']
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise


# ============================================================================
# TIME WINDOW CHECK FOR FILTERING (INDEPENDENT)
# ============================================================================

def _parse_time_window(window_str: str) -> tuple:
    """Parse time window string like '06:00-10:00' into (start_time, end_time)"""
    try:
        s, e = window_str.split('-')
        start_time = datetime.strptime(s.strip(), '%H:%M').time()
        end_time = datetime.strptime(e.strip(), '%H:%M').time()
        return start_time, end_time
    except Exception as e:
        logger.error(f"Error parsing time window '{window_str}': {e}")
        return None, None


def _is_time_in_window(check_time: datetime, window_str: str) -> bool:
    """Check if a given time is within a time window string (e.g., '06:00-10:00')"""
    if not window_str:
        return False
    
    start_time, end_time = _parse_time_window(window_str)
    if not start_time or not end_time:
        return False
    
    current_time = check_time.time()
    
    # Handle windows that cross midnight (e.g., 22:00-02:00)
    if start_time <= end_time:
        # Normal window (same day)
        return start_time <= current_time <= end_time
    else:
        # Window crosses midnight
        return current_time >= start_time or current_time <= end_time


def _get_filtering_windows(weekly_cfg: dict, day_name: str) -> dict:
    """Get filtering windows for a specific day from filtering_config"""
    day_cfg = weekly_cfg.get(day_name, {})
    filtering_config = day_cfg.get('filtering_config', {})
    
    return {
        "enabled": filtering_config.get('windows_enabled', True),
        "morning_window": filtering_config.get('morning_window', DEFAULT_FILTERING_WINDOWS['morning_window']),
        "evening_window": filtering_config.get('evening_window', DEFAULT_FILTERING_WINDOWS['evening_window']),
    }


def _is_within_filtering_window(check_time: datetime, weekly_cfg: dict) -> tuple:
    """Check if current time is within filtering windows (flexible duration)"""
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    day_name = days[check_time.weekday()]
    
    filtering_cfg = _get_filtering_windows(weekly_cfg, day_name)

    if not filtering_cfg.get('enabled', True):
        return False, f"{day_name.title()} filtering is disabled"

    morning_window = filtering_cfg.get('morning_window', DEFAULT_FILTERING_WINDOWS['morning_window'])
    evening_window = filtering_cfg.get('evening_window', DEFAULT_FILTERING_WINDOWS['evening_window'])

    # Check morning window
    if _is_time_in_window(check_time, morning_window):
        start, end = _parse_time_window(morning_window)
        if start and end:
            if start <= end:
                duration = (datetime.combine(datetime.min, end) - datetime.combine(datetime.min, start)).seconds // 3600
            else:
                duration = (24 - start.hour + end.hour)
            return True, f"Inside morning filtering window ({morning_window}, {duration}h duration)"
        return True, f"Inside morning filtering window {morning_window}"

    # Check evening window
    if _is_time_in_window(check_time, evening_window):
        start, end = _parse_time_window(evening_window)
        if start and end:
            if start <= end:
                duration = (datetime.combine(datetime.min, end) - datetime.combine(datetime.min, start)).seconds // 3600
            else:
                duration = (24 - start.hour + end.hour)
            return True, f"Inside evening filtering window ({evening_window}, {duration}h duration)"
        return True, f"Inside evening filtering window {evening_window}"

    return False, (
        f"Outside filtering windows — morning: {morning_window}, "
        f"evening: {evening_window}, current: {check_time.strftime('%H:%M')}"
    )


def check_filtering_time_window(**kwargs):
    """
    Check if current time is within filtering windows.
    Even if triggered by extraction, filtering respects its own schedule.
    """
    dag_run = kwargs.get('dag_run')
    is_manual = dag_run and (dag_run.external_trigger or dag_run.run_type == 'manual')
    
    # Manual runs bypass time window check
    if is_manual:
        logger.info("Manual trigger — bypassing filtering time window check")
        return True
    
    current_time = datetime.now()
    weekly_cfg = get_system_setting('weekly_workflow_settings', {})
    
    in_window, reason = _is_within_filtering_window(current_time, weekly_cfg)
    
    logger.info("=" * 80)
    logger.info("FILTERING TIME WINDOW CHECK")
    logger.info(f"  Check time: {current_time.strftime('%A %H:%M:%S')}")
    logger.info(f"  In filtering window: {'YES' if in_window else 'NO'}")
    logger.info(f"  Reason: {reason}")
    logger.info("=" * 80)
    
    if not in_window:
        logger.info("Outside filtering windows — skipping filter_links execution")
        raise AirflowSkipException(f"Outside filtering windows: {reason}")
    
    logger.info("FILTERING ALLOWED — within configured windows")
    return True


# ============================================================================
# CHAT LINK HELPERS
# ============================================================================

def build_chat_link(x_account_id: Optional[str], tweet_author_user_id: Optional[str]) -> Optional[str]:
    if not x_account_id or not tweet_author_user_id:
        return None
    if not str(x_account_id).strip().isdigit():
        return None
    if not str(tweet_author_user_id).strip().isdigit():
        return None
    return f"https://x.com/i/chat/{tweet_author_user_id.strip()}-{x_account_id.strip()}"


def get_x_account_id_for_account(account_id: int) -> Optional[str]:
    """
    Look up x_account_id from the accounts table for a given account_id.
    Returns None if not set or on any error.
    """
    try:
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT x_account_id FROM accounts WHERE account_id = %s", (account_id,))
                row = cur.fetchone()
                if row:
                    val = row[0] if isinstance(row, tuple) else row.get('x_account_id')
                    if val and str(val).strip().isdigit():
                        logger.info(f"  x_account_id for account {account_id}: {val}")
                        return str(val).strip()
                    logger.warning(f"  account {account_id} x_account_id={val!r} — not a valid numeric ID")
                else:
                    logger.warning(f"  account {account_id} not found in accounts table")
                return None
    except Exception as e:
        logger.error(f"Error fetching x_account_id for account {account_id}: {e}")
        return None


# ============================================================================
# CONFIGURATION
# ============================================================================

def get_weekly_workflow_config():
    HARDCODED_DEFAULTS = {
        'filter_amount': 5, 'category': 'x', 'workflow_type': 'x', 'collection_name': '',
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
            logger.info(f"   Collection normalised: '{raw_collection_name}' -> '{collection_name}'")

        filter_amount = filtering_config.get('filter_amount') or HARDCODED_DEFAULTS['filter_amount']
        try:
            filter_amount = int(filter_amount)
        except (TypeError, ValueError):
            filter_amount = HARDCODED_DEFAULTS['filter_amount']

        config = {
            'enabled': True, 
            'filter_amount': filter_amount, 
            'category': category,
            'workflow_type': workflow_type, 
            'collection_name': collection_name, 
            'day': current_day,
        }
        logger.info(f"Config for {current_day}: filter_amount={filter_amount}")
        return config

    except Exception as e:
        logger.error(f"Error loading weekly config: {e}")
        return {
            'enabled': True, 
            'filter_amount': HARDCODED_DEFAULTS['filter_amount'],
            'category': HARDCODED_DEFAULTS['category'], 
            'workflow_type': HARDCODED_DEFAULTS['workflow_type'],
            'collection_name': HARDCODED_DEFAULTS['collection_name'],
            'day': datetime.now().strftime('%A').lower(),
        }


def get_dag_execution_config():
    try:
        settings     = get_system_setting('filter_links_execution_settings', {})
        selected_dag = settings.get('selected_dag', 'report_with_workflows')
        if selected_dag not in ['executor', 'report', 'report_with_workflows']:
            selected_dag = 'report_with_workflows'
        return {
            'selected_dag':                  selected_dag,
            'trigger_executor':              selected_dag == 'executor',
            'trigger_report':                selected_dag == 'report',
            'trigger_report_with_workflows': selected_dag == 'report_with_workflows',
        }
    except Exception as e:
        logger.error(f"Error loading DAG execution config: {e}")
        return {'selected_dag': 'report_with_workflows', 'trigger_executor': False,
                'trigger_report': False, 'trigger_report_with_workflows': True}


def decide_which_dag_to_trigger(**kwargs):
    config = get_dag_execution_config()
    kwargs['ti'].xcom_push(key='execution_config', value=config)
    branch = {'executor': 'trigger_executor', 'report': 'trigger_report',
              'report_with_workflows': 'trigger_report_with_workflows'}.get(
        config['selected_dag'], 'trigger_report_with_workflows')
    logger.info(f"Branching to: {branch}")
    return branch


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================

def ensure_integer_account_id(account_id):
    if account_id is None: return 1
    if isinstance(account_id, int): return account_id
    if isinstance(account_id, str):
        if account_id.lower() == 'default': return 1
        try: return int(account_id)
        except ValueError: return 1
    try: return int(account_id)
    except (ValueError, TypeError): return 1


def check_tweet_id_length(tweet_id: str, required_length: int = REQUIRED_TWEET_ID_LENGTH) -> bool:
    if not tweet_id: return False
    s = str(tweet_id).strip()
    return s.isdigit() and len(s) == required_length


def get_filter_words_from_mongodb() -> List[str]:
    try:
        processing_settings = get_system_setting('extraction_processing_settings', {})
        words_to_filter     = processing_settings.get('words_to_filter', '')
        if not words_to_filter or not isinstance(words_to_filter, str) or not words_to_filter.strip():
            logger.warning("No filter words in MongoDB — using HARDCODED_FILTER_WORDS fallback")
            return HARDCODED_FILTER_WORDS
        filter_words = [w.strip().lower() for w in words_to_filter.split(',') if w.strip()]
        if not filter_words:
            logger.warning("Filter words blank after parsing — using HARDCODED_FILTER_WORDS")
            return HARDCODED_FILTER_WORDS
        logger.info(f"Loaded {len(filter_words)} filter words from MongoDB")
        return filter_words
    except Exception as e:
        logger.error(f"Error loading filter words: {e} — using HARDCODED_FILTER_WORDS")
        return HARDCODED_FILTER_WORDS


def check_content_for_filter_words(link_url: str, filter_words: List[str]) -> bool:
    if not filter_words or not link_url: return True
    link_lower = link_url.lower()
    for word in filter_words:
        if word and word in link_lower:
            logger.info(f"REJECTING URL containing '{word}': {link_url[:100]}")
            return False
    return True


# ============================================================================
# WORKFLOW FUNCTIONS
# ============================================================================

def connect_link_to_content_via_workflow(
    mongo_db, link_id: int, link_url: str,
    automa_workflow_id: ObjectId, workflow_name: str, account_id: int
) -> Dict[str, Any]:
    try:
        now = datetime.now(timezone.utc)
        workflow_metadata = mongo_db.workflow_metadata.find_one(
            {"automa_workflow_id": automa_workflow_id, "account_id": account_id}
        )
        if not workflow_metadata:
            return {"success": False, "reason": "workflow_metadata_not_found"}

        content_items = workflow_metadata.get('content_items', [])
        if not content_items:
            return {"success": False, "reason": "no_content_in_workflow"}

        connected_content_ids = []
        with get_postgres_connection() as conn:
            with conn.cursor() as cur:
                for item in content_items:
                    content_id = item.get('content_id')
                    if not content_id: continue
                    cur.execute("""
                        INSERT INTO link_content_mappings (link_id, content_id, workflow_id)
                        VALUES (%s, %s, %s) ON CONFLICT (link_id, content_id) DO NOTHING
                    """, (link_id, content_id, str(automa_workflow_id)))
                    connected_content_ids.append(content_id)

                primary = content_items[0].get('content_id') if content_items else None
                cur.execute("""
                    UPDATE links SET connected_content_id = %s, connected_via_workflow = %s,
                        content_connection_time = %s, connection_status = 'active'
                    WHERE links_id = %s
                """, (primary, workflow_name, now, link_id))
                conn.commit()

        return {"success": True, "link_id": link_id, "content_ids": connected_content_ids,
                "primary_content_id": primary, "workflow_id": str(automa_workflow_id)}
    except Exception as e:
        logger.error(f"Failed to connect link to content: {e}")
        return {"success": False, "reason": str(e)}


def generate_workflow_name(workflow_type: str, workflow_index: int,
                           tweeted_datetime: datetime, account_id: int) -> str:
    safe_type = re.sub(r'[^\w]+', '_', workflow_type.lower())
    name = (f"workflow{workflow_index}_{safe_type}_"
            f"{tweeted_datetime.strftime('%Y-%m-%d_%H-%M-%S')}_account_{account_id}")
    logger.info(f"Generated workflow name: {name}")
    return name


def get_available_workflows_for_account(
    mongo_db, account_id: int, category: str = None,
    workflow_type: str = None, collection_name: str = None
) -> List[Dict[str, Any]]:
    try:
        account_id = ensure_integer_account_id(account_id)
        query = {"has_content": True, "has_link": {"$ne": True}}

        query["postgres_account_id"] = account_id

        if category:        query["category"]        = category.lower()
        if workflow_type:   query["workflow_type"]   = workflow_type
        if collection_name: query["collection_name"] = collection_name

        logger.info(f"Workflow query for account {account_id}: {query}")
        workflows = list(mongo_db.workflow_metadata.find(query, {
            "automa_workflow_id": 1, "workflow_type": 1, "workflow_name": 1,
            "account_id": 1, "postgres_account_id": 1, "category": 1,
            "collection_name": 1, "database_name": 1,
        }))
        logger.info(f"  {len(workflows)} metadata docs before collection lookup")

        result = []
        for doc in workflows:
            database_name     = doc.get('database_name', 'execution_workflows')
            actual_collection = doc.get('collection_name')
            if not actual_collection:
                continue
            wf_col = mongo_db.client[database_name][actual_collection]
            if not wf_col.find_one({'_id': doc['automa_workflow_id']}):
                logger.warning(
                    f"  Workflow {doc['automa_workflow_id']} not found in "
                    f"{database_name}.{actual_collection}"
                )
                continue
            result.append({
                'workflow_id':        doc['workflow_name'],
                'workflow_type':      doc['workflow_type'],
                'automa_workflow_id': str(doc['automa_workflow_id']),
                'account_id':         account_id,
                'category':           doc.get('category', ''),
                'collection_name':    actual_collection,
                'database_name':      database_name,
                'has_content':        True,
                'available_for_links': True,
            })
        logger.info(f"  {len(result)} workflows confirmed")
        return result
    except Exception as e:
        logger.error(f"Error getting workflows for account {account_id}: {e}")
        return []


# ============================================================================
# MAIN FILTERING TASK
# ============================================================================

def filter_links_with_time_and_content(**kwargs):
    """
    FOUR-STAGE FILTERING LOGIC with independent time windows
    """
    dag_run              = kwargs.get('dag_run')
    is_manual            = dag_run and (dag_run.external_trigger or dag_run.run_type == 'manual')
    extraction_completed = dag_run.conf.get('extraction_completed', False) if dag_run else False

    if not extraction_completed and not is_manual:
        logger.info("No fresh extraction and not a manual trigger — skipping")
        return

    try:
        mongo_db = get_mongo_connection()
        config   = get_weekly_workflow_config()

        filter_amount   = config['filter_amount']
        category        = config.get('category', '')
        workflow_type   = config.get('workflow_type', '')
        collection_name = config.get('collection_name', '')

        filter_words = get_filter_words_from_mongodb()
        now_utc      = datetime.now(timezone.utc)

        with get_postgres_connection() as conn:
            with conn.cursor() as cur:

                # Fetch links including tweet_author_user_id
                cur.execute("""
                    SELECT
                        links_id, tweet_id, link,
                        COALESCE(account_id, 1) AS account_id,
                        tweeted_time, tweeted_date,
                        EXTRACT(EPOCH FROM tweeted_time) AS tweeted_timestamp,
                        tweet_author_user_id
                    FROM links
                    WHERE tweeted_time IS NOT NULL
                        AND tweet_id IS NOT NULL
                        AND tweet_id ~ '^[0-9]+$'
                        AND used = FALSE
                    ORDER BY tweeted_time DESC
                """)
                rows = cur.fetchall()

                if not rows:
                    logger.info("No unprocessed links to filter (used=FALSE pool is empty)")
                    return

                # ================================================================
                # INITIAL: tweet ID length validation
                # ================================================================
                all_links          = []
                filtered_by_length = 0

                for idx, row in enumerate(rows):
                    try:
                        if hasattr(row, 'get'):
                            link_id              = row.get('links_id')
                            tweet_id             = row.get('tweet_id')
                            link_url             = row.get('link')
                            account_id           = row.get('account_id')
                            tweeted_time         = row.get('tweeted_time')
                            tweeted_date         = row.get('tweeted_date')
                            tweeted_timestamp    = row.get('tweeted_timestamp')
                            tweet_author_user_id = row.get('tweet_author_user_id')
                        else:
                            link_id, tweet_id, link_url, account_id, tweeted_time, tweeted_date, tweeted_timestamp, tweet_author_user_id = row

                        if link_id is None or tweet_id is None:
                            continue
                        if isinstance(link_id, str) and link_id.isdigit():
                            link_id = int(link_id)

                        tweet_id_str = str(tweet_id).strip()
                        if not tweet_id_str or not tweet_id_str.isdigit():
                            continue
                        if not check_tweet_id_length(tweet_id_str):
                            filtered_by_length += 1
                            continue

                        all_links.append({
                            'link_id':              link_id,
                            'tweet_id':             int(tweet_id_str),
                            'link_url':             link_url,
                            'chat_link':            None,
                            'tweet_author_user_id': tweet_author_user_id,
                            'account_id':           ensure_integer_account_id(account_id),
                            'tweeted_time':         tweeted_time,
                            'tweeted_date':         tweeted_date,
                            'tweeted_timestamp':    tweeted_timestamp,
                        })
                    except Exception as e:
                        logger.error(f"Error processing row {idx}: {e}")

                logger.info("=" * 80)
                logger.info(f"INITIAL: fetched={len(rows)} filtered_by_length={filtered_by_length} "
                            f"valid={len(all_links)} "
                            f"with_author_id={sum(1 for l in all_links if l.get('tweet_author_user_id'))}")

                if not all_links:
                    logger.warning("No valid links after tweet ID validation")
                    return

                # ================================================================
                # STAGE 1: CONTENT FILTER
                # ================================================================
                logger.info("=" * 80)
                logger.info("STAGE 1: CONTENT FILTERING")
                logger.info(f"Filter words ({len(filter_words)}): {filter_words}")

                content_passed_links = []
                rejected_by_content  = 0
                clean_links          = 0

                for data in all_links:
                    is_clean = check_content_for_filter_words(data['link_url'], filter_words)
                    if is_clean:
                        clean_links += 1
                        cur.execute(
                            "UPDATE links SET filtered = TRUE, filtered_time = %s WHERE links_id = %s",
                            (now_utc, data['link_id'])
                        )
                        content_passed_links.append(data)
                    else:
                        rejected_by_content += 1
                        cur.execute(
                            "UPDATE links SET filtered = FALSE, filtered_time = %s WHERE links_id = %s",
                            (now_utc, data['link_id'])
                        )
                conn.commit()
                logger.info(
                    f"STAGE 1: clean={clean_links} rejected={rejected_by_content} "
                    f"passed={len(content_passed_links)}"
                )

                if not content_passed_links:
                    logger.warning("No links passed content filtering")
                    return

                # ================================================================
                # STAGE 2: QUANTITY FILTER
                # ================================================================
                logger.info("=" * 80)
                logger.info("STAGE 2: QUANTITY FILTERING")

                content_passed_links.sort(
                    key=lambda x: x['tweeted_timestamp'] if x['tweeted_timestamp'] else 0,
                    reverse=True
                )
                available_count         = len(content_passed_links)
                effective_filter_amount = min(filter_amount, available_count)

                if filter_amount > available_count:
                    logger.warning(
                        f"filter_amount ({filter_amount}) > available ({available_count}) — using all"
                    )

                selected_for_within_limit = content_passed_links[:effective_filter_amount]
                excluded_by_quantity      = content_passed_links[effective_filter_amount:]

                for data in selected_for_within_limit:
                    cur.execute(
                        "UPDATE links SET within_limit = TRUE WHERE links_id = %s",
                        (data['link_id'],)
                    )
                for data in excluded_by_quantity:
                    cur.execute(
                        "UPDATE links SET within_limit = FALSE WHERE links_id = %s",
                        (data['link_id'],)
                    )
                conn.commit()

                logger.info(
                    f"STAGE 2: within_limit=TRUE={len(selected_for_within_limit)} "
                    f"within_limit=FALSE={len(excluded_by_quantity)}"
                )

                # ================================================================
                # STAGE 2b: CHAT LINK COMPUTATION
                # ================================================================
                logger.info("=" * 80)
                logger.info("STAGE 2b: CHAT LINK COMPUTATION")

                x_account_id_cache: Dict[int, Optional[str]] = {}
                chat_links_built = 0
                chat_links_null  = 0

                for data in selected_for_within_limit:
                    link_account_id      = data['account_id']
                    tweet_author_user_id = data.get('tweet_author_user_id')

                    if link_account_id not in x_account_id_cache:
                        x_account_id_cache[link_account_id] = get_x_account_id_for_account(link_account_id)

                    x_account_id = x_account_id_cache[link_account_id]
                    chat_link    = build_chat_link(x_account_id, tweet_author_user_id)
                    data['chat_link'] = chat_link

                    if chat_link:
                        chat_links_built += 1
                        cur.execute(
                            "UPDATE links SET chat_link = %s WHERE links_id = %s",
                            (chat_link, data['link_id'])
                        )
                        logger.debug(
                            f"  link {data['link_id']}: "
                            f"sender={x_account_id} recipient={tweet_author_user_id} -> {chat_link}"
                        )
                    else:
                        chat_links_null += 1
                        if not x_account_id:
                            logger.warning(
                                f"  link {data['link_id']}: account {link_account_id} "
                                f"has no x_account_id — chat_link NULL"
                            )
                        elif not tweet_author_user_id:
                            logger.warning(
                                f"  link {data['link_id']}: no tweet_author_user_id — chat_link NULL"
                            )

                conn.commit()

                logger.info(f"STAGE 2b: chat_links_built={chat_links_built} null={chat_links_null}")

                # ================================================================
                # STAGE 3: WORKFLOW ASSIGNMENT
                # ================================================================
                logger.info("=" * 80)
                logger.info(
                    f"STAGE 3: WORKFLOW ASSIGNMENT — category={category} "
                    f"workflow_type={workflow_type} collection={collection_name or '(any)'}"
                )

                # Fetch all active accounts
                active_accounts = []
                try:
                    with get_postgres_connection() as acc_conn:
                        with acc_conn.cursor() as acc_cur:
                            acc_cur.execute(
                                "SELECT account_id, username, x_account_id "
                                "FROM accounts ORDER BY account_id"
                            )
                            acc_rows = acc_cur.fetchall()
                            for row in acc_rows:
                                if hasattr(row, 'get'):
                                    active_accounts.append({
                                        'account_id':   row.get('account_id'),
                                        'username':     row.get('username'),
                                        'x_account_id': row.get('x_account_id'),
                                    })
                                else:
                                    active_accounts.append({
                                        'account_id':   row[0],
                                        'username':     row[1],
                                        'x_account_id': row[2] if len(row) > 2 else None,
                                    })
                except Exception as e:
                    logger.error(f"Failed to fetch accounts: {e}")
                    raise

                if not active_accounts:
                    logger.warning("No active accounts found — skipping workflow assignment")
                    return

                logger.info(f"  Active accounts ({len(active_accounts)}): "
                            f"{[a['account_id'] for a in active_accounts]}")

                total_assigned  = 0
                total_updated   = 0
                total_connected = 0

                # Track per-link success across all accounts
                link_assignment_results: Dict[int, Dict[int, bool]] = {
                    data['link_id']: {} for data in selected_for_within_limit
                }

                for account in active_accounts:
                    account_id   = account['account_id']
                    x_account_id = (
                        str(account['x_account_id']).strip()
                        if account.get('x_account_id') else None
                    )

                    logger.info("=" * 60)
                    logger.info(
                        f"Account {account_id} ({account.get('username')})  "
                        f"x_account_id={x_account_id or 'NOT SET'}"
                    )

                    # Fetch all available workflows for this account
                    account_workflows = get_available_workflows_for_account(
                        mongo_db, account_id, category, workflow_type, collection_name
                    )

                    if not account_workflows:
                        logger.warning(
                            f"  No workflows available for account {account_id} — "
                            f"all {len(selected_for_within_limit)} link(s) skipped"
                        )
                        for data in selected_for_within_limit:
                            link_assignment_results[data['link_id']][account_id] = False
                        continue

                    logger.info(
                        f"  {len(account_workflows)} workflow(s) available, "
                        f"{len(selected_for_within_limit)} link(s) to assign"
                    )

                    workflow_index = 0

                    for data in selected_for_within_limit:
                        link_id              = data['link_id']
                        link_url             = data['link_url']
                        tweeted_time         = data['tweeted_time']
                        tweet_author_user_id = data.get('tweet_author_user_id')

                        # Build chat link using THIS account's x_account_id as sender
                        account_chat_link = build_chat_link(x_account_id, tweet_author_user_id)

                        if workflow_index >= len(account_workflows):
                            logger.warning(
                                f"  Account {account_id} ran out of workflows after "
                                f"{workflow_index} assignment(s) — "
                                f"link {link_id} and remaining links skipped"
                            )
                            link_assignment_results[link_id][account_id] = False
                            break

                        workflow = account_workflows[workflow_index]

                        try:
                            assignments = assign_link_to_all_workflow_types(
                                mongo_db,
                                link_url,
                                link_id,
                                account_id,
                                [workflow],
                                None,
                                tweeted_time.date() if tweeted_time else None,
                                workflow_index=workflow_index + 1,
                                chat_link=account_chat_link,
                            )

                            success = any(a['success'] for a in assignments)

                            if success:
                                for a in assignments:
                                    lc = a.get('link_content_connection', {})
                                    if lc and lc.get('success'):
                                        total_connected += 1
                                    inj = a.get('injection_status', {})
                                    if inj:
                                        logger.info(
                                            f"  link {link_id} → "
                                            f"workflow[{workflow_index + 1}]: "
                                            f"tweet_url={'✅' if inj.get('tweet_url_injected') else '❌'} "
                                            f"chat={'✅' if inj.get('chat_link_injected') else '❌'} "
                                            f"new_tab_count={inj.get('new_tab_count', 0)}"
                                        )
                                total_assigned += 1
                                workflow_index += 1

                            link_assignment_results[link_id][account_id] = success

                            cur.execute("""
                                UPDATE links SET
                                    workflow_status = %s,
                                    workflow_type   = %s
                                WHERE links_id = %s
                            """, (
                                'completed' if success else 'failed',
                                workflow_type,
                                link_id,
                            ))
                            total_updated += 1

                        except Exception as e:
                            logger.error(
                                f"  Assignment failed for account {account_id} "
                                f"link {link_id}: {e}"
                            )
                            import traceback
                            logger.error(traceback.format_exc())
                            link_assignment_results[link_id][account_id] = False
                            raise

                # ================================================================
                # MARK used = TRUE FOR SUCCESSFULLY ASSIGNED LINKS
                # ================================================================
                conn.commit()

                links_marked_used    = 0
                links_not_marked     = 0

                logger.info("=" * 80)
                logger.info("MARKING USED LINKS")

                for data in selected_for_within_limit:
                    link_id        = data['link_id']
                    account_results = link_assignment_results.get(link_id, {})
                    any_success     = any(account_results.values()) if account_results else False

                    if any_success:
                        cur.execute(
                            "UPDATE links SET used = TRUE, used_time = %s WHERE links_id = %s",
                            (now_utc, link_id)
                        )
                        links_marked_used += 1
                        logger.info(
                            f"  ✅ link {link_id} marked used=TRUE "
                            f"(account results: {account_results})"
                        )
                    else:
                        links_not_marked += 1
                        logger.warning(
                            f"  ⚠️ link {link_id} NOT marked used — "
                            f"all assignments failed (account results: {account_results})"
                        )

                conn.commit()

                logger.info(
                    f"USED MARKING: marked={links_marked_used} skipped={links_not_marked}"
                )

                # ================================================================
                # FINAL SUMMARY
                # ================================================================
                logger.info("=" * 80)
                logger.info("FILTERING COMPLETE")
                logger.info(
                    f"  Initial:    rows={len(rows)} valid={len(all_links)} "
                    f"with_author_id="
                    f"{sum(1 for l in all_links if l.get('tweet_author_user_id'))}"
                )
                logger.info(
                    f"  Stage 1:    clean={clean_links} rejected={rejected_by_content}"
                )
                logger.info(
                    f"  Stage 2:    within_limit={len(selected_for_within_limit)} "
                    f"excluded={len(excluded_by_quantity)}"
                )
                logger.info(
                    f"  Stage 2b:   chat_links_built={chat_links_built} null={chat_links_null}"
                )
                logger.info(
                    f"  Stage 3:    accounts={len(active_accounts)} "
                    f"total_assignments={total_assigned} "
                    f"links_updated={total_updated} "
                    f"connections={total_connected}"
                )
                logger.info(
                    f"  Used:       marked={links_marked_used} skipped={links_not_marked}"
                )

    except Exception as e:
        logger.error(f"Critical error in filter_links_with_time_and_content: {e}")
        import traceback
        logger.error(traceback.format_exc())
        raise


def assign_link_to_all_workflow_types(
    mongo_db, link_url: str, postgres_link_id: int, account_id: int,
    available_workflows: List[Dict[str, Any]], content_text: str = None,
    tweeted_date=None, workflow_index: int = 1, chat_link: str = None,
    all_link_ids: List[int] = None, all_link_urls: List[str] = None,
) -> List[Dict[str, Any]]:
    """
    Assign a link to workflow(s), injecting URLs into the Automa drawflow.
    """
    from datetime import date as date_type

    if tweeted_date and isinstance(tweeted_date, date_type) and not isinstance(tweeted_date, datetime):
        tweeted_date = datetime.combine(tweeted_date, datetime.min.time())

    naming_datetime = tweeted_date if tweeted_date else datetime.now(timezone.utc)
    results         = []
    account_id      = ensure_integer_account_id(account_id)

    if all_link_ids is None:
        all_link_ids = [postgres_link_id]
    if all_link_urls is None:
        all_link_urls = [link_url]

    username = f"account_{account_id}"
    try:
        doc = mongo_db.accounts.find_one({"account_id": account_id})
        if doc and doc.get("username"):
            username = re.sub(r"\W+", "_", str(doc["username"]))
    except Exception as e:
        logger.warning(f"Could not fetch username for account {account_id}: {e}")

    now    = datetime.now(timezone.utc)
    ts_str = now.strftime("%Y%m%d_%H%M%S")

    for wf in available_workflows:
        wf_id           = wf["workflow_id"]
        wf_type         = wf["workflow_type"]
        automa_wf_id    = wf.get("automa_workflow_id")
        database_name   = wf.get('database_name', 'execution_workflows')
        collection_name = wf.get('collection_name')

        success = False
        reason = None
        content_link_id = None
        metadata_id = None
        link_content_connection = None
        new_workflow_name = wf_id

        tweet_url_injected   = False
        chat_link_injected   = False
        first_new_tab_found  = False
        second_new_tab_found = False
        new_tab_nodes        = []

        try:
            has_link = bool(link_url and isinstance(link_url, str) and link_url.strip()
                           and ("http://" in link_url or "https://" in link_url))
            if not has_link:
                results.append({"workflow_id": wf_id, "workflow_type": wf_type, "success": False,
                                 "reason": "invalid_link_url", "content_link_id": None,
                                 "metadata_id": None, "link_content_connection": None})
                continue

            if not automa_wf_id or not collection_name:
                results.append({"workflow_id": wf_id, "workflow_type": wf_type, "success": False,
                                 "reason": "missing_workflow_metadata", "content_link_id": None,
                                 "metadata_id": None, "link_content_connection": None})
                continue

            wf_col       = mongo_db.client[database_name][collection_name]
            workflow_doc = wf_col.find_one({"_id": ObjectId(automa_wf_id)})
            if not workflow_doc:
                results.append({"workflow_id": wf_id, "workflow_type": wf_type, "success": False,
                                 "reason": f"workflow_not_found_in_{collection_name}",
                                 "content_link_id": None, "metadata_id": None, "link_content_connection": None})
                continue

            new_workflow_name    = generate_workflow_name(wf_type, workflow_index, naming_datetime, account_id)
            base_name            = f"{username}_{wf_type}_{postgres_link_id}_{ts_str}"
            pre_screenshot_name  = f"pre_{base_name}"
            post_screenshot_name = f"post_{base_name}"
            video_name           = f"recording_{base_name}.webm"

            screenshots_updated = 0
            drawflow_updated    = False

            if "drawflow" in workflow_doc and "nodes" in workflow_doc["drawflow"]:
                # Collect all new-tab blocks (standalone + inside groups)
                for node in workflow_doc["drawflow"]["nodes"]:
                    node_type = node.get('type')
                    if node_type == "BlockBasic" and node.get("label") == "new-tab":
                        new_tab_nodes.append({
                            'node': node, 'type': 'standalone', 'id': node.get('id'),
                            'position_y': node.get("position", {}).get("y", 0)
                        })
                    elif node_type == "BlockGroup" and "data" in node and "blocks" in node["data"]:
                        for block in node["data"]["blocks"]:
                            if block.get("id") == "new-tab":
                                new_tab_nodes.append({
                                    'node': node, 'block': block, 'type': 'ingroup',
                                    'id': node.get('id'),
                                    'position_y': node.get("position", {}).get("y", 0)
                                })
                                break

                # Sort top-to-bottom by Y position
                new_tab_nodes.sort(key=lambda x: x['position_y'])
                logger.info(f"  new-tab blocks found: {len(new_tab_nodes)}")

                # Inject URLs in order
                for idx, tab in enumerate(new_tab_nodes):
                    if idx == 0:    # first → primary tweet URL
                        target = tab['node'] if tab['type'] == 'standalone' else tab['block']
                        target["data"]["url"] = link_url.strip()
                        logger.info(f"  ✅ tweet URL → new-tab[0] ({tab['id']}): {link_url[:60]}...")
                        tweet_url_injected  = True
                        first_new_tab_found = True
                        drawflow_updated    = True

                    elif idx == 1:  # second → chat link
                        target = tab['node'] if tab['type'] == 'standalone' else tab['block']
                        if chat_link and chat_link.strip():
                            target["data"]["url"] = chat_link.strip()
                            logger.info(f"  ✅ chat link → new-tab[1] ({tab['id']}): {chat_link}")
                            chat_link_injected   = True
                            second_new_tab_found = True
                        else:
                            target["data"]["url"] = "https://x.com/messages"
                            logger.warning(
                                f"  ⚠️ No chat link for link {postgres_link_id} "
                                f"(account {account_id} x_account_id may be unset) "
                                f"— fallback URL used in new-tab[1]"
                            )
                        drawflow_updated = True

                # Update screenshot blocks
                for node in workflow_doc["drawflow"]["nodes"]:
                    if "data" in node and "blocks" in node["data"]:
                        for block in node["data"]["blocks"]:
                            if block.get("id") == "take-screenshot" and "data" in block:
                                d = block["data"]
                                if screenshots_updated == 0:
                                    d.update({"fileName": pre_screenshot_name, "ext": "png",
                                              "saveToComputer": True, "type": "fullpage", "fullPage": True})
                                    screenshots_updated += 1
                                    drawflow_updated = True
                                elif screenshots_updated == 1:
                                    d.update({"fileName": post_screenshot_name, "ext": "png",
                                              "saveToComputer": True, "type": "fullpage", "fullPage": True})
                                    screenshots_updated += 1
                                    drawflow_updated = True

            if not first_new_tab_found:
                logger.warning("  ⚠️ No new-tab blocks found — tweet URL not injected")
            if not second_new_tab_found and len(new_tab_nodes) < 2:
                logger.warning(f"  ⚠️ Only {len(new_tab_nodes)} new-tab block(s) — chat link not injected")

            # Update workflow document
            update_fields = {"name": new_workflow_name}
            if drawflow_updated:
                update_fields["drawflow"] = workflow_doc["drawflow"]
            wf_col.update_one({"_id": ObjectId(automa_wf_id)}, {"$set": update_fields})

            # Upsert workflow_metadata
            metadata_doc = {
                "automa_workflow_id":  ObjectId(automa_wf_id),
                "workflow_type":       wf_type,
                "workflow_name":       new_workflow_name,
                "postgres_content_id": postgres_link_id,
                "account_id":          account_id,
                "postgres_account_id": account_id,
                "has_link":            True,
                "link_url":            link_url.strip(),
                "link_assigned_at":    now,
                "has_content":         True,
                "content_preview":     (content_text or link_url)[:200],
                "content_hash":        hash(link_url),
                "status":              "ready_to_execute",
                "execute":             True,
                "executed":            False,
                "success":             False,
                "execution_attempts":  0,
                "updated_at":          now,
                "tweeted_date":        tweeted_date,
                "assignment_method":   "one_workflow_per_account",
                "assignment_source":   "weekly_schedule",
                "all_link_ids":        all_link_ids,
                "all_link_urls":       all_link_urls,
                "total_links_stored":  len(all_link_ids),
                "injected_urls": {
                    "tweet_url": link_url.strip(),
                    "chat_link": chat_link if chat_link else None,
                },
                "artifacts_folder": {
                    "pre_screenshot_name":  f"{pre_screenshot_name}.png",
                    "post_screenshot_name": f"{post_screenshot_name}.png",
                    "video_name":           video_name,
                    "generated_at":         now,
                    "base_name":            base_name,
                    "account_username":     username,
                    "workflow_type":        wf_type,
                    "postgres_link_id":     postgres_link_id,
                    "timestamp":            ts_str,
                },
                "injection_status": {
                    "tweet_url_injected":   tweet_url_injected,
                    "chat_link_injected":   chat_link_injected,
                    "first_new_tab_found":  first_new_tab_found,
                    "second_new_tab_found": second_new_tab_found,
                    "new_tab_count":        len(new_tab_nodes),
                },
            }
            if chat_link:
                metadata_doc["chat_link"] = chat_link

            existing = mongo_db.workflow_metadata.find_one(
                {"automa_workflow_id": ObjectId(automa_wf_id), "account_id": account_id}
            )
            if not existing:
                metadata_doc["created_at"] = now

            meta_result = mongo_db.workflow_metadata.update_one(
                {"automa_workflow_id": ObjectId(automa_wf_id), "account_id": account_id},
                {"$set": metadata_doc}, upsert=True,
            )
            metadata_id = (
                str(meta_result.upserted_id) if meta_result.upserted_id
                else str(mongo_db.workflow_metadata.find_one(
                    {"automa_workflow_id": ObjectId(automa_wf_id), "account_id": account_id}
                )["_id"])
            )

            # content_workflow_links record
            link_doc = {
                "postgres_content_id": postgres_link_id,
                "automa_workflow_id": ObjectId(automa_wf_id),
                "content_type": wf_type,
                "link_url": link_url.strip(),
                "account_id": account_id,
                "linked_at": now,
                "assignment_method": "one_workflow_per_account",
                "assignment_source": "weekly_schedule",
                "content_preview": (content_text or "")[:200],
                "content_hash": hash(link_url),
                "has_link": True,
                "has_content": True,
                "workflow_name": new_workflow_name,
                "all_link_ids":  all_link_ids,
                "all_link_urls": all_link_urls,
            }
            if chat_link:
                link_doc["chat_link"] = chat_link
            link_result     = mongo_db.content_workflow_links.insert_one(link_doc)
            content_link_id = str(link_result.inserted_id)

            link_content_connection = connect_link_to_content_via_workflow(
                mongo_db, postgres_link_id, link_url.strip(),
                ObjectId(automa_wf_id), new_workflow_name, account_id
            )
            success = True

        except Exception as e:
            reason = str(e)
            logger.error(f"Assignment FAILED for {wf_id}: {e}")
            import traceback
            logger.error(traceback.format_exc())

        results.append({
            "workflow_id":             wf_id,
            "workflow_type":           wf_type,
            "workflow_name":           new_workflow_name,
            "success":                 success,
            "reason":                  reason,
            "content_link_id":         content_link_id,
            "metadata_id":             metadata_id,
            "link_content_connection": link_content_connection,
            "all_link_ids":            all_link_ids,
            "all_link_urls":           all_link_urls,
            "injection_status": {
                "tweet_url_injected":   tweet_url_injected,
                "chat_link_injected":   chat_link_injected,
                "first_new_tab_found":  first_new_tab_found,
                "second_new_tab_found": second_new_tab_found,
                "new_tab_count":        len(new_tab_nodes),
            },
        })

    return results


# ============================================================================
# SYNC TASK
# ============================================================================

def sync_workflow_execution_status(**kwargs):
    """Sync workflow status between PostgreSQL and MongoDB"""
    mongo_db = get_mongo_connection()
    with get_postgres_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT links_id, link, account_id, chat_link FROM links WHERE processed_by_workflow = TRUE")
            for link_id, link_url, account_id, chat_link in cur.fetchall():
                upd = {"has_link": True, "link_url": link_url, "status": "ready_to_execute",
                       "execute": True, "updated_at": datetime.now(timezone.utc)}
                if chat_link:
                    upd["chat_link"] = chat_link
                mongo_db.workflow_metadata.update_many(
                    {"postgres_content_id": link_id, "account_id": account_id or 1}, {"$set": upd}
                )


def fix_workflow_metadata_account_ids(**kwargs):
    """Fix null account IDs in workflow metadata"""
    mongo_db = get_mongo_connection()
    result = mongo_db.workflow_metadata.update_many(
        {"account_id": None},
        {"$set": {"account_id": 1, "postgres_account_id": 1, "updated_at": datetime.now(timezone.utc)}}
    )
    logger.info(f"Fixed {result.modified_count} null account_ids in workflow_metadata")


# ============================================================================
# DAG DEFINITION
# ============================================================================

with DAG(
    dag_id='filter_links',
    default_args=DEFAULT_ARGS,
    description=(
        'Filter & assign links with independent time windows. '
        'Respects its own schedule even when triggered by extraction.'
    ),
    schedule_interval=None,  # Triggered by extract_links
    start_date=datetime(2025, 7, 31),
    catchup=False,
    tags=['filtering', 'workflow', 'independent_windows'],
    max_active_runs=1,
) as dag:

    start = DummyOperator(task_id='start')

    fix_account_ids = PythonOperator(
        task_id='fix_account_ids',
        python_callable=fix_workflow_metadata_account_ids,
    )

    # Check if we're within filtering time windows
    check_filtering_window = PythonOperator(
        task_id='check_filtering_window',
        python_callable=check_filtering_time_window,
        provide_context=True,
    )

    filter_links_task = PythonOperator(
        task_id='filter_links_with_time_and_content',
        python_callable=filter_links_with_time_and_content,
    )

    sync_status = PythonOperator(
        task_id='sync_workflow_status',
        python_callable=sync_workflow_execution_status,
    )

    decide_dag = BranchPythonOperator(
        task_id='decide_which_dag',
        python_callable=decide_which_dag_to_trigger,
    )

    trigger_executor = TriggerDagRunOperator(
        task_id='trigger_executor', 
        trigger_dag_id='local_executor',
        conf={'triggered_by': 'filter_links', 'trigger_time': '{{ ts }}', 'dag_run_id': '{{ dag_run.run_id }}'},
        wait_for_completion=False, 
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    trigger_report = TriggerDagRunOperator(
        task_id='trigger_report', 
        trigger_dag_id='filtering_report',
        conf={'triggered_by': 'filter_links', 'trigger_time': '{{ ts }}', 'dag_run_id': '{{ dag_run.run_id }}'},
        wait_for_completion=False, 
        trigger_rule=TriggerRule.NONE_FAILED,
    )
    trigger_report_with_workflows = TriggerDagRunOperator(
        task_id='trigger_report_with_workflows', 
        trigger_dag_id='filtering_report_with_workflows',
        conf={'triggered_by': 'filter_links', 'trigger_time': '{{ ts }}', 'dag_run_id': '{{ dag_run.run_id }}'},
        wait_for_completion=False, 
        trigger_rule=TriggerRule.NONE_FAILED,
    )

    end = DummyOperator(task_id='end', trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)

    # Updated dependencies with window check
    start >> fix_account_ids >> check_filtering_window >> filter_links_task >> sync_status >> decide_dag
    decide_dag >> [trigger_executor, trigger_report, trigger_report_with_workflows]
    [trigger_executor, trigger_report, trigger_report_with_workflows] >> end
