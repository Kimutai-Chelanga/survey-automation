"""
EXTRACTION: Live Search + Always-On Parent Extraction + Time Window Enforcement
UPDATED: With hourly filtering gap control
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pytz
import os
import logging
import json

logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================
REQUIRED_ACCOUNT_ID   = 1
REQUIRED_PROFILE_TYPE = 'local_chrome'
YOUR_TWITTER_USER_ID = "1946168476348858370"
MINIMUM_FILTERING_GAP_HOURS = 1  # Only run filtering once per hour

# =============================================================================
# BUILT-IN DEFAULTS
# =============================================================================
DEFAULT_ACCOUNTS = [
    {'username': 'touchofm_',     'priority': 1},
    {'username': 'Eileevalencia', 'priority': 2},
    {'username': 'Record_spot1',  'priority': 3},
    {'username': 'brill_writers', 'priority': 4},
    {'username': 'essayzpro',     'priority': 5},
    {'username': 'primewriters23a','priority': 6},
    {'username': 'essaygirl01',   'priority': 7},
    {'username': 'EssayNasrah',   'priority': 8},
    {'username': 'Sharifwriter1', 'priority': 9},
    {'username': 'EssaysAstute',  'priority': 10},
]

DEFAULT_TIME_FILTER = {
    "enabled":                  True,
    "hours_back":               48,
    "fast_mode":                True,
    "skip_enhanced_extraction": False,
    "max_scrolls":              15,
}

DEFAULT_EXTRACTION_WINDOWS = {
    "enabled":        True,
    "morning_window": "05:00-09:00",
    "evening_window": "17:00-21:00",
}

DEFAULT_FILTERING_WINDOWS = {
    "enabled":        True,
    "morning_window": "06:00-10:00",
    "evening_window": "18:00-22:00",
}

DEFAULT_SCHEDULE_INTERVAL = "0 * * * *"  # Every hour

# =============================================================================
# TIMEZONE & START DATE
# =============================================================================
timezone_str = os.getenv("DAG_TIMEZONE", "Africa/Nairobi")
try:
    dag_timezone = pytz.timezone(timezone_str)
except pytz.UnknownTimeZoneError:
    dag_timezone = pytz.timezone("Africa/Nairobi")

dag_start_date_str = os.getenv("DAG_START_DATE", "2025-06-27")
try:
    start_date_naive = datetime.strptime(dag_start_date_str, "%Y-%m-%d")
    start_date = dag_timezone.localize(start_date_naive)
except ValueError:
    start_date = dag_timezone.localize(datetime(2025, 6, 27))

# =============================================================================
# DATABASE CONNECTION
# =============================================================================
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://airflow:airflow@postgres:5432/messages")


def _get_engine():
    from sqlalchemy import create_engine
    return create_engine(DATABASE_URL)


def get_postgres_connection():
    """Get a PostgreSQL connection"""
    import psycopg2
    from urllib.parse import urlparse
    
    result = urlparse(DATABASE_URL)
    return psycopg2.connect(
        database=result.path[1:],
        user=result.username,
        password=result.password,
        host=result.hostname,
        port=result.port
    )


# =============================================================================
# SCHEDULE INTERVAL HELPER
# =============================================================================

def get_schedule_interval():
    """
    Get schedule interval from MongoDB settings.
    Returns a cron expression string.
    """
    try:
        from pymongo import MongoClient
        MONGODB_URI = os.getenv(
            'MONGODB_URI',
            'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'
        )
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = client['messages_db']
        
        extraction_doc = db.settings.find_one({'category': 'extraction'})
        if extraction_doc and 'settings' in extraction_doc:
            extraction_settings = extraction_doc['settings'].get('extraction_schedule_settings', {})
            interval = extraction_settings.get('schedule_interval')
            if interval:
                logger.info(f"Using schedule interval from extraction settings: {interval}")
                client.close()
                return interval
        
        settings_doc = db.settings.find_one({'category': 'system'})
        if settings_doc and 'settings' in settings_doc:
            extraction_settings = settings_doc['settings'].get('extraction_schedule_settings', {})
            interval = extraction_settings.get('schedule_interval')
            if interval:
                logger.info(f"Using schedule interval from system settings: {interval}")
                client.close()
                return interval
        
        client.close()
        logger.info(f"Using default schedule interval: {DEFAULT_SCHEDULE_INTERVAL}")
        return DEFAULT_SCHEDULE_INTERVAL
    except Exception as e:
        logger.warning(f"Could not load schedule interval from MongoDB: {e}")
        return DEFAULT_SCHEDULE_INTERVAL


# =============================================================================
# CHROME PROFILE RESOLUTION
# =============================================================================
def get_chrome_profile_for_account_1():
    from sqlalchemy import text
    engine = _get_engine()
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT account_id, username, profile_type FROM accounts WHERE account_id = :id"),
                {"id": REQUIRED_ACCOUNT_ID}
            ).fetchone()
    except Exception as e:
        raise ValueError(f"FATAL: DB query failed for account_id={REQUIRED_ACCOUNT_ID}: {e}")

    if not row:
        raise ValueError(f"FATAL: account_id={REQUIRED_ACCOUNT_ID} NOT FOUND")

    account_id, username, profile_type = row[0], row[1], row[2]

    if not username:
        raise ValueError(f"FATAL: account_id={account_id} has NULL username")
    if profile_type != REQUIRED_PROFILE_TYPE:
        raise ValueError(
            f"FATAL: account_id={account_id} profile_type='{profile_type}' "
            f"expected '{REQUIRED_PROFILE_TYPE}'"
        )

    full_path = f"/workspace/chrome_profiles/account_{username}"
    logger.info(f"Chrome profile: {full_path}")
    return full_path


# =============================================================================
# MONGODB SETTINGS HELPER
# =============================================================================
def _get_mongo_settings():
    try:
        from pymongo import MongoClient
        MONGODB_URI = os.getenv(
            'MONGODB_URI',
            'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin'
        )
        client       = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db           = client['messages_db']
        settings_doc = db.settings.find_one({'category': 'system'})
        client.close()

        if settings_doc and 'settings' in settings_doc:
            stored = settings_doc['settings'].get('weekly_workflow_settings', {})
            if stored:
                logger.info("MongoDB settings loaded successfully")
                return stored

        logger.warning("MongoDB settings document empty or missing — using built-in defaults")
    except Exception as e:
        logger.warning(f"MongoDB unavailable ({e}) — using built-in defaults")

    return {}


def _get_time_filter(weekly_cfg: dict) -> dict:
    stored = weekly_cfg.get('time_filter', {})
    merged = {**DEFAULT_TIME_FILTER, **stored}
    merged['enabled'] = True
    return merged


def _get_extraction_windows(weekly_cfg: dict, day_name: str) -> dict:
    """Get extraction windows for a specific day"""
    stored = weekly_cfg.get(day_name, {})
    return {**DEFAULT_EXTRACTION_WINDOWS, **stored}


def _get_filtering_windows(weekly_cfg: dict, day_name: str) -> dict:
    """Get filtering windows for a specific day from filtering_config"""
    day_cfg = weekly_cfg.get(day_name, {})
    filtering_config = day_cfg.get('filtering_config', {})
    
    return {
        "enabled": filtering_config.get('windows_enabled', True),
        "morning_window": filtering_config.get('morning_window', DEFAULT_FILTERING_WINDOWS['morning_window']),
        "evening_window": filtering_config.get('evening_window', DEFAULT_FILTERING_WINDOWS['evening_window']),
    }


# =============================================================================
# TIME WINDOW CHECKS
# =============================================================================
def _parse_time_window(window_str: str) -> tuple:
    """Parse time window string like '05:00-09:00' into (start_time, end_time)"""
    try:
        s, e = window_str.split('-')
        start_time = datetime.strptime(s.strip(), '%H:%M').time()
        end_time = datetime.strptime(e.strip(), '%H:%M').time()
        return start_time, end_time
    except Exception as e:
        logger.error(f"Error parsing time window '{window_str}': {e}")
        return None, None


def _is_time_in_window(check_time: datetime, window_str: str) -> bool:
    """Check if a given time is within a time window string"""
    if not window_str:
        return False
    
    start_time, end_time = _parse_time_window(window_str)
    if not start_time or not end_time:
        return False
    
    current_time = check_time.time()
    
    if start_time <= end_time:
        return start_time <= current_time <= end_time
    else:
        return current_time >= start_time or current_time <= end_time


def _is_within_extraction_window(check_time: datetime, weekly_cfg: dict) -> tuple:
    """Check if current time is within extraction windows"""
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    day_name = days[check_time.weekday()]
    day_cfg = _get_extraction_windows(weekly_cfg, day_name)

    if not day_cfg.get('enabled', True):
        return False, f"{day_name.title()} extraction is disabled"

    morning_window = day_cfg.get('morning_window', DEFAULT_EXTRACTION_WINDOWS['morning_window'])
    evening_window = day_cfg.get('evening_window', DEFAULT_EXTRACTION_WINDOWS['evening_window'])

    if _is_time_in_window(check_time, morning_window):
        start, end = _parse_time_window(morning_window)
        if start and end:
            if start <= end:
                duration = (datetime.combine(datetime.min, end) - datetime.combine(datetime.min, start)).seconds // 3600
            else:
                duration = (24 - start.hour + end.hour)
            return True, f"Inside morning extraction window ({morning_window}, {duration}h duration)"
        return True, f"Inside morning extraction window {morning_window}"

    if _is_time_in_window(check_time, evening_window):
        start, end = _parse_time_window(evening_window)
        if start and end:
            if start <= end:
                duration = (datetime.combine(datetime.min, end) - datetime.combine(datetime.min, start)).seconds // 3600
            else:
                duration = (24 - start.hour + end.hour)
            return True, f"Inside evening extraction window ({evening_window}, {duration}h duration)"
        return True, f"Inside evening extraction window {evening_window}"

    return False, (
        f"Outside extraction windows — morning: {morning_window}, "
        f"evening: {evening_window}, current: {check_time.strftime('%H:%M')}"
    )


def _is_within_filtering_window(check_time: datetime, weekly_cfg: dict) -> tuple:
    """Check if current time is within filtering windows"""
    days = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    day_name = days[check_time.weekday()]
    
    filtering_cfg = _get_filtering_windows(weekly_cfg, day_name)

    if not filtering_cfg.get('enabled', True):
        return False, f"{day_name.title()} filtering is disabled"

    morning_window = filtering_cfg.get('morning_window', DEFAULT_FILTERING_WINDOWS['morning_window'])
    evening_window = filtering_cfg.get('evening_window', DEFAULT_FILTERING_WINDOWS['evening_window'])

    if _is_time_in_window(check_time, morning_window):
        start, end = _parse_time_window(morning_window)
        if start and end:
            if start <= end:
                duration = (datetime.combine(datetime.min, end) - datetime.combine(datetime.min, start)).seconds // 3600
            else:
                duration = (24 - start.hour + end.hour)
            return True, f"Inside morning filtering window ({morning_window}, {duration}h duration)"
        return True, f"Inside morning filtering window {morning_window}"

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


# =============================================================================
# FILTERING GAP CHECK (NEW)
# =============================================================================
def _is_filtering_gap_satisfied(current_time: datetime) -> tuple:
    """
    Check if enough time has passed since last filtering run.
    Uses Airflow Variables for state tracking.
    Returns (satisfied, reason, last_run_time)
    """
    try:
        last_run_str = Variable.get("last_filtering_run_time", default_var=None)
        
        if not last_run_str:
            return True, "No previous filtering run found", None
        
        last_run = datetime.fromisoformat(last_run_str)
        # Ensure last_run is timezone-aware
        if last_run.tzinfo is None:
            last_run = dag_timezone.localize(last_run)
        
        hours_since = (current_time - last_run).total_seconds() / 3600
        
        if hours_since >= MINIMUM_FILTERING_GAP_HOURS:
            return True, f"Last run was {hours_since:.1f}h ago (≥ {MINIMUM_FILTERING_GAP_HOURS}h)", last_run
        else:
            next_allowed = last_run + timedelta(hours=MINIMUM_FILTERING_GAP_HOURS)
            return False, (f"Last run was {hours_since:.1f}h ago (< {MINIMUM_FILTERING_GAP_HOURS}h). "
                          f"Next allowed at {next_allowed.strftime('%H:%M')}"), last_run
    
    except Exception as e:
        logger.warning(f"Error checking filtering gap: {e}")
        return True, f"Error checking gap, allowing trigger: {e}", None


def _update_last_filtering_run():
    """Update the last filtering run time in Airflow Variables"""
    try:
        current_time = datetime.now(dag_timezone)
        Variable.set("last_filtering_run_time", current_time.isoformat())
        logger.info(f"Updated last filtering run time to {current_time.strftime('%H:%M:%S')}")
        return True
    except Exception as e:
        logger.error(f"Failed to update last filtering run time: {e}")
        return False


# =============================================================================
# MAIN GUARD: EXTRACTION TIME WINDOW CHECK
# =============================================================================
def check_extraction_allowed(**kwargs):
    dag_run      = kwargs['dag_run']
    current_time = datetime.now(dag_timezone)

    if dag_run.external_trigger or dag_run.run_type == 'manual':
        logger.info("Manual trigger — bypassing time window check")
        _push_settings_to_xcom(kwargs['ti'])
        return True

    weekly_cfg  = _get_mongo_settings()
    time_filter = _get_time_filter(weekly_cfg)

    in_window, reason = _is_within_extraction_window(current_time, weekly_cfg)

    logger.info("=" * 80)
    logger.info("EXTRACTION TIME WINDOW CHECK")
    logger.info(f"  Check time: {current_time.strftime('%A %H:%M:%S')}")
    logger.info(f"  In extraction window:  {'YES' if in_window else 'NO'}")
    logger.info(f"  Reason:     {reason}")
    logger.info("=" * 80)

    if not in_window:
        raise AirflowSkipException(f"Outside extraction window: {reason}")

    logger.info("EXTRACTION ALLOWED")
    kwargs['ti'].xcom_push(key='time_filter_settings', value=time_filter)
    return True


# =============================================================================
# FILTERING TIME WINDOW + GAP CHECK (UPDATED)
# =============================================================================
def check_filtering_allowed(**kwargs):
    """
    Check if we should trigger filtering:
    1. Must be within filtering windows
    2. Must have enough time passed since last filtering run
    """
    current_time = datetime.now(dag_timezone)
    weekly_cfg = _get_mongo_settings()
    
    # First check: Is current time within filtering windows?
    in_window, window_reason = _is_within_filtering_window(current_time, weekly_cfg)
    
    if not in_window:
        logger.info("=" * 80)
        logger.info("FILTERING TRIGGER CHECK")
        logger.info(f"  Check time: {current_time.strftime('%A %H:%M:%S')}")
        logger.info(f"  In filtering window: NO")
        logger.info(f"  Reason: {window_reason}")
        logger.info("=" * 80)
        raise AirflowSkipException(f"Outside filtering windows: {window_reason}")
    
    # Second check: Has enough time passed since last filtering?
    gap_satisfied, gap_reason, last_run = _is_filtering_gap_satisfied(current_time)
    
    logger.info("=" * 80)
    logger.info("FILTERING TRIGGER CHECK")
    logger.info(f"  Check time: {current_time.strftime('%A %H:%M:%S')}")
    logger.info(f"  In filtering window: YES ({window_reason})")
    logger.info(f"  Gap requirement: {'✅ SATISFIED' if gap_satisfied else '❌ NOT SATISFIED'}")
    logger.info(f"  Gap reason: {gap_reason}")
    if last_run:
        logger.info(f"  Last filtering run: {last_run.strftime('%H:%M:%S')}")
    logger.info("=" * 80)
    
    if not gap_satisfied:
        raise AirflowSkipException(f"Filtering gap not satisfied: {gap_reason}")
    
    # Update the last run time BEFORE triggering (to prevent race conditions)
    _update_last_filtering_run()
    
    logger.info("✅ FILTERING TRIGGER ALLOWED - within windows and gap satisfied")
    
    # Push settings to XCom
    filtering_cfg = _get_filtering_windows(weekly_cfg, current_time.strftime('%A').lower())
    kwargs['ti'].xcom_push(key='filtering_window_settings', value=filtering_cfg)
    return True


def _push_settings_to_xcom(ti):
    weekly_cfg  = _get_mongo_settings()
    time_filter = _get_time_filter(weekly_cfg)
    ti.xcom_push(key='time_filter_settings', value=time_filter)


# =============================================================================
# CONFIG LOGGER TASK
# =============================================================================
def log_fast_config(**kwargs):
    current_time = datetime.now(dag_timezone)
    dag_run      = kwargs['dag_run']
    weekly_cfg   = _get_mongo_settings()
    time_filter  = _get_time_filter(weekly_cfg)

    try:
        profile_path = get_chrome_profile_for_account_1()
    except ValueError as e:
        logger.error(str(e))
        raise

    days     = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
    day_name = days[current_time.weekday()]
    
    extraction_windows = _get_extraction_windows(weekly_cfg, day_name)
    filtering_windows = _get_filtering_windows(weekly_cfg, day_name)

    in_extraction_window, extraction_reason = _is_within_extraction_window(current_time, weekly_cfg)
    
    # Get last filtering run time
    last_filtering = Variable.get("last_filtering_run_time", default_var="Never")

    settings_source = "MongoDB" if weekly_cfg else "built-in defaults"
    
    # Get schedule interval from settings
    try:
        from pymongo import MongoClient
        MONGODB_URI = os.getenv('MONGODB_URI', 'mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin')
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=5000)
        db = client['messages_db']
        
        extraction_doc = db.settings.find_one({'category': 'extraction'})
        if extraction_doc and 'settings' in extraction_doc:
            extraction_settings = extraction_doc['settings'].get('extraction_schedule_settings', {})
            schedule_interval = extraction_settings.get('schedule_interval', '0 * * * *')
            schedule_desc = extraction_settings.get('schedule_description', 'Every hour')
        else:
            settings_doc = db.settings.find_one({'category': 'system'})
            if settings_doc and 'settings' in settings_doc:
                extraction_settings = settings_doc['settings'].get('extraction_schedule_settings', {})
                schedule_interval = extraction_settings.get('schedule_interval', '0 * * * *')
                schedule_desc = extraction_settings.get('schedule_description', 'Every hour')
            else:
                schedule_interval = '0 * * * *'
                schedule_desc = 'Every hour'
        client.close()
    except:
        schedule_interval = '0 * * * *'
        schedule_desc = 'Every hour'

    logger.info("=" * 80)
    logger.info("EXTRACTION CONFIGURATION")
    logger.info("=" * 80)
    logger.info(f"Run ID:              {dag_run.run_id}")
    logger.info(f"Run type:            {dag_run.run_type}")
    logger.info(f"Time:                {current_time.strftime('%H:%M:%S')}")
    logger.info(f"Schedule:            {schedule_desc} ({schedule_interval})")
    logger.info(f"Settings from:       {settings_source}")
    logger.info(f"In extraction window: {'YES' if in_extraction_window else 'NO'}  {extraction_reason}")
    logger.info(
        f"Extraction windows:  morning={extraction_windows.get('morning_window')} "
        f"evening={extraction_windows.get('evening_window')}"
    )
    logger.info(
        f"Filtering windows:   morning={filtering_windows.get('morning_window')} "
        f"evening={filtering_windows.get('evening_window')}"
    )
    logger.info(f"Filtering gap req:   {MINIMUM_FILTERING_GAP_HOURS}h minimum between runs")
    logger.info(f"Last filtering run:  {last_filtering}")
    logger.info("Source:              Live Search")
    logger.info("Parent extract:      ALWAYS ON")
    logger.info(
        f"Time filter:         ALWAYS ON  "
        f"hours_back={time_filter.get('hours_back', 48)}"
    )
    logger.info(f"Profile:             {profile_path}")
    logger.info("=" * 80)

    kwargs['ti'].xcom_push(key='chrome_profile_dir', value=profile_path)


# =============================================================================
# CHROME HEALTH CHECK
# =============================================================================
def check_chrome_health(**kwargs):
    try:
        response = requests.get('http://localhost:9222/json/version', timeout=5)
        if response.status_code == 200:
            info = response.json()
            logger.info(f"Chrome running: {info.get('Browser', 'Unknown')}")
            return True
    except Exception:
        logger.info("Chrome not running — start_chrome will launch it")
    return False


# =============================================================================
# BASH COMMAND STRINGS
# =============================================================================

_FIX_CHROME_OWNERSHIP_CMD = """
set -e

CHROME_PROFILE_BASE_DIR='/workspace/chrome_profiles'
PROFILE_DIR="{{ ti.xcom_pull(task_ids='log_fast_configuration', key='chrome_profile_dir') }}"

echo '================================================================'
echo 'CHROME LOCK CLEANUP (fix_chrome_ownership)'
echo "  Profile base: ${CHROME_PROFILE_BASE_DIR}"
echo "  Profile dir:  ${PROFILE_DIR}"
echo '================================================================'

find "${CHROME_PROFILE_BASE_DIR}" -type d -exec chmod 777 {} \\; 2>/dev/null || true
find "${CHROME_PROFILE_BASE_DIR}" -type f -exec chmod 666 {} \\; 2>/dev/null || true
echo '  Permissions fixed on entire profile tree'

for subdir in "${PROFILE_DIR}" "${PROFILE_DIR}/Default"; do
    [ -d "${subdir}" ] || continue
    chmod 777 "${subdir}" 2>/dev/null || true
    for lockname in SingletonLock SingletonCookie SingletonSocket; do
        TARGET="${subdir}/${lockname}"
        if [ -e "${TARGET}" ] || [ -L "${TARGET}" ]; then
            echo "  Found lock: ${TARGET} — forcing removal"
            chmod 666 "${TARGET}" 2>/dev/null || true
            rm -f "${TARGET}" 2>/dev/null \\
                || unlink "${TARGET}" 2>/dev/null \\
                || ln -sfn /dev/null "${TARGET}" 2>/dev/null \\
                || true
            if [ -e "${TARGET}" ] || [ -L "${TARGET}" ]; then
                echo "FATAL: Cannot remove ${TARGET}"
                ls -la "${subdir}" 2>/dev/null || true
                stat "${TARGET}" 2>/dev/null || true
                exit 1
            fi
            echo "  Removed: ${TARGET}"
        fi
    done
done

rm -f "${PROFILE_DIR}/lockfile" 2>/dev/null || true

REMAINING=$(find "${PROFILE_DIR}" -name 'Singleton*' 2>/dev/null | wc -l)
if [ "${REMAINING}" -gt 0 ]; then
    echo "FATAL: ${REMAINING} Singleton file(s) survived after cleanup:"
    find "${PROFILE_DIR}" -name 'Singleton*' 2>/dev/null
    exit 1
fi
echo '  All Singleton locks cleared'

for session_file in 'Last Session' 'Last Tabs' 'Current Session' 'Current Tabs'; do
    TARGET="${PROFILE_DIR}/Default/${session_file}"
    if [ -e "${TARGET}" ]; then
        rm -f "${TARGET}" 2>/dev/null || true
        echo "  Removed session file: ${session_file}"
    fi
done

find "${CHROME_PROFILE_BASE_DIR}" \\
    \\( -name '.org.chromium.Chromium.*' -o -name '.com.google.Chrome.*' \\) \\
    -exec rm -f {} \\; 2>/dev/null || true
echo '  Removed stale chromium temp files'

echo '================================================================'
echo 'Chrome lock cleanup complete'
echo '================================================================'
"""

_VERIFY_COOKIES_CMD = """
set -e
PROFILE_DIR="{{ ti.xcom_pull(task_ids='log_fast_configuration', key='chrome_profile_dir') }}"
SESSION_FILE="${PROFILE_DIR}/session_data.json"

[ -d "${PROFILE_DIR}" ] || { echo 'Profile dir missing'; exit 1; }
[ -f "${SESSION_FILE}" ] || { echo 'session_data.json missing — sync cookies first'; exit 1; }

FILE_SIZE=$(stat -c%s "${SESSION_FILE}")
[ "${FILE_SIZE}" -ge 100 ] || { echo "session_data.json too small (${FILE_SIZE} bytes)"; exit 1; }

HAS_AUTH=$(grep -c '"name": "auth_token"' "${SESSION_FILE}" || echo '0')
[ "${HAS_AUTH}" -gt 0 ] || { echo 'Missing auth_token cookie'; exit 1; }

echo "Cookies verified (${FILE_SIZE} bytes, auth_token present)"
"""

_START_CHROME_CMD = """
set -e

PROFILE_DIR="{{ ti.xcom_pull(task_ids='log_fast_configuration', key='chrome_profile_dir') }}"
DEBUG_URL='http://localhost:9222/json/version'

if curl -sf "${DEBUG_URL}" > /dev/null 2>&1; then
    echo 'Chrome already running — reusing session'
    exit 0
fi

MY_PID=$$

CHROME_PIDS=$(pgrep -f 'google-chrome.*user-data-dir' 2>/dev/null | grep -v "^${MY_PID}$" || true)
if [ -n "${CHROME_PIDS}" ]; then
    echo "Killing stale Chrome PIDs: ${CHROME_PIDS}"
    echo "${CHROME_PIDS}" | xargs kill -9 2>/dev/null || true
    sleep 2
fi

XVFB_PIDS=$(pgrep -f 'Xvfb.*:99' 2>/dev/null | grep -v "^${MY_PID}$" || true)
if [ -n "${XVFB_PIDS}" ]; then
    echo "Killing stale Xvfb PIDs: ${XVFB_PIDS}"
    echo "${XVFB_PIDS}" | xargs kill -9 2>/dev/null || true
    sleep 1
fi

rm -f /tmp/.X99-lock /tmp/.X11-unix/X99 2>/dev/null || true
[ -d /tmp/.X11-unix ] || (mkdir -p /tmp/.X11-unix && chmod 1777 /tmp/.X11-unix)

echo 'Safety-net Singleton lock check...'
for subdir in "${PROFILE_DIR}" "${PROFILE_DIR}/Default"; do
    [ -d "${subdir}" ] || continue
    chmod 777 "${subdir}" 2>/dev/null || true
    for lockname in SingletonLock SingletonCookie SingletonSocket; do
        TARGET="${subdir}/${lockname}"
        if [ -e "${TARGET}" ] || [ -L "${TARGET}" ]; then
            echo "  Found lock: ${TARGET} — forcing removal"
            chmod 666 "${TARGET}" 2>/dev/null || true
            rm -f "${TARGET}" 2>/dev/null \\
                || unlink "${TARGET}" 2>/dev/null \\
                || ln -sfn /dev/null "${TARGET}" 2>/dev/null \\
                || true
            if [ -e "${TARGET}" ] || [ -L "${TARGET}" ]; then
                echo "FATAL: Cannot remove ${TARGET}"
                ls -la "${subdir}" 2>/dev/null || true
                stat "${TARGET}" 2>/dev/null || true
                exit 1
            fi
            echo "  Removed: ${TARGET}"
        fi
    done
done
rm -f "${PROFILE_DIR}/lockfile" 2>/dev/null || true
echo 'Safety-net check complete — no locks remain'

export DISPLAY=:99
SCREEN_RES='1920x1080x24'
nohup Xvfb :99 -screen 0 ${SCREEN_RES} -nolisten tcp > /tmp/xvfb_fast.log 2>&1 &
XVFB_PID=$!
sleep 3
ps -p ${XVFB_PID} > /dev/null 2>&1 || {
    echo 'Xvfb failed to start'
    cat /tmp/xvfb_fast.log
    exit 1
}
echo "Xvfb started (pid ${XVFB_PID}, res ${SCREEN_RES})"

nohup google-chrome-stable \\
    --no-sandbox \\
    --disable-setuid-sandbox \\
    --disable-dev-shm-usage \\
    --user-data-dir="${PROFILE_DIR}" \\
    --profile-directory=Default \\
    --ignore-profile-directory-lock \\
    --disable-profile-directory-locking \\
    --no-process-singleton-dialog \\
    --remote-debugging-port=9222 \\
    --remote-debugging-address=0.0.0.0 \\
    --disable-background-timer-throttling \\
    --disable-backgrounding-occluded-windows \\
    --disable-renderer-backgrounding \\
    --disable-gpu \\
    --no-first-run \\
    --no-default-browser-check \\
    --disable-session-crashed-bubble \\
    --disable-restore-session-state \\
    https://x.com \\
    > /tmp/chrome_fast.log 2>&1 &

CHROME_BG_PID=$!
echo "Chrome process launched (bg pid ${CHROME_BG_PID})"

for i in $(seq 1 30); do
    if curl -sf "${DEBUG_URL}" > /dev/null 2>&1; then
        echo "Chrome ready after ${i}s"
        exit 0
    fi
    if ! kill -0 "${CHROME_BG_PID}" 2>/dev/null; then
        echo "Chrome process died unexpectedly after ${i}s"
        echo '--- chrome_fast.log (last 50 lines) ---'
        tail -50 /tmp/chrome_fast.log
        exit 1
    fi
    echo "Waiting for Chrome... (${i}/30)"
    sleep 1
done

echo 'Chrome failed to become ready within 30s'
echo '--- chrome_fast.log (last 50 lines) ---'
tail -50 /tmp/chrome_fast.log
exit 1
"""

_EXTRACT_DATA_CMD = """
set -e
PROFILE_DIR="{{ ti.xcom_pull(task_ids='log_fast_configuration', key='chrome_profile_dir') }}"

cd /opt/airflow/src/scripts/extraction

export CHROME_PROFILE_DIR="${PROFILE_DIR}"
export ACCOUNT_ID="{{ var.value.get('account_id', '1') }}"
export CHROME_DEBUG_PORT=9222
export FAST_EXTRACTION='true'
export YOUR_TWITTER_USER_ID="1946168476348858370"

echo '================================================================'
echo 'STARTING EXTRACTION'
echo '  Source:         Live Search (search?q=from:USER&f=live)'
echo '  Parent extract: ALWAYS ON'
echo '  Time filter:    ALWAYS ON (48h default)'
echo '  User ID:        ENABLED (tweet_author_user_id column)'
echo '  Your User ID:   1946168476348858370 (hardcoded)'
echo '  Node timeout:   1740s'
echo '================================================================'

timeout 1800 node main.js

echo '================================================================'
echo 'EXTRACTION COMPLETED'
echo '================================================================'
"""


# =============================================================================
# DAG DEFINITION
# =============================================================================

schedule_interval = get_schedule_interval()

with DAG(
    dag_id='extract_links',
    default_args={
        'owner':                  'airflow',
        'start_date':             start_date,
        'retries':                1,
        'retry_delay':            timedelta(minutes=5),
        'max_active_tis_per_dag': 1,
    },
    description=(
        "Extraction with configurable schedule. "
        "Filtering triggered at most once per hour regardless of extraction frequency."
    ),
    schedule_interval=schedule_interval,
    start_date=start_date,
    catchup=False,
    max_active_runs=1,
    tags=['extraction', 'configurable_schedule', 'hourly_filtering'],
) as dag:

    dag.timezone = dag_timezone

    log_config_task = PythonOperator(
        task_id='log_fast_configuration',
        python_callable=log_fast_config,
        provide_context=True,
    )

    check_extraction_allowed = PythonOperator(
        task_id='check_extraction_allowed',
        python_callable=check_extraction_allowed,
        provide_context=True,
    )

    fix_chrome_ownership = BashOperator(
        task_id='fix_chrome_ownership',
        bash_command=_FIX_CHROME_OWNERSHIP_CMD,
    )

    verify_cookies = BashOperator(
        task_id='verify_cookies_exist',
        bash_command=_VERIFY_COOKIES_CMD,
    )

    start_chrome = BashOperator(
        task_id='start_chrome',
        execution_timeout=timedelta(minutes=3),
        bash_command=_START_CHROME_CMD,
    )

    chrome_health_check = PythonOperator(
        task_id='chrome_health_check',
        python_callable=check_chrome_health,
        provide_context=True,
    )

    extract_data_fast = BashOperator(
        task_id='extract_data_fast',
        execution_timeout=timedelta(minutes=30),
        bash_command=_EXTRACT_DATA_CMD,
    )

    cleanup_chrome = BashOperator(
        task_id='cleanup_chrome',
        trigger_rule='all_done',
        bash_command='echo "Chrome session preserved for next run"',
    )

    # Check if filtering is allowed (window + gap)
    check_filtering_allowed = PythonOperator(
        task_id='check_filtering_allowed',
        python_callable=check_filtering_allowed,
        provide_context=True,
    )

    trigger_filtering_dag = TriggerDagRunOperator(
        task_id='trigger_filtering_dag',
        trigger_dag_id='filter_links',
        trigger_rule='none_failed_min_one_success',
        conf={
            'triggered_by':         'extract_links',
            'account_id':           REQUIRED_ACCOUNT_ID,
            'extraction_mode':      'fast',
            'extraction_completed': True,
            'trigger_time':         '{{ ts }}',
        },
        wait_for_completion=False,
    )

    # Task dependencies
    log_config_task >> check_extraction_allowed >> fix_chrome_ownership >> verify_cookies
    verify_cookies >> start_chrome >> chrome_health_check >> extract_data_fast
    extract_data_fast >> cleanup_chrome >> check_filtering_allowed >> trigger_filtering_dag
