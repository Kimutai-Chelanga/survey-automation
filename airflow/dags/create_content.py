# File: dags/create_content_by_account.py
# MULTI-CONTENT TYPE ARCHITECTURE - Processes all content types with specific prompts
# UPDATED: Deterministic generation + dynamic schedule read from 'content_run_schedule' DB setting

import os
import sys
import logging
import re
import time
import random
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from dotenv import load_dotenv
import google.generativeai as genai
import pytz

sys.path.append('/opt/airflow/src')

load_dotenv()

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ============================================================================
# SINGLE AUTHORITATIVE FILTER
# ============================================================================

FILLER_PHRASES = tuple(p.lower() for p in (
    'here are', 'below are', "i've created", 'list of', 'note:',
    'wisdom', 'this is gold', 'lol', 'this cracked', 'believe in',
    'what a gem', 'appreciate the', 'future self', 'learn something',
    'thankful for', 'echoing this', 'consider me', "let's explore",
    'much appreciated', 'yes, let', 'my stress', 'i could burst',
    'this sounds', "i'm absolutely", 'this truly', 'what kind',
    'sure, here', 'certainly', 'of course', 'absolutely',
    'here is', "here's", 'i have generated', 'as requested',
    'the following', 'please find',
))

MIN_LINE_LENGTH = 30


def filter_and_parse_lines(raw_text: str, seen: set = None) -> tuple:
    """
    Single authoritative filter for all content lines.
    Returns (valid_lines, skipped_count).
    seen set is shared across retry attempts to prevent cross-attempt duplicates.
    """
    if seen is None:
        seen = set()

    lines   = raw_text.splitlines()
    valid   = []
    skipped = 0

    for line in lines:
        line = line.strip()
        if not line:
            continue

        line = re.sub(r'^[\s\-\*\#\•\d\.\)\(]+', '', line).strip()
        if not line:
            skipped += 1
            continue

        lower = line.lower()

        if any(phrase in lower for phrase in FILLER_PHRASES):
            logger.debug(f"Skipped filler: {line[:60]}")
            skipped += 1
            continue

        if len(line) < MIN_LINE_LENGTH:
            logger.debug(f"Skipped too-short ({len(line)} chars): {line}")
            skipped += 1
            continue

        normalized = re.sub(r'\W+', ' ', lower).strip()
        if normalized in seen:
            logger.debug(f"Skipped duplicate: {line[:60]}")
            skipped += 1
            continue

        seen.add(normalized)
        valid.append(line)

    return valid, skipped


# ============================================================================
# SCHEDULE HELPERS
# ============================================================================

def get_run_schedule_from_db() -> dict:
    """
    Read the content_run_schedule setting saved by the Streamlit UI.
    Falls back to a single daily run at 08:00 if nothing is configured.
    """
    try:
        from core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT value
                    FROM system_settings
                    WHERE key = 'content_run_schedule'
                    LIMIT 1
                """)
                row = cursor.fetchone()
                if row and row['value']:
                    import json
                    setting = json.loads(row['value']) if isinstance(row['value'], str) else row['value']
                    logger.info(f"Loaded run schedule from DB: {setting}")
                    return setting
    except Exception as e:
        logger.warning(f"Could not load run schedule from DB: {e}")

    default = {'num_runs': 2, 'run_times': ['02:00', '03:00']}
    logger.info(f"Using default run schedule: {default}")
    return default


def build_schedule_interval_from_db() -> str:
    """
    Convert the saved run_times into a cron expression.
    - 1 time  → single cron:  '30 08 * * *'
    - 2+ times → comma-separated minutes/hours: '0 8,14 * * *'
      (if minutes differ we fall back to multiple individual crons joined — Airflow
       supports a comma list in the hours field only when minutes are the same,
       so we emit separate expressions joined with a newline, which Airflow handles
       via the timetable approach; for simplicity here we emit the most-common
       case and log a warning for mixed-minute schedules)
    """
    schedule = get_run_schedule_from_db()
    run_times = schedule.get('run_times', ['08:00'])

    if not run_times:
        return '0 8 * * *'

    # Build individual crons
    crons = []
    for rt in sorted(run_times):
        try:
            h, m = rt.split(':')
            crons.append(f"{int(m)} {int(h)} * * *")
        except Exception:
            logger.warning(f"Invalid run_time format: {rt} — skipping")

    if not crons:
        return '0 8 * * *'

    if len(crons) == 1:
        return crons[0]

    # Check if all minutes are the same — if so we can use a tidy multi-hour cron
    minutes = [c.split(' ')[0] for c in crons]
    hours   = [c.split(' ')[1] for c in crons]

    if len(set(minutes)) == 1:
        return f"{minutes[0]} {','.join(hours)} * * *"

    # Mixed minutes — Airflow doesn't support multiple full cron expressions
    # in schedule_interval natively. Use the first one and log a warning.
    # For true multi-cron support, switch to a Timetable plugin.
    logger.warning(
        f"Run times have different minutes {run_times}. "
        f"Only the first cron ({crons[0]}) will be used as schedule_interval. "
        f"For multiple distinct times, use a CronTriggerTimetable or separate DAGs."
    )
    return crons[0]


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def get_all_active_prompts() -> list:
    try:
        from core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT p.prompt_id, p.name AS prompt_name, p.prompt_type, a.username
                    FROM prompts p
                    LEFT JOIN accounts a ON p.account_id = a.account_id
                    WHERE p.is_active = TRUE
                    ORDER BY p.prompt_type, p.name
                """)
                return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error fetching active prompts: {e}")
        return []


def build_default_content_config() -> list:
    prompts = get_all_active_prompts()
    if not prompts:
        return []

    config = []
    for i in range(3):
        p = prompts[i % len(prompts)]
        config.append({
            'workflow_type':  p['prompt_type'],
            'prompt_name':    p['prompt_name'],
            'content_name':   p['prompt_name'],
            'content_amount': 25
        })
        logger.info(f"Default slot {i+1}: type={p['prompt_type']}, prompt={p['prompt_name']}")
    return config


def get_account_prompt_by_name(account_id: int, content_type: str, prompt_name: str) -> dict:
    try:
        from core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT prompt_id, name, content, prompt_type
                    FROM prompts
                    WHERE account_id = %s AND prompt_type = %s AND name = %s AND is_active = TRUE
                    ORDER BY updated_time DESC LIMIT 1
                """, (account_id, content_type, prompt_name))
                result = cursor.fetchone()
                return dict(result) if result else None
    except Exception as e:
        logger.error(f"Error fetching prompt: {e}")
        return None


def get_all_active_accounts() -> list:
    try:
        from core.database.postgres.connection import get_postgres_connection
        from psycopg2.extras import RealDictCursor

        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT account_id, username, profile_id FROM accounts ORDER BY account_id")
                return [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        logger.error(f"Error fetching accounts: {e}")
        return []


def save_content_to_db(
    content_items: list,
    account_id: int,
    prompt_id: int,
    content_type: str,
    content_name: str
) -> tuple:
    """
    Save content items. Returns (inserted_count, failed_count).
    save_bulk now returns a tuple — no double filtering.
    """
    try:
        from core.database.postgres.content_handler import get_content_handler

        handler       = get_content_handler(content_type)
        content_names = [content_name] * len(content_items)

        inserted, failed = handler.save_bulk(
            content_items=content_items,
            content_names=content_names,
            account_id=account_id,
            prompt_id=prompt_id
        )
        logger.info(
            f"Saved {inserted} items (failed {failed}) "
            f"for type '{content_type}' / name '{content_name}'"
        )
        return inserted, failed

    except Exception as e:
        logger.error(f"Error saving content: {e}")
        raise


def generate_with_retry(model, prompt, retries=3):
    for attempt in range(retries):
        try:
            response = model.generate_content(prompt)
            if response.text:
                return response.text
        except Exception as e:
            logger.warning(f"Gemini attempt {attempt + 1} failed: {e}")
            if attempt == retries - 1:
                raise
            time.sleep((2 ** attempt) + random.uniform(0, 0.5))
    return None


# ============================================================================
# DETERMINISTIC GENERATION — while loop until target is met
# ============================================================================

def generate_content_for_account(
    account_id: int,
    content_type: str,
    amount: int,
    content_name: str,
    prompt_name: str,
    max_attempts: int = 4
) -> int:
    """
    Generate exactly `amount` saved items via a while-loop retry.
    Only the deficit is requested on each retry. Cross-attempt dedup
    is maintained via the shared `seen` set.
    Returns total items actually saved to DB.
    """
    logger.info(
        f"Account {account_id} | type={content_type} | "
        f"prompt={prompt_name} | target={amount}"
    )

    key = os.getenv("GEMINI_API_KEY")
    if not key:
        raise Exception("GEMINI_API_KEY not found")

    prompt_data = get_account_prompt_by_name(account_id, content_type, prompt_name)
    if not prompt_data:
        logger.warning(f"No prompt '{prompt_name}' for account {account_id} / type {content_type}")
        return 0

    genai.configure(api_key=key)
    model = genai.GenerativeModel(
        "gemini-2.5-flash",
        generation_config={
            "temperature": 0.7,
            "top_p":       0.9,
            "top_k":       40,
            "max_output_tokens": 2048,
        }
    )

    total_saved = 0
    attempt     = 0
    seen        = set()   # shared across attempts for cross-attempt dedup

    while total_saved < amount and attempt < max_attempts:
        attempt  += 1
        remaining = amount - total_saved

        # Multiplier grows on later attempts (earlier ones had heavy losses)
        multiplier     = 3.5 + (attempt - 1)
        request_amount = max(int(remaining * multiplier), remaining + 20)

        logger.info(
            f"Attempt {attempt}/{max_attempts}: "
            f"need {remaining} more, requesting {request_amount}"
        )

        full_prompt = f"""
{prompt_data['content']}

Generate {request_amount} unique items for the content type '{content_type}'.
Content Name: {content_name}

STRICT OUTPUT FORMAT:
Return ONLY raw sentences. One sentence per line.

HARD REQUIREMENTS:
- Each line must be a complete natural sentence
- Each sentence must directly relate to the topic described above
- No numbering, bullets, dashes, prefixes, explanations, quotes, or hashtags
- Make each sentence different and engaging

FAILURE CONDITIONS:
If you add any formatting, numbering, or intro text, the output is invalid.
"""

        try:
            raw_text = generate_with_retry(model, full_prompt)
            if not raw_text:
                logger.warning(f"Attempt {attempt}: empty Gemini response")
                time.sleep(2 ** attempt)
                continue
        except Exception as e:
            logger.error(f"Attempt {attempt}: Gemini error: {e}")
            if attempt >= max_attempts:
                raise
            time.sleep(2 ** attempt)
            continue

        # Safe truncation — never cut mid-line
        if len(raw_text) > 20000:
            logger.warning(f"Attempt {attempt}: response too large, truncating safely")
            raw_text = raw_text[:20000].rsplit('\n', 1)[0]

        valid_lines, skipped = filter_and_parse_lines(raw_text, seen=seen)
        logger.info(
            f"Attempt {attempt}: {len(valid_lines)} valid, "
            f"{skipped} skipped from {len(raw_text.splitlines())} lines"
        )

        if not valid_lines:
            logger.warning(f"Attempt {attempt}: no valid lines. Preview: {raw_text[:200]}")
            continue

        lines_to_save = valid_lines[:remaining]

        try:
            saved, failed = save_content_to_db(
                lines_to_save, account_id,
                prompt_data['prompt_id'],
                content_type, content_name
            )
            total_saved += saved
            logger.info(
                f"Attempt {attempt}: saved {saved} (failed {failed}). "
                f"Total: {total_saved}/{amount}"
            )
        except Exception as e:
            logger.error(f"Attempt {attempt}: DB save error: {e}")
            if attempt >= max_attempts:
                raise

        if total_saved < amount and attempt < max_attempts:
            time.sleep(1.5)

    if total_saved < amount:
        logger.warning(
            f"⚠️ Account {account_id}: only saved {total_saved}/{amount} "
            f"after {attempt} attempt(s) for '{content_type}'"
        )
    else:
        logger.info(
            f"✅ Account {account_id}: saved {total_saved}/{amount} "
            f"using prompt '{prompt_name}' ({attempt} attempt(s))"
        )

    return total_saved


# ============================================================================
# DAILY CONFIG
# ============================================================================

def get_daily_content_config():
    try:
        from streamlit.ui.settings.settings_manager import get_daily_content_config
        return get_daily_content_config()
    except Exception as e:
        logger.error(f"Error getting daily config: {e}")
        return {'content_types': [], 'enabled': False, 'day_name': 'unknown'}


def generate_all_content_for_today():
    """
    Generate all configured content types for today.
    Falls back to DB-prompt defaults if no weekly settings are configured.
    Triggers create_workflows DAG on completion.
    """
    daily_config         = get_daily_content_config()
    content_types_config = daily_config.get('content_types', [])

    if not daily_config.get('enabled', False) or not content_types_config:
        logger.info(
            f"No weekly settings for {daily_config.get('day_name')} — "
            f"falling back to DB defaults"
        )
        content_types_config = build_default_content_config()
        if not content_types_config:
            msg = "No content types configured and no active prompts found. Cannot generate."
            logger.error(msg)
            return msg

    accounts = get_all_active_accounts()
    if not accounts:
        return "No active accounts found"

    overall = {
        'total_types_processed':  0,
        'total_items_generated':  0,
        'type_summaries':         []
    }

    for idx, cfg in enumerate(content_types_config):
        content_type   = cfg.get('workflow_type')
        content_name   = cfg.get('content_name')
        content_amount = cfg.get('content_amount', 25)
        prompt_name    = cfg.get('prompt_name')

        if not all([content_type, content_name, prompt_name]):
            logger.error(f"Invalid config at index {idx}: {cfg}")
            continue

        logger.info(f"\n{'='*80}\nTYPE {idx+1}: {content_type} | prompt={prompt_name}\n{'='*80}")

        type_generated = 0
        type_success   = 0
        type_failed    = 0

        for account in accounts:
            try:
                count = generate_content_for_account(
                    account_id=account['account_id'],
                    content_type=content_type,
                    amount=content_amount,
                    content_name=content_name,
                    prompt_name=prompt_name
                )
                if count > 0:
                    type_generated += count
                    type_success   += 1
                    logger.info(f"✅ {account['username']}: {count} items")
                else:
                    type_failed += 1
                    logger.warning(f"⚠️ {account['username']}: 0 items")
            except Exception as e:
                type_failed += 1
                logger.error(f"❌ {account['username']}: {e}")

        overall['total_types_processed']  += 1
        overall['total_items_generated']  += type_generated
        overall['type_summaries'].append({
            'content_type':    content_type,
            'prompt_name':     prompt_name,
            'content_name':    content_name,
            'content_amount':  content_amount,
            'total_generated': type_generated,
            'success_accounts': type_success,
            'failed_accounts':  type_failed,
        })

    # Final summary log
    summary_lines = [
        f"\n{'='*80}",
        "FINAL SUMMARY",
        f"Types processed: {overall['total_types_processed']}",
        f"Total items:     {overall['total_items_generated']}",
    ]
    for s in overall['type_summaries']:
        summary_lines.append(
            f"  {s['content_type'].upper()} | prompt={s['prompt_name']} | "
            f"generated={s['total_generated']} | "
            f"ok={s['success_accounts']} | fail={s['failed_accounts']}"
        )
    summary_lines.append('='*80)
    final_summary = '\n'.join(summary_lines)
    logger.info(final_summary)

    _trigger_create_workflows_dag(overall)
    return final_summary


def _trigger_create_workflows_dag(summary: dict):
    total = summary.get('total_items_generated', 0)
    if total == 0:
        logger.warning("No content generated — skipping create_workflows trigger")
        return

    run_id = f"triggered_by_create_content_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    conf   = {
        'triggered_by':         'create_content',
        'total_items_generated': total,
        'types_processed':       summary.get('total_types_processed', 0),
    }

    try:
        from airflow.api.common.experimental.trigger_dag import trigger_dag
        from airflow.utils import timezone as airflow_timezone
        trigger_dag(dag_id='create_workflows', run_id=run_id, conf=conf,
                    execution_date=airflow_timezone.utcnow(), replace_microseconds=False)
        logger.info(f"✅ Triggered create_workflows (run_id={run_id})")
    except ImportError:
        try:
            from airflow.api.common.trigger_dag import trigger_dag as trigger_dag_v2
            trigger_dag_v2(dag_id='create_workflows', run_id=run_id, conf=conf)
            logger.info(f"✅ Triggered create_workflows via v2 API (run_id={run_id})")
        except Exception as e2:
            logger.error(f"❌ Failed to trigger create_workflows (v2): {e2}")
    except Exception as e:
        logger.error(f"❌ Failed to trigger create_workflows: {e}")


def log_daily_configuration():
    try:
        daily_config         = get_daily_content_config()
        content_types_config = daily_config.get('content_types', [])
        schedule             = get_run_schedule_from_db()

        if not daily_config.get('enabled', False) or not content_types_config:
            default_config = build_default_content_config()
            logger.info(f"Day={daily_config.get('day_name')} | No weekly settings — using {len(default_config)} default(s)")
            content_types_config = default_config

        logger.info(f"Run schedule: {schedule.get('num_runs')} run(s)/day at {schedule.get('run_times')}")
        for i, ct in enumerate(content_types_config):
            logger.info(
                f"  Type {i+1}: {ct.get('workflow_type')} | "
                f"prompt={ct.get('prompt_name')} | "
                f"name={ct.get('content_name')} | "
                f"amount={ct.get('content_amount')}"
            )

        accounts = get_all_active_accounts()
        return f"Config: {len(content_types_config)} type(s), {len(accounts)} accounts, schedule={schedule.get('run_times')}"
    except Exception as e:
        logger.error(f"Error logging config: {e}")
        return f"Error: {e}"


def refresh_generation_settings():
    try:
        daily_config         = get_daily_content_config()
        content_types_config = daily_config.get('content_types', [])
        if not daily_config.get('enabled', False) or not content_types_config:
            content_types_config = build_default_content_config()

        accounts = get_all_active_accounts()
        for ct in content_types_config:
            ready = sum(
                1 for a in accounts
                if get_account_prompt_by_name(a['account_id'], ct['workflow_type'], ct['prompt_name'])
            )
            logger.info(
                f"  {ct['workflow_type']} [{ct['prompt_name']}]: "
                f"{ready}/{len(accounts)} accounts ready"
            )
        return f"Refreshed: {len(content_types_config)} type(s), {len(accounts)} accounts"
    except Exception as e:
        logger.error(f"Error refreshing settings: {e}")
        return f"Error: {e}"


# ============================================================================
# DAG DEFINITION — schedule_interval built from DB setting at parse time
# ============================================================================

default_args = {
    'owner':      'airflow',
    'start_date': datetime(2025, 6, 27),
    'retries':    0,
}

timezone_str = "Africa/Nairobi"
dag_timezone = pytz.timezone(timezone_str)

# Read the saved run times from DB at DAG parse time.
# Airflow re-parses DAGs periodically, so changes in the UI will be picked up
# without a manual redeploy (subject to dag_dir_list_interval, default 5 min).
_schedule = build_schedule_interval_from_db()
logger.info(f"create_content DAG schedule_interval resolved to: '{_schedule}'")

with DAG(
    "create_content",
    default_args=default_args,
    description=(
        f"Multi-type content generation. Schedule driven by 'content_run_schedule' "
        f"DB setting (currently: {_schedule}). Falls back to DB-prompt defaults if no "
        f"weekly settings configured. Triggers create_workflows on completion."
    ),
    schedule_interval=_schedule,
    catchup=False,
    max_active_runs=1,
    tags=['content', 'gemini', 'multi-type', 'prompt-specific', 'dynamic-schedule'],
) as dag:
    dag.timezone = dag_timezone

    start_task = DummyOperator(task_id="start")

    log_config_task = PythonOperator(
        task_id="log_daily_configuration",
        python_callable=log_daily_configuration,
    )

    refresh_settings_task = PythonOperator(
        task_id="refresh_generation_settings",
        python_callable=refresh_generation_settings,
    )

    generate_all_content_task = PythonOperator(
        task_id="generate_all_content_for_today",
        python_callable=generate_all_content_for_today,
    )

    end_task = DummyOperator(task_id="end")

    start_task >> log_config_task >> refresh_settings_task >> generate_all_content_task >> end_task
