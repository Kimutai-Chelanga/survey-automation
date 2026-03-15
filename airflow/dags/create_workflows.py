# File: dags/create_automa_account_centric.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from dag_components.automa_workflow_dag.config import (
    DEFAULT_ARGS,
    WORKFLOW_GENERATION_ENABLED,
    logger,
    get_today_automa_config,
    get_template_from_mongodb,
    get_content_by_name_and_type,
    get_workflow_names_from_config,
    get_collection_name,
    get_database_name,
    validate_automa_config,
    log_configuration_summary
)
from dag_components.automa_workflow_dag.workflow_generation import (
    create_workflow_with_multi_content,
    update_has_content
)
from dag_components.automa_workflow_dag.tracking import (
    log_workflow_generation,
    update_workflow_connection,
    log_execution_record
)
from dag_components.automa_workflow_dag.db_utils import log_sync_error
from core.database.postgres.connection import get_postgres_connection
from core.database.postgres.content_handler import get_content_handler
import time
import os
from datetime import datetime, date, timedelta
from bson import ObjectId


# ============================================================================
# DEFAULT CONFIG BUILDER
# ============================================================================

def get_all_active_prompts_from_db() -> list:
    """Fetch all active prompts ordered by type then name."""
    try:
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


def get_default_template_id() -> str:
    """
    Get the MongoDB _id of the default 'Automation_template' in category 'x'.
    Returns None if not found.
    """
    try:
        from core.database.mongodb.connection import get_mongo_collection
        col = get_mongo_collection("workflow_templates")
        if col is None:
            return None
        doc = col.find_one(
            {'template_name': 'Automation_template', 'category': 'x', 'is_active': True},
            {'_id': 1}
        )
        return str(doc['_id']) if doc else None
    except Exception as e:
        logger.error(f"Error fetching default template id: {e}")
        return None


def build_default_automa_config() -> dict:
    """
    Build a full automa config using defaults when no weekly settings exist.

    - collection_name is permanent (no date stamp) so the filtering config never
      needs to be updated after the first run.
    - press_keys and click_elements default to 'range' mode (0ms – 10000ms).
    - typing_delay defaults to 'fixed' mode at 4000ms (4 seconds), matching
      Automa's "Typing delay (millisecond)" field on the forms node.
      Range mode is intentionally not used for typing_delay — it is a direct
      passthrough value to Automa, not a workflow-level randomisation parameter.
    """
    today    = date.today()
    day_name = today.strftime('%A')
    day_key  = today.strftime('%A').lower()

    # 1. Fetch prompts
    prompts = get_all_active_prompts_from_db()
    if not prompts:
        logger.error("No active prompts found — cannot build default automa config")
        return None

    content_items = []
    for i in range(3):
        p = prompts[i % len(prompts)]
        content_items.append({
            'label':        f'CONTENT{i+1}',
            'content_type': p['prompt_type'],
            'content_name': p['prompt_name'],
        })
        logger.info(f"Default CONTENT{i+1}: {p['prompt_type']} / {p['prompt_name']}")

    # 2. Template
    template_id   = get_default_template_id()
    template_name = 'Automation_template'
    if not template_id:
        logger.error("Default template 'Automation_template' not found in MongoDB category 'x'")
        return None

    # 3. Permanent collection name (no date stamp)
    collection_name = 'x_x_automation_template_permanent'

    return {
        'config_date':          today.isoformat(),
        'day_name':             day_name,
        'day_key':              day_key,
        'content_items':        content_items,
        'content_type':         content_items[0]['content_type'],
        'content_name':         content_items[0]['content_name'],
        'source_category':      'x',
        'workflow_source_id':   template_id,
        'workflow_source_name': template_name,
        'destination_category': 'x',
        'workflow_type':        'x',
        'collection_name':      collection_name,
        'database':             'execution_workflows',
        # Press keys: range mode, 0ms – 10000ms (randomised per character block)
        'press_keys': {
            'mode':             'range',
            'min_milliseconds': 0,
            'max_milliseconds': 10000
        },
        # Click elements: range mode, 0ms – 10000ms
        'click_elements': {
            'mode':             'range',
            'min_milliseconds': 0,
            'max_milliseconds': 10000
        },
        # Typing delay: FIXED mode only — direct passthrough to Automa forms node
        # node['data']['delay'] = str(typing_delay_ms)
        # This is Automa's "Typing delay (millisecond)" field; range is not applicable.
        'typing_delay': {
            'mode':             'fixed',
            'min_milliseconds': 80,
            'max_milliseconds': 80
        },
        'updated_at': datetime.now().isoformat()
    }


# ============================================================================
# MAIN TASK
# ============================================================================

def generate_automa_workflows_task():
    """
    Generate workflows with MULTI-CONTENT support, scoped per account.

    CONTENT1 / CONTENT2 → character-by-character press-key blocks (press_keys timing).
    CONTENT3            → injected directly into the forms (text-field) node.
                          node.data.value  = content text
                          node.data.delay  = typing_delay resolved from typing_delay config
                          (matches Automa's "Typing delay (millisecond)" field)

    typing_delay is always 'fixed' mode — it is a direct passthrough to Automa's
    forms node, not a randomised timing parameter. The value set in the UI is the
    exact millisecond value written to node['data']['delay'].

    Each account's content is used to generate that account's workflows independently.
    Falls back to built-in defaults if no weekly settings are configured.
    Respects press_keys / click_elements 'mode' (fixed | range).

    FIX: postgres_account_id in workflow_metadata is now guaranteed to be the
    loop account's account_id, not whatever account_id happens to be stored on
    the content row. This ensures filter_links can correctly scope workflows
    per account.
    """
    if not WORKFLOW_GENERATION_ENABLED:
        logger.warning("Workflow generation is disabled")
        return "Workflow generation disabled."

    log_configuration_summary()

    # Try to get today's config; fall back to defaults
    automa_config = get_today_automa_config()

    if not automa_config:
        logger.warning("No automa config found for today — using built-in defaults")
        automa_config = build_default_automa_config()
        if not automa_config:
            return "❌ No automa config and could not build defaults (no prompts / no default template)"

    # Validate (also normalises timing configs in place)
    is_valid, error_msg = validate_automa_config(automa_config)
    if not is_valid:
        logger.error(f"❌ Config validation failed: {error_msg}")
        return f"Configuration validation failed: {error_msg}"

    # Log timing modes being used
    press_mode  = automa_config.get('press_keys', {}).get('mode', 'range')
    click_mode  = automa_config.get('click_elements', {}).get('mode', 'range')
    typing_mode = automa_config.get('typing_delay', {}).get('mode', 'fixed')
    typing_ms   = automa_config.get('typing_delay', {}).get('max_milliseconds', 4000)
    logger.info(f"⌨️  Press keys mode:   {press_mode}")
    logger.info(f"🖱️  Click elems mode:  {click_mode}")
    logger.info(f"⌚ Typing delay:      {typing_mode} @ {typing_ms}ms (Automa forms node)")

    # Extract config fields
    content_items_config = automa_config.get('content_items', [])
    workflow_source_id   = automa_config.get('workflow_source_id')
    destination_category = automa_config.get('destination_category')
    workflow_type        = automa_config.get('workflow_type')
    collection_name      = get_collection_name(automa_config)
    database_name        = get_database_name(automa_config)

    logger.info(f"🚀 Starting account-centric workflow generation:")
    logger.info(f"   Content items: {len(content_items_config)}")
    for item in content_items_config:
        logger.info(f"   - {item['label']}: {item['content_type']} / {item['content_name']}")
    logger.info(f"   Template ID:  {workflow_source_id}")
    logger.info(f"   Destination:  {destination_category} / {workflow_type}")
    logger.info(f"   Collection:   {database_name}.{collection_name}")

    # Load template once — shared across all accounts
    template_data = get_template_from_mongodb(workflow_source_id)
    if not template_data:
        return f"❌ Failed to load template {workflow_source_id}"

    # Get all active accounts
    from psycopg2.extras import RealDictCursor
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT account_id, username, profile_id FROM accounts ORDER BY account_id")
                accounts = [dict(r) for r in cursor.fetchall()]
    except Exception as e:
        logger.error(f"❌ Failed to fetch accounts: {e}")
        return f"❌ Failed to fetch accounts: {e}"

    if not accounts:
        return "❌ No active accounts found"

    logger.info(f"👥 Processing {len(accounts)} account(s)")

    overall_stats = {
        "total_generated": 0,
        "total_marked":    0,
        "total_skipped":   0,
        "total_connected": 0,
        "accounts_ok":     0,
        "accounts_failed": 0,
    }

    workflow_names = get_workflow_names_from_config(automa_config)

    for account in accounts:
        account_id       = account['account_id']
        account_username = account.get('username', f'account_{account_id}')

        logger.info("=" * 80)
        logger.info(f"👤 Account {account_id} ({account_username})")

        # ── Fetch content per label scoped to this account ────────────────────
        content_items_by_label = {}
        account_has_content    = True

        for item_config in content_items_config:
            label        = item_config['label']
            content_type = item_config['content_type']
            content_name = item_config['content_name']

            logger.info(f"   📥 Fetching {label} for account {account_id}: {content_type}/{content_name}")
            items = get_content_by_name_and_type(content_name, content_type, account_id=account_id)

            if not items:
                logger.warning(
                    f"   ⚠️ No unused content for {label} "
                    f"(account={account_id}, name='{content_name}', type='{content_type}') — skipping account"
                )
                account_has_content = False
                break

            content_items_by_label[label] = {
                'config':        item_config,
                'content_items': items,
                'content_type':  content_type,
            }
            logger.info(f"   ✅ Found {len(items)} items for {label}")

        if not account_has_content:
            overall_stats["accounts_failed"] += 1
            continue

        min_count = min(len(d['content_items']) for d in content_items_by_label.values())
        logger.info(f"   📊 Will generate {min_count} workflow(s) for account {account_id}")

        if min_count == 0:
            logger.warning(f"   ⚠️ No content available for account {account_id} — skipping")
            overall_stats["accounts_failed"] += 1
            continue

        account_stats = {"generated": 0, "marked": 0, "skipped": 0, "connected": 0}
        successful_workflow_ids = []

        for workflow_idx in range(min_count):
            try:
                content_items_data   = []
                all_content_handlers = {}

                for label in sorted(content_items_by_label.keys()):
                    data         = content_items_by_label[label]
                    content_item = data['content_items'][workflow_idx]
                    content_type = data['content_type']

                    content_items_data.append({
                        'label':        label,
                        'content_text': content_item.get('content'),
                        'content_id':   int(content_item.get('content_id')),
                        'content_type': content_type,
                        # FIX: always use the loop account_id, never trust the content row's
                        # account_id alone — content rows may have been generated under a
                        # different account or the field may be stale.
                        'account_id':   account_id,
                        'prompt_id':    content_item.get('prompt_id'),
                        'username':     account_username,
                        'profile_id':   account.get('profile_id') or content_item.get('profile_id'),
                    })

                    if content_type not in all_content_handlers:
                        all_content_handlers[content_type] = get_content_handler(content_type)

                name = workflow_names[workflow_idx % len(workflow_names)]
                logger.info(f"   🔨 Workflow #{workflow_idx + 1} for account {account_id}: '{name}'")

                result = create_workflow_with_multi_content(
                    workflow_type=workflow_type,
                    template_data=template_data,
                    name=name,
                    content_items_data=content_items_data,
                    automa_config=automa_config,
                    collection_name=collection_name,
                    database_name=database_name,
                )

                (success_status, automa_workflow_id, execution_start, execution_end,
                 execution_time_ms, blocks_generated, has_link, workflow_name,
                 template_used, workflow_id, content_ids_used) = result

                if success_status and automa_workflow_id:
                    try:
                        primary_item = content_items_data[0]
                        log_workflow_generation(
                            workflow_type=workflow_type,
                            name=workflow_name,
                            content_id=primary_item['content_id'],
                            automa_workflow_id=automa_workflow_id,
                            account_id=account_id,  # FIX: use loop account_id
                            prompt_id=primary_item['prompt_id'],
                            workflow_id=workflow_id,
                            username=account_username,  # FIX: use loop username
                            profile_id=account.get('profile_id'),  # FIX: use loop profile_id
                        )

                        for item in content_items_data:
                            update_workflow_connection(
                                workflow_type=item['content_type'],
                                content_id=item['content_id'],
                                automa_workflow_id=automa_workflow_id,
                                workflow_name=workflow_name,
                                account_id=account_id,  # FIX: use loop account_id
                                prompt_id=item['prompt_id'],
                                workflow_id=workflow_id,
                            )

                        account_stats["generated"] += 1
                        account_stats["connected"] += len(content_items_data)
                        successful_workflow_ids.append(automa_workflow_id)

                        logger.info(
                            f"   ✅ Workflow #{workflow_idx + 1} (account {account_id}): "
                            f"id={automa_workflow_id}, content_ids={content_ids_used}"
                        )

                    except Exception as e:
                        logger.error(f"   Post-generation failed for account {account_id} #{workflow_idx + 1}: {e}")
                        log_sync_error(workflow_type, str(automa_workflow_id), "postgres_to_mongo", str(e))
                else:
                    logger.error(f"   Failed to create workflow #{workflow_idx + 1} for account {account_id}")
                    account_stats["skipped"] += 1
                    continue

                # Mark all content items as used
                try:
                    for item in content_items_data:
                        all_content_handlers[item['content_type']].mark_as_used(item['content_id'])
                        account_stats["marked"] += 1
                except Exception as e:
                    logger.error(f"   Failed to mark content as used for account {account_id}: {e}")

                time.sleep(0.1)

            except Exception as e:
                logger.error(f"   Error on workflow #{workflow_idx + 1} for account {account_id}: {e}")
                import traceback
                logger.error(traceback.format_exc())
                account_stats["skipped"] += 1
                continue

        # Update has_content for this account's workflows
        try:
            if successful_workflow_ids:
                updated = update_has_content({workflow_type: successful_workflow_ids})
                logger.info(f"   ✅ has_content=True for {updated} workflows (account {account_id})")
        except Exception as e:
            logger.error(f"   Failed to update has_content for account {account_id}: {e}")

        logger.info(
            f"   📋 Account {account_id} summary: "
            f"generated={account_stats['generated']} "
            f"connected={account_stats['connected']} "
            f"marked={account_stats['marked']} "
            f"skipped={account_stats['skipped']}"
        )

        overall_stats["total_generated"] += account_stats["generated"]
        overall_stats["total_marked"]    += account_stats["marked"]
        overall_stats["total_skipped"]   += account_stats["skipped"]
        overall_stats["total_connected"] += account_stats["connected"]
        overall_stats["accounts_ok"]     += 1

    logger.info("=" * 80)
    logger.info("🏁 OVERALL SUMMARY")
    logger.info(f"   Accounts OK:      {overall_stats['accounts_ok']}")
    logger.info(f"   Accounts failed:  {overall_stats['accounts_failed']}")
    logger.info(f"   Total generated:  {overall_stats['total_generated']}")
    logger.info(f"   Total connected:  {overall_stats['total_connected']}")
    logger.info(f"   Total marked:     {overall_stats['total_marked']}")
    logger.info(f"   Total skipped:    {overall_stats['total_skipped']}")
    logger.info("=" * 80)

    return (
        f"Generated {overall_stats['total_generated']} workflows across "
        f"{overall_stats['accounts_ok']} account(s) "
        f"(press={press_mode}, click={click_mode}, typing=fixed@{typing_ms}ms), "
        f"connected {overall_stats['total_connected']}, "
        f"marked {overall_stats['total_marked']} as used, "
        f"skipped {overall_stats['total_skipped']}."
    )

# ============================================================================
# DAG Definition
# ============================================================================

with DAG(
    'create_workflows',
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    description="Generate Automa workflows with multi-content support. Falls back to defaults if no settings configured."
) as dag:
    from datetime import timedelta

    generate_task = PythonOperator(
        task_id='generate_workflows',
        python_callable=generate_automa_workflows_task,
        execution_timeout=timedelta(minutes=10),
        dag=dag
    )
