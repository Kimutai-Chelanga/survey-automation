"""
Reverse DAGs Page
=================
Allows reversing pipeline stages in reverse order:
  5. Execute (filtering_report_with_workflows)
  4. Filter Links (filter_links)
  3. Extract Links (extract_links)
  2. Create Workflows (create_workflows)
  1. Create Content (create_content)

Selecting a stage automatically includes all later stages that depend on it.
Each reversal is idempotent and shows a detailed audit trail.

ALIGNMENT NOTES (2026-03-06):
  filter_links DAG:
    - Links are GLOBAL — not owned by accounts.
    - Stage 1 (content filter): sets filtered=TRUE/FALSE, filtered_time
    - Stage 2 (quantity filter): sets within_limit=TRUE/FALSE
    - Stage 2b (chat link): sets chat_link on links table
    - Stage 3 (workflow assignment): sets workflow_status, workflow_type ONLY.
      Does NOT set used=TRUE on links. used remains FALSE.
    - MongoDB: writes workflow_metadata with assignment_source='weekly_schedule'
               and content_workflow_links docs.

  filter_links reversal therefore:
    - Resets filtered, filtered_time, within_limit, chat_link, workflow_status,
      workflow_type on links where workflow_status IS NOT NULL and filtered_time >= today
    - Deletes workflow_metadata docs with assignment_source='weekly_schedule' and
      link_assigned_at >= today
    - Clears has_link on remaining workflow_metadata docs
    - Deletes content_workflow_links docs linked_at >= today

  execute DAG:
    - Sets executed=TRUE, success=TRUE, workflow_status='completed',
      workflow_processed_time on links
    - Sets executed=TRUE on workflow_metadata docs

  execute reversal:
    - Resets executed, success, workflow_status, workflow_processed_time
      on links where executed=TRUE and workflow_processed_time >= today
    - Resets executed on workflow_metadata docs with execution_mode='dag_auto_download'
    - Deletes filter_reports docs generated today
"""

import streamlit as st
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Any

logger = logging.getLogger(__name__)

try:
    from pymongo import MongoClient
    MONGO_AVAILABLE = True
except ImportError:
    MONGO_AVAILABLE = False

from src.core.database.postgres.connection import get_postgres_connection
from psycopg2.extras import RealDictCursor
POSTGRES_AVAILABLE = True

MONGODB_URI = os.getenv(
    'MONGODB_URI',
    'mongodb://app_user:app_password@mongodb:27017/messages_db?authSource=admin'
)


# ============================================================================
# DB HELPERS
# ============================================================================

def _get_mongo_client():
    """Return a connected MongoClient, or None if unavailable.
    IMPORTANT: caller must call client.close() when done.
    Never use 'if client:' — pymongo Database/Client objects raise on bool().
    Always use 'if client is not None:'.
    """
    if not MONGO_AVAILABLE:
        return None
    try:
        client = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        return client
    except Exception as e:
        logger.error(f"MongoDB connection failed: {e}")
        return None


# ============================================================================
# REVERSAL FUNCTIONS — one per DAG stage
# ============================================================================

def reverse_execute_dag(dry_run: bool = False) -> Dict[str, Any]:
    """
    Reverse: filtering_report_with_workflows
    ─────────────────────────────────────────
    Undoes:
      • PostgreSQL links: executed=FALSE, workflow_status reset to 'completed'
        (back to post-filter_links state), success=FALSE, workflow_processed_time=NULL
        Matches on: executed=TRUE AND workflow_processed_time >= today
      • MongoDB workflow_metadata: executed=FALSE, status='ready_to_execute'
        Matches on: executed=TRUE AND execution_mode='dag_auto_download'
      • MongoDB filter_reports: deletes today's report docs (cannot unsend email)
    """
    log    = []
    totals = {
        "postgres_links_reset":    0,
        "mongo_metadata_reset":    0,
        "filter_reports_deleted":  0,
    }
    errors      = []
    now         = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # ── PostgreSQL ─────────────────────────────────────────────────────────────
    # execute DAG sets: executed=TRUE, success=TRUE, workflow_status='completed',
    # processed_by_workflow=TRUE, workflow_processed_time=now
    # We reset to the post-filter_links state:
    # executed=FALSE, success=FALSE, workflow_status='completed' stays
    # (filter_links set it to 'completed'; execute DAG did not change workflow_status)
    # Actually execute DAG sets workflow_status='completed' again via mark function —
    # so we reset executed/success/processed cols but leave workflow_status as-is
    # since filter_links already set it.
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if dry_run:
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM links
                        WHERE executed = TRUE
                          AND workflow_processed_time >= %s
                    """, (today_start,))
                    r   = cur.fetchone()
                    cnt = r['cnt'] if r else 0
                    totals["postgres_links_reset"] = cnt
                    log.append(
                        f"🔍 DRY RUN — would reset {cnt} executed links in PostgreSQL"
                    )
                else:
                    cur.execute("""
                        UPDATE links
                        SET executed                = FALSE,
                            processed_by_workflow   = FALSE,
                            workflow_processed_time = NULL,
                            success                 = FALSE
                        WHERE executed = TRUE
                          AND workflow_processed_time >= %s
                        RETURNING links_id
                    """, (today_start,))
                    rows = cur.fetchall()
                    totals["postgres_links_reset"] = len(rows)
                    conn.commit()
                    log.append(
                        f"✅ PostgreSQL: reset {len(rows)} executed links "
                        f"(executed→FALSE, success→FALSE, workflow_processed_time→NULL)"
                    )
    except Exception as e:
        errors.append(f"PostgreSQL error: {e}")
        log.append(f"❌ PostgreSQL error: {e}")

    # ── MongoDB ────────────────────────────────────────────────────────────────
    client = _get_mongo_client()
    if client is None:
        log.append("⚠️ MongoDB unavailable — skipped")
    else:
        try:
            db   = client['messages_db']
            filt = {
                "executed":       True,
                "execution_mode": "dag_auto_download",
            }

            if dry_run:
                cnt = db.workflow_metadata.count_documents(filt)
                totals["mongo_metadata_reset"] = cnt
                log.append(
                    f"🔍 DRY RUN — would reset {cnt} workflow_metadata docs in MongoDB"
                )
            else:
                result = db.workflow_metadata.update_many(
                    filt,
                    {"$set": {
                        "executed":         False,
                        "success":          False,
                        "status":           "ready_to_execute",
                        "executed_at":      None,
                        "execution_mode":   None,
                        "execution_source": None,
                        "updated_at":       now.isoformat(),
                        "_reversed_at":     now.isoformat(),
                        "_reversed_by":     "reverse_dags_page",
                    }}
                )
                totals["mongo_metadata_reset"] = result.modified_count
                log.append(
                    f"✅ MongoDB workflow_metadata: reset {result.modified_count} docs "
                    f"→ executed=False, status='ready_to_execute'"
                )

            report_filt = {
                "generated_at": {"$gte": today_start},
                "report_type":  "simple_link_filtering",
            }
            if dry_run:
                cnt2 = db.filter_reports.count_documents(report_filt)
                totals["filter_reports_deleted"] = cnt2
                log.append(f"🔍 DRY RUN — would delete {cnt2} filter_reports docs")
            else:
                del_result = db.filter_reports.delete_many(report_filt)
                totals["filter_reports_deleted"] = del_result.deleted_count
                log.append(
                    f"✅ MongoDB filter_reports: deleted {del_result.deleted_count} "
                    f"today's report docs"
                )
        except Exception as e:
            errors.append(f"MongoDB error: {e}")
            log.append(f"❌ MongoDB error: {e}")
        finally:
            client.close()

    log.append("📧 NOTE: Email already sent cannot be unsent — that action is irreversible.")

    return {
        "stage":   "execute",
        "dag":     "filtering_report_with_workflows",
        "dry_run": dry_run,
        "totals":  totals,
        "log":     log,
        "errors":  errors,
        "success": len(errors) == 0,
    }


def reverse_filter_links_dag(dry_run: bool = False) -> Dict[str, Any]:
    """
    Reverse: filter_links
    ──────────────────────
    Aligned with current filter_links DAG behaviour:

    Stage 1 sets: filtered=TRUE/FALSE, filtered_time=now
    Stage 2 sets: within_limit=TRUE/FALSE
    Stage 2b sets: chat_link on links table
    Stage 3 sets: workflow_status='completed'/'failed'/'stored_in_workflow'/'no_workflow_available',
                  workflow_type=<type>
                  Does NOT set used=TRUE — links.used remains FALSE throughout.

    MongoDB:
      - workflow_metadata: upserted with assignment_source='weekly_schedule',
        link_assigned_at=now, has_link=True
      - content_workflow_links: inserted with linked_at=now

    Reversal:
      • PostgreSQL links: reset filtered, filtered_time, within_limit, chat_link,
        workflow_status, workflow_type WHERE filtered_time >= today
        (workflow_status may be NULL on some rows — use filtered_time as the anchor)
      • MongoDB workflow_metadata: DELETE docs with assignment_source='weekly_schedule'
        AND link_assigned_at >= today
      • MongoDB workflow_metadata: CLEAR has_link on any remaining docs
        where link_assigned_at >= today (belt-and-suspenders)
      • MongoDB content_workflow_links: DELETE docs with linked_at >= today
    """
    log    = []
    totals = {
        "postgres_links_reset":           0,
        "mongo_metadata_deleted":         0,
        "mongo_metadata_has_link_cleared": 0,
        "content_workflow_links_deleted": 0,
    }
    errors      = []
    now         = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    # ── PostgreSQL ─────────────────────────────────────────────────────────────
    # filter_links writes filtered_time in Stage 1 for every link it processes.
    # We use filtered_time >= today as the anchor since used is never set to TRUE.
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if dry_run:
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM links
                        WHERE filtered_time >= %s
                    """, (today_start,))
                    r   = cur.fetchone()
                    cnt = r['cnt'] if r else 0
                    totals["postgres_links_reset"] = cnt
                    log.append(
                        f"🔍 DRY RUN — would reset {cnt} links in PostgreSQL "
                        f"(filtered_time >= today)"
                    )
                else:
                    cur.execute("""
                        UPDATE links
                        SET filtered                = NULL,
                            filtered_time           = NULL,
                            within_limit            = NULL,
                            chat_link               = NULL,
                            processed_by_workflow   = FALSE,
                            workflow_processed_time = NULL,
                            workflow_status         = NULL,
                            workflow_type           = NULL
                        WHERE filtered_time >= %s
                        RETURNING links_id
                    """, (today_start,))
                    rows = cur.fetchall()
                    totals["postgres_links_reset"] = len(rows)
                    conn.commit()
                    log.append(
                        f"✅ PostgreSQL: reset {len(rows)} links "
                        f"(filtered/within_limit/chat_link/workflow_status → NULL)"
                    )
    except Exception as e:
        errors.append(f"PostgreSQL error: {e}")
        log.append(f"❌ PostgreSQL error: {e}")

    # ── MongoDB ────────────────────────────────────────────────────────────────
    client = _get_mongo_client()
    if client is None:
        log.append("⚠️ MongoDB unavailable — skipped")
    else:
        try:
            db = client['messages_db']

            # Delete workflow_metadata docs written by filter_links
            # (these are upserts with assignment_source='weekly_schedule')
            meta_filt = {
                "assignment_source": "weekly_schedule",
                "link_assigned_at":  {"$gte": today_start},
            }
            if dry_run:
                cnt = db.workflow_metadata.count_documents(meta_filt)
                totals["mongo_metadata_deleted"] = cnt
                log.append(
                    f"🔍 DRY RUN — would delete {cnt} workflow_metadata "
                    f"link-assignment docs (assignment_source='weekly_schedule')"
                )
            else:
                del_result = db.workflow_metadata.delete_many(meta_filt)
                totals["mongo_metadata_deleted"] = del_result.deleted_count
                log.append(
                    f"✅ MongoDB workflow_metadata: deleted {del_result.deleted_count} "
                    f"link-assignment docs"
                )

            # Belt-and-suspenders: clear has_link on any surviving docs
            # that had link_assigned_at >= today (e.g. docs from create_workflows
            # that got a link injected but weren't caught by the delete above)
            remaining_filt = {"link_assigned_at": {"$gte": today_start}}
            if dry_run:
                cnt2 = db.workflow_metadata.count_documents(remaining_filt)
                totals["mongo_metadata_has_link_cleared"] = cnt2
                log.append(
                    f"🔍 DRY RUN — would clear has_link on {cnt2} remaining "
                    f"workflow_metadata docs"
                )
            else:
                flag_result = db.workflow_metadata.update_many(
                    remaining_filt,
                    {"$set": {
                        "has_link":         False,
                        "link_url":         None,
                        "chat_link":        None,
                        "link_assigned_at": None,
                        "status":           "generated",
                        "execute":          False,
                        "executed":         False,
                        "_reversed_at":     now.isoformat(),
                    }}
                )
                totals["mongo_metadata_has_link_cleared"] = flag_result.modified_count
                log.append(
                    f"✅ MongoDB workflow_metadata: cleared has_link on "
                    f"{flag_result.modified_count} remaining docs"
                )

            # Delete content_workflow_links docs
            cwl_filt = {"linked_at": {"$gte": today_start}}
            if dry_run:
                cnt3 = db.content_workflow_links.count_documents(cwl_filt)
                totals["content_workflow_links_deleted"] = cnt3
                log.append(
                    f"🔍 DRY RUN — would delete {cnt3} content_workflow_links docs"
                )
            else:
                cwl_result = db.content_workflow_links.delete_many(cwl_filt)
                totals["content_workflow_links_deleted"] = cwl_result.deleted_count
                log.append(
                    f"✅ MongoDB content_workflow_links: deleted "
                    f"{cwl_result.deleted_count} docs"
                )

        except Exception as e:
            errors.append(f"MongoDB error: {e}")
            log.append(f"❌ MongoDB error: {e}")
        finally:
            client.close()

    return {
        "stage":   "filter_links",
        "dag":     "filter_links",
        "dry_run": dry_run,
        "totals":  totals,
        "log":     log,
        "errors":  errors,
        "success": len(errors) == 0,
    }


def reverse_extract_links_dag(dry_run: bool = False) -> Dict[str, Any]:
    """
    Reverse: extract_links
    ───────────────────────
    Undoes:
      • PostgreSQL links: deletes rows inserted today that are unprocessed
        (tweet_id ON CONFLICT dedup makes it safe to re-extract after deletion)
      Matches on: scraped_time >= today AND used=FALSE AND executed=FALSE
      AND filtered IS NULL (untouched by filter_links — belt-and-suspenders
      to avoid deleting links that filter_links already processed)
    """
    log    = []
    totals = {"postgres_links_deleted": 0}
    errors      = []
    now         = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if dry_run:
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM links
                        WHERE scraped_time >= %s
                          AND used        = FALSE
                          AND executed    = FALSE
                          AND filtered    IS NULL
                    """, (today_start,))
                    r   = cur.fetchone()
                    cnt = r['cnt'] if r else 0
                    totals["postgres_links_deleted"] = cnt
                    log.append(
                        f"🔍 DRY RUN — would delete {cnt} unprocessed links "
                        f"(scraped_time >= today, filtered IS NULL)"
                    )
                else:
                    cur.execute("""
                        DELETE FROM links
                        WHERE scraped_time >= %s
                          AND used        = FALSE
                          AND executed    = FALSE
                          AND filtered    IS NULL
                        RETURNING links_id
                    """, (today_start,))
                    rows = cur.fetchall()
                    totals["postgres_links_deleted"] = len(rows)
                    conn.commit()
                    log.append(
                        f"✅ PostgreSQL: deleted {len(rows)} unprocessed links "
                        f"extracted today (filtered IS NULL guard applied)"
                    )
    except Exception as e:
        errors.append(f"PostgreSQL error: {e}")
        log.append(f"❌ PostgreSQL error: {e}")

    return {
        "stage":   "extract_links",
        "dag":     "extract_links",
        "dry_run": dry_run,
        "totals":  totals,
        "log":     log,
        "errors":  errors,
        "success": len(errors) == 0,
    }


def reverse_create_workflows_dag(dry_run: bool = False) -> Dict[str, Any]:
    """
    Reverse: create_workflows  (create_automa_account_centric.py)
    ──────────────────────────────────────────────────────────────
    The DAG writes to 4 places — we undo all 4:

    MongoDB (messages_db.workflow_metadata):
      • created_at stored as ISO string, executed=False, status='generated'
      → DELETED for docs created today that have not been executed
        AND have no link assigned (has_link != True).
        Docs that already have a link assigned belong to filter_links — skip them.

    MongoDB (execution_workflows.<collection_name>):
      • Pure Automa workflow JSON, _id = automa_workflow_id (ObjectId)
      → DELETED for each matching metadata doc.

    PostgreSQL (content):
      • used=TRUE, used_time, automa_workflow_id, workflow_name,
        workflow_status='completed', has_content=TRUE, workflow_generated_time
      → RESET to pre-workflow state.

    PostgreSQL (workflow_generation_log):
      • One row per workflow logged today
      → DELETED.

    Note: workflow_sync_log is diagnostic only — not reversed.
    """
    log    = []
    totals = {
        "mongo_workflow_docs_deleted": 0,
        "mongo_metadata_deleted":      0,
        "pg_content_reset":            0,
        "pg_generation_log_deleted":   0,
    }
    errors       = []
    now          = datetime.now(timezone.utc)
    today_start  = now.replace(hour=0, minute=0, second=0, microsecond=0)
    today_prefix = now.strftime("%Y-%m-%d")

    # ── MongoDB ────────────────────────────────────────────────────────────────
    client = _get_mongo_client()
    if client is None:
        log.append("⚠️ MongoDB unavailable — skipped")
    else:
        try:
            db = client['messages_db']

            # Only delete metadata docs from create_workflows that haven't
            # been linked yet (has_link != True). Docs with has_link=True
            # were updated by filter_links and will be cleaned by its reversal.
            meta_filt = {
                "created_at": {"$regex": f"^{today_prefix}"},
                "executed":   {"$ne": True},
                "has_link":   {"$ne": True},
            }

            if dry_run:
                cnt = db.workflow_metadata.count_documents(meta_filt)
                totals["mongo_metadata_deleted"]      = cnt
                totals["mongo_workflow_docs_deleted"] = cnt
                log.append(
                    f"🔍 DRY RUN — would delete {cnt} workflow_metadata docs "
                    f"and {cnt} execution_workflows documents "
                    f"(has_link!=True guard applied)"
                )
            else:
                meta_docs = list(db.workflow_metadata.find(
                    meta_filt,
                    {"automa_workflow_id": 1, "database_name": 1, "collection_name": 1}
                ))
                log.append(
                    f"🔍 Found {len(meta_docs)} workflow_metadata docs to delete "
                    f"(has_link!=True, not executed, created today)"
                )

                deleted_wf     = 0
                failed_wf_dels = 0
                for doc in meta_docs:
                    wf_id     = doc.get("automa_workflow_id")
                    db_name   = doc.get("database_name", "execution_workflows")
                    coll_name = doc.get("collection_name")
                    if wf_id is None or not coll_name:
                        log.append(
                            "  ⚠️ Skipping doc missing automa_workflow_id or collection_name"
                        )
                        continue
                    try:
                        r = client[db_name][coll_name].delete_one({"_id": wf_id})
                        deleted_wf += r.deleted_count
                        if r.deleted_count == 0:
                            log.append(
                                f"  ⚠️ Workflow doc not found in "
                                f"{db_name}.{coll_name}: {wf_id}"
                            )
                    except Exception as inner_e:
                        failed_wf_dels += 1
                        log.append(
                            f"  ⚠️ Could not delete workflow doc {wf_id}: {inner_e}"
                        )

                totals["mongo_workflow_docs_deleted"] = deleted_wf
                log.append(
                    f"✅ MongoDB execution_workflows: deleted {deleted_wf} workflow docs"
                    + (f" ({failed_wf_dels} failed)" if failed_wf_dels else "")
                )

                del_result = db.workflow_metadata.delete_many(meta_filt)
                totals["mongo_metadata_deleted"] = del_result.deleted_count
                log.append(
                    f"✅ MongoDB workflow_metadata: deleted {del_result.deleted_count} docs"
                )
        except Exception as e:
            errors.append(f"MongoDB error: {e}")
            log.append(f"❌ MongoDB error: {e}")
        finally:
            client.close()

    # ── PostgreSQL ─────────────────────────────────────────────────────────────
    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:

                # Reset content rows — only those not yet executed
                if dry_run:
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM content
                        WHERE used = TRUE
                          AND workflow_generated_time >= %s
                          AND workflow_status != 'executed'
                    """, (today_start,))
                    r = cur.fetchone()
                    totals["pg_content_reset"] = r['cnt'] if r else 0
                    log.append(
                        f"🔍 DRY RUN — would reset "
                        f"{totals['pg_content_reset']} content rows"
                    )
                else:
                    cur.execute("""
                        UPDATE content
                        SET used                    = FALSE,
                            used_time               = NULL,
                            automa_workflow_id      = NULL,
                            workflow_name           = NULL,
                            workflow_status         = 'pending',
                            has_content             = FALSE,
                            workflow_generated_time = NULL
                        WHERE used = TRUE
                          AND workflow_generated_time >= %s
                          AND workflow_status != 'executed'
                        RETURNING content_id
                    """, (today_start,))
                    rows = cur.fetchall()
                    totals["pg_content_reset"] = len(rows)
                    conn.commit()
                    log.append(
                        f"✅ PostgreSQL content: reset {len(rows)} rows "
                        f"(used→FALSE, workflow fields cleared)"
                    )

                # Delete workflow_generation_log rows today
                if dry_run:
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM workflow_generation_log
                        WHERE generated_time >= %s
                    """, (today_start,))
                    r = cur.fetchone()
                    totals["pg_generation_log_deleted"] = r['cnt'] if r else 0
                    log.append(
                        f"🔍 DRY RUN — would delete "
                        f"{totals['pg_generation_log_deleted']} workflow_generation_log rows"
                    )
                else:
                    cur.execute("""
                        DELETE FROM workflow_generation_log
                        WHERE generated_time >= %s
                        RETURNING 1
                    """, (today_start,))
                    deleted_log = len(cur.fetchall())
                    totals["pg_generation_log_deleted"] = deleted_log
                    conn.commit()
                    log.append(
                        f"✅ PostgreSQL workflow_generation_log: deleted {deleted_log} rows"
                    )
    except Exception as e:
        errors.append(f"PostgreSQL error: {e}")
        log.append(f"❌ PostgreSQL error: {e}")

    log.append("ℹ️ workflow_sync_log rows are diagnostic only — not reversed.")

    return {
        "stage":   "create_workflows",
        "dag":     "create_workflows",
        "dry_run": dry_run,
        "totals":  totals,
        "log":     log,
        "errors":  errors,
        "success": len(errors) == 0,
    }


def reverse_create_content_dag(dry_run: bool = False) -> Dict[str, Any]:
    """
    Reverse: create_content
    ────────────────────────
    Undoes:
      • PostgreSQL content: deletes unused content rows created today
        (only rows where used=FALSE — anything consumed by create_workflows is untouched)
    """
    log    = []
    totals = {"postgres_content_deleted": 0}
    errors      = []
    now         = datetime.now(timezone.utc)
    today_start = now.replace(hour=0, minute=0, second=0, microsecond=0)

    try:
        with get_postgres_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                if dry_run:
                    cur.execute("""
                        SELECT COUNT(*) AS cnt FROM content
                        WHERE created_time >= %s AND used = FALSE
                    """, (today_start,))
                    r   = cur.fetchone()
                    cnt = r['cnt'] if r else 0
                    totals["postgres_content_deleted"] = cnt
                    log.append(
                        f"🔍 DRY RUN — would delete {cnt} unused content rows from PostgreSQL"
                    )
                else:
                    cur.execute("""
                        DELETE FROM content
                        WHERE created_time >= %s AND used = FALSE
                        RETURNING content_id
                    """, (today_start,))
                    rows = cur.fetchall()
                    totals["postgres_content_deleted"] = len(rows)
                    conn.commit()
                    log.append(
                        f"✅ PostgreSQL: deleted {len(rows)} unused content rows created today"
                    )
    except Exception as e:
        errors.append(f"PostgreSQL error: {e}")
        log.append(f"❌ PostgreSQL error: {e}")

    return {
        "stage":   "create_content",
        "dag":     "create_content",
        "dry_run": dry_run,
        "totals":  totals,
        "log":     log,
        "errors":  errors,
        "success": len(errors) == 0,
    }


# ============================================================================
# PIPELINE DEFINITION  (ordered: earliest → latest)
# ============================================================================

PIPELINE = [
    {
        "index":      0,
        "stage":      "create_content",
        "label":      "Create Content",
        "dag":        "create_content",
        "icon":       "📝",
        "color":      "#6366f1",
        "description": "Generates content via AI prompts and stores it in PostgreSQL.",
        "reverse_fn": reverse_create_content_dag,
        "reversible": True,
        "warning":    "Only unused content created today will be deleted.",
    },
    {
        "index":      1,
        "stage":      "create_workflows",
        "label":      "Create Workflows",
        "dag":        "create_workflows",
        "icon":       "⚙️",
        "color":      "#8b5cf6",
        "description": (
            "Builds Automa workflow JSON in execution_workflows + writes workflow_metadata "
            "+ marks content as used."
        ),
        "reverse_fn": reverse_create_workflows_dag,
        "reversible": True,
        "warning": (
            "Deletes workflow docs from execution_workflows and workflow_metadata records "
            "where has_link != True (not yet linked). Resets content rows (used→FALSE). "
            "Only affects today's non-executed workflows."
        ),
    },
    {
        "index":      2,
        "stage":      "extract_links",
        "label":      "Extract Links",
        "dag":        "extract_links",
        "icon":       "🔗",
        "color":      "#ec4899",
        "description": "Scrapes Twitter links and inserts them into PostgreSQL.",
        "reverse_fn": reverse_extract_links_dag,
        "reversible": True,
        "warning": (
            "Only unprocessed links (used=FALSE, executed=FALSE, filtered IS NULL) "
            "extracted today will be deleted. The filtered IS NULL guard prevents "
            "deleting links already processed by filter_links."
        ),
    },
    {
        "index":      3,
        "stage":      "filter_links",
        "label":      "Filter Links",
        "dag":        "filter_links",
        "icon":       "🔍",
        "color":      "#f59e0b",
        "description": (
            "Filters links (content + quantity), computes per-account chat links, "
            "assigns one workflow per link per account. "
            "Sets workflow_status/workflow_type on links. Does NOT set used=TRUE."
        ),
        "reverse_fn": reverse_filter_links_dag,
        "reversible": True,
        "warning": (
            "Resets filtered/within_limit/chat_link/workflow_status/workflow_type on links "
            "(matched by filtered_time >= today). "
            "Deletes workflow_metadata link-assignment docs and content_workflow_links docs."
        ),
    },
    {
        "index":      4,
        "stage":      "execute",
        "label":      "Execute (Report + ZIP)",
        "dag":        "filtering_report_with_workflows",
        "icon":       "🚀",
        "color":      "#10b981",
        "description": (
            "Builds one master workflow ZIP per account, emails all ZIPs, "
            "marks links as executed in PostgreSQL and MongoDB."
        ),
        "reverse_fn": reverse_execute_dag,
        "reversible": True,
        "warning": (
            "⚠️ Emails already sent CANNOT be unsent. "
            "Resets executed/success flags on links and workflow_metadata. "
            "Deletes today's filter_reports docs."
        ),
    },
]


# ============================================================================
# PAGE CLASS
# ============================================================================

class ReverseDagsPage:
    """Streamlit page for reversing pipeline DAG stages."""

    def __init__(self, db_manager=None):
        self.db_manager = db_manager

    def render(self):
        st.markdown("""
            <style>
            .rdp-header {
                background: linear-gradient(135deg, #1e1b4b 0%, #312e81 50%, #1e1b4b 100%);
                border-radius: 12px;
                padding: 28px 32px;
                margin-bottom: 24px;
                border: 1px solid rgba(99,102,241,0.3);
            }
            .rdp-header h1 { margin: 0; color: #e0e7ff; font-size: 1.8rem; }
            .rdp-header p  { margin: 6px 0 0 0; color: #a5b4fc; font-size: 0.95rem; }

            .log-box {
                background: #0d0d1a;
                border: 1px solid #2d2d44;
                border-radius: 8px;
                padding: 14px 16px;
                font-family: 'JetBrains Mono', monospace;
                font-size: 0.82rem;
                color: #c9d1d9;
                max-height: 350px;
                overflow-y: auto;
            }
            .log-ok   { color: #4ade80; }
            .log-warn { color: #fbbf24; }
            .log-err  { color: #f87171; }
            .log-info { color: #60a5fa; }
            </style>
        """, unsafe_allow_html=True)

        st.markdown("""
            <div class="rdp-header">
                <h1>↩️ Reverse DAG Pipeline</h1>
                <p>Undo pipeline stages cleanly — select how far back to roll. Stages are reversed in
                dependency order (latest first). Useful when a DAG misbehaves and you need a clean re-run.</p>
            </div>
        """, unsafe_allow_html=True)

        st.subheader("Pipeline Overview")
        self._render_pipeline_visual()
        st.markdown("---")

        col_left, col_right = st.columns([2, 1])

        with col_right:
            dry_run = st.toggle(
                "🔍 Dry Run (preview only)", value=True,
                help="See what would be changed without touching data"
            )
            if dry_run:
                st.info("Dry run is ON — no data will be modified.")
            else:
                st.warning("⚠️ Dry run is OFF — changes WILL be committed.")

        with col_left:
            st.markdown(
                "**Select the earliest stage to reverse** "
                "(all later stages will also be included):"
            )
            stage_labels    = [f"{s['icon']} {s['label']}" for s in PIPELINE]
            reversed_labels = list(reversed(stage_labels))

            selected_label = st.selectbox(
                "Reverse from:",
                options=reversed_labels,
                index=0,
                help=(
                    "Selecting 'Filter Links' will also reverse 'Execute' — "
                    "later stages always included."
                )
            )

        selected_index_in_reversed = reversed_labels.index(selected_label)
        selected_pipeline_index    = (len(PIPELINE) - 1) - selected_index_in_reversed
        stages_to_reverse          = [s for s in PIPELINE if s["index"] >= selected_pipeline_index]
        stages_to_reverse_ordered  = list(reversed(stages_to_reverse))

        st.markdown("**Stages that will be reversed:**")
        cols = st.columns(len(stages_to_reverse_ordered))
        for i, stage in enumerate(stages_to_reverse_ordered):
            with cols[i]:
                st.markdown(f"""
                    <div style="background:rgba(99,102,241,0.12);border:1px solid {stage['color']};
                                border-radius:8px;padding:10px 12px;text-align:center;">
                        <div style="font-size:1.4rem">{stage['icon']}</div>
                        <div style="color:#e0e7ff;font-size:0.8rem;font-weight:600;margin-top:4px">
                            {stage['label']}
                        </div>
                        <div style="color:#a5b4fc;font-size:0.7rem;margin-top:2px">
                            {stage['dag']}
                        </div>
                    </div>
                """, unsafe_allow_html=True)

        for stage in stages_to_reverse_ordered:
            if stage.get("warning"):
                st.warning(f"**{stage['icon']} {stage['label']}:** {stage['warning']}")

        st.markdown("---")

        action_label   = "🔍 Preview Reversal" if dry_run else "↩️ Reverse Now"
        confirm_needed = not dry_run

        if confirm_needed:
            confirm = st.checkbox(
                f"✅ I understand this will modify data for "
                f"{len(stages_to_reverse_ordered)} stage(s). Proceed.",
                key="confirm_reverse"
            )
        else:
            confirm = True

        btn_col, _ = st.columns([1, 2])
        with btn_col:
            run_btn = st.button(
                action_label,
                type="primary",
                disabled=not confirm,
                use_container_width=True,
            )

        if run_btn:
            self._execute_reversals(stages_to_reverse_ordered, dry_run)

        if st.session_state.get("rdp_results"):
            self._render_results(st.session_state["rdp_results"])

    def _render_pipeline_visual(self):
        cols = st.columns(len(PIPELINE))
        for i, stage in enumerate(PIPELINE):
            with cols[i]:
                st.markdown(f"""
                    <div style="background:#1e1e2e;border:1px solid {stage['color']}44;
                                border-radius:8px;padding:12px 10px;text-align:center;
                                border-left:3px solid {stage['color']};">
                        <div style="font-size:1.5rem">{stage['icon']}</div>
                        <div style="color:#e0e7ff;font-size:0.78rem;font-weight:600;margin-top:4px">
                            {stage['label']}
                        </div>
                        <div style="color:#64748b;font-size:0.68rem;margin-top:3px">
                            DAG {i+1} of {len(PIPELINE)}
                        </div>
                    </div>
                """, unsafe_allow_html=True)

    def _execute_reversals(self, stages: list, dry_run: bool):
        all_results = []
        progress    = st.progress(0)
        status_text = st.empty()

        for idx, stage in enumerate(stages):
            status_text.text(f"Reversing {stage['label']}...")
            try:
                result = stage["reverse_fn"](dry_run=dry_run)
                all_results.append(result)
            except Exception as e:
                all_results.append({
                    "stage":   stage["stage"],
                    "dag":     stage["dag"],
                    "dry_run": dry_run,
                    "totals":  {},
                    "log":     [f"❌ Unexpected error: {e}"],
                    "errors":  [str(e)],
                    "success": False,
                })
            progress.progress((idx + 1) / len(stages))

        status_text.text("Done!")
        progress.empty()
        status_text.empty()

        st.session_state["rdp_results"] = all_results
        label = "Dry run complete" if dry_run else "Reversal complete"
        st.success(f"✅ {label} — {len(all_results)} stage(s) processed.")
        st.rerun()

    def _render_results(self, results: list):
        st.markdown("---")
        st.subheader("📋 Reversal Audit Trail")

        dry = any(r.get("dry_run") for r in results)
        if dry:
            st.info("These are **dry run** results — no data was changed.")
        else:
            st.success("These are **live** results — data has been modified.")

        for result in results:
            stage_info  = next((s for s in PIPELINE if s["stage"] == result["stage"]), {})
            icon        = stage_info.get("icon", "🔄")
            status_icon = "✅" if result["success"] else "❌"

            with st.expander(
                f"{status_icon} {icon} {stage_info.get('label', result['stage'])} "
                f"— {result['dag']}",
                expanded=True
            ):
                if result.get("totals"):
                    cols = st.columns(len(result["totals"]))
                    for i, (k, v) in enumerate(result["totals"].items()):
                        with cols[i]:
                            st.metric(k.replace("_", " ").title(), v)

                st.markdown("**Action log:**")
                log_html = "<div class='log-box'>"
                for line in result.get("log", []):
                    if line.startswith("✅"):
                        cls = "log-ok"
                    elif line.startswith("❌"):
                        cls = "log-err"
                    elif line.startswith("⚠️") or line.startswith("📧") or line.startswith("ℹ️"):
                        cls = "log-warn"
                    else:
                        cls = "log-info"
                    log_html += f"<div class='{cls}'>{line}</div>"
                log_html += "</div>"
                st.markdown(log_html, unsafe_allow_html=True)

                if result.get("errors"):
                    st.error("Errors: " + " | ".join(result["errors"]))

        if st.button("🗑️ Clear Results", key="clear_rdp_results"):
            st.session_state.pop("rdp_results", None)
            st.rerun()

