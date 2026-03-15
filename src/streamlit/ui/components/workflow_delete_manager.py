"""
FILE: ui/components/workflow_delete_manager.py

Central workflow delete manager with dry run preview, hard delete, and soft delete.
Integrated into stats_dashboard.py and template_workflows_manager.py.
"""

import streamlit as st
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
from bson import ObjectId
from pymongo import MongoClient
import os

logger = logging.getLogger(__name__)


# ============================================================================
# MONGODB HELPERS
# ============================================================================

def _get_mongo_client() -> MongoClient:
    return MongoClient(
        os.getenv("MONGODB_URI", "mongodb://admin:admin123@mongodb:27017/messages_db?authSource=admin"),
        serverSelectionTimeoutMS=5000,
    )


def _get_metadata_collection():
    """Return (collection, client) for workflow_metadata."""
    client = _get_mongo_client()
    return client["messages_db"]["workflow_metadata"], client


# ============================================================================
# DRY RUN — show exactly what WILL be deleted without touching anything
# ============================================================================

def dry_run_delete_workflow(
    workflow_id: str,
    storage_database: str,
    storage_collection: str,
) -> Dict[str, Any]:
    """
    Preview everything that would be deleted for one workflow.
    Returns a structured report — nothing is modified.
    """
    report = {
        "workflow_id": workflow_id,
        "storage_database": storage_database,
        "storage_collection": storage_collection,
        "found": {
            "workflow_document": False,
            "metadata_records": 0,
            "automa_execution_logs": 0,
        },
        "would_delete": [],
        "would_not_affect": [],
        "warnings": [],
        "error": None,
    }

    try:
        client = _get_mongo_client()

        # ── 1. Workflow document in its storage collection ──────────────────
        try:
            target_col = client[storage_database][storage_collection]
            doc = target_col.find_one({"_id": ObjectId(workflow_id)})
            if doc:
                report["found"]["workflow_document"] = True
                report["would_delete"].append(
                    f"Workflow document `{workflow_id[:12]}…` "
                    f"from `{storage_database}.{storage_collection}`"
                )
            else:
                report["warnings"].append(
                    f"Workflow document NOT found in `{storage_database}.{storage_collection}` "
                    f"(may have been deleted already or wrong collection)."
                )
        except Exception as e:
            report["warnings"].append(f"Could not check workflow document: {e}")

        # ── 2. workflow_metadata records ────────────────────────────────────
        try:
            meta_col = client["messages_db"]["workflow_metadata"]
            meta_count = meta_col.count_documents(
                {"automa_workflow_id": ObjectId(workflow_id)}
            )
            report["found"]["metadata_records"] = meta_count
            if meta_count:
                report["would_delete"].append(
                    f"{meta_count} record(s) in `messages_db.workflow_metadata` "
                    f"linked to workflow `{workflow_id[:12]}…`"
                )
        except Exception as e:
            report["warnings"].append(f"Could not check workflow_metadata: {e}")

        # ── 3. automa_execution_logs ────────────────────────────────────────
        try:
            logs_col = client["messages_db"]["automa_execution_logs"]
            log_count = logs_col.count_documents({"workflow_id": workflow_id})
            report["found"]["automa_execution_logs"] = log_count
            if log_count:
                report["would_delete"].append(
                    f"{log_count} execution log(s) in `messages_db.automa_execution_logs`"
                )
        except Exception as e:
            report["warnings"].append(f"Could not check automa_execution_logs: {e}")

        # ── 4. What is NOT affected ─────────────────────────────────────────
        report["would_not_affect"] = [
            "PostgreSQL tables (content, accounts, links) — untouched",
            "Other workflows in the same collection",
            "workflow_templates collection",
            "Artifact files / screenshots (external storage)",
        ]

        client.close()

    except Exception as e:
        report["error"] = str(e)
        logger.error(f"dry_run_delete_workflow error: {e}")

    return report


def dry_run_delete_bulk(workflows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Dry run for a list of workflows (bulk preview).
    Each item must have _id, storage_database, storage_collection.
    """
    summary = {
        "total_workflows": len(workflows),
        "workflow_documents_found": 0,
        "total_metadata_records": 0,
        "total_execution_logs": 0,
        "warnings": [],
        "per_workflow": [],
    }

    for wf in workflows:
        r = dry_run_delete_workflow(
            workflow_id=str(wf.get("_id", "")),
            storage_database=wf.get("storage_database", "unknown"),
            storage_collection=wf.get("storage_collection", "unknown"),
        )
        summary["per_workflow"].append(r)
        if r["found"]["workflow_document"]:
            summary["workflow_documents_found"] += 1
        summary["total_metadata_records"] += r["found"]["metadata_records"]
        summary["total_execution_logs"] += r["found"]["automa_execution_logs"]
        summary["warnings"].extend(r["warnings"])

    return summary


# ============================================================================
# ACTUAL DELETE — single workflow
# ============================================================================

def delete_workflow(
    workflow_id: str,
    storage_database: str,
    storage_collection: str,
    mode: str = "hard",          # "hard" | "soft"
    deleted_by: str = "user",
) -> Dict[str, Any]:
    """
    Delete (or soft-delete) a single workflow and its metadata.

    mode="hard"  → removes documents from MongoDB entirely.
    mode="soft"  → adds `deleted=True, deleted_at=...` to metadata only;
                   leaves the workflow document untouched (safe fallback).
    """
    result = {
        "workflow_id": workflow_id,
        "mode": mode,
        "success": False,
        "deleted": {
            "workflow_document": False,
            "metadata_records": 0,
            "execution_logs": 0,
        },
        "error": None,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    try:
        client = _get_mongo_client()

        if mode == "hard":
            # ── workflow document ──────────────────────────────────────────
            try:
                target_col = client[storage_database][storage_collection]
                del_doc = target_col.delete_one({"_id": ObjectId(workflow_id)})
                result["deleted"]["workflow_document"] = del_doc.deleted_count > 0
            except Exception as e:
                logger.warning(f"Could not delete workflow document: {e}")

            # ── metadata ──────────────────────────────────────────────────
            try:
                meta_col = client["messages_db"]["workflow_metadata"]
                del_meta = meta_col.delete_many(
                    {"automa_workflow_id": ObjectId(workflow_id)}
                )
                result["deleted"]["metadata_records"] = del_meta.deleted_count
            except Exception as e:
                logger.warning(f"Could not delete metadata: {e}")

            # ── execution logs ─────────────────────────────────────────────
            try:
                logs_col = client["messages_db"]["automa_execution_logs"]
                del_logs = logs_col.delete_many({"workflow_id": workflow_id})
                result["deleted"]["execution_logs"] = del_logs.deleted_count
            except Exception as e:
                logger.warning(f"Could not delete execution logs: {e}")

        else:  # soft delete
            try:
                meta_col = client["messages_db"]["workflow_metadata"]
                upd = meta_col.update_many(
                    {"automa_workflow_id": ObjectId(workflow_id)},
                    {
                        "$set": {
                            "deleted": True,
                            "deleted_at": datetime.now(timezone.utc).isoformat(),
                            "deleted_by": deleted_by,
                        }
                    },
                )
                result["deleted"]["metadata_records"] = upd.modified_count
                result["deleted"]["workflow_document"] = False  # not touched
            except Exception as e:
                logger.warning(f"Soft delete metadata failed: {e}")

        client.close()
        result["success"] = True

    except Exception as e:
        result["error"] = str(e)
        logger.error(f"delete_workflow error: {e}")

    return result


# ============================================================================
# STREAMLIT UI — renders the full delete panel for ONE workflow
# ============================================================================

def render_delete_panel(
    workflow: Dict[str, Any],
    category: str,
    workflow_type: str,
    on_delete_callback=None,         # optional callable() after successful delete
    panel_key_suffix: str = "",
):
    """
    Renders a self-contained delete panel inside whatever container the caller provides.

    Flow:
      1. User clicks "🗑️ Delete Workflow"
      2. Dry-run report appears showing exactly what will be removed
      3. User picks Hard vs Soft delete
      4. User confirms → deletion executes → success/error shown
    """
    wf_id = str(workflow.get("_id", ""))
    db = workflow.get("storage_database", "unknown")
    col = workflow.get("storage_collection", "unknown")
    wf_name = workflow.get("name", f"Workflow {wf_id[:8]}")

    panel_key = f"del_{wf_id}_{panel_key_suffix}"

    # ── Step 0: Initial delete button ──────────────────────────────────────
    if not st.session_state.get(f"{panel_key}_open", False):
        if st.button(
            "🗑️ Delete",
            key=f"{panel_key}_trigger",
            help="Preview and delete this workflow",
            use_container_width=True,
        ):
            st.session_state[f"{panel_key}_open"] = True
            st.session_state[f"{panel_key}_dry_run_done"] = False
            st.session_state[f"{panel_key}_confirmed"] = False
            st.rerun()
        return

    # ── Delete panel is open ────────────────────────────────────────────────
    st.markdown(
        f"""
        <div style="border:1px solid #e74c3c; border-radius:8px; padding:12px;
                    background:#fff5f5; margin:8px 0;">
        <b style="color:#e74c3c;">🗑️ Delete: {wf_name}</b>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # ── Step 1: Dry run ─────────────────────────────────────────────────────
    if not st.session_state.get(f"{panel_key}_dry_run_done", False):
        col1, col2 = st.columns([2, 1])
        with col1:
            st.info("Run a **dry run** first to see exactly what will be deleted — nothing is changed until you confirm.")
        with col2:
            if st.button("🔍 Run Dry Run", key=f"{panel_key}_dryrun", use_container_width=True):
                with st.spinner("Scanning…"):
                    report = dry_run_delete_workflow(wf_id, db, col)
                st.session_state[f"{panel_key}_dry_report"] = report
                st.session_state[f"{panel_key}_dry_run_done"] = True
                st.rerun()

        if st.button("✖ Cancel", key=f"{panel_key}_cancel0", use_container_width=False):
            st.session_state[f"{panel_key}_open"] = False
            st.rerun()
        return

    # ── Step 2: Show dry-run report ─────────────────────────────────────────
    report = st.session_state.get(f"{panel_key}_dry_report", {})

    st.markdown("#### 🔍 Dry Run Report")

    if report.get("error"):
        st.error(f"Dry run encountered an error: {report['error']}")
    else:
        # What WILL be deleted
        would_delete = report.get("would_delete", [])
        if would_delete:
            st.markdown("**Will be deleted:**")
            for item in would_delete:
                st.markdown(f"- 🔴 {item}")
        else:
            st.warning("Nothing found to delete — workflow may already be gone.")

        # What won't be affected
        st.markdown("**Will NOT be affected:**")
        for item in report.get("would_not_affect", []):
            st.markdown(f"- 🟢 {item}")

        # Warnings
        for w in report.get("warnings", []):
            st.warning(f"⚠️ {w}")

    # ── Step 3: Confirm + choose mode ───────────────────────────────────────
    if not st.session_state.get(f"{panel_key}_confirmed", False):
        st.markdown("---")
        st.markdown("**Choose delete mode:**")

        delete_mode = st.radio(
            "",
            options=["Hard Delete (removes documents)", "Soft Delete (marks as deleted, keeps data)"],
            key=f"{panel_key}_mode",
            horizontal=True,
            label_visibility="collapsed",
        )

        confirm_text = st.text_input(
            f'Type **DELETE** to confirm removal of `{wf_name[:40]}`',
            key=f"{panel_key}_confirm_text",
            placeholder="Type DELETE here",
        )

        col1, col2 = st.columns(2)
        with col1:
            proceed = st.button(
                "🗑️ Confirm Delete",
                key=f"{panel_key}_confirm_btn",
                type="primary",
                use_container_width=True,
                disabled=(confirm_text.strip() != "DELETE"),
            )
        with col2:
            if st.button("✖ Cancel", key=f"{panel_key}_cancel1", use_container_width=True):
                st.session_state[f"{panel_key}_open"] = False
                st.rerun()

        if proceed and confirm_text.strip() == "DELETE":
            mode = "hard" if "Hard" in delete_mode else "soft"
            with st.spinner("Deleting…"):
                del_result = delete_workflow(wf_id, db, col, mode=mode)
            st.session_state[f"{panel_key}_del_result"] = del_result
            st.session_state[f"{panel_key}_confirmed"] = True
            st.rerun()
        return

    # ── Step 4: Show result ─────────────────────────────────────────────────
    del_result = st.session_state.get(f"{panel_key}_del_result", {})

    if del_result.get("success"):
        st.success("✅ Workflow deleted successfully!")
        d = del_result.get("deleted", {})
        st.markdown(
            f"- Workflow document removed: {'✅' if d.get('workflow_document') else '—'}\n"
            f"- Metadata records removed: **{d.get('metadata_records', 0)}**\n"
            f"- Execution logs removed: **{d.get('execution_logs', 0)}**"
        )
        if on_delete_callback:
            on_delete_callback()
        if st.button("Close", key=f"{panel_key}_close_success"):
            # Clean up session state keys
            for k in [
                f"{panel_key}_open", f"{panel_key}_dry_run_done",
                f"{panel_key}_confirmed", f"{panel_key}_dry_report",
                f"{panel_key}_del_result",
            ]:
                st.session_state.pop(k, None)
            st.rerun()
    else:
        st.error(f"❌ Delete failed: {del_result.get('error', 'Unknown error')}")
        if st.button("Close", key=f"{panel_key}_close_fail"):
            st.session_state[f"{panel_key}_open"] = False
            st.rerun()


# ============================================================================
# STREAMLIT UI — bulk delete panel (operates on a list of workflows)
# ============================================================================

def render_bulk_delete_panel(
    workflows: List[Dict[str, Any]],
    category: str,
    workflow_type: str,
    panel_key: str = "bulk_delete",
    on_delete_callback=None,
):
    """
    Bulk delete panel for wiping multiple workflows at once.
    Shows aggregate dry-run summary before confirming.
    """
    if not workflows:
        return

    st.markdown("---")
    st.markdown("#### 🗑️ Bulk Delete")

    if not st.session_state.get(f"{panel_key}_open", False):
        if st.button(
            f"🗑️ Delete All {len(workflows)} Workflows in View",
            key=f"{panel_key}_trigger",
            help="Delete all workflows currently shown (respects active filters)",
        ):
            st.session_state[f"{panel_key}_open"] = True
            st.session_state[f"{panel_key}_dry_done"] = False
            st.rerun()
        return

    st.warning(
        f"⚠️ You are about to delete **{len(workflows)} workflows** "
        f"in category `{category}` / type `{workflow_type}`."
    )

    # Dry run
    if not st.session_state.get(f"{panel_key}_dry_done", False):
        col1, col2 = st.columns([3, 1])
        with col1:
            st.info("Run a dry run to see the full impact across all selected workflows.")
        with col2:
            if st.button("🔍 Bulk Dry Run", key=f"{panel_key}_dryrun", use_container_width=True):
                with st.spinner(f"Scanning {len(workflows)} workflows…"):
                    summary = dry_run_delete_bulk(workflows)
                st.session_state[f"{panel_key}_summary"] = summary
                st.session_state[f"{panel_key}_dry_done"] = True
                st.rerun()
        if st.button("✖ Cancel", key=f"{panel_key}_cancel0"):
            st.session_state[f"{panel_key}_open"] = False
            st.rerun()
        return

    # Show summary
    summary = st.session_state.get(f"{panel_key}_summary", {})
    st.markdown("#### 🔍 Bulk Dry Run Summary")

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Workflows", summary.get("total_workflows", 0))
    c2.metric("Documents Found", summary.get("workflow_documents_found", 0))
    c3.metric("Metadata Records", summary.get("total_metadata_records", 0))
    c4.metric("Execution Logs", summary.get("total_execution_logs", 0))

    if summary.get("warnings"):
        with st.expander(f"⚠️ {len(summary['warnings'])} warning(s)"):
            for w in summary["warnings"]:
                st.caption(w)

    with st.expander("Per-workflow breakdown", expanded=False):
        for r in summary.get("per_workflow", []):
            wf_id_short = r["workflow_id"][:12] + "…"
            found = r["found"]
            icon = "✅" if found["workflow_document"] else "⚠️"
            st.markdown(
                f"{icon} `{wf_id_short}` — "
                f"doc: {'✅' if found['workflow_document'] else '❌'} | "
                f"meta: {found['metadata_records']} | "
                f"logs: {found['automa_execution_logs']}"
            )

    # Confirm
    st.markdown("---")
    delete_mode = st.radio(
        "Delete mode:",
        ["Hard Delete (removes documents)", "Soft Delete (marks deleted, keeps data)"],
        key=f"{panel_key}_mode",
        horizontal=True,
    )

    confirm_text = st.text_input(
        f"Type **DELETE ALL** to confirm bulk deletion of {len(workflows)} workflows",
        key=f"{panel_key}_confirm_text",
        placeholder="Type DELETE ALL",
    )

    col1, col2 = st.columns(2)
    with col1:
        proceed = st.button(
            f"🗑️ Confirm Bulk Delete ({len(workflows)})",
            key=f"{panel_key}_confirm",
            type="primary",
            use_container_width=True,
            disabled=(confirm_text.strip() != "DELETE ALL"),
        )
    with col2:
        if st.button("✖ Cancel", key=f"{panel_key}_cancel1", use_container_width=True):
            st.session_state[f"{panel_key}_open"] = False
            st.rerun()

    if proceed and confirm_text.strip() == "DELETE ALL":
        mode = "hard" if "Hard" in delete_mode else "soft"
        results = {"success": 0, "failed": 0, "errors": []}

        progress = st.progress(0, text="Deleting…")
        for i, wf in enumerate(workflows):
            r = delete_workflow(
                workflow_id=str(wf.get("_id", "")),
                storage_database=wf.get("storage_database", "unknown"),
                storage_collection=wf.get("storage_collection", "unknown"),
                mode=mode,
            )
            if r["success"]:
                results["success"] += 1
            else:
                results["failed"] += 1
                results["errors"].append(r.get("error", "unknown"))
            progress.progress((i + 1) / len(workflows), text=f"Deleting {i+1}/{len(workflows)}…")

        progress.empty()

        if results["failed"] == 0:
            st.success(f"✅ Successfully deleted {results['success']} workflows!")
        else:
            st.warning(
                f"Deleted {results['success']}, failed {results['failed']}. "
                f"Errors: {'; '.join(results['errors'][:3])}"
            )

        if on_delete_callback:
            on_delete_callback()

        # Clear state
        for k in [f"{panel_key}_open", f"{panel_key}_dry_done", f"{panel_key}_summary"]:
            st.session_state.pop(k, None)
        st.rerun()
