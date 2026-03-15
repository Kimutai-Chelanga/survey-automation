"""
AutomaLogsViewer Component — Fixed for actual automa_logs collection structure.

FIX HISTORY (2026-02-17):
  1. Collection: was "automa_execution_logs" → now "automa_logs"
     (dynamic-orchestrator.js → mongoDBService.storeAutomaLogs() writes there)

  2. Field mapping: automa_logs docs have a flat structure:
       { execution_id, account, postgres_account_id, workflow_status, logs,
         created_at }
     The old viewer expected enriched fields (workflow_name, log_count,
     has_errors, history, log_metadata, …) that are never written.
     Adapter _normalise_log() maps actual → expected before rendering.

  3. logs field is a JSON string (AutomaExecutor.exportLogsAsJSON() returns
     a string). _parse_logs() safely parses it; elements that are still
     strings after parsing are skipped in _render_logs_detailed().

  4. history/logs elements: guarded with isinstance(step, dict) check so
     stray string elements never cause AttributeError.
"""

import json
import streamlit as st
from typing import Dict, Any, List, Optional
from datetime import datetime
from bson import ObjectId
from src.core.database.mongodb.connection import get_mongo_collection


# ── helpers ───────────────────────────────────────────────────────────────────

def _safe_json(value):
    """Return parsed JSON if value is a string, else return value as-is."""
    if isinstance(value, str):
        try:
            return json.loads(value)
        except Exception:
            return value
    return value


def _ts_to_str(ts):
    """Convert a millisecond timestamp to HH:MM:SS string."""
    try:
        return datetime.fromtimestamp(int(ts) / 1000).strftime('%H:%M:%S')
    except Exception:
        return str(ts)


# ── main class ────────────────────────────────────────────────────────────────

class AutomaLogsViewer:
    """Component for viewing and analysing Automa execution logs."""

    # ── The collection dynamic-orchestrator.js actually writes to ─────────────
    COLLECTION = "automa_logs"

    def __init__(self):
        self.logs_collection     = get_mongo_collection(self.COLLECTION)
        self.metadata_collection = get_mongo_collection("workflow_metadata")

    # ── Normalise raw MongoDB doc to viewer-expected shape ────────────────────
    def _normalise_log(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """
        Map the flat automa_logs document to the richer shape the viewer
        was written for.  All original fields are preserved.
        """
        # Parse the 'logs' field (may be a JSON string or already a dict/list)
        raw_logs = _safe_json(doc.get("logs", []))

        # If it's a dict with a 'logs' key (Automa export format)
        history       = []
        log_metadata  = {}
        exported_json = None

        if isinstance(raw_logs, dict):
            exported_json = raw_logs
            history       = raw_logs.get("logs", [])
            log_metadata  = raw_logs.get("metadata", {})
        elif isinstance(raw_logs, list):
            history = raw_logs

        # Count errors: step is an error if $isError is truthy OR type=="error"
        def _is_error_step(step):
            if not isinstance(step, dict):
                return False
            return bool(step.get("$isError")) or step.get("type") == "error"

        error_count   = sum(1 for s in history if _is_error_step(s))
        success_count = len([s for s in history if isinstance(s, dict)]) - error_count
        has_errors    = (doc.get("workflow_status", "").lower() == "error") or (error_count > 0)

        # Derive workflow name from execution_id or account
        workflow_name = (
            doc.get("workflow_name")
            or doc.get("account")
            or doc.get("execution_id", "Unknown")
        )

        return {
            # ── original fields (pass-through) ───────────────────────────────
            **doc,
            # ── derived / mapped fields ───────────────────────────────────────
            "workflow_name":   workflow_name,
            "workflow_type":   doc.get("workflow_type", doc.get("workflow_status", "unknown")),
            "workflow_version": doc.get("workflow_version", "—"),
            "log_source":      doc.get("log_source", "automa_logs"),
            "session_id":      doc.get("session_id", doc.get("execution_id", "N/A")),
            "log_count":       len([s for s in history if isinstance(s, dict)]),
            "has_errors":      has_errors,
            "error_count":     error_count,
            "success_count":   success_count,
            "history":         history,          # parsed list of step dicts
            "log_metadata":    log_metadata,     # metadata block if present
            "exported_json":   exported_json,    # full Automa export if present
            "log_id":          doc.get("log_id", ""),
        }

    # ── Public: render list ───────────────────────────────────────────────────
    def render_logs_list(self, filters: Optional[Dict[str, Any]] = None, limit: int = 20):
        """Render a list of execution logs with filters."""
        if self.logs_collection is None:
            st.error(f"❌ Cannot connect to `{self.COLLECTION}` collection")
            return

        try:
            query = {}
            if filters:
                if filters.get("workflow_type"):
                    query["workflow_type"] = filters["workflow_type"]
                if "has_errors" in filters:
                    # Map has_errors → workflow_status in the real collection
                    if filters["has_errors"] is True:
                        query["workflow_status"] = "error"
                    elif filters["has_errors"] is False:
                        query["workflow_status"] = {"$ne": "error"}

            raw_logs = list(
                self.logs_collection.find(query).sort("created_at", -1).limit(limit)
            )

            if not raw_logs:
                st.info(f"ℹ️ No logs found in `{self.COLLECTION}` matching the filters")
                return

            logs = [self._normalise_log(doc) for doc in raw_logs]

            # ── Summary metrics ───────────────────────────────────────────────
            total_steps   = sum(log.get("log_count", 0) for log in logs)
            failed_logs   = sum(1 for log in logs if log.get("has_errors"))
            success_logs  = len(logs) - failed_logs

            st.success(f"📊 Found {len(logs)} execution logs in `{self.COLLECTION}`")

            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Total Logs",  len(logs))
            c2.metric("✅ Success",   success_logs)
            c3.metric("❌ Failed",    failed_logs)
            c4.metric("Total Steps", total_steps)

            st.markdown("---")

            for log in logs:
                self._render_log_summary(log)

        except Exception as e:
            st.error(f"❌ Error loading logs: {e}")
            import traceback
            st.code(traceback.format_exc())

    # ── Render one log card ───────────────────────────────────────────────────
    def _render_log_summary(self, log: Dict[str, Any]):
        status_emoji  = "❌" if log.get("has_errors") else "✅"
        workflow_name = log.get("workflow_name", "Unknown Workflow")
        execution_id  = log.get("execution_id", "Unknown")
        created_at    = log.get("created_at", "Unknown time")
        log_count     = log.get("log_count", 0)

        exec_id_short = execution_id[:40] + "…" if len(execution_id) > 40 else execution_id
        title = f"{status_emoji} {workflow_name} — {exec_id_short} ({log_count} steps)"

        with st.expander(title, expanded=False):
            # ── Header columns ────────────────────────────────────────────────
            col1, col2, col3 = st.columns(3)

            with col1:
                st.write("**Workflow Information:**")
                st.write(f"• **Account:** {log.get('account', '—')}")
                st.write(f"• **PG Account ID:** {log.get('postgres_account_id', '—')}")
                st.write(f"• **Workflow status:** {log.get('workflow_status', '—')}")

            with col2:
                st.write("**Execution Details:**")
                session_id    = log.get("session_id", "N/A")
                session_short = session_id[:25] + "…" if len(session_id) > 25 else session_id
                st.write(f"• **Session:** {session_short}")

                if isinstance(created_at, datetime):
                    st.write(f"• **Created:** {created_at.strftime('%Y-%m-%d %H:%M:%S')}")
                else:
                    st.write(f"• **Created:** {created_at}")

                st.write(f"• **Steps parsed:** {log_count}")

            with col3:
                st.write("**Status:**")
                st.write(f"• **Result:** {'❌ Failed' if log.get('has_errors') else '✅ Success'}")
                st.write(f"• **Errors:** {log.get('error_count', 0)}")
                st.write(f"• **Successes:** {log.get('success_count', 0)}")

                meta = log.get("log_metadata", {})
                if isinstance(meta, dict) and meta.get("status"):
                    st.write(f"• **Automa status:** {meta['status']}")

            # ── Timing ────────────────────────────────────────────────────────
            meta = log.get("log_metadata", {})
            if isinstance(meta, dict) and meta.get("startedAt") and meta.get("endedAt"):
                st.markdown("---")
                st.markdown("**⏱️ Timing:**")
                ca, cb, cc = st.columns(3)
                ca.write(f"Started : {_ts_to_str(meta['startedAt'])}")
                cb.write(f"Ended   : {_ts_to_str(meta['endedAt'])}")
                cc.write(f"Duration: {(meta['endedAt'] - meta['startedAt']) / 1000:.2f}s")

            # ── Execution steps ───────────────────────────────────────────────
            st.markdown("---")
            st.markdown("**📝 Execution Steps:**")
            history = log.get("history", [])
            if history:
                self._render_logs_detailed(history)
            else:
                st.info("No detailed step logs available (history is empty)")

            # ── Automa export block ───────────────────────────────────────────
            exported = log.get("exported_json")
            if isinstance(exported, dict):
                with st.expander("📊 View Exported Logs (Automa Format)"):
                    st.write("**Metadata:**")
                    st.json(exported.get("metadata", {}))

                    if exported.get("logs"):
                        st.write(f"**Formatted Logs ({len(exported['logs'])} entries):**")
                        for entry in exported["logs"][:10]:
                            if isinstance(entry, dict):
                                st.json(entry)
                            else:
                                st.code(str(entry))
                        if len(exported["logs"]) > 10:
                            st.caption(f"… and {len(exported['logs']) - 10} more entries")

                    if exported.get("tableData"):
                        st.write(f"**Table Data ({len(exported['tableData'])} rows):**")
                        st.json(exported["tableData"][:5])

                    if exported.get("variables"):
                        st.write(f"**Variables ({len(exported['variables'])} vars):**")
                        st.json(exported["variables"])

            # ── Raw doc ───────────────────────────────────────────────────────
            with st.expander("🔍 View Complete Raw Log Document"):
                # Mask the heavy logs field to keep it readable
                display = {k: v for k, v in log.items()
                           if k not in ("history", "exported_json", "logs")}
                st.json(display)
                st.caption("(history / logs fields omitted from raw view — see steps above)")

            # ── Action buttons ────────────────────────────────────────────────
            c1, c2, c3 = st.columns(3)
            with c1:
                if st.button("📋 Copy Execution ID", key=f"copy_exec_{log.get('_id')}"):
                    st.code(log.get("execution_id", "N/A"), language=None)
            with c2:
                if st.button("📥 Export Log", key=f"export_log_{log.get('_id')}"):
                    self._export_log(log)
            with c3:
                wf_status = log.get("workflow_status", "")
                if wf_status == "error":
                    st.error("⚠️ Workflow ended with status=error")

    # ── Render step list ──────────────────────────────────────────────────────
    def _render_logs_detailed(self, logs: List[Any]):
        """Render detailed log steps — safely handles string elements."""
        if not logs:
            st.info("No log steps available")
            return

        # Filter to dict steps only; count strings as a warning
        dict_steps  = [s for s in logs if isinstance(s, dict)]
        str_steps   = [s for s in logs if isinstance(s, str)]
        other_steps = [s for s in logs if not isinstance(s, (dict, str))]

        if str_steps:
            st.caption(
                f"⚠️ {len(str_steps)} log element(s) are raw strings "
                f"(skipped in step view — visible in raw document)"
            )

        total_steps = len(dict_steps)
        error_steps = sum(
            1 for s in dict_steps
            if s.get("$isError") or s.get("type") == "error"
        )
        st.info(
            f"📊 {total_steps} steps | "
            f"❌ {error_steps} errors | "
            f"✅ {total_steps - error_steps} successful"
        )

        for idx, step in enumerate(dict_steps, 1):
            is_error  = bool(step.get("$isError")) or step.get("type") == "error"
            step_emoji = "❌" if is_error else "✅"
            step_name  = step.get("name", step.get("label", "Unknown Step"))
            step_desc  = step.get("description", step.get("message", ""))
            step_type  = step.get("type", step.get("blockType", "unknown"))

            with st.container():
                col1, col2 = st.columns([1, 11])
                with col1:
                    st.write(f"**{idx}.**")
                    st.write(step_emoji)
                with col2:
                    st.write(f"**{step_name}**")
                    if step_desc and step_desc != step_name:
                        st.caption(f"{step_desc} (type: {step_type})")
                    else:
                        st.caption(f"type: {step_type}")

                    if is_error:
                        err_msg = (
                            step.get("message")
                            or step.get("error")
                            or step.get("data", {}).get("message")
                            or "Unknown error"
                        )
                        st.error(f"⚠️ Error: {err_msg}")

                    details = []
                    if step.get("logId") or step.get("id"):
                        details.append(f"Log ID: {step.get('logId') or step.get('id')}")
                    if step.get("activeTabUrl"):
                        details.append(f"🔗 {step['activeTabUrl']}")
                    if step.get("blockId"):
                        details.append(f"Block: {step['blockId']}")
                    if step.get("timestamp"):
                        details.append(f"Time: {_ts_to_str(step['timestamp'])}")
                    if step.get("duration"):
                        details.append(f"Duration: {step['duration']}ms")
                    if details:
                        st.caption(" | ".join(details))

                    if step.get("data"):
                        with st.expander(f"Step {idx} data", expanded=False):
                            st.json(step["data"])

            if idx < len(dict_steps):
                st.markdown("---")

    # ── Export ────────────────────────────────────────────────────────────────
    def _export_log(self, log: Dict[str, Any]):
        try:
            log_copy = {k: str(v) if isinstance(v, ObjectId) else v
                        for k, v in log.items()
                        if k not in ("history", "exported_json")}
            json_data = json.dumps(log_copy, indent=2, default=str)
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            name = log.get("account", "unknown").replace(" ", "_")
            st.download_button(
                label="📥 Download Log JSON",
                data=json_data,
                file_name=f"automa_log_{name}_{ts}.json",
                mime="application/json",
                key=f"dl_log_{log.get('_id')}_{ts}",
            )
            st.success("✅ Log ready for download!")
        except Exception as e:
            st.error(f"Error exporting log: {e}")

    # ── For a specific workflow ───────────────────────────────────────────────
    def render_logs_for_workflow(self, metadata_id: str):
        if self.logs_collection is None or self.metadata_collection is None:
            st.error("❌ Cannot connect to required collections")
            return
        try:
            metadata = self.metadata_collection.find_one({"_id": ObjectId(metadata_id)})
            if not metadata:
                st.warning(f"No metadata found for ID: {metadata_id}")
                return
            execution_id = metadata.get("execution_id")
            if not execution_id:
                st.info("No execution ID in metadata — workflow may not have run yet")
                return
            raw = self.logs_collection.find_one({"execution_id": execution_id})
            if raw:
                log = self._normalise_log(raw)
                st.success(f"✅ Found execution log with {log.get('log_count', 0)} steps")
                self._render_log_summary(log)
            else:
                st.info("ℹ️ No execution logs found for this workflow.")
                st.caption(f"Execution ID: {execution_id}")
        except Exception as e:
            st.error(f"❌ Error loading logs: {e}")
            import traceback
            st.code(traceback.format_exc())


# ── Standalone tab renderer ───────────────────────────────────────────────────

def render_automa_logs_tab():
    """Standalone function to render the Automa Logs tab."""
    viewer = AutomaLogsViewer()

    st.subheader("📋 Automa Execution Logs")
    st.caption(
        f"Reads from `{AutomaLogsViewer.COLLECTION}` — "
        "populated by dynamic-orchestrator.js after each run"
    )

    col1, col2, col3 = st.columns(3)
    with col1:
        status = st.selectbox(
            "Status:",
            ["All", "Success Only", "Failed Only"],
            key="automa_logs_status_filter",
        )
    with col2:
        limit = st.number_input(
            "Show Last N:",
            min_value=5, max_value=100, value=20,
            key="automa_logs_limit",
        )
    with col3:
        st.write("")  # spacer

    filters = {}
    if status == "Failed Only":
        filters["has_errors"] = True
    elif status == "Success Only":
        filters["has_errors"] = False

    st.markdown("---")
    viewer.render_logs_list(filters=filters, limit=limit)
