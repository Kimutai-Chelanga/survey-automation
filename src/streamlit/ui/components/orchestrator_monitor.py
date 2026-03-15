"""
FIXES APPLIED
─────────────
1. BLANK ORCHESTRATOR IN AUTOMA
   MongoDB wraps the workflow under orchestrator_data:
       { _id, execution_id, orchestrator_data: { extVersion, name, drawflow, … } }
   Old code downloaded the entire Mongo doc → Automa saw a blank workflow.
   Fix: _extract_clean_workflow() pulls orchestrator_data out and strips
   the internal _screenshotPrefixes field (not part of the Automa spec).

2. DUPLICATE STREAMLIT KEY ERROR
   _render_orchestrator_card() called from both tabs → same _id → key collision.
   Fix: context param ("recent"/"details") prefixed into every widget key.

3. FULL PACKAGE IS NOW A ZIP (matching Manual Executor exactly)
   Old: single nested JSON blob — not importable into Automa.
   New: ZIP file containing:
       orchestrator_{name}.json  ← orchestrator, ready to import
       {workflow_name}.json      ← one file per sub-workflow, ready to import
   Identical structure to what "Generate & Download ZIP Package" produces
   in the Manual Executor page.

4. TIMING METRICS (2026-02-19)
   _render_timing_panel() now shows estimated_wait_ms / configured_wait_ms /
   actual_wait_ms for every orchestrator card (pulled from generated_orchestrators
   top-level fields) and a per-workflow breakdown table when available.
   Statistics tab also shows aggregate timing across all orchestrators.

5. AUTOMA LOGS FIX (2026-02-19)
   logs_collection was pointing to "automa_execution_logs" — this collection
   does not exist. dynamic-orchestrator.js writes to "automa_logs".
   Fixed: _connect_collections() now uses "automa_logs".
"""

import io
import json
import zipfile
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import pandas as pd
import streamlit as st
from bson import ObjectId

from src.core.database.mongodb.connection import get_mongo_collection


# =============================================================================
# Shared timing helper (same logic as dashboard.py)
# =============================================================================
def _render_timing_panel(
    estimated_wait_ms: Optional[float],
    configured_wait_ms: Optional[float],
    actual_wait_ms: Optional[float],
    wait_margin_ms: Optional[float] = None,
    estimation_method: Optional[str] = None,
    workflow_exec_ms: Optional[float] = None,
    total_inter_delay_ms: Optional[float] = None,
    per_workflow_analysis: Optional[list] = None,
    show_breakdown: bool = True,
):
    def _fmt(ms) -> str:
        if ms is None:
            return "—"
        s = ms / 1000
        return f"{s / 60:.1f} min" if s >= 60 else f"{s:.0f}s"

    def _delta(actual, configured) -> Optional[str]:
        if actual is None or configured is None:
            return None
        diff_s = (actual - configured) / 1000
        sign   = "+" if diff_s > 0 else ""
        return f"{sign}{diff_s:.0f}s vs configured"

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        st.metric(
            "⏱ Estimated Wait", _fmt(estimated_wait_ms),
            help="Bottom-up block analysis: Σ(delay timeouts + click waits + page loads)"
        )
    with c2:
        st.metric(
            "🛡️ Wait Margin", _fmt(wait_margin_ms),
            help="Safety buffer configured in Streamlit"
        )
    with c3:
        st.metric(
            "⚙️ Configured Wait", _fmt(configured_wait_ms),
            help="Estimated + margin — the actual sleep() call duration"
        )
    with c4:
        delta = _delta(actual_wait_ms, configured_wait_ms)
        st.metric(
            "✅ Actual Wait", _fmt(actual_wait_ms),
            delta=delta,
            delta_color="inverse",
            help="Real wall-clock ms reported by sleep()"
        )

    caption_parts = []
    if estimation_method:
        caption_parts.append(f"Method: **{estimation_method}**")
    if workflow_exec_ms is not None and total_inter_delay_ms is not None:
        caption_parts.append(
            f"{_fmt(workflow_exec_ms)} workflow exec + {_fmt(total_inter_delay_ms)} inter-delays"
            f" = {_fmt(estimated_wait_ms)} estimated"
        )
    if wait_margin_ms is not None and configured_wait_ms is not None:
        caption_parts.append(f"+ {_fmt(wait_margin_ms)} margin = {_fmt(configured_wait_ms)} configured")
    if caption_parts:
        st.caption("  •  ".join(caption_parts))

    if show_breakdown and per_workflow_analysis:
        with st.expander("📋 Per-workflow timing breakdown", expanded=False):
            rows = []
            for wf in per_workflow_analysis:
                ms = wf.get('estimatedMs', 0)
                bd = wf.get('breakdown', {})
                rows.append({
                    'Workflow':     wf.get('name', wf.get('docId', '—')),
                    'Estimated':    f"{ms / 1000:.1f}s",
                    'Delay blocks': f"{bd.get('delay_ms', 0) / 1000:.1f}s",
                    'Click waits':  f"{bd.get('click_ms', 0) / 1000:.1f}s",
                    'Other':        f"{bd.get('other_ms', 0) / 1000:.1f}s",
                    'Nodes':        bd.get('node_count', '—'),
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)


def _extract_timing_fields(doc: Dict) -> Dict:
    estimated_ms   = doc.get('estimated_total_ms')
    inter_delay_ms = doc.get('total_inter_delay_ms')
    wf_exec_ms     = doc.get('workflow_exec_ms')
    method         = doc.get('estimation_method')
    margin_ms      = doc.get('wait_margin_ms')
    per_wf         = doc.get('per_workflow_analysis')

    configured_ms = None
    if estimated_ms is not None and margin_ms is not None:
        configured_ms = estimated_ms + margin_ms

    actual_ms = None

    if estimated_ms is None:
        try:
            raw_gd = doc.get('orchestrator_data', {}).get('globalData', '{}')
            gd     = json.loads(raw_gd) if isinstance(raw_gd, str) else raw_gd
            estimated_ms   = estimated_ms   or gd.get('estimated_total_ms')
            inter_delay_ms = inter_delay_ms or gd.get('total_inter_delay_ms')
            wf_exec_ms     = wf_exec_ms     or gd.get('workflow_exec_ms')
            method         = method         or gd.get('estimation_method')
            margin_ms      = margin_ms      or gd.get('wait_margin_ms')
            if estimated_ms is not None and margin_ms is not None:
                configured_ms = estimated_ms + margin_ms
        except Exception:
            pass

    return {
        'estimated_ms':   estimated_ms,
        'configured_ms':  configured_ms,
        'actual_ms':      actual_ms,
        'margin_ms':      margin_ms,
        'method':         method,
        'wf_exec_ms':     wf_exec_ms,
        'inter_delay_ms': inter_delay_ms,
        'per_wf':         per_wf,
    }


class OrchestratorMonitor:
    """Monitor orchestrator executions and their workflows."""

    def __init__(self):
        self.orchestrators_collection = None
        self.logs_collection          = None
        self.metadata_collection      = None
        self.sessions_collection      = None

    # ── Collection setup ──────────────────────────────────────────────────────

    def _connect_collections(self):
        if self.orchestrators_collection is None:
            self.orchestrators_collection = get_mongo_collection("generated_orchestrators")
        if self.logs_collection is None:
            # ✅ FIX: was "automa_execution_logs" — dynamic-orchestrator.js
            # calls mongoDBService.storeAutomaLogs() which writes to "automa_logs"
            self.logs_collection = get_mongo_collection("automa_logs")
        if self.metadata_collection is None:
            self.metadata_collection = get_mongo_collection("workflow_metadata")
        if self.sessions_collection is None:
            self.sessions_collection = get_mongo_collection("execution_sessions")

    # ── Entry point ───────────────────────────────────────────────────────────

    def render(self):
        st.markdown("---")
        st.subheader("🎼 Orchestrator Executions")
        st.caption("Monitor dynamically generated orchestrator workflows and their execution history")

        self._connect_collections()

        if not self._has_orchestrators():
            self._render_no_orchestrators_message()
            return

        self._render_orchestrator_interface()

    def _has_orchestrators(self) -> bool:
        try:
            return (self.orchestrators_collection is not None and
                    self.orchestrators_collection.count_documents({}) > 0)
        except Exception as e:
            st.error(f"Error checking orchestrators: {e}")
            return False

    def _render_no_orchestrators_message(self):
        st.info("""
        No orchestrator executions yet.

        Orchestrators are dynamically built workflows that execute multiple workflows sequentially.

        To create orchestrators:
        1. Configure execution settings in Settings > Execution Configuration
        2. Upload orchestrator template
        3. Run the local_executor_orchestrator DAG in Airflow
        """)

    def _render_orchestrator_interface(self):
        tab1, tab2, tab3 = st.tabs([
            "Recent Executions",
            "Orchestrator Details",
            "Statistics",
        ])
        with tab1: self._render_recent_executions()
        with tab2: self._render_orchestrator_details()
        with tab3: self._render_statistics()

    # ── Serialisation helpers ─────────────────────────────────────────────────

    @staticmethod
    def _convert_objectid(obj):
        if isinstance(obj, ObjectId):
            return str(obj)
        if isinstance(obj, dict):
            return {k: OrchestratorMonitor._convert_objectid(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [OrchestratorMonitor._convert_objectid(i) for i in obj]
        if isinstance(obj, datetime):
            return obj.isoformat()
        return obj

    def _extract_clean_workflow(self, mongo_doc: Dict[str, Any]) -> Dict[str, Any]:
        raw     = mongo_doc.get('orchestrator_data', mongo_doc)
        cleaned = self._convert_objectid(raw)
        cleaned.pop('_screenshotPrefixes', None)
        return cleaned

    # ── ZIP builder ───────────────────────────────────────────────────────────

    def _build_zip_package(self, orchestrator: Dict[str, Any]) -> bytes:
        clean_orch         = self._extract_clean_workflow(orchestrator)
        included_workflows = clean_orch.get('includedWorkflows', {})

        orch_name = (
            clean_orch.get('name', 'orchestrator')
            .replace('/', '_').replace('\\', '_').replace(' ', '_')
        )

        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            zf.writestr(
                f'orchestrator_{orch_name}.json',
                json.dumps(clean_orch, indent=2, ensure_ascii=False),
            )
            for wf_id, wf_data in included_workflows.items():
                wf_clean = self._convert_objectid(wf_data)
                wf_name  = (
                    wf_clean.get('name', wf_id)
                    .replace('/', '_').replace('\\', '_').replace(' ', '_')
                )
                zf.writestr(
                    f'{wf_name}.json',
                    json.dumps(wf_clean, indent=2, ensure_ascii=False),
                )

        buf.seek(0)
        return buf.read()

    # ── Recent executions tab ─────────────────────────────────────────────────

    def _render_recent_executions(self):
        st.subheader("Recent Orchestrator Executions")
        try:
            orchestrators = list(
                self.orchestrators_collection.find().sort("created_at", -1).limit(10)
            )
            if not orchestrators:
                st.info("No recent orchestrator executions found")
                return

            st.success(f"Found {len(orchestrators)} recent orchestrator executions")
            for orch in orchestrators:
                self._render_orchestrator_card(orch, context="recent")

        except Exception as e:
            st.error(f"Error loading recent executions: {e}")
            import traceback
            with st.expander("View Error Details"):
                st.code(traceback.format_exc())

    # ── Card renderer ─────────────────────────────────────────────────────────

    def _render_orchestrator_card(self, orchestrator: Dict[str, Any], context: str = "card"):
        execution_id   = orchestrator.get('execution_id', 'Unknown')
        workflow_count = orchestrator.get('workflow_count', 0)
        node_count     = orchestrator.get('node_count', 0)
        status         = orchestrator.get('status', 'unknown')
        orch_id        = str(orchestrator.get('_id', 'unknown'))
        base_key       = f"{context}_{orch_id}"

        status_emoji = {
            'pending': '⏳', 'running': '▶️', 'completed': '✅',
            'failed': '❌', 'timeout': '⚠️',
        }.get(status, '❓')

        with st.expander(
            f"{status_emoji} {execution_id[:30]}... — "
            f"{workflow_count} workflows, {node_count} nodes — "
            f"{_fmt_ts(orchestrator.get('created_at'))}",
            expanded=False,
        ):
            c1, c2, c3, c4 = st.columns(4)
            with c1: st.metric("Total Workflows", workflow_count)
            with c2: st.metric("Total Nodes",     node_count)
            with c3: st.metric("Status",          status.upper())
            with c4: st.metric("Execution ID",    execution_id[:12] + "...")

            # ── ⏱ Timing panel ──────────────────────────────────────────────
            t = _extract_timing_fields(orchestrator)
            if any(v is not None for v in [t['estimated_ms'], t['configured_ms']]):
                st.markdown("---")
                st.markdown("#### ⏱ Orchestrator Wait Timing")

                actual_ms = t['actual_ms']
                if actual_ms is None and self.sessions_collection is not None:
                    try:
                        sess = self.sessions_collection.find_one(
                            {'dag_run_id': execution_id},
                            {'actual_wait_ms': 1, 'configured_wait_ms': 1}
                        )
                        if sess:
                            actual_ms = sess.get('actual_wait_ms')
                            if t['configured_ms'] is None:
                                t['configured_ms'] = sess.get('configured_wait_ms')
                    except Exception:
                        pass

                _render_timing_panel(
                    estimated_wait_ms=t['estimated_ms'],
                    configured_wait_ms=t['configured_ms'],
                    actual_wait_ms=actual_ms,
                    wait_margin_ms=t['margin_ms'],
                    estimation_method=t['method'],
                    workflow_exec_ms=t['wf_exec_ms'],
                    total_inter_delay_ms=t['inter_delay_ms'],
                    per_workflow_analysis=t['per_wf'],
                    show_breakdown=True,
                )
            else:
                st.markdown("---")
                st.caption("⏱ No timing data recorded for this orchestrator (pre-v10 run)")

            st.markdown("---")
            self._render_download_buttons(orchestrator, base_key=base_key)
            self._render_orchestrator_execution_logs(execution_id, base_key=base_key)
            self._render_included_workflows(orchestrator, base_key=base_key)

            with st.expander("View Orchestrator Structure", expanded=False):
                orch_data = orchestrator.get('orchestrator_data', {})
                if orch_data:
                    st.write(f"**Name:** {orch_data.get('name', 'Unknown')}")
                    st.write(f"**Version:** {orch_data.get('version', 'Unknown')}")
                    st.write(f"**Description:** {orch_data.get('description', 'N/A')}")

                    drawflow = orch_data.get('drawflow', {})
                    if drawflow:
                        st.markdown("---")
                        nodes = drawflow.get('nodes', [])
                        edges = drawflow.get('edges', [])
                        nc1, nc2 = st.columns(2)
                        with nc1: st.metric("Nodes", len(nodes))
                        with nc2: st.metric("Edges", len(edges))

                        if nodes:
                            node_types: Dict[str, int] = {}
                            for n in nodes:
                                lbl = n.get('label', 'unknown')
                                node_types[lbl] = node_types.get(lbl, 0) + 1
                            st.markdown("**Node Types:**")
                            for lbl, cnt in sorted(node_types.items()):
                                st.write(f"  {lbl}: {cnt}")

                    with st.expander("View Raw JSON"):
                        st.json(orch_data)
                else:
                    st.warning("No orchestrator data available")

    # ── Download buttons ──────────────────────────────────────────────────────

    def _render_download_buttons(self, orchestrator: Dict[str, Any], base_key: str = ""):
        col1, col2, col3 = st.columns([1, 1, 2])
        execution_id = orchestrator.get('execution_id', 'unknown')
        now          = datetime.now()
        ts           = now.strftime('%Y%m%d_%H%M%S')
        wf_count     = orchestrator.get('workflow_count', 0)

        with col1:
            clean_workflow = self._extract_clean_workflow(orchestrator)
            st.download_button(
                label="📥 Download Orchestrator",
                data=json.dumps(clean_workflow, indent=2, ensure_ascii=False),
                file_name=f"orchestrator_{execution_id[:20]}_{ts}.json",
                mime="application/json",
                help="Orchestrator only — import directly into Automa",
                key=f"dl_orch_{base_key}",
            )

        with col2:
            zip_bytes = self._build_zip_package(orchestrator)
            day_str   = now.strftime('%Y-%m-%d')
            time_str  = now.strftime('%H-%M-%S')
            st.download_button(
                label="📦 Download Full Package",
                data=zip_bytes,
                file_name=f"workflows_{day_str}_{time_str}_{wf_count}wf.zip",
                mime="application/zip",
                help=f"ZIP with orchestrator + all {wf_count} sub-workflow files",
                key=f"dl_full_{base_key}",
            )

        with col3:
            st.caption(
                f"Full package = ZIP with **{wf_count + 1} files** "
                f"({wf_count} sub-workflows + orchestrator). "
                f"Every file is importable directly into Automa."
            )

    # ── Execution logs ────────────────────────────────────────────────────────

    def _render_orchestrator_execution_logs(self, execution_id: str, base_key: str = ""):
        st.markdown("---")
        st.subheader("📋 Automa Execution Logs")
        try:
            if self.logs_collection is None:
                st.warning("Logs collection not available")
                return

            # ✅ automa_logs documents have: execution_id, account,
            # postgres_account_id, workflow_status, logs, created_at
            logs = list(
                self.logs_collection
                .find({'execution_id': execution_id})
                .sort("created_at", -1)
            )

            if not logs:
                st.info(
                    f"No Automa logs found for execution `{execution_id[:30]}…`\n\n"
                    "Logs are written after the orchestrator wait completes. "
                    "If the run just finished, try refreshing."
                )
                return

            st.success(f"Found {len(logs)} Automa log document(s)")

            for log in logs:
                self._render_automa_log_card(log, base_key=base_key)

        except Exception as e:
            st.error(f"Error loading execution logs: {e}")
            import traceback
            with st.expander("View Error Details"):
                st.code(traceback.format_exc())

    def _render_automa_log_card(self, log: Dict[str, Any], base_key: str = ""):
        """Render a single automa_logs document."""
        log_id       = str(log.get('_id', ''))
        account      = log.get('account', '—')
        status       = log.get('workflow_status', 'unknown')
        created_at   = log.get('created_at')
        execution_id = log.get('execution_id', '—')

        status_emoji = "✅" if status == "success" else "❌"

        with st.expander(
            f"{status_emoji} Account: {account} — Status: {status} — {_fmt_ts(created_at)}",
            expanded=True,
        ):
            c1, c2, c3 = st.columns(3)
            with c1:
                st.write(f"**Account:** {account}")
                st.write(f"**PG Account ID:** {log.get('postgres_account_id', '—')}")
            with c2:
                st.write(f"**Status:** {status}")
                st.write(f"**Session ID:** {str(log.get('session_id', '—'))[:30]}")
            with c3:
                st.write(f"**Execution ID:** {execution_id[:30]}…")
                if isinstance(created_at, datetime):
                    st.write(f"**Time:** {created_at.strftime('%Y-%m-%d %H:%M:%S')}")

            # ── Parse and display the logs field ──────────────────────────
            st.markdown("---")
            raw_logs = log.get('logs')

            if raw_logs is None:
                st.info("No logs field in this document")
                return

            # logs may be a JSON string or already parsed
            parsed = raw_logs
            if isinstance(raw_logs, str):
                try:
                    parsed = json.loads(raw_logs)
                except Exception:
                    st.code(raw_logs[:500])
                    return

            # Automa export format: { logs: [...], metadata: {...}, ... }
            if isinstance(parsed, dict):
                metadata = parsed.get('metadata', {})
                steps    = parsed.get('logs', [])

                if metadata:
                    st.markdown("**⏱ Automa Timing:**")
                    mc1, mc2, mc3 = st.columns(3)
                    started = metadata.get('startedAt')
                    ended   = metadata.get('endedAt')
                    with mc1:
                        if started:
                            st.write(f"Started: {datetime.fromtimestamp(started/1000).strftime('%H:%M:%S')}")
                    with mc2:
                        if ended:
                            st.write(f"Ended: {datetime.fromtimestamp(ended/1000).strftime('%H:%M:%S')}")
                    with mc3:
                        if started and ended:
                            st.write(f"Duration: {(ended - started)/1000:.1f}s")

                    st.markdown("---")

                self._render_steps_table(steps, base_key=f"{base_key}_{log_id}")

            elif isinstance(parsed, list):
                self._render_steps_table(parsed, base_key=f"{base_key}_{log_id}")
            else:
                st.json(parsed)

            # Raw document
            with st.expander("🔍 Raw Log Document", expanded=False):
                display = {k: v for k, v in log.items() if k != 'logs'}
                st.json({**self._convert_objectid(display), 'logs': '(shown above)'})

    def _render_steps_table(self, steps: list, base_key: str = ""):
        """Render Automa execution steps as a clean table."""
        if not steps:
            st.info("No steps recorded")
            return

        dict_steps = [s for s in steps if isinstance(s, dict)]
        if not dict_steps:
            st.info(f"{len(steps)} steps present but none are structured dicts")
            return

        error_count   = sum(1 for s in dict_steps if s.get('$isError') or s.get('type') == 'error')
        success_count = len(dict_steps) - error_count

        st.info(f"📊 {len(dict_steps)} steps — ✅ {success_count} ok — ❌ {error_count} errors")

        rows = []
        for i, step in enumerate(dict_steps, 1):
            is_error = bool(step.get('$isError')) or step.get('type') == 'error'
            msg      = step.get('message', step.get('description', ''))
            rows.append({
                '#':       i,
                'Status':  '❌' if is_error else '✅',
                'Name':    step.get('name', step.get('label', 'Unknown')),
                'Type':    step.get('type', '—'),
                'Message': (msg[:80] + '…') if len(msg) > 80 else msg,
                'URL':     step.get('activeTabUrl', '—'),
            })

        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        # Show error details inline
        errors = [s for s in dict_steps if s.get('$isError') or s.get('type') == 'error']
        if errors:
            st.markdown("**❌ Error Details:**")
            for err in errors:
                err_msg = (
                    err.get('message') or
                    err.get('error') or
                    (err.get('data') or {}).get('message') or
                    'Unknown error'
                )
                st.error(f"**{err.get('name', 'Unknown step')}:** {err_msg}")

    # ── Included workflows ────────────────────────────────────────────────────

    def _render_included_workflows(self, orchestrator: Dict[str, Any], base_key: str = ""):
        st.markdown("---")
        st.subheader("📦 Included Workflows")

        orch_data          = orchestrator.get('orchestrator_data', {})
        included_workflows = orch_data.get('includedWorkflows', {})

        if not included_workflows:
            st.info("No workflow information available")
            return

        st.success(f"This orchestrator includes {len(included_workflows)} workflow(s)")

        rows = []
        for wf_id, wf_data in included_workflows.items():
            desc = wf_data.get('description', '')
            rows.append({
                'ID':          wf_id[:12] + '…',
                'Name':        wf_data.get('name', 'Unknown'),
                'Version':     wf_data.get('version', 'N/A'),
                'Description': (desc[:50] + '…') if len(desc) > 50 else desc,
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        st.markdown("**Workflow Details:**")
        for idx, (wf_id, wf_data) in enumerate(list(included_workflows.items())[:10]):
            with st.expander(f"Workflow: {wf_data.get('name', 'Unknown')}", expanded=False):
                c1, c2 = st.columns(2)
                with c1:
                    st.write(f"**ID:** {wf_id[:20]}…")
                    st.write(f"**Name:** {wf_data.get('name', 'Unknown')}")
                    st.write(f"**Version:** {wf_data.get('version', 'N/A')}")
                with c2:
                    nodes = wf_data.get('drawflow', {}).get('nodes', {})
                    st.write(f"**Nodes:** {len(nodes) if isinstance(nodes, (dict, list)) else 0}")
                    st.write(f"**Has Settings:** {'✅' if wf_data.get('settings') else '❌'}")

                wf_clean = self._convert_objectid(wf_data)
                wf_name  = wf_clean.get('name', 'workflow').replace(' ', '_')
                st.download_button(
                    label=f"📥 Download {wf_data.get('name', 'workflow')}",
                    data=json.dumps(wf_clean, indent=2, ensure_ascii=False),
                    file_name=f"{wf_name}_{wf_id[:10]}.json",
                    mime="application/json",
                    help="Import directly into Automa",
                    key=f"dl_wf_{base_key}_{wf_id}_{idx}",
                )

                if self.metadata_collection is not None:
                    self._render_workflow_metadata_link(wf_id)

        if len(included_workflows) > 10:
            st.caption(f"Showing first 10 of {len(included_workflows)} workflows")

    def _render_workflow_metadata_link(self, workflow_id: str):
        try:
            meta = self.metadata_collection.find_one(
                {'automa_workflow_id': ObjectId(workflow_id)}
            )
            if meta:
                st.markdown("**Associated Metadata:**")
                c1, c2, c3 = st.columns(3)
                with c1: st.write(f"Content ID: {meta.get('postgres_content_id', 'N/A')}")
                with c2: st.write(f"Has Link: {'✅' if meta.get('has_link') else '❌'}")
                with c3: st.write(f"Executed: {'✅' if meta.get('executed') else '❌'}")
                if meta.get('link_url'):
                    url = meta['link_url']
                    st.markdown(f"**Link:** [{url}]({url})")
        except Exception:
            pass

    # ── Details tab ───────────────────────────────────────────────────────────

    def _render_orchestrator_details(self):
        st.subheader("Orchestrator Details")
        try:
            orchestrators = list(
                self.orchestrators_collection.find().sort("created_at", -1)
            )
            if not orchestrators:
                st.info("No orchestrators available")
                return

            options: Dict[str, Dict] = {}
            for orch in orchestrators:
                execution_id   = orch.get('execution_id', 'Unknown')
                workflow_count = orch.get('workflow_count', 0)
                label = (
                    f"{execution_id[:30]}… ({workflow_count} workflows) — "
                    f"{_fmt_ts(orch.get('created_at'), short=True)}"
                )
                options[label] = orch

            selected = st.selectbox(
                "Select Orchestrator:",
                options=list(options.keys()),
                key="orchestrator_detail_selector",
            )
            if selected:
                self._render_orchestrator_card(options[selected], context="details")

        except Exception as e:
            st.error(f"Error loading orchestrator details: {e}")

    # ── Statistics tab ────────────────────────────────────────────────────────

    def _render_statistics(self):
        st.subheader("📈 Orchestrator Statistics")
        try:
            orchestrators = list(self.orchestrators_collection.find())
            if not orchestrators:
                st.info("No orchestrator data available")
                return

            total       = len(orchestrators)
            total_wf    = sum(o.get('workflow_count', 0) for o in orchestrators)
            total_nodes = sum(o.get('node_count', 0)     for o in orchestrators)

            c1, c2, c3 = st.columns(3)
            with c1: st.metric("Total Orchestrators", total)
            with c2:
                st.metric("Total Workflows Orchestrated", total_wf)
                if total: st.caption(f"Avg: {total_wf / total:.1f} workflows/orchestrator")
            with c3:
                st.metric("Total Nodes", total_nodes)
                if total: st.caption(f"Avg: {total_nodes / total:.1f} nodes/orchestrator")

            # ── Timing statistics ──────────────────────────────────────────
            st.markdown("---")
            st.subheader("⏱ Timing Statistics")

            timing_docs = [_extract_timing_fields(o) for o in orchestrators]
            timed = [t for t in timing_docs if t['estimated_ms'] is not None]

            if not timed:
                st.info("No timing data yet — timing is recorded from v10 onwards.")
            else:
                def _safe_avg(vals):
                    vals = [v for v in vals if v is not None]
                    return sum(vals) / len(vals) if vals else None

                def _safe_min(vals):
                    vals = [v for v in vals if v is not None]
                    return min(vals) if vals else None

                def _safe_max(vals):
                    vals = [v for v in vals if v is not None]
                    return max(vals) if vals else None

                def _fmt(ms) -> str:
                    if ms is None: return "—"
                    s = ms / 1000
                    return f"{s / 60:.1f} min" if s >= 60 else f"{s:.0f}s"

                est_vals  = [t['estimated_ms']  for t in timed]
                conf_vals = [t['configured_ms'] for t in timed]
                marg_vals = [t['margin_ms']      for t in timed]

                tc1, tc2, tc3, tc4 = st.columns(4)
                with tc1:
                    st.metric("Avg Estimated Wait", _fmt(_safe_avg(est_vals)))
                    st.caption(f"Min: {_fmt(_safe_min(est_vals))}  Max: {_fmt(_safe_max(est_vals))}")
                with tc2:
                    st.metric("Avg Wait Margin", _fmt(_safe_avg(marg_vals)))
                    st.caption(f"Min: {_fmt(_safe_min(marg_vals))}  Max: {_fmt(_safe_max(marg_vals))}")
                with tc3:
                    st.metric("Avg Configured Wait", _fmt(_safe_avg(conf_vals)))
                    st.caption(f"Min: {_fmt(_safe_min(conf_vals))}  Max: {_fmt(_safe_max(conf_vals))}")
                with tc4:
                    actual_ms_vals = []
                    if self.sessions_collection is not None:
                        try:
                            exec_ids = [o.get('execution_id') for o in orchestrators if o.get('execution_id')]
                            sessions = list(self.sessions_collection.find(
                                {'dag_run_id': {'$in': exec_ids}, 'actual_wait_ms': {'$ne': None}},
                                {'actual_wait_ms': 1}
                            ))
                            actual_ms_vals = [s['actual_wait_ms'] for s in sessions if s.get('actual_wait_ms')]
                        except Exception:
                            pass
                    st.metric("Avg Actual Wait", _fmt(_safe_avg(actual_ms_vals)) if actual_ms_vals else "—")
                    if actual_ms_vals:
                        st.caption(f"Min: {_fmt(_safe_min(actual_ms_vals))}  Max: {_fmt(_safe_max(actual_ms_vals))}")
                    else:
                        st.caption("Not yet available")

                st.markdown("---")
                st.markdown("**Per-run timing history:**")
                timing_rows = []
                for o, t in zip(orchestrators, timing_docs):
                    timing_rows.append({
                        'Run':        _fmt_ts(o.get('created_at'), short=True),
                        'Workflows':  o.get('workflow_count', 0),
                        'Estimated':  _fmt(t['estimated_ms']),
                        'Margin':     _fmt(t['margin_ms']),
                        'Configured': _fmt(t['configured_ms']),
                        'Method':     t['method'] or '—',
                    })
                st.dataframe(pd.DataFrame(timing_rows), use_container_width=True, hide_index=True)

            # ── Status breakdown ───────────────────────────────────────────
            st.markdown("---")
            st.subheader("Status Breakdown")
            status_counts: Dict[str, int] = {}
            for o in orchestrators:
                s = o.get('status', 'unknown')
                status_counts[s] = status_counts.get(s, 0) + 1

            c1, c2 = st.columns(2)
            with c1:
                st.dataframe(
                    pd.DataFrame([
                        {'Status': s.upper(), 'Count': c}
                        for s, c in sorted(status_counts.items())
                    ]),
                    use_container_width=True, hide_index=True,
                )
            with c2:
                for s, c in sorted(status_counts.items()):
                    st.write(f"**{s.upper()}:** {(c / total) * 100:.1f}%")

            # ── Timeline ───────────────────────────────────────────────────
            st.markdown("---")
            st.subheader("Execution Timeline")

            seven_ago = datetime.now() - timedelta(days=7)
            recent    = [o for o in orchestrators if _ts_after(o.get('created_at'), seven_ago)]

            if recent:
                st.success(f"{len(recent)} orchestrators in the last 7 days")
                by_date: Dict[str, int] = {}
                for o in recent:
                    d = _ts_date_str(o.get('created_at'))
                    if d:
                        by_date[d] = by_date.get(d, 0) + 1
                if by_date:
                    df = pd.DataFrame(
                        [{'Date': d, 'Executions': c} for d, c in sorted(by_date.items())]
                    )
                    st.line_chart(df.set_index('Date'))
            else:
                st.info("No executions in the last 7 days")

        except Exception as e:
            st.error(f"Error calculating statistics: {e}")
            import traceback
            with st.expander("View Error Details"):
                st.code(traceback.format_exc())


# ── Timestamp utilities ───────────────────────────────────────────────────────

def _parse_ts(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            return None
    return None


def _fmt_ts(value, short: bool = False) -> str:
    dt = _parse_ts(value)
    if dt is None:
        return str(value) if value else "Unknown"
    return dt.strftime('%Y-%m-%d %H:%M' if short else '%Y-%m-%d %H:%M:%S')


def _ts_after(value, threshold: datetime) -> bool:
    dt = _parse_ts(value)
    return dt is not None and dt > threshold


def _ts_date_str(value) -> Optional[str]:
    dt = _parse_ts(value)
    return dt.strftime('%Y-%m-%d') if dt else None


# ── Public helper ─────────────────────────────────────────────────────────────

def render_orchestrator_monitor():
    """Drop-in replacement for the old render call."""
    OrchestratorMonitor().render()
