# src/streamlit/ui/pages/generate_manual_workflows/generate_manual_workflows.py
"""
Generate Manual Workflows — Streamlit page  v2.0.0
Changes over v1:
  - _do_extract_all renders a live diagnostic log panel using diag_events
    returned by the v12 extractor.
  - _render_diag_panel() formats structured events with colour-coded badges,
    phase labels, expandable DEBUG section, and a plain-text download button.
  - Per-survey summary table shows provider + reason for each result.
  - discovery_source and network_api_calls surfaced in results panel.
  - Diagnostic panel also shown on failure so errors are diagnosable.
  - Everything else unchanged from v1.
"""

import io
import csv
import json
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import streamlit as st
from psycopg2.extras import RealDictCursor

from src.core.database.postgres.connection import get_postgres_connection
from .gemini_answer_service import generate_answers_with_gemini
from .orchestrator import SurveySiteOrchestrator
from .utils.chrome_helpers import (
    ensure_chrome_running,
    get_debug_port_for_account,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Diagnostic event rendering
# ---------------------------------------------------------------------------

_LEVEL_STYLE = {
    "OK":      ("✅", "#155724", "#d4edda", "#c3e6cb"),
    "ERROR":   ("❌", "#721c24", "#f8d7da", "#f5c6cb"),
    "WARN":    ("⚠️", "#856404", "#fff3cd", "#ffeeba"),
    "INFO":    ("ℹ️", "#0c5460", "#d1ecf1", "#bee5eb"),
    "STEP":    ("▶️", "#383d41", "#e2e3e5", "#d6d8db"),
    "DEBUG":   ("🔍", "#555",    "#f8f9fa", "#dee2e6"),
    "SECTION": ("",  "#fff",    "#343a40", "#343a40"),
}

_PHASE_COLOR = {
    "INIT":      "#6c757d",
    "NETWORK":   "#0d6efd",
    "DISCOVERY": "#6f42c1",
    "NAV":       "#fd7e14",
    "IFRAME":    "#20c997",
    "EXTRACT":   "#0dcaf0",
    "DB":        "#198754",
    "SURVEY":    "#dc3545",
}


def _render_diag_panel(events: List[Dict], container=None):
    """
    Render DiagnosticLogger events as a formatted, colour-coded log panel.
    Pass container=st (default) or any st.container / st.expander.
    """
    tgt = container or st

    if not events:
        tgt.info("No diagnostic events captured.")
        return

    # Summary bar
    counts: Dict[str, int] = {}
    for e in events:
        counts[e.get("level", "INFO")] = counts.get(e.get("level", "INFO"), 0) + 1

    cols = tgt.columns(5)
    for i, (lvl, label) in enumerate([
        ("OK",    "✅ OK"),
        ("ERROR", "❌ Errors"),
        ("WARN",  "⚠️ Warnings"),
        ("STEP",  "▶️ Steps"),
        ("DEBUG", "🔍 Debug"),
    ]):
        cols[i].metric(label, counts.get(lvl, 0))

    tgt.markdown("---")

    plain_lines: List[str] = []
    debug_events: List[Dict] = []

    for e in events:
        level  = e.get("level", "INFO")
        phase  = e.get("phase", "")
        msg    = e.get("msg", "")
        detail = e.get("detail", "")
        ts     = e.get("ts", "")

        plain_lines.append(
            f"[{ts}] {level:7s} [{phase:10s}] {msg}"
            + (f"  -- {detail}" if detail else "")
        )

        if level == "SECTION":
            tgt.markdown(
                f"<div style='background:#343a40;color:#fff;padding:5px 12px;"
                f"border-radius:4px;margin:10px 0 4px;font-size:12px;"
                f"font-weight:700;letter-spacing:0.06em'>{msg}</div>",
                unsafe_allow_html=True,
            )
            continue

        if level == "DEBUG":
            debug_events.append(e)
            continue

        icon, text_col, bg_col, border_col = _LEVEL_STYLE.get(
            level, ("•", "#333", "#fff", "#ccc")
        )
        phase_color = _PHASE_COLOR.get(phase, "#888")

        phase_span = (
            f"<span style='background:{phase_color};color:#fff;padding:1px 7px;"
            f"border-radius:3px;font-size:10px;font-weight:700;"
            f"margin-right:7px;vertical-align:middle'>{phase}</span>"
        ) if phase else ""

        detail_span = (
            f"<span style='color:#777;font-size:11px;margin-left:10px'>{detail}</span>"
        ) if detail else ""

        ts_span = (
            f"<span style='color:#bbb;font-size:10px;margin-right:8px;"
            f"font-family:monospace'>{ts}</span>"
        )

        tgt.markdown(
            f"<div style='background:{bg_col};border-left:4px solid {border_col};"
            f"padding:5px 12px;margin:2px 0;border-radius:0 5px 5px 0;"
            f"font-size:13px;line-height:1.6'>"
            f"{ts_span}{icon}&nbsp;{phase_span}"
            f"<span style='color:{text_col}'>{msg}</span>"
            f"{detail_span}</div>",
            unsafe_allow_html=True,
        )

    # Debug section (collapsed)
    if debug_events:
        with tgt.expander(f"🔍 Debug details ({len(debug_events)} lines)", expanded=False):
            for e in debug_events:
                phase = e.get("phase", "")
                pc    = _PHASE_COLOR.get(phase, "#888")
                st.markdown(
                    f"<div style='font-size:11px;color:#555;padding:2px 8px;"
                    f"font-family:monospace;line-height:1.5'>"
                    f"<span style='color:#aaa'>{e.get('ts','')}</span>&nbsp;"
                    f"<span style='background:{pc};color:#fff;padding:0 5px;"
                    f"border-radius:2px;font-size:9px'>{phase}</span>&nbsp;"
                    f"{e.get('msg','')} "
                    f"<span style='color:#999'>{e.get('detail','')}</span></div>",
                    unsafe_allow_html=True,
                )

    tgt.download_button(
        "⬇️ Download full diagnostic log",
        data="\n".join(plain_lines),
        file_name=f"extraction_diag_{datetime.now():%Y%m%d_%H%M%S}.txt",
        mime="text/plain",
        key=f"dl_diag_{int(time.time())}",
    )


# ---------------------------------------------------------------------------
# Page class
# ---------------------------------------------------------------------------

class GenerateManualWorkflowsPage:

    def __init__(self, db_manager):
        self.db_manager   = db_manager
        self.orchestrator = SurveySiteOrchestrator(db_manager)

        for k, v in {
            "generation_in_progress": False,
            "generation_results":     None,
            "generation_logs":        [],
            "selected_action":        None,
        }.items():
            if k not in st.session_state:
                st.session_state[k] = v

    def log(self, msg: str, level: str = "INFO"):
        ts = datetime.now().strftime("%H:%M:%S")
        st.session_state.generation_logs.append(f"[{ts}] {level}: {msg}")
        if len(st.session_state.generation_logs) > 100:
            st.session_state.generation_logs = st.session_state.generation_logs[-100:]

    def clear_logs(self):
        st.session_state.generation_logs = []

    # ------------------------------------------------------------------
    # Main render
    # ------------------------------------------------------------------

    def render(self):
        st.title("⚙️ Generate Manual Workflows")

        st.markdown("""
        <div style='background:#1e3a5f;padding:18px;border-radius:10px;margin-bottom:18px;'>
        <h3 style='color:white;margin:0;'>Manual Workflow Generator</h3>
        <p style='color:#a0c4ff;margin:8px 0 0;'>
        🔍 <b>Extract</b> — extract screener questions from all surveys on the dashboard.<br>
        📝 <b>Answer</b> — Gemini AI answers questions per survey using your persona prompt.<br>
        ⚙️ <b>Create</b> — build a continuous-loop Automa workflow per survey.<br>
        📤 <b>Upload</b> — inject workflow directly into the running Chrome session.<br>
        🏆 <b>Results</b> — track pass/fail per survey.
        </p>
        </div>""", unsafe_allow_html=True)

        accounts     = self._load_accounts()
        survey_sites = self._load_survey_sites()
        prompts      = self._load_prompts()
        avail_sites  = self.orchestrator.get_available_sites()

        if not avail_sites:
            st.error("⚠️ No survey sites with both an extractor AND a workflow creator found.")
            with st.expander("🔍 Debug: module loading", expanded=True):
                col_a, col_b = st.columns(2)
                with col_a:
                    st.write("**Extractors loaded:**")
                    st.write(list(self.orchestrator.extractors.keys()) or ["none"])
                with col_b:
                    st.write("**Creators loaded:**")
                    st.write(list(self.orchestrator.workflow_creators.keys()) or ["none"])
                ext_only = self.orchestrator.get_extractor_only_sites()
                cre_only = self.orchestrator.get_creator_only_sites()
                if ext_only:
                    st.warning(f"Has extractor but NO creator: {ext_only}")
                if cre_only:
                    st.warning(f"Has creator but NO extractor: {cre_only}")
                from pathlib import Path
                base = Path(__file__).parent
                st.write("**Extractor dir:**", str(base / "extractors"), "— Exists:", (base / "extractors").exists())
                st.write("**Creator dir:**",   str(base / "workflow_creators"), "— Exists:", (base / "workflow_creators").exists())
                if st.button("🔄 Reload modules", key="reload_modules"):
                    self.orchestrator._load_modules(); st.rerun()
            return

        if not accounts:
            st.warning("⚠️ No accounts found.")
            return

        avail_names = {s["site_name"] for s in avail_sites}

        col1, col2 = st.columns(2)
        with col1:
            st.subheader("👤 Account")
            acc_opts = {}
            for a in accounts:
                has_p = any(p["account_id"] == a["account_id"] for p in prompts)
                acc_opts[f"{'✅' if has_p else '❌'} {a['username']} (ID:{a['account_id']})"] = a
            acct        = acc_opts[st.selectbox("Account:", list(acc_opts), key="wf_acct")]
            acct_prompt = next((p for p in prompts if p["account_id"] == acct["account_id"]), None)
            if acct_prompt:
                st.success(f"✅ Prompt: {acct_prompt['prompt_name']}")
            else:
                st.warning("⚠️ No prompt — create one in Prompts page")
            debug_port = get_debug_port_for_account(st.session_state, acct["account_id"])
            if debug_port:
                st.success(f"🟢 Chrome active (port {debug_port})")
            else:
                st.info("⚪ No Chrome session")

        with col2:
            st.subheader("🌐 Survey Site")
            db_sites = [s for s in survey_sites if s["site_name"] in avail_names]
            if not db_sites:
                st.error(f"No DB sites match loaded module names.  \nModule names: {sorted(avail_names)}")
                return
            site_opts = {s["site_name"]: s for s in db_sites}
            site      = site_opts[st.selectbox("Survey Site:", list(site_opts), key="wf_site")]
            si        = next((s for s in avail_sites if s["site_name"] == site["site_name"]), {})
            st.caption(f"Extractor v{si.get('extractor_version','?')} | Creator v{si.get('creator_version','?')}")

        st.markdown("---")

        t1, t2, t3, t4, t5 = st.tabs([
            "🔍 Extract Questions",
            "📝 Answer Questions",
            "⚙️ Create Workflows",
            "📤 Upload to Chrome",
            "🏆 Screening Results",
        ])
        with t1: self._tab_extract(acct, site, acct_prompt)
        with t2: self._tab_answer(acct, site, acct_prompt)
        with t3: self._tab_create(acct, site, acct_prompt)
        with t4: self._tab_upload(acct, site)
        with t5: self._tab_screening_results(acct, site)

        if st.session_state.generation_logs:
            st.markdown("---")
            with st.expander("📋 Logs", expanded=False):
                st.code("\n".join(st.session_state.generation_logs[-25:]), language="log")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Clear", key="clr_logs"):
                        self.clear_logs(); st.rerun()
                with c2:
                    st.download_button(
                        "Download Logs",
                        "\n".join(st.session_state.generation_logs),
                        f"logs_{datetime.now():%Y%m%d_%H%M%S}.txt",
                    )

        if st.session_state.generation_results:
            st.markdown("---")
            self._render_results(st.session_state.generation_results)

    # ======================================================================
    # TAB 1 — EXTRACT
    # ======================================================================

    def _tab_extract(self, acct, site, prompt):
        st.subheader("🔍 Extract Questions from Survey Site")

        ei = {}
        if site["site_name"] in self.orchestrator.extractors:
            ei = self.orchestrator.extractors[site["site_name"]].get_site_info()

        debug_port = get_debug_port_for_account(st.session_state, acct["account_id"])

        st.info(
            f"**Account:** {acct['username']}  \n"
            f"**Site:** {site['site_name']}  \n"
            f"**Extractor:** v{ei.get('version','?')}  \n"
            f"**Chrome:** {'🟢 Port ' + str(debug_port) + ' — live extraction' if debug_port else '🔴 No Chrome session — cannot extract'}  \n"
            f"**Prompt:** {prompt['prompt_name'] if prompt else '—'}"
        )

        if not prompt:
            st.error("❌ Account has no prompt"); return
        if not debug_port:
            st.error("❌ No active Chrome session. Start one from Accounts → Local Chrome first."); return

        urls = self._get_urls(acct["account_id"], site["site_id"])
        if not urls:
            st.warning("⚠️ No URLs configured for this account/site."); return

        url_map = {}
        for u in urls:
            star = "⭐ " if u.get("is_default") else ""
            used = " [used]" if u.get("is_used") else ""
            url_map[f"{star}{u['url']}{used}"] = u
        sel_url = url_map[st.selectbox("Dashboard / Listing URL:", list(url_map), key="ext_url")]

        raw_url = sel_url["url"].strip()
        if raw_url and not raw_url.startswith(("http://", "https://")):
            st.warning(f"⚠️ URL **`{raw_url}`** has no scheme — `https://` will be prepended automatically.")
        if sel_url.get("is_used"):
            st.warning("⚠️ URL already marked used — you can still proceed.")

        st.markdown("---")
        st.caption("Finds every available survey on the dashboard and extracts all screener questions from each. DQ/unavailable surveys are skipped automatically.")

        if st.button(
            "🚀 Extract All Surveys", type="primary", use_container_width=True,
            key="ext_btn_all",
            disabled=st.session_state.get("generation_in_progress", False),
        ):
            self._do_extract_all(acct, site, prompt, sel_url)

    def _do_extract_all(self, acct, site, prompt, url_info):
        self.log(f"Extract ALL surveys: {acct['username']} / {site['site_name']}")
        st.session_state.generation_in_progress = True

        progress_bar     = st.progress(0)
        status_text      = st.empty()
        survey_log       = st.empty()
        diag_placeholder = st.empty()   # filled after run completes

        def on_progress(current, total, msg):
            progress_bar.progress(int(current / total * 100) if total else 0)
            status_text.markdown(
                f"<div style='font-size:13px;padding:4px 0'>"
                f"<span style='color:#888'>[{current}/{total}]</span>&nbsp;{msg}</div>",
                unsafe_allow_html=True,
            )
            self.log(msg)

        try:
            from src.streamlit.ui.pages.accounts.chrome_session_manager import ChromeSessionManager
            profile_path = ChromeSessionManager(self.db_manager).get_profile_path(acct["username"])
            debug_port   = get_debug_port_for_account(st.session_state, acct["account_id"])

            result = self.orchestrator.extract_all_questions(
                account_id=acct["account_id"],
                site_id=site["site_id"],
                listing_url=url_info["url"],
                profile_path=profile_path,
                site_name=site["site_name"],
                debug_port=debug_port,
                progress_callback=on_progress,
            )

            progress_bar.progress(100)
            status_text.empty()

            diag_events = result.get("diag_events", [])

            # Always render the diagnostic panel immediately after extraction
            if diag_events:
                with diag_placeholder.container():
                    st.markdown("### 🔬 Extraction Diagnostics")
                    _render_diag_panel(diag_events)

            if result.get("success"):
                self._mark_url_used(url_info["url_id"])
                self.log(
                    f"Done: {result.get('surveys_successful',0)} surveys ok, "
                    f"{result['questions_found']} questions, {result['inserted']} inserted | "
                    f"source={result.get('discovery_source','?')} | "
                    f"api_calls={result.get('network_api_calls',0)}"
                )

                survey_results = result.get("survey_results", [])
                if survey_results:
                    icons = {"success": "✅", "dq": "❌", "error": "⚠️", "skip": "⏭️"}
                    rows  = []
                    for r in survey_results:
                        icon   = icons.get(r["status"], "❓")
                        name   = r.get("survey_name") or r.get("survey_label", "?")
                        qs     = r.get("questions", 0)
                        ins    = r.get("inserted", 0)
                        prov   = r.get("provider", "—")
                        reason = f"  _{r['reason']}_" if r.get("reason") else ""
                        rows.append(
                            f"{icon} **{name}** — `{r['status']}` — "
                            f"{qs} Qs / {ins} inserted — `{prov}`{reason}"
                        )
                    survey_log.markdown("\n\n".join(rows))

                st.session_state.generation_results = {
                    "action":              "extract_all_surveys",
                    "status":              "success",
                    "timestamp":           datetime.now().isoformat(),
                    "account":             {"id": acct["account_id"], "username": acct["username"]},
                    "site":                {"id": site["site_id"],    "name": site["site_name"]},
                    "url":                 {"url": url_info["url"]},
                    "questions_extracted": result["questions_found"],
                    "inserted":            result["inserted"],
                    "surveys_found":       result.get("surveys_found", 0),
                    "surveys_processed":   result.get("surveys_processed", 0),
                    "surveys_successful":  result.get("surveys_successful", 0),
                    "surveys_failed":      result.get("surveys_failed", 0),
                    "survey_results":      survey_results,
                    "batch_id":            result.get("batch_id", ""),
                    "discovery_source":    result.get("discovery_source", "?"),
                    "network_api_calls":   result.get("network_api_calls", 0),
                    "execution_time_seconds": result.get("execution_time_seconds", 0),
                    "diag_events":         diag_events,
                }
            else:
                self.log(f"Failed: {result.get('error')}", "ERROR")
                st.session_state.generation_results = {
                    "action":    "extract_all_surveys",
                    "status":    "failed",
                    "error":     result.get("error", "Unknown"),
                    "account":   {"id": acct["account_id"], "username": acct["username"]},
                    "site":      {"id": site["site_id"],    "name": site["site_name"]},
                    "timestamp": datetime.now().isoformat(),
                    "diag_events": diag_events,
                }

        except Exception as exc:
            progress_bar.empty()
            status_text.empty()
            self.log(str(exc), "ERROR")
            st.session_state.generation_results = {
                "action":    "extract_all_surveys",
                "status":    "failed",
                "error":     str(exc),
                "account":   {"id": acct["account_id"], "username": acct["username"]},
                "site":      {"id": site["site_id"],    "name": site["site_name"]},
                "timestamp": datetime.now().isoformat(),
            }
        finally:
            st.session_state.generation_in_progress = False
            st.rerun()

    def _do_extract(self, acct, site, prompt, url_info, use_chrome):
        self.log(f"Extraction start: {acct['username']} / {site['site_name']}")
        st.session_state.generation_in_progress = True
        try:
            from src.streamlit.ui.pages.accounts.chrome_session_manager import ChromeSessionManager
            profile_path = ChromeSessionManager(self.db_manager).get_profile_path(acct["username"])
            debug_port   = get_debug_port_for_account(st.session_state, acct["account_id"])
            if use_chrome and not debug_port:
                ok         = ensure_chrome_running(profile_path)
                debug_port = get_debug_port_for_account(st.session_state, acct["account_id"]) if ok else None
                self.log(f"Chrome: {'port ' + str(debug_port) if debug_port else 'failed → simulation'}")

            result = self.orchestrator.extract_questions(
                account_id=acct["account_id"], site_id=site["site_id"],
                url=url_info["url"], profile_path=profile_path,
                site_name=site["site_name"], debug_port=debug_port,
            )
            if result.get("success"):
                self._mark_url_used(url_info["url_id"])
                survey_name = result.get("survey_name", "Unknown Survey")
                self.log(f"Done: {result['questions_found']} found, {result['inserted']} inserted, survey='{survey_name}'")
                st.session_state.generation_results = {
                    "action": "extract_questions", "status": "success",
                    "timestamp": datetime.now().isoformat(),
                    "account": {"id": acct["account_id"], "username": acct["username"]},
                    "site":    {"id": site["site_id"],    "name": site["site_name"]},
                    "url":     {"url": url_info["url"]},
                    "questions_extracted": result["questions_found"],
                    "inserted":  result["inserted"],
                    "batch_id":  result["batch_id"],
                    "survey_name": survey_name,
                    "execution_time_seconds": result.get("execution_time_seconds", 0),
                }
            else:
                self.log(f"Failed: {result.get('error')}", "ERROR")
                st.session_state.generation_results = {
                    "action": "extract_questions", "status": "failed",
                    "error": result.get("error", "Unknown"),
                    "account": {"id": acct["account_id"], "username": acct["username"]},
                    "site":    {"id": site["site_id"],    "name": site["site_name"]},
                    "timestamp": datetime.now().isoformat(),
                }
        except Exception as exc:
            self.log(str(exc), "ERROR")
            st.session_state.generation_results = {
                "action": "extract_questions", "status": "failed", "error": str(exc),
                "account": {"id": acct["account_id"], "username": acct["username"]},
                "site":    {"id": site["site_id"],    "name": site["site_name"]},
                "timestamp": datetime.now().isoformat(),
            }
        finally:
            st.session_state.generation_in_progress = False
            st.rerun()

    # ======================================================================
    # TAB 2 — ANSWER
    # ======================================================================

    def _tab_answer(self, acct, site, prompt):
        st.subheader("📝 Answer Questions with Gemini AI")

        if not prompt:
            st.error("❌ No prompt — create one in the Prompts page first."); return

        import os
        if not os.environ.get("GEMINI_API_KEY", ""):
            st.error("❌ GEMINI_API_KEY not found in environment."); return

        survey_names = self._get_survey_names(acct["account_id"], site["site_id"])
        if not survey_names:
            st.info("ℹ️ No unused questions found — run **Extract Questions** first."); return

        selected_survey = st.selectbox(
            "📋 Select Survey to Answer:",
            options=["— All surveys —"] + survey_names,
            key="ans_survey_select",
        )

        qs = (
            self._unused_questions(acct["account_id"], site["site_id"], survey_name=selected_survey)
            if selected_survey and selected_survey != "— All surveys —"
            else self._unused_questions(acct["account_id"], site["site_id"])
        )

        if not qs:
            st.info("ℹ️ No unused questions for this selection."); return

        st.info(
            f"**Account:** {acct['username']}  \n**Site:** {site['site_name']}  \n"
            f"**Survey:** {selected_survey}  \n**Prompt:** {prompt['prompt_name']}  \n"
            f"**Questions to answer:** {len(qs)}  \n**Gemini API:** ✅ Key found"
        )

        with st.expander("👁️ View persona prompt", expanded=False):
            st.code(prompt["content"], language=None)

        with st.expander(f"📋 Preview questions ({len(qs)} total)", expanded=True):
            for i, q in enumerate(qs):
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"**Q{i+1}.** {q['question_text']}")
                    st.caption(f"Type: `{q['question_type']}` | Category: `{q.get('question_category') or '—'}` | Survey: `{q.get('survey_name') or '—'}`")
                    options = q.get("options") or []
                    if isinstance(options, str):
                        try:    options = json.loads(options)
                        except: options = []
                    if options:
                        st.caption(f"Options: {', '.join(str(o) for o in options[:5])}" + (" …" if len(options) > 5 else ""))
                with col2:
                    st.caption("→ Gemini")
                st.divider()

        if st.button(
            f"🚀 Answer All {len(qs)} Questions with Gemini",
            type="primary", use_container_width=True, key="ans_btn",
            disabled=st.session_state.get("generation_in_progress", False),
        ):
            self._do_answer(acct, site, prompt, qs, selected_survey)

    def _do_answer(self, acct, site, prompt, questions, survey_name=""):
        self.log(f"Gemini: {acct['username']} / {len(questions)} Qs / survey='{survey_name}'")
        st.session_state.generation_in_progress = True
        progress_bar = st.progress(0)
        status_text  = st.empty()

        def on_progress(current, total, msg):
            progress_bar.progress(int(current / total * 100))
            status_text.text(f"[{current}/{total}] {msg}")
            self.log(msg)

        enriched_prompt = dict(prompt)
        enriched_prompt["username"] = acct["username"]

        try:
            result = generate_answers_with_gemini(
                questions=questions, prompt=enriched_prompt,
                account_id=acct["account_id"], site_id=site["site_id"],
                progress_callback=on_progress,
            )
            progress_bar.progress(100)
            status_text.empty()
            self.log(f"Done: {result['answers_generated']} saved, {result['failed']} failed")

            if result["success"]:
                self._upsert_screening_result(
                    account_id=acct["account_id"], site_id=site["site_id"],
                    survey_name=survey_name if survey_name != "— All surveys —" else None,
                    batch_id=result["batch_id"],
                    screener_answers=result["answers_generated"],
                    status="pending",
                )

            st.session_state.generation_results = {
                "action": "answer_questions",
                "status": "success" if result["success"] else "failed",
                "timestamp": datetime.now().isoformat(),
                "account": {"id": acct["account_id"], "username": acct["username"]},
                "site":    {"id": site["site_id"],    "name": site["site_name"]},
                "survey_name":       survey_name,
                "answers_generated": result["answers_generated"],
                "answers_failed":    result["failed"],
                "batch_id":          result["batch_id"],
                "details":           result["details"],
                "execution_time_seconds": 0,
                "error": result.get("error"),
            }
        except Exception as exc:
            progress_bar.empty(); status_text.empty()
            self.log(str(exc), "ERROR")
            st.session_state.generation_results = {
                "action": "answer_questions", "status": "failed", "error": str(exc),
                "account": {"id": acct["account_id"], "username": acct["username"]},
                "site":    {"id": site["site_id"],    "name": site["site_name"]},
                "timestamp": datetime.now().isoformat(),
            }
        finally:
            st.session_state.generation_in_progress = False
            st.rerun()

    # ======================================================================
    # TAB 3 — CREATE WORKFLOWS
    # ======================================================================

    def _tab_create(self, acct, site, prompt):
        st.subheader("⚙️ Create Automa Workflows")

        ci = {}
        if site["site_name"] in self.orchestrator.workflow_creators:
            ci = self.orchestrator.workflow_creators[site["site_name"]].get_site_info()

        urls       = self._get_urls(acct["account_id"], site["site_id"])
        survey_url = urls[0]["url"] if urls else ""

        survey_names = self._get_survey_names(acct["account_id"], site["site_id"])
        if not survey_names:
            st.info("ℹ️ No surveys with unused questions found.  \nRun **Extract Questions** (Tab 1) first.")
            return

        selected_survey = st.selectbox("📋 Survey:", options=survey_names, key="cr_survey_select")
        if not selected_survey:
            return

        qs = self._unused_questions(acct["account_id"], site["site_id"], survey_name=selected_survey)

        st.info(
            f"**Account:** {acct['username']}  \n**Site:** {site['site_name']}  \n"
            f"**Survey:** {selected_survey}  \n**Creator:** {ci.get('template_name','Standard')} v{ci.get('version','?')}  \n"
            f"**Prompt:** {prompt['prompt_name'] if prompt else '— none'}"
        )

        if not qs:
            st.warning(f"⚠️ No unused questions for **{selected_survey}**."); return

        st.success(f"✅ {len(qs)} questions available")

        with st.expander("Preview questions"):
            for q in qs[:8]:
                st.markdown(f"- {q['question_text'][:90]}… (`{q['question_type']}`)")
            if len(qs) > 8:
                st.caption(f"… and {len(qs) - 8} more")

        c1, c2 = st.columns(2)
        with c1:
            n_wf = st.number_input("Workflows to create:", 1, min(10, len(qs)), 1, key="cr_n")
        with st.expander("Advanced"):
            inc_click = st.checkbox("Include click selectors", True, key="cr_click")
            inc_input = st.checkbox("Include input selectors", True, key="cr_input")

        if st.button(
            f"🚀 Create Workflow for '{selected_survey}'",
            type="primary", use_container_width=True, key="cr_btn",
            disabled=st.session_state.get("generation_in_progress", False),
        ):
            self._do_create(acct, site, prompt, n_wf, qs, survey_url, inc_click, inc_input, selected_survey)

    def _do_create(self, acct, site, prompt, n_wf, qs, survey_url,
                   inc_click, inc_input, survey_name=None):
        self.log(f"Creating {n_wf} workflow(s): {acct['username']} / {site['site_name']} / survey='{survey_name}'")
        st.session_state.generation_in_progress = True

        enriched = dict(prompt) if prompt else {}
        for f in ("age","gender","city","education_level","job_status","income_range",
                  "marital_status","has_children","household_size","username","email","phone"):
            if acct.get(f) is not None:
                enriched.setdefault(f, acct[f])

        try:
            result = self.orchestrator.create_workflows(
                account_id=acct["account_id"], site_id=site["site_id"],
                questions=qs, prompt=enriched, site_name=site["site_name"],
                workflow_count=n_wf, survey_url=survey_url,
                include_click_elements=inc_click, include_input_elements=inc_input,
            )
            if result.get("success"):
                self.log(f"Created {result['workflows_created']} workflow(s)")
                if survey_name:
                    self._mark_survey_complete(acct["account_id"], site["site_id"], survey_name)
                st.session_state.generation_results = {
                    "action": "create_workflows", "status": "success",
                    "timestamp": datetime.now().isoformat(),
                    "account": {"id": acct["account_id"], "username": acct["username"]},
                    "site":    {"id": site["site_id"],    "name": site["site_name"]},
                    "survey_name":       survey_name,
                    "workflows_created": result["workflows_created"],
                    "workflows":         result["workflows"],
                    "inserted":          result["inserted"],
                    "batch_id":          result["batch_id"],
                    "execution_time_seconds": result.get("execution_time_seconds", 0),
                }
            else:
                self.log(result.get("error", "?"), "ERROR")
                st.session_state.generation_results = {
                    "action": "create_workflows", "status": "failed",
                    "error": result.get("error", "Unknown"),
                    "account": {"id": acct["account_id"], "username": acct["username"]},
                    "site":    {"id": site["site_id"],    "name": site["site_name"]},
                    "survey_name": survey_name, "timestamp": datetime.now().isoformat(),
                }
        except Exception as exc:
            self.log(str(exc), "ERROR")
            st.session_state.generation_results = {
                "action": "create_workflows", "status": "failed", "error": str(exc),
                "account": {"id": acct["account_id"], "username": acct["username"]},
                "site":    {"id": site["site_id"],    "name": site["site_name"]},
                "survey_name": survey_name, "timestamp": datetime.now().isoformat(),
            }
        finally:
            st.session_state.generation_in_progress = False
            st.rerun()

    # ======================================================================
    # TAB 4 — UPLOAD
    # ======================================================================

    def _tab_upload(self, acct, site):
        st.subheader("📤 Upload Workflows to Chrome")

        all_survey_names = self._get_all_survey_names_with_workflows(acct["account_id"], site["site_id"])
        if not all_survey_names:
            st.info("No workflows yet.  \nGo to **Create Workflows** (Tab 3) first."); return

        col1, col2 = st.columns(2)
        with col1:
            selected_survey = st.selectbox("📋 Survey:", options=all_survey_names, key="ul_survey_select")
        with col2:
            st.caption(f"**Account:** {acct['username']}  \n**Site:** {site['site_name']}")

        wfs = self._get_workflows_for_survey(acct["account_id"], site["site_id"], selected_survey)
        if not wfs:
            st.warning(f"No workflows found for **{selected_survey}**."); return

        pending  = [w for w in wfs if not w.get("uploaded_to_chrome")]
        uploaded = [w for w in wfs if w.get("uploaded_to_chrome")]
        st.success(f"✅ **{len(wfs)}** workflow(s) — {len(pending)} pending, {len(uploaded)} uploaded")

        debug_port = get_debug_port_for_account(st.session_state, acct["account_id"])
        if debug_port:
            st.success(f"🟢 Chrome session active on port {debug_port}")
        else:
            st.warning("⚪ No active Chrome session.")

        st.markdown("---")
        wf_options = {}
        for w in wfs:
            dt    = w["created_time"].strftime("%Y-%m-%d %H:%M") if w.get("created_time") else "?"
            badge = " ✅" if w.get("uploaded_to_chrome") else ""
            wf_options[f"{w['workflow_name']}{badge}  ({dt})"] = w

        sel_wf = wf_options[st.selectbox("Select workflow:", list(wf_options), key="ul_sel")]

        with st.expander("👁️ Preview workflow JSON", expanded=False):
            d = sel_wf.get("workflow_data")
            if isinstance(d, str):
                try:    d = json.loads(d)
                except: d = {}
            st.json(d or {})

        d = sel_wf.get("workflow_data")
        if isinstance(d, str):
            try:    d = json.loads(d)
            except: d = {}
        st.download_button("⬇️ Download workflow JSON", data=json.dumps(d, indent=2),
                           file_name=f"{sel_wf['workflow_name']}.json",
                           mime="application/json", key="dl_single")

        st.markdown("---")
        st.markdown("#### 🚀 Auto-upload to running Chrome session")
        st.info("Injects the workflow directly into Automa's IndexedDB.")

        if not debug_port:
            st.button("Upload to Chrome  (start a session first)", disabled=True,
                      use_container_width=True, key="ul_btn_disabled")
        else:
            col_opt1, col_opt2 = st.columns(2)
            with col_opt1:
                mark_uploaded = st.checkbox("Mark as uploaded after success", True, key="ul_mark")
            with col_opt2:
                auto_open = st.checkbox("Open Automa popup after upload", False, key="ul_open")

            if st.button("📤 Upload to Chrome", type="primary", use_container_width=True,
                         key="ul_btn",
                         disabled=st.session_state.get("generation_in_progress", False)):
                self._do_upload_workflow(sel_wf, debug_port, acct["account_id"],
                                         mark_uploaded, auto_open)

        if pending and debug_port:
            st.markdown("---")
            st.markdown("#### 📦 Bulk upload")
            n = st.slider(f"Upload latest N pending (of {len(pending)}):",
                          1, min(10, len(pending)), min(3, len(pending)), key="ul_bulk_n")
            if st.button(f"📤 Upload {n} pending", use_container_width=True, key="ul_bulk_btn"):
                self._do_bulk_upload(pending[:n], debug_port, acct["account_id"])

    def _inject_workflow_js(self, workflow_data: dict) -> str:
        wf_json = json.dumps(workflow_data)
        return f"""
(async () => {{
    const wfData = {wf_json};
    if (!wfData.id) wfData.id = crypto.randomUUID ? crypto.randomUUID() : Math.random().toString(36).slice(2);
    wfData.createdAt = wfData.createdAt || Date.now();
    wfData.updatedAt = Date.now();
    wfData.isProtected = false;
    return new Promise((resolve, reject) => {{
        const req = indexedDB.open('automa-db', 1);
        req.onsuccess = (event) => {{
            const db = event.target.result;
            const storeNames = Array.from(db.objectStoreNames);
            const storeName  = storeNames.find(n => n.includes('workflow')) || storeNames[0];
            if (!storeName) {{ reject('No workflow store found'); return; }}
            const put = db.transaction([storeName], 'readwrite').objectStore(storeName).put(wfData);
            put.onsuccess = () => resolve('✅ "' + wfData.name + '" uploaded');
            put.onerror   = (e) => reject('put failed: ' + e.target.error);
        }};
        req.onerror          = (e) => reject('open failed: ' + e.target.error);
        req.onupgradeneeded  = (e) => {{
            if (!e.target.result.objectStoreNames.contains('user-workflows'))
                e.target.result.createObjectStore('user-workflows', {{ keyPath: 'id' }});
        }};
    }});
}})()
"""

    def _get_active_tab_via_cdp(self, debug_port: int) -> Optional[dict]:
        import requests as _req
        try:
            tabs = _req.get(f"http://localhost:{debug_port}/json", timeout=5).json()
            for tab in tabs:
                if tab.get("type") == "page" and not tab.get("url","").startswith(("chrome://","devtools://")):
                    return tab
            return tabs[0] if tabs else None
        except Exception as exc:
            logger.error(f"_get_active_tab_via_cdp: {exc}"); return None

    def _execute_js_in_tab(self, debug_port: int, tab_id: str, js: str) -> dict:
        import requests as _req, websocket, json as _json
        try:
            tabs   = _req.get(f"http://localhost:{debug_port}/json", timeout=5).json()
            tab    = next((t for t in tabs if t.get("id") == tab_id), None)
            if not tab:
                return {"success": False, "error": f"Tab {tab_id} not found"}
            ws_url = tab.get("webSocketDebuggerUrl")
            if not ws_url:
                return {"success": False, "error": "No WebSocket URL"}
            ws = websocket.create_connection(ws_url, timeout=15)
            ws.send(_json.dumps({"id":1,"method":"Runtime.evaluate","params":{
                "expression": js, "awaitPromise": True,
                "returnByValue": True, "userGesture": True,
            }}))
            raw    = ws.recv(); ws.close()
            result = _json.loads(raw)
            if "error" in result:
                return {"success": False, "error": str(result["error"])}
            rv = result.get("result", {}).get("result", {})
            if rv.get("subtype") == "error":
                return {"success": False, "error": rv.get("description","JS error")}
            return {"success": True, "value": rv.get("value","ok")}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def _do_upload_workflow(self, workflow, debug_port, account_id,
                            mark_uploaded=True, auto_open_automa=False):
        self.log(f"Uploading '{workflow['workflow_name']}' to port {debug_port}")
        st.session_state.generation_in_progress = True
        status = st.empty()
        try:
            d = workflow.get("workflow_data")
            if isinstance(d, str):
                try:    d = json.loads(d)
                except: d = {}
            if not d:
                st.error("❌ Workflow has no data."); return

            status.info("🔍 Connecting to Chrome…")
            tab = self._get_active_tab_via_cdp(debug_port)
            if not tab:
                st.error("❌ Could not reach Chrome."); return

            status.info("📤 Injecting workflow…")
            result = self._execute_js_in_tab(debug_port, tab["id"], self._inject_workflow_js(d))
            status.empty()

            if result["success"]:
                self.log(f"Upload success: {result.get('value','')}")
                st.success(f"✅ **{workflow['workflow_name']}** uploaded!  \nOpen Automa popup → Execute.")
                if mark_uploaded:
                    self._mark_workflow_uploaded(workflow.get("workflow_id"))
                if auto_open_automa:
                    self._execute_js_in_tab(debug_port, tab["id"],
                        "window.open('chrome-extension://infppggnoaenmfagbfknfkancpbljcca/newtab.html','_blank');")
            else:
                err = result.get("error","Unknown")
                self.log(f"Upload failed: {err}", "ERROR")
                st.error(f"❌ Upload failed: {err}  \n\nUse ⬇️ Download and import manually.")
        except Exception as exc:
            status.empty(); self.log(str(exc), "ERROR"); st.error(f"❌ {exc}")
        finally:
            st.session_state.generation_in_progress = False

    def _do_bulk_upload(self, workflows, debug_port, account_id):
        progress = st.progress(0)
        ok = fail = 0
        for idx, wf in enumerate(workflows):
            progress.progress(int((idx+1)/len(workflows)*100))
            d = wf.get("workflow_data")
            if isinstance(d, str):
                try:    d = json.loads(d)
                except: d = {}
            if not d:
                fail += 1; continue
            tab = self._get_active_tab_via_cdp(debug_port)
            if not tab:
                fail += 1; continue
            result = self._execute_js_in_tab(debug_port, tab["id"], self._inject_workflow_js(d))
            if result["success"]:
                ok += 1; self._mark_workflow_uploaded(wf.get("workflow_id"))
            else:
                fail += 1; self.log(f"Bulk fail {wf['workflow_name']}: {result.get('error')}", "WARNING")
        progress.progress(100)
        if ok:
            st.success(f"✅ {ok} uploaded." + (f" ❌ {fail} failed." if fail else ""))
        else:
            st.error("❌ All uploads failed.")

    def _mark_workflow_uploaded(self, workflow_id: Optional[int]):
        if not workflow_id: return
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE workflows SET uploaded_to_chrome=TRUE, uploaded_at=CURRENT_TIMESTAMP WHERE workflow_id=%s", (workflow_id,))
                    conn.commit()
        except Exception as exc:
            logger.error(f"_mark_workflow_uploaded: {exc}")

    # ======================================================================
    # TAB 5 — SCREENING RESULTS
    # ======================================================================

    def _tab_screening_results(self, acct, site):
        st.subheader("🏆 Screening Results")
        results = self._load_screening_results(acct["account_id"], site["site_id"])
        if not results:
            st.info("No screening records yet."); return

        total    = len(results)
        passed   = sum(1 for r in results if r["status"] == "passed")
        failed   = sum(1 for r in results if r["status"] == "failed")
        complete = sum(1 for r in results if r["status"] == "complete")
        pending  = sum(1 for r in results if r["status"] == "pending")

        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Total", total); c2.metric("✅ Passed", passed)
        c3.metric("🏁 Complete", complete); c4.metric("❌ Failed", failed)
        c5.metric("⏳ Pending", pending)

        if total > 0:
            pass_rate = int((passed + complete) / total * 100)
            st.progress(pass_rate / 100, text=f"Pass rate: {pass_rate}%")

        st.markdown("---")
        for r in results:
            icon = {"passed":"✅","failed":"❌","complete":"🏁","pending":"⏳","error":"⚠️"}.get(r["status"],"❓")
            with st.expander(
                f"{icon} **{r.get('survey_name') or 'Unknown Survey'}** — "
                f"{r['status'].upper()} — "
                f"{r['started_at'].strftime('%Y-%m-%d %H:%M') if r.get('started_at') else '?'}",
                expanded=(r["status"] == "pending"),
            ):
                col_info, col_actions = st.columns([3, 1])
                with col_info:
                    st.markdown(
                        f"**Batch ID:** `{r.get('batch_id') or '—'}`  \n"
                        f"**Screener answers:** {r.get('screener_answers', 0)}  \n"
                        f"**Started:** {r['started_at'].strftime('%Y-%m-%d %H:%M') if r.get('started_at') else '—'}  \n"
                        f"**Completed:** {r['completed_at'].strftime('%Y-%m-%d %H:%M') if r.get('completed_at') else '—'}"
                    )
                    if r.get("notes"):
                        st.caption(f"Notes: {r['notes']}")
                with col_actions:
                    rid = r["result_id"]
                    if r["status"] != "passed":
                        if st.button("✅ Mark Passed",   key=f"pass_{rid}", use_container_width=True):
                            self._update_screening_status(rid, "passed"); st.rerun()
                    if r["status"] != "complete":
                        if st.button("🏁 Mark Complete", key=f"comp_{rid}", use_container_width=True):
                            self._update_screening_status(rid, "complete"); st.rerun()
                    if r["status"] != "failed":
                        if st.button("❌ Mark Failed",   key=f"fail_{rid}", use_container_width=True):
                            self._update_screening_status(rid, "failed"); st.rerun()
                    new_note = st.text_input("Note:", key=f"note_{rid}", placeholder="Optional…")
                    if new_note:
                        if st.button("💾 Save Note", key=f"savenote_{rid}", use_container_width=True):
                            self._save_screening_note(rid, new_note); st.rerun()

        st.markdown("---")
        if st.button("📥 Export CSV", key="exp_screening"):
            buf = io.StringIO()
            w   = csv.DictWriter(buf, fieldnames=["survey_name","status","screener_answers",
                                                   "survey_answers","started_at","completed_at","batch_id","notes"])
            w.writeheader()
            for r in results:
                w.writerow({"survey_name": r.get("survey_name",""), "status": r.get("status",""),
                             "screener_answers": r.get("screener_answers",0), "survey_answers": r.get("survey_answers",0),
                             "started_at": str(r.get("started_at","")), "completed_at": str(r.get("completed_at","")),
                             "batch_id": r.get("batch_id",""), "notes": r.get("notes","")})
            st.download_button("⬇️ Download CSV", data=buf.getvalue(),
                               file_name=f"screening_{acct['username']}_{site['site_name'].replace(' ','_')}.csv",
                               mime="text/csv", key="dl_screening_csv")

    # ======================================================================
    # Results renderer
    # ======================================================================

    def _render_results(self, r: Dict):
        action = r.get("action", "?")
        titles = {
            "extract_questions":   "Extract Results",
            "extract_all_surveys": "Extract All Surveys Results",
            "answer_questions":    "Answer Results (Gemini AI)",
            "create_workflows":    "Workflow Creation Results",
        }
        st.subheader(f"✅ {titles.get(action, action)}")

        if r.get("status") == "failed":
            st.error(f"❌ {r.get('error', 'Unknown error')}")
            diag = r.get("diag_events", [])
            if diag:
                with st.expander("🔬 Diagnostic log (failure details)", expanded=True):
                    _render_diag_panel(diag)
            if st.button("Clear", key="clr_fail"):
                st.session_state.generation_results = None; st.rerun()
            return

        c1,c2,c3,c4 = st.columns(4)
        with c1:
            if action in ("extract_questions","extract_all_surveys"):
                st.metric("Questions extracted", r.get("questions_extracted",0))
            elif action == "answer_questions":
                st.metric("✅ Answers saved", r.get("answers_generated",0))
            elif action == "create_workflows":
                st.metric("Workflows created", r.get("workflows_created",0))
        with c2: st.metric("Account", r["account"]["username"])
        with c3: st.metric("Site",    r["site"]["name"])
        with c4: st.metric("Time",    f"{r.get('execution_time_seconds',0)}s")

        if r.get("survey_name"):
            st.info(f"📋 Survey: **{r['survey_name']}**")

        if action == "extract_questions":
            st.json({"batch_id": r.get("batch_id"), "inserted": r.get("inserted",0),
                     "survey": r.get("survey_name",""), "url": r.get("url",{}).get("url","")})

        elif action == "extract_all_surveys":
            c_a,c_b,c_c,c_d,c_e = st.columns(5)
            c_a.metric("Surveys found",     r.get("surveys_found",0))
            c_b.metric("✅ Successful",      r.get("surveys_successful",0))
            c_c.metric("❌ Failed/DQ",       r.get("surveys_failed",0))
            c_d.metric("Qs inserted",        r.get("inserted",0))
            c_e.metric("Source",             r.get("discovery_source","?").upper())
            if r.get("network_api_calls"):
                st.caption(f"Network API responses captured: {r['network_api_calls']}")

            survey_results = r.get("survey_results",[])
            if survey_results:
                with st.expander("📋 Per-survey breakdown", expanded=True):
                    for sr in survey_results:
                        icon = {"success":"✅","dq":"❌","error":"⚠️","skip":"⏭️"}.get(sr["status"],"❓")
                        st.markdown(
                            f"{icon} **{sr.get('survey_name') or sr.get('survey_label','?')}** — "
                            f"`{sr['status']}` — {sr.get('questions',0)} Qs, "
                            f"{sr.get('inserted',0)} inserted — provider: `{sr.get('provider','?')}`"
                        )
                        if sr.get("reason"):
                            st.caption(f"  Reason: {sr['reason']}")

            diag = r.get("diag_events",[])
            if diag:
                with st.expander("🔬 Full diagnostic log", expanded=False):
                    _render_diag_panel(diag)

        elif action == "answer_questions":
            answered = r.get("answers_generated",0); failed = r.get("answers_failed",0)
            col_a,col_b,col_c = st.columns(3)
            with col_a: st.metric("✅ Saved",  answered)
            with col_b: st.metric("❌ Failed", failed)
            with col_c:
                total = answered + failed
                st.metric("Success rate", f"{int(answered/total*100) if total else 0}%")
            if r.get("batch_id"):
                st.caption(f"Batch: `{r['batch_id']}`")
            details = r.get("details",[])
            if details:
                with st.expander(f"📋 Answer details ({len(details)} questions)", expanded=True):
                    for d in details:
                        icon  = "✅" if d["status"] == "success" else "❌"
                        badge = f"`{d.get('answer','—')}`" if d["status"] == "success" else f"*{d.get('error',d['status'])}*"
                        st.markdown(f"{icon} **Q:** {d['question_text'][:90]}" + ("…" if len(d['question_text'])>90 else ""))
                        st.markdown(f"&nbsp;&nbsp;&nbsp;&nbsp;→ {badge}")
                        st.divider()

        elif action == "create_workflows" and "workflows" in r:
            for i, wf in enumerate(r["workflows"]):
                with st.expander(f"📋 {wf.get('workflow_name',f'WF {i+1}')}", expanded=(i==0)):
                    st.caption(f"Questions baked in: {wf.get('batch_size',0)} | Answers embedded: {wf.get('answers_baked',0)}")
                    d = wf.get("workflow_data",{})
                    st.download_button("⬇️ Download", data=json.dumps(d,indent=2),
                                       file_name=f"{wf.get('workflow_name','workflow')}.json",
                                       mime="application/json", key=f"dl_wf_{i}")

        if st.button("Clear results", key="clr_res"):
            st.session_state.generation_results = None; st.rerun()

    # ======================================================================
    # DB helpers
    # ======================================================================

    def _pg(self):
        return get_postgres_connection()

    def _get_urls(self, account_id, site_id):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("SELECT url_id,url,is_default,is_used,used_at,notes FROM account_urls WHERE account_id=%s AND site_id=%s ORDER BY is_default DESC, created_at DESC", (account_id,site_id))
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_get_urls: {e}"); return []

    def _mark_url_used(self, url_id):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE account_urls SET is_used=TRUE,used_at=CURRENT_TIMESTAMP WHERE url_id=%s",(url_id,)); conn.commit()
        except Exception as e:
            logger.error(f"_mark_url_used: {e}")

    def _get_survey_names(self, account_id: int, site_id: int) -> List[str]:
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("SELECT DISTINCT survey_name FROM questions WHERE account_id=%s AND survey_site_id=%s AND (used_in_workflow IS NULL OR used_in_workflow=FALSE) AND is_active=TRUE AND survey_name IS NOT NULL ORDER BY survey_name",(account_id,site_id))
                    return [row[0] for row in c.fetchall()]
        except Exception as e:
            logger.error(f"_get_survey_names: {e}"); return []

    def _get_all_survey_names_with_workflows(self, account_id: int, site_id: int) -> List[str]:
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("SELECT DISTINCT q.survey_name FROM workflows w JOIN questions q ON w.question_id=q.question_id WHERE w.account_id=%s AND w.site_id=%s AND w.is_active=TRUE AND q.survey_name IS NOT NULL ORDER BY q.survey_name",(account_id,site_id))
                    rows = [row[0] for row in c.fetchall()]
                    if rows: return rows
                    c.execute("SELECT DISTINCT workflow_name FROM workflows WHERE account_id=%s AND site_id=%s AND is_active=TRUE ORDER BY workflow_name",(account_id,site_id))
                    return [row[0] for row in c.fetchall()]
        except Exception as e:
            logger.error(f"_get_all_survey_names_with_workflows: {e}"); return []

    def _unused_questions(self, account_id: int, site_id: int, survey_name: Optional[str]=None) -> List[Dict]:
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    if survey_name:
                        c.execute("SELECT question_id,question_text,question_type,click_element,options,required,question_category,input_element,submit_element,survey_name FROM questions WHERE account_id=%s AND survey_site_id=%s AND (used_in_workflow IS NULL OR used_in_workflow=FALSE) AND is_active=TRUE AND survey_name=%s ORDER BY order_index, extracted_at DESC",(account_id,site_id,survey_name))
                    else:
                        c.execute("SELECT question_id,question_text,question_type,click_element,options,required,question_category,input_element,submit_element,survey_name FROM questions WHERE account_id=%s AND survey_site_id=%s AND (used_in_workflow IS NULL OR used_in_workflow=FALSE) AND is_active=TRUE ORDER BY survey_name, order_index, extracted_at DESC",(account_id,site_id))
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_unused_questions: {e}"); return []

    def _mark_survey_complete(self, account_id: int, site_id: int, survey_name: str):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE questions SET survey_complete=TRUE, survey_completed_at=CURRENT_TIMESTAMP WHERE account_id=%s AND survey_site_id=%s AND survey_name=%s",(account_id,site_id,survey_name)); conn.commit()
        except Exception as e:
            logger.error(f"_mark_survey_complete: {e}")

    def _get_workflows_for_survey(self, account_id: int, site_id: int, survey_name: str) -> List[Dict]:
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("SELECT w.workflow_id,w.workflow_name,w.workflow_data,w.created_time,w.question_id,w.uploaded_to_chrome FROM workflows w LEFT JOIN questions q ON w.question_id=q.question_id WHERE w.account_id=%s AND w.site_id=%s AND w.is_active=TRUE AND (q.survey_name=%s OR w.workflow_name ILIKE %s) ORDER BY w.created_time DESC",(account_id,site_id,survey_name,f"%{survey_name}%"))
                    rows = [dict(r) for r in c.fetchall()]
                    if rows: return rows
                    c.execute("SELECT workflow_id,workflow_name,workflow_data,created_time,question_id,uploaded_to_chrome FROM workflows WHERE account_id=%s AND site_id=%s AND is_active=TRUE ORDER BY created_time DESC",(account_id,site_id))
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_get_workflows_for_survey: {e}"); return []

    def _upsert_screening_result(self, account_id, site_id, survey_name, batch_id, screener_answers, status="pending"):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("INSERT INTO screening_results (account_id,site_id,survey_name,batch_id,screener_answers,status,started_at) VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)",(account_id,site_id,survey_name,batch_id,screener_answers,status)); conn.commit()
        except Exception as e:
            logger.error(f"_upsert_screening_result: {e}")

    def _load_screening_results(self, account_id, site_id) -> List[Dict]:
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("SELECT result_id,survey_name,batch_id,status,screener_answers,survey_answers,started_at,completed_at,notes FROM screening_results WHERE account_id=%s AND site_id=%s ORDER BY started_at DESC",(account_id,site_id))
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_screening_results: {e}"); return []

    def _update_screening_status(self, result_id, status):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    completed_sql = "CURRENT_TIMESTAMP" if status in ("complete","failed") else "NULL"
                    c.execute(f"UPDATE screening_results SET status=%s, completed_at={completed_sql} WHERE result_id=%s",(status,result_id)); conn.commit()
        except Exception as e:
            logger.error(f"_update_screening_status: {e}")

    def _save_screening_note(self, result_id, note):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE screening_results SET notes=%s WHERE result_id=%s",(note,result_id)); conn.commit()
        except Exception as e:
            logger.error(f"_save_screening_note: {e}")

    def _load_accounts(self):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("SELECT account_id,username,country,profile_id,age,gender,city,education_level,job_status,income_range,marital_status,has_children,household_size,industry,email,phone FROM accounts ORDER BY username")
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_accounts: {e}"); return []

    def _load_survey_sites(self):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("SELECT site_id,site_name,description FROM survey_sites ORDER BY site_name")
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_survey_sites: {e}"); return []

    def _load_prompts(self):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("SELECT prompt_id,account_id,name AS prompt_name,content,prompt_type FROM prompts WHERE is_active=TRUE")
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_prompts: {e}"); return []