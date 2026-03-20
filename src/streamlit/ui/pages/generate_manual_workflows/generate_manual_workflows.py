# src/streamlit/ui/pages/generate_manual_workflows/generate_manual_workflows.py
"""
Generate Manual Workflows — Streamlit page
"""

import json
import logging
import random
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import streamlit as st
from psycopg2.extras import RealDictCursor

from src.core.database.postgres.connection import get_postgres_connection
from .orchestrator import SurveySiteOrchestrator
from .utils.chrome_helpers import (
    ensure_chrome_running,
    get_debug_port_for_account,
)

logger = logging.getLogger(__name__)


class GenerateManualWorkflowsPage:

    def __init__(self, db_manager):
        self.db_manager  = db_manager
        self.orchestrator = SurveySiteOrchestrator(db_manager)

        for k, v in {
            "generation_in_progress": False,
            "generation_results":     None,
            "generation_logs":        [],
            "selected_action":        None,
        }.items():
            if k not in st.session_state:
                st.session_state[k] = v

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------

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
        🔍 <b>Extract Questions</b> &amp; ⚙️ <b>Create Workflows</b> use site-specific modules.<br>
        📝 <b>Answer Questions</b> &amp; 📥 <b>Download Workflows</b> are demo versions.<br>
        Workflows are exported as Automa JSON — import them directly into the Automa extension.
        </p>
        </div>""", unsafe_allow_html=True)

        accounts      = self._load_accounts()
        survey_sites  = self._load_survey_sites()
        prompts       = self._load_prompts()
        avail_sites   = self.orchestrator.get_available_sites()

        # ---- Debug panel when no sites are found ----
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
                base    = Path(__file__).parent
                ext_dir = base / "extractors"
                cre_dir = base / "workflow_creators"

                st.write("**Expected extractor dir:**", str(ext_dir))
                st.write("Exists:", ext_dir.exists())
                if ext_dir.exists():
                    st.write("Files found:", [f.name for f in ext_dir.glob("*.py")])

                st.write("**Expected creator dir:**", str(cre_dir))
                st.write("Exists:", cre_dir.exists())
                if cre_dir.exists():
                    st.write("Files found:", [f.name for f in cre_dir.glob("*.py")])

                st.info(
                    "Required layout:\n"
                    "```\n"
                    "generate_manual_workflows/\n"
                    "  base/\n"
                    "    base_extractor.py\n"
                    "    base_workflow_creator.py\n"
                    "  extractors/\n"
                    "    __init__.py\n"
                    "    topsurveys_extractor.py   ← site_name = 'Top Surveys'\n"
                    "  workflow_creators/\n"
                    "    __init__.py\n"
                    "    topsurveys_workflow.py    ← site_name = 'Top Surveys'\n"
                    "```\n"
                    "The DB survey_sites.site_name must **exactly** match "
                    "the site_name returned by get_site_info()."
                )

                if st.button("🔄 Reload modules", key="reload_modules"):
                    self.orchestrator._load_modules()
                    st.rerun()
            return

        if not accounts:
            st.warning("⚠️ No accounts found.")
            return

        avail_names = {s["site_name"] for s in avail_sites}

        # ---- Account selector ----
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("👤 Account")
            acc_opts = {}
            for a in accounts:
                has_p = any(p["account_id"] == a["account_id"] for p in prompts)
                acc_opts[f"{'✅' if has_p else '❌'} {a['username']} (ID:{a['account_id']})"] = a
            acct = acc_opts[st.selectbox("Account:", list(acc_opts), key="wf_acct")]

            acct_prompt = next((p for p in prompts if p["account_id"] == acct["account_id"]), None)
            if acct_prompt:
                st.success(f"✅ Prompt: {acct_prompt['prompt_name']}")
            else:
                st.warning("⚠️ No prompt — create one in Prompts page")

            debug_port = get_debug_port_for_account(st.session_state, acct["account_id"])
            if debug_port:
                st.success(f"🟢 Chrome active (port {debug_port})")
            else:
                st.info("⚪ No Chrome session — extraction will simulate")

        # ---- Survey site selector ----
        with col2:
            st.subheader("🌐 Survey Site")
            db_sites = [s for s in survey_sites if s["site_name"] in avail_names]
            if not db_sites:
                st.error(
                    f"No DB sites match loaded module names.\n"
                    f"Module names: {sorted(avail_names)}\n"
                    f"Add a survey site with the exact same name in Accounts → Survey Sites."
                )
                return

            site_opts = {s["site_name"]: s for s in db_sites}
            site = site_opts[st.selectbox("Survey Site:", list(site_opts), key="wf_site")]
            si   = next((s for s in avail_sites if s["site_name"] == site["site_name"]), {})
            st.caption(
                f"Extractor v{si.get('extractor_version','?')} | "
                f"Creator v{si.get('creator_version','?')}"
            )

        st.markdown("---")

        # ---- Tabs ----
        t1, t2, t3, t4 = st.tabs([
            "🔍 Extract Questions",
            "📝 Answer Questions (Demo)",
            "⚙️ Create Workflows",
            "📥 Download Workflows",
        ])
        with t1: self._tab_extract(acct, site, acct_prompt)
        with t2: self._tab_answer(acct, site, acct_prompt)
        with t3: self._tab_create(acct, site, acct_prompt)
        with t4: self._tab_download(acct, site)

        # ---- Shared log viewer ----
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
                        f"logs_{datetime.now():%Y%m%d_%H%M%S}.txt"
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
            f"**Chrome:** {'🟢 Port ' + str(debug_port) + ' — live extraction' if debug_port else '⚪ No session — simulation'}  \n"
            f"**Prompt:** {prompt['prompt_name'] if prompt else '—'}"
        )

        if not prompt:
            st.error("❌ Account has no prompt")
            return

        urls = self._get_urls(acct["account_id"], site["site_id"])
        if not urls:
            st.warning("⚠️ No URLs configured for this account/site (Accounts → Survey Sites).")
            return

        url_map = {}
        for u in urls:
            star = "⭐ " if u.get("is_default") else ""
            used = " [used]" if u.get("is_used") else ""
            lbl  = f"{star}{u['url']}{used}"
            url_map[lbl] = u
        sel_url = url_map[st.selectbox("URL:", list(url_map), key="ext_url")]

        if sel_url.get("is_used"):
            st.warning("⚠️ URL already used — you can still proceed.")

        use_chrome = st.checkbox("Use running Chrome session", bool(debug_port), key="ext_chrome")

        if st.button("🚀 Extract", type="primary", use_container_width=True, key="ext_btn"):
            self._do_extract(acct, site, prompt, sel_url, use_chrome)

    def _do_extract(self, acct, site, prompt, url_info, use_chrome):
        self.log(f"Extraction start: {acct['username']} / {site['site_name']}")
        st.session_state.generation_in_progress = True

        try:
            from src.streamlit.ui.pages.accounts.chrome_session_manager import ChromeSessionManager
            profile_path = ChromeSessionManager(self.db_manager).get_profile_path(acct["username"])

            debug_port = get_debug_port_for_account(st.session_state, acct["account_id"])
            if use_chrome and not debug_port:
                ok = ensure_chrome_running(profile_path)
                debug_port = get_debug_port_for_account(st.session_state, acct["account_id"]) if ok else None
                self.log(f"Chrome start: {'ok port ' + str(debug_port) if debug_port else 'failed → simulation'}")

            result = self.orchestrator.extract_questions(
                account_id=acct["account_id"], site_id=site["site_id"],
                url=url_info["url"], profile_path=profile_path,
                site_name=site["site_name"],
                debug_port=debug_port,
            )

            if result.get("success"):
                self._mark_url_used(url_info["url_id"])
                self.log(f"Done: {result['questions_found']} found, {result['inserted']} inserted")
                st.session_state.generation_results = {
                    "action": "extract_questions", "status": "success",
                    "timestamp": datetime.now().isoformat(),
                    "account": {"id": acct["account_id"], "username": acct["username"]},
                    "site":    {"id": site["site_id"],    "name": site["site_name"]},
                    "url":     {"url": url_info["url"]},
                    "questions_extracted": result["questions_found"],
                    "inserted":            result["inserted"],
                    "batch_id":            result["batch_id"],
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
                "action": "extract_questions", "status": "failed",
                "error": str(exc),
                "account": {"id": acct["account_id"], "username": acct["username"]},
                "site":    {"id": site["site_id"],    "name": site["site_name"]},
                "timestamp": datetime.now().isoformat(),
            }
        finally:
            st.session_state.generation_in_progress = False
            st.rerun()

    # ======================================================================
    # TAB 2 — ANSWER (Demo)
    # ======================================================================

    def _tab_answer(self, acct, site, prompt):
        st.subheader("📝 Answer Questions")

        if not prompt:
            st.error("❌ No prompt — create one in the Prompts page")
            return

        qs = self._unused_questions(acct["account_id"], site["site_id"])
        if not qs:
            st.info("No questions yet — run Extract first.")
            return

        st.success(f"✅ {len(qs)} questions to answer")

        # Show questions and how they will be answered based on the prompt
        with st.expander("📋 Preview answers based on your prompt", expanded=True):
            from src.streamlit.ui.pages.generate_manual_workflows.base.base_workflow_creator import BaseWorkflowCreator

            # Build a minimal persona from the prompt + account fields
            enriched = dict(prompt)
            for f in ("age", "gender", "city", "education_level", "job_status",
                    "income_range", "marital_status", "has_children",
                    "household_size", "username", "email", "phone"):
                if acct.get(f) is not None:
                    enriched.setdefault(f, acct[f])

            # Use the site's workflow creator to parse persona and determine answers
            creator = self.orchestrator.workflow_creators.get(site["site_name"])
            if creator:
                persona = creator.parse_persona(enriched)
            else:
                persona = enriched

            st.caption(f"**Persona:** {', '.join(f'{k}={v}' for k, v in persona.items() if v and k not in ('content', 'prompt_type', 'prompt_id', 'prompt_name'))}")
            st.markdown("---")

            for i, q in enumerate(qs):
                col1, col2 = st.columns([2, 1])
                with col1:
                    st.write(f"**Q{i+1}.** {q['question_text']}")
                    st.caption(f"Type: `{q['question_type']}` | Category: `{q.get('question_category', '—')}`")
                with col2:
                    if creator:
                        answer = creator.best_answer(q, persona)
                        if answer:
                            st.success(f"→ **{answer}**")
                        else:
                            options = q.get("options") or []
                            if isinstance(options, str):
                                try:
                                    import json
                                    options = json.loads(options)
                                except Exception:
                                    options = []
                            if options:
                                mid = options[len(options) // 2]
                                st.info(f"→ {mid} *(mid option)*")
                            else:
                                st.info("→ *(text/open)*")
                    else:
                        st.warning("No creator loaded")
                st.divider()

        if st.button("🚀 Generate Answers", type="primary", use_container_width=True, key="ans_btn"):
            st.session_state.generation_results = {
                "action":              "answer_questions",
                "status":              "success",
                "timestamp":           datetime.now().isoformat(),
                "account":             {"id": acct["account_id"], "username": acct["username"]},
                "site":                {"id": site["site_id"],    "name": site["site_name"]},
                "answers_generated":   len(qs),
                "execution_time_seconds": 0,
                "is_demo":             True,
            }
            st.rerun()

    # ======================================================================
    # TAB 3 — CREATE WORKFLOWS
    # ======================================================================

    def _tab_create(self, acct, site, prompt):
        st.subheader("⚙️ Create Automa Workflows")

        ci = {}
        if site["site_name"] in self.orchestrator.workflow_creators:
            ci = self.orchestrator.workflow_creators[site["site_name"]].get_site_info()

        urls = self._get_urls(acct["account_id"], site["site_id"])
        survey_url = urls[0]["url"] if urls else ""

        st.info(
            f"**Account:** {acct['username']}  \n"
            f"**Site:** {site['site_name']}  \n"
            f"**Creator:** {ci.get('template_name','Standard')} v{ci.get('version','?')}  \n"
            f"**Survey URL baked in:** {survey_url[:60] + '...' if survey_url else '— none set'}  \n"
            f"**Prompt:** {prompt['prompt_name'] if prompt else '— none (using defaults)'}"
        )

        qs = self._unused_questions(acct["account_id"], site["site_id"])
        if not qs:
            st.info("No unused questions — run Extract first."); return

        st.success(f"✅ {len(qs)} questions available")
        with st.expander("Preview questions"):
            for q in qs[:8]:
                st.markdown(f"- {q['question_text'][:90]}… ({q['question_type']})")
            if len(qs) > 8:
                st.caption(f"… and {len(qs)-8} more")

        c1, c2 = st.columns(2)
        with c1: n_wf = st.number_input("Workflows to create:", 1, min(10,len(qs)), min(3,len(qs)), key="cr_n")
        with c2: st.caption("Each workflow = one screener batch + DQ handling")

        with st.expander("Advanced"):
            inc_click = st.checkbox("Include click selectors", True, key="cr_click")
            inc_input = st.checkbox("Include input selectors", True, key="cr_input")

        if st.button("🚀 Create Workflows", type="primary", use_container_width=True, key="cr_btn"):
            self._do_create(acct, site, prompt, n_wf, qs, survey_url, inc_click, inc_input)

    def _do_create(self, acct, site, prompt, n_wf, qs, survey_url, inc_click, inc_input):
        self.log(f"Creating {n_wf} workflows: {acct['username']} / {site['site_name']}")
        st.session_state.generation_in_progress = True

        # Merge account demographic data into prompt so persona parser can use it
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
                self.log(f"Created {result['workflows_created']} workflows")
                st.session_state.generation_results = {
                    "action":            "create_workflows",
                    "status":            "success",
                    "timestamp":         datetime.now().isoformat(),
                    "account":           {"id": acct["account_id"], "username": acct["username"]},
                    "site":              {"id": site["site_id"],    "name": site["site_name"]},
                    "workflows_created": result["workflows_created"],
                    "workflows":         result["workflows"],
                    "inserted":          result["inserted"],
                    "batch_id":          result["batch_id"],
                    "execution_time_seconds": result.get("execution_time_seconds",0),
                }
            else:
                self.log(result.get("error","?"), "ERROR")
                st.session_state.generation_results = {
                    "action":"create_workflows","status":"failed",
                    "error": result.get("error","Unknown"),
                    "account":{"id":acct["account_id"],"username":acct["username"]},
                    "site":   {"id":site["site_id"],   "name":site["site_name"]},
                    "timestamp":datetime.now().isoformat(),
                }
        except Exception as exc:
            self.log(str(exc), "ERROR")
            st.session_state.generation_results = {
                "action":"create_workflows","status":"failed","error":str(exc),
                "account":{"id":acct["account_id"],"username":acct["username"]},
                "site":   {"id":site["site_id"],   "name":site["site_name"]},
                "timestamp":datetime.now().isoformat(),
            }
        finally:
            st.session_state.generation_in_progress = False
            st.rerun()

    # ======================================================================
    # TAB 4 — DOWNLOAD
    # ======================================================================

    def _tab_download(self, acct, site):
        st.subheader("📥 Download Workflows as Automa JSON")

        wfs = self._get_workflows(acct["account_id"], site["site_id"])
        if not wfs:
            st.info("No workflows yet — create some in Tab 3."); return

        st.success(f"✅ {len(wfs)} workflows")

        with st.expander("Preview"):
            for w in wfs[:5]:
                dt = w["created_time"].strftime("%Y-%m-%d") if w.get("created_time") else "?"
                st.markdown(f"- **{w['workflow_name']}** ({dt})")
            if len(wfs) > 5:
                st.caption(f"… and {len(wfs)-5} more")

        n = st.slider("Include:", 1, min(20, len(wfs)), min(5, len(wfs)), key="dl_n")
        inc_manifest = st.checkbox("Add manifest header", True, key="dl_manifest")

        if st.button("📦 Build package", type="primary", use_container_width=True, key="dl_btn"):
            pkg = self._build_package(wfs[:n], acct, site, inc_manifest)
            fname = f"{acct['username']}_{site['site_name'].replace(' ','_')}_automa.json"
            st.download_button(
                "⬇️ Download Automa JSON",
                data=json.dumps(pkg, indent=2),
                file_name=fname,
                mime="application/json",
                key="dl_json",
            )
            st.success(f"✅ {n} workflows ready to import into Automa")

    def _build_package(self, wfs, acct, site, manifest):
        items = []
        for w in wfs:
            d = w.get("workflow_data")
            if isinstance(d, str):
                try:    d = json.loads(d)
                except: d = {}
            items.append(d or {"name": w["workflow_name"]})

        pkg: Dict = {"workflows": items}
        if manifest:
            pkg["manifest"] = {
                "generated_at":   datetime.now().isoformat(),
                "account":        acct["username"],
                "site":           site["site_name"],
                "workflow_count": len(items),
                "format":         "automa-import-v1",
                "notes":          "Import via Automa → Workflows → Import",
            }
        return pkg

    # ======================================================================
    # Results
    # ======================================================================

    def _render_results(self, r: Dict):
        action = r.get("action","?")
        titles = {
            "extract_questions": "Extract Results",
            "answer_questions":  "Answer Results (Demo)",
            "create_workflows":  "Workflow Creation Results",
        }
        st.subheader(f"✅ {titles.get(action, action)}")

        if r.get("status") == "failed":
            st.error(f"❌ {r.get('error','Unknown error')}")
            if st.button("Clear", key="clr_fail"):
                st.session_state.generation_results = None; st.rerun()
            return

        c1,c2,c3,c4 = st.columns(4)
        with c1:
            if action == "extract_questions":
                st.metric("Extracted", r.get("questions_extracted",0))
            elif action == "answer_questions":
                st.metric("Answered", r.get("answers_generated",0))
            elif action == "create_workflows":
                st.metric("Workflows", r.get("workflows_created",0))
        with c2: st.metric("Account", r["account"]["username"])
        with c3: st.metric("Site",    r["site"]["name"])
        with c4: st.metric("Time",   f"{r.get('execution_time_seconds',0)}s")

        if action == "extract_questions":
            st.json({"batch_id": r.get("batch_id"), "inserted": r.get("inserted",0),
                     "url": r.get("url",{}).get("url","")})

        elif action == "create_workflows" and "workflows" in r:
            for i, wf in enumerate(r["workflows"]):
                with st.expander(f"📋 {wf.get('workflow_name',f'WF {i+1}')}", expanded=(i==0)):
                    st.caption(f"Questions in batch: {wf.get('batch_size',1)}")
                    if wf.get("question_text"):
                        st.caption(f"First Q: {wf['question_text'][:100]}…")
                    d = wf.get("workflow_data",{})
                    st.download_button(
                        "⬇️ Download this workflow",
                        data=json.dumps(d, indent=2),
                        file_name=f"{wf.get('workflow_name','workflow')}.json",
                        mime="application/json",
                        key=f"dl_wf_{i}",
                    )

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
                    c.execute("""
                        SELECT url_id,url,is_default,is_used,used_at,notes
                        FROM account_urls
                        WHERE account_id=%s AND site_id=%s
                        ORDER BY is_default DESC, created_at DESC
                    """, (account_id, site_id))
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_get_urls: {e}"); return []

    def _mark_url_used(self, url_id):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE account_urls SET is_used=TRUE,used_at=CURRENT_TIMESTAMP WHERE url_id=%s",
                              (url_id,))
                    conn.commit()
        except Exception as e:
            logger.error(f"_mark_url_used: {e}")

    def _unused_questions(self, account_id, site_id):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("""
                        SELECT question_id,question_text,question_type,click_element,
                               options,required,question_category,input_element,submit_element
                        FROM questions
                        WHERE account_id=%s AND survey_site_id=%s
                          AND (used_in_workflow IS NULL OR used_in_workflow=FALSE)
                          AND is_active=TRUE
                        ORDER BY extracted_at DESC
                    """, (account_id, site_id))
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_unused_questions: {e}"); return []

    def _get_workflows(self, account_id, site_id):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("""
                        SELECT workflow_id,workflow_name,workflow_data,created_time,question_id
                        FROM workflows
                        WHERE account_id=%s AND site_id=%s AND is_active=TRUE
                        ORDER BY created_time DESC
                    """, (account_id, site_id))
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_get_workflows: {e}"); return []

    def _load_accounts(self):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("""
                        SELECT account_id,username,country,profile_id,
                               age,gender,city,education_level,job_status,
                               income_range,marital_status,has_children,
                               household_size,industry,email,phone
                        FROM accounts ORDER BY username
                    """)
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
                    c.execute("""
                        SELECT prompt_id,account_id,name AS prompt_name,content,prompt_type
                        FROM prompts WHERE is_active=TRUE
                    """)
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_prompts: {e}"); return []