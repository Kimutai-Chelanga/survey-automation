# src/streamlit/ui/pages/generate_manual_workflows/generate_manual_workflows.py
"""
Generate Manual Workflows — Streamlit page v3.0.0
Changes over v2:
  - Removed extraction, workflow creation, and upload tabs
  - Added direct AI answering with browser-use
  - Simplified to just answer questions using AI agent
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

# Import browser-use
from browser_use import Agent, Browser
from browser_use import ChatOpenAI, ChatAnthropic, ChatGoogle
import asyncio

logger = logging.getLogger(__name__)


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

    def render(self):
        st.title("🤖 AI Survey Answerer")
        
        st.markdown("""
        <div style='background:#1e3a5f;padding:18px;border-radius:10px;margin-bottom:18px;'>
        <h3 style='color:white;margin:0;'>AI-Powered Survey Answering</h3>
        <p style='color:#a0c4ff;margin:8px 0 0;'>
        🤖 <b>AI Agent</b> — Uses browser-use to automatically navigate and answer surveys.<br>
        📝 <b>LLM</b> — Generates answers based on your persona prompt.<br>
        🏆 <b>Results</b> — Track pass/fail per survey attempt.
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
                with st.expander("👁️ View persona prompt", expanded=False):
                    st.code(acct_prompt["content"], language=None)
            else:
                st.warning("⚠️ No prompt — create one in Prompts page")
            debug_port = get_debug_port_for_account(st.session_state, acct["account_id"])
            if debug_port:
                st.success(f"🟢 Chrome active (port {debug_port})")
            else:
                st.info("⚪ No Chrome session - will start one automatically")

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

        # Direct AI answering tab
        self._tab_answer_direct(acct, site, acct_prompt)

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
            
        # Screening results tab
        st.markdown("---")
        self._tab_screening_results(acct, site)

    # ======================================================================
    # Direct AI Answering with browser-use
    # ======================================================================

    def _tab_answer_direct(self, acct, site, prompt):
        st.subheader("🤖 AI Survey Answerer")
        
        if not prompt:
            st.error("❌ No prompt — create one in the Prompts page first.")
            return

        import os
        
        # Check for LLM API keys
        has_openai = bool(os.environ.get("OPENAI_API_KEY"))
        has_anthropic = bool(os.environ.get("ANTHROPIC_API_KEY"))
        has_gemini = bool(os.environ.get("GEMINI_API_KEY"))
        
        if not (has_openai or has_anthropic or has_gemini):
            st.error("❌ No LLM API key found. Add OPENAI_API_KEY, ANTHROPIC_API_KEY, or GEMINI_API_KEY to .env")
            return

        # Get URLs for this account/site
        urls = self._get_urls(acct["account_id"], site["site_id"])
        if not urls:
            st.warning("⚠️ No URLs configured for this account/site.")
            return

        url_map = {}
        for u in urls:
            star = "⭐ " if u.get("is_default") else ""
            used = " [used]" if u.get("is_used") else ""
            url_map[f"{star}{u['url']}{used}"] = u
            
        selected_url = st.selectbox(
            "Dashboard / Survey URL to start from:",
            list(url_map),
            key="answer_url"
        )
        survey_url = url_map[selected_url]["url"].strip()
        
        # Ensure URL has scheme
        if survey_url and not survey_url.startswith(("http://", "https://")):
            survey_url = "https://" + survey_url
            st.info(f"URL normalized to: {survey_url}")
            
        # Number of surveys to answer
        num_surveys = st.number_input(
            "Number of surveys to answer:",
            min_value=1,
            max_value=50,
            value=5,
            key="num_surveys"
        )
        
        # Model selection
        model_options = []
        if has_openai:
            model_options.append("openai (GPT-4o)")
        if has_anthropic:
            model_options.append("anthropic (Claude)")
        if has_gemini:
            model_options.append("gemini (Gemini)")
            
        model_choice = st.selectbox(
            "AI Model:",
            model_options,
            key="model_choice"
        )
        
        st.info(
            f"**Account:** {acct['username']}  \n"
            f"**Site:** {site['site_name']}  \n"
            f"**Starting URL:** {survey_url}  \n"
            f"**Surveys to attempt:** {num_surveys}  \n"
            f"**Model:** {model_choice}  \n"
            f"**Prompt:** {prompt['prompt_name']}"
        )
        
        # Advanced options
        with st.expander("⚙️ Advanced Options"):
            headless = st.checkbox("Run headless (no visible browser)", value=False, key="headless")
            use_cloud = st.checkbox("Use Browser-Use Cloud (stealth mode)", value=False, key="use_cloud")
            max_steps = st.number_input("Max steps per survey:", min_value=10, max_value=200, value=50, key="max_steps")
            
        # Answer button
        if st.button(
            f"🚀 Answer {num_surveys} Survey(s) with AI Agent",
            type="primary",
            use_container_width=True,
            key="answer_btn",
            disabled=st.session_state.get("generation_in_progress", False),
        ):
            self._do_direct_answering(
                acct, site, prompt, survey_url, num_surveys,
                model_choice, headless, use_cloud, max_steps
            )
            
    def _do_direct_answering(
        self, acct, site, prompt, start_url, num_surveys,
        model_choice, headless, use_cloud, max_steps
    ):
        """Execute direct survey answering using browser-use agent"""
        self.log(f"Starting AI answering: {acct['username']} / {site['site_name']}")
        st.session_state.generation_in_progress = True
        
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        # Build persona instruction for the agent
        persona = self._build_persona_instruction(prompt, acct)
        
        # Build task instruction
        task = f"""
        You are a survey participant with the following persona:
        {persona}
        
        Your task:
        1. Go to {start_url}
        2. Find and click on available surveys (up to {num_surveys} surveys)
        3. For each survey, answer all questions truthfully according to your persona
        4. Handle any disqualification (DQ) pages appropriately
        5. Submit each survey and continue to the next one
        6. Track which surveys you completed successfully
        
        Important:
        - Be patient and wait for pages to load
        - If you see a survey that asks for information inconsistent with your persona, still answer truthfully
        - If you get disqualified, note that and move to the next survey
        - Complete the survey fully before moving on
        """
        
        try:
            # Setup browser
            browser = Browser(
                headless=headless,
                use_cloud=use_cloud,
            )
            
            # Setup LLM based on choice
            if "openai" in model_choice.lower():
                llm = ChatOpenAI(model='gpt-4o')
            elif "anthropic" in model_choice.lower() or "claude" in model_choice.lower():
                llm = ChatAnthropic(model='claude-3-5-sonnet-20241022')
            else:  # gemini
                llm = ChatGoogle(model='gemini-2.0-flash-exp')
            
            # Create agent
            agent = Agent(
                task=task,
                llm=llm,
                browser=browser,
                max_steps_per_attempt=max_steps,
            )
            
            # Run the agent
            status_text.info("🤖 AI Agent is starting...")
            
            # Run async in a synchronous context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            result = loop.run_until_complete(agent.run())
            loop.close()
            
            progress_bar.progress(100)
            status_text.empty()
            
            # Process result
            success = result.is_successful() if hasattr(result, 'is_successful') else bool(result)
            
            if success:
                self.log(f"✅ Agent completed {num_surveys} survey attempts")
                st.success("🎉 AI Agent completed its task!")
                
                # Save screening result
                self._upsert_screening_result(
                    account_id=acct["account_id"],
                    site_id=site["site_id"],
                    survey_name="AI_Answered",
                    batch_id=f"ai_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                    screener_answers=num_surveys,
                    status="complete"
                )
                
                # Mark URL as used
                self._mark_url_used_by_url(site["site_id"], start_url)
                
                st.session_state.generation_results = {
                    "action": "direct_answering",
                    "status": "success",
                    "timestamp": datetime.now().isoformat(),
                    "account": {"id": acct["account_id"], "username": acct["username"]},
                    "site": {"id": site["site_id"], "name": site["site_name"]},
                    "start_url": start_url,
                    "surveys_attempted": num_surveys,
                    "result": str(result),
                }
            else:
                self.log(f"❌ Agent failed", "ERROR")
                st.error("AI Agent encountered an error")
                st.session_state.generation_results = {
                    "action": "direct_answering",
                    "status": "failed",
                    "error": str(result) if not isinstance(result, bool) else "Unknown error",
                    "account": {"id": acct["account_id"], "username": acct["username"]},
                    "site": {"id": site["site_id"], "name": site["site_name"]},
                    "timestamp": datetime.now().isoformat(),
                }
                
        except Exception as exc:
            progress_bar.empty()
            status_text.empty()
            self.log(str(exc), "ERROR")
            st.error(f"❌ Error: {exc}")
            st.session_state.generation_results = {
                "action": "direct_answering",
                "status": "failed",
                "error": str(exc),
                "account": {"id": acct["account_id"], "username": acct["username"]},
                "site": {"id": site["site_id"], "name": site["site_name"]},
                "timestamp": datetime.now().isoformat(),
            }
        finally:
            st.session_state.generation_in_progress = False
            st.rerun()
            
    def _build_persona_instruction(self, prompt: Dict, acct: Dict) -> str:
        """Build a persona instruction string from prompt and account data"""
        instruction_parts = []
        
        # Add account demographic info
        if acct.get("age"):
            instruction_parts.append(f"- Age: {acct['age']}")
        if acct.get("gender"):
            instruction_parts.append(f"- Gender: {acct['gender']}")
        if acct.get("city"):
            instruction_parts.append(f"- City: {acct['city']}")
        if acct.get("education_level"):
            instruction_parts.append(f"- Education: {acct['education_level']}")
        if acct.get("job_status"):
            instruction_parts.append(f"- Employment: {acct['job_status']}")
        if acct.get("income_range"):
            instruction_parts.append(f"- Income: {acct['income_range']}")
        if acct.get("marital_status"):
            instruction_parts.append(f"- Marital Status: {acct['marital_status']}")
        if acct.get("has_children") is not None:
            instruction_parts.append(f"- Has Children: {'Yes' if acct['has_children'] else 'No'}")
        if acct.get("household_size"):
            instruction_parts.append(f"- Household Size: {acct['household_size']}")
            
        # Add custom prompt content
        if prompt and prompt.get("content"):
            instruction_parts.append(f"\nAdditional instructions: {prompt['content']}")
            
        return "\n".join(instruction_parts) if instruction_parts else "A typical survey participant"
        
    def _mark_url_used_by_url(self, site_id: int, url: str):
        """Mark URL as used by its URL string"""
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute(
                        "UPDATE account_urls SET is_used=TRUE, used_at=CURRENT_TIMESTAMP "
                        "WHERE site_id=%s AND url=%s",
                        (site_id, url)
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"_mark_url_used_by_url: {e}")

    # ======================================================================
    # Screening Results Tab
    # ======================================================================

    def _tab_screening_results(self, acct, site):
        st.subheader("🏆 Screening Results")
        results = self._load_screening_results(acct["account_id"], site["site_id"])
        if not results:
            st.info("No screening records yet.")
            return

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
            "direct_answering": "AI Survey Answering Results",
            "extract_questions": "Extract Results",
            "extract_all_surveys": "Extract All Surveys Results",
            "answer_questions": "Answer Results (Gemini AI)",
            "create_workflows": "Workflow Creation Results",
        }
        st.subheader(f"✅ {titles.get(action, action)}")

        if r.get("status") == "failed":
            st.error(f"❌ {r.get('error', 'Unknown error')}")
            if st.button("Clear", key="clr_fail"):
                st.session_state.generation_results = None; st.rerun()
            return

        if action == "direct_answering":
            st.success(f"✅ Completed: {r.get('surveys_attempted', 0)} survey(s)")
            st.json({
                "account": r["account"]["username"],
                "site": r["site"]["name"],
                "start_url": r.get("start_url", ""),
                "timestamp": r.get("timestamp", ""),
            })
            
        elif action == "extract_questions":
            c1,c2,c3,c4 = st.columns(4)
            with c1: st.metric("Questions extracted", r.get("questions_extracted",0))
            with c2: st.metric("Account", r["account"]["username"])
            with c3: st.metric("Site",    r["site"]["name"])
            with c4: st.metric("Time",    f"{r.get('execution_time_seconds',0)}s")
            if r.get("survey_name"):
                st.info(f"📋 Survey: **{r['survey_name']}**")
            st.json({"batch_id": r.get("batch_id"), "inserted": r.get("inserted",0)})
            
        elif action == "answer_questions":
            answered = r.get("answers_generated",0); failed = r.get("answers_failed",0)
            col_a,col_b,col_c = st.columns(3)
            with col_a: st.metric("✅ Saved",  answered)
            with col_b: st.metric("❌ Failed", failed)
            with col_c:
                total = answered + failed
                st.metric("Success rate", f"{int(answered/total*100) if total else 0}%")

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