"""
Generate Manual Workflows — Streamlit page v3.7.3 (Browserless version)
- Uses browser-use with Browserless WebSocket for AI browser automation
- Applies proxy settings from database, with UI to edit/update them
- Uses cookies from database for persistent login sessions
- AI-only: no extraction or workflow creation features
"""

import asyncio
import csv
import io
import json
import logging
import os
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import streamlit as st
from psycopg2.extras import RealDictCursor

from src.core.database.postgres.connection import get_postgres_connection
from .orchestrator import SurveySiteOrchestrator

# ============================================================================
# BROWSER-USE LLM IMPORTS
# ============================================================================
from browser_use import Agent, Browser
from browser_use.llm import ChatOpenAI, ChatAnthropic, ChatGoogle

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Model registry — maps UI label → (class, kwargs)
# ---------------------------------------------------------------------------
MODEL_REGISTRY: Dict[str, Dict[str, Any]] = {
    "openai — GPT-4o": {
        "cls": ChatOpenAI,
        "kwargs": {"model": "gpt-4o", "temperature": 0.7}
    },
    "anthropic — Claude 3.5": {
        "cls": ChatAnthropic,
        "kwargs": {"model": "claude-3-5-sonnet-20241022", "temperature": 0.7}
    },
    "gemini — Gemini 2.0 Flash": {
        "cls": ChatGoogle,
        "kwargs": {"model": "gemini-2.0-flash-exp", "temperature": 0.7}
    },
}

# Environment variable names for API keys
MODEL_ENV_KEYS: Dict[str, str] = {
    "openai — GPT-4o": "OPENAI_API_KEY",
    "anthropic — Claude 3.5": "ANTHROPIC_API_KEY",
    "gemini — Gemini 2.0 Flash": "GEMINI_API_KEY",
}


def run_async(coro):
    """
    Run async coroutine in Streamlit context.
    Uses a robust approach: creates a new event loop in a separate thread if needed.
    """
    try:
        loop = asyncio.get_running_loop()
        # A loop is already running; create a new thread with its own loop.
        import threading
        result = None
        exception = None

        def run_in_thread():
            nonlocal result, exception
            new_loop = asyncio.new_event_loop()
            asyncio.set_event_loop(new_loop)
            try:
                result = new_loop.run_until_complete(coro)
            except Exception as e:
                exception = e
            finally:
                new_loop.close()

        thread = threading.Thread(target=run_in_thread)
        thread.start()
        thread.join()
        if exception:
            raise exception
        return result
    except RuntimeError:
        # No running loop, we can create one and run
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


class GenerateManualWorkflowsPage:

    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.orchestrator = SurveySiteOrchestrator(db_manager)

        # Ensure proxy_configs table exists (migration)
        self._ensure_proxy_configs_table()

        # Initialize session state
        for k, v in {
            "generation_in_progress": False,
            "generation_results": None,
            "generation_logs": [],
            "editing_proxy": False,
            "temp_proxy": None,
        }.items():
            if k not in st.session_state:
                st.session_state[k] = v

    # ------------------------------------------------------------------
    # Database schema helpers
    # ------------------------------------------------------------------
    def _ensure_proxy_configs_table(self):
        """Create proxy_configs table and add active_proxy_id column to accounts if needed."""
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    # Check if proxy_configs exists
                    c.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_name = 'proxy_configs'
                        )
                    """)
                    exists = c.fetchone()[0]
                    if not exists:
                        c.execute("""
                            CREATE TABLE proxy_configs (
                                proxy_id SERIAL PRIMARY KEY,
                                account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                                proxy_type VARCHAR(10) NOT NULL,
                                host VARCHAR(255) NOT NULL,
                                port INTEGER NOT NULL,
                                username VARCHAR(255),
                                password VARCHAR(255),
                                is_active BOOLEAN DEFAULT TRUE,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                        """)
                        logger.info("✅ Created proxy_configs table")
                        conn.commit()

                    # Add active_proxy_id column if missing
                    c.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name = 'accounts' AND column_name = 'active_proxy_id'
                    """)
                    if not c.fetchone():
                        c.execute("""
                            ALTER TABLE accounts ADD COLUMN active_proxy_id INTEGER
                            REFERENCES proxy_configs(proxy_id)
                        """)
                        logger.info("✅ Added active_proxy_id column to accounts")
                        conn.commit()
        except Exception as e:
            logger.error(f"Error ensuring proxy configs table: {e}")

    def _get_account_proxy(self, account_id: int) -> Optional[Dict[str, Any]]:
        """Fetch active proxy configuration for an account."""
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("""
                        SELECT proxy_id, proxy_type, host, port, username, password
                        FROM proxy_configs
                        WHERE account_id = %s AND is_active = TRUE
                        ORDER BY updated_at DESC
                        LIMIT 1
                    """, (account_id,))
                    row = c.fetchone()
                    if row:
                        return dict(row)
            return None
        except Exception as e:
            logger.error(f"Error fetching proxy for account {account_id}: {e}")
            return None

    def _save_proxy_config(self, account_id: int, proxy_type: str, host: str, port: int,
                           username: str = "", password: str = "") -> Dict[str, Any]:
        """Save or update proxy config for an account."""
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    # Deactivate any existing active proxy for this account
                    c.execute("""
                        UPDATE proxy_configs
                        SET is_active = FALSE
                        WHERE account_id = %s AND is_active = TRUE
                    """, (account_id,))

                    # Insert new proxy config
                    c.execute("""
                        INSERT INTO proxy_configs (account_id, proxy_type, host, port, username, password, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                        RETURNING proxy_id
                    """, (account_id, proxy_type, host, port, username or None, password or None))
                    proxy_id = c.fetchone()[0]

                    # Update accounts table to point to this proxy as active
                    c.execute("""
                        UPDATE accounts SET active_proxy_id = %s, updated_time = CURRENT_TIMESTAMP
                        WHERE account_id = %s
                    """, (proxy_id, account_id))

                    conn.commit()
                    self.log(f"✅ Saved proxy config for account {account_id}")
                    return {'success': True, 'proxy_id': proxy_id}
        except Exception as e:
            self.log(f"Error saving proxy config: {e}", "ERROR")
            return {'success': False, 'error': str(e)}

    def _delete_proxy_config(self, account_id: int) -> Dict[str, Any]:
        """Delete all proxy configs for an account."""
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("DELETE FROM proxy_configs WHERE account_id = %s", (account_id,))
                    c.execute("UPDATE accounts SET active_proxy_id = NULL WHERE account_id = %s", (account_id,))
                    conn.commit()
                    self.log(f"✅ Deleted proxy config for account {account_id}")
                    return {'success': True}
        except Exception as e:
            self.log(f"Error deleting proxy config: {e}", "ERROR")
            return {'success': False, 'error': str(e)}

    # ------------------------------------------------------------------
    # Cookie helpers
    # ------------------------------------------------------------------
    async def _get_account_cookies_raw(self, account_id: int) -> Optional[Dict]:
        """Fetch cookies from account_cookies table."""
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("""
                        SELECT cookie_data, has_cookies, updated_at
                        FROM account_cookies
                        WHERE account_id = %s
                        ORDER BY updated_at DESC
                        LIMIT 1
                    """, (account_id,))
                    row = c.fetchone()
                    if row:
                        return dict(row)
            return None
        except Exception as e:
            logger.error(f"Error fetching account cookies: {e}")
            return None

    async def _get_account_cookies_for_injection(self, account_id: int) -> Optional[List[Dict]]:
        """Fetch cookies from database and format for Playwright storage_state."""
        try:
            cookie_info = await self._get_account_cookies_raw(account_id)
            if not cookie_info or not cookie_info.get("has_cookies"):
                return None
            
            cookies = cookie_info["cookie_data"]
            if isinstance(cookies, str):
                cookies = json.loads(cookies)
            
            # Convert from EditThisCookie format to Playwright storage_state format
            formatted_cookies = []
            for c in cookies:
                formatted_cookies.append({
                    "name": c["name"],
                    "value": c["value"],
                    "domain": c.get("domain", ""),
                    "path": c.get("path", "/"),
                    "secure": c.get("secure", False),
                    "httpOnly": c.get("httpOnly", False),
                    "sameSite": c.get("sameSite", "Lax"),
                })
            
            return formatted_cookies
        except Exception as e:
            logger.error(f"Error fetching cookies for injection: {e}")
            return None

    # ------------------------------------------------------------------
    # Logging helpers
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
        st.title("🤖 AI Survey Answerer")

        st.markdown("""
        <div style='background:#1e3a5f;padding:18px;border-radius:10px;margin-bottom:18px;'>
        <h3 style='color:white;margin:0;'>AI-Powered Survey Answering</h3>
        <p style='color:#a0c4ff;margin:8px 0 0;'>
        🤖 <b>AI Agent</b> — Uses Browserless + browser-use for browser automation.<br>
        📝 <b>Persona Prompt</b> — Guides the AI to answer as your persona.<br>
        🍪 <b>Cookies</b> — Injected from database for persistent login.<br>
        🏆 <b>Results</b> — Track pass/fail per survey attempt.
        </p>
        </div>""", unsafe_allow_html=True)

        accounts = self._load_accounts()
        survey_sites = self._load_survey_sites()
        prompts = self._load_prompts()
        avail_sites = self.orchestrator.get_available_sites()

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
            acct = acc_opts[st.selectbox("Account:", list(acc_opts), key="wf_acct")]
            acct_prompt = next((p for p in prompts if p["account_id"] == acct["account_id"]), None)
            if acct_prompt:
                st.success(f"✅ Prompt: {acct_prompt['prompt_name']}")
                with st.expander("👁️ View persona prompt", expanded=False):
                    st.code(acct_prompt["content"], language=None)
            else:
                st.warning("⚠️ No prompt — create one in Prompts page")

        with col2:
            st.subheader("🌐 Survey Site")
            db_sites = [s for s in survey_sites if s["site_name"] in avail_names]
            if not db_sites:
                st.error(f"No DB sites match loaded module names.\nModule names: {sorted(avail_names)}")
                return
            site_opts = {s["site_name"]: s for s in db_sites}
            site = site_opts[st.selectbox("Survey Site:", list(site_opts), key="wf_site")]
            si = next((s for s in avail_sites if s["site_name"] == site["site_name"]), {})
            st.caption(f"Extractor v{si.get('extractor_version','?')} | Creator v{si.get('creator_version','?')}")

        st.markdown("---")
        self._tab_answer_direct(acct, site, acct_prompt)

        if st.session_state.generation_logs:
            st.markdown("---")
            with st.expander("📋 Logs", expanded=False):
                st.code("\n".join(st.session_state.generation_logs[-25:]), language="log")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Clear", key="clr_logs"):
                        self.clear_logs()
                        st.rerun()
                with c2:
                    st.download_button(
                        "Download Logs",
                        "\n".join(st.session_state.generation_logs),
                        f"logs_{datetime.now():%Y%m%d_%H%M%S}.txt",
                    )

        if st.session_state.generation_results:
            st.markdown("---")
            self._render_results(st.session_state.generation_results)

        st.markdown("---")
        self._tab_screening_results(acct, site)

    # ------------------------------------------------------------------
    # Direct AI Answering — Browserless + browser-use (FIXED)
    # ------------------------------------------------------------------
    def _tab_answer_direct(self, acct, site, prompt):
        st.subheader("🤖 AI Survey Answerer")

        if not prompt:
            st.error("❌ No prompt — create one in the Prompts page first.")
            return

        available_models = [
            label for label, env_key in MODEL_ENV_KEYS.items()
            if os.environ.get(env_key)
        ]
        if not available_models:
            st.error(
                "❌ No LLM API key found.\n"
                "Add `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, or `GEMINI_API_KEY` to `.env`."
            )
            return

        urls = self._get_urls(acct["account_id"], site["site_id"])
        if not urls:
            st.warning("⚠️ No URLs configured for this account/site.")
            return

        url_map: Dict[str, Dict] = {}
        for u in urls:
            star = "⭐ " if u.get("is_default") else ""
            used = " [used]" if u.get("is_used") else ""
            url_map[f"{star}{u['url']}{used}"] = u

        selected_label = st.selectbox(
            "Dashboard / Survey URL to start from:",
            list(url_map),
            key="answer_url"
        )
        survey_url = url_map[selected_label]["url"].strip()
        if survey_url and not survey_url.startswith(("http://", "https://")):
            survey_url = "https://" + survey_url
            st.info(f"URL normalised to: {survey_url}")

        num_surveys = st.number_input(
            "Number of surveys to answer:",
            min_value=1, max_value=50, value=1, key="num_surveys"
        )
        model_choice = st.selectbox("AI Model:", available_models, key="model_choice")

        # ----- Proxy configuration UI -----
        st.markdown("---")
        st.subheader("🌐 Proxy Settings")

        DEFAULT_PROXY = {
            "proxy_type": "http",
            "host": "proxy-us.proxy-cheap.com",
            "port": 5959,
            "username": "pcpafN3XBx-res-us",
            "password": "PC_8j0HzeNGa7ZOCVq3C"
        }

        stored_proxy = self._get_account_proxy(acct["account_id"])
        
        if st.session_state.get("temp_proxy") is None:
            if stored_proxy:
                st.session_state.temp_proxy = stored_proxy.copy()
            else:
                st.session_state.temp_proxy = DEFAULT_PROXY.copy()

        proxy_to_use = st.session_state.get("temp_proxy") or stored_proxy or DEFAULT_PROXY

        if proxy_to_use:
            st.success(f"🔌 **Active proxy:** {proxy_to_use['proxy_type']}://{proxy_to_use['host']}:{proxy_to_use['port']}")
            if proxy_to_use.get('username'):
                st.caption(f"Username: {proxy_to_use['username']}")
        else:
            st.info("ℹ️ No proxy configured. Using direct connection.")

        if st.button("✏️ Configure Proxy", key="edit_proxy_btn"):
            st.session_state.editing_proxy = not st.session_state.editing_proxy
            st.rerun()

        if st.session_state.editing_proxy:
            with st.form("proxy_form"):
                st.write("**Proxy Configuration**")
                col1, col2 = st.columns(2)
                with col1:
                    proxy_type = st.selectbox(
                        "Proxy Type",
                        options=["http", "https", "socks5", "socks4"],
                        index=["http", "https", "socks5", "socks4"].index(proxy_to_use.get('proxy_type', 'http')),
                        key="proxy_type"
                    )
                    host = st.text_input("Host", value=proxy_to_use.get('host', DEFAULT_PROXY['host']), key="proxy_host")
                    port = st.number_input("Port", value=proxy_to_use.get('port', DEFAULT_PROXY['port']), step=1, key="proxy_port")
                with col2:
                    username = st.text_input("Username (optional)", value=proxy_to_use.get('username', DEFAULT_PROXY['username']), key="proxy_user")
                    password = st.text_input("Password (optional)", type="password", value=proxy_to_use.get('password', DEFAULT_PROXY['password']), key="proxy_pass")

                col_save, col_cancel = st.columns(2)
                with col_save:
                    save_btn = st.form_submit_button("💾 Save for this run", use_container_width=True)
                with col_cancel:
                    cancel_btn = st.form_submit_button("Cancel", use_container_width=True)

                if save_btn:
                    st.session_state.temp_proxy = {
                        "proxy_type": proxy_type,
                        "host": host,
                        "port": port,
                        "username": username if username else None,
                        "password": password if password else None
                    }
                    st.session_state.editing_proxy = False
                    st.success("✅ Proxy set for this run (not saved to DB).")
                    st.rerun()
                if cancel_btn:
                    st.session_state.editing_proxy = False
                    st.rerun()

        if proxy_to_use and st.button("💾 Save this proxy to account (persistent)", key="save_proxy_to_db"):
            result = self._save_proxy_config(
                acct["account_id"],
                proxy_to_use['proxy_type'],
                proxy_to_use['host'],
                proxy_to_use['port'],
                proxy_to_use.get('username', ''),
                proxy_to_use.get('password', '')
            )
            if result['success']:
                st.success("✅ Proxy saved to database. Will be used for future runs.")
                st.session_state.temp_proxy = None
                st.rerun()
            else:
                st.error(f"Failed to save: {result.get('error')}")

        if stored_proxy and st.button("🗑️ Delete proxy from account", key="delete_proxy"):
            result = self._delete_proxy_config(acct["account_id"])
            if result['success']:
                st.success("✅ Proxy deleted from database.")
                st.session_state.temp_proxy = DEFAULT_PROXY.copy()
                st.rerun()
            else:
                st.error(f"Failed to delete: {result.get('error')}")

        st.markdown("---")
        st.info(
            f"**Account:** {acct['username']}\n"
            f"**Site:** {site['site_name']}\n"
            f"**Starting URL:** {survey_url}\n"
            f"**Surveys to attempt:** {num_surveys}\n"
            f"**Model:** {model_choice}\n"
            f"**Persona prompt:** {prompt['prompt_name']}"
        )

        with st.expander("⚙️ Advanced Options"):
            max_steps = st.number_input(
                "Max steps per survey:", min_value=10, max_value=500, value=200, key="max_steps"
            )

        if st.button(
            f"🚀 Answer {num_surveys} Survey(s) with AI Agent",
            type="primary",
            use_container_width=True,
            key="answer_btn",
            disabled=st.session_state.get("generation_in_progress", False),
        ):
            # Run async function using run_async helper
            run_async(self._do_direct_answering(
                acct, site, prompt, survey_url, num_surveys,
                model_choice, max_steps
            ))

    # ------------------------------------------------------------------
    async def _do_direct_answering(
        self, acct, site, prompt, start_url, num_surveys,
        model_choice, max_steps
    ):
        import asyncio
        import os
        from datetime import datetime

        from browser_use import Browser, Agent

        self.log(f"Starting AI answering: {acct['username']} / {site['site_name']}")
        st.session_state.generation_in_progress = True

        with st.spinner("🤖 AI Agent is working..."):

            browser = None

            try:
                # =========================
                # ENV CHECK
                # =========================
                token = os.getenv("BROWSERLESS_TOKEN")
                ws_base = os.getenv("BROWSERLESS_WS_URL", "wss://production-sfo.browserless.io")

                if not token:
                    raise Exception("❌ Missing BROWSERLESS_TOKEN")

                # =========================
                # ✅ BROWSERLESS (OFFICIAL PROXY SUPPORT)
                # =========================
                cdp_url = (
                    f"{ws_base}"
                    f"?token={token}"
                    f"&proxy=residential"
                    f"&proxyCountry=us"
                    f"&proxySticky=true"
                )

                self.log(f"CDP URL: {cdp_url}")

                # =========================
                # START BROWSER
                # =========================
                browser = Browser(cdp_url=cdp_url)
                await browser.start()

                self.log("✅ Browser connected successfully")

                # =========================
                # COOKIES
                # =========================
                cookies = await self._get_account_cookies_for_injection(acct["account_id"])

                if cookies and hasattr(browser, "context") and browser.context:
                    await browser.context.add_cookies(cookies)
                    self.log(f"✅ Injected {len(cookies)} cookies")
                else:
                    self.log("⚠️ No cookies found")

                # =========================
                # LLM SETUP
                # =========================
                model_cfg = MODEL_REGISTRY[model_choice]
                api_key = os.getenv(MODEL_ENV_KEYS.get(model_choice))

                if not api_key:
                    raise Exception("Missing model API key")

                llm = model_cfg["cls"](**{
                    **model_cfg["kwargs"],
                    "api_key": api_key
                })

                # =========================
                # TASK (UNCHANGED)
                # =========================
                persona = self._build_persona_system_message(prompt, acct)

                base_task = f"""
    Persona:
    {persona}

    You are a REAL human completing surveys.

    ## CRITICAL RULES
    - Stay consistent with your persona at ALL times
    - Never contradict previous answers
    - Answer like a real human

    ## Step-by-step process

    1. Go to {start_url}

    2. Start a survey

    3. Answer questions:
    - Read carefully
    - Match persona
    - Never skip required fields

    ## Human behavior
    - Wait 2–5 seconds before each action
    - Scroll occasionally
    - Do NOT rush

    ## Answering strategy
    - Vary answers naturally
    - Avoid patterns
    - Be realistic

    ## Attention checks
    - If instructed to pick a specific answer → obey exactly

    ## Navigation
    - Click Next / Continue after answering
    - Wait for page load

    ## Completion
    STOP only if:
    - "Thank you" → SUCCESS
    - "Disqualified" → DQ

    ## Never:
    - Stop early
    - Rush
    - Act like a bot

    Goal:
    Finish the survey OR reach disqualification page.
    """

                max_steps = max(max_steps, 300)

                completed = 0
                disqualified = 0

                # =========================
                # LOOP SURVEYS
                # =========================
                for i in range(num_surveys):

                    self.log(f"Running survey {i+1}/{num_surveys}")

                    agent = Agent(
                        task=base_task,
                        llm=llm,
                        browser=browser
                    )

                    result = await agent.run(max_steps=max_steps)
                    result_str = str(result).lower()

                    if "disqual" in result_str:
                        disqualified += 1
                        status = "disqualified"
                    elif "thank" in result_str or "complete" in result_str:
                        completed += 1
                        status = "completed"
                    else:
                        status = "incomplete"

                    self.log(f"Survey {i+1}: {status}")

                    self._record_survey_attempt(
                        account_id=acct["account_id"],
                        site_id=site["site_id"],
                        survey_name=f"Survey_{i+1}",
                        batch_id=f"ai_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        status=status,
                        notes=result_str[:300]
                    )

                # =========================
                # FINAL UI
                # =========================
                if completed or disqualified:
                    st.success(f"✅ Completed: {completed} | DQ: {disqualified}")
                else:
                    st.warning("⚠️ No surveys completed")

            except Exception as e:
                self.log(str(e), "ERROR")
                st.error(f"❌ {e}")

            finally:
                if browser:
                    try:
                        await browser.stop()
                    except:
                        pass

                st.session_state.generation_in_progress = False
                st.rerun()

    # ------------------------------------------------------------------
    # Persona builder
    # ------------------------------------------------------------------
    def _build_persona_system_message(self, prompt: Dict, acct: Dict) -> str:
        """
        Returns a string describing the persona for the AI to embody.
        """
        lines = [
            "You are a specific person answering survey questions. Embody this identity:",
            "",
        ]

        demo_fields = [
            ("age", "Age"),
            ("gender", "Gender"),
            ("city", "City / Location"),
            ("education_level", "Education level"),
            ("job_status", "Employment status"),
            ("income_range", "Household income"),
            ("marital_status", "Marital status"),
            ("household_size", "Household size"),
            ("industry", "Industry / sector"),
        ]
        for field, label in demo_fields:
            if acct.get(field):
                lines.append(f"• {label}: {acct[field]}")

        if acct.get("has_children") is not None:
            lines.append(f"• Has children: {'Yes' if acct['has_children'] else 'No'}")

        if prompt and prompt.get("content"):
            lines += [
                "",
                "Additional persona details:",
                prompt["content"].strip(),
            ]

        lines += [
            "",
            "Survey answering rules:",
            "- Choose the option that best matches this persona",
            "- Stay consistent — do not contradict answers given earlier",
            "- For free-text fields write naturally (1–2 sentences max)",
            "- If multiple answers apply, pick the one most characteristic of this persona",
        ]
        return "\n".join(lines)

    def _mark_url_used_by_url(self, site_id: int, url: str):
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

    def _record_survey_attempt(self, account_id: int, site_id: int, survey_name: str,
                               batch_id: str, status: str, notes: str = ""):
        """
        Record one survey attempt result (per survey, not batch).
        Uses the screening_results table but repurposed for per-survey tracking.
        """
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute(
                        "INSERT INTO screening_results "
                        "(account_id, site_id, survey_name, batch_id, screener_answers, status, started_at, completed_at, notes) "
                        "VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s)",
                        (account_id, site_id, survey_name, batch_id, 1, status, notes)
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"_record_survey_attempt: {e}")

    # ------------------------------------------------------------------
    # Screening Results Tab (updated to show per-survey results)
    # ------------------------------------------------------------------
    def _tab_screening_results(self, acct, site):
        st.subheader("🏆 Survey Attempts")
        results = self._load_screening_results(acct["account_id"], site["site_id"])
        if not results:
            st.info("No survey attempts recorded yet.")
            return

        total = len(results)
        completed = sum(1 for r in results if r["status"] == "completed")
        disqualified = sum(1 for r in results if r["status"] == "disqualified")
        incomplete = sum(1 for r in results if r["status"] == "incomplete")

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total", total)
        c2.metric("✅ Completed", completed)
        c3.metric("❌ Disqualified", disqualified)
        c4.metric("⚠️ Incomplete", incomplete)

        if total > 0:
            pass_rate = int((completed) / total * 100) if total else 0
            st.progress(pass_rate / 100, text=f"Completion rate: {pass_rate}%")

        st.markdown("---")
        for r in results:
            icon = {"completed": "✅", "disqualified": "❌", "incomplete": "⚠️", "pending": "⏳", "error": "⚠️"}.get(r["status"], "❓")
            with st.expander(
                f"{icon} **{r.get('survey_name') or 'Unknown Survey'}** — "
                f"{r['status'].upper()} — "
                f"{r['started_at'].strftime('%Y-%m-%d %H:%M') if r.get('started_at') else '?'}",
                expanded=False,
            ):
                col_info, col_actions = st.columns([3, 1])
                with col_info:
                    st.markdown(
                        f"**Batch ID:** `{r.get('batch_id') or '—'}`\n"
                        f"**Started:** {r['started_at'].strftime('%Y-%m-%d %H:%M') if r.get('started_at') else '—'}\n"
                        f"**Completed:** {r['completed_at'].strftime('%Y-%m-%d %H:%M') if r.get('completed_at') else '—'}\n"
                    )
                    if r.get("notes"):
                        st.caption(f"Notes: {r['notes']}")
                with col_actions:
                    rid = r["result_id"]
                    if r["status"] != "completed":
                        if st.button("✅ Mark Completed", key=f"pass_{rid}", use_container_width=True):
                            self._update_screening_status(rid, "completed")
                            st.rerun()
                    if r["status"] != "disqualified":
                        if st.button("❌ Mark Disqualified", key=f"fail_{rid}", use_container_width=True):
                            self._update_screening_status(rid, "disqualified")
                            st.rerun()
                    new_note = st.text_input("Note:", key=f"note_{rid}", placeholder="Optional…")
                    if new_note:
                        if st.button("💾 Save Note", key=f"savenote_{rid}", use_container_width=True):
                            self._save_screening_note(rid, new_note)
                            st.rerun()

        st.markdown("---")
        if st.button("📥 Export CSV", key="exp_screening"):
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=[
                "survey_name", "status", "started_at", "completed_at", "batch_id", "notes"
            ])
            w.writeheader()
            for r in results:
                w.writerow({
                    "survey_name": r.get("survey_name", ""),
                    "status": r.get("status", ""),
                    "started_at": str(r.get("started_at", "")),
                    "completed_at": str(r.get("completed_at", "")),
                    "batch_id": r.get("batch_id", ""),
                    "notes": r.get("notes", ""),
                })
            st.download_button(
                "⬇️ Download CSV",
                data=buf.getvalue(),
                file_name=f"screening_{acct['username']}_{site['site_name'].replace(' ', '_')}.csv",
                mime="text/csv",
                key="dl_screening_csv"
            )

    # ------------------------------------------------------------------
    # Results renderer (updated)
    # ------------------------------------------------------------------
    def _render_results(self, r: Dict):
        if r.get("action") != "direct_answering":
            return
            
        st.subheader("✅ AI Survey Answering Results")

        if r.get("status") == "failed":
            st.error(f"❌ {r.get('error', 'Unknown error')}")
            if st.button("Clear", key="clr_fail"):
                st.session_state.generation_results = None
                st.rerun()
            return

        st.success(f"✅ Completed: {r.get('completed', 0)} surveys, {r.get('disqualified', 0)} disqualifications.")
        
        if "details" in r:
            st.write("**Per-survey details:**")
            for d in r["details"]:
                st.write(f"- Survey {d['survey_number']}: {d['outcome']} — {d['output_snippet'][:100]}...")
        else:
            st.json({
                "account": r["account"]["username"],
                "site": r["site"]["name"],
                "model": r.get("model", ""),
                "start_url": r.get("start_url", ""),
                "timestamp": r.get("timestamp", ""),
            })

        if st.button("Clear results", key="clr_res"):
            st.session_state.generation_results = None
            st.rerun()

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------
    def _pg(self):
        return get_postgres_connection()

    def _get_urls(self, account_id, site_id):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT url_id,url,is_default,is_used,used_at,notes "
                        "FROM account_urls WHERE account_id=%s AND site_id=%s "
                        "ORDER BY is_default DESC, created_at DESC",
                        (account_id, site_id)
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_get_urls: {e}")
            return []

    def _load_screening_results(self, account_id, site_id) -> List[Dict]:
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT result_id,survey_name,batch_id,status,started_at,completed_at,notes "
                        "FROM screening_results WHERE account_id=%s AND site_id=%s "
                        "ORDER BY started_at DESC",
                        (account_id, site_id)
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_screening_results: {e}")
            return []

    def _update_screening_status(self, result_id, status):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    completed_sql = "CURRENT_TIMESTAMP" if status in ("completed", "disqualified") else "NULL"
                    c.execute(
                        f"UPDATE screening_results SET status=%s, completed_at={completed_sql} "
                        f"WHERE result_id=%s",
                        (status, result_id)
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"_update_screening_status: {e}")

    def _save_screening_note(self, result_id, note):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute(
                        "UPDATE screening_results SET notes=%s WHERE result_id=%s",
                        (note, result_id)
                    )
                    conn.commit()
        except Exception as e:
            logger.error(f"_save_screening_note: {e}")

    def _load_accounts(self):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT account_id,username,country,profile_id,age,gender,city,"
                        "education_level,job_status,income_range,marital_status,"
                        "has_children,household_size,industry,email,phone "
                        "FROM accounts ORDER BY username"
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_accounts: {e}")
            return []

    def _load_survey_sites(self):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT site_id,site_name,description FROM survey_sites ORDER BY site_name"
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_survey_sites: {e}")
            return []

    def _load_prompts(self):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT prompt_id,account_id,name AS prompt_name,content,prompt_type "
                        "FROM prompts WHERE is_active=TRUE"
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_prompts: {e}")
            return []