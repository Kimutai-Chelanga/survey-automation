"""
Generate Manual Workflows — Streamlit page v6.2.0 (BrightData + CAPTCHA)
"""

import asyncio
import logging
import os
import traceback
from datetime import datetime
from typing import Dict, Optional

import streamlit as st

from src.core.database.postgres.connection import get_postgres_connection
from .orchestrator import SurveySiteOrchestrator

# Local modules
from .constants import (
    STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR,
    MODEL_REGISTRY, MODEL_ENV_KEYS, DEFAULT_PROXY, SCREENSHOT_LABELS, PROXY_COUNTRIES,
    CAPSOLVER_API_KEY
)
from .db_utils import (
    ensure_tables, load_accounts, load_survey_sites, load_prompts, get_urls,
    record_survey_attempt, verify_status_constraint
)
from .cookie_utils import load_cookies_from_db, save_cookies_to_db, delete_cookies_from_db, get_all_cookie_records
from .proxy_utils import get_account_proxy, save_proxy_config, delete_proxy_config
from .screenshot_utils import take_screenshot
from .persona_utils import build_persona_system_message
from .agent_utils import (
    create_undetected_browser, extract_surveys_with_crawl4ai, run_survey_agent,
    get_llm, perform_google_login, solve_captcha_if_present
)
from .ui_components import (
    display_batch_details, display_results, display_screening_results_tab,
    display_cookie_status
)

from src.streamlit.ui.pages.accounts.chrome_session_manager import ChromeSessionManager

logger = logging.getLogger(__name__)


def run_async(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


# ---------------------------------------------------------------------------
# Robust "Continue with Google" helper
# ---------------------------------------------------------------------------
async def _click_continue_with_google(page, log_func=None, batch_id: str = "") -> bool:
    """
    Try every reasonable selector / text variant for a 'Continue with Google'
    or 'Sign in with Google' element.  Works for <button>, <a>, <div> etc.
    Returns True if the element was found and clicked.
    """
    selectors = [
        # Playwright built-in text matchers (case-insensitive substring)
        "text=Continue with Google",
        "text=Sign in with Google",
        "text=Continue With Google",
        "text=Login with Google",
        "text=Log in with Google",
        # Common aria labels
        "[aria-label*='Google' i]",
        # Any element whose inner text contains "google" (broad fallback)
        "[class*='google' i]",
        "[id*='google' i]",
        # Google's own one-tap / gsi button
        "div.nsm7Bb-HzV7m-LgbsSe",   # GSI button wrapper
        "#google-signin-button",
        "#googleSignInButton",
        # Social login wrappers that just have Google's logo SVG
        "a[href*='google'][href*='oauth']",
        "a[href*='google'][href*='login']",
        "a[href*='google'][href*='auth']",
        "button[data-provider='google']",
        "[data-testid*='google' i]",
    ]

    for sel in selectors:
        try:
            elem = page.locator(sel).first
            if await elem.is_visible(timeout=2000):
                if log_func:
                    log_func(f"✅ Found Google OAuth element via selector: {sel}", batch_id=batch_id)
                await elem.click()
                await page.wait_for_timeout(5000)
                return True
        except Exception:
            continue

    # Last-resort: scan all visible elements for text containing "google"
    try:
        all_clickable = await page.query_selector_all(
            "a, button, div[role='button'], span[role='button'], [tabindex]"
        )
        for elem in all_clickable:
            try:
                text = (await elem.inner_text()).strip().lower()
                if "google" in text and len(text) < 60:
                    if await elem.is_visible():
                        if log_func:
                            log_func(f"✅ Found Google OAuth element via text scan: '{text}'", batch_id=batch_id)
                        await elem.click()
                        await page.wait_for_timeout(5000)
                        return True
            except Exception:
                continue
    except Exception:
        pass

    if log_func:
        log_func("⚠️ Could not find a 'Continue with Google' element on this page.", "WARNING", batch_id=batch_id)
    return False


class GenerateManualWorkflowsPage:
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.orchestrator = SurveySiteOrchestrator(db_manager)
        ensure_tables()

        try:
            from ..hyperbrowser_utils import get_mongodb_client
            client, _ = get_mongodb_client()
        except Exception:
            client = None
        self.chrome_manager = ChromeSessionManager(db_manager, client)

        # Session state defaults
        defaults = {
            "generation_in_progress": False,
            "generation_results": None,
            "generation_logs": [],
            "editing_proxy": False,
            "temp_proxy": None,
            "survey_progress": [],
            "batches": {},
            "batch_details_counter": 0,
            "selected_batch_for_details": None,
        }
        for k, v in defaults.items():
            if k not in st.session_state:
                st.session_state[k] = v

    # ----------------------------------------------------------------------
    # Logging
    # ----------------------------------------------------------------------
    def log(self, msg: str, level: str = "INFO", batch_id: Optional[str] = None):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = f"[{ts}] {level}: {msg}"
        st.session_state.generation_logs.append(entry)
        if len(st.session_state.generation_logs) > 500:
            st.session_state.generation_logs = st.session_state.generation_logs[-500:]
        if batch_id and batch_id in st.session_state.batches:
            st.session_state.batches[batch_id].setdefault("logs", []).append(entry)
            if len(st.session_state.batches[batch_id]["logs"]) > 500:
                st.session_state.batches[batch_id]["logs"] = st.session_state.batches[batch_id]["logs"][-500:]
        getattr(logger, level.lower(), logger.info)(msg)

    def clear_logs(self):
        st.session_state.generation_logs = []

    # ----------------------------------------------------------------------
    # Core async execution
    # ----------------------------------------------------------------------
    async def _do_direct_answering(
        self, acct, site, prompt, start_url, num_surveys, model_choice,
        google_email: str, google_password: str, proxy_cfg: Optional[Dict],
    ):
        batch_id = f"ai_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        st.session_state.batches[batch_id] = {
            "logs": [], "screenshots": [],
            "timestamp": datetime.now().isoformat(),
            "account": acct["username"], "site": site["site_name"],
        }
        self.log(f"═══ Batch {batch_id} ═══  {acct['username']} / {site['site_name']}", batch_id=batch_id)
        st.session_state.generation_in_progress = True

        status_ph = st.empty()
        progress_ph = st.empty()

        browser = None
        complete_count = passed_count = failed_count = error_count = 0
        survey_details = []

        try:
            # Chrome profile
            profile_path = self.chrome_manager.get_profile_path(acct['username'])
            if not os.path.exists(profile_path):
                self.log(f"Creating Chrome profile for {acct['username']}", batch_id=batch_id)
                create_result = self.chrome_manager.create_profile_for_account(acct['account_id'], acct['username'])
                if not create_result.get('success'):
                    raise Exception(f"Could not create profile: {create_result.get('error')}")
                profile_path = create_result['profile_path']

            # ── Launch stealth browser with proxy ──────────────────────────
            status_ph.info("🖥️ Launching stealth browser...")
            browser = await create_undetected_browser(
                user_data_dir=profile_path,
                headless=False,
                proxy=proxy_cfg,
                log_func=self.log,
                batch_id=batch_id
            )

            # ── Acquire a BrowserContext and Page — browser-use 0.1.40 API ──
            # Browser.new_context() → browser_use.BrowserContext
            # BrowserContext.get_current_page() → Playwright Page (or None)
            # BrowserContext.new_page() → creates a fresh Playwright Page
            context = await browser.new_context()
            page = await context.get_current_page()
            if page is None:
                self.log("No existing page — opening a new one", batch_id=batch_id)
                page = await context.new_page()

            await take_screenshot(page, "01_survey_tab_open", batch_id, log_func=self.log)

            # Navigate to the start URL
            self.log(f"🌐 Navigating to {start_url}", batch_id=batch_id)
            await page.goto(start_url, wait_until="domcontentloaded")
            await page.wait_for_timeout(3000)

            # ── Attempt to click "Continue with Google" ────────────────────
            self.log("🔍 Looking for 'Continue with Google' button…", batch_id=batch_id)
            clicked = await _click_continue_with_google(page, log_func=self.log, batch_id=batch_id)

            if clicked:
                self.log("✅ Clicked Google OAuth button — waiting for redirect/popup…", batch_id=batch_id)
                await page.wait_for_timeout(5000)

                # If a new popup appeared (OAuth in a new tab), switch to it.
                # page.context is the raw Playwright BrowserContext; .pages is a
                # sync list property that gives all open Playwright pages.
                try:
                    playwright_ctx = page.context          # raw Playwright BrowserContext
                    all_pages = playwright_ctx.pages       # sync property → list[Page]
                    if len(all_pages) > 1:
                        oauth_page = all_pages[-1]
                        self.log("🔀 Detected OAuth popup — switching to it", batch_id=batch_id)
                        await oauth_page.wait_for_load_state("domcontentloaded")
                        await oauth_page.wait_for_timeout(3000)
                        account_selectors = [
                            "div[data-authuser]",
                            "li[data-identifier]",
                            "[data-email]",
                            "div.XID2Y",
                            "div.H0sPbc",
                        ]
                        for acc_sel in account_selectors:
                            try:
                                acc_elem = oauth_page.locator(acc_sel).first
                                if await acc_elem.is_visible(timeout=3000):
                                    await acc_elem.click()
                                    self.log("✅ Selected saved Google account from chooser", batch_id=batch_id)
                                    await oauth_page.wait_for_timeout(4000)
                                    break
                            except Exception:
                                continue
                except Exception as e:
                    self.log(f"⚠️ Could not check for OAuth popup: {e}", "WARNING", batch_id=batch_id)
            else:
                self.log(
                    "⚠️ No Google OAuth button found — the profile may already be logged in, "
                    "or the page layout is unexpected.",
                    "WARNING", batch_id=batch_id
                )

            # Solve any initial CAPTCHA on the landing/dashboard page
            await solve_captcha_if_present(page, log_func=self.log, batch_id=batch_id)

            # Extract surveys
            surveys = await extract_surveys_with_crawl4ai(page.url, log_func=self.log)
            if not surveys:
                self.log("No surveys found via Crawl4AI, falling back to agent-based discovery", "WARNING", batch_id=batch_id)

            llm = get_llm(model_choice)
            persona = build_persona_system_message(prompt, acct)

            for i in range(num_surveys):
                survey_num = i + 1
                self.log(f"── Survey {survey_num}/{num_surveys} ──", batch_id=batch_id)
                status_ph.info(f"🤖 Survey {survey_num}/{num_surveys}...")
                progress_ph.progress(i / num_surveys, text=f"Survey {survey_num}/{num_surveys}")
                st.session_state.survey_progress.append({"num": survey_num, "status": STATUS_PENDING, "note": "Running..."})

                survey_status = STATUS_ERROR
                result_snippet = ""

                try:
                    if surveys and i < len(surveys):
                        survey_card = surveys[i]
                        if survey_card.link_url.startswith("http"):
                            await page.goto(survey_card.link_url)
                            await page.wait_for_timeout(3000)
                        else:
                            try:
                                await page.click(survey_card.link_url)
                                await page.wait_for_timeout(3000)
                            except Exception:
                                self.log(f"Could not click survey using '{survey_card.link_url}', falling back to agent", "WARNING", batch_id=batch_id)

                    # Solve CAPTCHA on survey page if any
                    await solve_captcha_if_present(page, log_func=self.log, batch_id=batch_id)

                    outcome = await run_survey_agent(browser, llm, persona, page.url, log_func=self.log)
                    survey_status = outcome
                    result_snippet = f"Agent returned: {outcome}"
                    await take_screenshot(page, "05_survey_complete", batch_id, survey_num, log_func=self.log)

                except Exception as e:
                    self.log(f"Survey {survey_num} exception: {e}", "ERROR", batch_id=batch_id)
                    survey_status = STATUS_ERROR
                    result_snippet = str(e)[:300]

                st.session_state.survey_progress[-1] = {"num": survey_num, "status": survey_status, "note": result_snippet[:100]}
                if survey_status == STATUS_COMPLETE:
                    complete_count += 1
                elif survey_status == STATUS_PASSED:
                    passed_count += 1
                elif survey_status == STATUS_FAILED:
                    failed_count += 1
                else:
                    error_count += 1

                survey_details.append({"survey_number": survey_num, "outcome": survey_status, "output_snippet": result_snippet})
                record_survey_attempt(
                    account_id=acct["account_id"], site_id=site["site_id"],
                    survey_name=f"Survey_{survey_num}_{batch_id}",
                    batch_id=batch_id, status=survey_status,
                    notes=result_snippet[:300],
                )

                if i < num_surveys - 1:
                    await page.goto(start_url)
                    await page.wait_for_timeout(3000)

            progress_ph.progress(1.0, text="Done!")
            summary = f"✅ {complete_count} complete  🟡 {passed_count} passed  ❌ {failed_count} failed  ⚠️ {error_count} error"
            self.log(f"🏁 {summary}", batch_id=batch_id)
            status_ph.success(f"🏁 {summary}")

            st.session_state.generation_results = {
                "action": "direct_answering", "status": "success",
                "complete": complete_count, "passed": passed_count,
                "failed": failed_count, "error": error_count,
                "total": num_surveys, "details": survey_details,
                "account": acct, "site": {"name": site["site_name"]},
                "model": model_choice, "start_url": start_url,
                "batch_id": batch_id, "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            err_msg = f"{type(e).__name__}: {e}"
            self.log(err_msg, "ERROR", batch_id=batch_id)
            self.log(traceback.format_exc(), "ERROR", batch_id=batch_id)
            status_ph.error(f"❌ {err_msg}")
            st.session_state.generation_results = {
                "action": "direct_answering", "status": "failed",
                "error": err_msg, "batch_id": batch_id,
            }

        finally:
            if browser:
                try:
                    await browser.close()
                except Exception:
                    pass
            st.session_state.generation_in_progress = False
            st.rerun()

    # ----------------------------------------------------------------------
    # UI rendering
    # ----------------------------------------------------------------------
    def render(self):
        st.title("🤖 AI Survey Answerer (BrightData + CAPTCHA v6.2)")
        st.markdown("""
        <div style='background:#1e3a5f;padding:18px;border-radius:10px;margin-bottom:18px;'>
        <h3 style='color:white;margin:0;'>AI-Powered Survey Answering with BrightData & Stealth</h3>
        <p style='color:#a0c4ff;margin:8px 0 0;'>
        🌎 <b>BrightData residential proxy</b> – choose any country from the dropdown.<br>
        🛡️ <b>Capsolver CAPTCHA solving</b> – automatic reCAPTCHA/hCaptcha bypass.<br>
        🔐 Persistent Chrome profile & undetected‑chromedriver.<br>
        📋 Crawl4AI extraction – no brittle selectors.<br>
        🤖 Single AI agent answers qualification + main survey.
        </p>
        </div>""", unsafe_allow_html=True)

        schema_check = verify_status_constraint()
        if not schema_check.get("ok") and schema_check.get("missing"):
            st.error(f"⚠️ Schema mismatch — missing statuses: `{schema_check['missing']}`. Run migration SQL.")
            with st.expander("🔧 Migration SQL"):
                st.code("""
ALTER TABLE screening_results DROP CONSTRAINT IF EXISTS screening_results_status_check;
ALTER TABLE screening_results ADD CONSTRAINT screening_results_status_check
    CHECK (status IN ('pending','passed','failed','complete','error'));
""", language="sql")

        accounts = load_accounts()
        survey_sites = load_survey_sites()
        prompts = load_prompts()
        avail_sites = self.orchestrator.get_available_sites()

        if not avail_sites:
            st.error("⚠️ No survey sites with both extractor AND workflow creator found.")
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
                with st.expander("👁️ View persona prompt"):
                    st.code(acct_prompt["content"], language=None)
            else:
                st.warning("⚠️ No prompt — create one in Prompts page")

        with col2:
            st.subheader("🌐 Survey Site")
            db_sites = [s for s in survey_sites if s["site_name"] in avail_names]
            if not db_sites:
                st.error("No DB sites match loaded module names.")
                return
            site_opts = {s["site_name"]: s for s in db_sites}
            site = site_opts[st.selectbox("Survey Site:", list(site_opts), key="wf_site")]
            si = next((s for s in avail_sites if s["site_name"] == site["site_name"]), {})
            st.caption(f"Extractor v{si.get('extractor_version','?')} | Creator v{si.get('creator_version','?')}")

        st.markdown("---")
        display_cookie_status(acct)
        st.markdown("---")
        self._render_answer_direct(acct, site, acct_prompt)

        if st.session_state.survey_progress:
            st.markdown("---")
            st.subheader("📊 Run Progress")
            for entry in st.session_state.survey_progress:
                icon = {"complete": "✅", "passed": "🟡", "failed": "❌", "pending": "⏳", "error": "⚠️"}.get(entry.get("status", "pending"), "❓")
                st.write(f"{icon} Survey {entry['num']}: **{entry['status'].upper()}** — {entry.get('note', '')}")

        if st.session_state.generation_results and st.session_state.generation_results.get("batch_id"):
            st.markdown("---")
            st.subheader("📁 Latest Run — Logs & Screenshots")
            display_batch_details(
                st.session_state.generation_results["batch_id"],
                st.session_state.batches,
                SCREENSHOT_LABELS,
                key_suffix="_latest",
            )

        all_batches = sorted(st.session_state.batches.keys(), reverse=True)
        if all_batches:
            st.markdown("---")
            st.subheader("🗂️ Inspect Any Run")
            chosen = st.selectbox("Select batch:", all_batches, key="inspect_batch_select")
            if chosen:
                with st.expander(f"📁 {chosen}", expanded=False):
                    display_batch_details(
                        chosen,
                        st.session_state.batches,
                        SCREENSHOT_LABELS,
                        key_suffix="_inspect",
                    )

        if st.session_state.generation_logs:
            st.markdown("---")
            with st.expander("📋 Global Logs (last 100)", expanded=False):
                st.code("\n".join(st.session_state.generation_logs[-100:]), language="log")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Clear logs", key="clr_logs"):
                        self.clear_logs()
                        st.rerun()
                with c2:
                    st.download_button(
                        "⬇️ Download",
                        "\n".join(st.session_state.generation_logs),
                        f"logs_{datetime.now():%Y%m%d_%H%M%S}.txt",
                    )

        if st.session_state.generation_results:
            st.markdown("---")
            display_results(st.session_state.generation_results)

        st.markdown("---")
        display_screening_results_tab(acct, site, st.session_state.batches)

    def _render_answer_direct(self, acct, site, prompt):
        st.subheader("🤖 AI Survey Answerer")
        if not prompt:
            st.error("❌ No prompt found — create one in the Prompts page first.")
            return

        available_models = [k for k, v in MODEL_ENV_KEYS.items() if os.environ.get(v)]
        if not available_models:
            st.error("❌ No LLM API key found. Set OPENAI_API_KEY / ANTHROPIC_API_KEY / GEMINI_API_KEY.")
            return

        urls = get_urls(acct["account_id"], site["site_id"])
        if not urls:
            st.warning("⚠️ No URLs configured for this account/site.")
            return

        url_map = {}
        for u in urls:
            label = f"{'⭐ ' if u.get('is_default') else ''}{u['url']}{'  [used]' if u.get('is_used') else ''}"
            url_map[label] = u

        selected_label = st.selectbox("Dashboard / Survey URL:", list(url_map), key="answer_url")
        start_url = url_map[selected_label]["url"].strip()
        if start_url and not start_url.startswith(("http://", "https://")):
            start_url = "https://" + start_url
            st.info(f"URL normalised to: {start_url}")

        num_surveys = st.number_input("Surveys to answer:", min_value=1, max_value=50, value=1, key="num_surveys")
        model_choice = st.selectbox("AI Model:", available_models, key="model_choice")

        st.markdown("---")
        st.subheader("🔑 Google Account Credentials")
        st.caption("Used as **fallback** if the persistent profile is not yet logged in.")
        col_e, col_p = st.columns(2)
        with col_e:
            google_email = st.text_input("Google Email", value=acct.get("email", ""), key="google_email", placeholder="you@gmail.com")
        with col_p:
            google_password = st.text_input("Google Password", type="password", key="google_password", placeholder="your Google password")

        st.markdown("---")
        st.subheader("🌐 Proxy Settings (BrightData Residential)")
        stored_proxy = get_account_proxy(acct["account_id"])
        if st.session_state.get("temp_proxy") is None:
            if stored_proxy:
                st.session_state.temp_proxy = stored_proxy.copy()
            else:
                st.session_state.temp_proxy = DEFAULT_PROXY.copy()
        proxy_to_use = st.session_state.temp_proxy or stored_proxy or DEFAULT_PROXY
        if proxy_to_use:
            country_display = proxy_to_use.get("country", "US")
            st.success(f"🔌 {proxy_to_use['proxy_type']}://{proxy_to_use['host']}:{proxy_to_use['port']}  |  🌍 Country: {country_display}")
            if proxy_to_use.get("username"):
                st.caption(f"Username: {proxy_to_use['username'][:30]}...")
        if st.button("✏️ Configure Proxy", key="edit_proxy_btn"):
            st.session_state.editing_proxy = not st.session_state.editing_proxy
            st.rerun()
        if st.session_state.editing_proxy:
            with st.form("proxy_form"):
                c1, c2 = st.columns(2)
                with c1:
                    ptype = st.selectbox("Type", ["http", "https", "socks5", "socks4"],
                                         index=["http","https","socks5","socks4"].index(proxy_to_use.get("proxy_type","http")))
                    host = st.text_input("Host", value=proxy_to_use.get("host", DEFAULT_PROXY["host"]))
                    port = st.number_input("Port", value=proxy_to_use.get("port", DEFAULT_PROXY["port"]), step=1)
                with c2:
                    uname = st.text_input("Username", value=proxy_to_use.get("username", DEFAULT_PROXY["username"]))
                    pwd = st.text_input("Password", type="password", value=proxy_to_use.get("password", DEFAULT_PROXY["password"]))
                    current_country = proxy_to_use.get("country", "US")
                    country_idx = PROXY_COUNTRIES.index(current_country) if current_country in PROXY_COUNTRIES else 0
                    country = st.selectbox("Country (BrightData)", PROXY_COUNTRIES, index=country_idx)
                sb, cb = st.columns(2)
                with sb:
                    save = st.form_submit_button("💾 Save for this run", use_container_width=True)
                with cb:
                    cancel = st.form_submit_button("Cancel", use_container_width=True)
                if save:
                    st.session_state.temp_proxy = {
                        "proxy_type": ptype,
                        "host": host,
                        "port": int(port),
                        "username": uname or None,
                        "password": pwd or None,
                        "country": country
                    }
                    st.session_state.editing_proxy = False
                    st.rerun()
                if cancel:
                    st.session_state.editing_proxy = False
                    st.rerun()
        if proxy_to_use and st.button("💾 Save proxy to account (persistent)", key="save_proxy_db"):
            res = save_proxy_config(
                acct["account_id"],
                proxy_to_use["proxy_type"],
                proxy_to_use["host"],
                proxy_to_use["port"],
                proxy_to_use.get("username", ""),
                proxy_to_use.get("password", ""),
                proxy_to_use.get("country", "US")
            )
            if res["success"]:
                st.success("✅ Saved.")
                st.session_state.temp_proxy = None
                st.rerun()
            else:
                st.error(res.get("error"))
        if stored_proxy and st.button("🗑️ Delete proxy from account", key="del_proxy"):
            res = delete_proxy_config(acct["account_id"])
            if res["success"]:
                st.success("✅ Deleted.")
                st.session_state.temp_proxy = DEFAULT_PROXY.copy()
                st.rerun()

        st.markdown("---")
        if CAPSOLVER_API_KEY:
            st.success("✅ Capsolver API key detected – automatic CAPTCHA solving enabled.")
        else:
            st.warning("⚠️ No Capsolver API key set (set CAPSOLVER_API_KEY environment variable). CAPTCHAs may block progress.")

        st.info(
            f"**Account:** {acct['username']}  |  **Site:** {site['site_name']}  |  "
            f"**URL:** {start_url}  |  **Surveys:** {num_surveys}  |  **Model:** {model_choice}  |  "
            f"**Prompt:** {prompt['prompt_name']}  |  **Email:** {google_email or '⚠️ not set'}"
        )

        profile_path = self.chrome_manager.get_profile_path(acct['username'])
        if os.path.exists(os.path.join(profile_path, 'Default')):
            st.success("✅ Persistent Chrome profile exists — will reuse existing login state.")
        else:
            st.info("ℹ️ No profile yet. Will create one and perform one‑time login if needed.")

        if st.button(
            f"🚀 Answer {num_surveys} Survey(s) with AI (BrightData + CAPTCHA)",
            type="primary",
            use_container_width=True,
            key="answer_btn",
            disabled=st.session_state.get("generation_in_progress", False),
        ):
            st.session_state.survey_progress = []
            run_async(self._do_direct_answering(
                acct, site, prompt, start_url, num_surveys, model_choice,
                google_email, google_password, proxy_to_use,
            ))