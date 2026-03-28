"""
Generate Manual Workflows — Streamlit page v5.1.0
═══════════════════════════════════════════════════════════════════════════════
KEY FIX (v5.1.0):
  Each browser_use Agent gets its OWN isolated Browser instance (bu_browser_qual
  and bu_browser_main). The qual browser is always closed + 2s CDP settle before
  Participate is clicked via Playwright, and a fresh main browser is created
  only after Participate succeeds. This eliminates the empty
  AgentHistoryList(all_results=[], all_model_outputs=[]) error caused by CDP
  conflicts when a shared browser_use Browser overlapped with Playwright control.
═══════════════════════════════════════════════════════════════════════════════
"""

import asyncio
import csv
import io
import json
import logging
import os
import traceback
import time
import urllib.request
from datetime import datetime
from typing import Any, Dict, List, Optional

import streamlit as st
from psycopg2.extras import RealDictCursor

from src.core.database.postgres.connection import get_postgres_connection
from .orchestrator import SurveySiteOrchestrator

from browser_use import Agent
from browser_use.browser.browser import Browser, BrowserConfig

from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI

from src.streamlit.ui.pages.accounts.chrome_session_manager import ChromeSessionManager

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Status constants
# ─────────────────────────────────────────────────────────────────────────────
STATUS_COMPLETE = "complete"
STATUS_PASSED   = "passed"
STATUS_FAILED   = "failed"
STATUS_PENDING  = "pending"
STATUS_ERROR    = "error"

# ─────────────────────────────────────────────────────────────────────────────
# Model registry
# ─────────────────────────────────────────────────────────────────────────────
MODEL_REGISTRY: Dict[str, Dict[str, Any]] = {
    "openai — GPT-4o": {
        "cls": ChatOpenAI,
        "kwargs": {"model": "gpt-4o", "temperature": 0.7},
    },
    "anthropic — Claude 3.5": {
        "cls": ChatAnthropic,
        "kwargs": {"model": "claude-3-5-sonnet-20241022", "temperature": 0.7},
    },
    "gemini — Gemini 2.5 Flash": {
        "cls": ChatGoogleGenerativeAI,
        "kwargs": {"model": "gemini-2.5-flash", "temperature": 0.7},
    },
}

MODEL_ENV_KEYS: Dict[str, str] = {
    "openai — GPT-4o":           "OPENAI_API_KEY",
    "anthropic — Claude 3.5":    "ANTHROPIC_API_KEY",
    "gemini — Gemini 2.5 Flash": "GEMINI_API_KEY",
}

COMPLETE_KEYWORDS = [
    "thank you", "thank-you", "thankyou", "survey complete", "survey completed",
    "you have completed", "submission received", "response recorded",
    "reward", "points added", "earned", "credited",
    "all done", "finished", "successfully submitted",
]
DISQUALIFIED_KEYWORDS = [
    "disqualif", "screen out", "screened out", "not eligible",
    "don't qualify", "do not qualify", "unfortunately", "not a match",
    "not selected", "quota full", "quota reached", "sorry, ", "we're sorry",
]


def run_async(coro):
    import threading
    try:
        asyncio.get_running_loop()
        result = exc = None

        def _run():
            nonlocal result, exc
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                result = loop.run_until_complete(coro)
            except Exception as e:
                exc = e
            finally:
                loop.close()

        t = threading.Thread(target=_run)
        t.start()
        t.join()
        if exc:
            raise exc
        return result
    except RuntimeError:
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
        self._ensure_tables()

        try:
            from ..hyperbrowser_utils import get_mongodb_client
            client, _ = get_mongodb_client()
        except Exception:
            client = None
        self.chrome_manager = ChromeSessionManager(db_manager, client)

        for k, v in {
            "generation_in_progress": False,
            "generation_results": None,
            "generation_logs": [],
            "editing_proxy": False,
            "temp_proxy": None,
            "survey_progress": [],
            "batches": {},
            "batch_details_counter": 0,
            "selected_batch_for_details": None,
        }.items():
            if k not in st.session_state:
                st.session_state[k] = v

    # ─────────────────────────────────────────────────────────────────────────
    # DB schema bootstrap
    # ─────────────────────────────────────────────────────────────────────────
    def _ensure_tables(self):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_name = 'proxy_configs'
                        )
                    """)
                    if not c.fetchone()[0]:
                        c.execute("""
                            CREATE TABLE proxy_configs (
                                proxy_id   SERIAL PRIMARY KEY,
                                account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                                proxy_type VARCHAR(10) NOT NULL,
                                host       VARCHAR(255) NOT NULL,
                                port       INTEGER NOT NULL,
                                username   VARCHAR(255),
                                password   VARCHAR(255),
                                is_active  BOOLEAN DEFAULT TRUE,
                                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                            )
                        """)

                    c.execute("""
                        SELECT column_name FROM information_schema.columns
                        WHERE table_name = 'accounts' AND column_name = 'active_proxy_id'
                    """)
                    if not c.fetchone():
                        c.execute("""
                            ALTER TABLE accounts
                            ADD COLUMN active_proxy_id INTEGER REFERENCES proxy_configs(proxy_id)
                        """)

                    c.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_name = 'account_cookies'
                        )
                    """)
                    if not c.fetchone()[0]:
                        c.execute("""
                            CREATE TABLE account_cookies (
                                cookie_id    SERIAL PRIMARY KEY,
                                account_id   INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                                domain       VARCHAR(255) NOT NULL DEFAULT 'google.com',
                                cookies_json TEXT NOT NULL,
                                captured_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                                UNIQUE (account_id, domain)
                            )
                        """)

                    conn.commit()
        except Exception as e:
            logger.error(f"_ensure_tables: {e}")

    # ─────────────────────────────────────────────────────────────────────────
    # Cookie DB helpers
    # ─────────────────────────────────────────────────────────────────────────
    def _load_cookies_from_db(self, account_id: int, domain: str = "google.com") -> Optional[List[Dict]]:
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT cookies_json FROM account_cookies "
                        "WHERE account_id=%s AND domain=%s "
                        "ORDER BY updated_at DESC LIMIT 1",
                        (account_id, domain),
                    )
                    row = c.fetchone()
                    return json.loads(row["cookies_json"]) if row else None
        except Exception as e:
            logger.error(f"_load_cookies_from_db: {e}")
            return None

    def _save_cookies_to_db(self, account_id: int, cookies: List[Dict],
                            domain: str = "google.com") -> bool:
        try:
            relevant = [c for c in cookies if domain.lstrip(".") in c.get("domain", "").lstrip(".")]
            if not relevant:
                relevant = cookies
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        INSERT INTO account_cookies (account_id, domain, cookies_json, updated_at)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (account_id, domain)
                        DO UPDATE SET cookies_json = EXCLUDED.cookies_json,
                                      updated_at   = CURRENT_TIMESTAMP
                    """, (account_id, domain, json.dumps(relevant)))
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"_save_cookies_to_db: {e}")
            return False

    def _delete_cookies_from_db(self, account_id: int, domain: str = "google.com") -> bool:
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute(
                        "DELETE FROM account_cookies WHERE account_id=%s AND domain=%s",
                        (account_id, domain),
                    )
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"_delete_cookies_from_db: {e}")
            return False

    def _get_all_cookie_records(self, account_id: int) -> List[Dict]:
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT cookie_id, domain, captured_at, updated_at, "
                        "LENGTH(cookies_json) as size_bytes "
                        "FROM account_cookies WHERE account_id=%s ORDER BY domain",
                        (account_id,),
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_get_all_cookie_records: {e}")
            return []

    # ─────────────────────────────────────────────────────────────────────────
    # Screenshot helpers
    # ─────────────────────────────────────────────────────────────────────────
    _ALLOWED_SCREENSHOTS = frozenset({
        "01_survey_tab_open",
        "02_qualification_start",
        "03_qualification_done",
        "04_survey_started",
        "05_survey_complete",
    })

    _SCREENSHOT_LABELS = {
        "01_survey_tab_open":      "1️⃣ Survey Tab Open",
        "02_qualification_start":  "2️⃣ Qualification Started",
        "03_qualification_done":   "3️⃣ Qualification Done",
        "04_survey_started":       "4️⃣ Survey Started",
        "05_survey_complete":      "5️⃣ Survey Complete",
    }

    async def _screenshot(self, page, label: str, batch_id: str,
                          survey_num: int = 0) -> Optional[bytes]:
        if label not in self._ALLOWED_SCREENSHOTS:
            return None
        try:
            img = await page.screenshot(type="png", full_page=False)
            self.log(f"📸 Screenshot: {self._SCREENSHOT_LABELS[label]}", batch_id=batch_id)
            st.session_state.batches[batch_id].setdefault("screenshots", []).append(
                (survey_num, img, label)
            )
            return img
        except Exception as e:
            self.log(f"Screenshot failed ({label}): {e}", "WARNING", batch_id=batch_id)
            return None

    # ─────────────────────────────────────────────────────────────────────────
    # DB schema helpers (proxy / screening)
    # ─────────────────────────────────────────────────────────────────────────
    def _verify_schema_status_constraint(self) -> Dict[str, Any]:
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        SELECT pg_get_constraintdef(oid)
                        FROM pg_constraint
                        WHERE conrelid = 'screening_results'::regclass
                          AND contype = 'c' AND conname LIKE '%status%'
                    """)
                    row = c.fetchone()
                    if row:
                        import re
                        vals = re.findall(r"'([^']+)'", row[0])
                        our  = {STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR}
                        missing = our - set(vals)
                        return {"ok": len(missing) == 0, "missing": sorted(missing)}
            return {"ok": True}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _get_account_proxy(self, account_id: int) -> Optional[Dict]:
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("""
                        SELECT proxy_id, proxy_type, host, port, username, password
                        FROM proxy_configs WHERE account_id=%s AND is_active=TRUE
                        ORDER BY updated_at DESC LIMIT 1
                    """, (account_id,))
                    row = c.fetchone()
                    return dict(row) if row else None
        except Exception as e:
            logger.error(f"_get_account_proxy: {e}")
            return None

    def _save_proxy_config(self, account_id, proxy_type, host, port, username="", password=""):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE proxy_configs SET is_active=FALSE WHERE account_id=%s AND is_active=TRUE",
                              (account_id,))
                    c.execute("""
                        INSERT INTO proxy_configs (account_id,proxy_type,host,port,username,password,is_active)
                        VALUES (%s,%s,%s,%s,%s,%s,TRUE) RETURNING proxy_id
                    """, (account_id, proxy_type, host, port, username or None, password or None))
                    proxy_id = c.fetchone()[0]
                    c.execute("UPDATE accounts SET active_proxy_id=%s WHERE account_id=%s",
                              (proxy_id, account_id))
                    conn.commit()
                    return {"success": True, "proxy_id": proxy_id}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def _delete_proxy_config(self, account_id):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("DELETE FROM proxy_configs WHERE account_id=%s", (account_id,))
                    c.execute("UPDATE accounts SET active_proxy_id=NULL WHERE account_id=%s", (account_id,))
                    conn.commit()
                    return {"success": True}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ─────────────────────────────────────────────────────────────────────────
    # Logging
    # ─────────────────────────────────────────────────────────────────────────
    def log(self, msg: str, level: str = "INFO", batch_id: Optional[str] = None):
        ts    = datetime.now().strftime("%H:%M:%S")
        entry = f"[{ts}] {level}: {msg}"
        st.session_state.generation_logs.append(entry)
        if len(st.session_state.generation_logs) > 500:
            st.session_state.generation_logs = st.session_state.generation_logs[-500:]
        if batch_id and batch_id in st.session_state.batches:
            st.session_state.batches[batch_id].setdefault("logs", []).append(entry)
            if len(st.session_state.batches[batch_id]["logs"]) > 500:
                st.session_state.batches[batch_id]["logs"] = \
                    st.session_state.batches[batch_id]["logs"][-500:]
        getattr(logger, level.lower(), logger.info)(msg)

    def clear_logs(self):
        st.session_state.generation_logs = []

    # ─────────────────────────────────────────────────────────────────────────
    # Batch details display
    # ─────────────────────────────────────────────────────────────────────────
    def _display_batch_details(self, batch_id: str):
        batch = st.session_state.batches.get(batch_id)
        if not batch:
            st.info("No data recorded for this batch yet.")
            return

        st.session_state.batch_details_counter += 1
        ctr = st.session_state.batch_details_counter

        st.caption(
            f"🕐 {batch.get('timestamp','?')}  |  "
            f"👤 {batch.get('account','?')}  |  "
            f"🌐 {batch.get('site','?')}"
        )

        tab_logs, tab_shots = st.tabs(["📝 Logs", "📸 Screenshots (4)"])

        with tab_logs:
            logs = batch.get("logs", [])
            if logs:
                st.code("\n".join(logs), language="log")
                st.download_button(
                    "⬇️ Download logs", "\n".join(logs),
                    f"logs_{batch_id}.txt",
                    key=f"dl_log_{batch_id}_{ctr}",
                )
            else:
                st.info("No logs stored for this batch.")

        with tab_shots:
            shots = batch.get("screenshots", [])
            if shots:
                for i, (num, img_bytes, label) in enumerate(shots):
                    display_label = self._SCREENSHOT_LABELS.get(label, label)
                    st.markdown(f"**{display_label}**")
                    st.image(img_bytes, use_container_width=True)
                    st.download_button(
                        f"⬇️ {display_label}.png",
                        img_bytes,
                        f"ss_{batch_id}_{i}_{label}.png",
                        mime="image/png",
                        key=f"dl_ss_{batch_id}_{ctr}_{i}",
                    )
                    st.markdown("---")
            else:
                st.info("No screenshots captured for this batch.")

    # ─────────────────────────────────────────────────────────────────────────
    # Survey loading helper
    # ─────────────────────────────────────────────────────────────────────────
    async def _wait_for_surveys_to_load(
        self,
        page,
        batch_id: str,
        max_reloads: int = 5,
        reload_wait: float = 6.0,
        poll_interval: float = 2.0,
        max_polls_per_load: int = 10,
    ) -> bool:
        selectors = [
            "div.p-ripple-wrapper",
            ".list-item .reward-amount",
            "[class*='list-item']",
            "[class*='reward-amount']",
            "text=USD",
            ".survey-card",
            "button:has-text('Start')",
        ]

        for reload_attempt in range(max_reloads):
            self.log(
                f"🔄 Waiting for surveys — attempt {reload_attempt + 1}/{max_reloads}",
                batch_id=batch_id,
            )
            for poll in range(max_polls_per_load):
                for sel in selectors:
                    try:
                        if await page.locator(sel).first.is_visible(timeout=2000):
                            self.log(f"✅ Surveys visible via: {sel}", batch_id=batch_id)
                            return True
                    except Exception:
                        pass
                self.log(f"  ⏳ Poll {poll + 1}/{max_polls_per_load} — not yet visible", batch_id=batch_id)
                await asyncio.sleep(poll_interval)

            if reload_attempt < max_reloads - 1:
                self.log("↩️ Reloading page...", batch_id=batch_id)
                try:
                    await page.reload(wait_until="domcontentloaded", timeout=30_000)
                    await asyncio.sleep(reload_wait)
                    for nav_sel in [
                        "div:nth-child(2) > .p-nav-wrapper > .p-nav-item",
                        ".p-nav-item:has-text('Surveys')",
                        "a:has-text('Surveys')",
                    ]:
                        try:
                            loc = page.locator(nav_sel).first
                            if await loc.is_visible(timeout=3000):
                                await loc.click()
                                await asyncio.sleep(3)
                                break
                        except Exception:
                            pass
                except Exception as e:
                    self.log(f"Reload error: {e}", "WARNING", batch_id=batch_id)
                    await asyncio.sleep(reload_wait)

        self.log("⚠️ Surveys never loaded after all attempts", "WARNING", batch_id=batch_id)
        return False

    # ─────────────────────────────────────────────────────────────────────────
    # Abandonment modal
    # ─────────────────────────────────────────────────────────────────────────
    async def _dismiss_abandonment_modal(self, page, batch_id: str) -> bool:
        try:
            title = page.locator("text='What happened?'").first
            if not await title.is_visible(timeout=3000):
                return False
            self.log("⚠️ 'What happened?' modal detected — dismissing...", batch_id=batch_id)

            close_btn = page.locator(
                "button.p-dialog-header-close, button[aria-label='Close'], .p-dialog-header button"
            ).first
            if await close_btn.is_visible(timeout=2000):
                await close_btn.click()
                self.log("✅ Closed modal via X button", batch_id=batch_id)
                await asyncio.sleep(2)
                return True

            other = page.locator("text='Other'").first
            if await other.is_visible(timeout=2000):
                await other.click()
                await asyncio.sleep(0.5)
                submit = page.locator("button:has-text('Submit')").first
                if await submit.is_visible(timeout=2000):
                    await submit.click()
                    self.log("✅ Dismissed modal via Other + Submit", batch_id=batch_id)
                    await asyncio.sleep(2)
                    return True

            for txt in ["Skip", "Cancel", "No thanks", "Close"]:
                try:
                    btn = page.locator(f"button:has-text('{txt}')").first
                    if await btn.is_visible(timeout=1000):
                        await btn.click()
                        self.log(f"✅ Dismissed modal via '{txt}'", batch_id=batch_id)
                        await asyncio.sleep(2)
                        return True
                except Exception:
                    pass
        except Exception as e:
            self.log(f"_dismiss_abandonment_modal error: {e}", "WARNING", batch_id=batch_id)
        return False

    # ─────────────────────────────────────────────────────────────────────────
    # Survey modal detection
    # ─────────────────────────────────────────────────────────────────────────
    async def _detect_survey_modal(self, page, batch_id: str) -> bool:
        modal_selectors = [
            "[role='dialog']", "[role='alertdialog']", ".modal",
            "[class*='modal']", "[class*='dialog']",
            "[class*='qualification']", "[class*='Qualification']",
            "[class*='overlay']", "[class*='popup']", "[class*='Popup']",
            "div.p-dialog", "div.p-dialog-content", ".p-dialog-mask",
        ]
        for ms in modal_selectors:
            try:
                if await page.locator(ms).first.is_visible(timeout=2000):
                    self.log(f"✅ Survey opened as MODAL overlay: {ms}", batch_id=batch_id)
                    return True
            except Exception:
                pass

        content_selectors = [
            "text=Just a few questions before the survey",
            "text=Qualification",
            "text=household earns",
            "input[type='number']",
            "input[placeholder='Enter a number']",
        ]
        for cs in content_selectors:
            try:
                if await page.locator(cs).first.is_visible(timeout=2000):
                    self.log(f"✅ Survey qualification content detected: {cs}", batch_id=batch_id)
                    return True
            except Exception:
                pass
        return False

    # ─────────────────────────────────────────────────────────────────────────
    # Open survey card
    # ─────────────────────────────────────────────────────────────────────────
    async def _open_survey_card(self, page, batch_id):
        self.log("🔍 Opening survey using confirmed Automa selectors...", batch_id=batch_id)

        await self._dismiss_abandonment_modal(page, batch_id)

        surveys_nav_selectors = [
            "div:nth-child(2) > .p-nav-wrapper > .p-nav-item",
            ".p-nav-item:has-text('Surveys')",
            "a:has-text('Surveys')",
            "nav a:has-text('Surveys')",
        ]
        nav_clicked = False
        for sel in surveys_nav_selectors:
            try:
                loc = page.locator(sel).first
                if await loc.is_visible(timeout=4000):
                    await loc.scroll_into_view_if_needed()
                    await asyncio.sleep(0.5)
                    await page.evaluate("(el) => el.click()", await loc.element_handle())
                    self.log(f"✅ Clicked Surveys nav: {sel}", batch_id=batch_id)
                    nav_clicked = True
                    await asyncio.sleep(3)
                    break
            except Exception as e:
                self.log(f"⚠️ Nav selector failed [{sel}]: {e}", "WARNING", batch_id=batch_id)

        if not nav_clicked:
            self.log("⚠️ Could not click Surveys nav — trying from current page", "WARNING", batch_id=batch_id)

        await self._dismiss_abandonment_modal(page, batch_id)
        await asyncio.sleep(2)
        self.log(f"Current URL after nav: {page.url}", batch_id=batch_id)

        card_selectors = [
            "div.p-ripple-wrapper",
            ".list-item:nth-child(1) .reward-amount",
            ".list-item:nth-child(1)",
            "[class*='list-item']:nth-child(1)",
            "[class*='reward-amount']",
        ]

        for attempt, sel in enumerate(card_selectors):
            try:
                loc = page.locator(sel).first
                if not await loc.is_visible(timeout=4000):
                    self.log(f"  Selector not visible: {sel}", batch_id=batch_id)
                    continue

                self.log(f"🖱️ Clicking card (attempt {attempt + 1}): {sel}", batch_id=batch_id)
                await loc.scroll_into_view_if_needed()
                await asyncio.sleep(1)

                prev_url = page.url
                await page.evaluate("(el) => el.click()", await loc.element_handle())
                self.log(f"URL after click: {page.url}", batch_id=batch_id)

                await asyncio.sleep(1)
                if await page.locator("text='What happened?'").first.is_visible(timeout=2000):
                    self.log("⚠️ Abandonment modal after card click — dismissing", "WARNING", batch_id=batch_id)
                    await self._dismiss_abandonment_modal(page, batch_id)
                    await asyncio.sleep(2)
                    continue

                try:
                    new_page = await page.context.wait_for_event("page", timeout=8000)
                    await new_page.wait_for_load_state("domcontentloaded")
                    self.log(f"✅ Survey opened in NEW TAB: {new_page.url}", batch_id=batch_id)
                    return new_page
                except Exception:
                    pass

                await asyncio.sleep(5)
                if page.url != prev_url:
                    self.log(f"✅ Survey opened in SAME TAB: {page.url}", batch_id=batch_id)
                    return page

                for frame in page.frames:
                    frame_url = frame.url.lower()
                    if frame_url and frame_url not in ("about:blank", "") and "topsurveys.app" not in frame_url:
                        self.log(f"✅ Survey iframe detected: {frame.url}", batch_id=batch_id)
                        return page

                if await self._detect_survey_modal(page, batch_id):
                    return page

                self.log(f"❌ Selector {sel} did not open survey, trying next...", batch_id=batch_id)

            except Exception as e:
                self.log(f"⚠️ Card click failed [{sel}]: {e}", "WARNING", batch_id=batch_id)

        raise Exception("All survey card click attempts failed.")

    # ─────────────────────────────────────────────────────────────────────────
    # Outcome detection
    # ─────────────────────────────────────────────────────────────────────────
    def _detect_survey_outcome(self, result) -> str:
        try:
            combined = str(result).lower()

            agent_brain_dq_phrases = [
                "disqualified - i was disqualified",
                "disqualified - the survey",
                "evaluation_previous_goal=\"disqualified",
                "evaluation_previous_goal='disqualified",
                "i was disqualified from",
                "disqualified from the previous survey",
                "disqualified from this survey",
                "screen out", "screened out",
            ]
            agent_brain_complete_phrases = [
                "evaluation_previous_goal=\"success",
                "evaluation_previous_goal='success",
                "success - i successfully completed",
                "survey is complete", "survey has been completed",
                "successfully submitted", "successfully completed the survey",
            ]

            if hasattr(result, "history") and result.history:
                history_str = " ".join(str(h) for h in result.history[-5:]).lower()
                combined += " " + history_str

                for phrase in agent_brain_dq_phrases:
                    if phrase in combined:
                        return STATUS_FAILED
                for phrase in agent_brain_complete_phrases:
                    if phrase in combined:
                        return STATUS_COMPLETE

            if any(kw in combined for kw in DISQUALIFIED_KEYWORDS):
                return STATUS_FAILED
            if any(kw in combined for kw in COMPLETE_KEYWORDS):
                return STATUS_COMPLETE

            return STATUS_ERROR
        except Exception:
            return STATUS_ERROR

    # ─────────────────────────────────────────────────────────────────────────
    # Render
    # ─────────────────────────────────────────────────────────────────────────
    def render(self):
        st.title("🤖 AI Survey Answerer")
        st.markdown("""
        <div style='background:#1e3a5f;padding:18px;border-radius:10px;margin-bottom:18px;'>
        <h3 style='color:white;margin:0;'>AI-Powered Survey Answering</h3>
        <p style='color:#a0c4ff;margin:8px 0 0;'>
        🔐 <b>Step 1</b> — Launch Chrome with persistent profile; fallback to email+password login if needed.<br>
        🌐 <b>Step 2</b> — Navigate to survey site, click "Continue with Google".<br>
        📋 <b>Step 3</b> — Find surveys tab, reload until surveys appear.<br>
        🤖 <b>Step 4</b> — AI Agent answers surveys as your persona.<br>
        📸 <b>Diagnostics</b> — 5 screenshots: survey tab · qual start · qual done · survey started · complete.
        </p>
        </div>""", unsafe_allow_html=True)

        schema_check = self._verify_schema_status_constraint()
        if not schema_check.get("ok") and schema_check.get("missing"):
            st.error(f"⚠️ Schema mismatch — missing statuses: `{schema_check['missing']}`. Run migration SQL.")
            with st.expander("🔧 Migration SQL"):
                st.code("""
ALTER TABLE screening_results DROP CONSTRAINT IF EXISTS screening_results_status_check;
ALTER TABLE screening_results ADD CONSTRAINT screening_results_status_check
    CHECK (status IN ('pending','passed','failed','complete','error'));
""", language="sql")

        accounts     = self._load_accounts()
        survey_sites = self._load_survey_sites()
        prompts      = self._load_prompts()
        avail_sites  = self.orchestrator.get_available_sites()

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
            si   = next((s for s in avail_sites if s["site_name"] == site["site_name"]), {})
            st.caption(f"Extractor v{si.get('extractor_version','?')} | Creator v{si.get('creator_version','?')}")

        st.markdown("---")
        self._render_cookie_status(acct)
        st.markdown("---")
        self._tab_answer_direct(acct, site, acct_prompt)

        if st.session_state.survey_progress:
            st.markdown("---")
            st.subheader("📊 Run Progress")
            for entry in st.session_state.survey_progress:
                icon = {"complete":"✅","passed":"🟡","failed":"❌","pending":"⏳","error":"⚠️"}.get(
                    entry.get("status","pending"), "❓")
                st.write(f"{icon} Survey {entry['num']}: **{entry['status'].upper()}** — {entry.get('note','')}")

        if st.session_state.generation_results and st.session_state.generation_results.get("batch_id"):
            st.markdown("---")
            st.subheader("📁 Latest Run — Logs & Screenshots")
            self._display_batch_details(st.session_state.generation_results["batch_id"])

        all_batches = sorted(st.session_state.batches.keys(), reverse=True)
        if all_batches:
            st.markdown("---")
            st.subheader("🗂️ Inspect Any Run")
            chosen = st.selectbox("Select batch:", all_batches, key="inspect_batch_select")
            if chosen:
                with st.expander(f"📁 {chosen}", expanded=False):
                    self._display_batch_details(chosen)

        if st.session_state.generation_logs:
            st.markdown("---")
            with st.expander("📋 Global Logs (last 100)", expanded=False):
                st.code("\n".join(st.session_state.generation_logs[-100:]), language="log")
                c1, c2 = st.columns(2)
                with c1:
                    if st.button("Clear logs", key="clr_logs"):
                        self.clear_logs(); st.rerun()
                with c2:
                    st.download_button("⬇️ Download",
                                       "\n".join(st.session_state.generation_logs),
                                       f"logs_{datetime.now():%Y%m%d_%H%M%S}.txt")

        if st.session_state.generation_results:
            st.markdown("---")
            self._render_results(st.session_state.generation_results)

        st.markdown("---")
        self._tab_screening_results(acct, site)

    # ─────────────────────────────────────────────────────────────────────────
    # Cookie status panel
    # ─────────────────────────────────────────────────────────────────────────
    def _render_cookie_status(self, acct: Dict):
        st.subheader("🍪 Google Session Cookies")
        records = self._get_all_cookie_records(acct["account_id"])
        google_record = next((r for r in records if "google" in r["domain"].lower()), None)

        col_status, col_actions = st.columns([3, 2])
        with col_status:
            if google_record:
                updated = google_record.get("updated_at")
                updated_str = updated.strftime("%Y-%m-%d %H:%M UTC") if updated else "unknown"
                size_kb = (google_record.get("size_bytes") or 0) / 1024
                st.success(
                    f"✅ **Cookies stored** for `{google_record['domain']}`  \n"
                    f"Last updated: `{updated_str}` | Size: `{size_kb:.1f} KB`"
                )
                st.caption("Cookies will be injected automatically on the next run. "
                           "If login fails, delete them and re-run with your password.")
            else:
                st.warning("⚠️ **No cookies stored** for this account.  \n"
                           "Enter Google credentials below and run — cookies will be saved automatically after login.")

        with col_actions:
            if google_record:
                if st.button("🗑️ Delete stored cookies", key=f"del_ck_{acct['account_id']}",
                             use_container_width=True):
                    self._delete_cookies_from_db(acct["account_id"], "google.com")
                    st.success("Cookies deleted.")
                    st.rerun()

            with st.expander("📋 Paste cookies manually (JSON)"):
                st.caption("Export cookies from your browser using a cookie-export extension.")
                raw = st.text_area("Cookie JSON array:", height=120,
                                   key=f"manual_ck_{acct['account_id']}",
                                   placeholder='[{"name":"SID","value":"...","domain":".google.com",...}]')
                if st.button("💾 Save pasted cookies", key=f"save_manual_ck_{acct['account_id']}",
                             use_container_width=True):
                    try:
                        parsed = json.loads(raw.strip())
                        if not isinstance(parsed, list):
                            raise ValueError("Expected a JSON array")
                        ok = self._save_cookies_to_db(acct["account_id"], parsed, "google.com")
                        if ok:
                            st.success(f"✅ Saved {len(parsed)} cookies.")
                            st.rerun()
                        else:
                            st.error("Failed to save cookies to DB.")
                    except Exception as ex:
                        st.error(f"Invalid JSON: {ex}")

    # ─────────────────────────────────────────────────────────────────────────
    # Tab: configure and launch
    # ─────────────────────────────────────────────────────────────────────────
    def _tab_answer_direct(self, acct, site, prompt):
        st.subheader("🤖 AI Survey Answerer")
        if not prompt:
            st.error("❌ No prompt found — create one in the Prompts page first.")
            return

        available_models = [k for k, v in MODEL_ENV_KEYS.items() if os.environ.get(v)]
        if not available_models:
            st.error("❌ No LLM API key found. Set OPENAI_API_KEY / ANTHROPIC_API_KEY / GEMINI_API_KEY.")
            return

        urls = self._get_urls(acct["account_id"], site["site_id"])
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

        num_surveys  = st.number_input("Surveys to answer:", min_value=1, max_value=50, value=1, key="num_surveys")
        model_choice = st.selectbox("AI Model:", available_models, key="model_choice")

        st.markdown("---")
        st.subheader("🔑 Google Account Credentials")
        st.caption("Used as **fallback** if the persistent profile is not yet logged in.")

        col_e, col_p = st.columns(2)
        with col_e:
            google_email = st.text_input(
                "Google Email", value=acct.get("email", ""),
                key="google_email", placeholder="you@gmail.com",
            )
        with col_p:
            google_password = st.text_input(
                "Google Password", type="password",
                key="google_password", placeholder="your Google password",
            )

        st.markdown("---")
        st.subheader("🌐 Proxy Settings")
        DEFAULT_PROXY = {
            "proxy_type": "http", "host": "proxy-us.proxy-cheap.com",
            "port": 5959, "username": "pcpafN3XBx-res-us",
            "password": "PC_8j0HzeNGa7ZOCVq3C",
        }
        stored_proxy = self._get_account_proxy(acct["account_id"])
        if st.session_state.get("temp_proxy") is None:
            st.session_state.temp_proxy = (stored_proxy or DEFAULT_PROXY).copy()

        proxy_to_use = st.session_state.temp_proxy or stored_proxy or DEFAULT_PROXY

        if proxy_to_use:
            st.success(f"🔌 {proxy_to_use['proxy_type']}://{proxy_to_use['host']}:{proxy_to_use['port']}")
            if proxy_to_use.get("username"):
                st.caption(f"Username: {proxy_to_use['username']}")

        if st.button("✏️ Configure Proxy", key="edit_proxy_btn"):
            st.session_state.editing_proxy = not st.session_state.editing_proxy
            st.rerun()

        if st.session_state.editing_proxy:
            with st.form("proxy_form"):
                c1, c2 = st.columns(2)
                with c1:
                    ptype = st.selectbox("Type", ["http","https","socks5","socks4"],
                                         index=["http","https","socks5","socks4"].index(
                                             proxy_to_use.get("proxy_type","http")))
                    host  = st.text_input("Host",  value=proxy_to_use.get("host",  DEFAULT_PROXY["host"]))
                    port  = st.number_input("Port", value=proxy_to_use.get("port",  DEFAULT_PROXY["port"]), step=1)
                with c2:
                    uname = st.text_input("Username", value=proxy_to_use.get("username", DEFAULT_PROXY["username"]))
                    pwd   = st.text_input("Password", type="password",
                                          value=proxy_to_use.get("password", DEFAULT_PROXY["password"]))
                sb, cb = st.columns(2)
                with sb:
                    save = st.form_submit_button("💾 Save for this run", use_container_width=True)
                with cb:
                    cancel = st.form_submit_button("Cancel", use_container_width=True)
                if save:
                    st.session_state.temp_proxy = {
                        "proxy_type": ptype, "host": host, "port": int(port),
                        "username": uname or None, "password": pwd or None,
                    }
                    st.session_state.editing_proxy = False
                    st.rerun()
                if cancel:
                    st.session_state.editing_proxy = False
                    st.rerun()

        if proxy_to_use and st.button("💾 Save proxy to account (persistent)", key="save_proxy_db"):
            res = self._save_proxy_config(
                acct["account_id"], proxy_to_use["proxy_type"], proxy_to_use["host"],
                proxy_to_use["port"], proxy_to_use.get("username",""), proxy_to_use.get("password",""),
            )
            if res["success"]:
                st.success("✅ Saved.")
                st.session_state.temp_proxy = None
                st.rerun()
            else:
                st.error(res.get("error"))

        if stored_proxy and st.button("🗑️ Delete proxy from account", key="del_proxy"):
            res = self._delete_proxy_config(acct["account_id"])
            if res["success"]:
                st.success("✅ Deleted.")
                st.session_state.temp_proxy = DEFAULT_PROXY.copy()
                st.rerun()

        st.markdown("---")
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
            f"🚀 Answer {num_surveys} Survey(s) with AI",
            type="primary", use_container_width=True, key="answer_btn",
            disabled=st.session_state.get("generation_in_progress", False),
        ):
            st.session_state.survey_progress = []
            run_async(self._do_direct_answering(
                acct, site, prompt, start_url, num_surveys, model_choice,
                google_email, google_password, proxy_to_use,
            ))

    # =========================================================================
    # CORE: _do_direct_answering  (v5.1.0 — isolated browser instances)
    # =========================================================================
    # =========================================================================
    # CORE: _do_direct_answering  (v5.2.0 — unified qual+participate agent)
    # =========================================================================
    # KEY CHANGES vs v5.1.0:
    #   • Participate (button.p-btn--fill) can appear at ANY point — immediately
    #     after clicking a survey card, or only after several pages of qual
    #     questions.  The old two-phase split (qual agent stops before Participate,
    #     then Playwright clicks it) is replaced with:
    #       1. Instant Playwright check — if Participate is already visible, click
    #          it right away and skip the agent entirely (fast path).
    #       2. Combined qual+participate agent — answers qual questions AND clicks
    #          Participate when it eventually appears (one agent, one CDP session).
    #       3. Safety-net Playwright click — after the agent closes, re-check
    #          whether Participate is still visible and click if so.
    #   • New-tab detection after Participate — context.wait_for_event("page")
    #     catches surveys that open in a new tab; page is reassigned so the main
    #     agent follows the user there.
    #   • Each browser_use Agent still gets its OWN isolated Browser instance
    #     (bu_browser_qual / bu_browser_main); each is closed in its own finally
    #     block with a 3 s CDP settle before Playwright resumes.
    # =========================================================================
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

        status_ph   = st.empty()
        progress_ph = st.empty()

        playwright_instance = None
        pw_browser = None
        context = None
        page = None
        session_id = None
        ws_url = None
        complete_count = passed_count = failed_count = error_count = 0
        survey_details: List[Dict] = []

        try:
            # ------------------------------------------------------------------
            # STEP 0 – Chrome profile
            # ------------------------------------------------------------------
            status_ph.info("🖥️ Preparing Chrome profile...")
            profile_path = self.chrome_manager.get_profile_path(acct['username'])
            if not os.path.exists(profile_path):
                self.log(f"Creating Chrome profile for {acct['username']}", batch_id=batch_id)
                create_result = self.chrome_manager.create_profile_for_account(
                    acct['account_id'], acct['username']
                )
                if not create_result.get('success'):
                    raise Exception(f"Could not create profile: {create_result.get('error')}")
                profile_path = create_result['profile_path']

            # ------------------------------------------------------------------
            # STEP 1 – Start Chrome
            # ------------------------------------------------------------------
            session_id = f"persistent_{acct['username']}_{int(time.time())}"
            self.log(f"Starting Chrome with profile: {profile_path}", batch_id=batch_id)
            start_result = self.chrome_manager.run_persistent_chrome(
                session_id=session_id,
                profile_path=profile_path,
                username=acct['username'],
                account_id=acct['account_id'],
                survey_url=start_url,
                show_terminal=False,
            )
            if not start_result.get('success'):
                raise Exception(f"Failed to start Chrome: {start_result.get('error')}")

            debug_port = start_result['debug_port']
            self.log(f"✅ Chrome started on debug port {debug_port}", batch_id=batch_id)
            status_ph.success("Chrome running — connecting...")

            # ------------------------------------------------------------------
            # STEP 2 – Connect via CDP
            # ------------------------------------------------------------------
            ws_endpoint = f"http://localhost:{debug_port}/json/version"
            for attempt in range(10):
                try:
                    with urllib.request.urlopen(ws_endpoint) as response:
                        data = json.loads(response.read().decode())
                        ws_url = data.get('webSocketDebuggerUrl')
                        if ws_url:
                            break
                except Exception:
                    await asyncio.sleep(1)
            else:
                raise Exception("Chrome did not become ready within 10 seconds")

            from playwright.async_api import async_playwright
            playwright_instance = await async_playwright().start()
            pw_browser = await playwright_instance.chromium.connect_over_cdp(ws_url)
            context = pw_browser.contexts[0]
            page = context.pages[0] if context.pages else await context.new_page()
            self.log("✅ Connected to Chrome via CDP", batch_id=batch_id)

            try:
                await page.wait_for_function(
                    "() => document.readyState === 'complete'", timeout=10_000
                )
                self.log("Browser ready – document complete", batch_id=batch_id)
            except Exception:
                self.log("⚠️ readyState timeout (blank/chrome tab) — proceeding anyway",
                         "WARNING", batch_id=batch_id)
            await asyncio.sleep(1)

            # ------------------------------------------------------------------
            # STEP 3 – Google login check
            # ------------------------------------------------------------------
            self.log("Navigating to Google to verify login state...", batch_id=batch_id)
            for attempt in range(3):
                try:
                    await page.goto("https://accounts.google.com/",
                                    wait_until="domcontentloaded", timeout=45_000)
                    await asyncio.sleep(3)
                    break
                except Exception as e:
                    if attempt == 2:
                        raise
                    self.log(f"Google nav attempt {attempt+1} failed: {e}, retrying...",
                             "WARNING", batch_id=batch_id)
                    await asyncio.sleep(5)

            current_url = page.url
            self.log(f"Google landing URL: {current_url}", batch_id=batch_id)

            needs_login = (
                "accounts.google.com/signin" in current_url
                or "accounts.google.com/v3/signin" in current_url
                or "identifier" in current_url
            )

            if needs_login:
                self.log("⚠️ Not logged into Google – performing one-time login", batch_id=batch_id)
                if not google_email or not google_password:
                    raise Exception("Google login required but no credentials provided.")
                await self._perform_google_login(page, google_email, google_password, batch_id)
            else:
                self.log(f"✅ Already logged into Google via profile (landed on: {current_url})",
                         batch_id=batch_id)

            # ------------------------------------------------------------------
            # STEP 4 – Navigate to survey site
            # ------------------------------------------------------------------
            status_ph.info("🌐 Navigating to survey site...")
            self.log(f"→ Navigating to: {start_url}", batch_id=batch_id)
            await page.goto(start_url, wait_until="domcontentloaded", timeout=30_000)
            await page.wait_for_timeout(3000)
            self.log(f"Survey site URL: {page.url}", batch_id=batch_id)

            google_clicked = False
            for sel in [
                "button:has-text('Continue with Google')",
                "button:has-text('Sign in with Google')",
                "button:has-text('Login with Google')",
                "a:has-text('Continue with Google')",
                "[data-provider='google']",
                "button:has-text('Google')",
            ]:
                try:
                    btn = page.locator(sel).first
                    if await btn.is_visible(timeout=3000):
                        await btn.click()
                        self.log(f"✅ Clicked Google OAuth: {sel}", batch_id=batch_id)
                        google_clicked = True
                        await page.wait_for_timeout(5000)
                        break
                except Exception:
                    pass

            if not google_clicked:
                self.log("⚠️ No 'Continue with Google' button — may already be logged in",
                         "WARNING", batch_id=batch_id)

            try:
                picker = page.locator(f"[data-email='{google_email}']").first
                if await picker.is_visible(timeout=4000):
                    await picker.click()
                    self.log(f"✅ Selected account from picker: {google_email}", batch_id=batch_id)
                    await page.wait_for_timeout(4000)
            except Exception:
                pass

            await page.wait_for_timeout(3000)
            self.log(f"Post-OAuth URL: {page.url}", batch_id=batch_id)

            # ------------------------------------------------------------------
            # STEP 5 – Surveys tab + wait for cards
            # ------------------------------------------------------------------
            status_ph.info("📋 Finding surveys tab...")
            surveys_tab_clicked = False
            for sel in [
                "div:nth-child(2) > .p-nav-wrapper > .p-nav-item",
                ".p-nav-item:has-text('Surveys')",
                "a:has-text('Surveys')",
                "nav a:has-text('Surveys')",
                "li a:has-text('Surveys')",
                "[href*='survey']",
            ]:
                try:
                    nav = page.locator(sel).first
                    if await nav.is_visible(timeout=3000):
                        await nav.click()
                        self.log(f"✅ Clicked Surveys nav tab: {sel}", batch_id=batch_id)
                        surveys_tab_clicked = True
                        await page.wait_for_timeout(4000)
                        break
                except Exception:
                    pass

            if not surveys_tab_clicked:
                self.log("⚠️ Could not click Surveys tab — staying on current page",
                         "WARNING", batch_id=batch_id)

            status_ph.info("⏳ Waiting for surveys to load...")
            surveys_found = await asyncio.wait_for(
                self._wait_for_surveys_to_load(
                    page, batch_id,
                    max_reloads=5, reload_wait=6.0,
                    poll_interval=2.0, max_polls_per_load=10,
                ),
                timeout=180.0,
            )
            if not surveys_found:
                raise Exception("Surveys never loaded after exhausting all reload attempts.")

            await self._screenshot(page, "01_survey_tab_open", batch_id)

            # ------------------------------------------------------------------
            # STEP 6 – LLM setup
            # ------------------------------------------------------------------
            model_cfg = MODEL_REGISTRY[model_choice]
            api_key   = os.getenv(MODEL_ENV_KEYS[model_choice], "")
            if not api_key:
                raise Exception(f"Missing API key for {model_choice}")
            llm = model_cfg["cls"](**{**model_cfg["kwargs"], "api_key": api_key})
            self.log(f"✅ LLM ready: {model_choice}", batch_id=batch_id)

            persona = self._build_persona_system_message(prompt, acct)

            # ------------------------------------------------------------------
            # STEP 7 – Survey loop
            # ------------------------------------------------------------------
            for i in range(num_surveys):
                survey_num = i + 1
                self.log(f"── Survey {survey_num}/{num_surveys} ──", batch_id=batch_id)
                status_ph.info(f"🤖 Survey {survey_num}/{num_surveys}...")
                progress_ph.progress(i / num_surveys, text=f"Survey {survey_num}/{num_surveys}")

                st.session_state.survey_progress.append(
                    {"num": survey_num, "status": STATUS_PENDING, "note": "Running..."}
                )

                survey_status  = STATUS_ERROR
                result_snippet = ""

                try:
                    # ── Open survey card ──────────────────────────────────────
                    try:
                        survey_page = await asyncio.wait_for(
                            self._open_survey_card(page, batch_id),
                            timeout=60.0,
                        )
                        if survey_page != page:
                            page = survey_page
                            context = page.context
                        self.log(f"🌐 Active survey URL: {page.url}", batch_id=batch_id)
                    except asyncio.TimeoutError:
                        raise Exception("Timed out opening survey card after 60s")

                    await asyncio.sleep(2)
                    await self._screenshot(page, "02_qualification_start", batch_id, survey_num)

                    # ── DETECT: is Participate already visible right now? ─────
                    # If button.p-btn--fill is already on screen, there are no
                    # qualification questions — skip the agent and click directly.
                    participate_sel = "button.p-btn--fill"
                    participate_now = False
                    try:
                        btn = page.locator(participate_sel).first
                        if await btn.is_visible(timeout=3000):
                            participate_now = True
                            self.log(
                                "⚡ Participate visible immediately — no qual questions, clicking now",
                                batch_id=batch_id,
                            )
                    except Exception:
                        pass

                    if not participate_now:
                        # ── QUAL + PARTICIPATE AGENT ──────────────────────────
                        # Single agent handles both scenarios:
                        #   A) Qual questions are present → answer them all,
                        #      then click Participate when it appears.
                        #   B) Participate is visible with no questions →
                        #      click it immediately.
                        #
                        # CRITICAL: bu_browser_qual is always closed in finally
                        # with a 3 s settle before Playwright resumes.  This
                        # prevents the empty AgentHistoryList CDP conflict.
                        bu_browser_qual = Browser(
                            config=BrowserConfig(cdp_url=ws_url, headless=False)
                        )

                        qual_agent = Agent(
                            task=f"""
{persona}

════════════════════════════════════
QUALIFICATION + PARTICIPATE
════════════════════════════════════
You are on a survey platform. One of two situations is true:

SITUATION A — Qualification questions are shown before Participate:
  Answer ALL qualification questions, then click the green Participate button
  (CSS selector: button.p-btn--fill) when it becomes visible.

SITUATION B — Participate button is already visible with no questions:
  Click the green Participate button (button.p-btn--fill) immediately.

HOW TO ANSWER QUESTIONS (if present):
1. Read each question and answer based on the persona's real attributes.
2. Checkboxes: tick all that apply to the persona.
3. Radio buttons: click the single best matching option.
4. Dropdowns (div.options): click the container first, then pick the option.
   Use ArrowDown keys to navigate dropdown options if needed.
5. Number inputs: type the number ONLY — no $, no commas, no symbols.
6. After answering each page/section, click the Next / Continue / ➔ arrow
   button (button#submitButton or .p-btn-icon--right > svg).
7. "I can't answer this question" — only use if truly not applicable.
8. There may be MULTIPLE pages of questions — keep going until Participate
   becomes visible.

PARTICIPATE BUTTON:
- CSS selector: button.p-btn--fill
- This is the green button labelled "Participate".
- Click it as soon as it is visible — whether that is on the very first
  screen or only after answering several pages of questions.
- After clicking Participate the main survey will start loading. STOP there —
  do not answer the survey itself, that is handled separately.

STOP and report when:
a) You have clicked Participate → report PARTICIPATED
b) You see disqualification / "sorry" / "not eligible" → report DISQUALIFIED
c) You return to the survey list without clicking Participate → report DISQUALIFIED
""",
                            llm=llm,
                            browser=bu_browser_qual,
                            max_actions_per_step=5,
                        )

                        try:
                            qual_result = await asyncio.wait_for(
                                qual_agent.run(max_steps=50),
                                timeout=300.0,
                            )
                        except asyncio.TimeoutError:
                            raise Exception("Qualification/Participate agent timed out after 5 minutes")
                        finally:
                            # ALWAYS close the qual browser so CDP settles
                            # before Playwright touches the page again.
                            try:
                                await bu_browser_qual.close()
                            except Exception:
                                pass
                            await asyncio.sleep(3)  # let CDP fully release

                        qual_outcome = self._detect_survey_outcome(qual_result)
                        self.log(f"Qual+Participate agent result: {qual_outcome}", batch_id=batch_id)

                        await self._screenshot(page, "03_qualification_done", batch_id, survey_num)

                        # ── Disqualified during qualification ─────────────────
                        if qual_outcome == STATUS_FAILED:
                            self.log("❌ Disqualified during qualification", batch_id=batch_id)
                            survey_status  = STATUS_FAILED
                            result_snippet = "Disqualified during qualification"
                            st.session_state.survey_progress[-1] = {
                                "num": survey_num, "status": survey_status, "note": result_snippet,
                            }
                            failed_count += 1
                            survey_details.append({
                                "survey_number": survey_num,
                                "outcome":        survey_status,
                                "output_snippet": result_snippet,
                            })
                            self._record_survey_attempt(
                                account_id=acct["account_id"], site_id=site["site_id"],
                                survey_name=f"Survey_{survey_num}_{batch_id}",
                                batch_id=batch_id, status=survey_status,
                                notes=result_snippet[:300],
                            )
                            if i < num_surveys - 1:
                                await asyncio.sleep(3)
                                await self._wait_for_surveys_to_load(page, batch_id, max_reloads=3)
                            continue

                        # ── Safety net: agent may have stopped one action short ─
                        # If Participate is still visible after the agent closed,
                        # Playwright clicks it now.
                        try:
                            still_there = page.locator(participate_sel).first
                            if await still_there.is_visible(timeout=3000):
                                self.log(
                                    "⚠️ Participate still visible after agent — clicking via Playwright safety net",
                                    "WARNING", batch_id=batch_id,
                                )
                                participate_now = True
                        except Exception:
                            pass

                    # ── Playwright clicks Participate ─────────────────────────
                    # Runs for two cases:
                    #   1. Participate was visible immediately (fast path, no agent).
                    #   2. Safety net: agent stopped before clicking Participate.
                    if participate_now:
                        participated = False
                        for p_sel in [
                            "button.p-btn--fill",               # confirmed Automa selector
                            "span.p-btn-label:nth-child(1)",    # Automa label fallback
                            "button:has-text('Participate')",
                            "button:has-text('Get Started')",
                            "button:has-text('Begin')",
                        ]:
                            try:
                                btn = page.locator(p_sel).first
                                if await btn.is_visible(timeout=4000):
                                    await btn.scroll_into_view_if_needed()
                                    await asyncio.sleep(0.3)
                                    try:
                                        await btn.hover()
                                    except Exception:
                                        pass
                                    await btn.click(timeout=5000)
                                    self.log(f"✅ Participate clicked: {p_sel}", batch_id=batch_id)
                                    participated = True
                                    await asyncio.sleep(4)
                                    break
                            except Exception as e:
                                self.log(
                                    f"  Participate sel failed [{p_sel}]: {e}",
                                    "WARNING", batch_id=batch_id,
                                )

                        if not participated:
                            # JS fallback
                            try:
                                clicked = await page.evaluate("""
                                    () => {
                                        const btn = Array.from(
                                            document.querySelectorAll('button, a, div[role="button"]')
                                        ).find(el => {
                                            const txt = (el.innerText || '').toLowerCase();
                                            return txt.includes('participate') ||
                                                   txt.includes('get started') ||
                                                   txt.includes('begin');
                                        });
                                        if (btn) {
                                            btn.scrollIntoView({ block: 'center' });
                                            btn.click();
                                            return true;
                                        }
                                        return false;
                                    }
                                """)
                                if clicked:
                                    self.log("✅ Participate clicked via JS fallback", batch_id=batch_id)
                                    participated = True
                                    await asyncio.sleep(4)
                                else:
                                    self.log(
                                        "❌ Participate button not found after all attempts",
                                        "WARNING", batch_id=batch_id,
                                    )
                            except Exception as pe:
                                self.log(f"JS participate fallback error: {pe}", "WARNING", batch_id=batch_id)

                    # ── Detect if survey opened in a NEW TAB ──────────────────
                    # Some external survey providers open in a new tab after
                    # Participate is clicked.  Follow the user there.
                    try:
                        new_tab = await context.wait_for_event("page", timeout=5000)
                        await new_tab.wait_for_load_state("domcontentloaded")
                        self.log(
                            f"✅ Survey opened in NEW TAB: {new_tab.url}",
                            batch_id=batch_id,
                        )
                        page = new_tab
                    except Exception:
                        # No new tab — survey is in the same tab (normal case)
                        self.log("Survey staying in same tab", batch_id=batch_id)

                    await self._screenshot(page, "04_survey_started", batch_id, survey_num)

                    # ── Agree and Continue gate (consent popup) ───────────────
                    try:
                        agree_btn = page.locator("button#gtm-agree-button").first
                        if await agree_btn.is_visible(timeout=4000):
                            await agree_btn.click()
                            self.log("✅ Clicked 'Agree and Continue'", batch_id=batch_id)
                            await asyncio.sleep(3)
                    except Exception:
                        pass

                    # ── Wait for first main survey question ───────────────────
                    self.log("Waiting for main survey questions to load...", batch_id=batch_id)
                    survey_started = False
                    for _ in range(15):
                        for sel in [
                            "input[type='radio']", "input[type='checkbox']",
                            "textarea", "input.p-input",
                            "button:has-text('Next')", "button:has-text('Continue')",
                            "[class*='question']", "[class*='Question']",
                            "input#ctl00_Content_btnContinue",
                        ]:
                            try:
                                if await page.locator(sel).first.is_visible(timeout=1000):
                                    survey_started = True
                                    self.log(
                                        f"✅ Main survey question visible: {sel}",
                                        batch_id=batch_id,
                                    )
                                    break
                            except Exception:
                                pass
                        if survey_started:
                            break
                        await asyncio.sleep(2)

                    if not survey_started:
                        self.log(
                            "⚠️ Main survey questions never appeared — proceeding anyway",
                            "WARNING", batch_id=batch_id,
                        )

                    # ── MAIN SURVEY AGENT — fresh isolated browser ────────────
                    # Created AFTER Participate is clicked + CDP has settled.
                    # Completely independent from bu_browser_qual (already closed).
                    bu_browser_main = Browser(
                        config=BrowserConfig(cdp_url=ws_url, headless=False)
                    )

                    main_agent = Agent(
                        task=f"""
{persona}

════════════════════════════════════
MAIN SURVEY
════════════════════════════════════
You have passed qualification and the main survey has started. Answer ALL questions.

INSTRUCTIONS:
- Radio / multiple choice: pick the best match for the persona.
- Checkboxes: select all that apply to the persona.
- Dropdowns (div.options): click the container, then pick with ArrowDown keys.
- Free-text / open-ended: 1–2 natural sentences in the persona's voice.
- Number inputs: type number only, no symbols.
- Never leave a required field blank.
- Click Next / Continue / Submit (input#ctl00_Content_btnContinue) after each page.
- Pause 2–4 seconds between actions.
- Follow attention-check questions exactly as written.
- Never contradict previous answers.

STOP when:
- "Thank You" / completion / reward credited page → report SUCCESS
- Disqualification page → report DISQUALIFIED
- Unrecoverable dead end → report ERROR
""",
                        llm=llm,
                        browser=bu_browser_main,
                        max_actions_per_step=5,
                    )

                    try:
                        result = await asyncio.wait_for(
                            main_agent.run(max_steps=60),
                            timeout=480.0,
                        )
                    except asyncio.TimeoutError:
                        raise Exception("Main survey timed out after 8 minutes")
                    finally:
                        try:
                            await bu_browser_main.close()
                        except Exception:
                            pass
                        await asyncio.sleep(1)

                    result_snippet = str(result)[:500]

                    if hasattr(result, "history") and result.history:
                        for idx, h in enumerate(result.history[-5:]):
                            self.log(f"  agent[-{5-idx}]: {str(h)[:200]}", batch_id=batch_id)

                    survey_status = self._detect_survey_outcome(result)
                    self.log(f"Survey {survey_num} → {survey_status}", batch_id=batch_id)

                except Exception as se:
                    self.log(f"Survey {survey_num} exception: {se}", "ERROR", batch_id=batch_id)
                    survey_status  = STATUS_ERROR
                    result_snippet = str(se)[:300]

                # ── Screenshot 5: final state ─────────────────────────────────
                try:
                    await self._screenshot(page, "05_survey_complete", batch_id, survey_num)
                except Exception as sse:
                    self.log(f"Final screenshot failed: {sse}", "WARNING", batch_id=batch_id)

                # ── Tally + record ────────────────────────────────────────────
                st.session_state.survey_progress[-1] = {
                    "num": survey_num, "status": survey_status, "note": result_snippet[:100],
                }
                if survey_status == STATUS_COMPLETE:  complete_count += 1
                elif survey_status == STATUS_PASSED:  passed_count   += 1
                elif survey_status == STATUS_FAILED:  failed_count   += 1
                else:                                 error_count    += 1

                survey_details.append({
                    "survey_number": survey_num,
                    "outcome":        survey_status,
                    "output_snippet": result_snippet,
                })
                self._record_survey_attempt(
                    account_id=acct["account_id"], site_id=site["site_id"],
                    survey_name=f"Survey_{survey_num}_{batch_id}",
                    batch_id=batch_id, status=survey_status,
                    notes=result_snippet[:300],
                )

                if i < num_surveys - 1:
                    self.log("⏳ Waiting for survey list to reload...", batch_id=batch_id)
                    await asyncio.sleep(3)
                    await self._wait_for_surveys_to_load(page, batch_id, max_reloads=3)

            # ------------------------------------------------------------------
            # STEP 8 – Stop Chrome
            # ------------------------------------------------------------------
            self.log("Stopping Chrome session – cookies will be synced", batch_id=batch_id)
            stop_result = self.chrome_manager.stop_session(session_id)
            if stop_result.get('success'):
                self.log("✅ Chrome closed, profile state saved", batch_id=batch_id)
            else:
                self.log(f"⚠️ Stop had issues: {stop_result.get('error')}", "WARNING", batch_id=batch_id)

            progress_ph.progress(1.0, text="Done!")
            summary = (
                f"✅ {complete_count} complete  🟡 {passed_count} passed  "
                f"❌ {failed_count} failed  ⚠️ {error_count} error"
            )
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
            for obj in (page, context, pw_browser, playwright_instance):
                if obj:
                    try:
                        if hasattr(obj, "close"):
                            await obj.close()
                        elif hasattr(obj, "stop"):
                            await obj.stop()
                    except Exception:
                        pass
            if session_id and session_id in self.chrome_manager.active_processes:
                try:
                    self.chrome_manager.stop_session(session_id)
                except Exception:
                    pass
            st.session_state.generation_in_progress = False
            st.rerun()
    # =========================================================================
    # Google login
    # =========================================================================
    async def _perform_google_login(self, page, email: str, password: str, batch_id: str):
        self.log("→ Navigating to Google sign-in", batch_id=batch_id)
        await page.goto(
            "https://accounts.google.com/signin/v2/identifier",
            wait_until="domcontentloaded", timeout=30_000,
        )
        await page.wait_for_timeout(2000)

        try:
            email_sel = 'input[type="email"], input[name="identifier"]'
            await page.wait_for_selector(email_sel, timeout=15_000)
            await page.fill(email_sel, email)
            self.log(f"✅ Email filled: {email}", batch_id=batch_id)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(4000)
        except Exception as e:
            raise Exception(f"Google email step failed: {e}")

        try:
            self.log("Waiting for password input...", batch_id=batch_id)
            pwd_sel = 'input[type="password"], input[name="Passwd"]'
            for ct in ["Verify it's you", "Confirm it's you", "This extra step",
                       "Get a verification code", "Check your phone", "Try another way"]:
                try:
                    if await page.locator(f"text='{ct}'").first.is_visible(timeout=1500):
                        raise Exception(f"Google security challenge detected: '{ct}'.")
                except Exception as ce:
                    if "security challenge" in str(ce):
                        raise
            await page.wait_for_selector(pwd_sel, timeout=20_000)
            await page.fill(pwd_sel, password)
            self.log("✅ Password filled", batch_id=batch_id)
            await page.keyboard.press("Enter")
            await page.wait_for_timeout(5000)
        except Exception as e:
            raise Exception(f"Google password step failed: {e}")

        for btn_text in ["Stay signed in", "Yes", "Continue", "Not now", "Remind me later"]:
            try:
                btn = page.locator(f"button:has-text('{btn_text}')").first
                if await btn.is_visible(timeout=2000):
                    await btn.click()
                    self.log(f"Clicked post-login prompt: '{btn_text}'", batch_id=batch_id)
                    await page.wait_for_timeout(2000)
                    break
            except Exception:
                pass

        final_url = page.url
        self.log(f"Post-login URL: {final_url}", batch_id=batch_id)
        if "accounts.google.com/signin" in final_url and "challenge" not in final_url:
            raise Exception("Still on Google sign-in page after password attempt.")
        self.log("✅ Password login successful", batch_id=batch_id)

    # =========================================================================
    # Persona builder
    # =========================================================================
    def _build_persona_system_message(self, prompt: Dict, acct: Dict) -> str:
        lines = ["You are a specific person answering survey questions. Embody this identity:", ""]
        for field, label in [
            ("age","Age"), ("gender","Gender"), ("city","City/Location"),
            ("education_level","Education"), ("job_status","Employment"),
            ("income_range","Income"), ("marital_status","Marital status"),
            ("household_size","Household size"), ("industry","Industry"),
        ]:
            if acct.get(field):
                lines.append(f"• {label}: {acct[field]}")
        if acct.get("has_children") is not None:
            lines.append(f"• Has children: {'Yes' if acct['has_children'] else 'No'}")
        if prompt and prompt.get("content"):
            lines += ["", "Additional persona details:", prompt["content"].strip()]
        lines += [
            "", "Rules:",
            "- Stay consistent throughout the survey",
            "- For free-text: 1–2 natural sentences",
            "- Pick the most characteristic answer for this persona",
        ]
        return "\n".join(lines)

    # =========================================================================
    # DB write
    # =========================================================================
    def _record_survey_attempt(self, account_id, site_id, survey_name,
                               batch_id, status, notes=""):
        status = {"completed": STATUS_COMPLETE, "disqualified": STATUS_FAILED,
                  "incomplete": STATUS_ERROR}.get(status, status)
        if status not in {STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR}:
            status = STATUS_ERROR
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        INSERT INTO screening_results
                            (account_id,site_id,survey_name,batch_id,screener_answers,
                             status,started_at,completed_at,notes)
                        VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s)
                    """, (account_id, site_id, survey_name, batch_id, 1,
                          status, (notes or "")[:1000]))
                    conn.commit()
                    self.log(f"✅ Recorded: {survey_name} → {status}")
        except Exception as e:
            self.log(f"❌ _record_survey_attempt: {e}", "ERROR")

    # =========================================================================
    # Screening results tab
    # =========================================================================
    def _tab_screening_results(self, acct, site):
        st.subheader("🏆 Survey Attempts")
        results = self._load_screening_results(acct["account_id"], site["site_id"])
        if not results:
            st.info("No attempts yet.")
            return

        total      = len(results)
        complete_n = sum(1 for r in results if r["status"] == STATUS_COMPLETE)
        passed_n   = sum(1 for r in results if r["status"] == STATUS_PASSED)
        failed_n   = sum(1 for r in results if r["status"] == STATUS_FAILED)
        error_n    = sum(1 for r in results if r["status"] in (STATUS_ERROR, STATUS_PENDING))

        c1,c2,c3,c4,c5 = st.columns(5)
        c1.metric("Total",       total)
        c2.metric("✅ Complete", complete_n)
        c3.metric("🟡 Passed",  passed_n)
        c4.metric("❌ Failed",  failed_n)
        c5.metric("⚠️ Error",  error_n)

        success_n = complete_n + passed_n
        if total > 0:
            st.progress(success_n / total,
                        text=f"Success rate: {int(success_n/total*100)}% ({success_n}/{total})")

        batches = sorted({r.get("batch_id") for r in results if r.get("batch_id")})
        if batches:
            sel = st.selectbox("Filter by batch:", ["All"] + batches, key="batch_filter")
            if sel != "All":
                results = [r for r in results if r.get("batch_id") == sel]

        st.markdown("---")
        for r in results:
            icon = {"complete":"✅","passed":"🟡","failed":"❌","pending":"⏳","error":"⚠️"}.get(
                r["status"],"❓")
            ts = r["started_at"].strftime("%Y-%m-%d %H:%M") if r.get("started_at") else "?"
            with st.expander(
                f"{icon} **{r.get('survey_name','?')}** — {r['status'].upper()} — {ts}",
                expanded=False,
            ):
                ci, ca = st.columns([3,1])
                with ci:
                    st.markdown(
                        f"**Batch:** `{r.get('batch_id','—')}`  \n"
                        f"**Started:** {ts}  \n"
                        f"**Completed:** "
                        f"{r['completed_at'].strftime('%Y-%m-%d %H:%M') if r.get('completed_at') else '—'}"
                    )
                    if r.get("notes"):
                        st.caption(r["notes"])
                with ca:
                    rid = r["result_id"]
                    if r["status"] != STATUS_COMPLETE:
                        if st.button("✅ Mark Complete", key=f"pass_{rid}", use_container_width=True):
                            self._update_screening_status(rid, STATUS_COMPLETE); st.rerun()
                    if r["status"] != STATUS_FAILED:
                        if st.button("❌ Mark DQ", key=f"fail_{rid}", use_container_width=True):
                            self._update_screening_status(rid, STATUS_FAILED); st.rerun()
                    note = st.text_input("Note:", key=f"note_{rid}", placeholder="Optional…")
                    if note and st.button("💾 Save", key=f"savenote_{rid}", use_container_width=True):
                        self._save_screening_note(rid, note); st.rerun()
                    if r.get("batch_id") and r["batch_id"] in st.session_state.batches:
                        if st.button("📋 View batch", key=f"vb_{rid}", use_container_width=True):
                            st.session_state.selected_batch_for_details = r["batch_id"]
                            st.rerun()

        if st.session_state.get("selected_batch_for_details"):
            bid = st.session_state.selected_batch_for_details
            st.markdown(f"### 📁 Batch: `{bid}`")
            self._display_batch_details(bid)
            if st.button("Close batch view", key="close_batch"):
                st.session_state.selected_batch_for_details = None
                st.rerun()

        st.markdown("---")
        if st.button("📥 Export CSV", key="exp_csv"):
            buf = io.StringIO()
            w = csv.DictWriter(buf, fieldnames=[
                "survey_name","status","started_at","completed_at","batch_id","notes"
            ])
            w.writeheader()
            for r in results:
                w.writerow({
                    "survey_name":  r.get("survey_name",""),
                    "status":       r.get("status",""),
                    "started_at":   str(r.get("started_at","")),
                    "completed_at": str(r.get("completed_at","")),
                    "batch_id":     r.get("batch_id",""),
                    "notes":        r.get("notes",""),
                })
            st.download_button(
                "⬇️ Download CSV", buf.getvalue(),
                f"screening_{acct['username']}_{site['site_name'].replace(' ','_')}.csv",
                mime="text/csv", key="dl_csv",
            )

    # =========================================================================
    # Results display
    # =========================================================================
    def _render_results(self, r: Dict):
        if r.get("action") != "direct_answering":
            return
        st.subheader("✅ Run Results")
        if r.get("status") == "failed":
            st.error(f"❌ {r.get('error','Unknown error')}")
            if st.button("Clear", key="clr_fail"):
                st.session_state.generation_results = None; st.rerun()
            return
        c1,c2,c3,c4 = st.columns(4)
        c1.metric("✅ Complete",  r.get("complete",0))
        c2.metric("🟡 Passed",   r.get("passed",0))
        c3.metric("❌ Failed/DQ", r.get("failed",0))
        c4.metric("⚠️ Error",    r.get("error",0))
        for d in r.get("details", []):
            icon = {"complete":"✅","passed":"🟡","failed":"❌"}.get(d["outcome"],"⚠️")
            st.write(f"{icon} Survey {d['survey_number']}: **{d['outcome']}**")
            if d.get("output_snippet"):
                with st.expander(f"Details #{d['survey_number']}"):
                    st.code(d["output_snippet"])
        st.caption(
            f"Account: {r['account']['username']} | Site: {r['site']['name']} | "
            f"Model: {r.get('model','')} | Batch: {r.get('batch_id','')} | {r.get('timestamp','')}"
        )
        if st.button("Clear results", key="clr_res"):
            st.session_state.generation_results = None
            st.session_state.survey_progress    = []
            st.rerun()

    # =========================================================================
    # DB helpers
    # =========================================================================
    def _pg(self):
        return get_postgres_connection()

    def _get_urls(self, account_id, site_id):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT url_id,url,is_default,is_used,used_at,notes "
                        "FROM account_urls WHERE account_id=%s AND site_id=%s "
                        "ORDER BY is_default DESC,created_at DESC",
                        (account_id, site_id),
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_get_urls: {e}"); return []

    def _load_screening_results(self, account_id, site_id):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT result_id,survey_name,batch_id,status,started_at,completed_at,notes "
                        "FROM screening_results WHERE account_id=%s AND site_id=%s "
                        "ORDER BY started_at DESC",
                        (account_id, site_id),
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_screening_results: {e}"); return []

    def _update_screening_status(self, result_id, status):
        if status not in {STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR}:
            return
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    if status in (STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED):
                        c.execute(
                            "UPDATE screening_results SET status=%s,completed_at=CURRENT_TIMESTAMP "
                            "WHERE result_id=%s", (status, result_id))
                    else:
                        c.execute("UPDATE screening_results SET status=%s WHERE result_id=%s",
                                  (status, result_id))
                    conn.commit()
        except Exception as e:
            logger.error(f"_update_screening_status: {e}")

    def _save_screening_note(self, result_id, note):
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE screening_results SET notes=%s WHERE result_id=%s",
                              (note, result_id))
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
                    c.execute(
                        "SELECT prompt_id,account_id,name AS prompt_name,content,prompt_type "
                        "FROM prompts WHERE is_active=TRUE"
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_prompts: {e}"); return []