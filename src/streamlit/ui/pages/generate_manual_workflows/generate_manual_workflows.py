"""
Generate Manual Workflows — Streamlit page v3.9.0
- Uses browser-use with Browserless WebSocket for AI browser automation
- Applies proxy settings from database, with UI to edit/update them
- Uses cookies from database for persistent login sessions
- AI-only: no extraction or workflow creation features
- FIXED: status values now match schema CHECK constraint
- FIXED: real-time status feedback during agent run
- FIXED: better completion detection from agent result
- FIXED: cookie injection timing (before navigation, not after)
- NEW: Screenshot capture after each survey attempt
- NEW: Per-run logs and screenshots stored with batch ID
- NEW: Filterable view to inspect logs and screenshots of any run
- FIXED: Duplicate element keys in batch details display
- NEW: Cookie management UI (upload, view, validate) in the AI page
"""

import asyncio
import csv
import io
import json
import logging
import os
import time
import traceback
from datetime import datetime
from typing import Any, Dict, List, Optional
from urllib.parse import quote

import numpy as np
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
# Schema-valid status values
# ---------------------------------------------------------------------------
STATUS_COMPLETE      = "complete"
STATUS_PASSED        = "passed"
STATUS_FAILED        = "failed"
STATUS_PENDING       = "pending"
STATUS_ERROR         = "error"

STATUS_LABELS = {
    STATUS_COMPLETE: "✅ Completed",
    STATUS_PASSED:   "🟡 Passed Screener",
    STATUS_FAILED:   "❌ Disqualified",
    STATUS_PENDING:  "⏳ Pending",
    STATUS_ERROR:    "⚠️ Error",
}

# ---------------------------------------------------------------------------
# Model registry
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
    "gemini — Gemini 2.5 Flash": {
        "cls": ChatGoogle,
        "kwargs": {"model": "gemini-2.5-flash", "temperature": 0.7}
    },
}

MODEL_ENV_KEYS: Dict[str, str] = {
    "openai — GPT-4o": "OPENAI_API_KEY",
    "anthropic — Claude 3.5": "ANTHROPIC_API_KEY",
    "gemini — Gemini 2.5 Flash": "GEMINI_API_KEY",
}

# ---------------------------------------------------------------------------
# Completion detection helpers
# ---------------------------------------------------------------------------
COMPLETE_KEYWORDS = [
    "thank you", "thank-you", "thankyou",
    "survey complete", "survey completed", "you have completed",
    "submission received", "response recorded",
    "reward", "points added", "earned", "credited",
    "all done", "finished", "successfully submitted",
]

DISQUALIFIED_KEYWORDS = [
    "disqualif", "screen out", "screened out",
    "not eligible", "don't qualify", "do not qualify",
    "unfortunately", "not a match", "not selected",
    "quota full", "quota reached",
    "sorry, ", "we're sorry",
]

def _detect_survey_outcome(result) -> str:
    try:
        result_str = str(result).lower()
        history_str = ""
        if hasattr(result, "history") and result.history:
            history_str = " ".join(str(h) for h in result.history[-5:]).lower()
        combined = result_str + " " + history_str

        if any(kw in combined for kw in DISQUALIFIED_KEYWORDS):
            return STATUS_FAILED
        if any(kw in combined for kw in COMPLETE_KEYWORDS):
            return STATUS_COMPLETE
        if hasattr(result, "final_url"):
            url = (result.final_url or "").lower()
            if any(kw in url for kw in ["complete", "finish", "done", "thank"]):
                return STATUS_COMPLETE
            if any(kw in url for kw in ["disqualif", "screenout", "sorry"]):
                return STATUS_FAILED
        return STATUS_ERROR
    except Exception:
        return STATUS_ERROR

def run_async(coro):
    try:
        loop = asyncio.get_running_loop()
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
        self._ensure_proxy_configs_table()
        # Session state initialisation
        for k, v in {
            "generation_in_progress": False,
            "generation_results": None,
            "generation_logs": [],
            "editing_proxy": False,
            "temp_proxy": None,
            "survey_progress": [],
            "batches": {},          # batch_id -> {logs: list, screenshots: list, timestamp}
            "batch_details_counter": 0,  # for unique keys in batch details
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

    def _verify_schema_status_constraint(self) -> Dict[str, Any]:
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        SELECT pg_get_constraintdef(oid)
                        FROM pg_constraint
                        WHERE conrelid = 'screening_results'::regclass
                          AND contype = 'c'
                          AND conname LIKE '%status%'
                    """)
                    row = c.fetchone()
                    if row:
                        constraint_def = row[0]
                        import re
                        vals = re.findall(r"'([^']+)'", constraint_def)
                        our_values = {STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED,
                                      STATUS_PENDING, STATUS_ERROR}
                        schema_values = set(vals)
                        missing = our_values - schema_values
                        return {
                            "ok": len(missing) == 0,
                            "schema_values": sorted(schema_values),
                            "our_values": sorted(our_values),
                            "missing": sorted(missing),
                        }
            return {"ok": True, "note": "No constraint found — inserts will succeed"}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def _get_account_proxy(self, account_id: int) -> Optional[Dict[str, Any]]:
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
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        UPDATE proxy_configs
                        SET is_active = FALSE
                        WHERE account_id = %s AND is_active = TRUE
                    """, (account_id,))
                    c.execute("""
                        INSERT INTO proxy_configs (account_id, proxy_type, host, port, username, password, is_active)
                        VALUES (%s, %s, %s, %s, %s, %s, TRUE)
                        RETURNING proxy_id
                    """, (account_id, proxy_type, host, port, username or None, password or None))
                    proxy_id = c.fetchone()[0]
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
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("""
                        SELECT cookie_data, cookie_count, updated_at
                        FROM account_cookies
                        WHERE account_id = %s AND is_active = TRUE
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
        try:
            cookie_info = await self._get_account_cookies_raw(account_id)
            if not cookie_info:
                return None
            if not cookie_info.get("cookie_count", 0):
                return None
            cookies = cookie_info["cookie_data"]
            if isinstance(cookies, str):
                cookies = json.loads(cookies)
            formatted_cookies = []
            for c in cookies:
                same_site = c.get("sameSite", "Lax")
                if same_site not in ("Strict", "Lax", "None"):
                    same_site = "Lax"
                formatted_cookies.append({
                    "name":     c["name"],
                    "value":    c["value"],
                    "domain":   c.get("domain", ""),
                    "path":     c.get("path", "/"),
                    "secure":   c.get("secure", False),
                    "httpOnly": c.get("httpOnly", False),
                    "sameSite": same_site,
                })
            return formatted_cookies if formatted_cookies else None
        except Exception as e:
            logger.error(f"Error fetching cookies for injection: {e}")
            return None

    # ------------------------------------------------------------------
    # Synchronous cookie management for UI
    # ------------------------------------------------------------------
    def _store_account_cookies_sync(self, account_id: int, cookies_json: str, username: str) -> Dict[str, Any]:
        """Store cookies for an account in PostgreSQL (synchronous)."""
        try:
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            self.log(f"Storing cookies for account {account_id} ({username})")

            try:
                cookies = json.loads(cookies_json)
            except json.JSONDecodeError as e:
                return {'success': False, 'error': f"Invalid JSON format: {str(e)}"}

            if not isinstance(cookies, list):
                return {'success': False, 'error': "Cookies must be a JSON array"}

            required_fields = ['name', 'value', 'domain']
            for i, cookie in enumerate(cookies):
                missing = [f for f in required_fields if f not in cookie]
                if missing:
                    return {'success': False, 'error': f"Cookie #{i} missing fields: {missing}"}

            cookie_count = len(cookies)
            self.log(f"Validated {cookie_count} cookies")

            with self._pg() as conn:
                with conn.cursor() as c:
                    # Deactivate previous cookies
                    deactivate_query = """
                    UPDATE account_cookies
                    SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
                    WHERE account_id = %s AND is_active = TRUE
                    """
                    c.execute(deactivate_query, (account_id,))
                    self.log("Deactivated previous cookies")

                    cookie_json_string = json.dumps(cookies)

                    insert_query = """
                    INSERT INTO account_cookies (
                        account_id, cookie_data, cookie_count,
                        uploaded_at, updated_at, is_active, cookie_source
                    )
                    VALUES (%s, %s::jsonb, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE, 'manual_workflows')
                    RETURNING cookie_id
                    """
                    c.execute(insert_query, (account_id, cookie_json_string, cookie_count))
                    result = c.fetchone()
                    if not result:
                        raise Exception("INSERT returned no rows")
                    cookie_id = result[0] if isinstance(result, tuple) else result.get('cookie_id')

                    # Update accounts table
                    update_account_query = """
                    UPDATE accounts
                    SET has_cookies = TRUE, cookies_last_updated = CURRENT_TIMESTAMP
                    WHERE account_id = %s
                    """
                    c.execute(update_account_query, (account_id,))
                    conn.commit()

            self.log(f"✓ Stored cookies with ID: {cookie_id}")
            return {
                'success': True,
                'cookie_id': cookie_id,
                'cookie_count': cookie_count,
                'account_id': account_id
            }
        except Exception as e:
            error_msg = f"Failed to store cookies: {str(e)}"
            self.log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _get_account_cookies_sync(self, account_id: int) -> Dict[str, Any]:
        """Get active cookies for an account from PostgreSQL (synchronous)."""
        try:
            if hasattr(account_id, 'item'):
                account_id = int(account_id)
            elif isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            query = """
            SELECT cookie_id, cookie_data::text, cookie_count,
                uploaded_at, updated_at, cookie_source, notes
            FROM account_cookies
            WHERE account_id = %s AND is_active = TRUE
            ORDER BY uploaded_at DESC
            LIMIT 1
            """
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(query, (account_id,))
                    row = c.fetchone()
                    if not row:
                        return {'has_cookies': False}

                    cookie_data_raw = row.get('cookie_data')
                    cookie_data = None
                    if cookie_data_raw:
                        if isinstance(cookie_data_raw, str):
                            cookie_data = json.loads(cookie_data_raw)
                        elif isinstance(cookie_data_raw, (list, dict)):
                            cookie_data = cookie_data_raw

                    return {
                        'has_cookies': True,
                        'cookie_id': row.get('cookie_id'),
                        'cookie_data': cookie_data,
                        'cookie_count': row.get('cookie_count', 0),
                        'uploaded_at': row.get('uploaded_at'),
                        'updated_at': row.get('updated_at'),
                        'cookie_source': row.get('cookie_source'),
                        'notes': row.get('notes')
                    }
        except Exception as e:
            self.log(f"Failed to get cookies for account {account_id}: {e}", "ERROR")
            return {'has_cookies': False, 'error': str(e)}

    async def _get_browser_context(self, browser):
        """Try multiple ways to get the Playwright browser context."""
        # Approach 1: direct .context attribute
        if hasattr(browser, 'context') and browser.context:
            return browser.context
        # Approach 2: private _context
        if hasattr(browser, '_context') and browser._context:
            return browser._context
        # Approach 3: via playwright_browser contexts list
        try:
            pb = getattr(browser, 'playwright_browser', None)
            if pb and pb.contexts:
                return pb.contexts[0]
        except Exception:
            pass
        # Approach 4: get current page and return its context
        try:
            page = await browser.get_current_page()
            if page:
                return page.context
        except Exception:
            pass
        return None

    def _check_cookie_validity_sync(self, account_id: int) -> Dict[str, Any]:
        """Check if stored cookies are still valid (synchronous)."""
        try:
            cookie_info = self._get_account_cookies_sync(account_id)
            if not cookie_info['has_cookies']:
                return {'valid': False, 'reason': 'No cookies stored', 'needs_capture': True}

            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                cookies = json.loads(cookies)

            # FIXED: look for 'auth-token' (hyphen) not 'auth_token' (underscore)
            auth_token = next((c for c in cookies if c['name'] in ('auth-token', 'auth_token', 'session', 'token')), None)
            if not auth_token:
                return {'valid': False, 'reason': 'No auth token found', 'needs_capture': True}

            current_time = time.time()
            if auth_token.get('expirationDate') and auth_token['expirationDate'] < current_time:
                return {'valid': False, 'reason': 'auth_token expired', 'needs_capture': True}

            uploaded_at = cookie_info.get('uploaded_at')
            if uploaded_at and (datetime.now() - uploaded_at).days > 30:
                return {'valid': True, 'warning': 'Cookies are over 30 days old', 'age_days': (datetime.now() - uploaded_at).days}

            return {'valid': True, 'has_auth_token': True, 'cookie_count': len(cookies)}
        except Exception as e:
            return {'valid': False, 'reason': f'Validation error: {str(e)}', 'needs_capture': True}
    # ------------------------------------------------------------------
    # Logging helpers (per batch)
    # ------------------------------------------------------------------
    def log(self, msg: str, level: str = "INFO", batch_id: Optional[str] = None):
        ts = datetime.now().strftime("%H:%M:%S")
        entry = f"[{ts}] {level}: {msg}"
        # Always store in global logs for backward compatibility
        st.session_state.generation_logs.append(entry)
        if len(st.session_state.generation_logs) > 200:
            st.session_state.generation_logs = st.session_state.generation_logs[-200:]

        # Also store per batch if batch_id provided
        if batch_id and batch_id in st.session_state.batches:
            if "logs" not in st.session_state.batches[batch_id]:
                st.session_state.batches[batch_id]["logs"] = []
            st.session_state.batches[batch_id]["logs"].append(entry)
            # Keep last 200 per batch
            if len(st.session_state.batches[batch_id]["logs"]) > 200:
                st.session_state.batches[batch_id]["logs"] = st.session_state.batches[batch_id]["logs"][-200:]

        getattr(logger, level.lower(), logger.info)(msg)

    def clear_logs(self):
        st.session_state.generation_logs = []

    # ------------------------------------------------------------------
    # Screenshot capture
    # ------------------------------------------------------------------
    async def _capture_screenshot(self, browser, description: str = "") -> Optional[bytes]:
        """Capture a screenshot of the current browser page."""
        try:
            # browser.context is a Playwright browser context
            if not hasattr(browser, "context") or browser.context is None:
                self.log("No browser context available for screenshot", "WARNING")
                return None
            pages = browser.context.pages
            if not pages:
                self.log("No pages open for screenshot", "WARNING")
                return None
            page = pages[0]
            screenshot = await page.screenshot(type="png")
            self.log(f"Screenshot captured: {description}", "INFO")
            return screenshot
        except Exception as e:
            self.log(f"Screenshot capture failed: {e}", "ERROR")
            return None

    # ------------------------------------------------------------------
    # Cookie management UI
    # ------------------------------------------------------------------
    def _render_cookie_management(self, current_account=None):
        """Render a section to manage cookies for any account."""
        with st.expander("🍪 Manage Account Cookies", expanded=False):
            st.write("Manually upload or view cookies for any account. This is useful to provide fresh cookies without restarting a Chrome session.")

            accounts = self._load_accounts()
            if not accounts:
                st.warning("No accounts found.")
                return

            # Select account
            account_options = {f"{a['username']} (ID: {a['account_id']})": a for a in accounts}
            default_index = 0
            if current_account:
                for i, (label, a) in enumerate(account_options.items()):
                    if a['account_id'] == current_account.get('account_id'):
                        default_index = i
                        break
            selected_label = st.selectbox(
                "Select Account",
                options=list(account_options.keys()),
                index=default_index,
                key="cookie_mgmt_account"
            )
            selected_account = account_options[selected_label]

            # Cookie status
            cookie_info = self._get_account_cookies_sync(selected_account['account_id'])
            if cookie_info['has_cookies']:
                st.success(f"✓ Cookies stored ({cookie_info['cookie_count']})")
                validity = self._check_cookie_validity_sync(selected_account['account_id'])
                if validity.get('valid'):
                    st.success("✓ Cookies appear valid")
                    if validity.get('warning'):
                        st.warning(validity['warning'])
                else:
                    st.error(f"❌ Cookies invalid: {validity.get('reason', 'Unknown')}")
            else:
                st.warning("⚠️ No cookies stored for this account")

            col1, col2 = st.columns(2)
            with col1:
                cookies_json = st.text_area(
                    "Paste EditThisCookie JSON",
                    placeholder='[\n  {\n    "domain": ".x.com",\n    "name": "auth_token",\n    "value": "...",\n    ...\n  }\n]',
                    height=150,
                    key="cookie_mgmt_json",
                    help="Paste the JSON exported from EditThisCookie extension"
                )
                if st.button("💾 Save Cookies", use_container_width=True):
                    if not cookies_json.strip():
                        st.error("Please paste cookie JSON first.")
                    else:
                        result = self._store_account_cookies_sync(
                            selected_account['account_id'],
                            cookies_json,
                            selected_account['username']
                        )
                        if result['success']:
                            st.success(f"✓ {result['cookie_count']} cookies saved!")
                            st.rerun()
                        else:
                            st.error(f"❌ {result['error']}")

            with col2:
                if st.button("👁️ View Stored Cookies", use_container_width=True):
                    st.session_state[f'view_cookies_{selected_account["account_id"]}'] = True

            # Modal for viewing cookies
            if st.session_state.get(f'view_cookies_{selected_account["account_id"]}'):
                st.markdown("---")
                st.subheader(f"🍪 Stored Cookies for {selected_account['username']}")
                cookie_info = self._get_account_cookies_sync(selected_account['account_id'])
                if cookie_info['has_cookies']:
                    cookies = cookie_info['cookie_data']
                    if isinstance(cookies, str):
                        cookies = json.loads(cookies)

                    # Show critical cookies first (customize as needed)
                    critical_names = ['auth_token', 'session', 'token']
                    st.write("**Critical Authentication Cookies:**")
                    for name in critical_names:
                        cookie = next((c for c in cookies if c['name'] == name), None)
                        if cookie:
                            with st.expander(f"🔑 {name}", expanded=False):
                                st.json({
                                    'name': cookie.get('name'),
                                    'domain': cookie.get('domain'),
                                    'value': cookie.get('value')[:20] + '...',
                                    'secure': cookie.get('secure'),
                                    'httpOnly': cookie.get('httpOnly'),
                                })
                        else:
                            st.warning(f"⚠️ Missing: {name}")

                    st.write(f"**All Cookies ({len(cookies)} total):**")
                    st.json(cookies)
                else:
                    st.info("No cookies stored for this account")

                if st.button("Close", key=f"close_view_cookies_{selected_account['account_id']}"):
                    del st.session_state[f'view_cookies_{selected_account["account_id"]}']
                    st.rerun()

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

        # Schema check warning
        schema_check = self._verify_schema_status_constraint()
        if not schema_check.get("ok"):
            if schema_check.get("missing"):
                st.error(
                    f"⚠️ **Schema mismatch detected!** "
                    f"Status values used by code but not in DB constraint: "
                    f"`{schema_check['missing']}`. "
                    f"Survey attempts will fail to save. Run the migration SQL below."
                )
                with st.expander("🔧 Migration SQL to fix schema"):
                    st.code("""
-- Fix screening_results status CHECK constraint to match application
ALTER TABLE screening_results
    DROP CONSTRAINT IF EXISTS screening_results_status_check;

ALTER TABLE screening_results
    ADD CONSTRAINT screening_results_status_check
    CHECK (status IN ('pending', 'passed', 'failed', 'complete', 'error'));
""", language="sql")

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

        # Cookie management expander (now visible before answering)
        self._render_cookie_management(acct)

        # Live progress display
        if st.session_state.survey_progress:
            st.markdown("---")
            st.subheader("📊 Run Progress")
            for entry in st.session_state.survey_progress:
                icon = {
                    STATUS_COMPLETE: "✅",
                    STATUS_PASSED:   "🟡",
                    STATUS_FAILED:   "❌",
                    STATUS_PENDING:  "⏳",
                    STATUS_ERROR:    "⚠️",
                }.get(entry.get("status", STATUS_PENDING), "❓")
                st.write(f"{icon} Survey {entry['num']}: **{entry['status'].upper()}** — {entry.get('note', '')}")

        # Display logs and screenshots for the current batch (if any)
        if st.session_state.generation_results and st.session_state.generation_results.get("batch_id"):
            batch_id = st.session_state.generation_results["batch_id"]
            self._display_batch_details(batch_id)

        if st.session_state.generation_logs:
            st.markdown("---")
            with st.expander("📋 Global Logs (last 50)", expanded=False):
                st.code("\n".join(st.session_state.generation_logs[-50:]), language="log")
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
    # Batch details display (logs and screenshots)
    # ------------------------------------------------------------------
    def _display_batch_details(self, batch_id: str):
        """Show logs and screenshots for a specific batch, with unique keys."""
        batch = st.session_state.batches.get(batch_id)
        if not batch:
            st.info("No detailed logs/screenshots for this batch.")
            return

        # Increment counter for unique keys
        st.session_state.batch_details_counter += 1
        counter = st.session_state.batch_details_counter

        with st.expander(f"📁 Batch {batch_id} Details", expanded=False):
            # Logs
            if batch.get("logs"):
                st.subheader("📝 Logs")
                st.code("\n".join(batch["logs"][-100:]), language="log")
                st.download_button(
                    "Download Logs (this batch)",
                    "\n".join(batch["logs"]),
                    f"logs_{batch_id}.txt",
                    key=f"dl_logs_{batch_id}_{counter}"
                )
            # Screenshots
            if batch.get("screenshots"):
                st.subheader("📸 Screenshots")
                for i, (survey_num, img_bytes, status) in enumerate(batch["screenshots"]):
                    st.write(f"Survey {survey_num} – {status}")
                    st.image(img_bytes, use_container_width=True)
                    st.download_button(
                        f"Download Screenshot {survey_num}",
                        img_bytes,
                        f"screenshot_{batch_id}_survey_{survey_num}.png",
                        mime="image/png",
                        key=f"dl_ss_btn_{batch_id}_{counter}_{i}"
                    )
            else:
                st.info("No screenshots captured for this batch.")

    # ------------------------------------------------------------------
    # Direct AI Answering tab
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
                        "port": int(port),
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
            # Reset progress tracking
            st.session_state.survey_progress = []
            run_async(self._do_direct_answering(
                acct, site, prompt, survey_url, num_surveys,
                model_choice, max_steps
            ))

    # ------------------------------------------------------------------
    # Core async answering logic
    # ------------------------------------------------------------------
    async def _do_direct_answering(
        self, acct, site, prompt, start_url, num_surveys,
        model_choice, max_steps
    ):
        batch_id = f"ai_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        st.session_state.batches[batch_id] = {
            "logs": [],
            "screenshots": [],
            "timestamp": datetime.now().isoformat(),
            "account": acct["username"],
            "site": site["site_name"],
        }
        self.log(f"Starting AI answering: {acct['username']} / {site['site_name']}", batch_id=batch_id)
        st.session_state.generation_in_progress = True

        status_placeholder = st.empty()
        progress_placeholder = st.empty()

        playwright_instance = None
        pw_browser = None
        context = None
        browser = None

        complete_count = 0
        passed_count   = 0
        failed_count   = 0
        error_count    = 0
        survey_details = []

        try:
            # ── ENV CHECK ──────────────────────────────────────────────
            token   = os.getenv("BROWSERLESS_TOKEN")
            ws_base = os.getenv("BROWSERLESS_WS_URL", "wss://production-sfo.browserless.io")

            if not token:
                raise Exception("❌ Missing BROWSERLESS_TOKEN environment variable")

            ws_endpoint = (
                f"{ws_base}"
                f"?token={token}"
                f"&proxy=residential"
                f"&proxyCountry=us"
                f"&proxySticky=true"
            )
            self.log(f"Connecting to Browserless: {ws_base}", batch_id=batch_id)
            status_placeholder.info("🔌 Connecting to browser...")

            # ── CONNECT VIA RAW PLAYWRIGHT (not browser-use Browser) ───
            from playwright.async_api import async_playwright
            playwright_instance = await async_playwright().start()
            pw_browser = await playwright_instance.chromium.connect_over_cdp(ws_endpoint)
            self.log("✅ Playwright browser connected", batch_id=batch_id)

            # ── FETCH COOKIES ──────────────────────────────────────────
            cookies = await self._get_account_cookies_for_injection(acct["account_id"])

            # ── CREATE CONTEXT AND INJECT COOKIES BEFORE NAVIGATION ───
            context = await pw_browser.new_context(
                viewport={"width": 1280, "height": 800},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/122.0.0.0 Safari/537.36"
                ),
            )

            if cookies:
                self.log(f"✅ Found {len(cookies)} cookies — injecting before navigation", batch_id=batch_id)
                await context.add_cookies(cookies)
                self.log("✅ Cookies injected into context", batch_id=batch_id)
            else:
                self.log("⚠️ No cookies found in DB", batch_id=batch_id)

            # ── VERIFY SESSION BEFORE HANDING TO AGENT ─────────────────
            self.log(f"Verifying session by navigating to {start_url} ...", batch_id=batch_id)
            verify_page = await context.new_page()
            await verify_page.goto(start_url, wait_until="domcontentloaded", timeout=30000)
            await verify_page.wait_for_timeout(2000)
            current_url = verify_page.url
            page_content = (await verify_page.content()).lower()
            self.log(f"Post-navigation URL: {current_url}", batch_id=batch_id)

            if "login" in current_url or "login" in page_content:
                self.log("❌ Session verification FAILED — still on login page after cookie injection", "ERROR", batch_id=batch_id)
                self.log("💡 Tip: Re-export cookies using the same US proxy you configured here", "ERROR", batch_id=batch_id)
                # Take a screenshot for debugging
                try:
                    ss = await verify_page.screenshot(type="png")
                    st.session_state.batches[batch_id]["screenshots"].append((0, ss, "login_page_debug"))
                    self.log("📸 Login page screenshot captured for debugging", batch_id=batch_id)
                except Exception:
                    pass
                await verify_page.close()
                raise Exception(
                    "Session not established — cookies are invalid or expired. "
                    "Re-export cookies while connected through the US proxy and save them again."
                )

            self.log("✅ Session verified — dashboard/surveys page loaded", batch_id=batch_id)
            await verify_page.close()

            # ── WRAP PLAYWRIGHT CONTEXT INTO browser-use Browser ───────
            # browser-use Browser can accept an existing Playwright context
            browser = Browser(cdp_url=ws_endpoint)
            # Override the internal context so browser-use uses OUR authenticated one
            browser._playwright = playwright_instance
            browser._browser = pw_browser
            browser._context = context
            # Mark browser as started so it doesn't try to reconnect
            browser._started = True
            self.log("✅ browser-use Browser initialised with authenticated context", batch_id=batch_id)

            # ── LLM SETUP ─────────────────────────────────────────────
            model_cfg = MODEL_REGISTRY[model_choice]
            api_key   = os.getenv(MODEL_ENV_KEYS.get(model_choice, ""))
            if not api_key:
                raise Exception(f"Missing API key for {model_choice}")

            llm = model_cfg["cls"](**{**model_cfg["kwargs"], "api_key": api_key})
            self.log(f"✅ LLM ready: {model_choice}", batch_id=batch_id)

            # ── PERSONA ───────────────────────────────────────────────
            persona   = self._build_persona_system_message(prompt, acct)
            max_steps = max(max_steps, 300)

            base_task = f"""
    Persona:
    {persona}

    You are a REAL human completing online surveys. Follow these rules strictly.

    ## STARTING POINT
    Navigate to: {start_url}

    ## YOUR GOAL
    Find and complete ONE survey. Answer every question naturally as your persona.

    ## STEP-BY-STEP
    1. Go to {start_url}
    2. You should already be logged in. If you see a login page, stop and report as error immediately.
    3. Find a survey to start (look for "Start", "Take Survey", "Begin" buttons).
    4. Answer every question that appears:
    - Read each question carefully
    - Choose the answer that best matches your persona
    - For text fields: write 1-2 natural sentences
    - Never leave required fields blank
    5. Click Next/Continue after each page of questions.
    6. Keep going until you reach either:
    - A "Thank You" or completion page → report SUCCESS
    - A disqualification/screen-out page → report DISQUALIFIED

    ## HUMAN BEHAVIOR
    - Pause 2–4 seconds between actions (you are a human, not a bot)
    - Scroll the page occasionally
    - Read questions fully before answering

    ## ATTENTION CHECKS
    If a question says "Please select [specific answer]" or similar → obey exactly.

    ## COMPLETION SIGNALS
    SUCCESS: "Thank you", "Survey complete", "Your response has been recorded", points/reward shown
    DISQUALIFIED: "Unfortunately", "You don't qualify", "Screen out", "Not eligible"

    ## NEVER
    - Stop in the middle of a survey
    - Rush through questions
    - Give contradictory answers
    """

            # ── SURVEY LOOP ───────────────────────────────────────────
            for i in range(num_surveys):
                survey_num = i + 1
                self.log(f"── Starting survey {survey_num}/{num_surveys} ──", batch_id=batch_id)
                status_placeholder.info(f"🤖 Running survey {survey_num} of {num_surveys}...")
                progress_placeholder.progress(
                    i / num_surveys,
                    text=f"Survey {survey_num}/{num_surveys} in progress..."
                )

                st.session_state.survey_progress.append({
                    "num":    survey_num,
                    "status": STATUS_PENDING,
                    "note":   "Running...",
                })

                survey_status  = STATUS_ERROR
                result_snippet = ""

                try:
                    agent = Agent(
                        task=base_task,
                        llm=llm,
                        browser=browser,
                    )

                    # Re-inject cookies before each survey to keep session alive
                    if cookies:
                        try:
                            await context.add_cookies(cookies)
                            self.log(f"✅ Cookies re-injected for survey {survey_num}", batch_id=batch_id)
                        except Exception as ce:
                            self.log(f"⚠️ Cookie re-injection failed: {ce}", "WARNING", batch_id=batch_id)

                    result = await agent.run(max_steps=max_steps)
                    result_snippet = str(result)[:500]

                    if hasattr(result, "history") and result.history:
                        for idx, h in enumerate(result.history[-10:]):
                            self.log(f"  History[-{min(10, len(result.history)) - idx}]: {str(h)[:200]}", batch_id=batch_id)

                    survey_status = _detect_survey_outcome(result)
                    self.log(f"Survey {survey_num} outcome: {survey_status}", batch_id=batch_id)
                    self.log(f"Result snippet: {result_snippet[:200]}", batch_id=batch_id)

                except Exception as survey_err:
                    self.log(f"Survey {survey_num} error: {survey_err}", "ERROR", batch_id=batch_id)
                    survey_status  = STATUS_ERROR
                    result_snippet = str(survey_err)[:300]

                # Screenshot after each attempt
                try:
                    pages = context.pages
                    if pages:
                        ss = await pages[-1].screenshot(type="png")
                        st.session_state.batches[batch_id]["screenshots"].append(
                            (survey_num, ss, survey_status)
                        )
                        self.log(f"📸 Screenshot captured for survey {survey_num}", batch_id=batch_id)
                except Exception as sse:
                    self.log(f"Screenshot failed: {sse}", "WARNING", batch_id=batch_id)

                st.session_state.survey_progress[-1] = {
                    "num":    survey_num,
                    "status": survey_status,
                    "note":   result_snippet[:100],
                }

                if survey_status == STATUS_COMPLETE:
                    complete_count += 1
                elif survey_status == STATUS_PASSED:
                    passed_count += 1
                elif survey_status == STATUS_FAILED:
                    failed_count += 1
                else:
                    error_count += 1

                survey_details.append({
                    "survey_number":  survey_num,
                    "outcome":        survey_status,
                    "output_snippet": result_snippet,
                })

                self._record_survey_attempt(
                    account_id=acct["account_id"],
                    site_id=site["site_id"],
                    survey_name=f"Survey_{survey_num}_{batch_id}",
                    batch_id=batch_id,
                    status=survey_status,
                    notes=result_snippet[:300],
                )

            # ── FINAL SUMMARY ─────────────────────────────────────────
            progress_placeholder.progress(1.0, text="All surveys processed!")
            summary = (
                f"✅ Complete: {complete_count} | "
                f"🟡 Passed: {passed_count} | "
                f"❌ Failed/DQ: {failed_count} | "
                f"⚠️ Error: {error_count}"
            )
            self.log(f"🏁 DONE — {summary}", batch_id=batch_id)
            status_placeholder.success(f"🏁 Finished! {summary}")

            st.session_state.generation_results = {
                "action":    "direct_answering",
                "status":    "success",
                "complete":  complete_count,
                "passed":    passed_count,
                "failed":    failed_count,
                "error":     error_count,
                "total":     num_surveys,
                "details":   survey_details,
                "account":   acct,
                "site":      {"name": site["site_name"]},
                "model":     model_choice,
                "start_url": start_url,
                "batch_id":  batch_id,
                "timestamp": datetime.now().isoformat(),
            }

        except Exception as e:
            err_msg = f"{type(e).__name__}: {e}"
            self.log(err_msg, "ERROR", batch_id=batch_id)
            self.log(traceback.format_exc(), "ERROR", batch_id=batch_id)
            status_placeholder.error(f"❌ {err_msg}")
            st.session_state.generation_results = {
                "action":   "direct_answering",
                "status":   "failed",
                "error":    err_msg,
                "batch_id": batch_id,
            }

        finally:
            # Clean up in correct order
            try:
                if context:
                    await context.close()
            except Exception:
                pass
            try:
                if pw_browser:
                    await pw_browser.close()
            except Exception:
                pass
            try:
                if playwright_instance:
                    await playwright_instance.stop()
            except Exception:
                pass

            st.session_state.generation_in_progress = False
            st.rerun()

    # ------------------------------------------------------------------
    # Persona builder
    # ------------------------------------------------------------------
    def _build_persona_system_message(self, prompt: Dict, acct: Dict) -> str:
        lines = [
            "You are a specific person answering survey questions. Embody this identity:",
            "",
        ]
        demo_fields = [
            ("age",             "Age"),
            ("gender",          "Gender"),
            ("city",            "City / Location"),
            ("education_level", "Education level"),
            ("job_status",      "Employment status"),
            ("income_range",    "Household income"),
            ("marital_status",  "Marital status"),
            ("household_size",  "Household size"),
            ("industry",        "Industry / sector"),
        ]
        for field, label in demo_fields:
            if acct.get(field):
                lines.append(f"• {label}: {acct[field]}")

        if acct.get("has_children") is not None:
            lines.append(f"• Has children: {'Yes' if acct['has_children'] else 'No'}")

        if prompt and prompt.get("content"):
            lines += ["", "Additional persona details:", prompt["content"].strip()]

        lines += [
            "",
            "Survey answering rules:",
            "- Choose the option that best matches this persona",
            "- Stay consistent — do not contradict answers given earlier",
            "- For free-text fields write naturally (1–2 sentences max)",
            "- If multiple answers apply, pick the one most characteristic of this persona",
        ]
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # DB write helpers
    # ------------------------------------------------------------------
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
        legacy_map = {
            "completed":    STATUS_COMPLETE,
            "disqualified": STATUS_FAILED,
            "incomplete":   STATUS_ERROR,
        }
        status = legacy_map.get(status, status)

        valid_statuses = {STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR}
        if status not in valid_statuses:
            self.log(f"⚠️ Invalid status '{status}' — defaulting to '{STATUS_ERROR}'", "WARNING")
            status = STATUS_ERROR

        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute(
                        """
                        INSERT INTO screening_results
                            (account_id, site_id, survey_name, batch_id,
                             screener_answers, status, started_at, completed_at, notes)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s)
                        """,
                        (account_id, site_id, survey_name, batch_id,
                         1, status, notes[:1000] if notes else "")
                    )
                    conn.commit()
                    self.log(f"✅ Saved survey attempt: {survey_name} → {status}")
        except Exception as e:
            self.log(f"❌ _record_survey_attempt failed: {e}", "ERROR")
            logger.error(f"_record_survey_attempt: {e}\n{traceback.format_exc()}")

    # ------------------------------------------------------------------
    # Screening Results Tab
    # ------------------------------------------------------------------
    def _tab_screening_results(self, acct, site):
        """Display survey attempts with filtering and batch details."""
        st.subheader("🏆 Survey Attempts")
        results = self._load_screening_results(acct["account_id"], site["site_id"])
        if not results:
            st.info("No survey attempts recorded yet.")
            return

        total = len(results)
        complete_n = sum(1 for r in results if r["status"] == STATUS_COMPLETE)
        passed_n = sum(1 for r in results if r["status"] == STATUS_PASSED)
        failed_n = sum(1 for r in results if r["status"] == STATUS_FAILED)
        error_n = sum(1 for r in results if r["status"] in (STATUS_ERROR, STATUS_PENDING))

        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Total", total)
        c2.metric("✅ Complete", complete_n)
        c3.metric("🟡 Passed", passed_n)
        c4.metric("❌ Failed/DQ", failed_n)
        c5.metric("⚠️ Error", error_n)

        success_n = complete_n + passed_n
        if total > 0:
            pass_rate = int(success_n / total * 100)
            st.progress(pass_rate / 100, text=f"Success rate: {pass_rate}% ({success_n}/{total})")

        # Filter by batch ID
        batches = sorted(set(r.get("batch_id") for r in results if r.get("batch_id")))
        if batches:
            selected_batch = st.selectbox("Filter by batch ID:", ["All"] + batches, key="batch_filter")
            if selected_batch != "All":
                results = [r for r in results if r.get("batch_id") == selected_batch]

        st.markdown("---")

        # Store selected batch for full‑width display
        if "selected_batch_for_details" not in st.session_state:
            st.session_state.selected_batch_for_details = None

        for r in results:
            icon = {
                STATUS_COMPLETE: "✅",
                STATUS_PASSED:   "🟡",
                STATUS_FAILED:   "❌",
                STATUS_PENDING:  "⏳",
                STATUS_ERROR:    "⚠️",
            }.get(r["status"], "❓")

            with st.expander(
                f"{icon} **{r.get('survey_name') or 'Unknown Survey'}** — "
                f"{r['status'].upper()} — "
                f"{r['started_at'].strftime('%Y-%m-%d %H:%M') if r.get('started_at') else '?'}",
                expanded=False,
            ):
                col_info, col_actions = st.columns([3, 1])
                with col_info:
                    st.markdown(
                        f"**Batch ID:** `{r.get('batch_id') or '—'}`\n\n"
                        f"**Started:** {r['started_at'].strftime('%Y-%m-%d %H:%M') if r.get('started_at') else '—'}\n\n"
                        f"**Completed:** {r['completed_at'].strftime('%Y-%m-%d %H:%M') if r.get('completed_at') else '—'}"
                    )
                    if r.get("notes"):
                        st.caption(f"Notes: {r['notes']}")
                with col_actions:
                    rid = r["result_id"]
                    if r["status"] != STATUS_COMPLETE:
                        if st.button("✅ Mark Complete", key=f"pass_{rid}", use_container_width=True):
                            self._update_screening_status(rid, STATUS_COMPLETE)
                            st.rerun()
                    if r["status"] != STATUS_FAILED:
                        if st.button("❌ Mark DQ", key=f"fail_{rid}", use_container_width=True):
                            self._update_screening_status(rid, STATUS_FAILED)
                            st.rerun()
                    new_note = st.text_input("Note:", key=f"note_{rid}", placeholder="Optional…")
                    if new_note:
                        if st.button("💾 Save Note", key=f"savenote_{rid}", use_container_width=True):
                            self._save_screening_note(rid, new_note)
                            st.rerun()
                    # View batch details button
                    if r.get("batch_id") and r["batch_id"] in st.session_state.batches:
                        if st.button("📋 View Batch Details", key=f"view_batch_{rid}", use_container_width=True):
                            st.session_state.selected_batch_for_details = r["batch_id"]
                            st.rerun()

        # Render the selected batch details in full width (outside the column)
        if st.session_state.selected_batch_for_details:
            batch_id = st.session_state.selected_batch_for_details
            self._display_batch_details(batch_id)
            if st.button("Close", key="close_batch_details"):
                st.session_state.selected_batch_for_details = None
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
                    "survey_name":  r.get("survey_name", ""),
                    "status":       r.get("status", ""),
                    "started_at":   str(r.get("started_at", "")),
                    "completed_at": str(r.get("completed_at", "")),
                    "batch_id":     r.get("batch_id", ""),
                    "notes":        r.get("notes", ""),
                })
            st.download_button(
                "⬇️ Download CSV",
                data=buf.getvalue(),
                file_name=f"screening_{acct['username']}_{site['site_name'].replace(' ', '_')}.csv",
                mime="text/csv",
                key="dl_screening_csv"
            )

    # ------------------------------------------------------------------
    # Results renderer
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

        total    = r.get("total", 0)
        complete = r.get("complete", 0)
        passed   = r.get("passed", 0)
        failed   = r.get("failed", 0)
        error    = r.get("error_count", r.get("error", 0))

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("✅ Complete",  complete)
        col2.metric("🟡 Passed",   passed)
        col3.metric("❌ Failed/DQ", failed)
        col4.metric("⚠️ Error",    error)

        if "details" in r and r["details"]:
            st.write("**Per-survey details:**")
            for d in r["details"]:
                icon = {
                    STATUS_COMPLETE: "✅",
                    STATUS_PASSED:   "🟡",
                    STATUS_FAILED:   "❌",
                }.get(d["outcome"], "⚠️")
                st.write(f"{icon} Survey {d['survey_number']}: **{d['outcome']}**")
                if d.get("output_snippet"):
                    with st.expander(f"Details for survey {d['survey_number']}"):
                        st.code(d["output_snippet"], language=None)

        st.caption(
            f"Account: {r['account']['username']} | "
            f"Site: {r['site']['name']} | "
            f"Model: {r.get('model', '')} | "
            f"Batch: {r.get('batch_id', '')} | "
            f"{r.get('timestamp', '')}"
        )

        if st.button("Clear results", key="clr_res"):
            st.session_state.generation_results = None
            st.session_state.survey_progress = []
            st.rerun()

    # ------------------------------------------------------------------
    # DB read helpers
    # ------------------------------------------------------------------
    def _pg(self):
        return get_postgres_connection()

    def _get_urls(self, account_id, site_id):
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(
                        "SELECT url_id, url, is_default, is_used, used_at, notes "
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
                        "SELECT result_id, survey_name, batch_id, status, "
                        "       started_at, completed_at, notes "
                        "FROM screening_results "
                        "WHERE account_id=%s AND site_id=%s "
                        "ORDER BY started_at DESC",
                        (account_id, site_id)
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_screening_results: {e}")
            return []

    def _update_screening_status(self, result_id, status):
        valid_statuses = {STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR}
        if status not in valid_statuses:
            logger.error(f"Invalid status for update: {status}")
            return
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    if status in (STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED):
                        c.execute(
                            "UPDATE screening_results SET status=%s, completed_at=CURRENT_TIMESTAMP "
                            "WHERE result_id=%s",
                            (status, result_id)
                        )
                    else:
                        c.execute(
                            "UPDATE screening_results SET status=%s WHERE result_id=%s",
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
                        "SELECT account_id, username, country, profile_id, age, gender, city, "
                        "       education_level, job_status, income_range, marital_status, "
                        "       has_children, household_size, industry, email, phone "
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
                        "SELECT site_id, site_name, description "
                        "FROM survey_sites ORDER BY site_name"
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
                        "SELECT prompt_id, account_id, name AS prompt_name, content, prompt_type "
                        "FROM prompts WHERE is_active=TRUE"
                    )
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"_load_prompts: {e}")
            return []