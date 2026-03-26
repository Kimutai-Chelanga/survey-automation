"""
cookie_manager.py  —  Reusable cookie DB helper
════════════════════════════════════════════════════════════════════════════════
Import this in ANY page that needs to read/write/display account cookies.

Usage example (Accounts page):
    from .cookie_manager import CookieManager
    cm = CookieManager(get_postgres_connection)
    cm.render_account_cookie_panel(account_id, account_email)

Playwright equivalents of BrowserQL cookie operations:
  BrowserQL  setCookies(cookies:[...]) → Playwright  context.add_cookies([...])
  BrowserQL  getCookies               → Playwright  context.cookies()
  BrowserQL  clearCookies             → Playwright  context.clear_cookies()
════════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Callable, Dict, List, Optional

import streamlit as st
from psycopg2.extras import RealDictCursor

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Migration SQL — run once in your DB console or on app startup
# ─────────────────────────────────────────────────────────────────────────────
MIGRATION_SQL = """
-- account_cookies table
-- Stores serialised Playwright cookie arrays per account per domain.
-- Run this migration once in your PostgreSQL console.

CREATE TABLE IF NOT EXISTS account_cookies (
    cookie_id    SERIAL PRIMARY KEY,
    account_id   INTEGER NOT NULL
                 REFERENCES accounts(account_id) ON DELETE CASCADE,
    domain       VARCHAR(255) NOT NULL DEFAULT 'google.com',
    cookies_json TEXT        NOT NULL,
    captured_at  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (account_id, domain)
);

CREATE INDEX IF NOT EXISTS idx_account_cookies_account_id
    ON account_cookies(account_id);
"""


# ─────────────────────────────────────────────────────────────────────────────
class CookieManager:
    """
    Thin wrapper around the account_cookies table.

    Parameters
    ----------
    pg_factory : Callable
        A zero-argument callable that returns a psycopg2 connection context
        manager, e.g. ``get_postgres_connection``.
    """

    def __init__(self, pg_factory: Callable):
        self._pg = pg_factory

    # ── Core DB operations ────────────────────────────────────────────────────

    def load(self, account_id: int, domain: str = "google.com") -> Optional[List[Dict]]:
        """
        Return the stored cookie list for this account+domain, or None.
        Equivalent to BrowserQL's  getCookies { cookies { name value ... } }
        """
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
            logger.error(f"CookieManager.load: {e}")
            return None

    def save(self, account_id: int, cookies: List[Dict],
             domain: str = "google.com") -> bool:
        """
        Upsert cookies to the DB.
        Filters to store only cookies relevant to *domain* unless the
        result would be empty (then stores everything).

        Equivalent to calling BrowserQL's setCookies after a successful login
        and persisting the result to your own storage.
        """
        if not cookies:
            return False
        try:
            relevant = [
                ck for ck in cookies
                if domain.lstrip(".") in ck.get("domain", "").lstrip(".")
            ]
            payload = json.dumps(relevant if relevant else cookies)

            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute("""
                        INSERT INTO account_cookies
                            (account_id, domain, cookies_json, updated_at)
                        VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                        ON CONFLICT (account_id, domain)
                        DO UPDATE SET
                            cookies_json = EXCLUDED.cookies_json,
                            updated_at   = CURRENT_TIMESTAMP
                    """, (account_id, domain, payload))
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"CookieManager.save: {e}")
            return False

    def delete(self, account_id: int, domain: str = "google.com") -> bool:
        """Delete stored cookies for this account+domain."""
        try:
            with self._pg() as conn:
                with conn.cursor() as c:
                    c.execute(
                        "DELETE FROM account_cookies "
                        "WHERE account_id=%s AND domain=%s",
                        (account_id, domain),
                    )
                    conn.commit()
            return True
        except Exception as e:
            logger.error(f"CookieManager.delete: {e}")
            return False

    def list_records(self, account_id: int) -> List[Dict]:
        """Return metadata for all stored cookie records for this account."""
        try:
            with self._pg() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute("""
                        SELECT cookie_id, domain, captured_at, updated_at,
                               LENGTH(cookies_json)          AS size_bytes,
                               json_array_length(cookies_json::json) AS cookie_count
                        FROM account_cookies
                        WHERE account_id=%s
                        ORDER BY domain
                    """, (account_id,))
                    return [dict(r) for r in c.fetchall()]
        except Exception as e:
            logger.error(f"CookieManager.list_records: {e}")
            return []

    # ── Playwright helpers (async) ────────────────────────────────────────────

    @staticmethod
    async def inject(context, cookies: List[Dict]) -> int:
        """
        Inject cookies into a Playwright BrowserContext.
        Maps to:  context.add_cookies(cookies)
        which is the Playwright equivalent of BrowserQL's setCookies mutation.

        Returns the number of successfully injected cookies.
        """
        if not cookies:
            return 0

        sanitized: List[Dict] = []
        for ck in cookies:
            entry: Dict = {"name": ck["name"], "value": ck["value"]}

            if ck.get("domain"):
                entry["domain"] = ck["domain"]
                entry["path"]   = ck.get("path", "/")
            elif ck.get("url"):
                entry["url"] = ck["url"]
            else:
                entry["domain"] = ".google.com"
                entry["path"]   = "/"

            if ck.get("secure")   is not None: entry["secure"]   = bool(ck["secure"])
            if ck.get("httpOnly") is not None: entry["httpOnly"] = bool(ck["httpOnly"])
            if ck.get("sameSite") in ("Strict", "Lax", "None"):
                entry["sameSite"] = ck["sameSite"]
            if ck.get("expires") and int(ck["expires"]) > 0:
                entry["expires"] = int(ck["expires"])

            sanitized.append(entry)

        try:
            await context.add_cookies(sanitized)
            return len(sanitized)
        except Exception as e:
            logger.warning(f"CookieManager.inject error: {e}")
            return 0

    @staticmethod
    async def extract(context) -> List[Dict]:
        """
        Extract all cookies from a Playwright BrowserContext.
        Maps to:  context.cookies()
        which is the Playwright equivalent of BrowserQL's getCookies mutation.
        """
        try:
            return await context.cookies()
        except Exception as e:
            logger.warning(f"CookieManager.extract error: {e}")
            return []

    # ── Streamlit UI panel ────────────────────────────────────────────────────

    def render_account_cookie_panel(
        self,
        account_id: int,
        account_email: str = "",
        domain: str = "google.com",
        key_prefix: str = "",
    ):
        """
        Render a full Streamlit cookie-management panel for one account.
        Call this from ANY page (Accounts page, Workflows page, etc.)

        Provides:
          • Status badge (stored / not stored)
          • Cookie count + size + last-updated timestamp
          • Delete button
          • Manual JSON paste import (from browser DevTools / cookie extension)
          • Download stored cookies as JSON
        """
        prefix = key_prefix or f"ck_{account_id}"
        records = self.list_records(account_id)
        record  = next((r for r in records if r["domain"] == domain), None)

        st.markdown(f"#### 🍪 Google Cookies — `{domain}`")

        # ── Status ──────────────────────────────────────────────────────────
        if record:
            updated     = record.get("updated_at")
            updated_str = updated.strftime("%Y-%m-%d %H:%M UTC") if updated else "unknown"
            count       = record.get("cookie_count") or "?"
            size_kb     = (record.get("size_bytes") or 0) / 1024

            st.success(
                f"✅ **{count} cookies stored**  \n"
                f"Domain: `{record['domain']}` | "
                f"Updated: `{updated_str}` | "
                f"Size: `{size_kb:.1f} KB`"
            )

            col_dl, col_del = st.columns(2)
            with col_dl:
                raw = self.load(account_id, domain)
                if raw:
                    st.download_button(
                        "⬇️ Export cookies (JSON)",
                        data=json.dumps(raw, indent=2),
                        file_name=f"cookies_{account_id}_{domain}.json",
                        mime="application/json",
                        key=f"{prefix}_dl",
                        use_container_width=True,
                    )
            with col_del:
                if st.button("🗑️ Delete stored cookies",
                             key=f"{prefix}_del", use_container_width=True):
                    self.delete(account_id, domain)
                    st.success("Cookies deleted.")
                    st.rerun()
        else:
            st.warning(
                f"⚠️ **No cookies stored** for this account / `{domain}`.  \n"
                "Cookies will be saved automatically after the first successful login."
            )

        # ── Manual import ────────────────────────────────────────────────────
        with st.expander("📋 Import cookies manually (JSON)"):
            st.markdown("""
**How to get your Google cookies:**
1. Open Chrome and sign into `https://accounts.google.com` manually.
2. Install the **EditThisCookie** extension (or similar).
3. Click the extension → **Export** (copies JSON to clipboard).
4. Paste below and click Save.

Alternatively export from DevTools → Application → Cookies → copy as JSON.
            """)
            raw_input = st.text_area(
                "Paste cookie JSON array:",
                height=150,
                key=f"{prefix}_paste",
                placeholder='[{"name":"SID","value":"xxx","domain":".google.com","path":"/","secure":true}]',
            )
            if st.button("💾 Save pasted cookies", key=f"{prefix}_save_paste",
                         use_container_width=True):
                if not raw_input.strip():
                    st.error("Nothing pasted.")
                else:
                    try:
                        parsed = json.loads(raw_input.strip())
                        if not isinstance(parsed, list):
                            raise ValueError("Expected a JSON array [ {...}, ... ]")
                        ok = self.save(account_id, parsed, domain)
                        if ok:
                            st.success(f"✅ Saved {len(parsed)} cookies for account {account_id}.")
                            st.rerun()
                        else:
                            st.error("DB save failed — check logs.")
                    except json.JSONDecodeError as ex:
                        st.error(f"Invalid JSON: {ex}")
                    except ValueError as ex:
                        st.error(str(ex))

        # ── All domains (if any) ─────────────────────────────────────────────
        if len(records) > 1:
            with st.expander(f"🌐 All stored domains ({len(records)})"):
                for rec in records:
                    col_d, col_u, col_x = st.columns([3, 3, 1])
                    with col_d:
                        cnt = rec.get("cookie_count") or "?"
                        st.write(f"`{rec['domain']}` — {cnt} cookies")
                    with col_u:
                        upd = rec.get("updated_at")
                        st.caption(upd.strftime("%Y-%m-%d %H:%M") if upd else "?")
                    with col_x:
                        if st.button("🗑️", key=f"{prefix}_del_{rec['cookie_id']}"):
                            self.delete(account_id, rec["domain"])
                            st.rerun()