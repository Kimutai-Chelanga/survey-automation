import subprocess
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional
import numpy as np
import time
import traceback
import json
from src.core.database.postgres import accounts as pg_accounts
from src.streamlit.ui.pages.accounts.chrome_session_manager import ChromeSessionManager
from src.streamlit.ui.pages.accounts.cookie_manager import CookieManager
import os
import logging
import tempfile
import stat
from pathlib import Path
import uuid

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class AccountsPage:
    """Accounts management page for survey automation."""

    def __init__(self, db_manager):
        self.db_manager = db_manager

        # ── CookieManager — shared instance used across all cookie operations ──
        self._cookie_manager = self._build_cookie_manager()

        # ── Session-state defaults ─────────────────────────────────────────────
        defaults = {
            'use_local_chrome':         True,
            'account_creation_logs':    [],
            'show_add_account':         False,
            'creating_account':         False,
            'creation_completed':       False,
            'username_to_create':       None,
            'country_to_create':        None,
            'creation_in_progress':     False,
            'account_creation_message': None,
            'account_creation_error':   False,
            'show_delete_account':      False,
            'deleting_account':         False,
            'deletion_completed':       False,
            'account_id_to_delete':     None,
            'deletion_in_progress':     False,
            'account_deletion_message': None,
            'account_deletion_error':   False,
            'local_chrome_sessions':    {},
            'selected_profile_for_session': None,
            'session_management_logs':  [],
        }
        for key, val in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = val

        # ── Chrome manager ─────────────────────────────────────────────────────
        try:
            from ..hyperbrowser_utils import get_mongodb_client
            client, db = get_mongodb_client()
            self.chrome_manager = ChromeSessionManager(db_manager, client)
        except Exception as e:
            logger.warning(f"Could not initialise Chrome manager: {e}")
            self.chrome_manager = ChromeSessionManager(db_manager, None)

        # ── DB schema migrations ───────────────────────────────────────────────
        self._ensure_survey_columns()
        self._ensure_survey_sites_table()
        self._ensure_demographic_columns()
        self._ensure_account_urls_table()
        self._ensure_question_columns()
        self._ensure_cookie_manager_schema()
        self._ensure_only_top_surveys()          # ← NEW: restrict to Top Surveys
        self._ensure_default_top_surveys_url()   # ← NEW: set default URL for all accounts

    # ── CookieManager factory ─────────────────────────────────────────────────
    def _build_cookie_manager(self) -> Optional[CookieManager]:
        """
        Return a CookieManager wrapped around self.db_manager.
        """
        db = self.db_manager

        class _PgFactory:
            def __enter__(self_inner):
                if hasattr(db, 'get_connection'):
                    self_inner._conn = db.get_connection()
                elif hasattr(db, 'connection'):
                    self_inner._conn = db.connection
                else:
                    raise RuntimeError("db_manager exposes no connection")
                return self_inner._conn

            def __exit__(self_inner, *args):
                try:
                    if args[1]:
                        self_inner._conn.rollback()
                    else:
                        self_inner._conn.commit()
                except Exception:
                    pass

        try:
            return CookieManager(lambda: _PgFactory())
        except Exception as e:
            logger.warning(f"Could not build CookieManager: {e}")
            return None

    # =========================================================================
    # Schema migration for CookieManager table
    # =========================================================================
    def _ensure_cookie_manager_schema(self):
        """Ensure account_cookies has domain and cookies_json columns."""
        try:
            additions = [
                ("domain",       "VARCHAR(255)"),
                ("cookies_json", "TEXT"),
            ]
            for col, col_type in additions:
                check = f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='account_cookies' AND column_name='{col}'
                """
                if not self.db_manager.execute_query(check, fetch=True):
                    self.db_manager.execute_query(
                        f"ALTER TABLE account_cookies ADD COLUMN {col} {col_type}"
                    )
                    logger.info(f"✅ Added {col} column to account_cookies")

            # Add unique constraint
            check_constraint = """
            SELECT constraint_name FROM information_schema.table_constraints
            WHERE table_name='account_cookies'
              AND constraint_type='UNIQUE'
              AND constraint_name='uq_account_cookies_account_domain'
            """
            if not self.db_manager.execute_query(check_constraint, fetch=True):
                try:
                    self.db_manager.execute_query("""
                        ALTER TABLE account_cookies
                        ADD CONSTRAINT uq_account_cookies_account_domain
                        UNIQUE (account_id, domain)
                    """)
                    logger.info("✅ Added UNIQUE(account_id, domain) to account_cookies")
                except Exception:
                    pass

            self.db_manager.execute_query("""
                CREATE INDEX IF NOT EXISTS idx_account_cookies_account_id
                ON account_cookies(account_id)
            """)
        except Exception as e:
            logger.error(f"Failed to ensure CookieManager schema: {e}")

    # =========================================================================
    # Force Top Surveys only + default URL
    # =========================================================================
    def _ensure_only_top_surveys(self):
        """Delete all survey sites except 'Top Surveys'."""
        try:
            self.db_manager.execute_query(
                "DELETE FROM survey_sites WHERE site_name != 'Top Surveys'"
            )
            self.db_manager.execute_query("""
                INSERT INTO survey_sites (site_name, description, is_active)
                SELECT 'Top Surveys', 'High‑paying survey platform', TRUE
                WHERE NOT EXISTS (SELECT 1 FROM survey_sites WHERE site_name = 'Top Surveys')
            """)
            logger.info("Survey sites cleaned – only 'Top Surveys' kept")
        except Exception as e:
            logger.error(f"Failed to enforce only Top Surveys: {e}")

    def _ensure_default_top_surveys_url(self):
        """Ensure each account has the default Top Surveys URL."""
        try:
            site_id_res = self.db_manager.execute_query(
                "SELECT site_id FROM survey_sites WHERE site_name = 'Top Surveys'", fetch=True
            )
            if not site_id_res:
                return
            site_id = site_id_res[0][0] if isinstance(site_id_res[0], tuple) else site_id_res[0]['site_id']

            accounts = self.db_manager.execute_query("SELECT account_id FROM accounts", fetch=True)
            if not accounts:
                return

            for acc in accounts:
                acc_id = acc[0] if isinstance(acc, tuple) else acc['account_id']
                self.db_manager.execute_query("""
                    INSERT INTO account_urls (account_id, site_id, url, is_default, is_used, notes)
                    VALUES (%s, %s, 'https://app.topsurveys.app/', TRUE, FALSE, 'Default Top Surveys URL')
                    ON CONFLICT (account_id, site_id, url) DO UPDATE
                    SET is_default = TRUE, is_used = FALSE, notes = EXCLUDED.notes, updated_at = CURRENT_TIMESTAMP
                """, (acc_id, site_id))
            logger.info("Default Top Surveys URL ensured for all accounts")
        except Exception as e:
            logger.error(f"Failed to set default URL: {e}")

    # =========================================================================
    # Database schema migration helpers (unchanged)
    # =========================================================================
    def _ensure_survey_columns(self):
        try:
            check_country = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name='accounts' AND column_name='country'
            """
            if not self.db_manager.execute_query(check_country, fetch=True):
                self.db_manager.execute_query("ALTER TABLE accounts ADD COLUMN country VARCHAR(100)")
                logger.info("✅ Added country column")

            check_surveys = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name='accounts' AND column_name='total_surveys_processed'
            """
            if not self.db_manager.execute_query(check_surveys, fetch=True):
                check_old = """
                SELECT column_name FROM information_schema.columns
                WHERE table_name='accounts' AND column_name='total_content_processed'
                """
                if self.db_manager.execute_query(check_old, fetch=True):
                    self.db_manager.execute_query(
                        "ALTER TABLE accounts RENAME COLUMN total_content_processed TO total_surveys_processed"
                    )
                else:
                    self.db_manager.execute_query(
                        "ALTER TABLE accounts ADD COLUMN total_surveys_processed INTEGER DEFAULT 0"
                    )
        except Exception as e:
            logger.error(f"Failed to ensure survey columns: {e}")

    def _ensure_demographic_columns(self):
        try:
            demographic_columns = [
                ("age", "INTEGER"), ("date_of_birth", "DATE"), ("gender", "VARCHAR(50)"),
                ("city", "VARCHAR(100)"), ("education_level", "VARCHAR(100)"),
                ("email", "VARCHAR(255)"), ("phone", "VARCHAR(50)"),
                ("job_status", "VARCHAR(100)"), ("industry", "VARCHAR(100)"),
                ("income_range", "VARCHAR(50)"), ("marital_status", "VARCHAR(50)"),
                ("household_size", "INTEGER"), ("has_children", "BOOLEAN"),
                ("shopping_habits", "TEXT"), ("brands_used", "TEXT"),
                ("hobbies", "TEXT"), ("internet_usage", "VARCHAR(100)"),
                ("device_type", "VARCHAR(100)"), ("owns_laptop", "BOOLEAN"),
                ("owns_tv", "BOOLEAN"), ("internet_provider", "VARCHAR(100)"),
                ("demographic_data", "JSONB"),
            ]
            for col_name, col_type in demographic_columns:
                check = f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='accounts' AND column_name='{col_name}'
                """
                if not self.db_manager.execute_query(check, fetch=True):
                    self.db_manager.execute_query(
                        f"ALTER TABLE accounts ADD COLUMN {col_name} {col_type}"
                    )
        except Exception as e:
            logger.error(f"Failed to ensure demographic columns: {e}")

    def _ensure_survey_sites_table(self):
        try:
            check_old = """
            SELECT column_name FROM information_schema.columns
            WHERE table_name='survey_sites' AND column_name='country'
            """
            if self.db_manager.execute_query(check_old, fetch=True):
                for col, col_type in [("site_name", "VARCHAR(255)"), ("is_active", "BOOLEAN DEFAULT TRUE")]:
                    check = f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name='survey_sites' AND column_name='{col}'
                    """
                    if not self.db_manager.execute_query(check, fetch=True):
                        self.db_manager.execute_query(
                            f"ALTER TABLE survey_sites ADD COLUMN {col} {col_type}"
                        )
            else:
                self.db_manager.execute_query("""
                    CREATE TABLE IF NOT EXISTS survey_sites (
                        site_id SERIAL PRIMARY KEY,
                        site_name VARCHAR(255) UNIQUE NOT NULL,
                        description TEXT,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                        is_active BOOLEAN DEFAULT TRUE
                    )
                """)
        except Exception as e:
            logger.error(f"Failed to create survey_sites table: {e}")

    def _ensure_account_urls_table(self):
        try:
            self.db_manager.execute_query("""
                CREATE TABLE IF NOT EXISTS account_urls (
                    url_id SERIAL PRIMARY KEY,
                    account_id INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
                    site_id INTEGER NOT NULL REFERENCES survey_sites(site_id) ON DELETE CASCADE,
                    url TEXT NOT NULL,
                    is_default BOOLEAN DEFAULT FALSE,
                    is_used BOOLEAN DEFAULT FALSE,
                    used_at TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    notes TEXT,
                    UNIQUE(account_id, site_id, url)
                )
            """)
            self.db_manager.execute_query("""
                CREATE INDEX IF NOT EXISTS idx_account_urls_account_site
                ON account_urls(account_id, site_id)
            """)
            self.db_manager.execute_query("""
                CREATE INDEX IF NOT EXISTS idx_account_urls_used
                ON account_urls(is_used) WHERE is_used = FALSE
            """)
        except Exception as e:
            logger.error(f"Failed to create account_urls table: {e}")

    def _ensure_question_columns(self):
        try:
            for col, col_type in [
                ("click_element",    "TEXT"),
                ("used_in_workflow", "BOOLEAN DEFAULT FALSE"),
                ("used_at",          "TIMESTAMP"),
                ("metadata",         "JSONB"),
            ]:
                check = f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='questions' AND column_name='{col}'
                """
                if not self.db_manager.execute_query(check, fetch=True):
                    self.db_manager.execute_query(
                        f"ALTER TABLE questions ADD COLUMN {col} {col_type}"
                    )
        except Exception as e:
            logger.error(f"Failed to ensure question columns: {e}")

    # =========================================================================
    # Helper methods
    # =========================================================================
    def _clear_account_creation_state(self, keep_message=False):
        st.session_state.show_add_account     = False
        st.session_state.creating_account     = False
        st.session_state.creation_completed   = False
        st.session_state.username_to_create   = None
        st.session_state.country_to_create    = None
        st.session_state.creation_in_progress = False
        if not keep_message:
            st.session_state.account_creation_message = None
            st.session_state.account_creation_error   = False
            self.clear_logs()

    def _clear_account_deletion_state(self):
        st.session_state.show_delete_account     = False
        st.session_state.deleting_account        = False
        st.session_state.deletion_completed      = False
        st.session_state.account_id_to_delete    = None
        st.session_state.deletion_in_progress    = False
        st.session_state.account_deletion_message = None
        st.session_state.account_deletion_error  = False

    def add_log(self, message, level="INFO"):
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        log_entry = f"[{timestamp}] {level}: {message}"
        if 'account_creation_logs' not in st.session_state:
            st.session_state.account_creation_logs = []
        st.session_state.account_creation_logs.append(log_entry)
        if len(st.session_state.account_creation_logs) > 50:
            st.session_state.account_creation_logs = st.session_state.account_creation_logs[-50:]
        getattr(logger, level.lower(), logger.info)(message)

    def clear_logs(self):
        st.session_state.account_creation_logs = []

    # =========================================================================
    # Cookie operations — all delegated to CookieManager
    # =========================================================================
    def _store_account_cookies(self, account_id, cookies_json, username) -> Dict[str, Any]:
        try:
            if hasattr(account_id, 'item') or isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            self.add_log(f"Storing cookies for account {account_id} ({username})")

            try:
                cookies = json.loads(cookies_json)
            except json.JSONDecodeError as e:
                return {'success': False, 'error': f"Invalid JSON: {e}"}

            if not isinstance(cookies, list):
                return {'success': False, 'error': "Cookies must be a JSON array"}

            for i, ck in enumerate(cookies):
                missing = [f for f in ('name', 'value', 'domain') if f not in ck]
                if missing:
                    return {'success': False, 'error': f"Cookie #{i} missing: {missing}"}

            if self._cookie_manager:
                domains = list({
                    ck.get('domain', '').lstrip('.')
                    for ck in cookies
                    if ck.get('domain')
                })
                canonical_domains = list({
                    '.'.join(d.split('.')[-2:]) for d in domains if d
                } or {'google.com'})

                saved_count = 0
                for domain in canonical_domains:
                    ok = self._cookie_manager.save(account_id, cookies, domain)
                    if ok:
                        saved_count += 1
                        self.add_log(f"✅ Saved cookies for domain: {domain}")
                    else:
                        self.add_log(f"⚠️ CookieManager.save failed for domain: {domain}", "WARNING")

                self.db_manager.execute_query(
                    "UPDATE accounts SET has_cookies=TRUE, cookies_last_updated=CURRENT_TIMESTAMP "
                    "WHERE account_id=%s",
                    (account_id,),
                )
                self.add_log(f"✓ Updated account {account_id} metadata")
                return {
                    'success':      saved_count > 0,
                    'cookie_count': len(cookies),
                    'account_id':   account_id,
                    'domains':      canonical_domains,
                    'error':        None if saved_count > 0 else "All domain saves failed",
                }

            return self._store_cookies_legacy_sql(account_id, cookies)

        except Exception as e:
            error_msg = f"Failed to store cookies: {e}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _store_cookies_legacy_sql(self, account_id: int, cookies: list) -> Dict[str, Any]:
        try:
            self.db_manager.execute_query(
                "UPDATE account_cookies SET is_active=FALSE, updated_at=CURRENT_TIMESTAMP "
                "WHERE account_id=%s AND is_active=TRUE",
                (account_id,),
            )
            cookie_json_string = json.dumps(cookies)
            result = self.db_manager.execute_query("""
                INSERT INTO account_cookies
                    (account_id, cookie_data, cookie_count,
                     uploaded_at, updated_at, is_active, cookie_source)
                VALUES (%s, %s::jsonb, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE, 'editthiscookie')
                RETURNING cookie_id
            """, (account_id, cookie_json_string, len(cookies)), fetch=True)

            if not result:
                raise Exception("INSERT returned no rows")
            first = result[0]
            cookie_id = first[0] if isinstance(first, tuple) else first.get('cookie_id')

            self.db_manager.execute_query(
                "UPDATE accounts SET has_cookies=TRUE, cookies_last_updated=CURRENT_TIMESTAMP "
                "WHERE account_id=%s",
                (account_id,),
            )
            return {'success': True, 'cookie_id': cookie_id, 'cookie_count': len(cookies)}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _get_account_cookies(self, account_id) -> Dict[str, Any]:
        try:
            if hasattr(account_id, 'item') or isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            if self._cookie_manager:
                records = self._cookie_manager.list_records(account_id)
                if records:
                    rec = records[0]
                    domain = rec.get('domain', 'google.com')
                    cookie_data = self._cookie_manager.load(account_id, domain)
                    return {
                        'has_cookies':    True,
                        'cookie_id':      rec.get('cookie_id'),
                        'cookie_data':    cookie_data,
                        'cookie_count':   rec.get('cookie_count') or (len(cookie_data) if cookie_data else 0),
                        'uploaded_at':    rec.get('captured_at'),
                        'updated_at':     rec.get('updated_at'),
                        'cookie_source':  'cookie_manager',
                        'domain':         domain,
                        'notes':          None,
                    }
                return {'has_cookies': False, 'cookies': None}

            result = self.db_manager.execute_query("""
                SELECT cookie_id, cookie_data::text, cookie_count,
                       uploaded_at, updated_at, cookie_source, notes
                FROM account_cookies
                WHERE account_id=%s AND is_active=TRUE
                ORDER BY uploaded_at DESC LIMIT 1
            """, (account_id,), fetch=True)

            if not result:
                return {'has_cookies': False, 'cookies': None}

            row = result[0]
            if isinstance(row, tuple):
                cookie_id, cookie_data_raw, cookie_count, uploaded_at, updated_at, cookie_source, notes = row
            else:
                cookie_id      = row.get('cookie_id')
                cookie_data_raw = row.get('cookie_data')
                cookie_count   = row.get('cookie_count')
                uploaded_at    = row.get('uploaded_at')
                updated_at     = row.get('updated_at')
                cookie_source  = row.get('cookie_source')
                notes          = row.get('notes')

            cookie_data = None
            if cookie_data_raw:
                cookie_data = json.loads(cookie_data_raw) if isinstance(cookie_data_raw, str) else cookie_data_raw

            return {
                'has_cookies':   True,
                'cookie_id':     cookie_id,
                'cookie_data':   cookie_data,
                'cookie_count':  cookie_count,
                'uploaded_at':   uploaded_at,
                'updated_at':    updated_at,
                'cookie_source': cookie_source,
                'notes':         notes,
            }
        except Exception as e:
            self.add_log(f"Failed to get cookies for account {account_id}: {e}", "ERROR")
            return {'has_cookies': False, 'error': str(e)}

    def _check_cookie_validity(self, account_id) -> Dict[str, Any]:
        try:
            if hasattr(account_id, 'item') or isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            cookie_info = self._get_account_cookies(account_id)
            if not cookie_info['has_cookies']:
                return {'valid': False, 'reason': 'No cookies stored', 'needs_capture': True}

            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                cookies = json.loads(cookies)

            auth_token = next((c for c in cookies if c['name'] == 'auth_token'), None)
            ct0        = next((c for c in cookies if c['name'] == 'ct0'), None)

            if not auth_token:
                return {'valid': False, 'reason': 'No auth_token found', 'needs_capture': True}
            if not ct0:
                return {'valid': False, 'reason': 'No ct0 found', 'needs_capture': True}

            if auth_token.get('expirationDate') and auth_token['expirationDate'] < time.time():
                return {'valid': False, 'reason': 'auth_token expired', 'needs_capture': True}

            uploaded_at = cookie_info.get('uploaded_at')
            if uploaded_at and (datetime.now() - uploaded_at).days > 30:
                return {
                    'valid':   True,
                    'warning': 'Cookies are over 30 days old',
                    'age_days': (datetime.now() - uploaded_at).days,
                }

            return {'valid': True, 'has_auth_token': True, 'has_ct0': True, 'cookie_count': len(cookies)}
        except Exception as e:
            return {'valid': False, 'reason': f'Validation error: {e}', 'needs_capture': True}

    # =========================================================================
    # Account URL management methods
    # =========================================================================
    def _get_account_urls(self, account_id: int, site_id: Optional[int] = None,
                          show_used: bool = False) -> List[Dict[str, Any]]:
        try:
            query = """
                SELECT au.url_id, au.account_id, au.site_id, ss.site_name,
                       au.url, au.is_default, au.is_used, au.used_at,
                       au.created_at, au.notes
                FROM account_urls au
                JOIN survey_sites ss ON au.site_id = ss.site_id
                WHERE au.account_id = %s
            """
            params = [account_id]
            if site_id:
                query  += " AND au.site_id = %s"
                params.append(site_id)
            if not show_used:
                query  += " AND (au.is_used = FALSE OR au.is_used IS NULL)"
            query += " ORDER BY ss.site_name, au.is_default DESC, au.created_at DESC"
            result = self.db_manager.execute_query(query, params, fetch=True)
            return [dict(row) for row in result] if result else []
        except Exception as e:
            self.add_log(f"Error getting account URLs: {e}", "ERROR")
            return []

    def _add_account_url(self, account_id, site_id, url, is_default=False, notes=""):
        try:
            if is_default:
                self.db_manager.execute_query(
                    "UPDATE account_urls SET is_default=FALSE WHERE account_id=%s AND site_id=%s AND is_default=TRUE",
                    (account_id, site_id),
                )
            result = self.db_manager.execute_query(
                "INSERT INTO account_urls (account_id,site_id,url,is_default,notes) VALUES(%s,%s,%s,%s,%s) RETURNING url_id",
                (account_id, site_id, url, is_default, notes), fetch=True,
            )
            if result:
                url_id = result[0][0] if isinstance(result[0], tuple) else result[0]['url_id']
                return {'success': True, 'url_id': url_id}
            return {'success': False, 'error': 'Failed to add URL'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _update_account_url(self, url_id, url, is_default, notes):
        try:
            res = self.db_manager.execute_query(
                "SELECT account_id, site_id FROM account_urls WHERE url_id=%s", (url_id,), fetch=True
            )
            if not res:
                return {'success': False, 'error': 'URL not found'}
            account_id = res[0][0] if isinstance(res[0], tuple) else res[0]['account_id']
            site_id    = res[0][1] if isinstance(res[0], tuple) else res[0]['site_id']
            if is_default:
                self.db_manager.execute_query(
                    "UPDATE account_urls SET is_default=FALSE WHERE account_id=%s AND site_id=%s AND is_default=TRUE AND url_id!=%s",
                    (account_id, site_id, url_id),
                )
            self.db_manager.execute_query(
                "UPDATE account_urls SET url=%s,is_default=%s,notes=%s,updated_at=CURRENT_TIMESTAMP WHERE url_id=%s",
                (url, is_default, notes, url_id),
            )
            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _delete_account_url(self, url_id):
        try:
            self.db_manager.execute_query("DELETE FROM account_urls WHERE url_id=%s", (url_id,))
            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _mark_url_used(self, url_id):
        try:
            self.db_manager.execute_query(
                "UPDATE account_urls SET is_used=TRUE,used_at=CURRENT_TIMESTAMP WHERE url_id=%s", (url_id,)
            )
            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _mark_url_unused(self, url_id):
        try:
            self.db_manager.execute_query(
                "UPDATE account_urls SET is_used=FALSE,used_at=NULL WHERE url_id=%s", (url_id,)
            )
            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _get_default_url_for_account(self, account_id, site_id):
        # Always return the fixed Top Surveys URL
        return "https://app.topsurveys.app/"

    # =========================================================================
    # Account creation / deletion
    # =========================================================================
    def _create_account_in_postgres_minimal(self, username, country=None, demographic_data=None):
        try:
            current_time = datetime.now()
            columns      = ["username", "country", "created_time", "updated_time", "total_surveys_processed"]
            values       = [username, country, current_time, current_time, 0]
            placeholders = ["%s", "%s", "%s", "%s", "%s"]

            if demographic_data:
                for key, value in demographic_data.items():
                    if value is not None and key not in columns:
                        columns.append(key)
                        values.append(value)
                        placeholders.append("%s")

            result = self.db_manager.execute_query(
                f"INSERT INTO accounts ({', '.join(columns)}) VALUES ({', '.join(placeholders)}) RETURNING account_id",
                values, fetch=True,
            )
            if not result:
                raise Exception("Account creation failed")
            account_id = result[0][0] if isinstance(result[0], tuple) else result[0]['account_id']
            self.add_log(f"✅ Account created — ID: {account_id}")
            return {'account_id': account_id}
        except Exception as e:
            raise Exception(f"Failed to create account: {e}")

    def _create_local_chrome_profile_for_account(self, username, account_id):
        try:
            self.add_log(f"Creating persistent Chrome profile for {username}...")
            result = self.chrome_manager.create_profile_for_account(account_id, username)
            if not result['success']:
                raise Exception(result.get('error', 'Unknown error'))
            label = "NEW" if result.get('is_new') else "EXISTING"
            self.add_log(f"✓ {label} profile: {result['profile_id']}")
            return {
                'profile_id':   result['profile_id'],
                'profile_type': 'local_chrome',
                'profile_path': result.get('profile_path', ''),
                'created_at':   result.get('created_at'),
                'mongodb_id':   result.get('mongodb_id'),
                'is_persistent': True,
                'is_new':       result.get('is_new', False),
            }
        except Exception as e:
            raise Exception(f"Failed to create local Chrome profile: {e}")

    def _handle_account_creation(self, username, country=None, cookies_json=None, demographic_data=None):
        if st.session_state.get('creation_in_progress'):
            return
        st.session_state.creation_in_progress = True
        try:
            self.clear_logs()
            self.add_log(f"=== Starting account creation for: {username} ===")
            status_placeholder = st.empty()
            with status_placeholder.container():
                with st.status("Creating account...", expanded=True) as status:
                    st.write("🔄 Creating account record...")
                    account_result = self._create_account_in_postgres_minimal(username, country, demographic_data)
                    account_id     = account_result['account_id']

                    st.write("🔄 Creating local Chrome profile...")
                    profile_result = self._create_local_chrome_profile_for_account(username, account_id)

                    st.write("🔄 Linking profile to account...")
                    self.db_manager.execute_query(
                        "UPDATE accounts SET profile_id=%s, profile_type='local_chrome', updated_time=CURRENT_TIMESTAMP WHERE account_id=%s",
                        (profile_result['profile_id'], account_id),
                    )

                    msg = (
                        f"✓ Account '{username}' created!\n"
                        f"ID: {account_id} | Country: {country} | Profile: {profile_result['profile_path']}"
                    )
                    status.update(label="✓ Account created!", state="complete")
                    st.session_state.account_creation_message = msg
                    st.session_state.account_creation_error   = False
                    st.session_state.creation_completed       = True
                    st.session_state.creating_account         = False
                    st.session_state.creation_in_progress     = False
                    st.cache_data.clear()
                    st.success(msg)
                    time.sleep(3)
                    st.rerun()
        except Exception as e:
            st.session_state.account_creation_message = f"Error: {e}"
            st.session_state.account_creation_error   = True
            st.session_state.creation_completed       = True
            st.session_state.creating_account         = False
            st.session_state.creation_in_progress     = False
            st.error(f"Account creation failed: {e}")
            time.sleep(2)
            st.rerun()

    def _delete_account_from_postgres(self, account_id):
        try:
            result = self.db_manager.execute_query(
                "SELECT username, profile_id FROM accounts WHERE account_id=%s", (account_id,), fetch=True
            )
            if not result:
                raise Exception(f"Account {account_id} not found")
            username, profile_id = result[0]
            for table in ['account_cookies', 'extraction_state', 'answers', 'questions',
                          'prompts', 'prompt_backups', 'account_urls', 'workflows']:
                try:
                    self.db_manager.execute_query(f"DELETE FROM {table} WHERE account_id=%s", (account_id,))
                except Exception as e:
                    self.add_log(f"Failed to delete from {table}: {e}", "WARNING")
            self.db_manager.execute_query("DELETE FROM accounts WHERE account_id=%s", (account_id,))
            return {'deleted_from_postgres': True, 'username': username, 'profile_id': profile_id}
        except Exception as e:
            return {'deleted_from_postgres': False, 'error': str(e)}

    def _delete_account_from_mongodb(self, account_id, profile_id, mongodb_id=None):
        try:
            from streamlit_hyperbrowser_manager import get_mongodb_client
            client, db = get_mongodb_client()
            db.accounts.delete_many({'profile_id': profile_id, 'is_active': True})
            db.browser_sessions.delete_many({'profile_id': profile_id})
            if mongodb_id:
                from bson import ObjectId
                db.accounts.delete_one({'_id': ObjectId(mongodb_id) if isinstance(mongodb_id, str) else mongodb_id})
            client.close()
            return {'deleted_from_mongodb': True}
        except Exception as e:
            return {'deleted_from_mongodb': False, 'error': str(e)}

    def _handle_account_deletion(self, account_id):
        if st.session_state.get('deletion_in_progress'):
            return
        st.session_state.deletion_in_progress = True
        try:
            self.clear_logs()
            status_placeholder = st.empty()
            with status_placeholder.container():
                with st.status("Deleting account...", expanded=True) as status:
                    result = self.db_manager.execute_query(
                        "SELECT username, profile_id, mongo_object_id FROM accounts WHERE account_id=%s",
                        (account_id,), fetch=True,
                    )
                    if not result:
                        raise Exception(f"Account {account_id} not found")
                    username, profile_id, mongo_object_id = result[0]

                    st.write("🔄 Deleting from MongoDB...")
                    self._delete_account_from_mongodb(account_id, profile_id, mongo_object_id)

                    st.write("🔄 Deleting from PostgreSQL...")
                    pg_result = self._delete_account_from_postgres(account_id)
                    if not pg_result.get('deleted_from_postgres'):
                        raise Exception(pg_result.get('error', 'Unknown error'))

                    status.update(label=f"'{username}' deleted!", state="complete")
                    st.session_state.account_deletion_message = f"Account '{username}' deleted."
                    st.session_state.account_deletion_error   = False
                    st.session_state.deletion_completed       = True
                    st.session_state.deleting_account         = False
                    st.session_state.deletion_in_progress     = False
                    st.cache_data.clear()
                    time.sleep(3)
                    st.rerun()
        except Exception as e:
            st.session_state.account_deletion_message = f"Error: {e}"
            st.session_state.account_deletion_error   = True
            st.session_state.deletion_completed       = True
            st.session_state.deleting_account         = False
            st.session_state.deletion_in_progress     = False
            st.error(f"Deletion failed: {e}")
            time.sleep(2)
            st.rerun()

    # =========================================================================
    # Survey Sites (only Top Surveys)
    # =========================================================================
    @st.cache_data(ttl=300)
    def load_survey_sites_data(_self) -> pd.DataFrame:
        try:
            data = _self.db_manager.execute_query(
                "SELECT site_id,site_name,description,created_at,updated_at,is_active FROM survey_sites ORDER BY site_name",
                fetch=True,
            )
            if not data:
                return pd.DataFrame()
            df = pd.DataFrame([dict(row) if hasattr(row, 'keys') else {
                'site_id': row[0], 'site_name': row[1], 'description': row[2],
                'created_at': row[3], 'updated_at': row[4], 'is_active': row[5],
            } for row in data])
            for col in ('created_at', 'updated_at'):
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            return df
        except Exception as e:
            st.error(f"Error loading survey sites: {e}")
            return pd.DataFrame()

    def _add_or_update_survey_site(self, site_name, description="", site_id=None):
        # Block any site other than Top Surveys
        if site_name != "Top Surveys":
            return {'success': False, 'error': 'Only "Top Surveys" is allowed.'}
        try:
            if site_id:
                result = self.db_manager.execute_query(
                    "UPDATE survey_sites SET site_name=%s,description=%s,updated_at=CURRENT_TIMESTAMP WHERE site_id=%s RETURNING site_id",
                    (site_name, description, site_id), fetch=True,
                )
                return {'success': bool(result), 'site_id': site_id, 'action': 'updated'} if result else {'success': False, 'error': 'Not found'}
            existing = self.db_manager.execute_query(
                "SELECT site_id FROM survey_sites WHERE site_name=%s", (site_name,), fetch=True
            )
            if existing:
                return {'success': False, 'error': f'"{site_name}" already exists'}
            result = self.db_manager.execute_query(
                "INSERT INTO survey_sites (site_name,description) VALUES(%s,%s) RETURNING site_id",
                (site_name, description), fetch=True,
            )
            if result:
                new_id = result[0][0] if isinstance(result[0], tuple) else result[0]['site_id']
                return {'success': True, 'site_id': new_id, 'action': 'inserted'}
            return {'success': False, 'error': 'Insert failed'}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _delete_survey_site(self, site_id):
        # Prevent deletion of Top Surveys
        res = self.db_manager.execute_query(
            "SELECT site_name FROM survey_sites WHERE site_id=%s", (site_id,), fetch=True
        )
        if res:
            site_name = res[0][0] if isinstance(res[0], tuple) else res[0]['site_name']
            if site_name == "Top Surveys":
                return {'success': False, 'error': 'Cannot delete Top Surveys site.'}
        try:
            result = self.db_manager.execute_query(
                "SELECT COUNT(*) FROM account_urls WHERE site_id=%s", (site_id,), fetch=True
            )
            count = result[0][0] if result else 0
            if count > 0:
                return {'success': False, 'error': f'{count} account URLs reference this site'}
            self.db_manager.execute_query("DELETE FROM survey_sites WHERE site_id=%s", (site_id,))
            return {'success': True}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _get_survey_url_for_account(self, account_id, site_name):
        # Always return the fixed default URL
        return "https://app.topsurveys.app/"

    # =========================================================================
    # Data loading
    # =========================================================================
    @st.cache_data(ttl=300)
    def load_accounts_data(_self) -> pd.DataFrame:
        try:
            data = _self.db_manager.execute_query("""
                SELECT a.account_id, a.username, a.country, a.profile_id, a.profile_type,
                       a.created_time, a.updated_time, a.mongo_object_id,
                       a.total_surveys_processed,
                       COALESCE(a.has_cookies, FALSE) as has_cookies,
                       a.cookies_last_updated, a.is_active,
                       a.age, a.gender, a.city, a.education_level,
                       a.job_status, a.industry, a.income_range,
                       a.marital_status, a.household_size, a.has_children
                FROM accounts a
                ORDER BY a.created_time DESC
                LIMIT 1000
            """, fetch=True)
            if not data:
                return pd.DataFrame()

            df = pd.DataFrame([dict(row) if hasattr(row, 'keys') else {
                'account_id':              row[0],
                'username':                row[1],
                'country':                 row[2],
                'profile_id':              row[3],
                'profile_type':            row[4],
                'created_time':            row[5],
                'updated_time':            row[6],
                'mongo_object_id':         row[7],
                'total_surveys_processed': row[8] if row[8] is not None else 0,
                'has_cookies':             bool(row[9]),
                'cookies_last_updated':    row[10],
                'is_active':               bool(row[11]) if len(row) > 11 else True,
                'age':                     row[12] if len(row) > 12 else None,
                'gender':                  row[13] if len(row) > 13 else None,
                'city':                    row[14] if len(row) > 14 else None,
                'education_level':         row[15] if len(row) > 15 else None,
                'job_status':              row[16] if len(row) > 16 else None,
                'industry':                row[17] if len(row) > 17 else None,
                'income_range':            row[18] if len(row) > 18 else None,
                'marital_status':          row[19] if len(row) > 19 else None,
                'household_size':          row[20] if len(row) > 20 else None,
                'has_children':            row[21] if len(row) > 21 else None,
            } for row in data])

            for col in ('created_time', 'updated_time', 'cookies_last_updated'):
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])

            df['active_sessions'] = 0
            for info in st.session_state.get('local_chrome_sessions', {}).values():
                acc_id = info.get('account_id')
                if acc_id:
                    df.loc[df['account_id'] == acc_id, 'active_sessions'] += 1

            return df
        except Exception as e:
            st.error(f"Error loading accounts data: {e}")
            return pd.DataFrame()

    # =========================================================================
    # Rendering
    # =========================================================================
    def _render_quick_stats(self):
        try:
            result = self.db_manager.execute_query("""
                SELECT COUNT(*) as total,
                       COUNT(CASE WHEN profile_type='local_chrome' THEN 1 END) as local,
                       SUM(total_surveys_processed) as surveys
                FROM accounts
            """, fetch=True)
            if result:
                row = result[0]
                stats = dict(row) if hasattr(row, 'keys') else {
                    'total': row[0], 'local': row[1], 'surveys': row[2] or 0
                }
                col1, col2, col3, col4 = st.columns(4)
                col1.metric("Total Accounts", stats['total'])
                col2.metric("Local Chrome",   stats['local'])
                col3.metric("Active Sessions", len(st.session_state.get('local_chrome_sessions', {})))
                col4.metric("Total Surveys",  stats['surveys'])
        except Exception:
            st.info("Load data to see stats")

    def _render_accounts_overview(self, df):
        display_df = df[['account_id','username','country','profile_type','profile_id',
                         'active_sessions','total_surveys_processed','has_cookies','created_time']].copy()
        display_df.columns = ['ID','Username','Country','Type','Profile ID',
                               'Active Sessions','Surveys Processed','Has Cookies','Created']
        display_df['Type']       = display_df['Type'].apply(lambda x: '🖥️ Local' if x == 'local_chrome' else 'Unknown')
        display_df['Profile ID'] = display_df['Profile ID'].apply(lambda x: f"{str(x)[:12]}..." if x and len(str(x)) > 12 else (x or "None"))
        display_df['Country']    = display_df['Country'].apply(lambda x: str(x) if x and str(x).strip() else "—")
        display_df['Has Cookies']= display_df['Has Cookies'].apply(lambda x: '✓' if x else '✗')
        display_df['Created']    = display_df['Created'].dt.strftime('%Y-%m-%d %H:%M')
        st.dataframe(display_df, use_container_width=True)

    def _render_account_details_view(self, df):
        for _, row in df.iterrows():
            with st.expander(f"🏷️ {row['username']} (ID: {row['account_id']})", expanded=False):
                col1, col2, col3, col4 = st.columns(4)

                with col1:
                    st.write("**Account Info**")
                    st.write(f"Account ID: `{row['account_id']}`")
                    st.write(f"Username: `{row['username']}`")
                    st.write(f"Country: `{row.get('country', 'Not set')}`")
                    pid = row['profile_id']
                    st.write(f"Profile ID: `{str(pid)[:12] if pid else 'None'}...`")
                    st.write(f"Type: `{row['profile_type']}`")
                    st.write(f"Created: {row['created_time'].strftime('%Y-%m-%d %H:%M')}")
                    st.write(f"Active Sessions: {row['active_sessions']}")

                with col2:
                    st.write("**Survey Stats**")
                    st.write(f"Total Surveys: {row['total_surveys_processed']}")
                    url_count = self._get_account_urls(row['account_id'], show_used=True)
                    st.write(f"Configured URLs: {len(url_count)}")
                    st.write("**Basic Info**")
                    if row.get('age'):           st.write(f"Age: {row['age']}")
                    if row.get('gender'):         st.write(f"Gender: {row['gender']}")
                    if row.get('city'):           st.write(f"City: {row['city']}")
                    if row.get('education_level'): st.write(f"Education: {row['education_level']}")

                with col3:
                    st.write("**Employment**")
                    if row.get('job_status'):    st.write(f"Status: {row['job_status']}")
                    if row.get('industry'):      st.write(f"Industry: {row['industry']}")
                    if row.get('income_range'):  st.write(f"Income: {row['income_range']}")
                    st.write("**Household**")
                    if row.get('marital_status'): st.write(f"Marital: {row['marital_status']}")
                    if row.get('household_size'): st.write(f"Household Size: {row['household_size']}")
                    if row.get('has_children') is not None:
                        st.write(f"Has Children: {'Yes' if row['has_children'] else 'No'}")

                with col4:
                    st.write("**Actions**")
                    if st.button("▶️ Start Session", key=f"start_session_{row['account_id']}",
                                 use_container_width=True, type="primary"):
                        survey_url = self._get_survey_url_for_account(
                            row['account_id'], row.get('country', 'United States')
                        )
                        result_s = self._start_local_chrome_session(
                            row['profile_id'], row['account_id'], row['username'], survey_url
                        )
                        if result_s.get('success'):
                            st.success("✓ Session started!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"Failed: {result_s.get('error')}")

                st.markdown("---")

                if self._cookie_manager:
                    self._cookie_manager.render_account_cookie_panel(
                        account_id=int(row['account_id']),
                        account_email=row.get('username', ''),
                        domain="google.com",
                        key_prefix=f"acct_{row['account_id']}",
                    )
                else:
                    st.write("**🍪 Upload / Update Cookies**")
                    cookies_json_input = st.text_area(
                        "Paste EditThisCookie JSON",
                        height=120,
                        key=f"cookie_upload_{row['account_id']}",
                        placeholder='[{"domain":".google.com","name":"SID","value":"..."}]',
                    )
                    col_c1, col_c2 = st.columns([2, 1])
                    with col_c1:
                        if cookies_json_input:
                            try:
                                parsed = json.loads(cookies_json_input)
                                if isinstance(parsed, list) and parsed:
                                    st.success(f"✓ {len(parsed)} cookies detected")
                                else:
                                    st.warning("⚠️ Expected a JSON array")
                            except json.JSONDecodeError:
                                st.error("❌ Invalid JSON")
                    with col_c2:
                        if st.button("💾 Save Cookies", key=f"save_cookies_{row['account_id']}",
                                     use_container_width=True,
                                     disabled=not bool(cookies_json_input and cookies_json_input.strip())):
                            res = self._store_account_cookies(row['account_id'], cookies_json_input, row['username'])
                            if res['success']:
                                st.success(f"✓ {res['cookie_count']} cookies saved!")
                                st.cache_data.clear()
                                time.sleep(1)
                                st.rerun()
                            else:
                                st.error(f"❌ {res['error']}")

    def _render_performance_analytics_view(self, df):
        if len(df) > 0:
            col1, col2 = st.columns(2)
            with col1:
                st.subheader("Account Type Distribution")
                type_counts = df['profile_type'].value_counts()
                fig = px.pie(values=type_counts.values, names=type_counts.index, title="By Type")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                st.subheader("Survey Processing")
                top = df.nlargest(10, 'total_surveys_processed')[['username', 'total_surveys_processed']]
                fig2 = px.bar(top, x='username', y='total_surveys_processed', title="Top 10 Accounts")
                st.plotly_chart(fig2, use_container_width=True)

    # ── Add Account Modal ──────────────────────────────────────────────────────
    def render_add_account_modal(self):
        if not st.session_state.get('show_add_account', False):
            return

        with st.expander("➕ Add New Account", expanded=True):
            self.render_creation_logs()

            if st.session_state.get('creating_account', False):
                st.info("⏳ Creating account... please wait.")
                if (not st.session_state.get('creation_completed') and
                        not st.session_state.get('creation_in_progress') and
                        st.session_state.get('username_to_create')):
                    self._handle_account_creation(
                        st.session_state.username_to_create,
                        st.session_state.country_to_create,
                        None,
                        st.session_state.get('demographic_data', {}),
                    )
                return

            with st.form("add_account_form"):
                st.write("**Account Configuration:**")
                col1, col2 = st.columns(2)
                with col1:
                    username = st.text_input("Username *", placeholder="Enter username")
                with col2:
                    country = st.selectbox("Country *", options=[
                        "United States","Canada","United Kingdom","Australia","Germany","France",
                        "Japan","Brazil","India","Mexico","Spain","Italy","Netherlands","Sweden",
                        "Norway","Denmark","Finland","New Zealand","Singapore","South Africa","Other",
                    ])

                st.markdown("---")
                st.markdown("### 📋 Optional Demographic Information")

                with st.expander("1️⃣ Basic Personal Info"):
                    col1, col2 = st.columns(2)
                    with col1:
                        age  = st.number_input("Age", min_value=18, max_value=120, value=None, step=1)
                        gender = st.selectbox("Gender", ["","Male","Female","Non-binary","Prefer not to say"])
                    with col2:
                        date_of_birth = st.date_input("Date of Birth", value=None,
                                                       min_value=date(1960,1,1), max_value=date(2008,12,31),
                                                       format="YYYY/MM/DD")
                        city = st.text_input("City", placeholder="e.g., Nairobi")
                    education_level = st.selectbox("Education Level", [
                        "","High School","Some College","Associate Degree","Bachelor's Degree",
                        "Master's Degree","Doctorate","Trade School","Other",
                    ])

                with st.expander("2️⃣ Contact & Account Info"):
                    col1, col2 = st.columns(2)
                    with col1: email = st.text_input("Email Address", placeholder="user@example.com")
                    with col2: phone = st.text_input("Phone Number", placeholder="+1 234 567 8900")

                with st.expander("3️⃣ Employment & Income"):
                    col1, col2 = st.columns(2)
                    with col1:
                        job_status = st.selectbox("Job Status", [
                            "","Student","Employed Full-time","Employed Part-time",
                            "Self-employed","Unemployed","Retired","Homemaker",
                        ])
                        industry = st.text_input("Industry", placeholder="e.g., Technology")
                    with col2:
                        income_range = st.selectbox("Income Range", [
                            "","Under $25,000","$25,000 - $50,000","$50,000 - $75,000",
                            "$75,000 - $100,000","$100,000 - $150,000","Over $150,000","Prefer not to say",
                        ])

                with st.expander("4️⃣ Household Information"):
                    col1, col2 = st.columns(2)
                    with col1:
                        marital_status = st.selectbox("Marital Status", [
                            "","Single","Married","Divorced","Widowed","Domestic Partnership",
                        ])
                        household_size = st.number_input("Household Size", min_value=1, max_value=20, value=None, step=1)
                    with col2:
                        has_children = st.selectbox("Have Children?", ["","Yes","No"])

                with st.expander("5️⃣ Lifestyle & Habits"):
                    shopping_habits = st.text_area("Shopping Habits", placeholder="e.g., Online shopping...")
                    brands_used     = st.text_area("Brands You Use", placeholder="e.g., Nike, Apple...")
                    hobbies         = st.text_area("Hobbies & Interests", placeholder="e.g., Gaming, Sports...")
                    internet_usage  = st.selectbox("Internet Usage", [
                        "","Light (1-2 hours/day)","Moderate (3-5 hours/day)","Heavy (6+ hours/day)","Constant",
                    ])

                with st.expander("6️⃣ Device & Tech Usage"):
                    col1, col2 = st.columns(2)
                    with col1:
                        device_type = st.selectbox("Primary Device", [
                            "","Smartphone","Tablet","Laptop","Desktop Computer","Multiple Devices",
                        ])
                        owns_laptop = st.selectbox("Owns Laptop?", ["","Yes","No"])
                    with col2:
                        owns_tv           = st.selectbox("Owns TV?", ["","Yes","No"])
                        internet_provider = st.text_input("Internet Provider", placeholder="e.g., Comcast")

                if st.session_state.get('account_creation_message'):
                    fn = st.error if st.session_state.get('account_creation_error') else st.success
                    fn(st.session_state.account_creation_message)

                col_submit, col_cancel = st.columns(2)
                with col_submit:
                    submitted = st.form_submit_button("Create Local Chrome Account", use_container_width=True)
                with col_cancel:
                    cancel = st.form_submit_button("Cancel", use_container_width=True)

                if submitted:
                    if not username.strip():
                        st.warning("Please enter a username")
                    else:
                        demographic_data = {k: v for k, v in {
                            'age':               age if age else None,
                            'date_of_birth':     date_of_birth if date_of_birth else None,
                            'gender':            gender or None,
                            'city':              city or None,
                            'education_level':   education_level or None,
                            'email':             email or None,
                            'phone':             phone or None,
                            'job_status':        job_status or None,
                            'industry':          industry or None,
                            'income_range':      income_range or None,
                            'marital_status':    marital_status or None,
                            'household_size':    household_size if household_size else None,
                            'has_children':      (has_children == "Yes") if has_children else None,
                            'shopping_habits':   shopping_habits or None,
                            'brands_used':       brands_used or None,
                            'hobbies':           hobbies or None,
                            'internet_usage':    internet_usage or None,
                            'device_type':       device_type or None,
                            'owns_laptop':       (owns_laptop == "Yes") if owns_laptop else None,
                            'owns_tv':           (owns_tv == "Yes") if owns_tv else None,
                            'internet_provider': internet_provider or None,
                        }.items() if v is not None}
                        st.session_state.username_to_create   = username.strip()
                        st.session_state.country_to_create    = country
                        st.session_state.demographic_data     = demographic_data
                        st.session_state.creating_account     = True
                        st.session_state.account_creation_message = None
                        st.session_state.account_creation_error   = False
                        st.session_state.creation_completed   = False
                        st.session_state.creation_in_progress = False
                        st.rerun()

                if cancel:
                    self._clear_account_creation_state()
                    st.rerun()

    # ── Delete Account Modal ───────────────────────────────────────────────────
    def render_delete_account_modal(self, accounts_df):
        if not st.session_state.get('show_delete_account', False):
            return
        with st.expander("🗑️ Delete Account", expanded=True):
            self.render_creation_logs()
            if st.session_state.get('deleting_account', False):
                st.info("⏳ Deleting...")
                if (not st.session_state.get('deletion_completed') and
                        not st.session_state.get('deletion_in_progress') and
                        st.session_state.get('account_id_to_delete')):
                    self._handle_account_deletion(st.session_state.account_id_to_delete)
                return

            st.warning("⚠️ **This cannot be undone!** Deletes from PostgreSQL and MongoDB.")

            with st.form("delete_account_form"):
                if not accounts_df.empty:
                    account_options = [
                        (f"{r['username']} (ID: {r['account_id']}) — {str(r['profile_id'])[:8]}...", idx)
                        for idx, r in accounts_df.iterrows()
                    ]
                    selected = st.selectbox("Select Account",
                                            [("Choose...", None)] + account_options,
                                            format_func=lambda x: x[0])
                    if selected[1] is not None:
                        row = accounts_df.iloc[selected[1]]
                        col1, col2 = st.columns(2)
                        col1.write(f"**Username:** {row['username']}")
                        col2.write(f"**Surveys:** {row['total_surveys_processed']}")
                        confirm = st.checkbox(
                            f"I understand this permanently deletes '{row['username']}' and all related data"
                        )
                    else:
                        confirm = False
                else:
                    st.info("No accounts to delete.")
                    selected, confirm = (None, None), False

                if st.session_state.get('account_deletion_message'):
                    fn = st.error if st.session_state.get('account_deletion_error') else st.success
                    fn(st.session_state.account_deletion_message)

                col_del, col_can = st.columns(2)
                with col_del:
                    delete_ok = st.form_submit_button(
                        "🗑️ Delete Account", use_container_width=True, type="primary",
                        disabled=not (selected[1] is not None and confirm),
                    )
                with col_can:
                    cancel = st.form_submit_button("Cancel", use_container_width=True)

                if delete_ok and selected[1] is not None and confirm:
                    row = accounts_df.iloc[selected[1]]
                    st.session_state.account_id_to_delete    = row['account_id']
                    st.session_state.deleting_account        = True
                    st.session_state.account_deletion_message = None
                    st.session_state.account_deletion_error  = False
                    st.session_state.deletion_completed      = False
                    st.session_state.deletion_in_progress    = False
                    st.rerun()

                if cancel:
                    self._clear_account_deletion_state()
                    st.rerun()

    # =========================================================================
    # Local Chrome tab
    # =========================================================================
    def _render_local_chrome_tab(self, accounts_df):
        st.subheader("🖥️ Local Chrome Session Management")
        st.info("✓ Free • ✓ Persistent profiles • ✓ Auto cookie sync on stop • ✓ Custom start URLs")
        st.info("ℹ️ Default URL: `https://app.topsurveys.app/` (fixed).")

        if accounts_df.empty:
            st.warning("No accounts available.")
            return

        local_accounts = accounts_df[accounts_df['profile_type'] == 'local_chrome']
        if local_accounts.empty:
            st.warning("No Local Chrome accounts found.")
            return

        col1, col2 = st.columns([2, 1])

        with col1:
            st.subheader("Active Sessions")
            local_sessions = st.session_state.get('local_chrome_sessions', {})
            if local_sessions:
                by_user: Dict[str, list] = {}
                for sid, info in local_sessions.items():
                    by_user.setdefault(info.get('account_username', 'Unknown'), []).append((sid, info))
                for acc_user, sessions in by_user.items():
                    st.markdown(f"**👤 {acc_user}** — {len(sessions)} session(s)")
                    for session_id, info in sessions:
                        s1, s2, s3 = st.columns([3, 2, 1])
                        with s1:
                            st.write(f"`{session_id[:12]}...`")
                            if info.get('profile_path'): st.caption(f"📁 {info['profile_path']}")
                            if info.get('start_url'):    st.caption(f"🌐 {info['start_url'][:50]}")
                            if info.get('debug_port'):   st.caption(f"🔌 port {info['debug_port']}")
                        with s2:
                            if info.get('started_at'):
                                dur = datetime.now() - info['started_at']
                                h, rem = divmod(dur.total_seconds(), 3600)
                                m, _   = divmod(rem, 60)
                                st.write(f"**{int(h):02d}:{int(m):02d}**")
                            if info.get('browser_url'):
                                st.link_button("🖥️ VNC", info['browser_url'], use_container_width=True)
                        with s3:
                            if st.button("🛑", key=f"stop_local_{session_id[:8]}"):
                                res = self._stop_local_chrome_session(session_id)
                                st.success("Stopped") if res.get('success') else st.error(res.get('error'))
                                st.rerun()
                        st.divider()
            else:
                st.info("No active sessions")

        with col2:
            st.subheader("Start Session")
            account_options = [
                (f"{r['username']} (ID: {r['account_id']})", idx)
                for idx, r in local_accounts.iterrows()
            ]
            selected = st.selectbox("Select Account",
                                    [("Choose account...", None)] + account_options,
                                    format_func=lambda x: x[0], key="local_tab_select")

            if selected[1] is not None:
                row        = local_accounts.iloc[selected[1]]
                account_id = row['account_id']
                username   = row['username']

                st.info(f"**👤 {username}**")

                # Fixed URL – no selection
                start_url = "https://app.topsurveys.app/"
                st.info(f"🔗 Start URL: `{start_url}` (fixed for Top Surveys)")

                profile_path = self.chrome_manager.get_profile_path(username)
                if os.path.exists(os.path.join(profile_path, 'Default')):
                    st.success("✓ Existing profile — state preserved")
                else:
                    st.info("📁 New profile will be created")

                cookie_info = self._get_account_cookies(account_id)
                if cookie_info['has_cookies']:
                    st.success(f"✓ {cookie_info['cookie_count']} cookies stored")
                else:
                    st.warning("⚠️ No cookies stored")

                if st.button("▶️ Start Chrome Session", type="primary",
                             use_container_width=True, key=f"start_local_{account_id}"):
                    with st.spinner("Starting Chrome..."):
                        res = self._start_local_chrome_session(
                            row['profile_id'], account_id, username, start_url
                        )
                        if res.get('success'):
                            st.success("✓ Chrome started!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error(f"Failed: {res.get('error')}")

    # =========================================================================
    # Chrome session lifecycle
    # =========================================================================
    def _start_local_chrome_session(self, profile_id, account_id, account_username, start_url=None):
        try:
            self.add_log(f"Starting LOCAL Chrome session for: {account_username}")
            DEFAULT = "https://app.topsurveys.app/"
            if not start_url or start_url.strip() == "":
                start_url = DEFAULT
            if hasattr(account_id, 'item') or isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            profile_path = self.chrome_manager.get_profile_path(account_username)
            session_id   = f"local_{account_username}_{int(time.time())}"

            self._ensure_account_cookie_script(account_id, account_username)

            result = self.chrome_manager.run_persistent_chrome(
                session_id=session_id,
                profile_path=profile_path,
                username=account_username,
                account_id=account_id,
                survey_url=start_url,
                show_terminal=True,
            )
            if not result.get("success"):
                raise Exception(result.get("error", "Unknown error"))

            debug_port = result.get("debug_port", 9222)
            self.add_log(f"✓ Chrome started — debug port {debug_port}, URL: {start_url}")

            if self.chrome_manager.mongo_client:
                try:
                    db = self.chrome_manager.mongo_client["messages_db"]
                    db.browser_sessions.insert_one({
                        "session_id":           session_id,
                        "session_type":         "local_chrome",
                        "profile_id":           profile_id,
                        "profile_path":         profile_path,
                        "postgres_account_id":  int(account_id),
                        "account_username":     account_username,
                        "debug_port":           debug_port,
                        "startup_urls":         result.get("startup_urls", []),
                        "start_url":            start_url,
                        "created_at":           datetime.now(),
                        "is_active":            True,
                        "session_status":       "active",
                        "is_persistent":        True,
                        "profile_preserved":    True,
                    })
                    self.add_log("✓ Stored in MongoDB")
                except Exception as e:
                    self.add_log(f"⚠ MongoDB storage failed: {e}", "WARNING")

            st.session_state.local_chrome_sessions[session_id] = {
                "session_id":       session_id,
                "session_type":     "local_chrome",
                "profile_id":       profile_id,
                "account_id":       int(account_id),
                "account_username": account_username,
                "started_at":       datetime.now(),
                "debug_port":       debug_port,
                "browser_url":      result.get("vnc_url"),
                "profile_path":     profile_path,
                "is_persistent":    True,
                "startup_urls":     result.get("startup_urls", []),
                "start_url":        start_url,
            }

            return {
                "success":    True,
                "session_id": session_id,
                "vnc_url":    result.get("vnc_url"),
                "debug_port": debug_port,
                "start_url":  start_url,
            }
        except Exception as e:
            error_msg = f"Failed to start local session: {e}"
            self.add_log(error_msg, "ERROR")
            return {"success": False, "error": error_msg}

    def _stop_local_chrome_session(self, session_id):
        try:
            local_sessions = st.session_state.get('local_chrome_sessions', {})
            if session_id not in local_sessions:
                return {'success': False, 'error': 'Session not found'}

            session_info = local_sessions[session_id]
            profile_path = session_info.get('profile_path', 'Unknown')

            result = self.chrome_manager.stop_session(session_id)

            if result.get('success'):
                self.add_log(f"✅ Chrome stopped — profile + cookies saved: {profile_path}")
            else:
                self.add_log(f"⚠️ Stop had issues: {result.get('error')}", "WARNING")

            st.session_state.local_chrome_sessions.pop(session_id, None)

            if self.chrome_manager.mongo_client:
                try:
                    db = self.chrome_manager.mongo_client['messages_db']
                    db.browser_sessions.update_one(
                        {'session_id': session_id},
                        {'$set': {'is_active': False, 'ended_at': datetime.now(), 'session_status': 'stopped'}},
                    )
                except Exception as e:
                    self.add_log(f"⚠️ MongoDB update failed: {e}", "WARNING")

            return {'success': True, 'message': f"Session stopped. Profile: {profile_path}"}
        except Exception as e:
            error_msg = f"Failed to stop session: {e}"
            self.add_log(error_msg, "ERROR")
            return {'success': False, 'error': error_msg}

    def _ensure_account_cookie_script(self, account_id, username):
        try:
            if hasattr(account_id, 'item') or isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)
            cookie_info = self._get_account_cookies(account_id)
            if not cookie_info['has_cookies']:
                self.add_log(f"⚠️ No cookies stored for {username}", "WARNING")
                return {'success': False, 'reason': 'no_cookies'}
            return self._generate_cookie_copy_script(account_id, username)
        except Exception as e:
            self.add_log(f"⚠️ Error ensuring cookie script: {e}", "WARNING")
            return {'success': False, 'error': str(e)}

    def _generate_cookie_copy_script(self, account_id, username):
        try:
            cookie_info = self._get_account_cookies(account_id)
            if not cookie_info['has_cookies']:
                return {'success': False, 'error': 'No cookies found'}

            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                cookies = json.loads(cookies)

            cookie_json   = json.dumps(cookies, indent=2)
            escaped_json  = cookie_json.replace("'", "'\"'\"'")
            safe_username = "".join(c for c in username if c.isalnum() or c in "-_")

            scripts_dir = Path("/app/cookie_scripts")
            scripts_dir.mkdir(exist_ok=True, mode=0o777)
            script_path = scripts_dir / f"copy_cookies_{safe_username}.sh"

            profile_path = self.chrome_manager.get_profile_path(username)
            script_content = f"""#!/bin/bash
# Account: {username} (ID: {account_id})
# Profile: {profile_path}
# Generated: {cookie_info.get('updated_at', 'unknown')}
# Cookies: {cookie_info.get('cookie_count', '?')}
set -e
export DISPLAY=:99
command -v xclip &>/dev/null || {{ echo "❌ xclip not installed"; exit 1; }}
echo '{escaped_json}' | xclip -selection clipboard
echo "✅ Cookies copied to clipboard — paste in EditThisCookie in VNC"
"""
            script_path.write_text(script_content)
            os.chmod(script_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
            self.add_log(f"✓ Cookie script: {script_path}")
            return {
                'success':         True,
                'script_path':     str(script_path),
                'script_filename': script_path.name,
                'cookie_count':    cookie_info.get('cookie_count'),
                'command':         f"cd /app/cookie_scripts && ./{script_path.name}",
            }
        except Exception as e:
            return {'success': False, 'error': str(e)}

    def _sync_cookies_to_session_file(self, account_id, username):
        try:
            if hasattr(account_id, 'item') or isinstance(account_id, (np.integer, np.int64)):
                account_id = int(account_id)

            cookie_info = self._get_account_cookies(account_id)
            if not cookie_info['has_cookies']:
                return {'success': False, 'reason': 'no_cookies'}

            cookies = cookie_info['cookie_data']
            if isinstance(cookies, str):
                cookies = json.loads(cookies)

            profile_path = self.chrome_manager.get_profile_path(username)
            session_file = os.path.join(profile_path, 'session_data.json')

            critical  = ['auth_token', 'ct0', 'kdt']
            names     = [c['name'] for c in cookies]
            has_auth  = 'auth_token' in names

            session_data = {
                'timestamp': datetime.now().isoformat(),
                'accountId': account_id,
                'profileDir': profile_path,
                'cookies': cookies,
                'localStorage': {},
                'metadata': {
                    'cookieCount': len(cookies),
                    'savedBy':     'streamlit_cookie_sync',
                    'syncedFrom':  'postgresql_via_cookie_manager',
                    'hasCriticalCookies': has_auth,
                    'capturedAt':  datetime.now().isoformat(),
                },
            }

            os.makedirs(profile_path, exist_ok=True)
            if os.path.exists(session_file):
                import shutil
                shutil.copy2(session_file, f"{session_file}.backup.{int(time.time())}")

            temp = session_file + '.tmp'
            with open(temp, 'w') as f:
                json.dump(session_data, f, indent=2)
            os.rename(temp, session_file)
            os.chmod(session_file, 0o644)

            return {'success': True, 'session_file': session_file,
                    'cookie_count': len(cookies), 'has_auth_token': has_auth}
        except Exception as e:
            return {'success': False, 'error': str(e)}

    # =========================================================================
    # Log rendering
    # =========================================================================
    def render_creation_logs(self):
        if st.session_state.get('account_creation_logs'):
            with st.expander("🔍 Creation Logs", expanded=True):
                log_text = "\n".join(st.session_state.account_creation_logs)
                st.code(log_text, language="log")
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("Clear Logs"):
                        self.clear_logs()
                        st.rerun()
                with col2:
                    st.download_button(
                        "Download Log File", data=log_text,
                        file_name=f"account_creation_logs_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt",
                        mime="text/plain",
                    )

    # =========================================================================
    # Main render
    # =========================================================================
    def render(self):
        st.title("👥 Accounts Management")
        self._render_quick_stats()
        st.markdown("---")

        tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🖥️ Local Chrome", "📈 Analytics", "🌐 Survey Sites"])

        with st.spinner("Loading data..."):
            accounts_df     = self.load_accounts_data()
            survey_sites_df = self.load_survey_sites_data()

        with tab1:
            self._render_overview_tab(accounts_df)
        with tab2:
            self._render_local_chrome_tab(accounts_df)
        with tab3:
            self._render_analytics_tab(accounts_df)
        with tab4:
            self._render_survey_sites_tab(survey_sites_df)

    def _render_overview_tab(self, accounts_df):
        st.subheader("Accounts Overview")
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True, key="overview_refresh"):
                st.cache_data.clear(); st.rerun()
        with col2:
            if st.button("➕ Add Account", use_container_width=True, key="overview_add"):
                st.session_state.show_add_account = True; st.rerun()
        with col3:
            if st.button("🗑️ Delete Account", use_container_width=True, key="overview_delete"):
                st.session_state.show_delete_account = True; st.rerun()

        st.markdown("---")
        self.render_add_account_modal()
        self.render_delete_account_modal(accounts_df)
        st.markdown("---")

        col1, col2, col3 = st.columns(3)
        with col1:
            status_filter = st.selectbox("Account Type",
                ["All","Local Chrome","With Active Sessions"], key="overview_status_filter")
        with col2:
            time_filter = st.selectbox("Time Range",
                ["All Time","Last 7 Days","Last 30 Days","Last 90 Days"], key="overview_time_filter")
        with col3:
            view_option = st.selectbox("View Mode",
                ["Accounts Overview","Account Details","Performance Analytics"], key="overview_view_option")

        st.markdown("---")
        if accounts_df.empty:
            st.info("No accounts found. Use 'Add Account'.")
        else:
            filtered_df = self._apply_filters(accounts_df, status_filter, time_filter)
            st.write(f"Showing {len(filtered_df)} of {len(accounts_df)} accounts")
            if view_option == "Account Details":
                self._render_account_details_view(filtered_df)
            elif view_option == "Performance Analytics":
                self._render_performance_analytics_view(filtered_df)
            else:
                self._render_accounts_overview(filtered_df)

    def _apply_filters(self, df, status_filter, time_filter):
        filtered = df.copy()
        if status_filter == "Local Chrome":
            filtered = filtered[filtered['profile_type'] == 'local_chrome']
        elif status_filter == "With Active Sessions":
            filtered = filtered[filtered['active_sessions'] > 0]
        if time_filter != "All Time":
            days = {"Last 7 Days": 7, "Last 30 Days": 30, "Last 90 Days": 90}[time_filter]
            filtered = filtered[filtered['created_time'] >= datetime.now() - timedelta(days=days)]
        return filtered

    def _render_analytics_tab(self, accounts_df):
        st.subheader("📈 Account Analytics")
        if accounts_df.empty:
            st.info("No accounts to analyse"); return

        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔄 Refresh", use_container_width=True, key="analytics_refresh"):
                st.cache_data.clear(); st.rerun()
        col2.metric("Total Accounts", len(accounts_df))
        col3.metric("Active Sessions", len(st.session_state.get('local_chrome_sessions', {})))

        st.markdown("---")
        display = accounts_df[['username','country','profile_type','active_sessions',
                                'total_surveys_processed','has_cookies','created_time']].copy()
        display.columns = ['Username','Country','Type','Sessions','Surveys','Cookies','Created']
        display['Type']    = display['Type'].apply(lambda x: '🖥️ Local' if x == 'local_chrome' else x)
        display['Cookies'] = display['Cookies'].apply(lambda x: '✓' if x else '✗')
        display['Country'] = display['Country'].apply(lambda x: str(x) if x and str(x).strip() else "—")
        st.dataframe(display, use_container_width=True, hide_index=True)

        st.markdown("---")
        col1, col2 = st.columns(2)
        with col1:
            fig = px.pie(values=accounts_df['profile_type'].value_counts().values,
                         names=accounts_df['profile_type'].value_counts().index)
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            top = accounts_df.nlargest(10, 'total_surveys_processed')
            fig2 = px.bar(top, x='username', y='total_surveys_processed')
            st.plotly_chart(fig2, use_container_width=True)

    def _render_survey_sites_tab(self, survey_sites_df):
        st.subheader("🌐 Survey Sites Management")
        st.info("Only **Top Surveys** is available. The default URL is fixed: `https://app.topsurveys.app/`")

        if survey_sites_df.empty:
            st.warning("Top Surveys not found – will be created automatically.")
            # Force creation
            self._ensure_only_top_surveys()
            self._ensure_default_top_surveys_url()
            st.rerun()
            return

        # Show read‑only info
        for _, row in survey_sites_df.iterrows():
            st.markdown(f"**Site:** {row['site_name']}")
            st.write(row.get('description', ''))
            st.caption(f"Added: {row.get('created_at', 'unknown')}")

        # Show account URLs for Top Surveys
        accounts_df = self.load_accounts_data()
        if not accounts_df.empty:
            st.markdown("---")
            st.subheader("🔗 Account URLs (Top Surveys only)")
            for _, acc_row in accounts_df.iterrows():
                with st.expander(f"{acc_row['username']} (ID: {acc_row['account_id']})"):
                    site_id = survey_sites_df.iloc[0]['site_id'] if not survey_sites_df.empty else None
                    if site_id:
                        urls = self._get_account_urls(acc_row['account_id'], site_id, show_used=True)
                        st.code("https://app.topsurveys.app/", language=None)
                        if urls:
                            st.write("**Stored URL entry:**")
                            for u in urls:
                                st.write(f"- {u['url']} (default: {u['is_default']}, used: {u['is_used']})")
                        else:
                            st.info("No URL entry – will be created automatically on first use.")