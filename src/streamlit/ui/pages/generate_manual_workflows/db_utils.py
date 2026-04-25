"""
Database helper functions for accounts, sites, prompts, screening results, etc.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from psycopg2.extras import RealDictCursor

from src.core.database.postgres.connection import get_postgres_connection
from .constants import STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR

logger = logging.getLogger(__name__)


def get_postgres():
    return get_postgres_connection()


def ensure_tables():
    """Create necessary tables if they don't exist, including proxy country column."""
    try:
        with get_postgres() as conn:
            with conn.cursor() as c:
                # proxy_configs
                c.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'proxy_configs')")
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
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            country    VARCHAR(5) DEFAULT 'US'
                        )
                    """)
                else:
                    # Add country column if missing
                    c.execute("SELECT column_name FROM information_schema.columns WHERE table_name='proxy_configs' AND column_name='country'")
                    if not c.fetchone():
                        c.execute("ALTER TABLE proxy_configs ADD COLUMN country VARCHAR(5) DEFAULT 'US'")
                
                # accounts.active_proxy_id
                c.execute("SELECT column_name FROM information_schema.columns WHERE table_name = 'accounts' AND column_name = 'active_proxy_id'")
                if not c.fetchone():
                    c.execute("ALTER TABLE accounts ADD COLUMN active_proxy_id INTEGER REFERENCES proxy_configs(proxy_id)")
                
                # account_cookies
                c.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'account_cookies')")
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
        logger.error(f"ensure_tables: {e}")


def load_accounts() -> List[Dict]:
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT account_id, username, country, profile_id, age, gender, city,
                           education_level, job_status, income_range, marital_status,
                           has_children, household_size, industry, email, phone
                    FROM accounts ORDER BY username
                """)
                return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f"load_accounts: {e}")
        return []


def load_survey_sites() -> List[Dict]:
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT site_id, site_name, description FROM survey_sites ORDER BY site_name")
                return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f"load_survey_sites: {e}")
        return []


def load_prompts() -> List[Dict]:
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("SELECT prompt_id, account_id, name AS prompt_name, content, prompt_type FROM prompts WHERE is_active=TRUE")
                return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f"load_prompts: {e}")
        return []


def get_urls(account_id: int, site_id: int) -> List[Dict]:
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT url_id, url, is_default, is_used, used_at, notes
                    FROM account_urls
                    WHERE account_id=%s AND site_id=%s
                    ORDER BY is_default DESC, created_at DESC
                """, (account_id, site_id))
                return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f"get_urls: {e}")
        return []


def load_screening_results(account_id: int, site_id: int) -> List[Dict]:
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT result_id, survey_name, batch_id, status, started_at, completed_at, notes
                    FROM screening_results
                    WHERE account_id=%s AND site_id=%s
                    ORDER BY started_at DESC
                """, (account_id, site_id))
                return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f"load_screening_results: {e}")
        return []


def update_screening_status(result_id: int, status: str):
    if status not in {STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR}:
        return
    try:
        with get_postgres() as conn:
            with conn.cursor() as c:
                if status in (STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED):
                    c.execute("UPDATE screening_results SET status=%s, completed_at=CURRENT_TIMESTAMP WHERE result_id=%s", (status, result_id))
                else:
                    c.execute("UPDATE screening_results SET status=%s WHERE result_id=%s", (status, result_id))
                conn.commit()
    except Exception as e:
        logger.error(f"update_screening_status: {e}")


def save_screening_note(result_id: int, note: str):
    try:
        with get_postgres() as conn:
            with conn.cursor() as c:
                c.execute("UPDATE screening_results SET notes=%s WHERE result_id=%s", (note, result_id))
                conn.commit()
    except Exception as e:
        logger.error(f"save_screening_note: {e}")


def record_survey_attempt(account_id: int, site_id: int, survey_name: str, batch_id: str, status: str, notes: str = ""):
    status_map = {"completed": STATUS_COMPLETE, "disqualified": STATUS_FAILED, "incomplete": STATUS_ERROR}
    status = status_map.get(status, status)
    if status not in {STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR}:
        status = STATUS_ERROR
    try:
        with get_postgres() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO screening_results
                        (account_id, site_id, survey_name, batch_id, screener_answers,
                         status, started_at, completed_at, notes)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, %s)
                """, (account_id, site_id, survey_name, batch_id, 1, status, (notes or "")[:1000]))
                conn.commit()
    except Exception as e:
        logger.error(f"record_survey_attempt: {e}")


def verify_status_constraint() -> dict:
    try:
        with get_postgres() as conn:
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
                    import re
                    vals = re.findall(r"'([^']+)'", row[0])
                    our = {STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_PENDING, STATUS_ERROR}
                    missing = our - set(vals)
                    return {"ok": len(missing) == 0, "missing": sorted(missing)}
        return {"ok": True}
    except Exception as e:
        return {"ok": False, "error": str(e)}


def get_batches_for_account_site(account_id: int, site_id: int) -> List[Dict[str, Any]]:
    """
    Returns a list of distinct batch_ids with summary stats for a given account and site.
    Each dict: batch_id, total_surveys, complete_count, passed_count, failed_count, error_count.
    """
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT
                        batch_id,
                        COUNT(*) as total_surveys,
                        SUM(CASE WHEN status = %s THEN 1 ELSE 0 END) as complete_count,
                        SUM(CASE WHEN status = %s THEN 1 ELSE 0 END) as passed_count,
                        SUM(CASE WHEN status = %s THEN 1 ELSE 0 END) as failed_count,
                        SUM(CASE WHEN status = %s THEN 1 ELSE 0 END) as error_count
                    FROM screening_results
                    WHERE account_id = %s AND site_id = %s AND batch_id IS NOT NULL
                    GROUP BY batch_id
                    ORDER BY MAX(started_at) DESC
                """, (STATUS_COMPLETE, STATUS_PASSED, STATUS_FAILED, STATUS_ERROR, account_id, site_id))
                return [dict(row) for row in c.fetchall()]
    except Exception as e:
        logger.error(f"get_batches_for_account_site: {e}")
        return []