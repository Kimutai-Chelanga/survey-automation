"""
Cookie management for Google login persistence.
"""

import json
import logging
from typing import List, Dict, Optional
from psycopg2.extras import RealDictCursor

from .db_utils import get_postgres

logger = logging.getLogger(__name__)


def load_cookies_from_db(account_id: int, domain: str = "google.com") -> Optional[List[Dict]]:
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT cookies_json FROM account_cookies
                    WHERE account_id=%s AND domain=%s
                    ORDER BY updated_at DESC LIMIT 1
                """, (account_id, domain))
                row = c.fetchone()
                return json.loads(row["cookies_json"]) if row else None
    except Exception as e:
        logger.error(f"load_cookies_from_db: {e}")
        return None


def save_cookies_to_db(account_id: int, cookies: List[Dict], domain: str = "google.com") -> bool:
    try:
        relevant = [c for c in cookies if domain.lstrip(".") in c.get("domain", "").lstrip(".")]
        if not relevant:
            relevant = cookies
        with get_postgres() as conn:
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO account_cookies (account_id, domain, cookies_json, updated_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (account_id, domain)
                    DO UPDATE SET cookies_json = EXCLUDED.cookies_json, updated_at = CURRENT_TIMESTAMP
                """, (account_id, domain, json.dumps(relevant)))
                conn.commit()
        return True
    except Exception as e:
        logger.error(f"save_cookies_to_db: {e}")
        return False


def delete_cookies_from_db(account_id: int, domain: str = "google.com") -> bool:
    try:
        with get_postgres() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM account_cookies WHERE account_id=%s AND domain=%s", (account_id, domain))
                conn.commit()
        return True
    except Exception as e:
        logger.error(f"delete_cookies_from_db: {e}")
        return False


def get_all_cookie_records(account_id: int) -> List[Dict]:
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT cookie_id, domain, captured_at, updated_at, LENGTH(cookies_json) as size_bytes
                    FROM account_cookies
                    WHERE account_id=%s ORDER BY domain
                """, (account_id,))
                return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f"get_all_cookie_records: {e}")
        return []