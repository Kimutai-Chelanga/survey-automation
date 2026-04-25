"""
Proxy configuration management with BrightData country targeting.
"""

import logging
import re
from typing import Dict, Optional
from psycopg2.extras import RealDictCursor

from .db_utils import get_postgres
from .constants import DEFAULT_PROXY

logger = logging.getLogger(__name__)


def get_account_proxy(account_id: int) -> Optional[Dict]:
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT proxy_id, proxy_type, host, port, username, password, country
                    FROM proxy_configs
                    WHERE account_id=%s AND is_active=TRUE
                    ORDER BY updated_at DESC LIMIT 1
                """, (account_id,))
                row = c.fetchone()
                return dict(row) if row else None
    except Exception as e:
        logger.error(f"get_account_proxy: {e}")
        return None


def save_proxy_config(account_id: int, proxy_type: str, host: str, port: int,
                      username: str = "", password: str = "", country: str = "US"):
    try:
        with get_postgres() as conn:
            with conn.cursor() as c:
                # Ensure country column exists (migration)
                try:
                    c.execute("ALTER TABLE proxy_configs ADD COLUMN IF NOT EXISTS country VARCHAR(5)")
                except Exception:
                    pass
                c.execute("UPDATE proxy_configs SET is_active=FALSE WHERE account_id=%s AND is_active=TRUE", (account_id,))
                c.execute("""
                    INSERT INTO proxy_configs (account_id, proxy_type, host, port, username, password, is_active, country)
                    VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s)
                    RETURNING proxy_id
                """, (account_id, proxy_type, host, port, username or None, password or None, country))
                proxy_id = c.fetchone()[0]
                c.execute("UPDATE accounts SET active_proxy_id=%s WHERE account_id=%s", (proxy_id, account_id))
                conn.commit()
                return {"success": True, "proxy_id": proxy_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


def delete_proxy_config(account_id: int):
    try:
        with get_postgres() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM proxy_configs WHERE account_id=%s", (account_id,))
                c.execute("UPDATE accounts SET active_proxy_id=NULL WHERE account_id=%s", (account_id,))
                conn.commit()
                return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


def format_brightdata_username(raw_username: str, country: str) -> str:
    """
    Convert base BrightData username to country‑specific version.
    Example: "brd-customer-...-zone-residential_proxy1" becomes
             "brd-customer-...-zone-residential_proxy1-country-US"
    """
    if not raw_username:
        return raw_username
    # Remove any existing -country-XX suffix
    base = re.sub(r'-country-[A-Z]{2}$', '', raw_username)
    return f"{base}-country-{country.upper()}"