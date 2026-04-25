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
                c.execute(
                    "UPDATE proxy_configs SET is_active=FALSE WHERE account_id=%s AND is_active=TRUE",
                    (account_id,)
                )
                c.execute("""
                    INSERT INTO proxy_configs
                        (account_id, proxy_type, host, port, username, password, is_active, country)
                    VALUES (%s, %s, %s, %s, %s, %s, TRUE, %s)
                    RETURNING proxy_id
                """, (account_id, proxy_type, host, port, username or None, password or None, country))
                proxy_id = c.fetchone()[0]
                c.execute(
                    "UPDATE accounts SET active_proxy_id=%s WHERE account_id=%s",
                    (proxy_id, account_id)
                )
                conn.commit()
                return {"success": True, "proxy_id": proxy_id}
    except Exception as e:
        return {"success": False, "error": str(e)}


def delete_proxy_config(account_id: int):
    try:
        with get_postgres() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM proxy_configs WHERE account_id=%s", (account_id,))
                c.execute(
                    "UPDATE accounts SET active_proxy_id=NULL WHERE account_id=%s",
                    (account_id,)
                )
                conn.commit()
                return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


def format_brightdata_username(raw_username: str, country: str) -> str:
    """
    Build the correct BrightData country-targeted username.

    BrightData format:
        brd-customer-<id>-zone-<zone>-country-<CC>

    Rules:
    - Strip any existing -country-XX suffix before appending the new one.
    - Country code is always uppercased.
    - If country is empty or "ANY", return the base username unchanged
      (BrightData will use random country selection).
    """
    if not raw_username:
        return raw_username

    country = (country or "").strip().upper()

    # Remove any existing -country-XX suffix
    base = re.sub(r'-country-[A-Z]{2,3}$', '', raw_username.strip())

    if not country or country == "ANY":
        return base

    return f"{base}-country-{country}"


def build_proxy_string(proxy: Dict, country: Optional[str] = None) -> Optional[str]:
    """
    Build a proxy URL string safe for Chrome's --proxy-server arg.

    For BrightData (and most authenticated proxies) Chrome launched via CDP
    does NOT support credentials embedded in the --proxy-server URL.
    This function therefore returns only the host:port portion for use in
    --proxy-server.  Credentials are handled separately via Playwright's
    route-based auth injection (see agent_utils.create_undetected_browser).

    Returns the bare "scheme://host:port" string, or None if proxy is empty.
    """
    if not proxy or not proxy.get("host") or not proxy.get("port"):
        return None

    proxy_type = proxy.get("proxy_type", "http")
    host = proxy["host"]
    port = proxy["port"]

    return f"{proxy_type}://{host}:{port}"


def test_proxy_connection(proxy: Dict, test_url: str = "https://geo.brdtest.com/welcome.txt") -> Dict:
    """
    Synchronous proxy connectivity test using the requests library.
    Returns {"success": bool, "response": str, "error": str, "ip": str}.
    """
    import requests
    import urllib.parse

    result = {"success": False, "response": "", "error": "", "ip": ""}

    if not proxy or not proxy.get("host"):
        result["error"] = "No proxy configured"
        return result

    proxy_type = proxy.get("proxy_type", "http")
    host = proxy["host"]
    port = proxy["port"]
    username = proxy.get("username", "")
    password = proxy.get("password", "")
    country = proxy.get("country", "US")

    # Apply country suffix for BrightData
    if "brd.superproxy.io" in host and username:
        username = format_brightdata_username(username, country)

    if username and password:
        enc_user = urllib.parse.quote(str(username), safe="")
        enc_pass = urllib.parse.quote(str(password), safe="")
        proxy_url = f"{proxy_type}://{enc_user}:{enc_pass}@{host}:{port}"
    else:
        proxy_url = f"{proxy_type}://{host}:{port}"

    proxies = {"http": proxy_url, "https": proxy_url}

    try:
        resp = requests.get(test_url, proxies=proxies, timeout=15, verify=False)
        result["success"] = resp.status_code == 200
        result["response"] = resp.text[:500]
        result["error"] = "" if result["success"] else f"HTTP {resp.status_code}"

        # Also fetch the external IP
        try:
            ip_resp = requests.get(
                "https://api.ipify.org?format=json",
                proxies=proxies, timeout=10, verify=False
            )
            result["ip"] = ip_resp.json().get("ip", "unknown")
        except Exception:
            result["ip"] = "unknown"

    except requests.exceptions.ProxyError as e:
        result["error"] = f"Proxy error: {e}"
    except requests.exceptions.ConnectTimeout:
        result["error"] = "Connection timed out"
    except Exception as e:
        result["error"] = str(e)

    return result