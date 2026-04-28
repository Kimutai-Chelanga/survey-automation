"""
Proxy configuration management.
Supports BrightData and Proxy-Cheap (and any standard HTTP/SOCKS proxy).

PROXY-CHEAP SOCKS5 NOTES:
--------------------------
- Protocol: SOCKS5 on port 9595
- Credentials are generated on the Proxy-Cheap dashboard credentials generator
  with Connection Type set to SOCKS5.
- Username format example: pcwS65b60G-resfix-us-nnid-0
- The username encodes session type, country, and session ID — do NOT modify it.
- test_proxy_connection() uses socks5h:// so DNS resolves through the proxy,
  matching how Chrome routes traffic via --proxy-server=socks5://...
- Requires: pip install requests[socks]  (PySocks backend)

BRIGHTDATA NOTES:
-----------------
- Uses HTTP CONNECT tunnel (_AuthProxyTunnel in agent_utils.py).
- Country targeting is appended as -country-XX suffix to the username.
- format_brightdata_username() handles this — only call it for BrightData hosts.
"""

import logging
import re
import urllib.parse
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

    NOTE: This function is ONLY for BrightData hosts.
    For Proxy-Cheap, country is baked into the username at dashboard generation
    time — do NOT call this function for Proxy-Cheap credentials.
    """
    if not raw_username:
        return raw_username

    country = (country or "").strip().upper()

    # Remove any existing -country-XX suffix
    base = re.sub(r'-country-[A-Z]{2,3}$', '', raw_username.strip())

    if not country or country == "ANY":
        return base

    return f"{base}-country-{country}"


def is_brightdata(host: str) -> bool:
    """Return True if the host is a BrightData superproxy endpoint."""
    return "brd.superproxy.io" in (host or "")


def build_proxy_string(proxy: Dict, country: Optional[str] = None) -> Optional[str]:
    """
    Build a bare "scheme://host:port" proxy URL string safe for Chrome's
    --proxy-server arg (no credentials — those are injected by the tunnel
    for BrightData, or embedded in the flag URL for Proxy-Cheap SOCKS5).

    Returns None if proxy is empty/incomplete.
    """
    if not proxy or not proxy.get("host") or not proxy.get("port"):
        return None

    proxy_type = proxy.get("proxy_type", "http")
    host = proxy["host"]
    port = proxy["port"]

    return f"{proxy_type}://{host}:{port}"


def test_proxy_connection(proxy: Dict) -> Dict:
    """
    Synchronous proxy connectivity test using the requests library.

    PROXY-CHEAP SOCKS5:
    -------------------
    Uses socks5h:// scheme so that DNS resolution also goes through the proxy,
    matching how Chrome routes traffic when launched with:
        --proxy-server=socks5://user:pass@proxy-us.proxy-cheap.com:9595

    Requires PySocks: pip install requests[socks]

    Test URL is plain HTTP (http://ipv4.icanhazip.com) — not HTTPS — because:
    - SOCKS5 proxies the full TCP connection, so HTTP and HTTPS both work.
    - Using a plain HTTP test URL keeps the test lightweight and avoids any
      TLS negotiation overhead, while still confirming the proxy is routing
      traffic correctly and returning the proxy's external IP.

    BRIGHTDATA:
    -----------
    BrightData supports HTTP CONNECT. The test uses the configured proxy_type
    (http/https). Country suffix is applied to the username before testing.

    Returns {"success": bool, "response": str, "error": str, "ip": str}.
    """
    import requests

    result = {"success": False, "response": "", "error": "", "ip": ""}

    if not proxy or not proxy.get("host"):
        result["error"] = "No proxy configured"
        return result

    proxy_type = proxy.get("proxy_type", "http")
    host       = proxy["host"]
    port       = proxy["port"]
    username   = proxy.get("username") or ""
    password   = proxy.get("password") or ""
    country    = proxy.get("country", "US")

    # Apply BrightData country-suffix rewriting ONLY for BrightData hosts.
    # Proxy-Cheap usernames already encode country/session from the dashboard.
    if is_brightdata(host) and username:
        username = format_brightdata_username(username, country)

    # For SOCKS5 use socks5h:// so DNS resolves through proxy (matches Chrome behaviour).
    # For HTTP/HTTPS keep the scheme as-is.
    if proxy_type == "socks5":
        scheme = "socks5h"
    elif proxy_type == "socks4":
        scheme = "socks4"
    else:
        scheme = proxy_type  # http or https

    if username and password:
        enc_user  = urllib.parse.quote(str(username), safe="")
        enc_pass  = urllib.parse.quote(str(password), safe="")
        proxy_url = f"{scheme}://{enc_user}:{enc_pass}@{host}:{port}"
    else:
        proxy_url = f"{scheme}://{host}:{port}"

    proxies = {"http": proxy_url, "https": proxy_url}

    # Plain HTTP test URL — works for both SOCKS5 and HTTP proxies.
    test_url = "http://ipv4.icanhazip.com"

    try:
        resp = requests.get(test_url, proxies=proxies, timeout=15, verify=False)
        result["success"] = resp.status_code == 200
        result["response"] = resp.text.strip()[:500]
        result["ip"]       = resp.text.strip() if result["success"] else "unknown"
        result["error"]    = "" if result["success"] else f"HTTP {resp.status_code}"

    except requests.exceptions.ProxyError as e:
        result["error"] = f"Proxy error: {e}"
    except requests.exceptions.ConnectTimeout:
        result["error"] = (
            "Connection timed out. "
            "Check that your Proxy-Cheap credentials are correct, "
            "your bandwidth has not run out (check dashboard), "
            "and port 9595 is reachable from this server."
        )
    except Exception as e:
        error_str = str(e)
        # Helpful hint for missing PySocks dependency
        if "socks" in error_str.lower() and "install" in error_str.lower():
            result["error"] = (
                f"SOCKS support missing: {error_str}. "
                "Run: pip install requests[socks]"
            )
        else:
            result["error"] = error_str

    return result