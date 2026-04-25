"""
Agent utilities with proxy authentication, Capsolver, and stealth browser.
Compatible with browser-use 0.1.40.

PROXY STRATEGY — CDP + Authenticated Proxy
-------------------------------------------
Chrome launched via CDP does NOT support credentials in --proxy-server.
Both of these fail:
  --proxy-server=http://user:pass@host:port  → ERR_NO_SUPPORTED_PROXIES
  --proxy-server=http://host:port            → ERR_INVALID_AUTH_CREDENTIALS

The only reliable pattern for authenticated proxies + CDP persistent profiles:

  1. Spin up a tiny local HTTP CONNECT tunnel (thread-based, stdlib only).
     It listens on 127.0.0.1:<free_port> with NO auth required.
  2. When Chrome connects to it, the tunnel authenticates upstream to
     BrightData using the real credentials via the Proxy-Authorization header.
  3. Pass --proxy-server=http://127.0.0.1:<tunnel_port> to Chrome.
     No credentials in the URL — ERR_NO_SUPPORTED_PROXIES is avoided.
     The tunnel handles auth transparently.

This preserves the persistent Chrome profile (user-data-dir) and CDP
connection while supporting fully authenticated residential proxies.

AGENT ISOLATION STRATEGY
--------------------------
browser-use 0.1.40's Agent.run() calls browser.close() and/or
context.close() when it finishes, which in CDP mode kills the entire
Playwright browser connection and Chrome process.

Fix: we create a throw-away "shadow" Browser object for each agent run
that wraps the same CDP URL but has its close() patched to a no-op.
The agent tears down its shadow browser — the real Browser object and
Chrome process are completely unaffected.
"""

import asyncio
import base64
import glob
import json
import os
import logging
import select
import shutil
import signal
import socket
import subprocess
import threading
import time
from typing import Dict, List, Optional, Any

import aiohttp
import requests as _requests
from browser_use import Agent, Browser, BrowserConfig as BrowserUseConfig
from browser_use.browser.context import BrowserContext, BrowserContextConfig
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from crawl4ai import AsyncWebCrawler, BrowserConfig as Crawl4AIBrowserConfig, LLMExtractionStrategy, LLMConfig
from pydantic import BaseModel, Field

from .constants import (
    MODEL_REGISTRY, MODEL_ENV_KEYS, COMPLETE_KEYWORDS, DISQUALIFIED_KEYWORDS,
    STATUS_COMPLETE, STATUS_FAILED, STATUS_ERROR, CAPSOLVER_API_KEY, CAPSOLVER_API_URL
)

logger = logging.getLogger(__name__)

_CDP_PORT_START = int(os.environ.get("CHROME_DEBUG_PORT_START", "9222"))
_CDP_PORT_END   = 9322


class SurveyCard(BaseModel):
    title:     str           = Field(description="The survey title or name")
    reward:    str           = Field(description="Reward amount (e.g., '$1.50')")
    link_url:  str           = Field(description="The URL or clickable element to start the survey")
    unique_id: Optional[str] = Field(default=None, description="Any unique identifier for the survey")


# ---------------------------------------------------------------------------
# Port utilities
# ---------------------------------------------------------------------------
def _find_free_port(start: int = _CDP_PORT_START, end: int = _CDP_PORT_END) -> int:
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(f"No free TCP port in range {start}-{end}.")


def _find_free_port_any() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _kill_chrome_on_port(port: int) -> None:
    try:
        result = subprocess.run(["lsof", "-ti", f"tcp:{port}"],
                                capture_output=True, text=True, timeout=5)
        for pid in result.stdout.strip().splitlines():
            try:
                subprocess.run(["kill", "-9", pid], timeout=3)
                logger.info("Killed stale process %s on port %s", pid, port)
            except Exception:
                pass
    except Exception:
        pass


def _kill_chrome_using_profile(profile_path: str, log_func=None, batch_id: str = "") -> None:
    try:
        import psutil
        killed = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                name    = (proc.info.get('name') or '').lower()
                cmdline = ' '.join(proc.info.get('cmdline') or [])
                if 'chrome' in name and profile_path in cmdline:
                    os.kill(proc.info['pid'], signal.SIGKILL)
                    killed.append(proc.info['pid'])
            except Exception:
                pass
        if killed and log_func:
            log_func(f"Killed {len(killed)} Chrome process(es) using profile", batch_id=batch_id)
        if killed:
            time.sleep(2)
    except ImportError:
        try:
            subprocess.run(["pkill", "-9", "-f", f"chrome.*{profile_path}"],
                           timeout=5, capture_output=True)
            time.sleep(2)
        except Exception:
            pass


def _cleanup_profile_locks(profile_path: str, log_func=None, batch_id: str = "") -> None:
    lock_names    = ['SingletonLock', 'SingletonCookie', 'SingletonSocket', 'lockfile']
    session_names = ['Last Session', 'Last Tabs', 'Current Session', 'Current Tabs']
    dirs_to_clean = [profile_path, os.path.join(profile_path, 'Default')]
    removed = []

    for directory in dirs_to_clean:
        if not os.path.isdir(directory):
            continue
        for name in lock_names:
            target = os.path.join(directory, name)
            if os.path.exists(target) or os.path.islink(target):
                try:
                    os.remove(target)
                    removed.append(target)
                except Exception:
                    try:
                        os.unlink(target)
                        removed.append(target)
                    except Exception as e2:
                        if log_func:
                            log_func(f"Could not remove lock {target}: {e2}", "WARNING", batch_id=batch_id)
        if directory.endswith('Default'):
            for name in session_names:
                target = os.path.join(directory, name)
                if os.path.exists(target):
                    try:
                        os.remove(target)
                        removed.append(target)
                    except Exception:
                        pass

    try:
        for pattern in [
            os.path.join(profile_path, '.org.chromium.Chromium.*'),
            os.path.join(profile_path, '.com.google.Chrome.*'),
        ]:
            for match in glob.glob(pattern):
                try:
                    os.remove(match)
                    removed.append(match)
                except Exception:
                    pass
    except Exception:
        pass

    if removed and log_func:
        log_func(f"Removed {len(removed)} stale lock/session file(s)", batch_id=batch_id)
    elif log_func:
        log_func("No stale lock files found", batch_id=batch_id)


# ---------------------------------------------------------------------------
# Local authenticating proxy tunnel
# ---------------------------------------------------------------------------
class _AuthProxyTunnel:
    """
    A minimal HTTP CONNECT proxy tunnel that runs locally on 127.0.0.1.
    Chrome connects with NO credentials; the tunnel injects Proxy-Authorization.
    """

    def __init__(self, upstream_host: str, upstream_port: int,
                 username: str, password: str, log_func=None, batch_id: str = ""):
        self.upstream_host = upstream_host
        self.upstream_port = upstream_port
        self.username      = username
        self.password      = password
        self.log_func      = log_func
        self.batch_id      = batch_id
        self.local_port    = _find_free_port_any()
        self._server_sock  = None
        self._thread       = None
        self._stop         = threading.Event()

    def _proxy_auth_header(self) -> bytes:
        creds = base64.b64encode(
            f"{self.username}:{self.password}".encode()
        ).decode()
        return f"Proxy-Authorization: Basic {creds}\r\n".encode()

    def _tunnel(self, client_sock: socket.socket) -> None:
        try:
            client_sock.settimeout(30)
            data = b""
            while b"\r\n\r\n" not in data:
                chunk = client_sock.recv(4096)
                if not chunk:
                    return
                data += chunk

            first_line = data.split(b"\r\n")[0].decode(errors="replace")
            method = first_line.split(" ")[0].upper() if " " in first_line else ""

            upstream = socket.create_connection(
                (self.upstream_host, self.upstream_port), timeout=30
            )
            upstream.settimeout(30)

            if method == "CONNECT":
                headers_end = data.index(b"\r\n\r\n")
                head = data[:headers_end]
                auth_bytes = self._proxy_auth_header()
                upstream.sendall(head + b"\r\n" + auth_bytes + b"\r\n")

                resp = b""
                while b"\r\n\r\n" not in resp:
                    chunk = upstream.recv(4096)
                    if not chunk:
                        break
                    resp += chunk

                resp_line = resp.split(b"\r\n")[0].decode(errors="replace")
                if "200" in resp_line:
                    client_sock.sendall(b"HTTP/1.1 200 Connection established\r\n\r\n")
                    self._relay(client_sock, upstream)
                else:
                    client_sock.sendall(resp)
                    if self.log_func:
                        self.log_func(f"Proxy CONNECT failed: {resp_line}", batch_id=self.batch_id)
            else:
                headers_end = data.index(b"\r\n\r\n")
                head = data[:headers_end]
                body = data[headers_end + 4:]
                auth_bytes = self._proxy_auth_header()
                upstream.sendall(head + b"\r\n" + auth_bytes + b"\r\n" + body)
                self._relay(client_sock, upstream)

        except Exception as e:
            logger.debug("Proxy tunnel error: %s", e)
        finally:
            try:
                client_sock.close()
            except Exception:
                pass

    @staticmethod
    def _relay(a: socket.socket, b: socket.socket) -> None:
        a.settimeout(1)
        b.settimeout(1)
        try:
            while True:
                try:
                    r, _, _ = select.select([a, b], [], [], 5)
                except Exception:
                    break
                if not r:
                    continue
                for src, dst in [(a, b), (b, a)]:
                    if src in r:
                        try:
                            chunk = src.recv(65536)
                            if not chunk:
                                return
                            dst.sendall(chunk)
                        except Exception:
                            return
        finally:
            try:
                a.close()
            except Exception:
                pass
            try:
                b.close()
            except Exception:
                pass

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                self._server_sock.settimeout(1)
                try:
                    client_sock, _ = self._server_sock.accept()
                except socket.timeout:
                    continue
                t = threading.Thread(target=self._tunnel, args=(client_sock,), daemon=True)
                t.start()
            except Exception:
                break

    def start(self) -> int:
        self._server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_sock.bind(("127.0.0.1", self.local_port))
        self._server_sock.listen(50)
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()
        if self.log_func:
            self.log_func(
                f"Local auth proxy tunnel: 127.0.0.1:{self.local_port} "
                f"-> {self.upstream_host}:{self.upstream_port}",
                batch_id=self.batch_id,
            )
        return self.local_port

    def stop(self) -> None:
        self._stop.set()
        try:
            self._server_sock.close()
        except Exception:
            pass


def _test_upstream_proxy(proxy_config: Dict, test_url: str = "https://www.topsurveys.app/") -> tuple[bool, str]:
    import requests
    proxy_type = proxy_config.get("proxy_type", "http")
    host = proxy_config["host"]
    port = proxy_config["port"]
    username = proxy_config.get("username")
    password = proxy_config.get("password")
    proxy_url = f"{proxy_type}://{username}:{password}@{host}:{port}" if username and password else f"{proxy_type}://{host}:{port}"
    proxies = {"http": proxy_url, "https": proxy_url}
    try:
        resp = requests.get(test_url, proxies=proxies, timeout=15, verify=False)
        return resp.status_code == 200, f"HTTP {resp.status_code}"
    except Exception as e:
        return False, str(e)


# ---------------------------------------------------------------------------
# Chrome binary discovery
# ---------------------------------------------------------------------------
def _find_chrome() -> str:
    env_path = os.environ.get("CHROME_EXECUTABLE", "") or os.environ.get("CHROME_PATH", "")
    if env_path and os.path.exists(env_path):
        return env_path
    candidates = [
        "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium-browser",
        "/usr/bin/chromium",
        "/usr/local/bin/chromium",
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
        "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe",
    ]
    for path in candidates:
        if path and os.path.exists(path):
            return path
    for name in ("google-chrome-stable", "google-chrome", "chromium-browser", "chromium"):
        found = shutil.which(name)
        if found:
            return found
    raise FileNotFoundError(
        "Could not locate a Chrome/Chromium binary. "
        "Set CHROME_EXECUTABLE in your .env or install Chrome."
    )


# ---------------------------------------------------------------------------
# Chrome process launcher
# ---------------------------------------------------------------------------
def _launch_chrome_with_profile(
    user_data_dir: str,
    headless: bool,
    proxy_server: Optional[str],
    extra_args: List[str],
    cdp_port: int,
) -> subprocess.Popen:
    chrome_path = _find_chrome()

    args = [
        chrome_path,
        f"--remote-debugging-port={cdp_port}",
        "--remote-debugging-address=127.0.0.1",
        f"--user-data-dir={user_data_dir}",
        "--no-first-run",
        "--no-default-browser-check",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--disable-background-networking",
        "--disable-default-apps",
        "--disable-extensions",
        "--disable-sync",
        "--disable-translate",
        "--disable-blink-features=AutomationControlled",
        "--metrics-recording-only",
        "--mute-audio",
        "--safebrowsing-disable-auto-update",
        "--ignore-certificate-errors",
        "--ignore-ssl-errors",
        "--ignore-gpu-blocklist",
        "--window-size=1920,1080",
    ]

    if headless:
        args.append("--headless=new")
    else:
        args.append(f"--display={os.environ.get('DISPLAY', ':99')}")

    if proxy_server:
        args.append(f"--proxy-server={proxy_server}")

    args.extend(extra_args)

    env = os.environ.copy()
    env.setdefault("DISPLAY", ":99")

    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env)


# ---------------------------------------------------------------------------
# CDP readiness probe
# ---------------------------------------------------------------------------
def _wait_for_cdp(port: int, timeout: float = 30.0, log_func=None) -> bool:
    deadline = time.time() + timeout
    attempt  = 0
    while time.time() < deadline:
        attempt += 1
        try:
            r = _requests.get(f"http://127.0.0.1:{port}/json/version", timeout=2)
            if r.status_code == 200:
                if log_func:
                    log_func(f"CDP responded on port {port} after {attempt} attempts")
                return True
        except Exception:
            pass
        time.sleep(0.75)
    return False


# ---------------------------------------------------------------------------
# Helper: get a live page from the playwright browser
# ---------------------------------------------------------------------------
async def _get_live_page(browser: Browser, log_func=None, batch_id: str = ""):
    """
    Retrieve a live Playwright Page. Walks existing contexts first; creates
    a new context+page if needed.
    """
    try:
        playwright_browser = await browser.get_playwright_browser()
        for ctx in playwright_browser.contexts:
            pages = ctx.pages
            if pages:
                page = pages[0]
                if not page.is_closed():
                    if log_func:
                        log_func("Reusing existing browser page", batch_id=batch_id)
                    return page, ctx
        if log_func:
            log_func("No open pages — opening fresh context+page", batch_id=batch_id)
        ctx = await playwright_browser.new_context()
        page = await ctx.new_page()
        return page, ctx
    except Exception as e:
        if log_func:
            log_func(f"_get_live_page error: {e}", "ERROR", batch_id=batch_id)
        raise


# ---------------------------------------------------------------------------
# Helper: build a throw-away shadow Browser for the agent
# ---------------------------------------------------------------------------
async def _create_shadow_browser(cdp_port: int, log_func=None, batch_id: str = "") -> Browser:
    """
    Create a second browser-use Browser object pointing at the SAME CDP port.

    The agent will call close() on this shadow browser when it finishes.
    Because it's a separate Python object, closing it does NOT affect the
    real Browser or the underlying Chrome process — only the Playwright
    connection object inside the shadow is torn down.

    We additionally patch shadow.close() to a no-op so even that lightweight
    Playwright disconnect cannot interfere.
    """
    shadow_config = BrowserUseConfig(
        headless=False,
        cdp_url=f"http://127.0.0.1:{cdp_port}",
    )
    shadow = Browser(config=shadow_config)
    # Pre-connect so the agent doesn't need to
    await shadow.get_playwright_browser()

    # Patch close to a complete no-op — agent can call it freely
    async def _noop_close():
        if log_func:
            log_func("Shadow browser close() intercepted (no-op)", batch_id=batch_id)

    shadow.close = _noop_close  # type: ignore[method-assign]

    if log_func:
        log_func(f"Shadow browser created on CDP port {cdp_port}", batch_id=batch_id)
    return shadow


# ---------------------------------------------------------------------------
# Public: create_undetected_browser
# ---------------------------------------------------------------------------
async def create_undetected_browser(
    user_data_dir: str,
    headless: bool = False,
    proxy: Optional[Dict] = None,
    log_func=None,
    batch_id: str = "",
) -> Browser:
    """
    Create the primary browser-use Browser with persistent Chrome profile,
    BrightData proxy tunnel, and stealth flags.

    Returns a Browser with _cdp_port attached.
    """
    from .proxy_utils import format_brightdata_username

    if proxy and proxy.get("host") and proxy.get("port"):
        ok, msg = _test_upstream_proxy(proxy)
        if not ok:
            raise RuntimeError(f"Upstream proxy test failed: {msg}. Check credentials/zone.")
        if log_func:
            log_func(f"Proxy upstream test passed: {msg}", batch_id=batch_id)

    cdp_port = _find_free_port()
    if log_func:
        log_func(f"Using CDP port {cdp_port}", batch_id=batch_id)
    _kill_chrome_on_port(cdp_port)

    proxy_server: Optional[str]        = None
    tunnel: Optional[_AuthProxyTunnel] = None

    if proxy and proxy.get("host") and proxy.get("port"):
        proxy_type = proxy.get("proxy_type", "http")
        host       = proxy["host"]
        port       = proxy["port"]
        username   = proxy.get("username")
        password   = proxy.get("password")
        country    = proxy.get("country", "US")

        if "brd.superproxy.io" in host and username:
            username = format_brightdata_username(username, country)
            if log_func:
                log_func(f"BrightData country targeting: {country}", batch_id=batch_id)

        if username and password:
            tunnel = _AuthProxyTunnel(
                upstream_host=host,
                upstream_port=int(port),
                username=username,
                password=password,
                log_func=log_func,
                batch_id=batch_id,
            )
            local_port   = tunnel.start()
            await asyncio.sleep(0.5)
            proxy_server = f"http://127.0.0.1:{local_port}"
            if log_func:
                masked_user = (str(username)[:30] + "...") if len(str(username)) > 30 else str(username)
                log_func(
                    f"Proxy: {proxy_type}://{host}:{port}  |  user: {masked_user}  "
                    f"|  local tunnel: 127.0.0.1:{local_port}",
                    batch_id=batch_id,
                )
        else:
            proxy_server = f"{proxy_type}://{host}:{port}"
            if log_func:
                log_func(f"Proxy (no auth): {proxy_server}", batch_id=batch_id)

    os.makedirs(user_data_dir, exist_ok=True)
    if log_func:
        log_func("Checking for stale Chrome processes / lock files", batch_id=batch_id)
    _kill_chrome_using_profile(user_data_dir, log_func=log_func, batch_id=batch_id)
    _cleanup_profile_locks(user_data_dir, log_func=log_func, batch_id=batch_id)

    bare_proxy_log = f"{proxy.get('host')}:{proxy.get('port')}" if proxy else "none"
    if log_func:
        log_func(
            f"Launching Chrome — profile: {user_data_dir!r}  CDP: {cdp_port}"
            + (f"  upstream proxy: {bare_proxy_log}" if proxy_server else "  no proxy"),
            batch_id=batch_id,
        )

    chrome_proc = _launch_chrome_with_profile(
        user_data_dir=user_data_dir,
        headless=headless,
        proxy_server=proxy_server,
        extra_args=[],
        cdp_port=cdp_port,
    )

    await asyncio.sleep(2)
    if chrome_proc.poll() is not None:
        stdout = chrome_proc.stdout.read().decode(errors="replace") if chrome_proc.stdout else ""
        stderr = chrome_proc.stderr.read().decode(errors="replace") if chrome_proc.stderr else ""
        if tunnel:
            tunnel.stop()
        raise RuntimeError(
            f"Chrome exited immediately (code {chrome_proc.returncode}).\n"
            f"STDOUT: {stdout[:500]}\nSTDERR: {stderr[:500]}"
        )

    if not _wait_for_cdp(port=cdp_port, timeout=90.0, log_func=log_func):
        stdout = chrome_proc.stdout.read().decode(errors="replace") if chrome_proc.stdout else ""
        stderr = chrome_proc.stderr.read().decode(errors="replace") if chrome_proc.stderr else ""
        chrome_proc.terminate()
        if tunnel:
            tunnel.stop()
        raise RuntimeError(
            f"Chrome did not expose CDP on port {cdp_port} within 30 s.\n"
            f"STDOUT: {stdout[:500]}\nSTDERR: {stderr[:500]}"
        )

    if log_func:
        log_func(f"Chrome CDP ready on port {cdp_port}", batch_id=batch_id)

    browser_config = BrowserUseConfig(
        headless=headless,
        cdp_url=f"http://127.0.0.1:{cdp_port}",
    )
    browser = Browser(config=browser_config)
    await browser.get_playwright_browser()

    if log_func:
        if tunnel:
            log_func("Proxy auth tunnel active — BrightData credentials injected transparently", batch_id=batch_id)
        log_func("browser-use connected to Chrome via CDP", batch_id=batch_id)

    # Patch the REAL browser's close to also stop tunnel + Chrome process
    _original_close = browser.close

    async def _patched_close():
        try:
            await _original_close()
        finally:
            try:
                chrome_proc.terminate()
                chrome_proc.wait(timeout=5)
            except Exception:
                pass
            if tunnel:
                try:
                    tunnel.stop()
                except Exception:
                    pass

    browser.close = _patched_close  # type: ignore[method-assign]
    browser._cdp_port = cdp_port

    return browser


# ---------------------------------------------------------------------------
# Crawl4AI survey extraction
# ---------------------------------------------------------------------------
async def extract_surveys_with_crawl4ai(
    page_url: str,
    cdp_url: str = "http://127.0.0.1:9222",
    log_func=None,
) -> List[SurveyCard]:
    if log_func:
        log_func(f"Extracting surveys from {page_url} using Crawl4AI via CDP: {cdp_url}")

    browser_config = Crawl4AIBrowserConfig(
        browser_type="chromium",
        headless=False,
        cdp_url=cdp_url,
        verbose=True,
    )

    llm_config = LLMConfig(provider="openai/gpt-4o")
    extraction_strategy = LLMExtractionStrategy(
        llm_config=llm_config,
        schema=SurveyCard.model_json_schema(),
        extraction_type="schema",
        instruction=(
            "Extract all available survey cards. For each, provide title, reward amount, "
            "and the link URL or clickable element identifier. If no URL is present, provide "
            "a CSS selector or text that can be used to click the card."
        ),
    )

    async with AsyncWebCrawler(config=browser_config, verbose=True) as crawler:
        result = await crawler.arun(
            url=page_url,
            extraction_strategy=extraction_strategy,
            wait_for="css:.list-item, .survey-card, .p-ripple-wrapper",
            verbose=True,
        )
        if result.success and result.extracted_content:
            try:
                data = json.loads(result.extracted_content)
                surveys = [SurveyCard(**item) for item in data]
                if log_func:
                    log_func(f"Extracted {len(surveys)} surveys")
                return surveys
            except Exception as e:
                if log_func:
                    log_func(f"Failed to parse extracted surveys: {e}", "WARNING")
                return []
        else:
            if log_func:
                log_func("Crawl4AI extraction failed or returned no data", "WARNING")
            return []


# ---------------------------------------------------------------------------
# CAPTCHA solving
# ---------------------------------------------------------------------------
async def solve_captcha_if_present(page, log_func=None, batch_id: str = ""):
    if not CAPSOLVER_API_KEY:
        if log_func:
            log_func("CAPTCHA solving skipped: No Capsolver API key", batch_id=batch_id)
        return False

    captcha_selectors = [
        'iframe[src*="recaptcha"]',
        'iframe[src*="hcaptcha"]',
        'iframe[src*="turnstile"]',
        'div.g-recaptcha',
        'div.h-captcha',
        'div[data-sitekey]',
    ]

    solved = False
    for selector in captcha_selectors:
        try:
            element = await page.query_selector(selector)
            if element:
                if log_func:
                    log_func(f"CAPTCHA detected: {selector}. Attempting to solve...", batch_id=batch_id)
                sitekey = None
                if selector.startswith("iframe"):
                    parent = await element.evaluate_handle("el => el.closest('[data-sitekey]')")
                    if parent:
                        sitekey = await parent.get_attribute("data-sitekey")
                else:
                    sitekey = await element.get_attribute("data-sitekey")

                if not sitekey:
                    scripts = await page.query_selector_all(
                        'script[src*="recaptcha"], script[src*="hcaptcha"]'
                    )
                    for script in scripts:
                        content = await script.inner_html()
                        import re
                        match = re.search(r'data-sitekey[\s]*=[\s]*["\']([^"\']+)["\']', content)
                        if match:
                            sitekey = match.group(1)
                            break

                if sitekey:
                    page_url = page.url
                    if log_func:
                        log_func(f"Sitekey: {sitekey}, page URL: {page_url}", batch_id=batch_id)
                    token = await call_capsolver(sitekey, page_url, log_func)
                    if token:
                        await page.evaluate(f"""
                            function() {{
                                var callback = document.getElementById('g-recaptcha-response');
                                if (callback) {{
                                    callback.value = '{token}';
                                    callback.dispatchEvent(new Event('change'));
                                }}
                                var hcaptchaCallback = document.querySelector('[name="h-captcha-response"]');
                                if (hcaptchaCallback) {{
                                    hcaptchaCallback.value = '{token}';
                                    hcaptchaCallback.dispatchEvent(new Event('change'));
                                }}
                                if (typeof grecaptcha !== 'undefined') {{
                                    grecaptcha.execute();
                                }}
                            }}
                        """)
                        solved = True
                        if log_func:
                            log_func("CAPTCHA solved and token injected", batch_id=batch_id)
                        await asyncio.sleep(3)
                    else:
                        if log_func:
                            log_func("Failed to solve CAPTCHA via Capsolver", batch_id=batch_id)
                else:
                    if log_func:
                        log_func("Could not find sitekey for CAPTCHA", batch_id=batch_id)
        except Exception as e:
            if log_func:
                log_func(f"Error solving CAPTCHA: {e}", "ERROR", batch_id=batch_id)
    return solved


async def call_capsolver(sitekey: str, page_url: str, log_func=None) -> Optional[str]:
    if not CAPSOLVER_API_KEY:
        return None
    task_type = "ReCaptchaV2TaskProxyless"
    if "hcaptcha" in page_url.lower():
        task_type = "HCaptchaTaskProxyless"

    payload = {
        "clientKey": CAPSOLVER_API_KEY,
        "task": {
            "type":       task_type,
            "websiteURL": page_url,
            "websiteKey": sitekey,
        },
    }
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(f"{CAPSOLVER_API_URL}/createTask", json=payload) as resp:
                result = await resp.json()
                if result.get("errorId") != 0:
                    if log_func:
                        log_func(f"Capsolver error: {result.get('errorDescription')}", "ERROR")
                    return None
                task_id = result.get("taskId")
                if not task_id:
                    return None
            for _ in range(30):
                await asyncio.sleep(2)
                async with session.post(
                    f"{CAPSOLVER_API_URL}/getTaskResult",
                    json={"clientKey": CAPSOLVER_API_KEY, "taskId": task_id},
                ) as poll_resp:
                    poll_result = await poll_resp.json()
                    if poll_result.get("status") == "ready":
                        return (
                            poll_result.get("solution", {}).get("gRecaptchaResponse")
                            or poll_result.get("solution", {}).get("token")
                        )
                    elif poll_result.get("errorId") != 0:
                        if log_func:
                            log_func(
                                f"Capsolver polling error: {poll_result.get('errorDescription')}",
                                "ERROR",
                            )
                        return None
            if log_func:
                log_func("Capsolver timeout", "WARNING")
            return None
        except Exception as e:
            if log_func:
                log_func(f"Capsolver exception: {e}", "ERROR")
            return None


# ---------------------------------------------------------------------------
# Survey agent — shadow browser pattern
# ---------------------------------------------------------------------------
async def run_survey_agent(
    browser: Browser,
    llm,
    persona: str,
    start_url: str,
    log_func=None,
) -> str:
    """
    Run a single survey agent using a throw-away shadow Browser.

    The shadow Browser connects to the same CDP port as the real browser
    but has its close() patched to a no-op. When the agent tears down
    the shadow, Chrome is completely unaffected. The real Browser and
    Chrome process continue running for the next survey.
    """
    agent_task = f"""
{persona}

SURVEY COMPLETION AGENT
========================
You are to complete ONE survey from start to finish.

STEPS:
1. Navigate to {start_url} (if not already there).
2. Wait for the survey dashboard to load.
3. Find the first available survey card and click it.
4. If qualification questions appear, answer them truthfully based on the persona.
5. When the green "Participate" button appears, click it.
6. Answer all main survey questions page by page.
7. Continue until you see a "Thank you" / completion page.

RULES:
- For radio buttons: pick the best matching option for the persona.
- Checkboxes: select all that apply.
- Dropdowns: click the container, then use ArrowDown + Enter.
- Number inputs: type numbers only, no symbols.
- Free text: 1-2 natural sentences.
- Always click Next/Continue after each page.
- If disqualified, stop and report "DISQUALIFIED".
- If completed, report "COMPLETE".

FINAL REPORT:
- Return exactly "COMPLETE" or "DISQUALIFIED" as your final answer.
"""

    cdp_port = getattr(browser, '_cdp_port', 9222)

    # Give the agent a shadow browser — close() on it is a no-op
    shadow = await _create_shadow_browser(cdp_port, log_func=log_func, batch_id="")

    agent = Agent(
        task=agent_task,
        llm=llm,
        browser=shadow,
        max_actions_per_step=5,
    )

    try:
        result = await asyncio.wait_for(agent.run(max_steps=80), timeout=600.0)
        return detect_survey_outcome(result, log_func)
    except asyncio.TimeoutError:
        if log_func:
            log_func("Survey agent timed out after 600 s", "WARNING")
        return STATUS_ERROR
    except Exception as e:
        if log_func:
            log_func(f"Survey agent exception: {e}", "ERROR")
        return STATUS_ERROR


def detect_survey_outcome(result, log_func=None) -> str:
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
            "screen out",
            "screened out",
        ]
        agent_brain_complete_phrases = [
            "evaluation_previous_goal=\"success",
            "evaluation_previous_goal='success",
            "success - i successfully completed",
            "survey is complete",
            "survey has been completed",
            "successfully submitted",
            "successfully completed the survey",
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


# ---------------------------------------------------------------------------
# LLM factory
# ---------------------------------------------------------------------------
def get_llm(model_choice: str):
    cfg     = MODEL_REGISTRY[model_choice]
    api_key = os.getenv(MODEL_ENV_KEYS[model_choice], "")
    if not api_key:
        raise Exception(f"Missing API key for {model_choice}")
    cls_name = cfg["cls"]
    if cls_name == "ChatOpenAI":
        return ChatOpenAI(**{**cfg["kwargs"], "api_key": api_key})
    elif cls_name == "ChatAnthropic":
        return ChatAnthropic(**{**cfg["kwargs"], "api_key": api_key})
    elif cls_name == "ChatGoogleGenerativeAI":
        return ChatGoogleGenerativeAI(**{**cfg["kwargs"], "api_key": api_key})
    else:
        raise ValueError(f"Unknown LLM class: {cls_name}")


# ---------------------------------------------------------------------------
# Fallback Google login
# ---------------------------------------------------------------------------
async def perform_google_login(page, email: str, password: str, log_func=None):
    """Fallback Google login via direct navigation."""
    if log_func:
        log_func("Navigating to Google sign-in")
    await page.goto(
        "https://accounts.google.com/signin/v2/identifier",
        wait_until="domcontentloaded",
        timeout=30_000,
    )
    await page.wait_for_timeout(2000)
    try:
        email_sel = 'input[type="email"], input[name="identifier"]'
        await page.wait_for_selector(email_sel, timeout=15_000)
        await page.fill(email_sel, email)
        if log_func:
            log_func(f"Email filled: {email}")
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(4000)
    except Exception as e:
        raise Exception(f"Google email step failed: {e}")
    try:
        if log_func:
            log_func("Waiting for password input...")
        pwd_sel = 'input[type="password"], input[name="Passwd"]'
        for ct in [
            "Verify it's you", "Confirm it's you", "This extra step",
            "Get a verification code", "Check your phone", "Try another way",
        ]:
            try:
                if await page.locator(f"text='{ct}'").first.is_visible(timeout=1500):
                    raise Exception(f"Google security challenge detected: '{ct}'.")
            except Exception as ce:
                if "security challenge" in str(ce):
                    raise
        await page.wait_for_selector(pwd_sel, timeout=20_000)
        await page.fill(pwd_sel, password)
        if log_func:
            log_func("Password filled")
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(5000)
    except Exception as e:
        raise Exception(f"Google password step failed: {e}")
    for btn_text in ["Stay signed in", "Yes", "Continue", "Not now", "Remind me later"]:
        try:
            btn = page.locator(f"button:has-text('{btn_text}')").first
            if await btn.is_visible(timeout=2000):
                await btn.click()
                if log_func:
                    log_func(f"Clicked post-login prompt: '{btn_text}'")
                await page.wait_for_timeout(2000)
                break
        except Exception:
            pass
    final_url = page.url
    if log_func:
        log_func(f"Post-login URL: {final_url}")
    if "accounts.google.com/signin" in final_url and "challenge" not in final_url:
        raise Exception("Still on Google sign-in page after password attempt.")
    if log_func:
        log_func("Password login successful")