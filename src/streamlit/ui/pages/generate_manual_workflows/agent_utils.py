"""
Agent utilities with proxy authentication, Capsolver, and stealth browser.
Compatible with browser-use 0.1.40.

browser-use 0.1.40 API facts (verified from source):
  - Browser(config)           — sync constructor only, NO .start() method
  - await browser.new_context(ctx_config) → BrowserContext
  - await context.get_current_page()      → Page
  - BrowserConfig fields: headless, extra_chromium_args, proxy (ProxySettings),
                          chrome_instance_path, cdp_url, wss_url
  - NO user_data_dir on BrowserConfig — persistent profiles are handled by
    launching Chrome with --user-data-dir ourselves, then connecting via CDP.
"""

import asyncio
import glob
import json
import os
import logging
import shlex
import shutil
import signal
import socket
import subprocess
import time
from typing import Dict, List, Optional, Any

import aiohttp
import requests as _requests
from browser_use import Agent, Browser, BrowserConfig
from browser_use.browser.context import BrowserContext, BrowserContextConfig
from playwright._impl._api_structures import ProxySettings
from langchain_openai import ChatOpenAI
from langchain_anthropic import ChatAnthropic
from langchain_google_genai import ChatGoogleGenerativeAI
from crawl4ai import AsyncWebCrawler, LLMExtractionStrategy
from pydantic import BaseModel, Field

from .constants import (
    MODEL_REGISTRY, MODEL_ENV_KEYS, COMPLETE_KEYWORDS, DISQUALIFIED_KEYWORDS,
    STATUS_COMPLETE, STATUS_FAILED, STATUS_ERROR, CAPSOLVER_API_KEY, CAPSOLVER_API_URL
)

logger = logging.getLogger(__name__)

# CDP port range — we pick a free port dynamically so concurrent runs don't collide
_CDP_PORT_START = int(os.environ.get("CHROME_DEBUG_PORT_START", "9222"))
_CDP_PORT_END = 9322


class SurveyCard(BaseModel):
    title: str = Field(description="The survey title or name")
    reward: str = Field(description="Reward amount (e.g., '$1.50')")
    link_url: str = Field(description="The URL or clickable element to start the survey")
    unique_id: Optional[str] = Field(default=None, description="Any unique identifier for the survey")


# ---------------------------------------------------------------------------
# Port utilities
# ---------------------------------------------------------------------------
def _find_free_port(start: int = _CDP_PORT_START, end: int = _CDP_PORT_END) -> int:
    """Return the first TCP port in [start, end] that is not currently bound."""
    for port in range(start, end + 1):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                s.bind(("127.0.0.1", port))
                return port
            except OSError:
                continue
    raise RuntimeError(
        f"No free TCP port found in range {start}–{end}. "
        "Kill stale Chrome processes or widen the port range."
    )


def _kill_chrome_on_port(port: int) -> None:
    """Best-effort: kill any process already listening on `port`."""
    try:
        result = subprocess.run(
            ["lsof", "-ti", f"tcp:{port}"],
            capture_output=True, text=True, timeout=5
        )
        for pid in result.stdout.strip().splitlines():
            try:
                subprocess.run(["kill", "-9", pid], timeout=3)
                logger.info("Killed stale process %s on port %s", pid, port)
            except Exception:
                pass
    except Exception:
        pass


def _kill_chrome_using_profile(profile_path: str, log_func=None, batch_id: str = "") -> None:
    """
    Kill any Chrome process that is currently using `profile_path`.
    This is necessary because ChromeSessionManager sessions leave a
    SingletonLock behind that makes Chrome exit with code 21.
    """
    try:
        import psutil
        killed = []
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                name = (proc.info.get('name') or '').lower()
                cmdline = ' '.join(proc.info.get('cmdline') or [])
                if 'chrome' in name and profile_path in cmdline:
                    os.kill(proc.info['pid'], signal.SIGKILL)
                    killed.append(proc.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied, Exception):
                pass
        if killed and log_func:
            log_func(f"🔪 Killed {len(killed)} Chrome process(es) using profile", batch_id=batch_id)
        if killed:
            time.sleep(2)
    except ImportError:
        # psutil not available — fall back to pkill
        try:
            subprocess.run(
                ["pkill", "-9", "-f", f"chrome.*{profile_path}"],
                timeout=5, capture_output=True,
            )
            time.sleep(2)
        except Exception:
            pass


def _cleanup_profile_locks(profile_path: str, log_func=None, batch_id: str = "") -> None:
    """
    Remove Chrome singleton lock files from a profile directory so Chrome can
    open the profile even if a previous process crashed or was killed externally.

    Files removed:
      <profile>/SingletonLock
      <profile>/SingletonCookie
      <profile>/SingletonSocket
      <profile>/lockfile
      <profile>/Default/SingletonLock  (and siblings)
      <profile>/Default/Last Session
      <profile>/Default/Last Tabs
      <profile>/Default/Current Session
      <profile>/Default/Current Tabs
    """
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
                            log_func(f"⚠️ Could not remove lock {target}: {e2}", "WARNING", batch_id=batch_id)

        # Only remove session files from the Default sub-directory
        if directory.endswith('Default'):
            for name in session_names:
                target = os.path.join(directory, name)
                if os.path.exists(target):
                    try:
                        os.remove(target)
                        removed.append(target)
                    except Exception:
                        pass

    # Also clean up any .org.chromium / .com.google temp socket files
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
        log_func(f"🧹 Removed {len(removed)} stale lock/session file(s)", batch_id=batch_id)
    elif log_func:
        log_func("🧹 No stale lock files found", batch_id=batch_id)


# ---------------------------------------------------------------------------
# Chrome binary discovery
# ---------------------------------------------------------------------------
def _find_chrome() -> str:
    """Return the path to a usable Chrome / Chromium binary."""
    # Prefer the env var set in docker-compose
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
    proxy_string: Optional[str],
    extra_args: List[str],
    cdp_port: int,
) -> subprocess.Popen:
    """
    Start a Chrome process with remote debugging enabled on `cdp_port`.

    Key Docker fixes
    ----------------
    • DISPLAY is forwarded so Chrome can use the Xvfb virtual display.
    • The profile path is passed as a single argument (no shell=True) so
      spaces in directory names are handled correctly.
    • stdout/stderr are captured to a pipe so we can log Chrome startup
      errors instead of silently swallowing them.
    """
    chrome_path = _find_chrome()

    # Build arg list — avoid shell=True so spaces in paths are safe
    args = [
        chrome_path,
        f"--remote-debugging-port={cdp_port}",
        f"--remote-debugging-address=127.0.0.1",
        f"--user-data-dir={user_data_dir}",   # subprocess handles quoting
        "--no-first-run",
        "--no-default-browser-check",
        "--no-sandbox",                        # required inside Docker
        "--disable-dev-shm-usage",             # required inside Docker (/dev/shm too small)
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
        # headless=new is the modern flag; falls back gracefully on older Chrome
        args.append("--headless=new")
    else:
        # Non-headless inside Docker needs the Xvfb display
        args.append(f"--display={os.environ.get('DISPLAY', ':99')}")

    if proxy_string:
        args.append(f"--proxy-server={proxy_string}")

    args.extend(extra_args)

    # Inherit the environment but ensure DISPLAY is set for Chrome
    env = os.environ.copy()
    env.setdefault("DISPLAY", ":99")

    proc = subprocess.Popen(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
    )
    return proc


# ---------------------------------------------------------------------------
# CDP readiness probe
# ---------------------------------------------------------------------------
def _wait_for_cdp(port: int, timeout: float = 30.0, log_func=None) -> bool:
    """Poll until Chrome's CDP /json/version endpoint responds."""
    deadline = time.time() + timeout
    attempt = 0
    while time.time() < deadline:
        attempt += 1
        try:
            r = _requests.get(
                f"http://127.0.0.1:{port}/json/version",
                timeout=2,
            )
            if r.status_code == 200:
                if log_func:
                    log_func(f"✅ CDP responded on port {port} after {attempt} attempts")
                return True
        except Exception:
            pass
        time.sleep(0.75)
    return False


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
    Create a browser-use 0.1.40 Browser with:
      • a persistent Chrome profile (via --user-data-dir)
      • BrightData proxy support
      • stealth / anti-detection flags
      • Docker / Xvfb compatibility

    Strategy
    --------
    browser-use 0.1.40 removed `user_data_dir` from BrowserConfig and also
    removed the `.start()` method from Browser.  The supported path for a
    persistent profile is:
      1. Launch Chrome ourselves with --user-data-dir + --remote-debugging-port
      2. Tell browser-use to connect via CDP  (cdp_url="http://localhost:<port>")
    """

    # ----------------------------------------------------------------
    # Pick a free CDP port (avoids collisions between concurrent runs)
    # ----------------------------------------------------------------
    cdp_port = _find_free_port()
    if log_func:
        log_func(f"🔌 Using CDP port {cdp_port}", batch_id=batch_id)

    # Kill anything already on that port just in case
    _kill_chrome_on_port(cdp_port)

    # ----------------------------------------------------------------
    # Build proxy string
    # ----------------------------------------------------------------
    proxy_string: Optional[str] = None

    if proxy and proxy.get("host") and proxy.get("port"):
        proxy_type = proxy.get("proxy_type", "http")
        host = proxy["host"]
        port = proxy["port"]
        username = proxy.get("username")
        password = proxy.get("password")
        country = proxy.get("country", "US")

        if "brd.superproxy.io" in host and username:
            from .proxy_utils import format_brightdata_username
            username = format_brightdata_username(username, country)
            if log_func:
                log_func(f"🌎 Targeting country: {country} via BrightData", batch_id=batch_id)

        if username and password:
            proxy_string = f"{proxy_type}://{username}:{password}@{host}:{port}"
        else:
            proxy_string = f"{proxy_type}://{host}:{port}"

        if log_func:
            masked = proxy_string.split("@")[0] + "@..." if "@" in proxy_string else proxy_string
            log_func(f"🌐 Using proxy: {masked}", batch_id=batch_id)

    # ----------------------------------------------------------------
    # Ensure the profile directory exists
    # ----------------------------------------------------------------
    os.makedirs(user_data_dir, exist_ok=True)

    # ----------------------------------------------------------------
    # Kill any existing Chrome process using this profile and clean up
    # its lock files.  This is the root cause of "exit code 21":
    # ChromeSessionManager leaves a SingletonLock when a manual session
    # is stopped, and Chrome refuses to open a locked profile.
    # ----------------------------------------------------------------
    if log_func:
        log_func("🔍 Checking for stale Chrome processes / lock files…", batch_id=batch_id)
    _kill_chrome_using_profile(user_data_dir, log_func=log_func, batch_id=batch_id)
    _cleanup_profile_locks(user_data_dir, log_func=log_func, batch_id=batch_id)

    # ----------------------------------------------------------------
    # Launch Chrome
    # ----------------------------------------------------------------
    if log_func:
        log_func(
            f"🖥️ Launching Chrome — profile: {user_data_dir!r}  CDP port: {cdp_port}",
            batch_id=batch_id,
        )

    chrome_proc = _launch_chrome_with_profile(
        user_data_dir=user_data_dir,
        headless=headless,
        proxy_string=proxy_string,
        extra_args=[],   # stealth flags are now inside _launch_chrome_with_profile
        cdp_port=cdp_port,
    )

    # Give Chrome a moment to start, then check whether it died immediately
    await asyncio.sleep(2)
    if chrome_proc.poll() is not None:
        stdout = chrome_proc.stdout.read().decode(errors="replace") if chrome_proc.stdout else ""
        stderr = chrome_proc.stderr.read().decode(errors="replace") if chrome_proc.stderr else ""
        raise RuntimeError(
            f"Chrome exited immediately (code {chrome_proc.returncode}).\n"
            f"STDOUT: {stdout[:500]}\nSTDERR: {stderr[:500]}"
        )

    if not _wait_for_cdp(port=cdp_port, timeout=30.0, log_func=log_func):
        # Collect Chrome's output for debugging before giving up
        stdout = chrome_proc.stdout.read().decode(errors="replace") if chrome_proc.stdout else ""
        stderr = chrome_proc.stderr.read().decode(errors="replace") if chrome_proc.stderr else ""
        chrome_proc.terminate()
        raise RuntimeError(
            f"Chrome did not expose CDP on port {cdp_port} within 30 s.\n"
            f"STDOUT: {stdout[:500]}\nSTDERR: {stderr[:500]}\n"
            "Check that Chrome is installed and the port is not already in use."
        )

    if log_func:
        log_func(f"✅ Chrome CDP ready on port {cdp_port}", batch_id=batch_id)

    # ----------------------------------------------------------------
    # Connect browser-use via CDP
    # ----------------------------------------------------------------
    browser_config = BrowserConfig(
        headless=headless,
        cdp_url=f"http://127.0.0.1:{cdp_port}",
    )
    browser = Browser(config=browser_config)

    # Trigger lazy initialisation
    await browser.get_playwright_browser()

    if log_func:
        log_func("✅ browser-use connected to Chrome via CDP", batch_id=batch_id)

    # ----------------------------------------------------------------
    # Patch browser.close() to also terminate the Chrome subprocess
    # ----------------------------------------------------------------
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

    browser.close = _patched_close  # type: ignore[method-assign]

    return browser


# ---------------------------------------------------------------------------
# Crawl4AI survey extraction
# ---------------------------------------------------------------------------
async def extract_surveys_with_crawl4ai(page_url: str, log_func=None) -> List[SurveyCard]:
    """Use Crawl4AI + LLM to extract survey cards."""
    if log_func:
        log_func(f"🔍 Extracting surveys from {page_url} using Crawl4AI...")
    extraction_strategy = LLMExtractionStrategy(
        provider="openai/gpt-4o",
        schema=SurveyCard.model_json_schema(),
        extraction_type="schema",
        instruction=(
            "Extract all available survey cards. For each, provide title, reward amount, "
            "and the link URL or clickable element identifier. If no URL is present, provide "
            "a CSS selector or text that can be used to click the card."
        ),
    )
    async with AsyncWebCrawler(verbose=True) as crawler:
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
                    log_func(f"✅ Extracted {len(surveys)} surveys")
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
    """
    Detect common CAPTCHA widgets and solve them using Capsolver.
    Supports reCAPTCHA v2, v3, hCaptcha, and Cloudflare Turnstile.
    """
    if not CAPSOLVER_API_KEY:
        if log_func:
            log_func("⚠️ CAPTCHA solving skipped: No Capsolver API key", batch_id=batch_id)
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
                    log_func(f"🛡️ CAPTCHA detected: {selector}. Attempting to solve...", batch_id=batch_id)
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
                        match = re.search(
                            r'data-sitekey[\s]*=[\s]*["\']([^"\']+)["\']', content
                        )
                        if match:
                            sitekey = match.group(1)
                            break

                if sitekey:
                    page_url = page.url
                    if log_func:
                        log_func(f"🔑 Sitekey: {sitekey}, page URL: {page_url}", batch_id=batch_id)
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
                            log_func("✅ CAPTCHA solved and token injected", batch_id=batch_id)
                        await asyncio.sleep(3)
                    else:
                        if log_func:
                            log_func("❌ Failed to solve CAPTCHA via Capsolver", batch_id=batch_id)
                else:
                    if log_func:
                        log_func("⚠️ Could not find sitekey for CAPTCHA", batch_id=batch_id)
        except Exception as e:
            if log_func:
                log_func(f"Error solving CAPTCHA: {e}", "ERROR", batch_id=batch_id)
    return solved


async def call_capsolver(sitekey: str, page_url: str, log_func=None) -> Optional[str]:
    """Call Capsolver API to solve reCAPTCHA / hCaptcha."""
    if not CAPSOLVER_API_KEY:
        return None
    task_type = "ReCaptchaV2TaskProxyless"
    if "hcaptcha" in page_url.lower():
        task_type = "HCaptchaTaskProxyless"

    payload = {
        "clientKey": CAPSOLVER_API_KEY,
        "task": {
            "type": task_type,
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
# Survey agent
# ---------------------------------------------------------------------------
async def run_survey_agent(browser: Browser, llm, persona: str, start_url: str, log_func=None) -> str:
    """Run a single browser-use agent for one complete survey."""
    agent_task = f"""
{persona}

════════════════════════════════════
SURVEY COMPLETION AGENT
════════════════════════════════════
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
    agent = Agent(
        task=agent_task,
        llm=llm,
        browser=browser,
        max_actions_per_step=5,
    )
    result = await asyncio.wait_for(agent.run(max_steps=80), timeout=600.0)
    return detect_survey_outcome(result, log_func)


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
    cfg = MODEL_REGISTRY[model_choice]
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
        log_func("→ Navigating to Google sign-in")
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
            log_func(f"✅ Email filled: {email}")
        await page.keyboard.press("Enter")
        await page.wait_for_timeout(4000)
    except Exception as e:
        raise Exception(f"Google email step failed: {e}")
    try:
        if log_func:
            log_func("Waiting for password input...")
        pwd_sel = 'input[type="password"], input[name="Passwd"]'
        for ct in [
            "Verify it's you",
            "Confirm it's you",
            "This extra step",
            "Get a verification code",
            "Check your phone",
            "Try another way",
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
            log_func("✅ Password filled")
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
        log_func("✅ Password login successful")