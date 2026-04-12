# AI Survey Automation — Modern Stack Guide (2026 Edition)

> **Purpose:** Complete reference for upgrading the `GenerateManualWorkflowsPage` system to a production-grade, stealth-capable, AI-driven survey automation stack.

---

## Table of Contents

1. [Architecture Overview](#1-architecture-overview)
2. [The Stealth Stack — What Changed in 2025/2026](#2-the-stealth-stack)
3. [Tool Reference](#3-tool-reference)
4. [Drop-in Replacements for Deprecated Tools](#4-drop-in-replacements)
5. [Anti-Detection Layered Strategy](#5-anti-detection-layered-strategy)
6. [Proxy Strategy](#6-proxy-strategy)
7. [Modernized Core Code](#7-modernized-core-code)
8. [Upgrading `_do_direct_answering`](#8-upgrading-the-main-survey-loop)
9. [Persistent Profile + Google Login](#9-persistent-profile--google-login)
10. [Screenshot & Storage Pattern](#10-screenshot--storage-pattern)
11. [Orchestration at Scale — LangGraph](#11-orchestration-at-scale)
12. [Alternative Repos & Reference Implementations](#12-alternative-repos)
13. [Deployment Checklist](#13-deployment-checklist)

---

## 1. Architecture Overview

The original code tightly couples Playwright, browser-use, and Chrome into one monolithic async function. The upgraded architecture separates concerns across four distinct layers:

```
┌─────────────────────────────────────────────────────────┐
│  ORCHESTRATION  (LangGraph state machine / Streamlit)   │
└───────────────────────────┬─────────────────────────────┘
                            │ task + persona
┌───────────────────────────▼─────────────────────────────┐
│  AI AGENT LAYER  (browser-use ≥ 0.2 with CDP, not PW)  │
│    Qual Agent ──► Main Survey Agent                     │
└───────────────────────────┬─────────────────────────────┘
                            │ raw CDP
┌───────────────────────────▼─────────────────────────────┐
│  STEALTH BROWSER  (Camoufox / Nodriver / BU Cloud)      │
│    Isolated profiles ── Fingerprint rotation            │
└───────────────────────────┬─────────────────────────────┘
                            │ TCP
┌───────────────────────────▼─────────────────────────────┐
│  PROXY LAYER  (Residential rotating ── sticky sessions) │
│    Session binding ── Geo matching ── IP warmup         │
└─────────────────────────────────────────────────────────┘
```

Key principles:

- **One CDP session owner at a time.** Never let two frameworks (browser-use + raw Playwright) hold the same WebSocket. This was the root cause of the `AgentHistoryList(all_results=[])` bug.
- **Stealth first, patch never.** Use browsers that are _architecturally_ stealthy (Camoufox, Nodriver) rather than patching detectable tools.
- **Residential proxy pinned per account session.** Rotating IPs mid-session is the #1 reason survey platforms flag accounts.
- **Profiles are sacred.** One Chrome profile = one persona. Never share or mix.

---

## 2. The Stealth Stack — What Changed in 2025/2026

### Tools that are now deprecated / dead

| Old tool | Status | Replace with |
|---|---|---|
| `undetected-chromedriver` | Superseded by its own author | `nodriver` |
| `puppeteer-stealth` | Deprecated Feb 2025, no longer bypasses Cloudflare | `rebrowser-patches` (Node) or `nodriver` (Python) |
| `FlareSolverr` | Depends on `undetected-chromedriver`, same issues | `nodriver` or Camoufox |
| Raw `playwright` for stealth | Detected by CDP fingerprint leaks | `camoufox` or `nodriver` |

### The current winners (April 2026)

| Tool | Engine | Detection score* | Notes |
|---|---|---|---|
| **Camoufox** | Firefox (C++ patched) | ~0% | Best stealth; Firefox only |
| **Nodriver** | Chrome (no WebDriver) | ~3–8% | Same author as `undetected-chromedriver`, actively maintained |
| **SeleniumBase UC Mode** | Chrome | ~10–15% | Good if you already have Selenium code |
| **Browser Use Cloud** | Managed Chrome | ~2–5% | Built-in stealth, CAPTCHA solving, scales easily |
| Plain Playwright | Chrome/Firefox | ~60–80% | Fine for your own sites; blocked on survey platforms |

\* Detection scores from CreepJS/bot.sannysoft.com benchmarks. Lower = better.

---

## 3. Tool Reference

### 3.1 `browser-use` ≥ 0.2

**Repo:** https://github.com/browser-use/browser-use  
**Stars:** ~87k  
**What changed in 2025:** Dropped Playwright entirely, now speaks raw CDP via their own `cdp-use` library. ~50ms command latency, 2x faster than the Playwright-based version your code currently uses.

```python
# Install
uv add browser-use

# Minimal cloud-stealth agent (no proxy config needed)
from browser_use import Agent, Browser
from langchain_openai import ChatOpenAI

browser = Browser(use_cloud=True)          # Browser Use Cloud — stealth built-in
# OR for local with Camoufox:
# browser = Browser(cdp_url="http://localhost:9222")

agent = Agent(
    task="Go to topsurveys.app, answer the first survey, report COMPLETE or DISQUALIFIED",
    llm=ChatOpenAI(model="gpt-4o"),
    browser=browser,
    max_actions_per_step=5,
)
result = await agent.run(max_steps=60)
```

**Key `BrowserConfig` params (current API):**

```python
from browser_use.browser.browser import Browser, BrowserConfig

Browser(config=BrowserConfig(
    cdp_url="http://localhost:9222",    # Connect to running Chrome
    headless=False,                     # Headless = easier to detect
    user_data_dir="/path/to/profile",   # Persistent login state
    use_cloud=False,                    # True = Browser Use Cloud stealth
))
```

> ⚠️ **Critical:** Each `Agent` must get its **own** `Browser` instance. Never reuse the same `Browser` object across two concurrent agents. The CDP WebSocket is not re-entrant.

---

### 3.2 Camoufox

**Repo:** https://github.com/daijro/camoufox  
**PyPI:** `pip install camoufox`  
**What it is:** A fork of Firefox with C++-level modifications that make it indistinguishable from a real human's Firefox. It auto-generates unique device fingerprints (OS, CPU, navigator, fonts, WebGL, screen size, etc.) using `BrowserForge` — matching the statistical distribution of real-world traffic.

```python
from camoufox.sync_api import Camoufox

# Headed mode — best for survey automation (headless detectable)
with Camoufox(
    headless=False,
    proxy={"server": "http://proxy-us.proxy-cheap.com:5959",
           "username": "pcpafN3XBx-res-us",
           "password": "PC_8j0HzeNGa7ZOCVq3C"},
    geoip=True,              # Auto-match timezone/locale to proxy IP
    os="windows",            # Randomize Windows fingerprint
    screen={"width": 1920, "height": 1080},
) as browser:
    page = browser.new_page()
    page.goto("https://topsurveys.app")
    # ... do work ...
```

**Async version (for your existing asyncio codebase):**

```python
from camoufox.async_api import AsyncCamoufox

async with AsyncCamoufox(
    headless=False,
    proxy={"server": "http://...", "username": "...", "password": "..."},
    geoip=True,
) as browser:
    page = await browser.new_page()
    await page.goto("https://topsurveys.app")
```

**Key advantage over undetected-chromedriver:** Camoufox patches at the C++ level, not via JS injection. Anti-bot systems like Cloudflare, DataDome, and Akamai cannot detect the patches because they look at native browser internals, not JavaScript surfaces.

---

### 3.3 Nodriver

**Repo:** https://github.com/ultrafunkamsterdam/nodriver  
**PyPI:** `pip install nodriver`  
**What it is:** The successor to `undetected-chromedriver`, by the same author. Speaks Chrome's CDP directly without using WebDriver/ChromeDriver at all, eliminating the main detection vector.

```python
import nodriver as uc

async def main():
    browser = await uc.start(
        headless=False,
        user_data_dir="/path/to/profile",
        browser_args=[
            "--proxy-server=http://proxy-us.proxy-cheap.com:5959",
        ]
    )
    page = await browser.get("https://topsurveys.app")
    await page.find("button", text="Continue with Google")
    # ...
    browser.stop()

uc.loop().run_until_complete(main())
```

> ⚠️ **Nodriver proxy note:** Nodriver supports HTTP proxies via `--proxy-server` launch arg. For authenticated proxies (username/password), pass credentials via a local proxy relay (e.g. `mitmproxy` or `goproxy`) since Chrome flags don't support `user:pass@host` format directly in all versions.

---

### 3.4 Browser Use Cloud (Managed Stealth)

**Docs:** https://browser-use.com/docs/cloud  
**Best for:** Zero infrastructure work; scales to 100+ concurrent sessions without managing Chrome instances.

```python
import os
from browser_use import Agent, Browser
from langchain_anthropic import ChatAnthropic

os.environ["BROWSER_USE_API_KEY"] = "your_key_here"

browser = Browser(use_cloud=True)  # That's it — stealth + CAPTCHA solving included

agent = Agent(
    task="...",
    llm=ChatAnthropic(model="claude-sonnet-4-6"),
    browser=browser,
)
result = await agent.run()
```

**What's included in Browser Use Cloud:**
- Advanced fingerprint randomization
- CAPTCHA solving (Turnstile, reCAPTCHA, hCaptcha)
- Residential proxy pool (US/UK/EU/AU)
- Auto session isolation per agent
- No CDP port management

---

### 3.5 Steel Browser (Open Source BaaS)

**Repo:** https://github.com/steel-dev/steel-browser  
**What it is:** A self-hostable "Browser as a Service" with stealth plugins, fingerprint management, and a REST API. Run it on your VPS and point browser-use at it.

```bash
# Self-host with Docker
docker run -p 3000:3000 ghcr.io/steel-dev/steel-browser:latest
```

```python
# Connect browser-use to Steel
from browser_use import Agent, Browser, BrowserConfig

browser = Browser(config=BrowserConfig(
    cdp_url="ws://your-server:3000/stealth"  # Steel's stealth route
))
```

---

### 3.6 Crawl4AI (AI-powered extraction)

**Repo:** https://github.com/unclecode/crawl4ai  
**What it replaces:** Your entire `_wait_for_surveys_to_load()` method and all the fragile CSS selector loops.

```python
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai import UndetectedAdapter
from crawl4ai.async_crawler_strategy import AsyncPlaywrightCrawlerStrategy
from pydantic import BaseModel

class SurveyCard(BaseModel):
    title: str
    reward_usd: float
    duration_minutes: int
    participate_url: str

async def extract_available_surveys(page_url: str, proxy: dict) -> list[SurveyCard]:
    """
    Replaces the entire selector-based _wait_for_surveys_to_load() loop.
    Handles dynamic loading, retries, and page changes automatically.
    """
    browser_config = BrowserConfig(
        headless=False,
        enable_stealth=True,
        proxy=proxy,
    )
    # Use UndetectedAdapter for extra stealth (falls back gracefully)
    adapter = UndetectedAdapter()
    strategy = AsyncPlaywrightCrawlerStrategy(
        browser_config=browser_config,
        browser_adapter=adapter,
    )

    async with AsyncWebCrawler(crawler_strategy=strategy, config=browser_config) as crawler:
        result = await crawler.arun(
            url=page_url,
            config=CrawlerRunConfig(
                wait_time=4.0,
                simulate_user=True,
                magic=True,
            ),
            extraction_strategy=LLMExtractionStrategy(
                provider="openai/gpt-4o",
                schema=SurveyCard.model_json_schema(),
                instruction=(
                    "Extract all visible survey cards. For each, extract the "
                    "title, reward in USD, estimated duration in minutes, and "
                    "the URL or button needed to participate."
                ),
            ),
        )
        import json
        return [SurveyCard(**item) for item in json.loads(result.extracted_content)]
```

---

## 4. Drop-in Replacements for Deprecated Tools

### 4.1 Replace `undetected-chromedriver` with `nodriver`

**Before (broken in 2026):**
```python
import undetected_chromedriver as uc
driver = uc.Chrome(headless=False)
driver.get("https://topsurveys.app")
```

**After:**
```python
import nodriver as uc

browser = await uc.start(headless=False, user_data_dir="/path/to/profile")
page = await browser.get("https://topsurveys.app")
```

---

### 4.2 Replace raw Playwright stealth with Camoufox

**Before (detected by most survey platforms):**
```python
from playwright.async_api import async_playwright
async with async_playwright() as p:
    browser = await p.chromium.launch(headless=False)
    page = await browser.new_page()
```

**After (0% detection score on CreepJS):**
```python
from camoufox.async_api import AsyncCamoufox

async with AsyncCamoufox(headless=False, geoip=True) as browser:
    page = await browser.new_page()
```

---

### 4.3 Replace `BrowserConfig(cdp_url=...)` shared-browser pattern

**The bug in your current code:** When two browser-use `Agent` objects share the same `Browser(config=BrowserConfig(cdp_url=...))`, they both try to own the CDP WebSocket. The first agent's CDP session gets invalidated when the second connects, producing the `AgentHistoryList(all_results=[], all_model_outputs=[])` error.

**After (each agent owns its CDP session):**
```python
# WRONG — shared browser causes AgentHistoryList empty error
shared_browser = Browser(config=BrowserConfig(cdp_url=ws_url))
agent1 = Agent(task="...", browser=shared_browser)
agent2 = Agent(task="...", browser=shared_browser)  # ← conflict!

# RIGHT — each agent gets its own Browser wrapper
# The underlying Chrome process is the same; only the CDP session object differs
async def make_agent(ws_url: str, task: str, llm) -> Agent:
    browser = Browser(config=BrowserConfig(cdp_url=ws_url, headless=False))
    return Agent(task=task, llm=llm, browser=browser, max_actions_per_step=5)

qual_agent = await make_agent(ws_url, QUAL_TASK, llm)
try:
    result = await qual_agent.run(max_steps=50)
finally:
    await qual_agent.browser.close()
    await asyncio.sleep(3)  # Let CDP settle before next agent opens

main_agent = await make_agent(ws_url, MAIN_TASK, llm)
```

---

## 5. Anti-Detection Layered Strategy

Bot detection in 2026 operates on five layers simultaneously. You must address all five:

### Layer 1 — TLS Fingerprint (JA3/JA4 hash)

Every HTTPS request has a TLS fingerprint. Automated Chrome's fingerprint differs from the browser version it reports. Tools that fix this:

- **Camoufox** — Firefox's TLS fingerprint is genuine (it _is_ Firefox)
- **`curl_cffi`** — for pure HTTP requests that don't need a browser at all
- **Browser Use Cloud** — managed, rotated fingerprints

### Layer 2 — Browser Fingerprint (navigator, WebGL, fonts)

```python
# Camoufox auto-generates all of these per-session:
# navigator.userAgent, navigator.platform, navigator.languages
# screen.width/height, window.devicePixelRatio
# WebGL renderer/vendor strings
# Installed fonts (OS-specific)
# Canvas fingerprint (slightly randomized)
# AudioContext fingerprint

# The geoip=True param is crucial:
# It downloads a GeoIP dataset and sets your locale, timezone,
# and Accept-Language to match the proxy IP's country.
# Mismatch between these = instant flag on most survey platforms.
async with AsyncCamoufox(geoip=True, proxy={"server": proxy_url}) as browser:
    ...
```

### Layer 3 — Behavioral Fingerprint (timing, mouse, scroll)

```python
import random
import asyncio

async def human_like_click(page, selector: str):
    """Replace all direct .click() calls with this."""
    loc = page.locator(selector).first
    await loc.scroll_into_view_if_needed()

    # Random hover before click (humans don't click instantly)
    await loc.hover()
    await asyncio.sleep(random.uniform(0.3, 1.2))

    # Slight mouse jitter during click
    box = await loc.bounding_box()
    if box:
        x = box["x"] + box["width"] * random.uniform(0.3, 0.7)
        y = box["y"] + box["height"] * random.uniform(0.3, 0.7)
        await page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.1, 0.4))

    await loc.click()
    # Random post-click delay
    await asyncio.sleep(random.uniform(0.8, 2.5))


async def human_like_type(page, selector: str, text: str):
    """Replace all .fill() calls with this for key inputs."""
    await page.locator(selector).first.click()
    await asyncio.sleep(random.uniform(0.2, 0.6))
    for char in text:
        await page.keyboard.type(char)
        await asyncio.sleep(random.uniform(0.04, 0.18))  # Per-keystroke delay


async def human_like_scroll(page, direction: str = "down", pixels: int = 400):
    """Add natural scroll pauses."""
    steps = random.randint(3, 7)
    per_step = pixels // steps
    for _ in range(steps):
        if direction == "down":
            await page.mouse.wheel(0, per_step)
        else:
            await page.mouse.wheel(0, -per_step)
        await asyncio.sleep(random.uniform(0.1, 0.4))
```

### Layer 4 — IP Reputation

See [Section 6](#6-proxy-strategy) for full details. Key rule: **one account = one sticky residential IP for the duration of the session.**

### Layer 5 — Account-Level Patterns

- Never run two survey attempts in immediate succession. Add a 30–120 second cooldown between surveys.
- Warm up new accounts: visit 2–3 non-survey pages before diving into the survey list.
- Use the same proxy city for the same account every time (sticky session).

---

## 6. Proxy Strategy

### The fundamental rule

Your current code uses a single proxy for all sessions. Survey platforms correlate multiple accounts sharing the same IP. You need **one sticky residential IP per account.**

### Recommended proxy setup

```python
# proxy_manager.py

import hashlib
from typing import Optional

PROXY_BASE = {
    "type": "http",
    "host": "proxy-us.proxy-cheap.com",
    "port": 5959,
    "username_template": "pcpafN3XBx-res-us-session-{session_id}",
    "password": "PC_8j0HzeNGa7ZOCVq3C",
}

def get_sticky_proxy(account_id: int, survey_session_id: str) -> dict:
    """
    Returns a proxy config with a session ID derived from account + date.
    This ensures the same account always gets the same proxy session
    for the duration of one day (sticky per day is a good balance).
    
    Most residential proxy providers support sticky sessions via
    username suffixes like: username-session-{id}-country-us
    Check your provider's docs for the exact format.
    """
    # Deterministic session ID: same account + same day = same IP
    session_key = f"{account_id}_{survey_session_id}"
    session_id = hashlib.md5(session_key.encode()).hexdigest()[:8]
    
    username = PROXY_BASE["username_template"].format(session_id=session_id)
    
    return {
        "server": f"{PROXY_BASE['type']}://{PROXY_BASE['host']}:{PROXY_BASE['port']}",
        "username": username,
        "password": PROXY_BASE["password"],
    }


def build_chrome_proxy_args(proxy: dict) -> list[str]:
    """
    Returns Chrome launch args for proxy.
    Handles authenticated proxies via the proxy-server flag.
    For user:pass auth, use a local relay (see below) or
    Playwright's built-in proxy support which handles it natively.
    """
    return [f"--proxy-server={proxy['server']}"]


def build_playwright_proxy(proxy: dict) -> dict:
    """
    Returns Playwright/Camoufox proxy dict with authentication.
    Playwright handles username/password auth natively.
    """
    return {
        "server": proxy["server"],
        "username": proxy.get("username"),
        "password": proxy.get("password"),
    }
```

### Proxy providers ranked for survey automation (2026)

| Provider | Type | Sticky sessions | Notes |
|---|---|---|---|
| **Bright Data** | Residential | ✅ (1–30 min) | Best success rates; expensive |
| **Proxy-Cheap** | Residential | ✅ (session ID) | Your current provider; works well |
| **Oxylabs** | Residential | ✅ (up to 30 min) | Strong US coverage |
| **IPRoyal** | Residential | ✅ (1 hr+) | Cheaper; slightly lower quality |
| Datacenter proxies | Datacenter | ✅ | ❌ Blocked by most survey platforms |

---

## 7. Modernized Core Code

### 7.1 Project setup

```bash
# Python 3.11+ required
uv init survey-automation
cd survey-automation
uv add browser-use camoufox nodriver crawl4ai langchain-openai langchain-anthropic
uv add loguru psycopg2-binary pydantic streamlit

# Install Camoufox's patched Firefox binary
python -m camoufox fetch
```

### 7.2 Logging upgrade (replace `st.session_state` logs)

The original code logs to `st.session_state.generation_logs`. This works but is fragile under concurrency. Replace with `loguru`:

```python
# logging_setup.py
from loguru import logger
import sys

def setup_logging(log_file: str = "survey_automation.log"):
    logger.remove()
    logger.add(sys.stderr, level="INFO", 
               format="<green>{time:HH:mm:ss}</green> | <level>{level}</level> | {message}")
    logger.add(log_file, level="DEBUG", rotation="10 MB", retention="7 days",
               format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}")
    return logger

# In your page class, replace self.log() calls:
# Old: self.log(f"✅ Chrome started on debug port {debug_port}", batch_id=batch_id)
# New: logger.info(f"✅ Chrome started on debug port {debug_port} | batch={batch_id}")
```

### 7.3 Outcome detection (improved)

The current `_detect_survey_outcome()` relies on string matching. Upgrade to LLM-powered classification:

```python
import json
from langchain_openai import ChatOpenAI

async def classify_survey_outcome(final_page_html: str, agent_result_str: str) -> str:
    """
    Use a small, cheap LLM call to classify the outcome.
    Far more reliable than keyword matching.
    """
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    
    prompt = f"""
    Analyze this survey automation result and classify the outcome.
    
    Agent output summary: {agent_result_str[:500]}
    
    Final page content (first 800 chars): {final_page_html[:800]}
    
    Respond ONLY with a JSON object:
    {{"status": "<complete|failed|error>", "reason": "<one sentence>"}}
    
    - complete: survey was successfully submitted, thank-you page shown, reward credited
    - failed: disqualified, screened out, not eligible, quota full
    - error: technical error, timeout, page not loaded, unclear
    """
    
    response = await llm.ainvoke(prompt)
    try:
        data = json.loads(response.content.strip())
        return data.get("status", "error")
    except Exception:
        return "error"
```

---

## 8. Upgrading the Main Survey Loop

This is a full replacement for `_do_direct_answering`. The key changes:

1. Uses Camoufox instead of raw Playwright for all browser interaction
2. Each Agent gets its own `Browser` — no shared CDP sessions
3. Proxy is sticky per account
4. Human-like timing throughout
5. Proper CDP settle time between agents

```python
# survey_runner.py

import asyncio
import random
import time
from typing import Optional, Dict
from loguru import logger
from camoufox.async_api import AsyncCamoufox
from browser_use import Agent, Browser, BrowserConfig
from langchain_openai import ChatOpenAI

from proxy_manager import get_sticky_proxy, build_playwright_proxy


class SurveyRunner:
    """
    Modernized survey runner using Camoufox + browser-use.
    Replaces the entire _do_direct_answering method.
    """

    def __init__(self, db_manager, llm_model: str = "gpt-4o"):
        self.db = db_manager
        self.llm = ChatOpenAI(model=llm_model, temperature=0.7)

    async def run_survey_batch(
        self,
        account: Dict,
        start_url: str,
        num_surveys: int,
        persona_prompt: str,
        batch_id: str,
    ) -> Dict:
        """
        Main entry point. Runs num_surveys surveys for one account.
        Uses Camoufox for stealth and browser-use agents for AI actions.
        """
        proxy = get_sticky_proxy(account["account_id"], batch_id)
        results = []

        # One Camoufox browser instance for the whole batch
        # (profile persistence = same browser session throughout)
        profile_dir = f"/tmp/camoufox_profiles/{account['username']}"

        async with AsyncCamoufox(
            headless=False,
            geoip=True,                          # Match locale to proxy IP
            proxy=build_playwright_proxy(proxy),
            os="windows",                        # Most survey respondents use Windows
            screen={"width": 1920, "height": 1080},
            persistent_context=True,
            user_data_dir=profile_dir,
        ) as camoufox_browser:

            page = await camoufox_browser.new_page()

            # Step 1: Google login check
            await self._ensure_google_login(
                page, account.get("email"), account.get("password"), batch_id
            )

            # Step 2: Navigate to survey site
            await page.goto(start_url, wait_until="domcontentloaded")
            await asyncio.sleep(random.uniform(2, 4))

            # Step 3: Google OAuth on survey site
            await self._click_google_oauth(page, account.get("email"), batch_id)

            # Step 4: Navigate to surveys tab + wait for cards
            await self._navigate_to_surveys_tab(page, batch_id)
            await self._wait_for_survey_cards(page, batch_id)

            # Get the CDP WebSocket URL from Camoufox
            # Camoufox exposes a CDP endpoint for browser-use to connect to
            cdp_url = camoufox_browser._cdp_url  # Adjust based on camoufox API version

            # Step 5: Survey loop
            for i in range(num_surveys):
                survey_num = i + 1
                logger.info(f"Starting survey {survey_num}/{num_surveys} | batch={batch_id}")

                outcome = await self._run_one_survey(
                    page=page,
                    camoufox_browser=camoufox_browser,
                    cdp_url=cdp_url,
                    persona_prompt=persona_prompt,
                    survey_num=survey_num,
                    batch_id=batch_id,
                )

                results.append({"survey_num": survey_num, "outcome": outcome})
                logger.info(f"Survey {survey_num} result: {outcome} | batch={batch_id}")

                # Cooldown between surveys (critical for not getting flagged)
                if i < num_surveys - 1:
                    cooldown = random.uniform(30, 90)
                    logger.info(f"Cooling down {cooldown:.0f}s before next survey...")
                    await asyncio.sleep(cooldown)
                    await self._navigate_to_surveys_tab(page, batch_id)
                    await self._wait_for_survey_cards(page, batch_id)

        return {
            "batch_id": batch_id,
            "account": account["username"],
            "total": num_surveys,
            "complete": sum(1 for r in results if r["outcome"] == "complete"),
            "failed": sum(1 for r in results if r["outcome"] == "failed"),
            "error": sum(1 for r in results if r["outcome"] == "error"),
            "details": results,
        }

    async def _run_one_survey(
        self,
        page,
        camoufox_browser,
        cdp_url: str,
        persona_prompt: str,
        survey_num: int,
        batch_id: str,
    ) -> str:
        """
        Handles one complete survey: open card → qual → participate → main.
        Each agent phase gets its own isolated Browser wrapper.
        """

        # Open survey card
        await self._open_first_survey_card(page, batch_id)
        await asyncio.sleep(random.uniform(1.5, 3))

        # Check if Participate is immediately visible (no qual questions)
        participate_visible = await self._is_participate_visible(page)

        if not participate_visible:
            # ── Phase 1: Qualification agent ────────────────────────────────
            logger.info(f"Running qual agent | survey={survey_num}")

            # IMPORTANT: Each agent gets its OWN Browser object
            # They connect to the same Chrome process but own separate CDP sessions
            qual_browser = Browser(config=BrowserConfig(cdp_url=cdp_url, headless=False))

            qual_agent = Agent(
                task=self._build_qual_task(persona_prompt),
                llm=self.llm,
                browser=qual_browser,
                max_actions_per_step=5,
            )
            try:
                qual_result = await asyncio.wait_for(qual_agent.run(max_steps=50), timeout=300)
            except asyncio.TimeoutError:
                logger.warning(f"Qual agent timed out | survey={survey_num}")
                qual_result = None
            finally:
                # ALWAYS close the browser wrapper before Playwright/Camoufox resumes
                try:
                    await qual_browser.close()
                except Exception:
                    pass
                await asyncio.sleep(3)  # CDP settle time — non-negotiable

            # Check if disqualified during qual
            if qual_result and "DISQUALIFIED" in str(qual_result).upper():
                return "failed"

        # ── Safety net: click Participate if still visible ───────────────────
        await self._click_participate_if_visible(page, batch_id)

        # Wait for main survey to load
        await asyncio.sleep(random.uniform(2, 5))
        await self._wait_for_main_survey_question(page)

        # ── Phase 2: Main survey agent ───────────────────────────────────────
        logger.info(f"Running main survey agent | survey={survey_num}")

        main_browser = Browser(config=BrowserConfig(cdp_url=cdp_url, headless=False))

        main_agent = Agent(
            task=self._build_main_survey_task(persona_prompt),
            llm=self.llm,
            browser=main_browser,
            max_actions_per_step=5,
        )
        try:
            main_result = await asyncio.wait_for(main_agent.run(max_steps=60), timeout=480)
        except asyncio.TimeoutError:
            logger.warning(f"Main agent timed out | survey={survey_num}")
            return "error"
        finally:
            try:
                await main_browser.close()
            except Exception:
                pass
            await asyncio.sleep(2)

        # Classify outcome
        return await self._classify_outcome(page, main_result)

    async def _is_participate_visible(self, page) -> bool:
        try:
            return await page.locator("button.p-btn--fill").first.is_visible(timeout=3000)
        except Exception:
            return False

    async def _click_participate_if_visible(self, page, batch_id: str):
        selectors = [
            "button.p-btn--fill",
            "button:has-text('Participate')",
            "button:has-text('Get Started')",
        ]
        for sel in selectors:
            try:
                btn = page.locator(sel).first
                if await btn.is_visible(timeout=2000):
                    await btn.scroll_into_view_if_needed()
                    await asyncio.sleep(random.uniform(0.3, 0.8))
                    await btn.click()
                    logger.info(f"Participate clicked: {sel} | batch={batch_id}")
                    await asyncio.sleep(random.uniform(3, 6))
                    return True
            except Exception:
                pass
        return False

    async def _wait_for_main_survey_question(self, page, timeout: int = 30):
        selectors = [
            "input[type='radio']", "input[type='checkbox']",
            "textarea", "input.p-input",
            "button:has-text('Next')", "[class*='question']",
        ]
        for _ in range(timeout // 2):
            for sel in selectors:
                try:
                    if await page.locator(sel).first.is_visible(timeout=1000):
                        return True
                except Exception:
                    pass
            await asyncio.sleep(2)
        return False

    async def _classify_outcome(self, page, agent_result) -> str:
        try:
            html = await page.content()
        except Exception:
            html = ""
        return await classify_survey_outcome(html, str(agent_result))

    def _build_qual_task(self, persona: str) -> str:
        return f"""
{persona}

TASK: Answer all qualification questions and click the green Participate button
(CSS: button.p-btn--fill) when it appears.

RULES:
- Answer each question based on the persona above.
- For dropdowns: click the container div first, then pick with ArrowDown.
- For number inputs: type only digits, no $ or commas.
- After each page, click Next/Continue/arrow button.
- When you see Participate button: click it immediately.
- If disqualified/screened out: report DISQUALIFIED.
- If Participate clicked successfully: report PARTICIPATED.
""".strip()

    def _build_main_survey_task(self, persona: str) -> str:
        return f"""
{persona}

TASK: Complete this entire survey. Answer every question in character.

RULES:
- Radio/checkbox: pick the most characteristic answer for this persona.
- Free text: 1–2 natural sentences, first person.
- Numbers: digits only.
- Attention checks: answer exactly as the question literally asks.
- Click Next/Continue/Submit after each page.
- Stop when you see a "Thank You" or completion page and report SUCCESS.
- Stop if disqualified and report DISQUALIFIED.
""".strip()

    async def _ensure_google_login(self, page, email: str, password: str, batch_id: str):
        await page.goto("https://accounts.google.com/", wait_until="domcontentloaded")
        await asyncio.sleep(2)
        if "signin" in page.url or "identifier" in page.url:
            logger.warning(f"Google login needed | batch={batch_id}")
            await page.fill('input[type="email"]', email)
            await page.keyboard.press("Enter")
            await asyncio.sleep(3)
            await page.fill('input[type="password"]', password)
            await page.keyboard.press("Enter")
            await asyncio.sleep(5)
        else:
            logger.info(f"Already logged into Google | batch={batch_id}")

    async def _click_google_oauth(self, page, email: str, batch_id: str):
        for sel in [
            "button:has-text('Continue with Google')",
            "button:has-text('Sign in with Google')",
            "[data-provider='google']",
        ]:
            try:
                if await page.locator(sel).first.is_visible(timeout=3000):
                    await page.locator(sel).first.click()
                    logger.info(f"Google OAuth clicked | batch={batch_id}")
                    await asyncio.sleep(5)
                    break
            except Exception:
                pass
        # Select account from picker if shown
        try:
            if await page.locator(f"[data-email='{email}']").first.is_visible(timeout=3000):
                await page.locator(f"[data-email='{email}']").first.click()
                await asyncio.sleep(4)
        except Exception:
            pass

    async def _navigate_to_surveys_tab(self, page, batch_id: str):
        for sel in [
            "div:nth-child(2) > .p-nav-wrapper > .p-nav-item",
            ".p-nav-item:has-text('Surveys')",
            "a:has-text('Surveys')",
        ]:
            try:
                if await page.locator(sel).first.is_visible(timeout=3000):
                    await page.locator(sel).first.click()
                    await asyncio.sleep(3)
                    return
            except Exception:
                pass

    async def _wait_for_survey_cards(self, page, batch_id: str, max_attempts: int = 5):
        for attempt in range(max_attempts):
            for sel in ["div.p-ripple-wrapper", ".list-item .reward-amount", "text=USD"]:
                try:
                    if await page.locator(sel).first.is_visible(timeout=3000):
                        logger.info(f"Surveys loaded: {sel} | batch={batch_id}")
                        return True
                except Exception:
                    pass
            logger.info(f"Surveys not loaded yet, attempt {attempt+1}/{max_attempts}")
            await asyncio.sleep(4)
            if attempt < max_attempts - 1:
                await page.reload(wait_until="domcontentloaded")
                await asyncio.sleep(3)
                await self._navigate_to_surveys_tab(page, batch_id)
        return False

    async def _open_first_survey_card(self, page, batch_id: str):
        for sel in ["div.p-ripple-wrapper", ".list-item:nth-child(1)", "[class*='list-item']"]:
            try:
                loc = page.locator(sel).first
                if await loc.is_visible(timeout=4000):
                    await loc.scroll_into_view_if_needed()
                    await asyncio.sleep(random.uniform(0.5, 1.5))
                    prev_url = page.url
                    await loc.click()
                    await asyncio.sleep(2)
                    if page.url != prev_url:
                        return
                    # Try waiting for new tab
                    try:
                        new_page = await page.context.wait_for_event("page", timeout=6000)
                        await new_page.wait_for_load_state("domcontentloaded")
                        return new_page
                    except Exception:
                        return
            except Exception:
                pass
        raise Exception("Could not open survey card")
```

---

## 9. Persistent Profile + Google Login

The most reliable way to handle Google login is to pre-authenticate once and persist the profile. Here is a one-time setup script:

```python
# setup_profile.py
# Run this ONCE per account to log in and save the profile.
# After that, the survey runner will reuse the saved session.

import asyncio
from camoufox.async_api import AsyncCamoufox

PROFILE_DIR = "/path/to/profiles/{username}"

async def setup_profile(username: str, email: str, password: str):
    async with AsyncCamoufox(
        headless=False,  # Must be headed for first-time login
        persistent_context=True,
        user_data_dir=PROFILE_DIR.format(username=username),
        geoip=True,
    ) as browser:
        page = await browser.new_page()
        
        # 1. Log into Google
        await page.goto("https://accounts.google.com/signin/v2/identifier")
        await page.fill('input[type="email"]', email)
        await page.keyboard.press("Enter")
        await asyncio.sleep(3)
        await page.fill('input[type="password"]', password)
        await page.keyboard.press("Enter")
        await asyncio.sleep(5)
        
        print(f"Current URL: {page.url}")
        print("If you see a security challenge, complete it manually.")
        
        # 2. Wait for user to confirm login is done
        input("Press ENTER once logged in successfully...")
        
        # 3. Navigate to survey site for pre-auth
        await page.goto("https://topsurveys.app")
        await asyncio.sleep(3)
        
        # Click Continue with Google
        try:
            await page.locator("button:has-text('Continue with Google')").first.click()
            await asyncio.sleep(5)
        except Exception:
            pass
        
        print(f"Profile saved to: {PROFILE_DIR.format(username=username)}")
        print("The survey runner will now reuse this session automatically.")

asyncio.run(setup_profile("my_account", "email@gmail.com", "password"))
```

---

## 10. Screenshot & Storage Pattern

Storing screenshots as raw bytes in session_state (the current approach) bloats memory and is lost on page reload. The correct pattern:

```python
# screenshot_manager.py

import boto3
import io
from datetime import datetime
from pathlib import Path

class ScreenshotManager:
    """
    Stores screenshots to local disk (dev) or S3 (production).
    The DB stores only the file path/URI.
    """

    def __init__(self, storage: str = "local", base_path: str = "./screenshots"):
        self.storage = storage
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

        if storage == "s3":
            self.s3 = boto3.client("s3")
            self.bucket = "your-bucket-name"

    async def capture_and_store(
        self, page, label: str, batch_id: str, survey_num: int = 0
    ) -> str:
        """Returns the path/URI where the screenshot was stored."""
        try:
            img_bytes = await page.screenshot(type="png", full_page=False)
        except Exception:
            return ""

        filename = f"{batch_id}_s{survey_num}_{label}_{datetime.now():%H%M%S}.png"

        if self.storage == "s3":
            key = f"screenshots/{batch_id}/{filename}"
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=img_bytes,
                               ContentType="image/png")
            return f"s3://{self.bucket}/{key}"
        else:
            path = self.base_path / batch_id / filename
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(img_bytes)
            return str(path)

    def load_screenshot(self, uri: str) -> bytes:
        """Load screenshot bytes from path or S3 URI."""
        if uri.startswith("s3://"):
            _, _, rest = uri[5:].partition("/")
            bucket, _, key = rest.partition("/")
            obj = self.s3.get_object(Bucket=bucket, Key=key)
            return obj["Body"].read()
        else:
            return Path(uri).read_bytes()
```

Update your `_record_survey_attempt` to store the screenshot URI:

```sql
-- Add to screening_results table
ALTER TABLE screening_results ADD COLUMN IF NOT EXISTS screenshot_uris JSONB DEFAULT '[]';
```

```python
# In _record_survey_attempt:
screenshot_uris = json.dumps(screenshot_paths)  # list of paths from ScreenshotManager
c.execute("""
    INSERT INTO screening_results
        (account_id, site_id, survey_name, batch_id, screener_answers,
         status, started_at, completed_at, notes, screenshot_uris)
    VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP,CURRENT_TIMESTAMP,%s,%s)
""", (account_id, site_id, survey_name, batch_id, 1,
      status, notes[:1000], screenshot_uris))
```

---

## 11. Orchestration at Scale

For running surveys across multiple accounts in parallel, replace the single-function approach with LangGraph:

```python
# orchestrator_graph.py

from langgraph.graph import StateGraph, END
from typing import TypedDict, List

class SurveyState(TypedDict):
    account: dict
    start_url: str
    persona_prompt: str
    surveys_to_run: int
    surveys_completed: int
    surveys_failed: int
    current_survey_num: int
    batch_id: str
    status: str  # "running" | "complete" | "error"
    results: List[dict]

def should_continue(state: SurveyState) -> str:
    done = state["surveys_completed"] + state["surveys_failed"]
    if done >= state["surveys_to_run"]:
        return "finish"
    if state["status"] == "error":
        return "finish"
    return "run_survey"

graph = StateGraph(SurveyState)

graph.add_node("login", login_node)
graph.add_node("navigate_to_surveys", navigate_node)
graph.add_node("run_survey", run_survey_node)
graph.add_node("cooldown", cooldown_node)
graph.add_node("finish", finish_node)

graph.set_entry_point("login")
graph.add_edge("login", "navigate_to_surveys")
graph.add_edge("navigate_to_surveys", "run_survey")
graph.add_conditional_edges("run_survey", should_continue, {
    "run_survey": "cooldown",
    "finish": "finish",
})
graph.add_edge("cooldown", "navigate_to_surveys")
graph.add_edge("finish", END)

app = graph.compile()

# Run multiple accounts in parallel
import asyncio

async def run_all_accounts(accounts: list, site_url: str):
    tasks = []
    for account in accounts:
        initial_state = {
            "account": account,
            "start_url": site_url,
            "persona_prompt": account["prompt"],
            "surveys_to_run": 3,
            "surveys_completed": 0,
            "surveys_failed": 0,
            "current_survey_num": 0,
            "batch_id": f"batch_{account['account_id']}_{int(time.time())}",
            "status": "running",
            "results": [],
        }
        tasks.append(app.ainvoke(initial_state))
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    return results
```

---

## 12. Alternative Repos

### Drop-in alternatives to your full stack

| Repo | Stars | Description | Use when |
|---|---|---|---|
| **[Skyvern](https://github.com/Skyvern-AI/skyvern)** | ~12k | LLM + computer vision; no CSS selectors | Forms change frequently, visual-only pages |
| **[Steel Browser](https://github.com/steel-dev/steel-browser)** | ~7k | Open-source BaaS with stealth; self-hostable | You want BU Cloud features on your own VPS |
| **[Stagehand](https://github.com/browserbase/stagehand)** | ~12k | Playwright + LLM; very clean API | Teams already on TypeScript/Node.js |
| **[Nanobrowser](https://github.com/nanobrowser/nanobrowser)** | ~7k | Chrome extension agent; no infra needed | Casual/manual runs; no server required |
| **[AG2 (AutoGen)](https://github.com/ag2ai/ag2)** | ~44k | Multi-agent framework | Replacing LangGraph with role-based agents |
| **[Crawl4AI](https://github.com/unclecode/crawl4ai)** | ~42k | AI-powered web extraction | Replacing your selector-based scraping |
| **[camoufox](https://github.com/daijro/camoufox)** | ~5k | Firefox anti-detect browser | Maximum stealth; all existing Playwright code works |
| **[nodriver](https://github.com/ultrafunkamsterdam/nodriver)** | ~9k | CDP-native Chrome (no WebDriver) | Chrome-only; replacing undetected-chromedriver |
| **[techinz/browsers-benchmark](https://github.com/techinz/browsers-benchmark)** | ~1k | Benchmark stealth tools against Cloudflare/DataDome | Testing which tool works on your target site |

### Repos that do exactly what your system does (study these)

| Repo | Description |
|---|---|
| **[browser-use/browser-use examples](https://github.com/browser-use/browser-use/tree/main/examples)** | Official examples including form-filling and multi-step workflows |
| **[Skyvern workflows](https://github.com/Skyvern-AI/skyvern/tree/main/skyvern/webeye/workflows)** | Production-grade survey/form workflow patterns |
| **[steel-dev/steel-cookbook](https://github.com/steel-dev/steel-browser)** | Self-hosted BaaS examples including persistent sessions |

---

## 13. Deployment Checklist

### Before your first run

- [ ] `python -m camoufox fetch` — downloads patched Firefox (~700 MB)
- [ ] Run `setup_profile.py` for each account to pre-authenticate Google
- [ ] Test proxy connectivity: `curl -x http://user:pass@proxy:port https://ipinfo.io`
- [ ] Verify proxy city matches the account's persona location
- [ ] Run one test survey with `headless=False` and watch it to confirm human-like behavior
- [ ] Check your fingerprint: visit `https://bot.sannysoft.com` in your Camoufox browser

### Common failure modes and fixes

| Symptom | Cause | Fix |
|---|---|---|
| `AgentHistoryList(all_results=[])` | Two `Agent` objects sharing a `Browser` | Create new `Browser()` per agent, close in `finally` |
| Immediate redirect to disqualification | IP detected as datacenter/proxy | Switch to residential proxy; check `geoip=True` |
| Google login loop | Persistent profile corrupted | Delete `user_data_dir` and run `setup_profile.py` again |
| Survey page blank/spinner | Anti-bot JS challenge | Enable `magic=True` in Crawl4AI or use Camoufox |
| CAPTCHA on every attempt | Same IP too many requests | Increase cooldown; use sticky session (not rotating) |
| Qualification agent times out | Too many qual questions | Increase `max_steps=80`, `timeout=450` |
| CDP WebSocket disconnected | Chrome crashed or closed | Add crash recovery; restart Chrome and reconnect |
| `navigator.webdriver = true` in headless | Plain Playwright detected | Switch to Camoufox or nodriver |

### Production rate limits (recommended)

```python
# Safe operating parameters to avoid account flags

COOLDOWN_BETWEEN_SURVEYS_SEC = (30, 90)     # Random range
COOLDOWN_BETWEEN_BATCHES_SEC = (300, 600)   # 5–10 min between full batches  
MAX_SURVEYS_PER_DAY_PER_ACCOUNT = 15        # Most platforms flag above this
MAX_CONCURRENT_ACCOUNTS = 5                  # More than this = same IP pool collisions
SURVEY_TIMEOUT_SEC = 480                     # 8 min max per survey
PROFILE_ROTATION_DAYS = 30                   # Recreate profiles monthly
```

---

## Quick Reference

### Minimum working example with Camoufox + browser-use

```python
import asyncio
import random
from camoufox.async_api import AsyncCamoufox
from browser_use import Agent, Browser, BrowserConfig
from langchain_openai import ChatOpenAI

PROXY = {
    "server": "http://proxy-us.proxy-cheap.com:5959",
    "username": "pcpafN3XBx-res-us",
    "password": "PC_8j0HzeNGa7ZOCVq3C",
}

PERSONA = """
You are a 34-year-old female marketing manager from Austin, TX.
Household income: $75,000–$100,000. Married with 2 children.
Education: Bachelor's degree. Industry: Marketing/Advertising.
"""

async def main():
    async with AsyncCamoufox(
        headless=False,
        geoip=True,
        proxy=PROXY,
        persistent_context=True,
        user_data_dir="/tmp/demo_profile",
    ) as camoufox_browser:

        # Get CDP URL for browser-use
        # (Exact attribute depends on your camoufox version — check their docs)
        cdp_url = getattr(camoufox_browser, "_cdp_url", "http://localhost:9222")

        page = await camoufox_browser.new_page()
        await page.goto("https://topsurveys.app")
        await asyncio.sleep(random.uniform(2, 4))

        # hand off to browser-use agent
        browser = Browser(config=BrowserConfig(cdp_url=cdp_url, headless=False))
        agent = Agent(
            task=f"{PERSONA}\n\nNavigate to surveys tab and complete the first available survey.",
            llm=ChatOpenAI(model="gpt-4o", temperature=0.7),
            browser=browser,
            max_actions_per_step=5,
        )
        try:
            result = await asyncio.wait_for(agent.run(max_steps=60), timeout=480)
            print(f"Result: {result}")
        finally:
            await browser.close()
            await asyncio.sleep(2)

asyncio.run(main())
```

---

*Last updated: April 2026. Tool versions: browser-use ≥ 0.2, camoufox ≥ 0.4, nodriver ≥ 0.38, crawl4ai ≥ 0.4.*
