"""
Agent utilities with proxy authentication, Capsolver, and stealth browser.
"""

import asyncio
import json
import os
import logging
from typing import Dict, List, Optional, Any

import aiohttp
from browser_use import Agent, Browser, BrowserConfig
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


class SurveyCard(BaseModel):
    title: str = Field(description="The survey title or name")
    reward: str = Field(description="Reward amount (e.g., '$1.50')")
    link_url: str = Field(description="The URL or clickable element to start the survey")
    unique_id: Optional[str] = Field(default=None, description="Any unique identifier for the survey")


async def create_undetected_browser(
    user_data_dir: str,
    headless: bool = False,
    proxy: Optional[Dict] = None,
    log_func=None,
    batch_id: str = ""
) -> Browser:
    """Create stealth browser with BrightData proxy support and country targeting."""
    extra_args = [
        "--disable-blink-features=AutomationControlled",
        "--disable-features=ChromeWhatsNewUI,ChromeForcedMigration",
        "--no-sandbox",
        "--disable-dev-shm-usage",
        "--disable-web-security",
        "--disable-gpu",
        "--disable-software-rasterizer",
        "--disable-background-networking",
        "--disable-default-apps",
        "--disable-sync",
        "--disable-translate",
        "--hide-scrollbars",
        "--metrics-recording-only",
        "--mute-audio",
        "--no-first-run",
        "--safebrowsing-disable-auto-update",
        "--ignore-certificate-errors",
        "--ignore-ssl-errors",
    ]

    proxy_string = None
    if proxy and proxy.get("host") and proxy.get("port"):
        proxy_type = proxy.get("proxy_type", "http")
        host = proxy["host"]
        port = proxy["port"]
        username = proxy.get("username")
        password = proxy.get("password")
        country = proxy.get("country", "US")

        # BrightData special handling: format username with country
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

    browser_config = BrowserConfig(
        headless=headless,
        user_data_dir=user_data_dir,
        extra_chromium_args=extra_args,
        proxy=proxy_string,
    )
    browser = Browser(config=browser_config)
    await browser.start()
    # Remove navigator.webdriver on all pages
    context = browser.context
    pages = await context.get_pages()
    if pages:
        for page in pages:
            await page.evaluate("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return browser


async def extract_surveys_with_crawl4ai(page_url: str, log_func=None) -> List[SurveyCard]:
    """Use Crawl4AI + LLM to extract survey cards."""
    if log_func:
        log_func(f"🔍 Extracting surveys from {page_url} using Crawl4AI...")
    extraction_strategy = LLMExtractionStrategy(
        provider="openai/gpt-4o",
        schema=SurveyCard.model_json_schema(),
        extraction_type="schema",
        instruction="Extract all available survey cards. For each, provide title, reward amount, and the link URL or clickable element identifier. If no URL is present, provide a CSS selector or text that can be used to click the card."
    )
    async with AsyncWebCrawler(verbose=True) as crawler:
        result = await crawler.arun(
            url=page_url,
            extraction_strategy=extraction_strategy,
            wait_for="css:.list-item, .survey-card, .p-ripple-wrapper",
            verbose=True
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


async def solve_captcha_if_present(page, log_func=None, batch_id: str = ""):
    """
    Detect common CAPTCHA widgets and solve them using Capsolver.
    Supports reCAPTCHA v2, v3, hCaptcha, and Cloudflare Turnstile.
    """
    if not CAPSOLVER_API_KEY:
        if log_func:
            log_func("⚠️ CAPTCHA solving skipped: No Capsolver API key", batch_id=batch_id)
        return False

    # Common selectors for CAPTCHA iframes
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
                # Extract sitekey and page URL
                sitekey = None
                if selector.startswith('iframe'):
                    parent = await element.evaluate_handle("el => el.closest('[data-sitekey]')")
                    if parent:
                        sitekey = await parent.get_attribute("data-sitekey")
                else:
                    sitekey = await element.get_attribute("data-sitekey")

                if not sitekey:
                    # Try to find sitekey in script tags
                    scripts = await page.query_selector_all('script[src*="recaptcha"], script[src*="hcaptcha"]')
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
                        log_func(f"🔑 Sitekey: {sitekey}, page URL: {page_url}", batch_id=batch_id)
                    token = await call_capsolver(sitekey, page_url, log_func)
                    if token:
                        # Inject token into page
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
        }
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
                async with session.post(f"{CAPSOLVER_API_URL}/getTaskResult", json={"clientKey": CAPSOLVER_API_KEY, "taskId": task_id}) as poll_resp:
                    poll_result = await poll_resp.json()
                    if poll_result.get("status") == "ready":
                        token = poll_result.get("solution", {}).get("gRecaptchaResponse") or poll_result.get("solution", {}).get("gRecaptchaResponse")
                        return token
                    elif poll_result.get("errorId") != 0:
                        if log_func:
                            log_func(f"Capsolver polling error: {poll_result.get('errorDescription')}", "ERROR")
                        return None
            if log_func:
                log_func("Capsolver timeout")
            return None
        except Exception as e:
            if log_func:
                log_func(f"Capsolver exception: {e}", "ERROR")
            return None


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
            "disqualified - i was disqualified", "disqualified - the survey",
            "evaluation_previous_goal=\"disqualified", "evaluation_previous_goal='disqualified",
            "i was disqualified from", "disqualified from the previous survey",
            "disqualified from this survey", "screen out", "screened out",
        ]
        agent_brain_complete_phrases = [
            "evaluation_previous_goal=\"success", "evaluation_previous_goal='success",
            "success - i successfully completed", "survey is complete",
            "survey has been completed", "successfully submitted",
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


async def perform_google_login(page, email: str, password: str, log_func=None):
    """Fallback Google login."""
    if log_func:
        log_func("→ Navigating to Google sign-in")
    await page.goto("https://accounts.google.com/signin/v2/identifier", wait_until="domcontentloaded", timeout=30_000)
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
        for ct in ["Verify it's you", "Confirm it's you", "This extra step", "Get a verification code", "Check your phone", "Try another way"]:
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