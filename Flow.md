# 🐍 Python Survey Automation Codebase — Complete Beginner's Guide

> A step-by-step walkthrough of every file, function, library, and concept used in this AI-powered survey answering system.

---

## 📁 File Map (What Each File Does)

| File | Purpose |
|------|---------|
| `constants.py` | Stores fixed values (status codes, API URLs, defaults) used everywhere |
| `db_utils.py` | All database read/write operations (PostgreSQL) |
| `agent_utils.py` | Browser automation, CAPTCHA solving, AI agent logic |
| `cookie_utils.py` | Saves/loads browser cookies from the database |
| `proxy_utils.py` | Manages proxy server configs (BrightData residential proxies) |
| `persona_utils.py` | Builds the AI's "personality" prompt from account data |
| `screenshot_utils.py` | Takes screenshots of the browser during runs |
| `ui_components.py` | Reusable Streamlit UI widgets (tables, logs, results) |
| `page.py` (main) | The main Streamlit page — ties everything together |

---

## 📚 Libraries Used — What They Are & Why

### Standard Library (built into Python, no install needed)

| Library | `import` | What it does in this project |
|---------|----------|------------------------------|
| `asyncio` | `import asyncio` | Runs async/await code — needed because browser automation is non-blocking |
| `os` | `import os` | Reads environment variables (API keys), checks file paths |
| `json` | `import json` | Converts Python dicts ↔ JSON strings (for cookies, survey data) |
| `logging` | `import logging` | Writes debug/error messages to the console/log file |
| `re` | `import re` | Regular expressions — pattern matching in strings |
| `io` | `import io` | Creates in-memory file objects (for CSV export) |
| `csv` | `import csv` | Writes CSV files for exporting results |
| `tempfile` | `import tempfile` | Creates temporary files on disk (for screenshots) |
| `traceback` | `import traceback` | Prints full error stack traces for debugging |
| `datetime` | `from datetime import datetime` | Gets current timestamps for logging/batch IDs |
| `typing` | `from typing import Dict, Optional, List, Any` | Type hints — documents what types functions expect/return |

### Third-Party Libraries (must be `pip install`-ed)

| Library | `import` | What it does in this project |
|---------|----------|------------------------------|
| `streamlit` | `import streamlit as st` | Builds the web UI (buttons, inputs, dropdowns) |
| `psycopg2` | `from psycopg2.extras import RealDictCursor` | Connects to PostgreSQL database |
| `pydantic` | `from pydantic import BaseModel, Field` | Validates data shapes (e.g., `SurveyCard`) |
| `aiohttp` | `import aiohttp` | Makes async HTTP requests (to Capsolver API) |
| `browser_use` | `from browser_use import Agent, Browser, BrowserConfig` | AI-controlled browser automation |
| `crawl4ai` | `from crawl4ai import AsyncWebCrawler, LLMExtractionStrategy` | Crawls web pages and extracts structured data using LLMs |
| `langchain_openai` | `from langchain_openai import ChatOpenAI` | Connects to OpenAI (GPT-4o) |
| `langchain_anthropic` | `from langchain_anthropic import ChatAnthropic` | Connects to Anthropic (Claude) |
| `langchain_google_genai` | `from langchain_google_genai import ChatGoogleGenerativeAI` | Connects to Google (Gemini) |

---

## 🔑 `constants.py` — The Config Hub

This file holds **no logic** — just values that other files import. Think of it as a shared dictionary.

```python
# Status constants — strings used to label survey outcomes
STATUS_COMPLETE = "complete"   # Survey finished successfully
STATUS_PASSED   = "passed"     # Passed screening, not yet complete
STATUS_FAILED   = "failed"     # Disqualified / screened out
STATUS_PENDING  = "pending"    # Not started yet
STATUS_ERROR    = "error"      # Something went wrong
```

**Why use constants instead of raw strings?**
If you type `"complete"` in 10 files and later need to change it to `"done"`, you'd have to find and edit all 10 files. With a constant `STATUS_COMPLETE`, you change it once.

```python
# MODEL_REGISTRY — maps human-readable names to LangChain class configs
MODEL_REGISTRY = {
    "openai — GPT-4o": {
        "cls": "ChatOpenAI",              # Which class to instantiate
        "kwargs": {"model": "gpt-4o", "temperature": 0.7},  # Constructor arguments
    },
    ...
}

# MODEL_ENV_KEYS — maps model names to their environment variable for the API key
MODEL_ENV_KEYS = {
    "openai — GPT-4o": "OPENAI_API_KEY",
    ...
}
```

**`os.environ.get("CAPSOLVER_API_KEY", "")`**
- `os.environ` is a dictionary of all environment variables set in your system/shell
- `.get("KEY", "")` returns the value if it exists, or `""` (empty string) if not
- This lets you set secret keys outside your code (safer than hardcoding them)

```python
DEFAULT_PROXY = {
    "proxy_type": "http",
    "host": "brd.superproxy.io",
    "port": 33335,
    "username": "brd-customer-...",
    "password": "...",
    "country": "US"
}
```

A **proxy** is a middleman server — your requests go through it so websites see the proxy's IP, not yours.

---

## 🗄️ `db_utils.py` — Database Operations

### How PostgreSQL Connection Works

```python
from src.core.database.postgres.connection import get_postgres_connection

def get_postgres():
    return get_postgres_connection()
```

`get_postgres_connection()` is a custom function (in another file) that returns a **connection object** to the PostgreSQL database. This is used as a **context manager** (`with get_postgres() as conn`) which automatically closes the connection when done.

---

### `ensure_tables()` — Create Tables if Missing

**File:** `db_utils.py`  
**Purpose:** Checks if required database tables exist; creates them if not (like a setup script).

```python
def ensure_tables():
    try:
        with get_postgres() as conn:          # Open DB connection
            with conn.cursor() as c:          # Open a cursor (like a query runner)
                # Check if 'proxy_configs' table exists
                c.execute("SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = 'proxy_configs')")
                if not c.fetchone()[0]:       # fetchone() returns one row; [0] = first column
                    c.execute("""
                        CREATE TABLE proxy_configs (...)
                    """)
                conn.commit()                 # Save changes permanently
    except Exception as e:
        logger.error(f"ensure_tables: {e}")   # Log the error but don't crash
```

**Flow:**
1. Open connection → open cursor → run SQL → fetch result → create table if needed → commit → close

**Key concepts:**
- `with` statement = context manager (auto-cleanup)
- `c.execute(sql)` = run a SQL query
- `c.fetchone()` = get one result row as a tuple
- `conn.commit()` = save all changes (without this, changes are lost)
- `try/except` = catch errors gracefully

---

### `load_accounts()` — Read All Accounts

**File:** `db_utils.py`  
**Returns:** `List[Dict]` — a list of dictionaries, each representing one account row

```python
def load_accounts() -> List[Dict]:
    try:
        with get_postgres() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:  # Returns dicts, not tuples
                c.execute("""
                    SELECT account_id, username, country, ...
                    FROM accounts ORDER BY username
                """)
                return [dict(r) for r in c.fetchall()]  # Convert each row to a plain dict
    except Exception as e:
        logger.error(f"load_accounts: {e}")
        return []  # Return empty list on error
```

**`RealDictCursor`** — normally `fetchall()` returns tuples like `(1, "john", "US")`. With `RealDictCursor`, it returns dict-like objects: `{"account_id": 1, "username": "john", "country": "US"}`. Much easier to work with.

**`[dict(r) for r in c.fetchall()]`** — this is a **list comprehension**: for every row `r` returned by `fetchall()`, convert it to a plain Python dict and collect them into a list.

---

### `record_survey_attempt()` — Write a Survey Result

**File:** `db_utils.py`  
**Parameters:**
- `account_id: int` — which account took the survey
- `site_id: int` — which survey site
- `survey_name: str` — label for this survey
- `batch_id: str` — which batch run this belongs to
- `status: str` — one of the STATUS_* constants
- `notes: str` — optional extra info

```python
def record_survey_attempt(account_id, site_id, survey_name, batch_id, status, notes=""):
    # Normalize incoming status strings to our constants
    status_map = {"completed": STATUS_COMPLETE, "disqualified": STATUS_FAILED, "incomplete": STATUS_ERROR}
    status = status_map.get(status, status)  # If "completed" → "complete"; else keep as-is
    
    # Validate the status is one we know about
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
```

**`%s` placeholders** — never put variables directly in SQL strings (SQL injection risk). Instead, pass them as a tuple (second argument to `execute()`). psycopg2 safely escapes them.

**`(notes or "")[:1000]`** — if `notes` is `None`, use `""`. Then slice to max 1000 characters.

---

## 🤖 `agent_utils.py` — The Brain of the System

### `SurveyCard` — Pydantic Data Model

**File:** `agent_utils.py`

```python
from pydantic import BaseModel, Field

class SurveyCard(BaseModel):
    title: str = Field(description="The survey title or name")
    reward: str = Field(description="Reward amount (e.g., '$1.50')")
    link_url: str = Field(description="The URL or clickable element to start the survey")
    unique_id: Optional[str] = Field(default=None, description="Any unique identifier")
```

**Pydantic `BaseModel`** — like a Python dataclass but with automatic validation. If you try to create `SurveyCard(title=123)`, Pydantic coerces `123` to `"123"` or raises an error. Makes data reliable.

**`Optional[str]`** — means the field can be a `str` OR `None`.

---

### `create_undetected_browser()` — Launch Stealth Browser

**File:** `agent_utils.py`  
**Parameters:**
- `user_data_dir: str` — path to Chrome profile folder (saves cookies/sessions)
- `headless: bool` — `True` = no visible window; `False` = visible browser
- `proxy: Optional[Dict]` — proxy config dict or `None`
- `log_func` — a callable (function) to log messages
- `batch_id: str` — identifier for this run's logs

**Returns:** `Browser` (browser_use object)

```python
async def create_undetected_browser(user_data_dir, headless=False, proxy=None, log_func=None, batch_id=""):
    extra_args = [
        "--disable-blink-features=AutomationControlled",  # Hide that it's automated
        "--no-sandbox",                                    # Required in some environments
        ...
    ]

    proxy_string = None
    if proxy and proxy.get("host") and proxy.get("port"):
        proxy_type = proxy.get("proxy_type", "http")
        host = proxy["host"]
        port = proxy["port"]
        username = proxy.get("username")
        password = proxy.get("password")
        country = proxy.get("country", "US")

        # Special BrightData formatting
        if "brd.superproxy.io" in host and username:
            from .proxy_utils import format_brightdata_username
            username = format_brightdata_username(username, country)

        if username and password:
            proxy_string = f"{proxy_type}://{username}:{password}@{host}:{port}"
        else:
            proxy_string = f"{proxy_type}://{host}:{port}"

    browser_config = BrowserConfig(
        headless=headless,
        user_data_dir=user_data_dir,
        extra_chromium_args=extra_args,
        proxy=proxy_string,
    )
    browser = Browser(config=browser_config)
    await browser.start()  # async — waits for browser to actually launch

    # Patch navigator.webdriver to undefined (hides automation detection)
    context = browser.context
    pages = await context.get_pages()
    if pages:
        for page in pages:
            await page.evaluate("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return browser
```

**`async def`** — this is a **coroutine** function. It must be called with `await`. It can pause (at `await` points) to let other code run, making it non-blocking.

**f-strings** — `f"{proxy_type}://{username}:{password}@{host}:{port}"` — curly braces inject variable values into strings.

**`page.evaluate(js_string)`** — runs JavaScript directly in the browser page. Used here to remove the `webdriver` property that websites check to detect bots.

---

### `extract_surveys_with_crawl4ai()` — AI-Powered Survey Discovery

**File:** `agent_utils.py`  
**Parameters:**
- `page_url: str` — the URL to crawl
- `log_func` — logging function

**Returns:** `List[SurveyCard]`

```python
async def extract_surveys_with_crawl4ai(page_url: str, log_func=None) -> List[SurveyCard]:
    extraction_strategy = LLMExtractionStrategy(
        provider="openai/gpt-4o",
        schema=SurveyCard.model_json_schema(),    # Tells the LLM what shape to return
        extraction_type="schema",
        instruction="Extract all available survey cards..."
    )
    async with AsyncWebCrawler(verbose=True) as crawler:
        result = await crawler.arun(
            url=page_url,
            extraction_strategy=extraction_strategy,
            wait_for="css:.list-item, .survey-card",   # Wait for these CSS elements
        )
        if result.success and result.extracted_content:
            try:
                data = json.loads(result.extracted_content)         # Parse JSON string → list
                surveys = [SurveyCard(**item) for item in data]     # Build SurveyCard objects
                return surveys
            except Exception as e:
                ...
                return []
```

**`SurveyCard(**item)`** — `**item` unpacks a dict as keyword arguments. If `item = {"title": "Survey 1", "reward": "$2", "link_url": "..."}`, this is the same as `SurveyCard(title="Survey 1", reward="$2", link_url="...")`.

**`async with`** — like `with`, but for async context managers (the crawler needs to be started/closed asynchronously).

---

### `solve_captcha_if_present()` — CAPTCHA Detection & Solving

**File:** `agent_utils.py`  
**Parameters:**
- `page` — Playwright page object
- `log_func` — logging function
- `batch_id: str` — for log grouping

**Returns:** `bool` — `True` if a CAPTCHA was solved

```python
async def solve_captcha_if_present(page, log_func=None, batch_id=""):
    if not CAPSOLVER_API_KEY:       # Skip if no API key is set
        return False

    captcha_selectors = [
        'iframe[src*="recaptcha"]',   # CSS selectors for CAPTCHA iframes
        'div.g-recaptcha',
        'div[data-sitekey]',
        ...
    ]

    solved = False
    for selector in captcha_selectors:
        try:
            element = await page.query_selector(selector)  # Find element on page
            if element:
                # Extract the 'data-sitekey' attribute
                sitekey = await element.get_attribute("data-sitekey")
                
                if sitekey:
                    token = await call_capsolver(sitekey, page.url, log_func)
                    if token:
                        # Inject the solved token back into the page via JavaScript
                        await page.evaluate(f"""
                            function() {{
                                var callback = document.getElementById('g-recaptcha-response');
                                if (callback) {{
                                    callback.value = '{token}';
                                    callback.dispatchEvent(new Event('change'));
                                }}
                            }}
                        """)
                        solved = True
        except Exception as e:
            ...
    return solved
```

**CAPTCHA flow:**
1. Scan page for CAPTCHA widget HTML elements
2. Extract the `sitekey` (unique identifier for that CAPTCHA instance)
3. Send `sitekey` + page URL to Capsolver's API
4. Capsolver's servers solve it and return a token
5. Inject the token into the page's hidden input field
6. The website's JavaScript picks up the token and accepts it

---

### `call_capsolver()` — HTTP API Call to Solve CAPTCHA

**File:** `agent_utils.py`  
**Parameters:**
- `sitekey: str` — the CAPTCHA's site key
- `page_url: str` — the page where the CAPTCHA appears
- `log_func` — logging function

**Returns:** `Optional[str]` — the solved token, or `None` on failure

```python
async def call_capsolver(sitekey, page_url, log_func=None) -> Optional[str]:
    payload = {
        "clientKey": CAPSOLVER_API_KEY,
        "task": {
            "type": "ReCaptchaV2TaskProxyless",
            "websiteURL": page_url,
            "websiteKey": sitekey,
        }
    }
    async with aiohttp.ClientSession() as session:
        # Step 1: Create the task
        async with session.post(f"{CAPSOLVER_API_URL}/createTask", json=payload) as resp:
            result = await resp.json()
            task_id = result.get("taskId")

        # Step 2: Poll until solved (up to 30 times, 2 seconds apart = 60 seconds max)
        for _ in range(30):
            await asyncio.sleep(2)    # Wait 2 seconds between polls
            async with session.post(f"{CAPSOLVER_API_URL}/getTaskResult",
                                    json={"clientKey": CAPSOLVER_API_KEY, "taskId": task_id}) as poll_resp:
                poll_result = await poll_resp.json()
                if poll_result.get("status") == "ready":
                    token = poll_result.get("solution", {}).get("gRecaptchaResponse")
                    return token
```

**`aiohttp.ClientSession()`** — async HTTP client. Unlike `requests` (which blocks), `aiohttp` is non-blocking so it works in async code.

**`for _ in range(30)`** — `_` is a convention for "I don't need this loop variable". Loop 30 times.

**`await asyncio.sleep(2)`** — pause for 2 seconds without blocking the event loop (other tasks can run during this pause).

---

### `run_survey_agent()` — AI Agent Completes a Survey

**File:** `agent_utils.py`  
**Parameters:**
- `browser: Browser` — the already-open browser instance
- `llm` — the LangChain LLM object (GPT-4o, Claude, or Gemini)
- `persona: str` — the system prompt describing the person
- `start_url: str` — where to begin
- `log_func` — logging function

**Returns:** `str` — one of the STATUS_* constants

```python
async def run_survey_agent(browser, llm, persona, start_url, log_func=None) -> str:
    agent_task = f"""
{persona}
...
STEPS:
1. Navigate to {start_url}
2. Find the first available survey card and click it.
...
FINAL REPORT: Return exactly "COMPLETE" or "DISQUALIFIED"
"""
    agent = Agent(
        task=agent_task,
        llm=llm,
        browser=browser,
        max_actions_per_step=5,   # Max browser actions per AI reasoning step
    )
    # Run with a 600-second (10-minute) timeout
    result = await asyncio.wait_for(agent.run(max_steps=80), timeout=600.0)
    return detect_survey_outcome(result, log_func)
```

**`asyncio.wait_for(coro, timeout=600.0)`** — runs the coroutine but raises `asyncio.TimeoutError` if it takes more than 600 seconds.

**`f-string` with `{persona}` and `{start_url}`** — the persona and URL are injected into the task prompt string at runtime.

---

### `detect_survey_outcome()` — Parse Agent Result

**File:** `agent_utils.py`  
**Parameters:**
- `result` — whatever the agent returned (could be a complex object)
- `log_func` — logging function

**Returns:** `str` — STATUS_COMPLETE, STATUS_FAILED, or STATUS_ERROR

```python
def detect_survey_outcome(result, log_func=None) -> str:
    try:
        combined = str(result).lower()   # Convert everything to lowercase string

        # Phrases that indicate disqualification
        agent_brain_dq_phrases = [
            "disqualified - i was disqualified",
            "screen out",
            ...
        ]
        # Phrases that indicate success
        agent_brain_complete_phrases = [
            "success - i successfully completed",
            "survey is complete",
            ...
        ]
        
        # Also check the last 5 history entries
        if hasattr(result, "history") and result.history:
            history_str = " ".join(str(h) for h in result.history[-5:]).lower()
            combined += " " + history_str

        for phrase in agent_brain_dq_phrases:
            if phrase in combined:
                return STATUS_FAILED

        for phrase in agent_brain_complete_phrases:
            if phrase in combined:
                return STATUS_COMPLETE

        # Fall back to keyword lists from constants
        if any(kw in combined for kw in DISQUALIFIED_KEYWORDS):
            return STATUS_FAILED
        if any(kw in combined for kw in COMPLETE_KEYWORDS):
            return STATUS_COMPLETE

        return STATUS_ERROR    # If nothing matched, treat as error
    except Exception:
        return STATUS_ERROR
```

**`hasattr(result, "history")`** — checks if `result` has an attribute named `"history"` before accessing it (prevents `AttributeError`).

**`any(kw in combined for kw in DISQUALIFIED_KEYWORDS)`** — generator expression inside `any()`. Returns `True` if ANY keyword in the list is found in `combined`.

---

### `get_llm()` — Create the Right AI Model

**File:** `agent_utils.py`  
**Parameters:**
- `model_choice: str` — key from `MODEL_REGISTRY` (e.g., `"openai — GPT-4o"`)

**Returns:** LangChain LLM object

```python
def get_llm(model_choice: str):
    cfg = MODEL_REGISTRY[model_choice]          # Get config dict for this model
    api_key = os.getenv(MODEL_ENV_KEYS[model_choice], "")  # Get API key from environment

    if not api_key:
        raise Exception(f"Missing API key for {model_choice}")

    cls_name = cfg["cls"]    # e.g., "ChatOpenAI"

    if cls_name == "ChatOpenAI":
        return ChatOpenAI(**{**cfg["kwargs"], "api_key": api_key})
    elif cls_name == "ChatAnthropic":
        return ChatAnthropic(**{**cfg["kwargs"], "api_key": api_key})
    elif cls_name == "ChatGoogleGenerativeAI":
        return ChatGoogleGenerativeAI(**{**cfg["kwargs"], "api_key": api_key})
    else:
        raise ValueError(f"Unknown LLM class: {cls_name}")
```

**`{**cfg["kwargs"], "api_key": api_key}`** — dictionary unpacking (spread). Creates a new dict combining `cfg["kwargs"]` (e.g., `{"model": "gpt-4o", "temperature": 0.7}`) with `{"api_key": "sk-..."}`.

**`raise Exception(...)`** — deliberately throws an error to stop execution when something critical is missing.

---

## 🍪 `cookie_utils.py` — Session Persistence

Cookies store login state. By saving and reloading them, the browser stays logged into Google without re-entering credentials every run.

### `save_cookies_to_db()` — Persist Cookies

**File:** `cookie_utils.py`  
**Parameters:**
- `account_id: int`
- `cookies: List[Dict]` — list of cookie objects from the browser
- `domain: str` — e.g., `"google.com"`

```python
def save_cookies_to_db(account_id, cookies, domain="google.com") -> bool:
    # Filter: only keep cookies relevant to this domain
    relevant = [c for c in cookies if domain.lstrip(".") in c.get("domain", "").lstrip(".")]
    if not relevant:
        relevant = cookies    # If none match, save all (fallback)

    try:
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
```

**`json.dumps(relevant)`** — converts a Python list of dicts to a JSON string for storage in the database TEXT column.

**`ON CONFLICT ... DO UPDATE`** — PostgreSQL "upsert": if a row with this `account_id + domain` already exists, update it instead of failing with a duplicate key error.

**`domain.lstrip(".")`** — removes leading dot: `".google.com"` → `"google.com"` for consistent comparison.

---

## 🧭 `proxy_utils.py` — Proxy Management

### `format_brightdata_username()` — Country Targeting

**File:** `proxy_utils.py`  
**Parameters:**
- `raw_username: str` — base BrightData username
- `country: str` — 2-letter country code (e.g., `"US"`, `"GB"`)

**Returns:** `str` — modified username with country suffix

```python
def format_brightdata_username(raw_username: str, country: str) -> str:
    if not raw_username:
        return raw_username

    # Remove any existing -country-XX suffix using regex
    base = re.sub(r'-country-[A-Z]{2}$', '', raw_username)
    # Add the new country suffix
    return f"{base}-country-{country.upper()}"
```

**`re.sub(pattern, replacement, string)`** — finds all matches of `pattern` in `string` and replaces them with `replacement`.

**`r'-country-[A-Z]{2}$'`** — regex pattern:
- `r'...'` — raw string (backslashes not treated as escape sequences)
- `-country-` — literal text
- `[A-Z]{2}` — exactly 2 uppercase letters
- `$` — end of string

**`country.upper()`** — converts `"us"` to `"US"` (ensures uppercase).

---

## 🎭 `persona_utils.py` — Building AI Persona

### `build_persona_system_message()` — Construct the AI's Identity

**File:** `persona_utils.py`  
**Parameters:**
- `prompt: Dict` — prompt record from the database (has `"content"` key)
- `acct: Dict` — account record (has demographic fields)

**Returns:** `str` — the system message to give the AI agent

```python
def build_persona_system_message(prompt: Dict, acct: Dict) -> str:
    lines = ["You are a specific person answering survey questions. Embody this identity:", ""]

    # Add demographic fields if they exist in the account record
    for field, label in [
        ("age", "Age"),
        ("gender", "Gender"),
        ("city", "City/Location"),
        ("education_level", "Education"),
        ("job_status", "Employment"),
        ("income_range", "Income"),
        ("marital_status", "Marital status"),
        ("household_size", "Household size"),
        ("industry", "Industry"),
    ]:
        if acct.get(field):      # Only add if the field has a value
            lines.append(f"• {label}: {acct[field]}")

    # Boolean field needs special handling
    if acct.get("has_children") is not None:
        lines.append(f"• Has children: {'Yes' if acct['has_children'] else 'No'}")

    # Add the custom prompt content
    if prompt and prompt.get("content"):
        lines += ["", "Additional persona details:", prompt["content"].strip()]

    lines += ["", "Rules:", "- Stay consistent throughout the survey", ...]

    return "\n".join(lines)    # Join all lines with newline characters
```

**`acct.get(field)`** — safer than `acct[field]`: returns `None` instead of raising `KeyError` if the key doesn't exist.

**`'Yes' if acct['has_children'] else 'No'`** — inline ternary operator: `value_if_true if condition else value_if_false`.

**`"\n".join(lines)`** — joins a list of strings with newline between each: `["a", "b", "c"]` → `"a\nb\nc"`.

---

## 📸 `screenshot_utils.py` — Capturing Screenshots

### `take_screenshot()` — Save Browser Screenshot

**File:** `screenshot_utils.py`  
**Parameters:**
- `page` — Playwright page object
- `label: str` — must be in `ALLOWED_SCREENSHOTS` (whitelist check)
- `batch_id: str` — for log grouping
- `survey_num: int` — which survey number (for filename)
- `log_func` — logging function

**Returns:** `Optional[str]` — file path of saved screenshot, or `None`

```python
async def take_screenshot(page, label, batch_id, survey_num=0, log_func=None) -> Optional[str]:
    if label not in ALLOWED_SCREENSHOTS:    # Security: only allow whitelisted labels
        return None
    try:
        img_bytes = await page.screenshot(type="png", full_page=False)  # Capture visible area

        # Create a temporary file and write image bytes to it
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            tmp.write(img_bytes)
            tmp_path = tmp.name    # Get the path of the temp file

        if log_func:
            log_func(f"📸 Screenshot saved to {tmp_path}", batch_id=batch_id)
        return tmp_path
    except Exception as e:
        if log_func:
            log_func(f"Screenshot failed ({label}): {e}", "WARNING", batch_id=batch_id)
        return None
```

**`tempfile.NamedTemporaryFile(delete=False, suffix=".png")`** — creates a temp file that won't be deleted when closed (`delete=False`). The file stays on disk until explicitly deleted.

**`tmp.name`** — the OS-assigned path (e.g., `/tmp/tmpXYZ123.png`).

---

## 🖥️ `page.py` — The Main Streamlit Page

This is the orchestrator — it imports from all other files and wires everything together in the UI.

### `run_async()` — Bridge Between Sync and Async

**File:** `page.py`

```python
def run_async(coro):
    loop = asyncio.new_event_loop()       # Create a new event loop
    asyncio.set_event_loop(loop)          # Set it as the current loop
    try:
        return loop.run_until_complete(coro)  # Run the async coroutine synchronously
    finally:
        loop.close()                      # Always clean up, even on error
```

**Why needed?** Streamlit's `render()` method is a regular (synchronous) function. The browser automation code is `async`. `run_async()` bridges this gap by running the async code in a new event loop.

---

### `_click_continue_with_google()` — Find & Click Google Login

**File:** `page.py`  
**Parameters:**
- `page` — Playwright page object
- `log_func` — logging function
- `batch_id: str` — for log grouping

**Returns:** `bool` — `True` if clicked successfully

```python
async def _click_continue_with_google(page, log_func=None, batch_id="") -> bool:
    selectors = [
        "text=Continue with Google",      # Playwright text locator
        "text=Sign in with Google",
        "[aria-label*='Google' i]",       # Case-insensitive aria-label containing "Google"
        "[class*='google' i]",            # Class containing "google"
        "div.nsm7Bb-HzV7m-LgbsSe",       # Google's own GSI button class
        "a[href*='google'][href*='oauth']", # Link with 'google' and 'oauth' in href
        ...
    ]

    for sel in selectors:
        try:
            elem = page.locator(sel).first   # Find first matching element
            if await elem.is_visible(timeout=2000):   # Check if visible (wait max 2s)
                if log_func:
                    log_func(f"✅ Found Google OAuth element via selector: {sel}", batch_id=batch_id)
                await elem.click()
                await page.wait_for_timeout(5000)  # Wait 5s after clicking
                return True
        except Exception:
            continue    # Try next selector if this one fails

    # Last resort: scan all clickable elements' text
    try:
        all_clickable = await page.query_selector_all("a, button, div[role='button'], ...")
        for elem in all_clickable:
            try:
                text = (await elem.inner_text()).strip().lower()
                if "google" in text and len(text) < 60:
                    if await elem.is_visible():
                        await elem.click()
                        return True
            except Exception:
                continue
    except Exception:
        pass

    return False
```

**CSS attribute selectors:**
- `[attr*='value']` — attribute contains value
- `[attr*='value' i]` — same, case-insensitive
- `a[href*='google'][href*='oauth']` — `<a>` tag where href contains both "google" AND "oauth"

---

### `GenerateManualWorkflowsPage` — The Page Class

**File:** `page.py`

```python
class GenerateManualWorkflowsPage:
    def __init__(self, db_manager):           # Called when the class is instantiated
        self.db_manager = db_manager          # Store for use in methods
        self.orchestrator = SurveySiteOrchestrator(db_manager)  # Create sub-objects
        ensure_tables()                       # Set up DB tables on startup

        # Initialize Streamlit session state with defaults
        defaults = {
            "generation_in_progress": False,
            "generation_results": None,
            "generation_logs": [],
            ...
        }
        for k, v in defaults.items():
            if k not in st.session_state:    # Only set if not already set
                st.session_state[k] = v
```

**`st.session_state`** — Streamlit's way to persist data across re-runs. Every time a user clicks a button, Streamlit reruns the entire script. `session_state` lets you keep values between reruns.

**`self`** — in a class method, `self` refers to the current instance of the class. `self.db_manager` stores the value as an instance attribute accessible in all methods.

---

### `_do_direct_answering()` — Main Automation Flow

**File:** `page.py`  
**Parameters:** account, site, prompt, start_url, num_surveys, model_choice, google_email, google_password, proxy_cfg

This is the heart of the system. Here's the full flow:

```
1. Create batch_id (unique run identifier: "ai_20240115_143022")
2. Initialize batch record in session_state
3. Get/create Chrome profile path for the account
4. Launch stealth browser (via create_undetected_browser)
5. Navigate to start_url
6. Attempt to click "Continue with Google" button
7. If clicked → handle OAuth popup/redirect
8. Solve any initial CAPTCHA
9. Extract available surveys (via Crawl4AI)
10. For each survey (1 to num_surveys):
    a. Navigate to survey URL
    b. Solve CAPTCHA if present
    c. Run AI agent (run_survey_agent)
    d. Record outcome to database
    e. Navigate back to dashboard
11. Display summary
12. Store results in session_state
13. Close browser
14. st.rerun() to refresh the UI
```

**Key session_state updates during the run:**
```python
st.session_state.generation_in_progress = True   # Disables the Run button
st.session_state.survey_progress.append(...)      # Updates progress display
st.session_state.generation_results = {...}       # Final summary for display
st.session_state.generation_in_progress = False   # Re-enables the button
```

---

## 🎨 `ui_components.py` — Reusable UI Widgets

### `display_batch_details()` — Show Logs & Screenshots

**File:** `ui_components.py`  
**Parameters:**
- `batch_id: str` — which batch to display
- `batches_state: dict` — the full `st.session_state.batches` dict
- `screenshot_labels: dict` — maps label codes to display names

```python
def display_batch_details(batch_id, batches_state, screenshot_labels=None):
    batch = batches_state.get(batch_id)
    if not batch:
        st.info("No data recorded for this batch yet.")
        return

    # Creates two tabs in the UI
    tab_logs, tab_shots = st.tabs(["📝 Logs", "📸 Screenshots"])

    with tab_logs:
        logs = batch.get("logs", [])
        if logs:
            st.code("\n".join(logs), language="log")  # Monospace code block
            st.download_button("⬇️ Download logs", "\n".join(logs), ...)
```

**`st.tabs([...])`** — creates clickable tab panels. Returns tab objects you use as context managers.

**`with tab_logs:`** — everything inside this block renders in that tab.

---

### `display_screening_results_tab()` — Results Table with Actions

**File:** `ui_components.py`

This function:
1. Loads all survey attempts from the DB for this account/site
2. Shows summary metrics (total, complete, failed, etc.)
3. Shows a progress bar (`success_rate = (complete + passed) / total`)
4. Renders each attempt in an expander with action buttons (Mark Complete, Mark DQ, Add Note)
5. Offers CSV export

```python
# Progress bar
success_n = complete_n + passed_n
if total > 0:
    st.progress(success_n / total, text=f"Success rate: {int(success_n/total*100)}%")
```

**`int(success_n/total*100)`** — division, then multiply by 100, then truncate decimals: `7/10*100 = 70.0 → 70`.

---

## 🔄 Complete Data Flow — End to End

```
User clicks "🚀 Answer Surveys" button
        │
        ▼
run_async(_do_direct_answering(...))
        │
        ├── create_undetected_browser()          [agent_utils.py]
        │       └── BrowserConfig + Browser.start()
        │
        ├── page.goto(start_url)                 [Playwright]
        │
        ├── _click_continue_with_google()        [page.py]
        │       └── Tries 15+ CSS selectors
        │
        ├── solve_captcha_if_present()           [agent_utils.py]
        │       └── call_capsolver() → aiohttp POST to api.capsolver.com
        │
        ├── extract_surveys_with_crawl4ai()      [agent_utils.py]
        │       └── AsyncWebCrawler + LLMExtractionStrategy → List[SurveyCard]
        │
        ├── get_llm(model_choice)                [agent_utils.py]
        │       └── ChatOpenAI / ChatAnthropic / ChatGoogleGenerativeAI
        │
        ├── build_persona_system_message()       [persona_utils.py]
        │       └── str (system prompt)
        │
        └── FOR each survey:
                ├── page.goto(survey_url)
                ├── solve_captcha_if_present()
                ├── run_survey_agent()           [agent_utils.py]
                │       └── Agent.run() → detect_survey_outcome()
                ├── take_screenshot()            [screenshot_utils.py]
                └── record_survey_attempt()      [db_utils.py]
                        └── INSERT INTO screening_results
```

---

## 💡 Key Python Concepts Used — Quick Reference

| Concept | Example in Code | What it Means |
|---------|----------------|---------------|
| `async def` | `async def create_undetected_browser(...)` | Function that can pause with `await` |
| `await` | `await browser.start()` | Pause here until this async operation finishes |
| `async with` | `async with aiohttp.ClientSession() as session:` | Async context manager |
| Context manager | `with get_postgres() as conn:` | Auto-cleanup when block exits |
| List comprehension | `[dict(r) for r in c.fetchall()]` | Build list by transforming each item |
| Generator expression | `any(kw in text for kw in keywords)` | Lazy iteration (stops at first match) |
| Dict unpacking | `{**cfg["kwargs"], "api_key": key}` | Merge dicts |
| Ternary | `'Yes' if condition else 'No'` | Inline if/else |
| f-string | `f"User: {username}"` | String interpolation |
| Type hints | `def func(x: int) -> str:` | Document expected types |
| `Optional[str]` | `def func() -> Optional[str]:` | Returns `str` or `None` |
| `try/except` | `try: ... except Exception as e: ...` | Catch and handle errors |
| `raise` | `raise ValueError("message")` | Throw an error intentionally |
| `hasattr` | `if hasattr(result, "history"):` | Check if object has attribute |
| `%s` in SQL | `c.execute("... WHERE id=%s", (id,))` | Safe parameter substitution |
| Class | `class GenerateManualWorkflowsPage:` | Blueprint for objects |
| `self` | `self.db_manager = db_manager` | Reference to current instance |
| `__init__` | `def __init__(self, db_manager):` | Constructor — called on instantiation |

---

## 🏗️ Architecture Summary

```
┌─────────────────────────────────────────────────────┐
│                   Streamlit UI (page.py)            │
│  GenerateManualWorkflowsPage.render()               │
│  └── _render_answer_direct()                        │
│  └── _do_direct_answering()   ← async main loop     │
└───────────────┬─────────────────────────────────────┘
                │ imports & calls
    ┌───────────┼──────────────┬─────────────────┐
    ▼           ▼              ▼                 ▼
agent_utils  db_utils      cookie_utils     proxy_utils
(browser,    (postgres      (save/load       (proxy
captcha,     read/write)    cookies)         config)
AI agent)
    │           
    ▼           
persona_utils  screenshot_utils  constants  ui_components
(AI prompt)   (screenshots)     (config)   (UI widgets)
```

---

*Generated for learning purposes — every function, parameter, and library explained from first principles.*