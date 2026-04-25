# 🐍 Accounts, Chrome Sessions & Cookie Manager — Complete Beginner's Guide

> A step-by-step walkthrough of `accounts_page.py`, `chrome_session_manager.py`, and `cookie_manager.py` — covering every function, parameter, data flow, and storage location.

---

## 📁 File Map — What Each File Does

| File | Purpose | Think of it as... |
|------|---------|-------------------|
| `accounts_page.py` | Main Streamlit UI page — orchestrates everything | The "manager" |
| `chrome_session_manager.py` | Launches/stops Chrome browser sessions, extracts cookies on stop | The "Chrome handler" |
| `cookie_manager.py` | Reads/writes cookies to the database + renders cookie UI | The "cookie warehouse" |

---

## 🗄️ Where Data Gets Saved — Storage Map

Before diving into code, here's a complete map of **where everything is stored**:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         STORAGE LOCATIONS                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  PostgreSQL Database                                                        │
│  ├── accounts              ← account records (username, country, demographics)
│  ├── account_cookies       ← serialised cookie arrays (one row per domain)  │
│  ├── survey_sites          ← survey site records                            │
│  ├── account_urls          ← per-account per-site URLs                      │
│  └── proxy_configs         ← proxy settings per account                    │
│                                                                             │
│  MongoDB (optional)                                                         │
│  ├── messages_db.accounts  ← Chrome profile metadata                       │
│  └── messages_db.browser_sessions ← active/past session records            │
│                                                                             │
│  Filesystem (disk)                                                          │
│  ├── /workspace/chrome_profiles/account_{username}/  ← Chrome profile dir  │
│  │   ├── Default/Cookies  ← Chrome's SQLite cookie database                │
│  │   ├── Default/         ← browsing history, preferences, cache           │
│  │   └── session_data.json ← synced cookies in JSON format                 │
│  └── /app/cookie_scripts/copy_cookies_{username}.sh ← clipboard copy script│
│                                                                             │
│  st.session_state (in-memory, per browser tab)                             │
│  ├── local_chrome_sessions ← dict of active Chrome sessions                │
│  ├── account_creation_logs ← log entries for the creation wizard           │
│  └── show_add_account, creating_account, etc. ← UI state flags             │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📚 Libraries Used

### Standard Library

| Library | Import | What it does here |
|---------|--------|-------------------|
| `subprocess` | `import subprocess` | Launches external processes (Chrome browser) |
| `os` | `import os` | File paths, environment variables, process management |
| `time` | `import time` | `time.sleep()` — pause execution; `time.time()` — Unix timestamp |
| `signal` | `import signal` | Send OS signals to processes (`SIGTERM`, `SIGKILL`) |
| `sqlite3` | `import sqlite3` | Read Chrome's built-in cookie database (it's a SQLite file) |
| `json` | `import json` | Serialize/deserialize cookie data to/from strings |
| `shutil` | `import shutil` | Copy files (used to copy Chrome's Cookies SQLite DB safely) |
| `logging` | `import logging` | Write info/warning/error messages to logs |
| `tempfile` | `import tempfile` | Create temporary bash script files on disk |
| `stat` | `import stat` | Set file permissions (e.g., make a script executable) |
| `glob` | `import glob` | Find files by pattern (e.g., all `*.lock` files) |
| `socket` | `import socket` | Check if a port is available (for Chrome debug port) |
| `datetime` | `from datetime import datetime` | Timestamps for logs, session records |
| `typing` | `from typing import Dict, List, Optional, Any, Callable` | Type hints |
| `pathlib` | `from pathlib import Path` | Modern file path manipulation |

### Third-Party Libraries

| Library | Import | What it does here |
|---------|--------|-------------------|
| `streamlit` | `import streamlit as st` | The entire web UI (buttons, forms, tables, session state) |
| `pandas` | `import pandas as pd` | Loads account/site data into DataFrames for display |
| `plotly` | `import plotly.express as px` | Charts (pie chart, bar chart) in the analytics tab |
| `numpy` | `import numpy as np` | Type checking (`np.integer`, `np.int64`) — handles DB int types |
| `psutil` | `import psutil` | Lists running OS processes — needed to find/kill Chrome |
| `psycopg2` | `from psycopg2.extras import RealDictCursor` | PostgreSQL driver |

---

## 🍪 `cookie_manager.py` — The Cookie Warehouse

This is the simplest and most reusable file. It is a **thin wrapper** around the `account_cookies` database table.

### Database Table It Manages

```sql
CREATE TABLE account_cookies (
    cookie_id    SERIAL PRIMARY KEY,
    account_id   INTEGER NOT NULL REFERENCES accounts(account_id) ON DELETE CASCADE,
    domain       VARCHAR(255) NOT NULL DEFAULT 'google.com',
    cookies_json TEXT NOT NULL,           -- the full JSON array of cookies
    captured_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (account_id, domain)           -- one row per account per domain
);
```

**The UNIQUE constraint is crucial:** for any one account + domain combo, there is always exactly one row. When you save new cookies, it updates that row (upsert) rather than adding duplicates.

---

### `CookieManager.__init__()` — Constructor

**File:** `cookie_manager.py`

```python
class CookieManager:
    def __init__(self, pg_factory: Callable):
        self._pg = pg_factory    # Store the connection factory
```

**Parameters:**
- `pg_factory: Callable` — a **zero-argument function** that returns a context manager which yields a database connection

**Why a factory instead of a connection?** Because database connections should be short-lived. Instead of holding one open connection forever, we create a fresh connection for each operation and close it immediately after. The factory is the "blueprint" for making those connections.

**How it's built in `accounts_page.py`:**
```python
class _PgFactory:
    def __enter__(self_inner):                  # Called when entering `with` block
        self_inner._conn = db.get_connection()  # Get a connection from the pool
        return self_inner._conn                 # Yield the connection

    def __exit__(self_inner, *args):            # Called when leaving `with` block
        if args[1]:                             # args[1] is the exception (if any)
            self_inner._conn.rollback()         # Undo changes on error
        else:
            self_inner._conn.commit()           # Save changes on success

return CookieManager(lambda: _PgFactory())  # lambda = a function that returns _PgFactory()
```

**`lambda: _PgFactory()`** — `lambda` creates an anonymous function. This one takes no arguments and returns a new `_PgFactory()` instance each time it's called. So `self._pg()` in CookieManager creates a fresh `_PgFactory` each call.

---

### `CookieManager.save()` — Store Cookies to DB

**File:** `cookie_manager.py`  
**Parameters:**
- `account_id: int` — which account
- `cookies: List[Dict]` — list of cookie objects
- `domain: str` — e.g., `"google.com"` (default)

**Returns:** `bool` — `True` if saved successfully

```python
def save(self, account_id, cookies, domain="google.com") -> bool:
    if not cookies:
        return False
    try:
        # Filter: only keep cookies that belong to this domain
        relevant = [
            ck for ck in cookies
            if domain.lstrip(".") in ck.get("domain", "").lstrip(".")
        ]
        # If no cookies matched this domain, save everything (fallback)
        payload = json.dumps(relevant if relevant else cookies)

        with self._pg() as conn:             # Open DB connection via factory
            with conn.cursor() as c:
                c.execute("""
                    INSERT INTO account_cookies
                        (account_id, domain, cookies_json, updated_at)
                    VALUES (%s, %s, %s, CURRENT_TIMESTAMP)
                    ON CONFLICT (account_id, domain)
                    DO UPDATE SET
                        cookies_json = EXCLUDED.cookies_json,
                        updated_at   = CURRENT_TIMESTAMP
                """, (account_id, domain, payload))
                conn.commit()               # Save permanently
        return True
    except Exception as e:
        logger.error(f"CookieManager.save: {e}")
        return False
```

**`ON CONFLICT ... DO UPDATE`** — PostgreSQL "upsert":
- If a row with `(account_id, domain)` already exists → update it
- If not → insert a new row
- `EXCLUDED.cookies_json` refers to the value that *would have been inserted* (the new value)

**`domain.lstrip(".")`** — `".google.com"` → `"google.com"` (removes leading dot for consistent comparison)

**`json.dumps(relevant)`** — converts Python list of dicts to a JSON string: `[{"name":"SID","value":"abc",...}]` ready for TEXT column storage

---

### `CookieManager.load()` — Read Cookies from DB

**File:** `cookie_manager.py`  
**Parameters:**
- `account_id: int`
- `domain: str` — which domain to retrieve

**Returns:** `Optional[List[Dict]]` — the cookie list, or `None` if not found

```python
def load(self, account_id, domain="google.com") -> Optional[List[Dict]]:
    try:
        with self._pg() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute(
                    "SELECT cookies_json FROM account_cookies "
                    "WHERE account_id=%s AND domain=%s "
                    "ORDER BY updated_at DESC LIMIT 1",
                    (account_id, domain),
                )
                row = c.fetchone()
                return json.loads(row["cookies_json"]) if row else None
    except Exception as e:
        logger.error(f"CookieManager.load: {e}")
        return None
```

**`json.loads(row["cookies_json"])`** — converts the stored JSON string back to a Python list of dicts. This is the reverse of `json.dumps()`.

**`cursor_factory=RealDictCursor`** — makes `row["cookies_json"]` work (dict access) instead of `row[0]` (tuple access).

---

### `CookieManager.delete()` — Remove Cookies

**File:** `cookie_manager.py`  
**Returns:** `bool`

```python
def delete(self, account_id, domain="google.com") -> bool:
    try:
        with self._pg() as conn:
            with conn.cursor() as c:
                c.execute(
                    "DELETE FROM account_cookies WHERE account_id=%s AND domain=%s",
                    (account_id, domain),
                )
                conn.commit()
        return True
    except Exception as e:
        logger.error(f"CookieManager.delete: {e}")
        return False
```

Simple DELETE. Note: the `REFERENCES accounts(account_id) ON DELETE CASCADE` in the table definition means if you delete an account, all its cookies are deleted automatically by PostgreSQL.

---

### `CookieManager.list_records()` — Get Cookie Metadata

**File:** `cookie_manager.py`  
**Returns:** `List[Dict]` — metadata rows (NOT the actual cookie data, for efficiency)

```python
def list_records(self, account_id) -> List[Dict]:
    try:
        with self._pg() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute("""
                    SELECT cookie_id, domain, captured_at, updated_at,
                           LENGTH(cookies_json) AS size_bytes,
                           json_array_length(cookies_json::json) AS cookie_count
                    FROM account_cookies
                    WHERE account_id=%s
                    ORDER BY domain
                """, (account_id,))
                return [dict(r) for r in c.fetchall()]
    except Exception as e:
        logger.error(f"CookieManager.list_records: {e}")
        return []
```

**`LENGTH(cookies_json)`** — SQL function: returns the length in bytes of the text column. Used to display "Size: 12.3 KB".

**`json_array_length(cookies_json::json)`** — SQL function: parses the JSON and counts array items. `::json` casts the TEXT column to JSON type in PostgreSQL.

---

### `CookieManager.inject()` — Push Cookies into Playwright

**File:** `cookie_manager.py`  
This is an `async` static method — it doesn't use `self` (hence `@staticmethod`).

```python
@staticmethod
async def inject(context, cookies: List[Dict]) -> int:
    """
    Inject cookies into a Playwright BrowserContext.
    Playwright equivalent: context.add_cookies([...])
    """
    sanitized = []
    for ck in cookies:
        entry = {"name": ck["name"], "value": ck["value"]}

        if ck.get("domain"):
            entry["domain"] = ck["domain"]
            entry["path"]   = ck.get("path", "/")
        elif ck.get("url"):
            entry["url"] = ck["url"]
        else:
            entry["domain"] = ".google.com"    # Safe default
            entry["path"]   = "/"

        # Only add optional fields if they have valid values
        if ck.get("secure")   is not None: entry["secure"]   = bool(ck["secure"])
        if ck.get("httpOnly") is not None: entry["httpOnly"] = bool(ck["httpOnly"])
        if ck.get("sameSite") in ("Strict", "Lax", "None"):
            entry["sameSite"] = ck["sameSite"]
        if ck.get("expires") and int(ck["expires"]) > 0:
            entry["expires"] = int(ck["expires"])

        sanitized.append(entry)

    try:
        await context.add_cookies(sanitized)   # Playwright API call
        return len(sanitized)
    except Exception as e:
        logger.warning(f"CookieManager.inject error: {e}")
        return 0
```

**`@staticmethod`** — the method doesn't need `self` (no access to instance variables). Called as `CookieManager.inject(context, cookies)` or `cm.inject(context, cookies)`.

**`bool(ck["secure"])`** — converts to strict `True`/`False`. The cookie might have `1`/`0` or `"true"`/`"false"` from different sources; `bool()` normalizes them.

---

### `CookieManager.render_account_cookie_panel()` — The UI Widget

**File:** `cookie_manager.py`  
**Parameters:**
- `account_id: int`
- `account_email: str` — for display
- `domain: str` — default `"google.com"`
- `key_prefix: str` — unique prefix for Streamlit widget keys (prevents conflicts when rendering multiple panels)

This method renders the full cookie UI in a Streamlit page — status badge, delete button, download button, and manual paste import. It's designed to be called from ANY page that needs cookie management.

```python
def render_account_cookie_panel(self, account_id, account_email="", domain="google.com", key_prefix=""):
    prefix  = key_prefix or f"ck_{account_id}"
    records = self.list_records(account_id)              # Get metadata from DB
    record  = next((r for r in records if r["domain"] == domain), None)  # Find this domain's record

    st.markdown(f"#### 🍪 Google Cookies — `{domain}`")

    if record:
        # Show status with count, size, and timestamp
        st.success(f"✅ **{record['cookie_count']} cookies stored** ...")

        # Download and Delete buttons side by side
        col_dl, col_del = st.columns(2)
        with col_dl:
            raw = self.load(account_id, domain)         # Load actual cookies
            st.download_button("⬇️ Export cookies", json.dumps(raw, indent=2), ...)
        with col_del:
            if st.button("🗑️ Delete stored cookies", key=f"{prefix}_del"):
                self.delete(account_id, domain)
                st.rerun()
    else:
        st.warning("⚠️ No cookies stored...")

    # Expandable manual import section
    with st.expander("📋 Import cookies manually (JSON)"):
        raw_input = st.text_area("Paste cookie JSON array:", key=f"{prefix}_paste")
        if st.button("💾 Save pasted cookies", key=f"{prefix}_save_paste"):
            parsed = json.loads(raw_input.strip())
            ok = self.save(account_id, parsed, domain)
            if ok: st.rerun()
```

**`next((r for r in records if r["domain"] == domain), None)`** — generator expression inside `next()`. Finds the first record where domain matches, or returns `None` if none found.

**`key_prefix`** — every Streamlit widget needs a unique `key`. If you render this panel for 5 accounts in a loop, each needs different keys. `f"{prefix}_del"` becomes `"ck_1_del"`, `"ck_2_del"`, etc.

---

## 🖥️ `chrome_session_manager.py` — The Chrome Handler

This class manages the entire lifecycle of a Chrome browser session: create profile → launch Chrome → stop Chrome → extract cookies → save cookies.

### `ChromeSessionManager.__init__()` — Constructor

**File:** `chrome_session_manager.py`

```python
class ChromeSessionManager:
    def __init__(self, db_manager, mongo_client=None):
        self.db_manager   = db_manager
        self.mongo_client = mongo_client
        self.base_profile_dir = os.environ.get(
            'CHROME_PROFILE_DIR', '/workspace/chrome_profiles'
        )
        self.active_processes = {}    # Dict: session_id → process info
        os.makedirs(self.base_profile_dir, exist_ok=True)
        self._cookie_manager = self._build_cookie_manager()
```

**`self.active_processes = {}`** — an **instance-level dictionary** that tracks running Chrome processes. Key = `session_id` (string), Value = dict with the process object, profile path, account info, etc.

**`os.environ.get('CHROME_PROFILE_DIR', '/workspace/chrome_profiles')`** — try to read `CHROME_PROFILE_DIR` from environment variables; use `'/workspace/chrome_profiles'` as the default if it's not set.

**`os.makedirs(path, exist_ok=True)`** — create the directory if it doesn't exist. `exist_ok=True` means don't raise an error if it already exists.

---

### `create_profile_for_account()` — Set Up a Chrome Profile Directory

**File:** `chrome_session_manager.py`  
**Parameters:**
- `account_id: int`
- `username: str`

**Returns:** `Dict` with `success`, `profile_id`, `profile_path`, `mongodb_id`, `is_new`

```python
def create_profile_for_account(self, account_id, username) -> Dict[str, Any]:
    try:
        profile_id   = f"account_{username}"                              # e.g., "account_john"
        profile_path = os.path.join(self.base_profile_dir, profile_id)   # e.g., "/workspace/chrome_profiles/account_john"
        os.makedirs(profile_path, exist_ok=True)                         # Create the directory

        mongodb_id = None
        if self.mongo_client:
            # Check if this profile already exists in MongoDB
            db = self.mongo_client['messages_db']
            existing = db.accounts.find_one({'profile_id': profile_id})
            if existing:
                db.accounts.update_one(...)    # Update existing record
                mongodb_id = str(existing['_id'])
            else:
                result = db.accounts.insert_one({...})  # Create new record
                mongodb_id = str(result.inserted_id)

        return {
            'success':    True,
            'profile_id': profile_id,
            'profile_path': profile_path,
            'mongodb_id': mongodb_id,
            'is_new':     not os.path.exists(os.path.join(profile_path, 'Default')),
        }
    except Exception as e:
        return {'success': False, 'error': str(e)}
```

**`os.path.join(a, b)`** — joins path parts with the OS separator: `"/workspace/chrome_profiles"` + `"account_john"` → `"/workspace/chrome_profiles/account_john"`.

**`not os.path.exists(...)`** — `is_new` is `True` if the `Default` subfolder doesn't exist yet (Chrome creates it on first launch).

**WHERE it saves:**
- Filesystem: creates `/workspace/chrome_profiles/account_{username}/`
- MongoDB: `messages_db.accounts` collection — one document per profile

---

### `run_persistent_chrome()` — Launch Chrome

**File:** `chrome_session_manager.py`  
**Parameters:**
- `session_id: str` — unique identifier for this session
- `profile_path: str` — the Chrome user-data directory
- `username: str`
- `account_id: int`
- `survey_url: str` — the URL to open
- `show_terminal: bool`

**Returns:** `Dict` with `success`, `vnc_url`, `debug_port`, `session_id`

**The key mechanism — writing a bash script:**

```python
bash_script = f"""#!/bin/bash
CHROME_PROFILE_DIR="{profile_path}"
SURVEY_URL="{survey_url}"
DEBUG_PORT={debug_port}
export DISPLAY=:99          # X11 display number for the VNC session

# Remove stale lock files (Chrome crashes leave these behind)
for lockfile in SingletonLock SingletonCookie ...; do
    rm -f "$CHROME_PROFILE_DIR/$lockfile" 2>/dev/null || true
done

# Kill any existing Chrome using ONLY this specific profile
pkill -f "chrome.*$CHROME_PROFILE_DIR" 2>/dev/null || true
sleep 2

# Start Chrome
google-chrome-stable \\
    --user-data-dir="$CHROME_PROFILE_DIR" \\
    --remote-debugging-port=$DEBUG_PORT \\
    --no-sandbox \\
    ...
    "$SURVEY_URL" &         # & = run in background

CHROME_PID=$!               # $! = PID of last background command

# Trap SIGTERM so Chrome can save state before we kill it
trap "kill -TERM $CHROME_PID; wait $CHROME_PID" SIGTERM SIGINT

wait $CHROME_PID            # Wait for Chrome to exit

# Cleanup on exit
rm -f "$CHROME_PROFILE_DIR/SingletonLock" 2>/dev/null || true
"""
```

**Then write the script to disk and launch it:**

```python
with tempfile.NamedTemporaryFile(delete=False, suffix=".sh", mode="w") as f:
    f.write(bash_script)
    script_path = f.name        # e.g., "/tmp/tmpXYZ123.sh"

os.chmod(script_path, 0o755)    # Make it executable

proc = subprocess.Popen(
    ["bash", script_path],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    preexec_fn=os.setsid,        # Create a new process group (for SIGTERM to group)
)
```

**`subprocess.Popen`** — starts an external process asynchronously (Python doesn't wait for it to finish). Returns a `proc` object we can use to check status or kill it later.

**`preexec_fn=os.setsid`** — before starting the subprocess, call `os.setsid()` which creates a new **session/process group**. This lets us kill Chrome AND all its child processes at once with `os.killpg(pgid, signal.SIGTERM)`.

**WHERE it saves:**
```python
self.active_processes[session_id] = {
    "process":      proc,          # The subprocess.Popen object
    "profile_path": profile_path,
    "username":     username,
    "account_id":   account_id,
    "debug_port":   debug_port,
    "started_at":   datetime.now(),
    ...
}
```
→ Saved in `self.active_processes` (in-memory dict on the `ChromeSessionManager` instance)

---

### `_extract_cookies_from_profile()` — Read Chrome's SQLite Cookie DB

**File:** `chrome_session_manager.py`  
**Parameters:**
- `profile_path: str` — path to the Chrome profile folder
- `account_id: int`

**Returns:** `Dict` with `success`, `cookies`, `count`

```python
def _extract_cookies_from_profile(self, profile_path, account_id) -> Dict[str, Any]:
    try:
        cookies_db = os.path.join(profile_path, 'Default', 'Cookies')
        # e.g., "/workspace/chrome_profiles/account_john/Default/Cookies"

        if not os.path.exists(cookies_db):
            return {'success': False, 'error': 'Cookie database not found'}

        # IMPORTANT: Copy the file before opening it
        # Chrome may have the file locked — copying avoids this
        temp_db = cookies_db + '.temp'
        shutil.copy2(cookies_db, temp_db)

        conn   = sqlite3.connect(temp_db)   # Open SQLite database
        conn.text_factory = bytes           # Return bytes instead of str (Chrome stores some as bytes)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT name, value, host_key, path, is_secure, is_httponly, "
            "has_expires, expires_utc FROM cookies"
        )

        cookies = []
        for row in cursor.fetchall():
            name      = row[0].decode('utf-8')   # bytes → string
            value     = row[1].decode('utf-8')
            domain    = row[2].decode('utf-8')
            ...

            # Chrome stores timestamps as microseconds since Jan 1, 1601
            # Unix timestamps are seconds since Jan 1, 1970
            # We need to convert between these two epoch systems
            expires_utc = row[7]
            if expires_utc and expires_utc != 0:
                seconds_since_1601 = expires_utc / 1_000_000      # microseconds → seconds
                chrome_epoch = dt.datetime(1601, 1, 1)
                unix_epoch   = dt.datetime(1970, 1, 1)
                delta        = (chrome_epoch - unix_epoch).total_seconds()  # -11644473600
                expiration_date = seconds_since_1601 - delta       # Chrome time → Unix time

            cookies.append({
                'name':           name,
                'value':          value,
                'domain':         domain,
                'path':           path,
                'secure':         secure,
                'httpOnly':       http_only,
                'expirationDate': expiration_date,
            })

        conn.close()
        os.remove(temp_db)      # Clean up the temp copy

        return {'success': True, 'cookies': cookies, 'count': len(cookies)}
    except Exception as e:
        return {'success': False, 'error': str(e)}
```

**Chrome's timestamp system:** Chrome stores cookie expiry as microseconds since January 1, 1601 (not the Unix epoch of 1970). The conversion subtracts the difference between the two epochs (about 11,644,473,600 seconds).

**`shutil.copy2(src, dst)`** — copies a file preserving metadata. Used because Chrome may have an exclusive lock on the `Cookies` file while running. Reading from a copy avoids lock conflicts.

**WHERE this reads from:**
- `/workspace/chrome_profiles/account_{username}/Default/Cookies` — Chrome's SQLite database

---

### `_sync_cookies_on_stop()` — Save Cookies When Chrome Closes

**File:** `chrome_session_manager.py`  
**Parameters:**
- `account_id: int`
- `profile_path: str`

**Returns:** `Dict` with `success`, `synced`, `domains_saved`

```python
def _sync_cookies_on_stop(self, account_id, profile_path) -> Dict[str, Any]:
    # Step 1: Extract cookies from Chrome's SQLite DB
    extract_result = self._extract_cookies_from_profile(profile_path, account_id)
    if not extract_result['success']:
        return {'success': False, 'error': extract_result.get('error')}

    cookies = extract_result['cookies']

    if self._cookie_manager:
        # Step 2: Group cookies by their registered domain
        # e.g., "accounts.google.com" and ".google.com" both → "google.com"
        domain_map: Dict[str, list] = {}
        for ck in cookies:
            raw_domain = ck.get('domain', '').lstrip('.')   # Remove leading dot
            parts      = raw_domain.split('.')               # Split by dot
            # Take only last 2 parts: "accounts.google.com" → ["google", "com"] → "google.com"
            canonical  = '.'.join(parts[-2:]) if len(parts) >= 2 else raw_domain or 'unknown'
            domain_map.setdefault(canonical, []).append(ck)

        # Step 3: Save each domain's cookies separately
        synced_domains = []
        for domain, domain_cookies in domain_map.items():
            ok = self._cookie_manager.save(account_id, domain_cookies, domain)
            if ok:
                synced_domains.append(domain)

        # Step 4: Update the accounts table flags
        self._update_account_cookie_flags(account_id)

        return {'success': True, 'synced': len(cookies), 'domains_saved': synced_domains}

    # Fallback: if no CookieManager, use direct SQL
    return self._store_cookies_via_db_manager(account_id, cookies)
```

**`parts[-2:]`** — Python negative indexing: `-2` = second from last. `["accounts", "google", "com"][-2:]` = `["google", "com"]`. Then `'.'.join(...)` = `"google.com"`.

**`domain_map.setdefault(canonical, []).append(ck)`** — if `canonical` key doesn't exist in `domain_map`, create it with an empty list `[]`, then append `ck` to that list. Equivalent to:
```python
if canonical not in domain_map:
    domain_map[canonical] = []
domain_map[canonical].append(ck)
```

**WHERE it saves:** → `account_cookies` table in PostgreSQL, via `CookieManager.save()`

---

### `stop_session()` — Graceful Chrome Shutdown

**File:** `chrome_session_manager.py`  
**Parameters:**
- `session_id: str`

**Returns:** `Dict` with `success`, `message`, `profile_path`

This is the most complex method — it coordinates a graceful shutdown:

```
stop_session(session_id)
        │
        ├── 1. Find session info in self.active_processes
        │
        ├── 2. Try CDP (Chrome DevTools Protocol) to close gracefully
        │       └── urllib.request.urlopen("http://localhost:{debug_port}/json/close")
        │
        ├── 3. Send SIGTERM to Chrome process group
        │       └── os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        │
        ├── 4. Wait up to 10 seconds for Chrome to exit
        │       └── Poll psutil.process_iter() every 0.5s
        │
        ├── 5. If still running → force kill with SIGKILL
        │
        ├── 6. Clean up lock files
        │       └── _cleanup_singleton_locks(profile_path)
        │
        ├── 7. Delete temp bash script from /tmp/
        │
        ├── 8. Extract & store cookies  ← THE KEY STEP
        │       └── _sync_cookies_on_stop(account_id, profile_path)
        │               └── _extract_cookies_from_profile()  → reads SQLite DB
        │               └── CookieManager.save()            → writes to PostgreSQL
        │
        ├── 9. Update MongoDB session record
        │       └── db.browser_sessions.update_one({session_id}, {is_active: False})
        │
        └── 10. Remove from self.active_processes
```

```python
# Step 2: CDP close attempt
try:
    import urllib.request
    urllib.request.urlopen(f"http://localhost:{debug_port}/json/close", timeout=3)
except Exception:
    pass    # If this fails (Chrome already closed, port not open), continue

# Step 3: SIGTERM to process group
proc = proc_info['process']
if proc.poll() is None:    # poll() returns None if process is still running
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
    except ProcessLookupError:    # Process already dead
        pass

# Step 4: Wait for Chrome to exit
deadline = time.time() + 10    # 10 seconds from now
while time.time() < deadline:
    chrome_still_running = False
    for p in psutil.process_iter(['name', 'cmdline']):
        if 'chrome' in p.info['name'].lower():
            if profile_path in ' '.join(p.info['cmdline']):
                chrome_still_running = True
                break
    if not chrome_still_running:
        break            # Chrome exited — great!
    time.sleep(0.5)
else:
    # The while loop completed without breaking → Chrome still running
    # Force kill
    ...
```

**`proc.poll()`** — checks if the subprocess has finished. Returns `None` if still running, or the exit code if finished.

**`os.killpg(pgid, signal)`** — sends a signal to an entire **process group**. Because Chrome spawns many child processes (renderer, GPU, network), this kills all of them at once.

**`signal.SIGTERM`** — "please stop gracefully" signal. Chrome gets a chance to save state.  
**`signal.SIGKILL`** — "stop immediately, no cleanup" signal. Used as last resort.

**`while ... else:`** — Python's unusual loop construct. The `else` block runs ONLY if the loop finished without a `break`. Here it means "Chrome didn't exit within 10 seconds".

---

### `_cleanup_singleton_locks()` — Remove Chrome Lock Files

**File:** `chrome_session_manager.py`

Chrome creates lock files (`SingletonLock`, `SingletonCookie`, etc.) when it starts. If Chrome crashes, these files aren't removed, and the next Chrome launch refuses to start (thinks another instance is running).

```python
def _cleanup_singleton_locks(self, profile_path=None):
    lock_filenames = ['SingletonLock', 'SingletonCookie', 'SingletonSocket', 'lockfile']

    for directory in dirs_to_clean:
        for name in lock_filenames:
            target = os.path.join(directory, name)
            if os.path.exists(target) or os.path.islink(target):
                try:
                    os.remove(target)       # Try normal remove
                except Exception:
                    os.unlink(target)       # unlink works on symlinks too
```

**`os.path.islink(target)`** — `SingletonLock` is often a **symbolic link** (a pointer to another file) not a regular file. `os.path.exists()` returns `False` for broken symlinks but `os.path.islink()` still returns `True`.

---

### `_get_next_available_port()` — Find a Free Port

**File:** `chrome_session_manager.py`  
**Returns:** `int` — an available port number

```python
def _get_next_available_port(self, start_port=9222, end_port=9322) -> int:
    import socket
    for port in range(start_port, end_port + 1):
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(('', port))    # Try to bind to this port
                return port            # If no error → port is available
        except OSError:
            continue                   # Port is in use → try next one
    raise Exception("No available ports in range")
```

Chrome needs a unique **debug port** (for the Chrome DevTools Protocol). If you run two Chrome instances, they can't both use port 9222. This method finds the next free port in the range 9222–9322.

**`socket.AF_INET`** — IPv4 address family  
**`socket.SOCK_STREAM`** — TCP connection type  
**`s.bind(('', port))`** — try to claim this port. Raises `OSError` if already in use.

---

## 👥 `accounts_page.py` — The Main Orchestrator

This is the largest file. It wires everything together into the Streamlit UI.

### `AccountsPage.__init__()` — Constructor

**File:** `accounts_page.py`

```python
class AccountsPage:
    def __init__(self, db_manager):
        self.db_manager = db_manager

        # Build the CookieManager (shared across all operations)
        self._cookie_manager = self._build_cookie_manager()

        # Set up Streamlit session state defaults
        defaults = {
            'use_local_chrome':         True,
            'account_creation_logs':    [],
            'show_add_account':         False,
            'creating_account':         False,
            ...
        }
        for key, val in defaults.items():
            if key not in st.session_state:    # Only set if not already set
                st.session_state[key] = val

        # Build the ChromeSessionManager
        try:
            from ..hyperbrowser_utils import get_mongodb_client
            client, db = get_mongodb_client()
            self.chrome_manager = ChromeSessionManager(db_manager, client)
        except Exception as e:
            self.chrome_manager = ChromeSessionManager(db_manager, None)  # No MongoDB

        # Run database schema migrations
        self._ensure_survey_columns()
        self._ensure_demographic_columns()
        self._ensure_cookie_manager_schema()
        ...
```

**Why run migrations in `__init__`?** Every time the app starts (or the page is first loaded), it checks if all necessary database columns and tables exist and creates them if missing. This is safer than relying on manual migration scripts.

---

### Schema Migration Methods — `_ensure_*()` Pattern

These all follow the same pattern: check if something exists → create it if not.

```python
def _ensure_demographic_columns(self):
    try:
        demographic_columns = [
            ("age", "INTEGER"),
            ("gender", "VARCHAR(50)"),
            ("email", "VARCHAR(255)"),
            ...
        ]
        for col_name, col_type in demographic_columns:
            check = f"""
            SELECT column_name FROM information_schema.columns
            WHERE table_name='accounts' AND column_name='{col_name}'
            """
            if not self.db_manager.execute_query(check, fetch=True):
                # Column doesn't exist → add it
                self.db_manager.execute_query(
                    f"ALTER TABLE accounts ADD COLUMN {col_name} {col_type}"
                )
    except Exception as e:
        logger.error(f"Failed to ensure demographic columns: {e}")
```

**`information_schema.columns`** — a special PostgreSQL view (virtual table) that contains metadata about all columns in all tables. Querying it tells you what columns exist without touching actual data.

**`ALTER TABLE ... ADD COLUMN`** — SQL command to add a new column to an existing table. Existing rows get `NULL` for the new column.

---

### `_store_account_cookies()` — Save Cookies from UI Input

**File:** `accounts_page.py`  
**Parameters:**
- `account_id: int`
- `cookies_json: str` — raw JSON string pasted by user
- `username: str`

**Returns:** `Dict` with `success`, `cookie_count`, `domains`, `error`

```python
def _store_account_cookies(self, account_id, cookies_json, username) -> Dict[str, Any]:
    try:
        # Handle numpy integer types from pandas DataFrame
        if hasattr(account_id, 'item') or isinstance(account_id, (np.integer, np.int64)):
            account_id = int(account_id)    # Convert to plain Python int

        # Parse the JSON string
        try:
            cookies = json.loads(cookies_json)
        except json.JSONDecodeError as e:
            return {'success': False, 'error': f"Invalid JSON: {e}"}

        if not isinstance(cookies, list):
            return {'success': False, 'error': "Cookies must be a JSON array"}

        # Validate each cookie has required fields
        for i, ck in enumerate(cookies):
            missing = [f for f in ('name', 'value', 'domain') if f not in ck]
            if missing:
                return {'success': False, 'error': f"Cookie #{i} missing: {missing}"}

        if self._cookie_manager:
            # Determine which domains appear in the cookies
            domains = list({
                ck.get('domain', '').lstrip('.')
                for ck in cookies
                if ck.get('domain')
            })
            # Reduce to "canonical" domain (last two parts)
            canonical_domains = list({
                '.'.join(d.split('.')[-2:]) for d in domains if d
            } or {'google.com'})   # Fallback to google.com if no domains found

            # Save cookies for each domain found
            saved_count = 0
            for domain in canonical_domains:
                ok = self._cookie_manager.save(account_id, cookies, domain)
                if ok:
                    saved_count += 1

            # Update the accounts table metadata
            self.db_manager.execute_query(
                "UPDATE accounts SET has_cookies=TRUE, cookies_last_updated=CURRENT_TIMESTAMP "
                "WHERE account_id=%s",
                (account_id,),
            )
            return {
                'success':      saved_count > 0,
                'cookie_count': len(cookies),
                'domains':      canonical_domains,
                'error':        None if saved_count > 0 else "All domain saves failed",
            }
    except Exception as e:
        return {'success': False, 'error': f"Failed to store cookies: {e}"}
```

**`hasattr(account_id, 'item')`** — pandas and numpy integers have an `.item()` method that converts them to Python native types. Regular Python `int` doesn't. This check handles both cases.

**Set comprehension `{ck.get('domain', '').lstrip('.') for ck in cookies if ck.get('domain')}`** — like a list comprehension but creates a `set` (no duplicates). The curly braces `{}` create a set.

---

### `_handle_account_creation()` — Create a New Account

**File:** `accounts_page.py`

```python
def _handle_account_creation(self, username, country=None, cookies_json=None, demographic_data=None):
    if st.session_state.get('creation_in_progress'):
        return    # Guard: don't run twice simultaneously

    st.session_state.creation_in_progress = True
    try:
        with st.status("Creating account...", expanded=True) as status:
            st.write("🔄 Creating account record...")
            # Step 1: Insert into accounts table in PostgreSQL
            account_result = self._create_account_in_postgres_minimal(username, country, demographic_data)
            account_id = account_result['account_id']

            st.write("🔄 Creating local Chrome profile...")
            # Step 2: Create the Chrome profile directory on disk
            profile_result = self._create_local_chrome_profile_for_account(username, account_id)

            st.write("🔄 Linking profile to account...")
            # Step 3: Store the profile path in the accounts table
            self.db_manager.execute_query(
                "UPDATE accounts SET profile_id=%s, profile_type='local_chrome' WHERE account_id=%s",
                (profile_result['profile_id'], account_id),
            )

            status.update(label="✓ Account created!", state="complete")
            st.cache_data.clear()    # Clear cached account list so new account appears
            st.rerun()               # Refresh the page
    except Exception as e:
        st.session_state.account_creation_error = True
        st.rerun()
```

**`st.status(...)`** — Streamlit widget that shows a collapsible status box with a spinner. Calling `status.update(state="complete")` changes it to a green checkmark.

**`st.cache_data.clear()`** — `load_accounts_data()` is decorated with `@st.cache_data(ttl=300)` which caches its result for 300 seconds. Without clearing the cache, the new account wouldn't appear for up to 5 minutes.

**`st.rerun()`** — tells Streamlit to immediately re-run the entire script from the top. This refreshes the UI with the latest data.

---

### `@st.cache_data` — Caching Database Queries

```python
@st.cache_data(ttl=300)
def load_accounts_data(_self) -> pd.DataFrame:
    # Reads from PostgreSQL and returns a DataFrame
    ...
```

**`@st.cache_data(ttl=300)`** — decorator that memoizes (caches) the function's return value. The result is reused for 300 seconds (5 minutes) instead of re-querying the database on every page interaction.

**Why `_self` instead of `self`?** Streamlit's caching ignores parameters starting with `_`. Since `self` (the class instance) changes, using `self` would cause cache misses on every call. Using `_self` tells Streamlit "don't use this in the cache key" — so the database results are shared across all instances.

---

### `_start_local_chrome_session()` — Launch Chrome from the UI

**File:** `accounts_page.py`  

```python
def _start_local_chrome_session(self, profile_id, account_id, account_username, start_url=None):
    try:
        DEFAULT = "https://mylocation.org/"
        if not start_url or start_url.strip() == "":
            start_url = DEFAULT

        # Convert numpy int to Python int if needed
        if hasattr(account_id, 'item') or isinstance(account_id, (np.integer, np.int64)):
            account_id = int(account_id)

        profile_path = self.chrome_manager.get_profile_path(account_username)
        session_id   = f"local_{account_username}_{int(time.time())}"
        # e.g., "local_john_1710000000"

        # Optionally generate a cookie clipboard script
        self._ensure_account_cookie_script(account_id, account_username)

        # Launch Chrome via ChromeSessionManager
        result = self.chrome_manager.run_persistent_chrome(
            session_id=session_id,
            profile_path=profile_path,
            username=account_username,
            account_id=account_id,
            survey_url=start_url,
        )

        if not result.get("success"):
            raise Exception(result.get("error", "Unknown error"))

        # Store session in MongoDB
        if self.chrome_manager.mongo_client:
            db = self.chrome_manager.mongo_client["messages_db"]
            db.browser_sessions.insert_one({
                "session_id":    session_id,
                "account_id":    int(account_id),
                "start_url":     start_url,
                "created_at":    datetime.now(),
                "is_active":     True,
            })

        # Store session in Streamlit session state
        st.session_state.local_chrome_sessions[session_id] = {
            "session_id":       session_id,
            "account_id":       int(account_id),
            "account_username": account_username,
            "started_at":       datetime.now(),
            "debug_port":       result.get("debug_port"),
            ...
        }

        return {"success": True, "session_id": session_id, ...}
    except Exception as e:
        return {"success": False, "error": str(e)}
```

**Session ID format:** `f"local_{account_username}_{int(time.time())}"` — e.g., `"local_john_1710000000"`. Using a Unix timestamp ensures uniqueness even if the same user starts multiple sessions.

**WHERE it saves:**
- `st.session_state.local_chrome_sessions` — in-memory, lost on page refresh
- `messages_db.browser_sessions` in MongoDB — persistent across refreshes

---

### `_generate_cookie_copy_script()` — Create a Clipboard Script

**File:** `accounts_page.py`

This method generates a bash script that copies the account's cookies to the clipboard inside the VNC environment. Useful when you want to paste cookies into a browser extension manually.

```python
def _generate_cookie_copy_script(self, account_id, username):
    cookie_info = self._get_account_cookies(account_id)
    cookies     = cookie_info['cookie_data']

    cookie_json  = json.dumps(cookies, indent=2)
    # Escape single quotes for bash heredoc safety
    escaped_json = cookie_json.replace("'", "'\"'\"'")

    # Only allow alphanumeric, dash, underscore in filename
    safe_username = "".join(c for c in username if c.isalnum() or c in "-_")

    scripts_dir = Path("/app/cookie_scripts")
    scripts_dir.mkdir(exist_ok=True, mode=0o777)    # Create with full permissions
    script_path = scripts_dir / f"copy_cookies_{safe_username}.sh"

    script_content = f"""#!/bin/bash
echo '{escaped_json}' | xclip -selection clipboard
echo "✅ Cookies copied to clipboard"
"""

    script_path.write_text(script_content)
    # Set permissions: owner can read/write/execute; others can read/execute
    os.chmod(script_path, stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH)
```

**`"".join(c for c in username if c.isalnum() or c in "-_")`** — filters username characters to only safe ones. `"john.doe@email.com"` → `"johndoeemail.com"` (removes `.` and `@`). Prevents path traversal attacks.

**`stat.S_IRWXU | stat.S_IRGRP | stat.S_IXGRP`** — bitwise OR of permission flags:
- `S_IRWXU` = owner: read+write+execute (rwx)
- `S_IRGRP` = group: read (r--)
- `S_IXGRP` = group: execute (--x)
Result: `rwxr-x---`

**WHERE it saves:** `/app/cookie_scripts/copy_cookies_{username}.sh` on disk

---

### `render()` — The Main UI Entry Point

**File:** `accounts_page.py`

```python
def render(self):
    st.title("👥 Accounts Management")
    self._render_quick_stats()          # 4 metric boxes at the top
    st.markdown("---")                  # Horizontal rule

    # Create 4 tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🖥️ Local Chrome", "📈 Analytics", "🌐 Survey Sites"])

    with st.spinner("Loading data..."):
        accounts_df     = self.load_accounts_data()      # Read accounts from PostgreSQL
        survey_sites_df = self.load_survey_sites_data()  # Read sites from PostgreSQL

    with tab1: self._render_overview_tab(accounts_df)
    with tab2: self._render_local_chrome_tab(accounts_df)
    with tab3: self._render_analytics_tab(accounts_df)
    with tab4: self._render_survey_sites_tab(survey_sites_df)
```

**`st.tabs([...])`** — creates clickable tab panels. Returns one object per tab, used as context managers.

**`with tab1:`** — everything inside renders in that tab.

---

## 🔄 Complete System Flow — From Button Click to Cookie Saved

### Flow 1: Creating a New Account

```
User fills form → clicks "Create Local Chrome Account"
        │
        ▼
AccountsPage.render_add_account_modal()
        │  (form submit detected)
        ▼
Sets session state:
  st.session_state.username_to_create = "john"
  st.session_state.creating_account = True
st.rerun()
        │
        ▼
AccountsPage._handle_account_creation("john", "USA", {...})
        │
        ├── _create_account_in_postgres_minimal()
        │       └── INSERT INTO accounts (username, country, age, gender, ...)
        │       └── RETURNS account_id = 42
        │
        ├── _create_local_chrome_profile_for_account("john", 42)
        │       └── ChromeSessionManager.create_profile_for_account(42, "john")
        │               ├── os.makedirs("/workspace/chrome_profiles/account_john/")
        │               └── MongoDB: db.accounts.insert_one({profile_id: "account_john", ...})
        │
        └── db.execute("UPDATE accounts SET profile_id='account_john' WHERE account_id=42")
        
SAVED TO:
  PostgreSQL: accounts table (new row)
  Filesystem: /workspace/chrome_profiles/account_john/ (new directory)
  MongoDB:    messages_db.accounts (new document)
```

---

### Flow 2: Starting a Chrome Session

```
User selects account → selects URL → clicks "▶️ Start Chrome Session"
        │
        ▼
AccountsPage._start_local_chrome_session(profile_id, account_id, username, url)
        │
        ├── _ensure_account_cookie_script(account_id, username)
        │       └── _get_account_cookies(account_id)          ← reads PostgreSQL
        │       └── _generate_cookie_copy_script()
        │               └── writes /app/cookie_scripts/copy_cookies_john.sh
        │
        ├── ChromeSessionManager.run_persistent_chrome(session_id, profile_path, ...)
        │       ├── Kill existing Chrome using this profile (psutil)
        │       ├── _cleanup_singleton_locks(profile_path)    ← deletes lock files
        │       ├── _get_next_available_port(9222, 9322)       ← finds free port
        │       ├── Write bash script to /tmp/tmpXYZ.sh
        │       └── subprocess.Popen(["bash", "/tmp/tmpXYZ.sh"])  ← Chrome launches!
        │
        ├── MongoDB: db.browser_sessions.insert_one({session_id, is_active: True, ...})
        │
        └── st.session_state.local_chrome_sessions[session_id] = {...}

SAVED TO:
  st.session_state: local_chrome_sessions dict (in-memory)
  MongoDB: messages_db.browser_sessions (new document)
  Filesystem: /tmp/tmpXYZ.sh (temp script, cleaned up on stop)
  
Chrome is now running with profile: /workspace/chrome_profiles/account_john/
```

---

### Flow 3: Stopping a Chrome Session

```
User clicks "🛑" stop button
        │
        ▼
AccountsPage._stop_local_chrome_session(session_id)
        │
        └── ChromeSessionManager.stop_session(session_id)
                │
                ├── 1. CDP close: GET http://localhost:{debug_port}/json/close
                │
                ├── 2. SIGTERM → Chrome process group
                │
                ├── 3. Wait up to 10s for Chrome to exit
                │
                ├── 4. Force SIGKILL if needed
                │
                ├── 5. _cleanup_singleton_locks()
                │
                ├── 6. os.remove(temp script path)
                │
                ├── 7. _sync_cookies_on_stop(account_id, profile_path)
                │       │
                │       ├── _extract_cookies_from_profile()
                │       │       ├── shutil.copy2(Chrome/Default/Cookies → temp file)
                │       │       ├── sqlite3.connect(temp_file)
                │       │       ├── SELECT * FROM cookies
                │       │       └── Convert Chrome timestamp → Unix timestamp
                │       │
                │       └── CookieManager.save(account_id, cookies, domain)
                │               └── INSERT INTO account_cookies ... ON CONFLICT DO UPDATE
                │
                ├── 8. MongoDB: update session {is_active: False, ended_at: now}
                │
                └── 9. del self.active_processes[session_id]

SAVED TO:
  PostgreSQL: account_cookies table (upserted per domain)
  PostgreSQL: accounts table (has_cookies=TRUE, cookies_last_updated=now)
  MongoDB: browser_sessions document updated
  Removed from: st.session_state.local_chrome_sessions
```

---

### Flow 4: Saving Cookies Manually (Paste in UI)

```
User pastes cookie JSON → clicks "💾 Save pasted cookies"
        │
        ▼
CookieManager.render_account_cookie_panel() (in cookie_manager.py)
        │
        ├── json.loads(raw_input)         ← parse string to Python list
        ├── Validate it's a list
        └── CookieManager.save(account_id, parsed, domain)
                ├── Filter cookies to this domain
                ├── json.dumps(relevant)
                └── INSERT ... ON CONFLICT DO UPDATE  ← PostgreSQL upsert

SAVED TO:
  PostgreSQL: account_cookies table
```

---

## 💡 Key Python Concepts in This Codebase

| Concept | Where Used | Explanation |
|---------|-----------|-------------|
| Context manager `with` | Every DB operation | Auto-close connections/cursors even on errors |
| `@st.cache_data(ttl=N)` | `load_accounts_data()` | Cache expensive DB queries for N seconds |
| `st.session_state` | Throughout | Persist data between Streamlit reruns |
| `subprocess.Popen` | Chrome launch | Start external programs without blocking Python |
| `psutil.process_iter` | Chrome kill | Iterate over all running OS processes |
| `signal.SIGTERM/SIGKILL` | Chrome stop | OS signals to request/force process termination |
| `sqlite3` | Cookie extraction | Read Chrome's local cookie database |
| `@staticmethod` | `CookieManager.inject/extract` | Method that doesn't need `self` |
| `lambda` | DB factory | Anonymous function: `lambda: _PgFactory()` |
| `hasattr` | Type checking | Check if object has a method before calling it |
| `isinstance(x, np.integer)` | ID conversion | Handle pandas/numpy integer types safely |
| Dictionary `setdefault` | Domain grouping | `dict.setdefault(key, []).append(item)` |
| Set comprehension `{...}` | Domain deduplication | Like list comprehension but removes duplicates |
| Negative indexing `[-2:]` | Domain parsing | `["a","b","c"][-2:]` = `["b","c"]` |
| `str.lstrip(".")` | Domain normalization | `".google.com"` → `"google.com"` |
| `os.killpg(pgid, signal)` | Process groups | Kill Chrome + all its child processes at once |
| Bitwise OR `\|` | File permissions | Combine permission flags: `S_IRWXU \| S_IRGRP` |
| `while ... else:` | Timeout loop | `else` runs only if loop finishes without `break` |
| `proc.poll()` | Process status | Returns `None` if process still running |
| f-string multiline | Bash script | `f"""..."""` with variables injected |

---

## 🗺️ Complete Architecture Diagram

```
                          ┌─────────────────────────┐
                          │      Browser / User      │
                          └────────────┬────────────┘
                                       │ HTTP
                          ┌────────────▼────────────┐
                          │    Streamlit App         │
                          │  accounts_page.py        │
                          │  (AccountsPage class)    │
                          └──┬──────────┬────────────┘
                             │          │
              ┌──────────────▼──┐   ┌───▼──────────────────┐
              │  CookieManager  │   │ ChromeSessionManager  │
              │cookie_manager.py│   │chrome_session_manager │
              └──────┬──────────┘   └───┬──────────────────┘
                     │                  │
         ┌───────────▼──────────┐  ┌───▼──────────────┐
         │     PostgreSQL       │  │  Chrome Process   │
         │  account_cookies     │  │  (subprocess)     │
         │  accounts            │  └───┬──────────────┘
         │  survey_sites        │      │ reads/writes
         │  account_urls        │  ┌───▼──────────────┐
         └──────────────────────┘  │  Filesystem       │
                                   │ /workspace/chrome_│
         ┌──────────────────────┐  │  profiles/*/     │
         │      MongoDB         │  │  Default/Cookies  │
         │  browser_sessions    │  │  (SQLite DB)      │
         │  accounts            │  └──────────────────┘
         └──────────────────────┘
```

---

*Generated for learning purposes — every function, storage location, and data flow explained from first principles.*