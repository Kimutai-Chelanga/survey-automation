import subprocess
import os
import time
import logging
import tempfile
import signal
import sqlite3
import json
import shutil
import datetime as dt
from datetime import datetime
from typing import Dict, Any, Optional
import psutil
from pathlib import Path
import glob

logger = logging.getLogger(__name__)


class ChromeSessionManager:
    """Manages local Chrome sessions with terminal window and persistent profiles"""

    def __init__(self, db_manager, mongo_client=None):
        self.db_manager = db_manager
        self.mongo_client = mongo_client
        self.base_profile_dir = os.environ.get(
            'CHROME_PROFILE_DIR', '/workspace/chrome_profiles'
        )
        self.active_processes = {}
        os.makedirs(self.base_profile_dir, exist_ok=True)
        logger.info(f"ChromeSessionManager initialized with profile dir: {self.base_profile_dir}")

    # =========================================================================
    # SINGLETON LOCK CLEANUP
    # =========================================================================

    def _cleanup_singleton_locks(self, profile_path: str = None):
        """Remove all Chrome Singleton lock files."""
        dirs_to_clean = []

        if profile_path:
            dirs_to_clean.append(profile_path)
            dirs_to_clean.append(os.path.join(profile_path, 'Default'))
        else:
            try:
                for entry in os.listdir(self.base_profile_dir):
                    full = os.path.join(self.base_profile_dir, entry)
                    if os.path.isdir(full):
                        dirs_to_clean.append(full)
                        dirs_to_clean.append(os.path.join(full, 'Default'))
            except Exception as e:
                logger.warning(f"Could not list profile directories: {e}")

        lock_filenames = ['SingletonLock', 'SingletonCookie', 'SingletonSocket', 'lockfile']
        session_filenames = ['Last Session', 'Last Tabs', 'Current Session', 'Current Tabs']
        removed = []

        for directory in dirs_to_clean:
            if not os.path.isdir(directory):
                continue

            for name in lock_filenames:
                target = os.path.join(directory, name)
                if os.path.exists(target) or os.path.islink(target):
                    try:
                        os.remove(target)
                        removed.append(target)
                        logger.info(f"Removed lock file: {target}")
                    except Exception:
                        try:
                            os.unlink(target)
                            removed.append(target)
                            logger.info(f"Unlinked lock file: {target}")
                        except Exception as e2:
                            logger.warning(f"Could not remove {target}: {e2}")

            if directory.endswith('/Default') or directory.endswith('\\Default'):
                for name in session_filenames:
                    target = os.path.join(directory, name)
                    if os.path.exists(target):
                        try:
                            os.remove(target)
                            removed.append(target)
                            logger.info(f"Removed session file: {target}")
                        except Exception as e:
                            logger.warning(f"Could not remove session file {target}: {e}")

        try:
            patterns = [
                os.path.join(self.base_profile_dir, '**', '.org.chromium.Chromium.*'),
                os.path.join(self.base_profile_dir, '**', '.com.google.Chrome.*'),
            ]
            for pattern in patterns:
                for match in glob.glob(pattern, recursive=True):
                    try:
                        os.remove(match)
                        removed.append(match)
                        logger.info(f"Removed chromium temp file: {match}")
                    except Exception as e:
                        logger.warning(f"Could not remove {match}: {e}")
        except Exception as e:
            logger.warning(f"Error cleaning chromium temp files: {e}")

        if removed:
            logger.info(f"Singleton lock cleanup: removed {len(removed)} file(s)")
        else:
            logger.info("Singleton lock cleanup: no lock files found")

        return removed

    def _kill_all_chrome_everywhere(self):
        """Kill every Chrome-related process on this host."""
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if proc.info['name'] and 'chrome' in proc.info['name'].lower():
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

    def _cleanup_x11_orphans(self):
        """Clean up orphaned X11/VNC processes."""
        orphan_names = ['Xvfb', 'fluxbox', 'x11vnc', 'websockify', 'xterm']
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                proc_info = proc.info
                for orphan in orphan_names:
                    name_match = proc_info['name'] and orphan in proc_info['name']
                    cmd_match = proc_info['cmdline'] and any(
                        orphan in ' '.join(proc_info['cmdline']).lower() for _ in [1]
                    )
                    if name_match or cmd_match:
                        try:
                            os.kill(proc_info['pid'], signal.SIGTERM)
                        except ProcessLookupError:
                            pass
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
        logger.info("Cleaned up orphaned X11/VNC processes")

    def _force_kill_all_chrome_processes(self):
        """Force kill ALL Chrome processes."""
        logger.info("Force killing ALL Chrome processes...")
        for attempt in range(3):
            killed_count = 0
            signals_to_try = [signal.SIGKILL] if attempt == 2 else [signal.SIGTERM]

            for sig in signals_to_try:
                for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                    try:
                        proc_info = proc.info
                        if not proc_info['name']:
                            continue
                        if any(n in proc_info['name'].lower() for n in ['chrome', 'google-chrome']):
                            cmdline = ' '.join(proc_info['cmdline'] or [])
                            if any(
                                indicator in cmdline.lower()
                                for indicator in ['chrome', 'google-chrome', 'user-data-dir']
                            ):
                                try:
                                    os.kill(proc_info['pid'], sig)
                                    killed_count += 1
                                except ProcessLookupError:
                                    pass
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass

            logger.info(f"Killed {killed_count} Chrome process(es) (attempt {attempt + 1})")

            if killed_count == 0:
                break

            time.sleep(2)

        self._cleanup_x11_orphans()
        self._cleanup_singleton_locks()

    # =========================================================================
    # PROFILE MANAGEMENT
    # =========================================================================

    def create_profile_for_account(self, account_id: int, username: str) -> Dict[str, Any]:
        """Create a Chrome profile directory for an account."""
        try:
            if hasattr(account_id, 'item'):
                account_id = int(account_id)

            profile_id = f"account_{username}"
            profile_path = os.path.join(self.base_profile_dir, profile_id)

            os.makedirs(profile_path, exist_ok=True)

            mongodb_id = None
            if self.mongo_client:
                try:
                    db = self.mongo_client['messages_db']
                    existing = db.accounts.find_one({'profile_id': profile_id})
                    if existing:
                        db.accounts.update_one(
                            {'profile_id': profile_id},
                            {'$set': {
                                'postgres_account_id': account_id,
                                'username': username,
                                'profile_path': profile_path,
                                'updated_at': datetime.now(),
                                'is_active': True,
                            }}
                        )
                        mongodb_id = str(existing['_id'])
                    else:
                        result = db.accounts.insert_one({
                            'profile_id': profile_id,
                            'profile_type': 'local_chrome',
                            'profile_path': profile_path,
                            'postgres_account_id': account_id,
                            'username': username,
                            'created_at': datetime.now(),
                            'is_active': True,
                            'usage_count': 0,
                        })
                        mongodb_id = str(result.inserted_id)
                except Exception as e:
                    logger.warning(f"MongoDB error: {e}")

            return {
                'success': True,
                'profile_id': profile_id,
                'profile_path': profile_path,
                'mongodb_id': mongodb_id,
                'created_at': datetime.now(),
                'is_new': not os.path.exists(os.path.join(profile_path, 'Default')),
            }

        except Exception as e:
            return {'success': False, 'error': str(e)}

    def get_profile_path(self, username: str) -> str:
        """Get the profile path for a username."""
        return os.path.join(self.base_profile_dir, f"account_{username}")

    # =========================================================================
    # SESSION LIFECYCLE (with account_id)
    # =========================================================================

    def run_persistent_chrome(
        self,
        session_id: str,
        profile_path: str,
        username: str,
        account_id: int,   # NEW: store account ID
        survey_url: str = None,
        show_terminal: bool = True,
    ) -> Dict[str, Any]:
        """Start Chrome with persistent profile, now storing account_id."""
        if not survey_url or survey_url.strip() == "":
            survey_url = "https://mylocation.org/"
            logger.info(f"No URL provided, using default: {survey_url}")

        # Clean up previous session for this profile
        for sid, info in list(self.active_processes.items()):
            if info.get('profile_path') == profile_path:
                logger.info(f"Stopping existing session for this profile: {sid}")
                self.stop_session(sid)

        # Kill only Chrome processes using this specific profile
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                if 'chrome' in (proc.info.get('name') or '').lower():
                    if profile_path in ' '.join(proc.info.get('cmdline') or []):
                        os.kill(proc.info['pid'], signal.SIGTERM)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        time.sleep(2)

        self._cleanup_singleton_locks(profile_path)
        time.sleep(1)

        debug_port = self._get_next_available_port(9222, 9322)
        logger.info(f"Allocated debug port {debug_port} for session {session_id}")

        safe_username = "".join(c for c in username if c.isalnum() or c in "-_")
        account_cookie_script = f"/app/cookie_scripts/copy_cookies_{safe_username}.sh"

        bash_script = f"""#!/bin/bash

CHROME_PROFILE_DIR="{profile_path}"
SURVEY_URL="{survey_url}"
DEBUG_PORT={debug_port}

export DISPLAY=:99

echo "=== Chrome Session Starting ==="
echo "Profile : $CHROME_PROFILE_DIR"
echo "URL     : $SURVEY_URL"
echo "Display : $DISPLAY"
echo "Debug   : $DEBUG_PORT"
echo "==============================="

# Remove stale lock files
for lockfile in SingletonLock SingletonCookie SingletonSocket lockfile; do
    for target in "$CHROME_PROFILE_DIR/$lockfile" "$CHROME_PROFILE_DIR/Default/$lockfile"; do
        [ -e "$target" ] || [ -L "$target" ] && rm -f "$target" 2>/dev/null || true
    done
done

# Remove session restore files
for session_file in "Last Session" "Last Tabs" "Current Session" "Current Tabs"; do
    target="$CHROME_PROFILE_DIR/Default/$session_file"
    [ -e "$target" ] && rm -f "$target" 2>/dev/null || true
done

mkdir -p "$CHROME_PROFILE_DIR/Default"

# Kill any existing Chrome using ONLY this profile
pkill -f "chrome.*$CHROME_PROFILE_DIR" 2>/dev/null || true
sleep 2

# Start Chrome with unique debug port
google-chrome-stable \\
--no-sandbox \\
--disable-setuid-sandbox \\
--user-data-dir="$CHROME_PROFILE_DIR" \\
--remote-debugging-port=$DEBUG_PORT \\
--remote-debugging-address=0.0.0.0 \\
--remote-allow-origins=* \\
--no-first-run \\
--disable-session-crashed-bubble \\
--disable-restore-session-state \\
--disable-sync \\
--disable-default-apps \\
--disable-notifications \\
--disable-infobars \\
--disable-breakpad \\
--disable-dev-shm-usage \\
--lang=en-KE,en-US,en \\
--window-size=1280,720 \\
--window-position=0,0 \\
"$SURVEY_URL" &

CHROME_PID=$!
echo "Chrome PID : $CHROME_PID"
echo "VNC        : http://localhost:6080/vnc.html"
echo "Password   : secret"
echo "Debug Port : $DEBUG_PORT"

# Trap SIGTERM so Chrome can save state before exiting
trap "echo 'Received SIGTERM — shutting down Chrome gracefully'; kill -TERM $CHROME_PID; wait $CHROME_PID" SIGTERM SIGINT

wait $CHROME_PID

# Cleanup lock files on exit
for lockfile in SingletonLock SingletonCookie SingletonSocket lockfile; do
    for target in "$CHROME_PROFILE_DIR/$lockfile" "$CHROME_PROFILE_DIR/Default/$lockfile"; do
        [ -e "$target" ] || [ -L "$target" ] && rm -f "$target" 2>/dev/null || true
    done
done

echo "Chrome exited cleanly."
"""

        with tempfile.NamedTemporaryFile(delete=False, suffix=".sh", mode="w") as f:
            f.write(bash_script)
            script_path = f.name

        os.chmod(script_path, 0o755)

        proc = subprocess.Popen(
            ["bash", script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=os.setsid,
        )

        self.active_processes[session_id] = {
            "process": proc,
            "profile_path": profile_path,
            "username": username,
            "account_id": account_id,      # NEW: store account ID
            "script_path": script_path,
            "account_cookie_script": account_cookie_script,
            "started_at": datetime.now(),
            "has_terminal": show_terminal,
            "debug_port": debug_port,
            "startup_urls": [survey_url],
            "session_url": survey_url,
        }

        time.sleep(5)

        logger.info(f"Chrome session started : {session_id}")
        logger.info(f"  URL     : {survey_url}")
        logger.info(f"  Profile : {profile_path}")
        logger.info(f"  VNC     : http://localhost:6080/vnc.html")
        logger.info(f"  Debug   : localhost:{debug_port}")

        return {
            "success": True,
            "session_id": session_id,
            "vnc_url": "http://localhost:6080/vnc.html",
            "debug_port": debug_port,
            "profile_path": profile_path,
            "account_cookie_script": account_cookie_script,
            "has_terminal": show_terminal,
            "startup_urls": [survey_url],
            "message": f"Chrome started — URL: {survey_url} — VNC: http://localhost:6080/vnc.html (password: secret)",
        }

    def _extract_cookies_from_profile(self, profile_path: str, account_id: int) -> Dict[str, Any]:
        """
        Extract all cookies from the Chrome profile's Cookies database.
        Returns a dict with success status and the cookie JSON list.
        """
        try:
            cookies_db = os.path.join(profile_path, 'Default', 'Cookies')
            if not os.path.exists(cookies_db):
                logger.warning(f"Cookie database not found: {cookies_db}")
                return {'success': False, 'error': 'Cookie database not found'}

            # Copy the database to avoid locking issues
            temp_db = cookies_db + '.temp'
            shutil.copy2(cookies_db, temp_db)

            conn = sqlite3.connect(temp_db)
            conn.text_factory = bytes
            cursor = conn.cursor()
            cursor.execute("SELECT name, value, host_key, path, is_secure, is_httponly, has_expires, expires_utc FROM cookies")
            cookies = []
            for row in cursor.fetchall():
                name = row[0].decode('utf-8')
                value = row[1].decode('utf-8')
                domain = row[2].decode('utf-8')
                path = row[3].decode('utf-8')
                secure = bool(row[4])
                http_only = bool(row[5])
                expires_utc = row[7]

                expiration_date = None
                if expires_utc and expires_utc != 0:
                    # Chrome time base: 1601-01-01 in microseconds
                    seconds_since_1601 = expires_utc / 1_000_000
                    # Convert to Unix timestamp
                    chrome_epoch = dt.datetime(1601, 1, 1)
                    unix_epoch = dt.datetime(1970, 1, 1)
                    delta = (chrome_epoch - unix_epoch).total_seconds()
                    expiration_date = seconds_since_1601 - delta

                cookies.append({
                    'name': name,
                    'value': value,
                    'domain': domain,
                    'path': path,
                    'secure': secure,
                    'httpOnly': http_only,
                    'sameSite': 'Lax',      # default; Chrome doesn't store in cookies table
                    'expirationDate': expiration_date,
                })
            conn.close()
            os.remove(temp_db)

            logger.info(f"Extracted {len(cookies)} cookies from profile {profile_path}")
            return {'success': True, 'cookies': cookies, 'count': len(cookies)}

        except Exception as e:
            logger.error(f"Cookie extraction failed: {e}")
            return {'success': False, 'error': str(e)}

    def _store_cookies_via_db_manager(self, account_id: int, cookies: list) -> Dict[str, Any]:
        """Store cookies in the account_cookies table using db_manager."""
        if not self.db_manager:
            return {'success': False, 'error': 'No db_manager available'}

        try:
            # Deactivate previous cookies
            deactivate_query = """
            UPDATE account_cookies
            SET is_active = FALSE, updated_at = CURRENT_TIMESTAMP
            WHERE account_id = %s AND is_active = TRUE
            """
            self.db_manager.execute_query(deactivate_query, (account_id,))

            cookie_json_string = json.dumps(cookies)
            cookie_count = len(cookies)

            insert_query = """
            INSERT INTO account_cookies (
                account_id, cookie_data, cookie_count,
                uploaded_at, updated_at, is_active, cookie_source
            )
            VALUES (%s, %s::jsonb, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP, TRUE, 'auto_sync')
            RETURNING cookie_id
            """
            result = self.db_manager.execute_query(insert_query, (account_id, cookie_json_string, cookie_count), fetch=True)

            if not result:
                raise Exception("INSERT returned no rows")

            first_row = result[0]
            cookie_id = first_row[0] if isinstance(first_row, tuple) else first_row.get('cookie_id')

            # Update accounts table
            update_account_query = """
            UPDATE accounts
            SET has_cookies = TRUE, cookies_last_updated = CURRENT_TIMESTAMP
            WHERE account_id = %s
            """
            self.db_manager.execute_query(update_account_query, (account_id,))

            logger.info(f"Stored {cookie_count} cookies for account {account_id} (auto-sync)")
            return {'success': True, 'cookie_id': cookie_id, 'cookie_count': cookie_count}

        except Exception as e:
            logger.error(f"Failed to store cookies: {e}")
            return {'success': False, 'error': str(e)}

    def stop_session(self, session_id: str) -> Dict[str, Any]:
        """
        Stop a Chrome session gracefully, then extract and store cookies.
        """
        proc_info = self.active_processes.get(session_id)
        if not proc_info:
            return {"success": False, "error": "Session not found"}

        profile_path = proc_info.get('profile_path', '')
        debug_port = proc_info.get('debug_port', 9222)
        account_id = proc_info.get('account_id')   # NEW: get stored account ID

        logger.info(f"Stopping Chrome session gracefully: {session_id}")

        try:
            # 1. Ask Chrome to close gracefully via CDP
            try:
                import urllib.request
                urllib.request.urlopen(
                    f"http://localhost:{debug_port}/json/close",
                    timeout=3
                )
            except Exception:
                pass

            # 2. Send SIGTERM to the process group
            proc = proc_info['process']
            if proc.poll() is None:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    logger.info("Sent SIGTERM to process group")
                except ProcessLookupError:
                    pass

            # 3. Wait up to 10 seconds for Chrome to save and exit gracefully
            deadline = time.time() + 10
            while time.time() < deadline:
                chrome_still_running = False
                for p in psutil.process_iter(['name', 'cmdline']):
                    try:
                        if 'chrome' in (p.info.get('name') or '').lower():
                            if profile_path in ' '.join(p.info.get('cmdline') or []):
                                chrome_still_running = True
                                break
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                if not chrome_still_running:
                    logger.info("Chrome exited gracefully ✅")
                    break
                time.sleep(0.5)
            else:
                # 4. Force kill if still running
                logger.warning("Chrome did not exit gracefully — force killing")
                for proc_item in psutil.process_iter(['pid', 'name', 'cmdline']):
                    try:
                        if 'chrome' in (proc_item.info.get('name') or '').lower():
                            if profile_path in ' '.join(proc_item.info.get('cmdline') or []):
                                os.kill(proc_item.info['pid'], signal.SIGKILL)
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        pass
                time.sleep(2)

            # 5. Clean up lock files
            if profile_path:
                removed = self._cleanup_singleton_locks(profile_path)
                logger.info(f"Lock cleanup: removed {len(removed)} file(s)")

            # 6. Remove temp script
            script_path = proc_info.get('script_path', '')
            if script_path and os.path.exists(script_path):
                try:
                    os.remove(script_path)
                except Exception as e:
                    logger.warning(f"Could not remove script {script_path}: {e}")

            # 7. Extract and store cookies if we have account_id
            if account_id and profile_path:
                logger.info(f"Extracting cookies from profile for account {account_id}")
                extract_result = self._extract_cookies_from_profile(profile_path, account_id)
                if extract_result['success']:
                    store_result = self._store_cookies_via_db_manager(account_id, extract_result['cookies'])
                    if store_result['success']:
                        logger.info(f"✅ Auto-synced {store_result['cookie_count']} cookies for account {account_id}")
                    else:
                        logger.warning(f"Failed to store extracted cookies: {store_result.get('error')}")
                else:
                    logger.warning(f"Cookie extraction failed: {extract_result.get('error')}")
            else:
                logger.info("No account_id or profile_path, skipping cookie sync")

            # 8. Update MongoDB
            if self.mongo_client:
                try:
                    db = self.mongo_client['messages_db']
                    db.browser_sessions.update_one(
                        {'session_id': session_id},
                        {'$set': {
                            'is_active': False,
                            'ended_at': datetime.now(),
                            'session_status': 'stopped'
                        }}
                    )
                except Exception as e:
                    logger.warning(f"MongoDB update failed: {e}")

            # 9. Remove from active processes
            del self.active_processes[session_id]

            logger.info(f"✅ Session {session_id} stopped — profile saved at {profile_path}")
            return {
                "success": True,
                "message": "Chrome closed gracefully — profile state saved",
                "profile_path": profile_path,
            }

        except Exception as e:
            logger.error(f"Failed to stop session {session_id}: {e}")
            return {"success": False, "error": str(e)}

    # =========================================================================
    # UTILITY
    # =========================================================================

    def list_profiles(self) -> Dict[str, Any]:
        """List all Chrome profiles."""
        profiles = []
        for p in os.listdir(self.base_profile_dir):
            path = os.path.join(self.base_profile_dir, p)
            if os.path.isdir(path):
                profiles.append({
                    'profile_id': p,
                    'profile_path': path,
                    'created': datetime.fromtimestamp(os.path.getctime(path)),
                })
        return {'success': True, 'profiles': profiles}

    def list_active_sessions(self) -> Dict[str, Any]:
        """List all active Chrome sessions."""
        active = []
        for sid, info in self.active_processes.items():
            active.append({
                'session_id': sid,
                'profile_path': info['profile_path'],
                'username': info.get('username'),
                'account_id': info.get('account_id'),  # NEW: include account ID
                'debug_port': info.get('debug_port', 9222),
                'has_terminal': info.get('has_terminal', False),
                'startup_urls': info.get('startup_urls', []),
                'status': 'running' if info['process'].poll() is None else 'stopped',
            })
        return {'success': True, 'sessions': active}

    def _get_next_available_port(self, start_port: int = 9222, end_port: int = 9322) -> int:
        """Get the next available port in range."""
        import socket
        for port in range(start_port, end_port + 1):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('', port))
                    return port
            except OSError:
                continue
        raise Exception("No available ports in range")