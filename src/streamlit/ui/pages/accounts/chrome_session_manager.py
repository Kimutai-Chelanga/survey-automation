import subprocess
import os
import time
import logging
import tempfile
import signal
from datetime import datetime
from typing import Dict, Any, Optional
import psutil
import json
import base64
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
        """
        Remove all Chrome Singleton lock files.

        This is the permanent fix for the cross-container lock problem:
        When Chrome sessions are stopped from Streamlit (or crash), they
        leave behind SingletonLock / SingletonCookie / SingletonSocket files
        that embed the creating container's hostname. The next container to
        run the DAG sees a "foreign" lock and Chrome refuses to start even
        though no Chrome process is actually running.

        We clean up:
          - <profile>/SingletonLock|Cookie|Socket
          - <profile>/Default/SingletonLock|Cookie|Socket
          - <profile>/lockfile  (Chrome's internal lock)
          - <profile>/Default/Last Session|Last Tabs  (restore dialog blocker)
          - Any .org.chromium.* / .com.google.Chrome.* temp lock files
        """
        dirs_to_clean = []

        if profile_path:
            dirs_to_clean.append(profile_path)
            dirs_to_clean.append(os.path.join(profile_path, 'Default'))
        else:
            # Clean all profiles under base dir
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

            # Remove Singleton and lockfile entries
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

            # Remove session files from Default/ only (to suppress restore dialog)
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

        # Remove stale .org.chromium / .com.google temp lock files anywhere under base_profile_dir
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

    # =========================================================================
    # INTERNAL HELPERS
    # =========================================================================

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
        """
        Force kill ALL Chrome processes (windows, tabs, helpers) and then
        clean up the lock files they leave behind.

        This is called both when stopping a session and before starting a new
        one to ensure a clean slate.
        """
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

        # Clean up orphaned X11/VNC processes
        self._cleanup_x11_orphans()

        # ── CRITICAL: remove lock files left behind by the killed processes ──
        # Chrome writes Singleton* files when it starts. If killed abruptly it
        # never cleans them up. The next Chrome invocation (or DAG run) will
        # see these stale files and refuse to start.
        self._cleanup_singleton_locks()

    # =========================================================================
    # PROFILE MANAGEMENT
    # =========================================================================

    def create_profile_for_account(self, account_id: int, username: str) -> Dict[str, Any]:
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
        return os.path.join(self.base_profile_dir, f"account_{username}")

    # =========================================================================
    # SESSION LIFECYCLE
    # =========================================================================

    def run_persistent_chrome(
        self,
        session_id: str,
        profile_path: str,
        username: str,
        start_url: str = "about:blank",   # ignored — 3 tabs always open
        show_terminal: bool = True,
        terminal_width: int = 600,
        terminal_height: int = 400,
    ) -> Dict[str, Any]:
        """
        Start Chrome with 3 tabs + terminal + account cookie script.

        Opens:
          1. Automa extension (Chrome Web Store)
          2. EditThisCookie extension (Chrome Web Store)
          3. X.com home page

        Also provides:
          - Interactive terminal for cookie management
          - Remote debugging on port 9222
          - VNC access on port 6080
          - Account-specific cookie script
        """
        # ── Enforce single session: stop any existing session first ──
        for sid in list(self.active_processes.keys()):
            self.stop_session(sid)

        self._kill_all_chrome_everywhere()

        # ── Clean all singleton locks BEFORE starting Chrome ──
        # This handles the case where a previous session (possibly in a
        # different container) left lock files behind.
        self._cleanup_singleton_locks(profile_path)

        time.sleep(3)

        # ── Startup URLs ──
        startup_url_1 = "https://chromewebstore.google.com/detail/automa/infppggnoaenmfagbfknfkancpbljcca"
        startup_url_2 = "https://chromewebstore.google.com/detail/editthiscookie/fngmhnnpilhplaeedifhccceomclgfbg"
        startup_url_3 = "https://x.com/home"

        # ── Account cookie script path ──
        safe_username = "".join(c for c in username if c.isalnum() or c in "-_")
        account_cookie_script = f"/app/cookie_scripts/copy_cookies_{safe_username}.sh"

        # ── Optional terminal window ──
        terminal_config = ""
        if show_terminal:
            terminal_config = f"""
# Open interactive terminal window for account management
xterm -title "Account: {username}" \\
    -geometry 80x25+10+10 \\
    -fa 'Monospace' -fs 10 \\
    -e bash -c "
        echo '=========================================='
        echo 'Chrome Session Terminal'
        echo 'Account: {username}'
        echo 'Profile: {profile_path}'
        echo '=========================================='
        echo ''
        echo 'Chrome started with 3 tabs:'
        echo '  1. Automa Extension'
        echo '  2. EditThisCookie Extension'
        echo '  3. X.com Home'
        echo ''
        echo 'Account Cookie Script:'
        echo '  {account_cookie_script}'
        echo ''
        echo 'Quick Commands:'
        echo '  cd /app/cookie_scripts'
        echo '  ls -la'
        echo '  ./{os.path.basename(account_cookie_script)}'
        echo ''
        echo 'Press Ctrl+C to close this terminal'
        echo '=========================================='
        cd /app/cookie_scripts
        bash
    " &
"""

        bash_script = f"""#!/bin/bash
set -e

CHROME_PROFILE_DIR="{profile_path}"
SESSION_ID="{session_id}"
ACCOUNT_USERNAME="{username}"
ACCOUNT_COOKIE_SCRIPT="{account_cookie_script}"

cleanup() {{
    pkill -TERM -f "chrome.*$CHROME_PROFILE_DIR" || true
    sleep 5
    pkill -KILL -f chrome || true
    pkill -f "xterm.*$ACCOUNT_USERNAME" || true
    pkill -f "Xvfb|fluxbox|x11vnc|websockify" || true

    # ── Clean up lock files on exit so the DAG won't be blocked ──
    for lockfile in SingletonLock SingletonCookie SingletonSocket lockfile; do
        for target in "$CHROME_PROFILE_DIR/$lockfile" "$CHROME_PROFILE_DIR/Default/$lockfile"; do
            [ -e "$target" ] || [ -L "$target" ] && rm -f "$target" 2>/dev/null || true
        done
    done

    exit 0
}}

trap cleanup SIGINT SIGTERM EXIT

# ── Ensure profile directory structure exists ──
mkdir -p "$CHROME_PROFILE_DIR/Default"

# ── Remove stale session / lock files (prevents restore dialog + lock errors) ──
for lockfile in SingletonLock SingletonCookie SingletonSocket lockfile; do
    for target in "$CHROME_PROFILE_DIR/$lockfile" "$CHROME_PROFILE_DIR/Default/$lockfile"; do
        [ -e "$target" ] || [ -L "$target" ] && rm -f "$target" 2>/dev/null || true
    done
done

for session_file in "Last Session" "Last Tabs" "Current Session" "Current Tabs"; do
    target="$CHROME_PROFILE_DIR/Default/$session_file"
    [ -e "$target" ] && rm -f "$target" 2>/dev/null || true
done

# ── Write Preferences to force 3-tab startup ──
cat > "$CHROME_PROFILE_DIR/Default/Preferences" << 'PREFEOF'
{{
  "session": {{
    "restore_on_startup": 5,
    "startup_urls": [
      "{startup_url_1}",
      "{startup_url_2}",
      "{startup_url_3}"
    ]
  }},
  "profile": {{
    "exit_type": "Normal"
  }},
  "browser": {{
    "show_home_button": true
  }}
}}
PREFEOF

# ── Start X server ──
Xvfb :99 -screen 0 1280x720x24 -ac &
sleep 3
export DISPLAY=:99

# ── Start window manager ──
fluxbox &
sleep 2

# ── Start VNC ──
x11vnc -display :99 -forever -shared -passwd secret -bg
websockify --web /usr/share/novnc 6080 localhost:5900 &

{terminal_config}

# ── Start Chrome with all 3 URLs on command line ──
google-chrome-stable \\
  --no-sandbox \\
  --disable-setuid-sandbox \\
  --user-data-dir="$CHROME_PROFILE_DIR" \\
  --remote-debugging-port=9222 \\
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
  --window-size=1280,720 \\
  --window-position=0,0 \\
  --new-window \\
  "{startup_url_1}" \\
  "{startup_url_2}" \\
  "{startup_url_3}" &

CHROME_PID=$!
echo "Chrome started with PID $CHROME_PID"
echo "  Tab 1: Automa Extension"
echo "  Tab 2: EditThisCookie Extension"
echo "  Tab 3: X.com Home"

wait $CHROME_PID

# Cleanup on natural exit
for lockfile in SingletonLock SingletonCookie SingletonSocket lockfile; do
    for target in "$CHROME_PROFILE_DIR/$lockfile" "$CHROME_PROFILE_DIR/Default/$lockfile"; do
        [ -e "$target" ] || [ -L "$target" ] && rm -f "$target" 2>/dev/null || true
    done
done
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
            "script_path": script_path,
            "account_cookie_script": account_cookie_script,
            "started_at": datetime.now(),
            "has_terminal": show_terminal,
            "debug_port": 9222,
            "startup_urls": [startup_url_1, startup_url_2, startup_url_3],
        }

        # Give Chrome a moment to start
        time.sleep(5)

        logger.info(f"Chrome session started: {session_id}")
        logger.info(f"  Tabs: Automa, EditThisCookie, X.com")
        logger.info(f"  VNC:  http://localhost:6080/vnc.html")
        logger.info(f"  Debug: localhost:9222")

        return {
            "success": True,
            "session_id": session_id,
            "vnc_url": "http://localhost:6080/vnc.html",
            "debug_port": 9222,
            "profile_path": profile_path,
            "account_cookie_script": account_cookie_script,
            "has_terminal": show_terminal,
            "startup_urls": [startup_url_1, startup_url_2, startup_url_3],
            "message": (
                f"Chrome started with 3 tabs: Automa, EditThisCookie, X.com"
                + (" and interactive terminal" if show_terminal else "")
            ),
        }

    def stop_session(self, session_id: str) -> Dict[str, Any]:
        """
        Stop a Chrome session and ensure ALL windows/tabs are closed and
        ALL lock files are cleaned up so the next DAG run can start Chrome
        without Singleton lock errors.
        """
        proc_info = self.active_processes.get(session_id)
        if not proc_info:
            return {"success": False, "error": "Session not found"}

        logger.info(f"Stopping Chrome session: {session_id}")
        profile_path = proc_info.get('profile_path', '')

        try:
            proc = proc_info['process']

            # ── 1. Terminate the bash script's process group ──
            if proc.poll() is None:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    logger.info(f"Sent SIGTERM to process group {proc.pid}")
                except ProcessLookupError:
                    pass

            # Give the trap handler a moment to run (it does its own cleanup)
            time.sleep(3)

            # ── 2. Force kill any surviving Chrome processes ──
            # _force_kill_all_chrome_processes() calls _cleanup_singleton_locks()
            # internally, so lock files are cleaned up as part of this step.
            self._force_kill_all_chrome_processes()

            # ── 3. Explicit lock cleanup for THIS profile ──
            # Belt-and-suspenders: in case _force_kill_all_chrome_processes
            # missed anything specific to this profile directory.
            if profile_path:
                removed = self._cleanup_singleton_locks(profile_path)
                if removed:
                    logger.info(
                        f"Explicit lock cleanup for {profile_path}: "
                        f"removed {len(removed)} file(s)"
                    )

            # ── 4. Clean up the temp bash script ──
            script_path = proc_info.get('script_path', '')
            if script_path and os.path.exists(script_path):
                try:
                    os.remove(script_path)
                except Exception as e:
                    logger.warning(f"Could not remove script {script_path}: {e}")

            # ── 5. Remove from active processes ──
            del self.active_processes[session_id]

            # ── 6. Verify no lock files remain ──
            remaining = []
            if profile_path:
                for check_dir in [profile_path, os.path.join(profile_path, 'Default')]:
                    for name in ['SingletonLock', 'SingletonCookie', 'SingletonSocket']:
                        target = os.path.join(check_dir, name)
                        if os.path.exists(target) or os.path.islink(target):
                            remaining.append(target)

            if remaining:
                logger.warning(
                    f"⚠ {len(remaining)} lock file(s) still present after cleanup: {remaining}"
                )
            else:
                logger.info(f"✅ Session {session_id} stopped — no lock files remain")

            return {
                "success": True,
                "message": "All Chrome windows closed and lock files cleaned up",
                "profile_path": profile_path,
                "lock_files_remaining": remaining,
            }

        except Exception as e:
            logger.error(f"Failed to stop session {session_id}: {e}")
            return {"success": False, "error": str(e)}

    # =========================================================================
    # UTILITY
    # =========================================================================

    def list_profiles(self) -> Dict[str, Any]:
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
        active = []
        for sid, info in self.active_processes.items():
            active.append({
                'session_id': sid,
                'profile_path': info['profile_path'],
                'username': info.get('username'),
                'debug_port': info.get('debug_port', 9222),
                'has_terminal': info.get('has_terminal', False),
                'startup_urls': info.get('startup_urls', []),
                'status': 'running' if info['process'].poll() is None else 'stopped',
            })
        return {'success': True, 'sessions': active}

    def _get_next_available_port(self, start_port: int = 9222, end_port: int = 9322) -> int:
        import socket
        for port in range(start_port, end_port + 1):
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                    s.bind(('', port))
                    return port
            except OSError:
                continue
        raise Exception("No available ports in range")
