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

        Clean up:
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

        # Remove lock files left behind
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
    # SESSION LIFECYCLE
    # =========================================================================

    def run_persistent_chrome(
        self,
        session_id: str,
        profile_path: str,
        username: str,
        survey_url: str = "https://example-survey.com",
        show_terminal: bool = True,
    ) -> Dict[str, Any]:
        """
        Start Chrome with a single tab pointing to survey_url.
        Xvfb/VNC/fluxbox are already running from start.sh — do NOT restart them.
        """
        # Stop any existing sessions first
        for sid in list(self.active_processes.keys()):
            self.stop_session(sid)

        # Kill any lingering Chrome processes
        self._kill_all_chrome_everywhere()

        # Clean singleton locks for this profile
        self._cleanup_singleton_locks(profile_path)

        time.sleep(2)

        safe_username = "".join(c for c in username if c.isalnum() or c in "-_")
        account_cookie_script = f"/app/cookie_scripts/copy_cookies_{safe_username}.sh"

        bash_script = f"""#!/bin/bash

    CHROME_PROFILE_DIR="{profile_path}"
    SURVEY_URL="{survey_url}"

    # Use the already-running display from start.sh
    export DISPLAY=:99

    echo "=== Chrome Session Starting ==="
    echo "Profile : $CHROME_PROFILE_DIR"
    echo "URL     : $SURVEY_URL"
    echo "Display : $DISPLAY"
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

    # Ensure profile directory exists
    mkdir -p "$CHROME_PROFILE_DIR/Default"

    # Kill any existing Chrome using this profile
    pkill -f "chrome.*$CHROME_PROFILE_DIR" 2>/dev/null || true
    sleep 2

    # Start Chrome
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
    --disable-dev-shm-usage \\
    --lang=en-KE,en-US,en \\
    --window-size=1280,720 \\
    --window-position=0,0 \\
    "$SURVEY_URL" &

    CHROME_PID=$!
    echo "Chrome PID : $CHROME_PID"
    echo "VNC        : http://localhost:6080/vnc.html"
    echo "Password   : secret"

    wait $CHROME_PID

    # Cleanup lock files on exit
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
            "startup_urls": [survey_url],
        }

        # Give Chrome time to start
        time.sleep(5)

        logger.info(f"Chrome session started : {session_id}")
        logger.info(f"  URL     : {survey_url}")
        logger.info(f"  Profile : {profile_path}")
        logger.info(f"  VNC     : http://localhost:6080/vnc.html")
        logger.info(f"  Debug   : localhost:9222")

        return {
            "success": True,
            "session_id": session_id,
            "vnc_url": "http://localhost:6080/vnc.html",
            "debug_port": 9222,
            "profile_path": profile_path,
            "account_cookie_script": account_cookie_script,
            "has_terminal": show_terminal,
            "startup_urls": [survey_url],
            "message": f"Chrome started — URL: {survey_url} — VNC: http://localhost:6080/vnc.html (password: secret)",
        }
    def stop_session(self, session_id: str) -> Dict[str, Any]:
        """
        Stop a Chrome session and ensure ALL windows/tabs are closed and
        ALL lock files are cleaned up.
        """
        proc_info = self.active_processes.get(session_id)
        if not proc_info:
            return {"success": False, "error": "Session not found"}

        logger.info(f"Stopping Chrome session: {session_id}")
        profile_path = proc_info.get('profile_path', '')

        try:
            proc = proc_info['process']

            # Terminate the bash script's process group
            if proc.poll() is None:
                try:
                    os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                    logger.info(f"Sent SIGTERM to process group {proc.pid}")
                except ProcessLookupError:
                    pass

            # Give the trap handler a moment to run
            time.sleep(3)

            # Force kill any surviving Chrome processes
            self._force_kill_all_chrome_processes()

            # Explicit lock cleanup for THIS profile
            if profile_path:
                removed = self._cleanup_singleton_locks(profile_path)
                if removed:
                    logger.info(
                        f"Explicit lock cleanup for {profile_path}: "
                        f"removed {len(removed)} file(s)"
                    )

            # Clean up the temp bash script
            script_path = proc_info.get('script_path', '')
            if script_path and os.path.exists(script_path):
                try:
                    os.remove(script_path)
                except Exception as e:
                    logger.warning(f"Could not remove script {script_path}: {e}")

            # Remove from active processes
            del self.active_processes[session_id]

            # Verify no lock files remain
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