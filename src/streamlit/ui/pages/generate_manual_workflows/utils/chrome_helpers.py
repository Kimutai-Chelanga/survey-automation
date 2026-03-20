# src/streamlit/ui/pages/generate_manual_workflows/utils/chrome_helpers.py
import logging
import subprocess
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


def ensure_chrome_running(profile_path: str, debug_port: int = 9222) -> bool:
    if _reachable(debug_port):
        return True
    return _start_chrome(profile_path, debug_port)


def _reachable(port: int) -> bool:
    try:
        import urllib.request
        with urllib.request.urlopen(f"http://localhost:{port}/json/version", timeout=2) as r:
            return r.status == 200
    except:
        return False


def _start_chrome(profile_path: str, debug_port: int) -> bool:
    for binary in ["google-chrome","google-chrome-stable","chromium","chromium-browser",
                   "/usr/bin/google-chrome","/usr/bin/chromium"]:
        try:
            subprocess.run(["which", binary], check=True, capture_output=True)
            break
        except subprocess.CalledProcessError:
            binary = None
    if not binary:
        logger.error("No Chrome binary found")
        return False

    Path(profile_path).mkdir(parents=True, exist_ok=True)
    try:
        subprocess.Popen([
            binary,
            f"--remote-debugging-port={debug_port}",
            f"--user-data-dir={profile_path}",
            "--no-first-run","--no-default-browser-check",
            "--disable-notifications","--start-maximized",
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    except Exception as exc:
        logger.error(f"Failed to start Chrome: {exc}")
        return False

    for _ in range(20):
        time.sleep(0.5)
        if _reachable(debug_port):
            return True
    return False


def get_debug_port_for_account(session_state, account_id: int) -> Optional[int]:
    """Return the CDP debug port for the active Chrome session of account_id."""
    for info in session_state.get("local_chrome_sessions", {}).values():
        if info.get("account_id") == account_id:
            port = info.get("debug_port")
            if port:
                return port
    return None


def get_debug_port_any(session_state) -> Optional[int]:
    """Return the first available CDP debug port."""
    for info in session_state.get("local_chrome_sessions", {}).values():
        p = info.get("debug_port")
        if p:
            return p
    return None