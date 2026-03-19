# src/streamlit/ui/pages/generate_manual_workflows/utils/chrome_helpers.py
"""
Chrome helpers for extraction
"""

import os
import subprocess
import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

def ensure_chrome_running(profile_path: str, debug_port: int = 9222) -> bool:
    """
    Ensure Chrome is running with remote debugging enabled
    
    Returns:
        True if Chrome is running and ready
    """
    # Check if Chrome is already running
    try:
        response = requests.get(f'http://localhost:{debug_port}/json/version', timeout=3)
        if response.status_code == 200:
            logger.info(f"Chrome already running on port {debug_port}")
            return True
    except:
        pass
    
    # Start Chrome
    logger.info(f"Starting Chrome with profile: {profile_path}")
    
    cmd = [
        'google-chrome-stable',
        f'--user-data-dir={profile_path}',
        f'--remote-debugging-port={debug_port}',
        '--remote-debugging-address=0.0.0.0',
        '--no-first-run',
        '--no-default-browser-check',
        '--disable-session-crashed-bubble',
        '--disable-restore-session-state',
        '--disable-gpu',
        '--disable-background-timer-throttling',
        '--disable-renderer-backgrounding',
        '--about:blank'
    ]
    
    try:
        subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Wait for Chrome to start
        for i in range(10):
            time.sleep(1)
            try:
                response = requests.get(f'http://localhost:{debug_port}/json/version', timeout=2)
                if response.status_code == 200:
                    logger.info(f"Chrome started successfully after {i+1}s")
                    return True
            except:
                continue
                
        logger.error("Chrome failed to start within timeout")
        return False
        
    except Exception as e:
        logger.error(f"Failed to start Chrome: {e}")
        return False