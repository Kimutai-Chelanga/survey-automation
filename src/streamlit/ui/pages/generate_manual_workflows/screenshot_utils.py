"""
Screenshot handling: capture and store to disk.
"""

import os
import tempfile
from typing import Optional

from .constants import ALLOWED_SCREENSHOTS, SCREENSHOT_LABELS


async def take_screenshot(page, label: str, batch_id: str, survey_num: int = 0, log_func=None) -> Optional[str]:
    """Take screenshot, save to temp file, return path."""
    if label not in ALLOWED_SCREENSHOTS:
        return None
    try:
        img_bytes = await page.screenshot(type="png", full_page=False)
        with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
            tmp.write(img_bytes)
            tmp_path = tmp.name
        if log_func:
            log_func(f"📸 Screenshot: {SCREENSHOT_LABELS.get(label, label)} saved to {tmp_path}", batch_id=batch_id)
        return tmp_path
    except Exception as e:
        if log_func:
            log_func(f"Screenshot failed ({label}): {e}", "WARNING", batch_id=batch_id)
        return None