"""
Screenshot handling: capture, save to persistent directory, register in session state and DB.
"""

import os
from datetime import datetime
from typing import Optional

import streamlit as st

from .constants import ALLOWED_SCREENSHOTS, SCREENSHOT_LABELS
from .db_utils import save_batch_screenshot

# Persistent directory — mapped to a Docker volume in docker-compose.yml
_SCREENSHOTS_DIR = os.environ.get("RECORDINGS_DIR", "/app/recordings")


def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


async def take_screenshot(
    page,
    label: str,
    batch_id: str,
    account_id: int,
    site_id: int,
    survey_num: int = 0,
    log_func=None,
) -> Optional[str]:
    """
    Take a screenshot, save it to persistent directory, register in session state,
    and store metadata in database.
    """
    if label not in ALLOWED_SCREENSHOTS:
        return None

    try:
        img_bytes = await page.screenshot(type="png", full_page=False)

        ts = datetime.now().strftime("%H%M%S")
        safe_label = label.replace(" ", "_")
        if survey_num:
            filename = f"{batch_id}_{safe_label}_survey{survey_num}_{ts}.png"
        else:
            filename = f"{batch_id}_{safe_label}_{ts}.png"

        save_dir = _ensure_dir(os.path.join(_SCREENSHOTS_DIR, batch_id))
        save_path = os.path.join(save_dir, filename)

        with open(save_path, "wb") as f:
            f.write(img_bytes)

        # Register in session state
        if "batches" in st.session_state and batch_id in st.session_state.batches:
            st.session_state.batches[batch_id].setdefault("screenshots", [])
            st.session_state.batches[batch_id]["screenshots"].append({
                "path": save_path,
                "label": SCREENSHOT_LABELS.get(label, label),
                "stage": label,
                "survey_num": survey_num,
                "timestamp": ts,
            })

        # Store metadata in database
        save_batch_screenshot(
            batch_id=batch_id,
            account_id=account_id,
            site_id=site_id,
            survey_num=survey_num,
            stage=label,
            label=SCREENSHOT_LABELS.get(label, label),
            file_path=save_path
        )

        if log_func:
            log_func(
                f"📸 Screenshot: {SCREENSHOT_LABELS.get(label, label)} → {save_path}",
                batch_id=batch_id,
            )

        return save_path

    except Exception as e:
        if log_func:
            log_func(f"Screenshot failed ({label}): {e}", "WARNING", batch_id=batch_id)
        return None