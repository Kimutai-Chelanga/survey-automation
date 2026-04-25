"""
Screenshot handling: capture, save to persistent directory, register in session state.

WHY THE OLD VERSION SHOWED 0 SCREENSHOTS
-----------------------------------------
The old take_screenshot() saved to /tmp/ (ephemeral, not web-served) and
returned the path but never stored it anywhere the UI could find it.
display_batch_details() reads st.session_state.batches[batch_id]["screenshots"],
which was always an empty list.

FIX
---
1. Save screenshots to RECORDINGS_DIR (persistent Docker volume, survives reruns).
2. After saving, append the path to st.session_state.batches[batch_id]["screenshots"]
   so display_batch_details() can find and render them.
"""

import os
from datetime import datetime
from typing import Optional

import streamlit as st

from .constants import ALLOWED_SCREENSHOTS, SCREENSHOT_LABELS

# Persistent directory — mapped to a Docker volume in docker-compose.yml
_SCREENSHOTS_DIR = os.environ.get("RECORDINGS_DIR", "/app/recordings")


def _ensure_dir(path: str) -> str:
    os.makedirs(path, exist_ok=True)
    return path


async def take_screenshot(
    page,
    label: str,
    batch_id: str,
    survey_num: int = 0,
    log_func=None,
) -> Optional[str]:
    """
    Take a screenshot, save it to a persistent directory, register it in
    st.session_state.batches[batch_id]["screenshots"], and return the path.

    Parameters
    ----------
    page       : Playwright Page object
    label      : short key identifying the screenshot stage (e.g. "01_survey_tab_open")
    batch_id   : the current run's batch ID
    survey_num : survey number within the batch (0 = pre-survey)
    log_func   : optional callable(msg, level?, batch_id?) for structured logging
    """
    if label not in ALLOWED_SCREENSHOTS:
        return None

    try:
        img_bytes = await page.screenshot(type="png", full_page=False)

        # Build a deterministic filename inside the persistent recordings volume
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

        # ── Register in session state so the UI can render it ──────────────
        if "batches" in st.session_state and batch_id in st.session_state.batches:
            st.session_state.batches[batch_id].setdefault("screenshots", [])
            st.session_state.batches[batch_id]["screenshots"].append({
                "path":  save_path,
                "label": SCREENSHOT_LABELS.get(label, label),
                "stage": label,
                "survey_num": survey_num,
                "timestamp": ts,
            })

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