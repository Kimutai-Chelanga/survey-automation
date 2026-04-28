"""
Generate a collage of screenshots with arrows showing the flow.
"""

import os
from datetime import datetime
from typing import List, Dict, Optional
from PIL import Image, ImageDraw

import streamlit as st

from .constants import SCREENSHOT_LABELS


def create_collage(screenshot_paths: List[str], arrow_color: str = "red", output_dir: str = None) -> Optional[str]:
    """
    Create a vertical collage of screenshots, with arrows between consecutive images.
    Returns the path to the generated collage image, or None if failed.
    """
    if len(screenshot_paths) < 2:
        return None

    images = []
    for path in screenshot_paths:
        if not os.path.exists(path):
            continue
        try:
            img = Image.open(path)
            # Resize to same width (max 800px) while preserving aspect ratio
            max_width = 800
            if img.width > max_width:
                ratio = max_width / img.width
                new_height = int(img.height * ratio)
                img = img.resize((max_width, new_height), Image.LANCZOS)
            images.append(img)
        except Exception:
            continue

    if len(images) < 2:
        return None

    # Calculate total height: sum of all image heights + (len(images)-1) * arrow_height
    arrow_height = 60
    total_width = max(img.width for img in images)
    total_height = sum(img.height for img in images) + (len(images) - 1) * arrow_height

    collage = Image.new("RGB", (total_width, total_height), "white")
    y_offset = 0

    draw = ImageDraw.Draw(collage)
    for i, img in enumerate(images):
        collage.paste(img, (0, y_offset))
        y_offset += img.height

        if i < len(images) - 1:
            # Draw arrow
            arrow_start = (total_width // 2, y_offset - 20)
            arrow_end = (total_width // 2, y_offset + 40)
            draw.line([arrow_start, arrow_end], fill=arrow_color, width=3)
            # arrow head
            draw.polygon([
                (arrow_end[0] - 10, arrow_end[1] - 10),
                arrow_end,
                (arrow_end[0] + 10, arrow_end[1] - 10)
            ], fill=arrow_color)
            y_offset += arrow_height

    # Save collage
    if output_dir is None:
        output_dir = os.environ.get("RECORDINGS_DIR", "/app/recordings")
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(output_dir, f"collage_{timestamp}.png")
    collage.save(output_path)
    return output_path


def build_screenshot_flow(batch_screenshots: List[Dict]) -> List[str]:
    """
    Given list of screenshot metadata from DB (ordered by created_at), return a list of file paths
    in correct flow order (pre‑survey then per survey).
    """
    # Sort by survey_num (0 first, then ascending), then by created_at
    sorted_shots = sorted(batch_screenshots, key=lambda x: (x["survey_num"], x["created_at"]))
    return [s["file_path"] for s in sorted_shots if os.path.exists(s["file_path"])]