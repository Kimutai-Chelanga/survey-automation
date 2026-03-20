# src/streamlit/ui/pages/generate_manual_workflows/base/base_extractor.py
"""
BaseExtractor — inherit from this for every new survey site.

Key helpers provided:
  connect_to_chrome_session(debug_port)  → (page, browser, pw)  via Playwright CDP
  save_questions_to_db(...)
  log_extraction(...)
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Valid question types per DB CHECK constraint
VALID_QUESTION_TYPES = {
    "multiple_choice", "text", "rating", "yes_no", "dropdown", "checkbox", "radio"
}

# Map extractor-produced types → valid DB types
QUESTION_TYPE_MAP = {
    "textarea":      "text",
    "open_ended":    "text",
    "free_text":     "text",
    "short_answer":  "text",
    "long_answer":   "text",
    "single_choice": "multiple_choice",
    "radio":         "radio",
    "scale":         "rating",
    "likert":        "rating",
    "likert_matrix": "rating",
    "matrix":        "rating",
    "slider":        "rating",
    "boolean":       "yes_no",
    "true_false":    "yes_no",
    "yes_no":        "yes_no",
    "select":        "dropdown",
    "multi_select":  "checkbox",
    "multiple_select": "checkbox",
}


class BaseExtractor(ABC):

    def __init__(self, db_manager=None):
        self.db_manager = db_manager

    @abstractmethod
    def get_site_info(self) -> Dict[str, Any]:
        """Return dict with at least: site_name, version, description."""
        ...

    @abstractmethod
    def extract_questions(self, account_id: int, site_id: int,
                          url: str, profile_path: str, **kwargs) -> Dict[str, Any]:
        """
        Must return:
          { success, questions, questions_found, inserted, batch_id,
            execution_time_seconds, error (if success=False) }
        """
        ...

    # ------------------------------------------------------------------
    # CDP connection
    # ------------------------------------------------------------------

    def connect_to_chrome_session(self, debug_port: int):
        """
        Connect to the already-running Chrome via Playwright CDP.
        Returns (page, browser, playwright_instance).
        Caller must call pw.stop() when done — do NOT close the browser.
        """
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except ImportError:
            raise RuntimeError(
                "playwright not installed. Run:  pip install playwright && playwright install chromium"
            )

        pw = sync_playwright().start()
        try:
            browser = pw.chromium.connect_over_cdp(f"http://localhost:{debug_port}")
        except Exception as exc:
            pw.stop()
            raise RuntimeError(f"Cannot reach Chrome on port {debug_port}: {exc}")

        ctx  = browser.contexts[0] if browser.contexts else browser.new_context()
        page = ctx.pages[0]        if ctx.pages        else ctx.new_page()
        logger.info(f"Connected to Chrome CDP port {debug_port}")
        return page, browser, pw

    # ------------------------------------------------------------------
    # Type normalisation
    # ------------------------------------------------------------------

    @staticmethod
    def normalize_question_type(raw_type: str) -> str:
        """
        Map any extractor-produced question type to a valid DB type.
        Falls back to 'text' if not recognised.
        """
        t = (raw_type or "text").lower().strip()
        if t in VALID_QUESTION_TYPES:
            return t
        return QUESTION_TYPE_MAP.get(t, "text")

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def save_questions_to_db(self, account_id: int, site_id: int,
                              questions: List[Dict], batch_id: str) -> int:
        if not self.db_manager or not questions:
            return 0

        inserted = 0
        skipped  = 0

        for q in questions:
            try:
                question_text = (q.get("question_text") or "").strip()
                if not question_text:
                    logger.warning("Skipping question with empty text")
                    continue

                # Normalise type to satisfy CHECK constraint
                question_type = self.normalize_question_type(q.get("question_type", "text"))

                # Skip duplicates
                dup = self.db_manager.execute_query(
                    """SELECT question_id FROM questions
                       WHERE account_id = %s
                         AND survey_site_id = %s
                         AND question_text = %s
                       LIMIT 1""",
                    (account_id, site_id, question_text),
                    fetch=True,
                )
                if dup:
                    skipped += 1
                    continue

                # Serialise options and metadata
                options_json  = json.dumps(q["options"])  if q.get("options")   else None
                metadata_json = json.dumps(q["metadata"]) if q.get("metadata")  else None

                self.db_manager.execute_query(
                    """INSERT INTO questions (
                           account_id,
                           survey_site_id,
                           question_text,
                           question_type,
                           question_category,
                           required,
                           order_index,
                           page_url,
                           click_element,
                           input_element,
                           submit_element,
                           options,
                           metadata,
                           extraction_batch_id,
                           is_active,
                           used_in_workflow,
                           extracted_at
                       ) VALUES (
                           %s, %s, %s, %s,
                           %s, %s, %s, %s,
                           %s, %s, %s,
                           %s::jsonb, %s::jsonb, %s,
                           TRUE, FALSE, CURRENT_TIMESTAMP
                       )""",
                    (
                        account_id,
                        site_id,
                        question_text,
                        question_type,
                        q.get("question_category"),
                        q.get("required", False),
                        q.get("order_index", 0),
                        q.get("page_url"),
                        q.get("click_element"),
                        q.get("input_element"),
                        q.get("submit_element"),
                        options_json,
                        metadata_json,
                        batch_id,
                    ),
                )
                inserted += 1

            except Exception as exc:
                logger.error(f"save_questions_to_db row error: {exc}  text={q.get('question_text','')[:60]}")

        logger.info(
            f"save_questions_to_db: inserted={inserted} skipped={skipped} "
            f"total={len(questions)} batch={batch_id}"
        )
        return inserted

    def log_extraction(self, account_id, site_id, batch_id,
                       questions_found, status="success", error_message=None):
        """
        Log to extraction_state using the record_extraction_batch DB function.
        Falls back silently if the function doesn't exist.
        """
        if not self.db_manager:
            return
        try:
            self.db_manager.execute_query(
                """SELECT record_extraction_batch(%s, %s, NULL, %s, %s)""",
                (account_id, site_id, batch_id, questions_found),
            )
        except Exception as exc:
            # Non-fatal — extraction already succeeded
            logger.warning(f"log_extraction failed (non-fatal): {exc}")