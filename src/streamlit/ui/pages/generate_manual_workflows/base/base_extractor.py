# src/streamlit/ui/pages/generate_manual_workflows/base/base_extractor.py
"""
BaseExtractor — inherit from this for every new survey site.
v2: save_questions_to_db now accepts optional survey_name kwarg
    and writes it to questions.survey_name
"""

import json
import logging
import time
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

VALID_QUESTION_TYPES = {
    "multiple_choice", "text", "rating", "yes_no",
    "dropdown", "checkbox", "radio"
}

QUESTION_TYPE_MAP = {
    "textarea":        "text",
    "open_ended":      "text",
    "free_text":       "text",
    "short_answer":    "text",
    "long_answer":     "text",
    "single_choice":   "multiple_choice",
    "radio":           "radio",
    "scale":           "rating",
    "likert":          "rating",
    "likert_matrix":   "rating",
    "matrix":          "rating",
    "slider":          "rating",
    "boolean":         "yes_no",
    "true_false":      "yes_no",
    "yes_no":          "yes_no",
    "select":          "dropdown",
    "multi_select":    "checkbox",
    "multiple_select": "checkbox",
}


class BaseExtractor(ABC):

    def __init__(self, db_manager=None):
        self.db_manager = db_manager

    @abstractmethod
    def get_site_info(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def extract_questions(self, account_id: int, site_id: int,
                          url: str, profile_path: str, **kwargs) -> Dict[str, Any]:
        ...

    # ------------------------------------------------------------------
    # CDP connection
    # ------------------------------------------------------------------

    def connect_to_chrome_session(self, debug_port: int):
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise RuntimeError(
                "playwright not installed. Run: pip install playwright && playwright install chromium"
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
        t = (raw_type or "text").lower().strip()
        if t in VALID_QUESTION_TYPES:
            return t
        return QUESTION_TYPE_MAP.get(t, "text")

    @staticmethod
    def normalize_url(url: str) -> str:
        """Ensure URL has a scheme; default to https:// if missing."""
        url = (url or "").strip()
        if url and not url.startswith(("http://", "https://")):
            url = "https://" + url
        return url

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def save_questions_to_db(
        self,
        account_id: int,
        site_id: int,
        questions: List[Dict],
        batch_id: str,
        survey_name: Optional[str] = None,   # ← NEW
    ) -> int:
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

                question_type = self.normalize_question_type(q.get("question_type", "text"))

                # Skip exact duplicates for this account+site
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

                options_json  = json.dumps(q["options"])  if q.get("options")  else None
                metadata_json = json.dumps(q["metadata"]) if q.get("metadata") else None

                # survey_name: prefer per-question field, fall back to parameter
                q_survey_name = q.get("survey_name") or survey_name

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
                           survey_name,
                           survey_complete,
                           is_active,
                           used_in_workflow,
                           extracted_at
                       ) VALUES (
                           %s, %s, %s, %s,
                           %s, %s, %s, %s,
                           %s, %s, %s,
                           %s::jsonb, %s::jsonb, %s,
                           %s, FALSE,
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
                        q_survey_name,
                    ),
                )
                inserted += 1

            except Exception as exc:
                logger.error(
                    f"save_questions_to_db row error: {exc}  "
                    f"text={q.get('question_text','')[:60]}"
                )

        logger.info(
            f"save_questions_to_db: inserted={inserted} skipped={skipped} "
            f"total={len(questions)} batch={batch_id} survey='{survey_name}'"
        )
        return inserted

    def log_extraction(self, account_id, site_id, batch_id,
                       questions_found, status="success", error_message=None):
        if not self.db_manager:
            return
        try:
            self.db_manager.execute_query(
                "SELECT record_extraction_batch(%s, %s, NULL, %s, %s)",
                (account_id, site_id, batch_id, questions_found),
            )
        except Exception as exc:
            logger.warning(f"log_extraction failed (non-fatal): {exc}")