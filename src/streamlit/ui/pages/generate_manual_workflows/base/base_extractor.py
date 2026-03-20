# src/streamlit/ui/pages/generate_manual_workflows/extraction/base_extractor.py
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
        page = ctx.pages[0]       if ctx.pages        else ctx.new_page()
        logger.info(f"Connected to Chrome CDP port {debug_port}")
        return page, browser, pw

    # ------------------------------------------------------------------
    # DB helpers
    # ------------------------------------------------------------------

    def save_questions_to_db(self, account_id: int, site_id: int,
                              questions: List[Dict], batch_id: str) -> int:
        if not self.db_manager or not questions:
            return 0

        inserted = 0
        for q in questions:
            try:
                dup = self.db_manager.execute_query(
                    "SELECT question_id FROM questions "
                    "WHERE account_id=%s AND survey_site_id=%s AND question_text=%s LIMIT 1",
                    (account_id, site_id, q.get("question_text", "")), fetch=True
                )
                if dup:
                    continue

                self.db_manager.execute_query(
                    """INSERT INTO questions
                       (account_id, survey_site_id, question_text, question_type,
                        question_category, required, order_index, page_url,
                        click_element, input_element, submit_element,
                        options, metadata, batch_id,
                        is_active, used_in_workflow, extracted_at)
                       VALUES (%s,%s,%s,%s, %s,%s,%s,%s, %s,%s,%s, %s,%s,%s,
                               TRUE,FALSE,CURRENT_TIMESTAMP)""",
                    (
                        account_id, site_id,
                        q.get("question_text", ""), q.get("question_type", "unknown"),
                        q.get("question_category"), q.get("required", False),
                        q.get("order_index", 0), q.get("page_url"),
                        q.get("click_element"), q.get("input_element"), q.get("submit_element"),
                        json.dumps(q["options"]) if q.get("options") else None,
                        json.dumps(q["metadata"]) if q.get("metadata") else None,
                        batch_id,
                    )
                )
                inserted += 1
            except Exception as exc:
                logger.error(f"save_questions_to_db row error: {exc}")

        logger.info(f"Saved {inserted}/{len(questions)} questions  batch={batch_id}")
        return inserted

    def log_extraction(self, account_id, site_id, batch_id,
                       questions_found, status="success", error_message=None):
        if not self.db_manager:
            return
        try:
            self.db_manager.execute_query(
                """INSERT INTO extraction_logs
                   (account_id,site_id,batch_id,questions_found,status,error_message,extracted_at)
                   VALUES (%s,%s,%s,%s,%s,%s,CURRENT_TIMESTAMP)
                   ON CONFLICT DO NOTHING""",
                (account_id, site_id, batch_id, questions_found, status, error_message)
            )
        except Exception:
            pass   # table may not exist yet