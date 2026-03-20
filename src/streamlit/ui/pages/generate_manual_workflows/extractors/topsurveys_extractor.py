# src/streamlit/ui/pages/generate_manual_workflows/extraction/extractors/topsurveys_extractor.py
"""
TopSurveys Extractor
====================
TopSurveys (topsurveys.app) is a Nuxt/Vue SPA.
Its CSS classes are hashed (e.g. "class_Abc123") and change on every deploy.

RELIABLE selectors we use instead:
  - Structural HTML:   role="radio", role="checkbox", role="button"
  - Native inputs:     input[type="radio"], input[type="checkbox"], textarea, select
  - ARIA:              aria-label, aria-required
  - Text content:      matched via JS contains()

WHAT THE EXTRACTOR DOES
  1. Connects to the running Chrome session via CDP (Playwright).
  2. Navigates to the given survey URL.
  3. Scrapes each screener question's text, type, options, and the selectors
     the workflow creator needs to answer it.
  4. Saves questions to the DB and returns the batch summary.

FALLBACK
  If no debug_port is supplied, or Playwright isn't installed, it returns
  15 realistic simulated questions so the rest of the pipeline still works.
"""

import json
import logging
import random
import re
import time
from typing import Any, Dict, List, Optional

from extraction.base_extractor import BaseExtractor  # type: ignore

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JS that runs inside the page to extract all questions in one round-trip
# ---------------------------------------------------------------------------
_EXTRACT_JS = """
() => {
    // ----------------------------------------------------------------
    // Helper: get visible text for an element
    // ----------------------------------------------------------------
    function getText(el) {
        return (el ? (el.innerText || el.textContent || '').trim() : '');
    }

    // ----------------------------------------------------------------
    // Helper: collect option labels for radio / checkbox groups
    // ----------------------------------------------------------------
    function getOptions(inputs) {
        return inputs.map(inp => {
            const forLbl = inp.id ? document.querySelector(`label[for='${inp.id}']`) : null;
            const parentLbl = inp.closest('label');
            const lbl = forLbl || parentLbl;
            return getText(lbl) || inp.value || inp.getAttribute('aria-label') || '';
        }).filter(Boolean);
    }

    // ----------------------------------------------------------------
    // Find question containers
    // Strategy: group inputs by their nearest common ancestor that
    // contains a text-like heading element.
    // ----------------------------------------------------------------
    const results = [];
    const seen = new Set();

    // Try containers that look like question wrappers
    const containerSelectors = [
        '[role="group"]',
        'fieldset',
        'form > div',
        'form > section',
        'main > div > div',   // generic Nuxt layout
    ];

    let containers = [];
    for (const sel of containerSelectors) {
        containers = Array.from(document.querySelectorAll(sel))
            .filter(el => {
                const rect = el.getBoundingClientRect();
                return rect.width > 50 && rect.height > 20;
            });
        if (containers.length > 0) break;
    }

    // Fallback: look for every visible input and walk up to find its question heading
    if (!containers.length) {
        const allInputs = Array.from(document.querySelectorAll(
            "input[type='radio'], input[type='checkbox'], textarea, select, input[type='text']"
        )).filter(i => {
            const r = i.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
        });

        for (const inp of allInputs) {
            let parent = inp.parentElement;
            let depth = 0;
            while (parent && depth < 6) {
                if (parent.querySelectorAll("input[type='radio'],input[type='checkbox']").length > 1
                    || parent.querySelector('textarea,select')) {
                    if (!seen.has(parent)) {
                        containers.push(parent);
                        seen.add(parent);
                    }
                    break;
                }
                parent = parent.parentElement;
                depth++;
            }
        }
    }

    // ----------------------------------------------------------------
    // Scrape each container
    // ----------------------------------------------------------------
    containers.forEach((c, idx) => {
        if (seen.has(c)) return;
        seen.add(c);

        // Question text: prefer heading tags, aria-label, then first text node
        const headings = c.querySelectorAll('h1,h2,h3,h4,h5,legend,p,[role="heading"],[aria-label]');
        let qText = '';
        for (const h of headings) {
            const t = getText(h);
            if (t.length > 5) { qText = t; break; }
        }
        if (!qText) qText = getText(c).split('\n')[0].trim();
        if (!qText || qText.length < 3) return;

        // Determine type
        const radios     = Array.from(c.querySelectorAll("input[type='radio']")).filter(i => !i.disabled);
        const checks     = Array.from(c.querySelectorAll("input[type='checkbox']")).filter(i => !i.disabled);
        const textarea   = c.querySelector('textarea');
        const select     = c.querySelector('select');
        const textInput  = c.querySelector("input[type='text'],input[type='number'],input[type='email']");

        let qType = 'text';
        let options = [];

        if (radios.length >= 2) {
            qType = 'multiple_choice';
            options = getOptions(radios);
        } else if (checks.length >= 1) {
            qType = 'checkbox';
            options = getOptions(checks);
        } else if (select) {
            qType = 'dropdown';
            options = Array.from(select.options).map(o => o.text.trim()).filter(t => t && t !== '--');
        } else if (textarea) {
            qType = 'textarea';
        } else if (textInput) {
            qType = 'text';
        }

        // Build the most stable selector for this container
        const cId = c.id ? '#' + c.id : null;
        const cRole = c.getAttribute('role') ? `[role="${c.getAttribute('role')}"]` : null;
        const clickEl = cId || cRole || null;

        // First input selector (for text/select)
        let inputEl = null;
        const firstInput = c.querySelector("input:not([type='hidden']),textarea,select");
        if (firstInput) {
            if (firstInput.id) inputEl = '#' + firstInput.id;
            else if (firstInput.name) inputEl = `[name="${firstInput.name}"]`;
        }

        const isRequired = !!(
            c.querySelector('[required],[aria-required="true"]') ||
            getText(c).includes('*')
        );

        results.push({
            question_text:     qText,
            question_type:     qType,
            question_category: 'screener',
            required:          isRequired,
            order_index:       idx,
            page_url:          window.location.href,
            click_element:     clickEl,
            input_element:     inputEl,
            submit_element:    null,
            options:           options,
            metadata: {
                source:    'topsurveys',
                simulated: false,
                scraped_at: new Date().toISOString(),
            }
        });
    });

    return results;
}
"""


class TopSurveysExtractor(BaseExtractor):

    def __init__(self, db_manager=None):
        super().__init__(db_manager)
        self._info = {
            "site_name":        "Top Surveys",
            "description":      "Survey panel via topsurveys.app (Nuxt/Vue SPA)",
            "version":          "2.0.0",
            "requires_login":   True,
            "requires_cookies": True,
        }

    def get_site_info(self) -> Dict[str, Any]:
        return self._info

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------

    def extract_questions(self, account_id: int, site_id: int,
                          url: str, profile_path: str, **kwargs) -> Dict[str, Any]:
        t0            = time.time()
        debug_port    = kwargs.get("debug_port")
        max_questions = kwargs.get("max_questions", 50)
        batch_id      = f"topsurveys_{account_id}_{int(t0)}"

        logger.info(f"[TopSurveys] Extraction start  account={account_id}  url={url}  port={debug_port}")

        # ---- Live CDP extraction ----
        if debug_port:
            try:
                questions = self._live_extract(url, debug_port, max_questions)
                logger.info(f"[TopSurveys] Live extraction: {len(questions)} questions")
            except Exception as exc:
                logger.warning(f"[TopSurveys] Live extraction failed ({exc}), using simulation")
                questions = self._simulate(url, max_questions)
        else:
            logger.info("[TopSurveys] No debug_port — using simulation")
            questions = self._simulate(url, max_questions)

        # ---- Persist ----
        inserted = self.save_questions_to_db(account_id, site_id, questions, batch_id)
        self.log_extraction(account_id, site_id, batch_id, len(questions))

        return {
            "success":                True,
            "questions":              questions,
            "questions_found":        len(questions),
            "inserted":               inserted,
            "batch_id":               batch_id,
            "execution_time_seconds": round(time.time() - t0, 2),
        }

    # ------------------------------------------------------------------
    # Live extraction
    # ------------------------------------------------------------------

    def _live_extract(self, url: str, debug_port: int, max_q: int) -> List[Dict]:
        page, browser, pw = self.connect_to_chrome_session(debug_port)
        questions = []

        try:
            logger.info(f"[TopSurveys] Navigating to {url}")
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(2500)   # wait for Vue hydration

            page_num = 0
            while len(questions) < max_q:
                page_num += 1
                logger.info(f"[TopSurveys] Scraping page {page_num}")

                if self._is_dq(page):
                    logger.info("[TopSurveys] DQ detected — stopping extraction")
                    break

                new_qs = page.evaluate(_EXTRACT_JS)
                if not isinstance(new_qs, list) or not new_qs:
                    logger.info("[TopSurveys] No questions found — done")
                    break

                questions.extend(new_qs)

                if not self._advance(page):
                    break

                page.wait_for_timeout(1800)

        finally:
            try: pw.stop()
            except: pass

        return questions[:max_q]

    def _advance(self, page) -> bool:
        """Click the Next button. Returns True if found."""
        js = """
() => {
    const labels = ['next','continue','submit','proceed'];
    const btns = Array.from(document.querySelectorAll(
        "button,input[type='submit'],[role='button']"
    )).filter(el => { const r = el.getBoundingClientRect(); return r.width > 0; });
    for (const l of labels) {
        const b = btns.find(el => (el.innerText||el.value||'').toLowerCase().trim().startsWith(l));
        if (b) { b.click(); return true; }
    }
    if (btns.length) { btns[btns.length-1].click(); return true; }
    return false;
}"""
        try:
            found = page.evaluate(js)
            if found:
                page.wait_for_timeout(1000)
            return bool(found)
        except:
            return False

    def _is_dq(self, page) -> bool:
        phrases = ["not eligible","don't qualify","not a match","screened out","unfortunately"]
        try:
            body = page.inner_text("body").lower()
            return any(p in body for p in phrases)
        except:
            return False

    # ------------------------------------------------------------------
    # Simulation fallback (no Chrome needed)
    # ------------------------------------------------------------------

    def _simulate(self, url: str, max_q: int) -> List[Dict]:
        """Realistic screener questions for TopSurveys demographics."""
        templates = [
            {"text": "What is your age?", "type": "multiple_choice", "cat": "demographics",
             "opts": ["18–24","25–34","35–44","45–54","55–64","65+"]},
            {"text": "What is your gender?", "type": "multiple_choice", "cat": "demographics",
             "opts": ["Male","Female","Non-binary","Prefer not to say"]},
            {"text": "What is your current employment status?", "type": "multiple_choice", "cat": "demographics",
             "opts": ["Employed full-time","Employed part-time","Self-employed","Student","Unemployed","Retired"]},
            {"text": "What is your annual household income?", "type": "multiple_choice", "cat": "demographics",
             "opts": ["Under $25,000","$25,000–$49,999","$50,000–$74,999","$75,000–$99,999","$100,000+"]},
            {"text": "What is the highest level of education you have completed?", "type": "multiple_choice",
             "cat": "demographics",
             "opts": ["High school","Some college","Associate degree","Bachelor's degree","Master's degree","Doctorate"]},
            {"text": "How often do you shop online?", "type": "multiple_choice", "cat": "screener",
             "opts": ["Daily","Weekly","Monthly","A few times a year","Never"]},
            {"text": "Which of the following devices do you own?", "type": "checkbox", "cat": "screener",
             "opts": ["Smartphone","Tablet","Laptop","Smart TV","Smart watch","None"]},
            {"text": "Which streaming services do you currently subscribe to?", "type": "checkbox", "cat": "screener",
             "opts": ["Netflix","Hulu","Disney+","Amazon Prime Video","Apple TV+","None"]},
            {"text": "How satisfied are you with your current internet service?", "type": "rating",
             "cat": "opinion", "opts": ["1","2","3","4","5"]},
            {"text": "In the past 3 months, have you purchased any health or wellness products?",
             "type": "multiple_choice", "cat": "screener", "opts": ["Yes","No"]},
            {"text": "Which best describes your living situation?", "type": "multiple_choice",
             "cat": "demographics", "opts": ["Own my home","Rent an apartment","Rent a house","Live with family","Other"]},
            {"text": "How many people live in your household?", "type": "multiple_choice",
             "cat": "demographics", "opts": ["1","2","3","4","5","6+"]},
            {"text": "Do you have children under 18 in your household?", "type": "multiple_choice",
             "cat": "demographics", "opts": ["Yes","No"]},
            {"text": "What is your primary mode of transportation?", "type": "multiple_choice",
             "cat": "lifestyle", "opts": ["Personal vehicle","Public transit","Rideshare","Bicycle","Walk"]},
            {"text": "How would you rate your overall health?", "type": "rating",
             "cat": "health", "opts": ["1 – Poor","2","3 – Average","4","5 – Excellent"]},
        ]
        random.shuffle(templates)
        out = []
        for i, t in enumerate(templates[:max_q]):
            out.append({
                "question_text":     t["text"],
                "question_type":     t["type"],
                "question_category": t["cat"],
                "required":          True,
                "order_index":       i,
                "page_url":          url,
                "click_element":     f"[data-q-idx='{i}']",
                "input_element":     f"[data-q-idx='{i}'] input",
                "submit_element":    "button[type='submit']",
                "options":           t.get("opts", []),
                "metadata": {"source": "topsurveys", "simulated": True},
            })
        return out