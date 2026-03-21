# src/streamlit/ui/pages/generate_manual_workflows/extractors/topsurveys_extractor.py
"""
TopSurveys Extractor  v4.0.0
============================
Changes from v3.0:
  - Navigates to the survey LISTING page (dashboard)
  - Finds ALL available survey links/buttons on that page
  - Clicks each one, extracts its screener questions, saves them with
    the survey_name, then navigates back to the listing
  - Failed/DQ surveys are recorded but don't stop the loop
  - Returns a combined result across all surveys found
"""

import json
import logging
import random
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

try:
    from extraction.base_extractor import BaseExtractor  # type: ignore
except ImportError:
    class BaseExtractor:
        def __init__(self, db_manager=None):
            self.db_manager = db_manager
        def get_site_info(self): return {}
        def extract_questions(self, *a, **kw): return {}
        def connect_to_chrome_session(self, port): raise RuntimeError("no playwright")
        def normalize_question_type(self, t): return "text"
        def save_questions_to_db(self, *a, **kw): return 0
        def log_extraction(self, *a, **kw): pass


# ---------------------------------------------------------------------------
# JS: find all survey links/buttons on the dashboard listing page
# ---------------------------------------------------------------------------
_FIND_SURVEYS_JS = """
() => {
    const surveys = [];
    const seen    = new Set();

    // Strategy 1: links that look like individual survey entries
    // TopSurveys uses cards/rows with a CTA button per survey
    const linkSelectors = [
        'a[href*="survey"]',
        'a[href*="questionnaire"]',
        'button[data-survey]',
        '[class*="survey-item"] a',
        '[class*="survey-card"] a',
        '[class*="survey-row"]  a',
        '[class*="survey-list"] a',
        '[class*="survey-entry"] a',
        '[class*="surveyCard"]  a',
        '[class*="surveyItem"]  a',
        '[class*="surveyRow"]   a',
        'li a[href]',        // generic list items
        '.card a[href]',
        '.item  a[href]',
    ];

    for (const sel of linkSelectors) {
        try {
            document.querySelectorAll(sel).forEach(el => {
                const href  = el.href || '';
                const text  = (el.innerText || el.textContent || '').trim();
                const rect  = el.getBoundingClientRect();
                if (rect.width < 10 || rect.height < 10) return;  // hidden
                if (!href || href === window.location.href) return;
                if (href.startsWith('javascript:'))           return;
                if (seen.has(href)) return;
                seen.add(href);
                surveys.push({ href, text: text.slice(0, 80), selector: sel });
            });
        } catch(e) {}
    }

    // Strategy 2: any visible "Start" / "Take" / "Begin" button
    // that is NOT already in the list
    const ctaKeywords = ['start', 'take', 'begin', 'participate', 'go', 'open'];
    document.querySelectorAll('button, a').forEach(el => {
        const txt  = (el.innerText || el.textContent || '').trim().toLowerCase();
        const href = el.href || '';
        const rect = el.getBoundingClientRect();
        if (rect.width < 10 || rect.height < 10) return;
        if (!ctaKeywords.some(k => txt.startsWith(k))) return;
        if (seen.has(href || txt)) return;
        seen.add(href || txt);
        surveys.push({
            href:     href || null,
            text:     txt.slice(0, 80),
            selector: null,
            isCTA:    true,
            // Store enough info to re-find the button later
            btnText:  (el.innerText || '').trim().slice(0, 60),
        });
    });

    return surveys;
}
"""

# ---------------------------------------------------------------------------
# JS: get the survey name from the current page
# ---------------------------------------------------------------------------
_SURVEY_NAME_JS = """
() => {
    const h1  = document.querySelector('h1,h2,[class*="title"],[class*="survey-name"]');
    const og  = document.querySelector('meta[property="og:title"]');
    const raw = (h1  && h1.innerText.trim())
             || (og  && og.getAttribute('content'))
             || document.title.split('|')[0].trim()
             || document.title.split('-')[0].trim()
             || 'Unknown Survey';
    return raw
        .replace(/\\s*(- TopSurveys|\\| TopSurveys|Survey|Screener)\\s*$/i, '')
        .trim() || 'Unknown Survey';
}
"""

# ---------------------------------------------------------------------------
# JS: extract all questions from the current survey page
# ---------------------------------------------------------------------------
_EXTRACT_JS = """
() => {
    function getText(el) {
        return (el ? (el.innerText || el.textContent || '').trim() : '');
    }
    function getOptions(inputs) {
        return inputs.map(inp => {
            const forLbl    = inp.id ? document.querySelector(`label[for='${inp.id}']`) : null;
            const parentLbl = inp.closest('label');
            const lbl       = forLbl || parentLbl;
            return getText(lbl) || inp.value || inp.getAttribute('aria-label') || '';
        }).filter(Boolean);
    }

    const results = [];
    const seen    = new Set();

    const containerSelectors = [
        '[role="group"]', 'fieldset', 'form > div',
        'form > section', 'main > div > div',
    ];

    let containers = [];
    for (const sel of containerSelectors) {
        containers = Array.from(document.querySelectorAll(sel)).filter(el => {
            const r = el.getBoundingClientRect();
            return r.width > 50 && r.height > 20;
        });
        if (containers.length > 0) break;
    }

    if (!containers.length) {
        const allInputs = Array.from(document.querySelectorAll(
            "input[type='radio'], input[type='checkbox'], textarea, select, input[type='text']"
        )).filter(i => { const r = i.getBoundingClientRect(); return r.width > 0 && r.height > 0; });
        for (const inp of allInputs) {
            let parent = inp.parentElement, depth = 0;
            while (parent && depth < 6) {
                if (parent.querySelectorAll("input[type='radio'],input[type='checkbox']").length > 1
                    || parent.querySelector('textarea,select')) {
                    if (!seen.has(parent)) { containers.push(parent); seen.add(parent); }
                    break;
                }
                parent = parent.parentElement; depth++;
            }
        }
    }

    seen.clear();

    containers.forEach((c, idx) => {
        if (seen.has(c)) return;
        seen.add(c);

        const headings = c.querySelectorAll('h1,h2,h3,h4,h5,legend,p,[role="heading"],[aria-label]');
        let qText = '';
        for (const h of headings) {
            const t = getText(h);
            if (t.length > 5) { qText = t; break; }
        }
        if (!qText) qText = getText(c).split('\\n')[0].trim();
        if (!qText || qText.length < 3) return;

        const radios   = Array.from(c.querySelectorAll("input[type='radio']")).filter(i => !i.disabled);
        const checks   = Array.from(c.querySelectorAll("input[type='checkbox']")).filter(i => !i.disabled);
        const textarea = c.querySelector('textarea');
        const select   = c.querySelector('select');
        const textInp  = c.querySelector("input[type='text'],input[type='number'],input[type='email']");

        let qType = 'text', options = [];
        if      (radios.length >= 2) { qType = 'multiple_choice'; options = getOptions(radios); }
        else if (checks.length >= 1) { qType = 'checkbox';        options = getOptions(checks); }
        else if (select)             { qType = 'dropdown';
            options = Array.from(select.options).map(o => o.text.trim()).filter(t => t && t !== '--');
        }
        else if (textarea)           { qType = 'textarea'; }
        else if (textInp)            { qType = 'text'; }

        const cId   = c.id   ? '#' + c.id                          : null;
        const cRole = c.getAttribute('role')
                    ? `[role="${c.getAttribute('role')}"]`           : null;
        const clickEl = cId || cRole || null;

        let inputEl = null;
        const first = c.querySelector("input:not([type='hidden']),textarea,select");
        if (first) {
            if (first.id)     inputEl = '#' + first.id;
            else if (first.name) inputEl = `[name="${first.name}"]`;
        }

        results.push({
            question_text:     qText,
            question_type:     qType,
            question_category: 'screener',
            required:          !!(c.querySelector('[required],[aria-required="true"]') || getText(c).includes('*')),
            order_index:       idx,
            page_url:          window.location.href,
            click_element:     clickEl,
            input_element:     inputEl,
            submit_element:    null,
            options,
            metadata: { source: 'topsurveys', simulated: false, scraped_at: new Date().toISOString() }
        });
    });

    return results;
}
"""

# ---------------------------------------------------------------------------
# JS: check for DQ on the current page
# ---------------------------------------------------------------------------
_DQ_JS = """
() => {
    const body = (document.body.innerText || '').toLowerCase();
    return ['not eligible',"don't qualify",'not a match',
            'screened out','unfortunately','disqualified',
            'quota full','survey is full'].some(p => body.includes(p));
}
"""

# ---------------------------------------------------------------------------
# JS: advance to the next page / click Start on a survey card
# ---------------------------------------------------------------------------
_CLICK_START_JS = """
(btnText) => {
    // Try exact match first, then partial
    const btns = Array.from(document.querySelectorAll(
        "button, a, input[type='submit'], [role='button']"
    )).filter(el => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; });

    if (btnText) {
        const exact = btns.find(el => (el.innerText||el.textContent||'').trim() === btnText);
        if (exact) { exact.click(); return true; }
        const partial = btns.find(el => (el.innerText||el.textContent||'').trim().toLowerCase()
                                         .includes(btnText.toLowerCase()));
        if (partial) { partial.click(); return true; }
    }

    // Fallback: any Start/Take/Begin button
    const keywords = ['start','take survey','begin','participate'];
    for (const k of keywords) {
        const b = btns.find(el => (el.innerText||'').toLowerCase().trim().startsWith(k));
        if (b) { b.click(); return true; }
    }
    return false;
}
"""


# ---------------------------------------------------------------------------
# Extractor class
# ---------------------------------------------------------------------------

class TopSurveysExtractor(BaseExtractor):

    def __init__(self, db_manager=None):
        super().__init__(db_manager)
        self._info = {
            "site_name":        "Top Surveys",
            "description":      "Survey panel — extracts all surveys from the dashboard",
            "version":          "4.0.0",
            "requires_login":   True,
            "requires_cookies": True,
        }

    def get_site_info(self) -> Dict[str, Any]:
        return self._info

    # ------------------------------------------------------------------
    # Single-survey extraction (original API — still works)
    # ------------------------------------------------------------------

    def extract_questions(
        self,
        account_id: int,
        site_id: int,
        url: str,
        profile_path: str,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Extract questions from ONE survey URL.
        If debug_port is supplied and the URL is a dashboard/listing page,
        delegates to extract_all_from_listing for full multi-survey extraction.
        """
        t0         = time.time()
        debug_port = kwargs.get("debug_port")
        batch_id   = f"topsurveys_{account_id}_{int(t0)}"

        if debug_port:
            try:
                questions, survey_name = self._extract_single(
                    url, debug_port, kwargs.get("max_questions", 50)
                )
            except Exception as exc:
                logger.warning(f"[TopSurveys] Live extraction failed ({exc}), simulating")
                questions, survey_name = self._simulate(url, kwargs.get("max_questions", 50))
        else:
            questions, survey_name = self._simulate(url, kwargs.get("max_questions", 50))

        for q in questions:
            q["survey_name"] = survey_name

        inserted = self.save_questions_to_db(
            account_id, site_id, questions, batch_id, survey_name=survey_name
        )
        self.log_extraction(account_id, site_id, batch_id, len(questions))

        return {
            "success":                True,
            "questions":              questions,
            "questions_found":        len(questions),
            "inserted":               inserted,
            "batch_id":               batch_id,
            "survey_name":            survey_name,
            "surveys_processed":      1,
            "execution_time_seconds": round(time.time() - t0, 2),
        }

    # ------------------------------------------------------------------
    # Multi-survey extraction — loops over all surveys on the dashboard
    # ------------------------------------------------------------------

    def extract_all_from_listing(
        self,
        account_id: int,
        site_id: int,
        listing_url: str,
        profile_path: str,
        debug_port: Optional[int] = None,
        max_surveys: int = 20,
        max_questions_per_survey: int = 30,
        progress_callback=None,   # callable(current, total, msg)
    ) -> Dict[str, Any]:
        """
        Navigate to listing_url, find all survey links, and extract
        screener questions from each one.

        Returns a combined result dict with per-survey breakdown.
        """
        t0 = time.time()

        if not debug_port:
            # No Chrome session — simulate multiple surveys
            return self._simulate_all(
                account_id, site_id, listing_url, max_surveys, t0
            )

        page, browser, pw = self.connect_to_chrome_session(debug_port)
        all_questions: List[Dict] = []
        survey_results: List[Dict] = []
        total_inserted = 0

        try:
            # ── 1. Navigate to dashboard ─────────────────────────────
            logger.info(f"[TopSurveys] Navigating to listing: {listing_url}")
            page.goto(listing_url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(3000)

            # ── 2. Find all survey links ──────────────────────────────
            surveys_found = page.evaluate(_FIND_SURVEYS_JS)
            logger.info(f"[TopSurveys] Found {len(surveys_found)} survey entries on listing page")

            if not surveys_found:
                logger.warning("[TopSurveys] No surveys found on listing page — trying simulation")
                return self._simulate_all(account_id, site_id, listing_url, max_surveys, t0)

            surveys_to_process = surveys_found[:max_surveys]
            total = len(surveys_to_process)

            # ── 3. Loop over each survey ──────────────────────────────
            for idx, survey_entry in enumerate(surveys_to_process):
                survey_href = survey_entry.get("href")
                survey_label = survey_entry.get("text") or f"Survey {idx + 1}"
                is_cta = survey_entry.get("isCTA", False)
                btn_text = survey_entry.get("btnText", "")

                if progress_callback:
                    progress_callback(
                        idx + 1, total,
                        f"Extracting survey {idx+1}/{total}: {survey_label[:50]}"
                    )

                logger.info(f"[TopSurveys] Processing survey {idx+1}/{total}: {survey_label[:60]}")

                batch_id = f"topsurveys_{account_id}_{int(time.time())}_{idx}"

                try:
                    # Navigate back to listing first (unless we're already there)
                    current_url = page.url
                    if listing_url not in current_url and current_url != listing_url:
                        page.goto(listing_url, wait_until="domcontentloaded", timeout=20_000)
                        page.wait_for_timeout(2500)

                    # Click through to the survey
                    if survey_href:
                        page.goto(survey_href, wait_until="domcontentloaded", timeout=20_000)
                    elif is_cta and btn_text:
                        clicked = page.evaluate(_CLICK_START_JS, btn_text)
                        if not clicked:
                            logger.warning(f"[TopSurveys] Could not click CTA for '{survey_label}'")
                            survey_results.append({
                                "survey_label": survey_label,
                                "status":       "skip",
                                "reason":       "could not click CTA",
                                "questions":    0,
                                "inserted":     0,
                            })
                            continue
                    else:
                        # Try re-finding the survey by index on the refreshed page
                        all_surveys_now = page.evaluate(_FIND_SURVEYS_JS)
                        if idx < len(all_surveys_now):
                            entry = all_surveys_now[idx]
                            if entry.get("href"):
                                page.goto(entry["href"], wait_until="domcontentloaded", timeout=20_000)
                            else:
                                logger.warning(f"[TopSurveys] No href for survey {idx+1}, skipping")
                                survey_results.append({
                                    "survey_label": survey_label,
                                    "status":       "skip",
                                    "reason":       "no navigable href",
                                    "questions":    0,
                                    "inserted":     0,
                                })
                                continue

                    page.wait_for_timeout(2500)

                    # Check for immediate DQ (survey not available, quota full, etc.)
                    is_dq = page.evaluate(_DQ_JS)
                    if is_dq:
                        logger.info(f"[TopSurveys] Survey '{survey_label}' DQ/unavailable, skipping")
                        survey_results.append({
                            "survey_label": survey_label,
                            "status":       "dq",
                            "reason":       "DQ/unavailable immediately",
                            "questions":    0,
                            "inserted":     0,
                        })
                        continue

                    # Get survey name from the page
                    try:
                        survey_name = page.evaluate(_SURVEY_NAME_JS) or survey_label
                    except Exception:
                        survey_name = survey_label

                    # Extract questions — may span multiple pages
                    questions: List[Dict] = []
                    page_num = 0

                    while len(questions) < max_questions_per_survey:
                        page_num += 1

                        # Re-check DQ on each page
                        if page.evaluate(_DQ_JS):
                            logger.info(f"[TopSurveys] DQ on page {page_num} of '{survey_name}'")
                            break

                        new_qs = page.evaluate(_EXTRACT_JS)
                        if not isinstance(new_qs, list) or not new_qs:
                            logger.info(f"[TopSurveys] No more questions on page {page_num}")
                            break

                        questions.extend(new_qs)

                        # Try to advance to next page
                        advanced = self._advance_page(page)
                        if not advanced:
                            break
                        page.wait_for_timeout(1800)

                    # Stamp survey_name on all questions
                    for q in questions:
                        q["survey_name"] = survey_name

                    # Save to DB
                    inserted = self.save_questions_to_db(
                        account_id, site_id,
                        questions[:max_questions_per_survey],
                        batch_id,
                        survey_name=survey_name,
                    )
                    self.log_extraction(account_id, site_id, batch_id, len(questions))

                    all_questions.extend(questions)
                    total_inserted += inserted

                    survey_results.append({
                        "survey_label": survey_label,
                        "survey_name":  survey_name,
                        "status":       "success",
                        "questions":    len(questions),
                        "inserted":     inserted,
                        "batch_id":     batch_id,
                    })

                    logger.info(
                        f"[TopSurveys] '{survey_name}': "
                        f"{len(questions)} questions, {inserted} inserted"
                    )

                    # Small pause before next survey
                    time.sleep(1.5)

                except Exception as exc:
                    logger.error(f"[TopSurveys] Error on survey '{survey_label}': {exc}")
                    survey_results.append({
                        "survey_label": survey_label,
                        "status":       "error",
                        "reason":       str(exc),
                        "questions":    0,
                        "inserted":     0,
                    })
                    # Navigate back to listing to recover
                    try:
                        page.goto(listing_url, wait_until="domcontentloaded", timeout=15_000)
                        page.wait_for_timeout(2000)
                    except Exception:
                        pass

        finally:
            try:
                pw.stop()
            except Exception:
                pass

        successful = [r for r in survey_results if r["status"] == "success"]
        failed     = [r for r in survey_results if r["status"] in ("dq", "error", "skip")]

        return {
            "success":                True,
            "questions":              all_questions,
            "questions_found":        len(all_questions),
            "inserted":               total_inserted,
            "surveys_found":          len(surveys_found) if 'surveys_found' in dir() else 0,
            "surveys_processed":      len(survey_results),
            "surveys_successful":     len(successful),
            "surveys_failed":         len(failed),
            "survey_results":         survey_results,
            "batch_id":               f"topsurveys_all_{account_id}_{int(t0)}",
            "execution_time_seconds": round(time.time() - t0, 2),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _extract_single(
        self, url: str, debug_port: int, max_q: int
    ) -> Tuple[List[Dict], str]:
        """Extract questions from a single already-open or navigated-to URL."""
        page, browser, pw = self.connect_to_chrome_session(debug_port)
        questions:   List[Dict] = []
        survey_name: str        = "Unknown Survey"

        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30_000)
            page.wait_for_timeout(2500)

            try:
                survey_name = page.evaluate(_SURVEY_NAME_JS) or "Unknown Survey"
            except Exception:
                pass

            page_num = 0
            while len(questions) < max_q:
                page_num += 1
                if page.evaluate(_DQ_JS):
                    break

                new_qs = page.evaluate(_EXTRACT_JS)
                if not isinstance(new_qs, list) or not new_qs:
                    break

                questions.extend(new_qs)
                if not self._advance_page(page):
                    break
                page.wait_for_timeout(1800)

        finally:
            try:
                pw.stop()
            except Exception:
                pass

        return questions[:max_q], survey_name

    def _advance_page(self, page) -> bool:
        """Click the Next button. Returns True if found and clicked."""
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
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Simulation fallback (no Chrome needed)
    # ------------------------------------------------------------------

    def _simulate(
        self, url: str, max_q: int
    ) -> Tuple[List[Dict], str]:
        survey_name = "Sample Consumer Survey"
        return self._make_simulated_questions(url, survey_name, max_q), survey_name

    def _simulate_all(
        self,
        account_id: int,
        site_id: int,
        url: str,
        max_surveys: int,
        t0: float,
    ) -> Dict[str, Any]:
        """Simulate multiple surveys for development / no-Chrome mode."""
        survey_names = [
            "Consumer Habits Survey",
            "Technology Usage Study",
            "Health & Wellness Poll",
            "Shopping Preferences Survey",
            "Media Consumption Study",
        ][:max_surveys]

        all_questions: List[Dict]  = []
        survey_results: List[Dict] = []
        total_inserted             = 0

        for i, name in enumerate(survey_names):
            batch_id  = f"topsurveys_sim_{account_id}_{int(t0)}_{i}"
            questions = self._make_simulated_questions(url, name, 10)
            for q in questions:
                q["survey_name"] = name

            inserted = self.save_questions_to_db(
                account_id, site_id, questions, batch_id, survey_name=name
            )
            all_questions.extend(questions)
            total_inserted += inserted
            survey_results.append({
                "survey_label": name,
                "survey_name":  name,
                "status":       "success",
                "questions":    len(questions),
                "inserted":     inserted,
                "batch_id":     batch_id,
            })

        return {
            "success":                True,
            "questions":              all_questions,
            "questions_found":        len(all_questions),
            "inserted":               total_inserted,
            "surveys_found":          len(survey_names),
            "surveys_processed":      len(survey_names),
            "surveys_successful":     len(survey_names),
            "surveys_failed":         0,
            "survey_results":         survey_results,
            "batch_id":               f"topsurveys_sim_{account_id}_{int(t0)}",
            "execution_time_seconds": round(time.time() - t0, 2),
        }

    def _make_simulated_questions(
        self, url: str, survey_name: str, max_q: int
    ) -> List[Dict]:
        templates = [
            {"text": "What is your age?", "type": "multiple_choice",
             "opts": ["18–24","25–34","35–44","45–54","55–64","65+"]},
            {"text": "What is your gender?", "type": "multiple_choice",
             "opts": ["Male","Female","Non-binary","Prefer not to say"]},
            {"text": "What is your employment status?", "type": "multiple_choice",
             "opts": ["Employed full-time","Employed part-time","Self-employed",
                      "Student","Unemployed","Retired"]},
            {"text": "What is your annual household income?", "type": "multiple_choice",
             "opts": ["Under $25,000","$25,000–$49,999","$50,000–$74,999",
                      "$75,000–$99,999","$100,000+"]},
            {"text": "What is your highest level of education?", "type": "multiple_choice",
             "opts": ["High school","Some college","Bachelor's degree",
                      "Master's degree","Doctorate"]},
            {"text": "How often do you shop online?", "type": "multiple_choice",
             "opts": ["Daily","Weekly","Monthly","Rarely","Never"]},
            {"text": "Which devices do you own?", "type": "checkbox",
             "opts": ["Smartphone","Tablet","Laptop","Smart TV","None"]},
            {"text": "Do you have children under 18?", "type": "multiple_choice",
             "opts": ["Yes","No"]},
            {"text": "How satisfied are you with your internet service?",
             "type": "rating", "opts": ["1","2","3","4","5"]},
            {"text": "What is your primary mode of transport?", "type": "multiple_choice",
             "opts": ["Personal vehicle","Public transit","Rideshare","Bicycle","Walk"]},
        ]
        random.shuffle(templates)
        out = []
        for i, t in enumerate(templates[:max_q]):
            out.append({
                "question_text":     t["text"],
                "question_type":     t["type"],
                "question_category": "screener",
                "required":          True,
                "order_index":       i,
                "page_url":          url,
                "click_element":     f"[data-q-idx='{i}']",
                "input_element":     f"[data-q-idx='{i}'] input",
                "submit_element":    "button[type='submit']",
                "options":           t.get("opts", []),
                "survey_name":       survey_name,
                "metadata":          {"source": "topsurveys", "simulated": True},
            })
        return out