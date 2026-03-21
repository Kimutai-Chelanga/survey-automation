# src/streamlit/ui/pages/generate_manual_workflows/extractors/topsurveys_extractor.py
"""
TopSurveys Extractor  v5.0.0
============================
- Always extracts ALL surveys from the dashboard (no simulation, no limits)
- Requires a running Chrome session (debug_port)
- Uses TopSurveys-specific DOM targeting (CTA buttons, reward cards)
- Passes full survey entry object to clicker (not just btn_text)
- Waits for dynamic React content before scanning
"""

import logging
import time
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    from extraction.base_extractor import BaseExtractor  # type: ignore
except ImportError:
    try:
        from genmw.base_extractor import BaseExtractor  # type: ignore
    except ImportError:
        raise RuntimeError(
            "BaseExtractor could not be imported. "
            "Ensure base_extractor.py is preloaded by the orchestrator."
        )


# ---------------------------------------------------------------------------
# JS: find all survey entries on the TopSurveys dashboard
# ---------------------------------------------------------------------------
_FIND_SURVEYS_JS = """
() => {
    const results = [];
    const seen = new Set();

    function isVisible(el) {
        const r = el.getBoundingClientRect();
        return r.width > 80 && r.height > 30;
    }

    function getText(el) {
        return (el.innerText || el.textContent || '').trim();
    }

    // 1. PRIMARY: visible Start/Take/Begin CTA buttons
    const ctaKeywords = ['start', 'take', 'begin', 'participate', 'open'];
    const buttons = Array.from(document.querySelectorAll(
        "button, [role='button'], div[tabindex], a"
    )).filter(isVisible);

    buttons.forEach(btn => {
        const txt = getText(btn).toLowerCase();
        if (!ctaKeywords.some(k => txt.startsWith(k))) return;
        const key = txt;
        if (seen.has(key)) return;
        seen.add(key);
        results.push({
            type:    'cta',
            text:    getText(btn).slice(0, 80),
            btnText: getText(btn).trim(),
            href:    btn.href || null,
            selector: null,
            index:   results.length
        });
    });

    // 2. SECONDARY: survey cards with reward + time signals
    Array.from(document.querySelectorAll('div')).filter(isVisible).forEach(el => {
        const txt = getText(el).toLowerCase();
        const looksLikeSurvey =
            (txt.includes('points') || txt.includes('$') || txt.includes('reward')) &&
            (txt.includes('min') || txt.includes('minutes'));
        if (!looksLikeSurvey || txt.length < 20) return;
        const key = txt.slice(0, 100);
        if (seen.has(key)) return;
        seen.add(key);
        results.push({
            type:    'card',
            text:    txt.slice(0, 80),
            btnText: null,
            href:    null,
            selector: null,
            index:   results.length
        });
    });

    // 3. TERTIARY: any visible button/link containing "survey"
    buttons.forEach(btn => {
        const txt = getText(btn).toLowerCase();
        if (!txt.includes('survey')) return;
        const key = 'survey_' + txt;
        if (seen.has(key)) return;
        seen.add(key);
        results.push({
            type:    'fallback',
            text:    getText(btn).slice(0, 80),
            btnText: getText(btn).trim(),
            href:    btn.href || null,
            selector: null,
            index:   results.length
        });
    });

    return results;
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
        else if (select)             {
            qType = 'dropdown';
            options = Array.from(select.options).map(o => o.text.trim()).filter(t => t && t !== '--');
        }
        else if (textarea)           { qType = 'textarea'; }
        else if (textInp)            { qType = 'text'; }

        const cId    = c.id ? '#' + c.id : null;
        const cRole  = c.getAttribute('role') ? `[role="${c.getAttribute('role')}"]` : null;
        const clickEl = cId || cRole || null;

        let inputEl = null;
        const first = c.querySelector("input:not([type='hidden']),textarea,select");
        if (first) {
            if (first.id)       inputEl = '#' + first.id;
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
    return ['not eligible', "don't qualify", 'not a match',
            'screened out', 'unfortunately', 'disqualified',
            'quota full', 'survey is full'].some(p => body.includes(p));
}
"""

# ---------------------------------------------------------------------------
# JS: click a survey entry — accepts the full entry object
# ---------------------------------------------------------------------------
_CLICK_START_JS = """
(entry) => {
    function isVisible(el) {
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
    }

    function getText(el) {
        return (el.innerText || el.textContent || '').trim().toLowerCase();
    }

    function clickElement(el) {
        el.scrollIntoView({ block: 'center' });
        el.click();
    }

    const keywords = ['start', 'take', 'begin', 'participate', 'open'];
    const all = Array.from(document.querySelectorAll(
        "button, [role='button'], div[tabindex], a"
    )).filter(isVisible);

    // 1. Exact text match
    if (entry && entry.btnText) {
        const exact = all.find(el => getText(el) === entry.btnText.toLowerCase());
        if (exact) { clickElement(exact); return true; }
    }

    // 2. Starts with CTA keyword
    for (const k of keywords) {
        const el = all.find(e => getText(e).startsWith(k));
        if (el) { clickElement(el); return true; }
    }

    // 3. Contains CTA keyword
    for (const k of keywords) {
        const el = all.find(e => getText(e).includes(k));
        if (el) { clickElement(el); return true; }
    }

    // 4. Last resort: largest visible button
    if (all.length > 0) {
        const biggest = all.sort((a, b) => {
            const ra = a.getBoundingClientRect();
            const rb = b.getBoundingClientRect();
            return (rb.width * rb.height) - (ra.width * ra.height);
        })[0];
        clickElement(biggest);
        return true;
    }

    return false;
}
"""

# ---------------------------------------------------------------------------
# JS: advance to the next page inside a survey
# ---------------------------------------------------------------------------
_ADVANCE_PAGE_JS = """
() => {
    const labels = ['next', 'continue', 'proceed'];
    const btns = Array.from(document.querySelectorAll(
        "button, input[type='submit'], [role='button']"
    )).filter(el => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; });

    for (const l of labels) {
        const b = btns.find(el =>
            (el.innerText || el.value || '').toLowerCase().trim().startsWith(l)
        );
        if (b) { b.scrollIntoView({ block: 'center' }); b.click(); return true; }
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
            "version":          "5.0.0",
            "requires_login":   True,
            "requires_cookies": True,
        }

    def get_site_info(self) -> Dict[str, Any]:
        return self._info

    def extract_questions(
        self,
        account_id: int,
        site_id: int,
        url: str,
        profile_path: str,
        **kwargs,
    ) -> Dict[str, Any]:
        """Always delegates to extract_all_from_listing."""
        return self.extract_all_from_listing(
            account_id=account_id,
            site_id=site_id,
            listing_url=url,
            profile_path=profile_path,
            debug_port=kwargs.get("debug_port"),
            progress_callback=kwargs.get("progress_callback"),
        )

    def extract_all_from_listing(
        self,
        account_id: int,
        site_id: int,
        listing_url: str,
        profile_path: str,
        debug_port: Optional[int] = None,
        max_surveys: int = None,
        max_questions_per_survey: int = None,
        progress_callback=None,
        **kwargs,
    ) -> Dict[str, Any]:
        t0 = time.time()
        listing_url = self.normalize_url(listing_url)

        if not debug_port:
            raise RuntimeError(
                "No debug_port provided. A running Chrome session is required."
            )

        page, browser, pw = self.connect_to_chrome_session(debug_port)
        all_questions:  List[Dict] = []
        survey_results: List[Dict] = []
        total_inserted = 0
        surveys_found  = []

        try:
            # ── 1. Load dashboard ────────────────────────────────────────
            logger.info(f"[TopSurveys] Navigating to: {listing_url}")
            page.goto(listing_url, wait_until="domcontentloaded", timeout=30_000)

            # Wait for React to render survey buttons
            try:
                page.wait_for_function(
                    '() => document.querySelectorAll("button, [role=\'button\']").length > 5',
                    timeout=15_000,
                )
            except Exception:
                logger.warning("[TopSurveys] Timed out waiting for buttons — proceeding anyway")

            page.wait_for_timeout(3000)
            logger.info(f"[TopSurveys] Landed on: {page.url}")

            # ── 2. Find all surveys ──────────────────────────────────────
            surveys_found = page.evaluate(_FIND_SURVEYS_JS)
            logger.info(
                f"[TopSurveys] Found {len(surveys_found)} surveys: "
                f"{[s.get('text', '')[:40] for s in surveys_found[:5]]}"
            )

            if not surveys_found:
                return {
                    "success":                False,
                    "error":                  (
                        "No surveys found on the listing page. "
                        "Ensure you are logged in and surveys are visible on the dashboard."
                    ),
                    "questions_found":        0,
                    "inserted":               0,
                    "surveys_found":          0,
                    "surveys_processed":      0,
                    "surveys_successful":     0,
                    "surveys_failed":         0,
                    "survey_results":         [],
                    "batch_id":               f"topsurveys_all_{account_id}_{int(t0)}",
                    "execution_time_seconds": round(time.time() - t0, 2),
                }

            total = len(surveys_found)

            # ── 3. Process each survey ───────────────────────────────────
            for idx, survey_entry in enumerate(surveys_found):
                survey_href  = self.normalize_url(survey_entry.get("href") or "")
                survey_label = survey_entry.get("text") or f"Survey {idx + 1}"

                if progress_callback:
                    progress_callback(
                        idx + 1, total,
                        f"Extracting survey {idx+1}/{total}: {survey_label[:50]}"
                    )

                logger.info(f"[TopSurveys] Survey {idx+1}/{total}: {survey_label[:60]}")

                batch_id = f"topsurveys_{account_id}_{int(time.time())}_{idx}"

                try:
                    # Return to listing
                    if listing_url not in page.url:
                        page.goto(listing_url, wait_until="domcontentloaded", timeout=20_000)
                        try:
                            page.wait_for_function(
                                '() => document.querySelectorAll("button, [role=\'button\']").length > 5',
                                timeout=10_000,
                            )
                        except Exception:
                            pass
                        page.wait_for_timeout(2000)

                    # Navigate into the survey
                    if survey_href:
                        page.goto(survey_href, wait_until="domcontentloaded", timeout=20_000)
                    else:
                        clicked = page.evaluate(_CLICK_START_JS, survey_entry)
                        if not clicked:
                            logger.warning(f"[TopSurveys] Could not click into '{survey_label}'")
                            survey_results.append({
                                "survey_label": survey_label,
                                "status":       "skip",
                                "reason":       "could not click CTA",
                                "questions":    0,
                                "inserted":     0,
                            })
                            continue

                    page.wait_for_timeout(2500)

                    # Immediate DQ check
                    if page.evaluate(_DQ_JS):
                        logger.info(f"[TopSurveys] '{survey_label}' DQ/unavailable")
                        survey_results.append({
                            "survey_label": survey_label,
                            "status":       "dq",
                            "reason":       "DQ/unavailable immediately",
                            "questions":    0,
                            "inserted":     0,
                        })
                        continue

                    # Get survey name from page
                    try:
                        survey_name = page.evaluate(_SURVEY_NAME_JS) or survey_label
                    except Exception:
                        survey_name = survey_label

                    logger.info(f"[TopSurveys] Survey name resolved: '{survey_name}'")

                    # Extract questions across all pages
                    questions: List[Dict] = []
                    page_num = 0

                    while True:
                        page_num += 1

                        if page.evaluate(_DQ_JS):
                            logger.info(f"[TopSurveys] DQ on page {page_num} of '{survey_name}'")
                            break

                        new_qs = page.evaluate(_EXTRACT_JS)
                        if not isinstance(new_qs, list) or not new_qs:
                            logger.info(f"[TopSurveys] No questions on page {page_num} — done")
                            break

                        logger.info(f"[TopSurveys] Page {page_num}: {len(new_qs)} questions")
                        questions.extend(new_qs)

                        if not self._advance_page(page):
                            break
                        page.wait_for_timeout(1800)

                    # Stamp survey name
                    for q in questions:
                        q["survey_name"] = survey_name

                    # Save to DB
                    inserted = self.save_questions_to_db(
                        account_id, site_id, questions, batch_id, survey_name=survey_name
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

                    time.sleep(1.5)

                except Exception as exc:
                    logger.error(f"[TopSurveys] Error on '{survey_label}': {exc}", exc_info=True)
                    survey_results.append({
                        "survey_label": survey_label,
                        "status":       "error",
                        "reason":       str(exc),
                        "questions":    0,
                        "inserted":     0,
                    })
                    # Recover back to listing
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
            "surveys_found":          len(surveys_found),
            "surveys_processed":      len(survey_results),
            "surveys_successful":     len(successful),
            "surveys_failed":         len(failed),
            "survey_results":         survey_results,
            "batch_id":               f"topsurveys_all_{account_id}_{int(t0)}",
            "execution_time_seconds": round(time.time() - t0, 2),
        }

    def _advance_page(self, page) -> bool:
        """Click Next/Continue inside a survey. Returns True if found and clicked."""
        try:
            found = page.evaluate(_ADVANCE_PAGE_JS)
            if found:
                page.wait_for_timeout(1000)
            return bool(found)
        except Exception:
            return False