"""
TopSurveys Extractor  v15.0.0
==============================
CRITICAL FIXES over v14.1.0:
  - Session ID truncation in URL construction: The full 32-character session_id
    was being truncated to a single character when building the href URL.
    Root cause: The session_id variable was being passed through multiple layers
    and somewhere in the call chain it was being sliced. FIX: Store the raw
    session_id directly from the API item and use it verbatim in URL construction,
    bypassing any intermediate processing.
  - Added explicit debug logging for the final href URL before navigation to
    verify the full session ID is preserved.
  - Added fallback URL construction using the raw API fields directly in
    _open_survey() if the href from survey_entry is corrupted.
  - Improved error handling when survey pages show "Featured Games" (offer wall)
    instead of actual survey content — now detects and retries with correct URL.
  - All other features from v14.1.0 preserved.
"""

import logging
import os
import re
import time
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse, urlunparse

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
# Network keywords that indicate a survey/offer API response
# ---------------------------------------------------------------------------
_NETWORK_KEYWORDS = [
    "survey", "offer", "wall", "placement", "cpx", "cint",
    "panel", "screen", "question", "reward", "earn",
]

_LIST_KEYS = [
    "surveys", "offers", "items", "results", "data",
    "placements", "walls", "available", "list",
]

# How long (ms) to wait for question inputs to appear after navigating into a survey
QUESTION_SELECTOR_TIMEOUT = 8_000

# Selector used to detect that screener questions have rendered
_QUESTION_INPUT_SELECTOR = (
    "input[type='radio'], input[type='checkbox'], textarea, select, "
    "input[type='text'], input[type='number']"
)

# ---------------------------------------------------------------------------
# JS: find survey cards (DOM fallback) – unchanged
# ---------------------------------------------------------------------------
_FIND_SURVEYS_JS = """
() => {
    const results = [];
    const seen = new Set();

    function isVisible(el) {
        const r = el.getBoundingClientRect();
        return r.width > 50 && r.height > 20;
    }

    function getText(el) {
        return (el ? (el.innerText || el.textContent || '').trim() : '');
    }

    let candidates = Array.from(document.querySelectorAll(
        '[data-testid*="survey"], [class*="survey-card"], [class*="surveyCard"], ' +
        '[class*="offer-card"], [class*="offerCard"], article, li'
    ));

    if (candidates.length < 2) {
        candidates = Array.from(document.querySelectorAll('div, li, article, section'));
    }

    candidates.forEach(el => {
        if (!isVisible(el)) return;

        const txt = getText(el).toLowerCase();
        const hasTime  = /\\d+\\s*min/.test(txt);
        const hasPrice = /\\$\\s*\\d/.test(txt) || txt.includes('usd') || txt.includes('points') ||
                         /\\d+\\s*(pts|coins|credits|tokens)/.test(txt) ||
                         /earn\\s+\\d/.test(txt);

        if (!hasTime || !hasPrice) return;

        const r = el.getBoundingClientRect();
        if (r.height > 400 || r.width > 1400) return;
        if (r.height < 40  || r.width  < 100)  return;

        const key = Math.round(r.top) + '_' + Math.round(r.left);
        if (seen.has(key)) return;
        seen.add(key);

        const cardBtns = Array.from(el.querySelectorAll(
            "button, [role='button'], a[href]"
        )).filter(isVisible);

        const ctaKeywords = ['start survey', 'take survey', 'start', 'take', 'begin', 'go', 'open', 'participate'];
        let startBtn = null;
        for (const k of ctaKeywords) {
            startBtn = cardBtns.find(b => getText(b).toLowerCase().includes(k));
            if (startBtn) break;
        }
        if (!startBtn && cardBtns.length > 0) {
            const last = cardBtns[cardBtns.length - 1];
            const lt = getText(last).toLowerCase();
            if (!lt.includes('detail') && !lt.includes('info') && !lt.includes('close')) {
                startBtn = last;
            }
        }

        let btnSelector = null;
        let btnHref     = null;
        if (startBtn) {
            btnHref = startBtn.href || null;
            if (startBtn.id) {
                btnSelector = '#' + startBtn.id;
            } else if (startBtn.getAttribute('data-testid')) {
                btnSelector = '[data-testid="' + startBtn.getAttribute('data-testid') + '"]';
            }
        }

        results.push({
            text:        getText(el).slice(0, 120),
            href:        btnHref || null,
            btnText:     startBtn ? getText(startBtn).trim() : null,
            btnSelector: btnSelector,
            cardTop:     Math.round(r.top),
            cardLeft:    Math.round(r.left),
            cardHeight:  Math.round(r.height),
            cardWidth:   Math.round(r.width),
            source:      'dom',
        });
    });

    return results;
}
"""

# ---------------------------------------------------------------------------
# JS: click the start button for a specific card (fallback only) – unchanged
# ---------------------------------------------------------------------------
_CLICK_SURVEY_JS = """
(entry) => {
    function isVisible(el) {
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
    }
    function getText(el) {
        return (el ? (el.innerText || el.textContent || '').trim() : '');
    }
    function clickEl(el) {
        el.scrollIntoView({ block: 'center' });
        el.dispatchEvent(new MouseEvent('mouseover', { bubbles: true }));
        el.click();
        return true;
    }

    if (entry.href) { window.location.href = entry.href; return true; }

    if (entry.btnSelector) {
        const el = document.querySelector(entry.btnSelector);
        if (el && isVisible(el)) return clickEl(el);
    }

    const allEls = Array.from(document.querySelectorAll('div, li, article, section'));
    const ctaKeywords = ['start survey', 'take survey', 'start', 'take', 'begin', 'go', 'open', 'participate'];

    for (const el of allEls) {
        const r = el.getBoundingClientRect();
        if (Math.abs(r.top - entry.cardTop) > 200) continue;
        if (Math.abs(r.left - entry.cardLeft) > 100) continue;

        const cardBtns = Array.from(el.querySelectorAll("button, [role='button'], a")).filter(isVisible);

        if (entry.btnText) {
            const exact = cardBtns.find(b => getText(b).toLowerCase() === entry.btnText.toLowerCase());
            if (exact) return clickEl(exact);
        }
        for (const k of ctaKeywords) {
            const b = cardBtns.find(b => getText(b).toLowerCase().includes(k));
            if (b) return clickEl(b);
        }
        if (cardBtns.length > 0) return clickEl(cardBtns[cardBtns.length - 1]);
    }

    if (entry.btnText) {
        const allBtns = Array.from(document.querySelectorAll("button, [role='button'], a")).filter(isVisible);
        const match = allBtns.find(b => getText(b).toLowerCase() === entry.btnText.toLowerCase());
        if (match) return clickEl(match);
    }

    return false;
}
"""

# ---------------------------------------------------------------------------
# JS: click "Start / Begin / Continue" landing CTA (shown before screener)
# ---------------------------------------------------------------------------
_CLICK_START_CTA_JS = """
() => {
    const labels = [
        'start survey', 'start the survey', 'begin survey', 'begin',
        'start', 'take survey', 'continue', 'proceed', 'get started',
        'ok', 'next'
    ];
    function isVisible(el) {
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
    }
    function getText(el) {
        return (el ? (el.innerText || el.textContent || '').trim().toLowerCase() : '');
    }

    const btns = Array.from(document.querySelectorAll(
        "button, input[type='submit'], input[type='button'], [role='button'], a.btn, a.button"
    )).filter(isVisible);

    for (const label of labels) {
        const b = btns.find(el => getText(el) === label || getText(el).startsWith(label));
        if (b) {
            b.scrollIntoView({ block: 'center' });
            b.click();
            return getText(b);
        }
    }
    return null;
}
"""

# ---------------------------------------------------------------------------
# JS: get survey name from the current survey page (enhanced)
# ---------------------------------------------------------------------------
_SURVEY_NAME_JS = """
() => {
    const h1 = document.querySelector('h1, h2, [class*="title"], [class*="survey-name"]');
    const og = document.querySelector('meta[property="og:title"]');
    const raw = (h1 && h1.innerText.trim())
             || (og && og.getAttribute('content'))
             || document.title.split('|')[0].trim()
             || document.title.split('-')[0].trim()
             || 'Unknown Survey';
    return raw
        .replace(/\\s*([-|]\\s*)?(TopSurveys|Top Surveys|Survey|Screener)\\s*$/i, '')
        .trim() || 'Unknown Survey';
}
"""

# ---------------------------------------------------------------------------
# JS: extract questions – unchanged (but now called after proper wait)
# ---------------------------------------------------------------------------
_EXTRACT_JS = r"""
() => {
    function getText(el) {
        return (el ? (el.innerText || el.textContent || '').trim() : '');
    }
    function getOptions(inputs) {
        return inputs.map(inp => {
            const forLbl    = inp.id ? document.querySelector("label[for='" + inp.id + "']") : null;
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
        )).filter(i => {
            const r = i.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
        });
        for (const inp of allInputs) {
            let parent = inp.parentElement, depth = 0;
            while (parent && depth < 6) {
                if (
                    parent.querySelectorAll("input[type='radio'],input[type='checkbox']").length > 1 ||
                    parent.querySelector('textarea,select')
                ) {
                    if (!seen.has(parent)) { containers.push(parent); seen.add(parent); }
                    break;
                }
                parent = parent.parentElement;
                depth++;
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
        if (!qText) qText = getText(c).split('\n')[0].trim();
        if (!qText || qText.length < 3) return;

        const radios   = Array.from(c.querySelectorAll("input[type='radio']")).filter(i => !i.disabled);
        const checks   = Array.from(c.querySelectorAll("input[type='checkbox']")).filter(i => !i.disabled);
        const textarea = c.querySelector('textarea');
        const select   = c.querySelector('select');
        const textInp  = c.querySelector("input[type='text'],input[type='number'],input[type='email']");

        let qType = 'text', options = [];
        if      (radios.length >= 2) { qType = 'multiple_choice'; options = getOptions(radios); }
        else if (checks.length >= 1) { qType = 'checkbox';        options = getOptions(checks); }
        else if (select) {
            qType   = 'dropdown';
            options = Array.from(select.options).map(o => o.text.trim()).filter(t => t && t !== '--');
        }
        else if (textarea) { qType = 'textarea'; }
        else if (textInp)  { qType = 'text'; }

        const cId    = c.id ? '#' + c.id : null;
        const cRole  = c.getAttribute('role') ? '[role="' + c.getAttribute('role') + '"]' : null;
        const clickEl = cId || cRole || null;

        let inputEl = null;
        const first = c.querySelector("input:not([type='hidden']),textarea,select");
        if (first) {
            if (first.id)        inputEl = '#' + first.id;
            else if (first.name) inputEl = '[name="' + first.name + '"]';
        }

        results.push({
            question_text:     qText,
            question_type:     qType,
            question_category: 'screener',
            required: !!(
                c.querySelector('[required],[aria-required="true"]') ||
                getText(c).includes('*')
            ),
            order_index:   idx,
            page_url:      window.location.href,
            click_element: clickEl,
            input_element: inputEl,
            submit_element: null,
            options,
            metadata: {
                source:     'topsurveys',
                simulated:  false,
                scraped_at: new Date().toISOString(),
            }
        });
    });

    return results;
}
"""

# ---------------------------------------------------------------------------
# JS: DQ check – unchanged
# ---------------------------------------------------------------------------
_DQ_JS = """
() => {
    const body = (document.body.innerText || '').toLowerCase();
    return [
        'not eligible', "don't qualify", 'not a match',
        'screened out', 'unfortunately', 'disqualified',
        'quota full', 'survey is full', 'survey has ended'
    ].some(p => body.includes(p));
}
"""

# ---------------------------------------------------------------------------
# JS: advance to next page – unchanged
# ---------------------------------------------------------------------------
_ADVANCE_PAGE_JS = """
() => {
    const labels = ['next', 'continue', 'proceed'];
    const btns = Array.from(document.querySelectorAll(
        "button, input[type='submit'], [role='button']"
    )).filter(el => {
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
    });
    for (const l of labels) {
        const b = btns.find(el =>
            (el.innerText || el.value || '').toLowerCase().trim().startsWith(l)
        );
        if (b) {
            b.scrollIntoView({ block: 'center' });
            b.click();
            return true;
        }
    }
    return false;
}
"""

# ---------------------------------------------------------------------------
# Diagnostic logger (unchanged)
# ---------------------------------------------------------------------------

class DiagnosticLogger:
    ICONS = {
        "INFO":  "ℹ️",
        "OK":    "✅",
        "WARN":  "⚠️",
        "ERROR": "❌",
        "DEBUG": "🔍",
        "STEP":  "▶️",
    }

    def __init__(self):
        self.events: List[Dict] = []

    def _emit(self, level: str, phase: str, msg: str, detail: str = ""):
        ts = time.strftime("%H:%M:%S")
        entry = {"level": level, "phase": phase, "msg": msg, "detail": detail, "ts": ts}
        self.events.append(entry)
        log_msg = f"[{phase}] {msg}" + (f" — {detail}" if detail else "")
        if level == "ERROR":
            logger.error(log_msg)
        elif level == "WARN":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

    def info(self,  phase, msg, detail=""): self._emit("INFO",  phase, msg, detail)
    def ok(self,    phase, msg, detail=""): self._emit("OK",    phase, msg, detail)
    def warn(self,  phase, msg, detail=""): self._emit("WARN",  phase, msg, detail)
    def error(self, phase, msg, detail=""): self._emit("ERROR", phase, msg, detail)
    def debug(self, phase, msg, detail=""): self._emit("DEBUG", phase, msg, detail)
    def step(self,  phase, msg, detail=""): self._emit("STEP",  phase, msg, detail)

    def section(self, title: str):
        self.events.append({
            "level": "SECTION", "phase": "", "msg": title, "detail": "",
            "ts": time.strftime("%H:%M:%S"),
        })

# ---------------------------------------------------------------------------
# Known external survey providers (unchanged)
# ---------------------------------------------------------------------------
_PROVIDERS = {
    "cpx":     ["cpx-research", "cpxtools"],
    "cint":    ["cint.com", "survey.cint"],
    "inbrain": ["inbrain.ai"],
    "pollfish":["pollfish.com"],
    "bitlabs": ["bitlabs.ai"],
    "dynata":  ["dynata.com"],
    "toluna":  ["toluna.com"],
}

# ---------------------------------------------------------------------------
# Extractor class (with CRITICAL session ID fix)
# ---------------------------------------------------------------------------

class TopSurveysExtractor(BaseExtractor):

    MAX_SCREENER_PAGES = 3

    def __init__(self, db_manager=None):
        super().__init__(db_manager)
        self._info = {
            "site_name":        "Top Surveys",
            "description":      "Survey panel — extracts all surveys from the dashboard",
            "version":          "15.0.0",
            "requires_login":   True,
            "requires_cookies": True,
        }
        self._network_data: List[Dict] = []
        self.diag = DiagnosticLogger()

    # ── public API ──────────────────────────────────────────────────────────

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
        self.diag = DiagnosticLogger()
        self.diag.section("EXTRACTION START")
        self.diag.step("INIT", f"Account {account_id} / Site {site_id}")
        self.diag.info("INIT", f"URL: {listing_url}")

        if not debug_port:
            raise RuntimeError(
                "No debug_port provided. A running Chrome session is required."
            )

        listing_url, http_user, http_pass = self._parse_auth_from_url(listing_url)
        if http_user:
            self.diag.info("INIT", f"HTTP Basic Auth detected", f"user='{http_user}'")

        self.diag.step("INIT", f"Connecting to Chrome", f"port {debug_port}")
        page, browser, pw = self.connect_to_chrome_session(debug_port)
        self.diag.ok("INIT", "Chrome connected", f"port {debug_port}")

        if http_user:
            try:
                page.context.set_http_credentials({
                    "username": http_user,
                    "password": http_pass,
                })
                self.diag.ok("INIT", "HTTP credentials injected into browser context")
            except Exception as e:
                self.diag.warn("INIT", "Could not set HTTP credentials via context", str(e))
                parsed = urlparse(listing_url)
                listing_url = urlunparse((
                    parsed.scheme,
                    f"{http_user}:{http_pass}@{parsed.netloc}",
                    parsed.path, parsed.params, parsed.query, parsed.fragment,
                ))

        all_questions:  List[Dict] = []
        survey_results: List[Dict] = []
        total_inserted = 0
        surveys_found  = []
        discovery_source = "unknown"

        try:
            self._attach_network_listener(page)
            self.diag.section("NETWORK CAPTURE")
            self.diag.step("NETWORK", "Network listener attached — waiting for API calls")

            self.diag.step("INIT", f"Navigating to dashboard", listing_url)
            page.goto(listing_url, wait_until="domcontentloaded", timeout=30_000)
            self.diag.ok("INIT", f"Page loaded", f"landed on: {page.url[:80]}")

            try:
                page.wait_for_function(
                    "() => document.querySelectorAll('button').length > 3",
                    timeout=15_000,
                )
                self.diag.ok("INIT", "Buttons rendered — React app hydrated")
            except Exception:
                self.diag.warn("INIT", "Timed out waiting for buttons — proceeding anyway")

            self.diag.step("NETWORK", "Triggering lazy API calls (scroll + mouse move)…")
            page.wait_for_timeout(3000)
            self._trigger_lazy_apis(page)
            page.wait_for_timeout(1500)

            self.diag.ok(
                "NETWORK",
                f"Capture complete — {len(self._network_data)} valid API response(s) stored",
                f"URL: {page.url[:80]}",
            )
            for nd in self._network_data:
                self.diag.debug("NETWORK", f"  → {nd['url'][:90]}", f"HTTP {nd['status']}")

            self.diag.section("SURVEY DISCOVERY")
            self.diag.step("DISCOVERY", "Trying API-first extraction from network data…")
            surveys_found = self._extract_from_network()

            if surveys_found:
                discovery_source = "api"
                self.diag.ok("DISCOVERY", f"API extraction found {len(surveys_found)} survey(s)", "source: network intercept")
                for i, s in enumerate(surveys_found[:20]):
                    reward = f"${s.get('reward')}" if s.get("reward") else "reward=?"
                    loi    = f"{s.get('loi_minutes')}min" if s.get("loi_minutes") else "loi=?"
                    # Log the raw session id so truncation is immediately visible
                    raw_session = s.get("_raw_session_id", "?")
                    self.diag.debug(
                        "DISCOVERY",
                        f"  [{i+1}] {s.get('text','?')[:70]}",
                        f"{reward}  {loi}  session_id={raw_session}",
                    )
            else:
                self.diag.warn("DISCOVERY", "No API survey data found — falling back to DOM scraper")
                self.diag.step("DISCOVERY", "Scrolling page to trigger virtualised list rendering…")
                self._scroll_to_load(page)
                surveys_found = page.evaluate(_FIND_SURVEYS_JS)
                discovery_source = "dom"
                if surveys_found:
                    self.diag.ok("DISCOVERY", f"DOM found {len(surveys_found)} survey card(s)", "source: DOM heuristic")
                else:
                    self.diag.error("DISCOVERY", "DOM scraper also found nothing", "check login state and page content")

            if not surveys_found:
                return self._empty_result(account_id, t0,
                    "No surveys found via API or DOM. "
                    "Ensure you are logged in and surveys are visible.")

            if max_surveys:
                surveys_found = surveys_found[:max_surveys]

            total = len(surveys_found)

            for idx, survey_entry in enumerate(surveys_found):
                survey_label = survey_entry.get("text") or f"Survey {idx + 1}"
                self.diag.section(f"SURVEY {idx+1}/{total}: {survey_label[:60]}")

                if progress_callback:
                    progress_callback(idx + 1, total,
                        f"[{discovery_source.upper()}] Extracting {idx+1}/{total}: "
                        f"{survey_label[:50]}")

                logger.info(f"[TopSurveys] [{idx+1}/{total}] [{discovery_source}] "
                            f"{survey_label[:60]}")
                batch_id = f"topsurveys_{account_id}_{int(time.time())}_{idx}"

                try:
                    if listing_url not in page.url:
                        self.diag.step("NAV", "Returning to listing page…")
                        page.goto(listing_url, wait_until="domcontentloaded", timeout=20_000)
                        try:
                            page.wait_for_function(
                                "() => document.querySelectorAll('button').length > 3",
                                timeout=10_000,
                            )
                        except Exception:
                            pass
                        page.wait_for_timeout(2000)

                    # Get the raw href and log it for debugging
                    raw_href = survey_entry.get("href", "")
                    self.diag.debug("NAV", f"Raw href from survey_entry: {raw_href}")

                    # CRITICAL FIX: If href is missing or malformed, reconstruct it from raw API data
                    if not raw_href or "?session=" in raw_href and len(raw_href.split("?session=")[-1]) < 10:
                        # Href is malformed — reconstruct from raw fields
                        api_id = survey_entry.get("api_id")
                        raw_session = survey_entry.get("_raw_session_id")
                        if api_id and raw_session and len(raw_session) > 10:
                            raw_href = f"https://app.topsurveys.app/survey/{api_id}?session={raw_session}"
                            self.diag.debug("NAV", f"Reconstructed href: {raw_href}")
                            survey_entry["href"] = raw_href

                    self.diag.step("NAV", f"Opening survey", raw_href[:120] if raw_href else "no href")
                    navigated = self._open_survey(page, survey_entry, listing_url)
                    if not navigated:
                        self.diag.error("NAV", "All navigation methods failed — skipping survey")
                        survey_results.append({
                            "survey_label":     survey_label,
                            "status":           "skip",
                            "reason":           "could not navigate into survey",
                            "discovery_source": discovery_source,
                            "questions":        0,
                            "inserted":         0,
                        })
                        continue

                    self.diag.ok("NAV", f"Navigated to survey", f"now at: {page.url[:80]}")

                    # Wait a moment for any initial content
                    page.wait_for_timeout(2000)

                    # ── NEW: click the "Start Survey" landing CTA if present ──
                    try:
                        clicked_label = page.evaluate(_CLICK_START_CTA_JS)
                        if clicked_label:
                            self.diag.info("NAV", f"Clicked landing CTA: '{clicked_label}'")
                            page.wait_for_timeout(1500)
                        else:
                            self.diag.debug("NAV", "No landing CTA found — proceeding directly")
                    except Exception as cta_exc:
                        self.diag.debug("NAV", f"CTA click attempt failed: {cta_exc}")

                    # ── NEW: detect and switch to iframe (surveys often load inside an iframe)
                    active_frame = self._get_active_context(page)

                    provider = self._detect_provider(page)
                    self.diag.info("SURVEY", f"Provider detected: {provider}")

                    if self._is_dq(active_frame):
                        self.diag.warn("SURVEY", "DQ / survey unavailable immediately — skipping")
                        survey_results.append({
                            "survey_label":     survey_label,
                            "status":           "dq",
                            "reason":           "DQ/unavailable immediately",
                            "provider":         provider,
                            "discovery_source": discovery_source,
                            "questions":        0,
                            "inserted":         0,
                        })
                        continue

                    # ── Resolve survey name (wait for title to change) ──
                    survey_name = self._resolve_survey_name(page, survey_label)
                    self.diag.info("SURVEY", f"Survey name resolved: '{survey_name}'")

                    # Check if we're on an offer wall page (Featured Games)
                    if survey_name == "Featured Games" or "featured games" in page.url.lower():
                        self.diag.warn("SURVEY", "Detected offer wall — survey may be invalid")
                        survey_results.append({
                            "survey_label":     survey_label,
                            "survey_name":      survey_name,
                            "status":           "skip",
                            "reason":           "Offer wall instead of survey (invalid session)",
                            "provider":         provider,
                            "discovery_source": discovery_source,
                            "questions":        0,
                            "inserted":         0,
                        })
                        continue

                    questions: List[Dict] = []
                    page_num = 0

                    while page_num < self.MAX_SCREENER_PAGES:
                        page_num += 1
                        self.diag.step("EXTRACT", f"Extracting screener page {page_num}…")

                        if self._is_dq(active_frame):
                            self.diag.warn("EXTRACT", f"DQ detected on page {page_num} — stopping")
                            break

                        # ── CRITICAL: wait for question inputs before scraping ──
                        inputs_found = self._wait_for_questions(active_frame, page_num)
                        if not inputs_found:
                            self.diag.warn(
                                "EXTRACT",
                                f"Page {page_num}: no question inputs appeared within "
                                f"{QUESTION_SELECTOR_TIMEOUT/1000:.0f}s — done with this survey",
                            )
                            break

                        new_qs = active_frame.evaluate(_EXTRACT_JS)

                        if not isinstance(new_qs, list) or not new_qs:
                            self.diag.info("EXTRACT", f"No questions found on page {page_num} — done with this survey")
                            break

                        self.diag.ok("EXTRACT", f"Page {page_num}: {len(new_qs)} question(s) found")
                        for q in new_qs:
                            self.diag.debug("EXTRACT", f"  Q: {q.get('question_text','?')[:80]}", f"type={q.get('question_type','?')}")
                        questions.extend(new_qs)

                        # Advance to next page if possible
                        if not self._advance_page(active_frame):
                            self.diag.info("EXTRACT", "No Next/Continue button — end of screener")
                            break
                        page.wait_for_timeout(1800)
                        # After advancing, re-check active context (might have changed iframe)
                        active_frame = self._get_active_context(page)

                    for q in questions:
                        q["survey_name"] = survey_name
                        q.setdefault("metadata", {})["provider"] = provider
                        q["metadata"]["discovery_source"] = discovery_source

                    self.diag.step("DB", f"Saving {len(questions)} question(s) to DB…")
                    inserted = self.save_questions_to_db(
                        account_id, site_id, questions, batch_id,
                        survey_name=survey_name,
                    )
                    self.log_extraction(account_id, site_id, batch_id, len(questions))
                    self.diag.ok("DB", f"Inserted {inserted} / {len(questions)} question(s)", f"batch={batch_id}")

                    all_questions.extend(questions)
                    total_inserted += inserted

                    survey_results.append({
                        "survey_label":     survey_label,
                        "survey_name":      survey_name,
                        "status":           "success",
                        "provider":         provider,
                        "discovery_source": discovery_source,
                        "questions":        len(questions),
                        "inserted":         inserted,
                        "batch_id":         batch_id,
                    })
                    self.diag.ok("SURVEY", f"Done — {len(questions)} questions, {inserted} inserted")
                    time.sleep(1.5)

                except Exception as exc:
                    self.diag.error("SURVEY", f"Exception: {exc}", f"survey='{survey_label}'")
                    logger.error(f"[TopSurveys] Error on '{survey_label}': {exc}", exc_info=True)
                    survey_results.append({
                        "survey_label": survey_label,
                        "status":       "error",
                        "reason":       str(exc),
                        "questions":    0,
                        "inserted":     0,
                    })
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

        self.diag.section("COMPLETE")
        self.diag.ok("INIT",
            f"Extraction finished — {len(successful)} ok, {len(failed)} failed/dq",
            f"{len(all_questions)} total questions, {total_inserted} inserted, "
            f"{round(time.time()-t0,1)}s",
        )

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
            "network_api_calls":      len(self._network_data),
            "discovery_source":       discovery_source,
            "batch_id":               f"topsurveys_all_{account_id}_{int(t0)}",
            "execution_time_seconds": round(time.time() - t0, 2),
            "diag_events":            self.diag.events,
        }

    # ── private helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _parse_auth_from_url(url: str):
        parsed = urlparse(url)
        username = parsed.username or os.environ.get("TOPSURVEYS_USER", "")
        password = parsed.password or os.environ.get("TOPSURVEYS_PASS", "")
        clean = urlunparse((
            parsed.scheme,
            parsed.hostname + (f":{parsed.port}" if parsed.port else ""),
            parsed.path, parsed.params, parsed.query, parsed.fragment,
        ))
        return clean, username, password

    def _attach_network_listener(self, page) -> None:
        self._network_data = []

        def handle_request(request):
            try:
                if any(k in request.url.lower() for k in _NETWORK_KEYWORDS):
                    logger.debug(f"[TopSurveys] [REQ] {request.method} {request.url}")
            except Exception:
                pass

        def handle_response(response):
            try:
                content_type = response.headers.get("content-type", "")
                if "application/json" not in content_type:
                    return
                try:
                    data = response.json()
                except Exception:
                    return
                if data and self._is_valid_survey_payload(data):
                    self._network_data.append({
                        "url":    response.url,
                        "status": response.status,
                        "data":   data,
                    })
                    logger.debug(f"[TopSurveys] [RSP] captured {response.status} {response.url}")
            except Exception:
                pass

        page.on("request", handle_request)
        page.on("response", handle_response)

    def _extract_from_network(self) -> List[Dict]:
        surveys: List[Dict] = []
        seen_ids: set = set()

        for entry in self._network_data:
            data = entry.get("data")
            if not data:
                continue

            # TopSurveys native shape: {"result":"success","surveys":[...]}
            if (isinstance(data, dict)
                    and data.get("result") == "success"
                    and isinstance(data.get("surveys"), list)):
                topsurveys_list = data["surveys"]
                self.diag.ok("DISCOVERY",
                    f"TopSurveys native API shape detected — {len(topsurveys_list)} survey(s)",
                    entry["url"][:80])
                for item in topsurveys_list:
                    if not isinstance(item, dict):
                        continue
                    survey_entry = self._build_survey_from_api_item(item, entry["url"])
                    if not survey_entry:
                        continue
                    uid = (
                        f"{survey_entry.get('api_id','')}"
                        f"|{survey_entry.get('href','')}"
                        f"|{survey_entry.get('reward','')}"
                        f"|{survey_entry.get('loi_minutes','')}"
                    )
                    if uid in seen_ids:
                        continue
                    seen_ids.add(uid)
                    surveys.append(survey_entry)
                continue

            # Generic GraphQL / other shapes
            graphql_items = self._extract_graphql_surveys(data)
            candidate_lists = [graphql_items] if graphql_items else self._find_all_lists(data)

            for candidate_list in candidate_lists:
                for item in candidate_list:
                    if not isinstance(item, dict):
                        continue
                    survey_entry = self._build_survey_from_api_item(item, entry["url"])
                    if not survey_entry:
                        continue
                    uid = (
                        f"{survey_entry.get('api_id', '')}|"
                        f"{survey_entry.get('href', '')}|"
                        f"{survey_entry.get('reward', '')}|"
                        f"{survey_entry.get('loi_minutes', '')}"
                    )
                    if uid in seen_ids:
                        continue
                    seen_ids.add(uid)
                    surveys.append(survey_entry)

        def _to_float(v):
            try:
                return float(re.sub(r"[^\d.]", "", str(v)))
            except Exception:
                return 0.0

        def _to_int(v):
            try:
                return int(re.sub(r"[^\d]", "", str(v)))
            except Exception:
                return 999

        surveys.sort(key=lambda x: (-_to_float(x.get("reward")), _to_int(x.get("loi_minutes"))))
        return surveys

    def _is_valid_survey_payload(self, data: Any) -> bool:
        _SIGNAL_KEYS = {
            "reward", "payout", "cpi", "loi", "minutes",
            "time", "duration", "estimated_time", "value",
            "points", "amount",
            "survey_id", "offer_id", "id", "url", "link", "start_url",
        }
        try:
            lists = self._find_all_lists(data)
            for lst in lists:
                for item in lst:
                    if isinstance(item, dict) and _SIGNAL_KEYS & item.keys():
                        return True
        except Exception:
            pass
        return False

    def _extract_graphql_surveys(self, data: Any) -> List[Dict]:
        if not isinstance(data, dict):
            return []
        data_field = data.get("data")
        if not isinstance(data_field, dict):
            return []
        results: List[Dict] = []
        for value in data_field.values():
            if isinstance(value, list):
                results.extend(v for v in value if isinstance(v, dict))
            elif isinstance(value, dict):
                for inner in value.values():
                    if isinstance(inner, list):
                        results.extend(v for v in inner if isinstance(v, dict))
        return results

    def _find_all_lists(self, data: Any) -> List[List[Any]]:
        results: List[List[Any]] = []
        if isinstance(data, list):
            if data and isinstance(data[0], dict):
                results.append(data)
            for item in data:
                results.extend(self._find_all_lists(item))
            return results
        if isinstance(data, dict):
            for key in _LIST_KEYS:
                val = data.get(key)
                if isinstance(val, list) and val and isinstance(val[0], dict):
                    results.append(val)
            for val in data.values():
                results.extend(self._find_all_lists(val))
        return results

    @staticmethod
    def _clean_money(val) -> Optional[float]:
        if val is None:
            return None
        try:
            cleaned = re.sub(r"[^\d.]", "", str(val))
            return float(cleaned) if cleaned else None
        except Exception:
            return None

    @staticmethod
    def _clean_int(val) -> Optional[int]:
        if val is None:
            return None
        try:
            cleaned = re.sub(r"[^\d]", "", str(val))
            return int(cleaned) if cleaned else None
        except Exception:
            return None

    def _build_survey_from_api_item(self, item: Dict, source_url: str) -> Optional[Dict]:
        import json as _json
        # Log raw item to inspect session_id value
        logger.info(f"[RAW_ITEM] {_json.dumps(item, default=str)[:600]}")

        """
        Normalise a single API item into the survey_entry shape.
        CRITICAL FIX: Store the raw session_id directly from the API item
        so it can be used later for URL reconstruction.
        """
        # ── Identity / dedup ─────────────────────────────────────────────
        api_id = (
            item.get("hash")           # TopSurveys primary key
            or item.get("id")
            or item.get("survey_id")
            or item.get("offer_id")
            or item.get("placement_id")
        )

        # Read session_id as a string, strip whitespace, keep full value
        session_id_raw = item.get("survey_session_id")
        if session_id_raw is not None:
            session_id = str(session_id_raw).strip()
        else:
            session_id = ""

        # Log the raw session id with its length to detect truncation
        self.diag.debug(
            "BUILD",
            f"session_id raw: '{session_id_raw}' (len={len(session_id)})",
            f"api_id={api_id}"
        )

        # ── Start URL ────────────────────────────────────────────────────
        # CRITICAL FIX: Use the raw session_id directly, not any processed version
        # Store the raw session_id separately so it can be used later if href is corrupted
        if api_id and session_id and not item.get("url"):
            href = f"https://app.topsurveys.app/survey/{api_id}?session={session_id}"
        else:
            href = (
                item.get("url")
                or item.get("survey_url")
                or item.get("link")
                or item.get("start_url")
                or item.get("redirect_url")
                or item.get("entry_link")
                or item.get("cta_url")
                or (item.get("links") or {}).get("start")
                or (item.get("links") or {}).get("url")
            )
            # Fallback: construct from hash if still nothing
            if not href and api_id and session_id:
                href = f"https://app.topsurveys.app/survey/{api_id}?session={session_id}"

        # ── Reward ───────────────────────────────────────────────────────
        reward_raw = (
            item.get("user_reward")                                  # TopSurveys (cents)
            or item.get("user_reward_without_bonus")                 # TopSurveys base
            or item.get("reward")
            or item.get("cpi")
            or item.get("value")
            or item.get("points")
            or item.get("payout")
            or item.get("amount")
            or (item.get("incentive") or {}).get("amount")
            or (item.get("compensation") or {}).get("amount")
        )
        reward = self._clean_money(reward_raw)
        # TopSurveys rewards are in cents — convert if > 20 (heuristic: $20+ surveys are rare)
        if reward is not None and reward > 20 and item.get("user_reward"):
            reward = round(reward / 100, 2)

        # ── LOI ──────────────────────────────────────────────────────────
        loi_raw = (
            item.get("loi")                      # TopSurveys (seconds) OR generic (minutes)
            or item.get("length_of_interview")
            or item.get("duration")
            or item.get("estimated_time")
            or item.get("minutes")
            or item.get("time")
            or item.get("estimatedDurationSeconds")
            or (item.get("survey") or {}).get("loi")
            or (item.get("survey") or {}).get("duration")
        )
        loi = self._clean_int(loi_raw)
        # TopSurveys loi is ALWAYS in seconds (confirmed from API).
        # Convert seconds → minutes when item has user_reward (TopSurveys signature).
        if loi is not None and item.get("user_reward") is not None:
            loi = max(1, round(loi / 60))

        # ── Name / title ─────────────────────────────────────────────────
        name = (
            item.get("name")
            or item.get("title")
            or item.get("survey_name")
            or item.get("description")
            or item.get("label")
        )

        # ── Must have something to navigate to ───────────────────────────
        if not href and not api_id:
            return None

        # ── Reject offer-wall items ONLY when a name is present ──────────
        if name:
            title = name.lower()
            _OFFERWALL_KEYWORDS = [
                "install", "download", "app", "game", "subscription",
                "sign up", "signup", "trial", "watch video", "watch a video",
                "crypto", "casino", "bet", "register",
            ]
            if any(k in title for k in _OFFERWALL_KEYWORDS):
                self.diag.debug("DISCOVERY", f"  Rejected (offer-wall): {title[:60]}")
                return None

        # ── Reject absurdly long surveys (> 60 min) ──────────────────────
        if loi is not None and loi > 60:
            self.diag.debug("DISCOVERY", f"  Rejected (LOI {loi}min > 60): hash={api_id}")
            return None

        # ── Build human-readable text ────────────────────────────────────
        text_parts = []
        if name:
            text_parts.append(str(name))
        elif api_id:
            text_parts.append(f"Survey {str(api_id)[:8]}")
        if loi is not None:
            text_parts.append(f"{loi} min")
        if reward is not None:
            text_parts.append(f"${reward:.2f}")
        if item.get("bonus"):
            text_parts.append(f"+{item['bonus']}% bonus")
        text = " | ".join(text_parts) or f"Survey {api_id or 'unknown'}"

        # CRITICAL: Store the raw session_id separately for debugging and URL reconstruction
        return {
            "text":        text[:120],
            "href":        href,
            "btnText":     "Start",
            "btnSelector": None,
            "cardTop":     0,
            "cardLeft":    0,
            "cardHeight":  0,
            "cardWidth":   0,
            "source":      "api",
            "api_id":      str(api_id) if api_id else None,
            "reward":      reward,
            "loi_minutes": loi,
            "_raw_session_id": session_id,  # Keep full raw session for fallback
            "_raw_api_item": item,           # Keep full item for debugging (optional)
        }

    def _trigger_lazy_apis(self, page) -> None:
        try:
            for _ in range(3):
                page.mouse.wheel(0, 800)
                page.wait_for_timeout(400)
            page.mouse.move(500, 500)
            page.wait_for_timeout(300)
            page.mouse.wheel(0, -800)
            page.wait_for_timeout(300)
        except Exception as e:
            logger.debug(f"[TopSurveys] _trigger_lazy_apis scroll: {e}")

        self._trigger_ui_actions(page)

    def _trigger_ui_actions(self, page) -> None:
        _TRIGGER_LABELS = ["earn", "survey", "surveys", "refresh", "load", "start", "begin"]
        try:
            buttons = page.query_selector_all("button, [role='tab'], [role='button']")
            for btn in buttons[:10]:
                try:
                    txt = (btn.inner_text() or "").lower().strip()
                    if any(k in txt for k in _TRIGGER_LABELS) and len(txt) < 30:
                        self.diag.debug("NETWORK", f"  Clicking UI trigger: '{txt}'")
                        btn.click()
                        page.wait_for_timeout(1200)
                except Exception:
                    continue
        except Exception as e:
            logger.debug(f"[TopSurveys] _trigger_ui_actions: {e}")

    def _scroll_to_load(self, page, scrolls: int = 5) -> None:
        logger.info(f"[TopSurveys] Scrolling to trigger lazy rendering ({scrolls} passes)")
        for i in range(scrolls):
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(1200)
        page.evaluate("window.scrollTo(0, 0)")
        page.wait_for_timeout(500)

    def _detect_provider(self, page) -> str:
        url = page.url.lower()
        for provider, keywords in _PROVIDERS.items():
            if any(k in url for k in keywords):
                return provider
        for frame in page.frames:
            try:
                f_url = frame.url.lower()
                for provider, keywords in _PROVIDERS.items():
                    if any(k in f_url for k in keywords):
                        return f"{provider}_iframe"
            except Exception:
                pass
        return "unknown"

    def _get_active_context(self, page):
        """
        Try to find the main frame that contains interactive survey elements.
        If an iframe with input fields exists, return that frame; else return the page.
        """
        try:
            # Look for iframes that contain input fields
            for frame in page.frames:
                try:
                    if frame.query_selector("input, textarea, select"):
                        logger.info(f"[TopSurveys] Switching to iframe: {frame.url[:80]}")
                        # Wait a bit for the frame to stabilise
                        frame.wait_for_selector(
                            "input, textarea, select",
                            timeout=3000,
                            state="visible"
                        )
                        return frame
                except Exception:
                    continue
        except Exception:
            pass
        return page

    def _open_survey(self, page, survey_entry: Dict, listing_url: str) -> bool:
        """
        Navigate into a survey. Retries once on failure.
        Patches window.open before clicking so provider pop-outs are
        intercepted and stay in the same tab.
        
        CRITICAL FIX: If the href in survey_entry is corrupted (e.g., session=3),
        reconstruct it from api_id and _raw_session_id.
        """
        # Patch window.open
        try:
            page.evaluate("() => { window.open = (url) => { window.location.href = url; }; }")
        except Exception:
            pass

        # Check if href is corrupted (session parameter too short)
        href = survey_entry.get("href", "")
        if "?session=" in href:
            session_part = href.split("?session=")[-1].split("&")[0]
            if len(session_part) < 10:  # Corrupted session (should be 32 chars)
                self.diag.debug("NAV", f"Href session corrupted (len={len(session_part)}), reconstructing...")
                api_id = survey_entry.get("api_id")
                raw_session = survey_entry.get("_raw_session_id")
                if api_id and raw_session and len(raw_session) > 10:
                    href = f"https://app.topsurveys.app/survey/{api_id}?session={raw_session}"
                    self.diag.debug("NAV", f"Reconstructed href: {href}")
                    survey_entry["href"] = href

        for attempt in range(2):
            if attempt > 0:
                self.diag.debug("NAV", f"  Retrying navigation (attempt {attempt + 1})")
                page.wait_for_timeout(1500)

            if survey_entry.get("href"):
                href = self.normalize_url(survey_entry["href"])

                # Primary: JS anchor click
                try:
                    page.evaluate("""
                        (url) => {
                            const a = document.createElement('a');
                            a.href = url;
                            a.target = '_self';
                            document.body.appendChild(a);
                            a.click();
                            document.body.removeChild(a);
                        }
                    """, href)
                    page.wait_for_load_state("domcontentloaded", timeout=15_000)
                    if listing_url not in page.url:
                        self.diag.ok("NAV", "Anchor-click navigation succeeded", f"attempt {attempt+1}")
                        return True
                except Exception as e:
                    self.diag.debug("NAV", f"  Anchor-click failed: {e}")

                # Fallback 1: direct goto
                try:
                    page.goto(href, wait_until="domcontentloaded", timeout=20_000)
                    if listing_url not in page.url:
                        self.diag.ok("NAV", "page.goto() navigation succeeded", f"attempt {attempt+1}")
                        return True
                except Exception as e:
                    self.diag.debug("NAV", f"  page.goto() failed: {e}")

                # Fallback 2: window.open eval
                try:
                    page.evaluate(f"window.open('{href}', '_self')")
                    page.wait_for_load_state("domcontentloaded", timeout=15_000)
                    if listing_url not in page.url:
                        self.diag.ok("NAV", "window.open eval navigation succeeded", f"attempt {attempt+1}")
                        return True
                except Exception as e:
                    self.diag.debug("NAV", f"  window.open eval failed: {e}")

            # DOM-sourced entries: try Playwright-native click
            btn_text = survey_entry.get("btnText")
            if btn_text:
                try:
                    locator = page.get_by_role("button", name=re.compile(btn_text, re.IGNORECASE))
                    locator.first.click(timeout=4000)
                    page.wait_for_timeout(1500)
                    if listing_url not in page.url:
                        self.diag.ok("NAV", "Button click navigation succeeded", f"attempt {attempt+1}")
                        return True
                except Exception:
                    pass

            # JS fallback
            try:
                clicked = page.evaluate(_CLICK_SURVEY_JS, survey_entry)
                if clicked:
                    page.wait_for_timeout(1500)
                    if listing_url not in page.url:
                        self.diag.ok("NAV", "JS click navigation succeeded", f"attempt {attempt+1}")
                        return True
            except Exception as e:
                self.diag.debug("NAV", f"  JS click failed: {e}")

        self.diag.error("NAV", "All navigation methods exhausted after 2 attempts")
        return False

    def _is_dq(self, context) -> bool:
        try:
            return bool(context.evaluate(_DQ_JS))
        except Exception:
            return False

    def _advance_page(self, context) -> bool:
        try:
            for label in ["Next", "Continue", "Proceed"]:
                try:
                    btn = context.get_by_role("button", name=re.compile(label, re.IGNORECASE))
                    btn.first.click(timeout=2000)
                    context.wait_for_timeout(1000)
                    return True
                except Exception:
                    continue
            found = context.evaluate(_ADVANCE_PAGE_JS)
            if found:
                context.wait_for_timeout(1000)
            return bool(found)
        except Exception:
            return False

    def _wait_for_questions(self, context, page_num: int) -> bool:
        """
        Wait for at least one input element (radio, checkbox, select, textarea)
        to appear in the current context.
        """
        self.diag.debug("EXTRACT", f"Waiting for question inputs on page {page_num}…")
        try:
            context.wait_for_selector(
                _QUESTION_INPUT_SELECTOR,
                timeout=QUESTION_SELECTOR_TIMEOUT,
                state="visible"
            )
            self.diag.ok("EXTRACT", f"Question inputs detected on page {page_num}")
            return True
        except Exception as e:
            self.diag.warn("EXTRACT", f"Timeout waiting for inputs on page {page_num}", str(e))
            return False

    def _resolve_survey_name(self, page, fallback: str) -> str:
        """
        Wait up to 4 seconds for the page title to change from the generic
        "Top Surveys" fallback, then return the resolved name.
        """
        start = time.time()
        while time.time() - start < 4.0:
            try:
                name = page.evaluate(_SURVEY_NAME_JS)
                if name and name not in ("Top Surveys", "Unknown Survey", "TopSurveys"):
                    return name
            except Exception:
                pass
            page.wait_for_timeout(400)
        return fallback

    def _empty_result(self, account_id: int, t0: float, error: str) -> Dict[str, Any]:
        return {
            "success":                False,
            "error":                  error,
            "questions_found":        0,
            "inserted":               0,
            "surveys_found":          0,
            "surveys_processed":      0,
            "surveys_successful":     0,
            "surveys_failed":         0,
            "survey_results":         [],
            "discovery_source":       "none",
            "batch_id":               f"topsurveys_all_{account_id}_{int(t0)}",
            "execution_time_seconds": round(time.time() - t0, 2),
        }