# src/streamlit/ui/pages/generate_manual_workflows/extractors/topsurveys_extractor.py
"""
TopSurveys Extractor  v13.0.0
==============================
Key changes over v12.0.0:
  - CRITICAL FIX: _build_survey_from_api_item now maps the ACTUAL TopSurveys
    API fields confirmed from network capture:
      user_reward / user_reward_without_bonus (not "reward")
      loi in SECONDS — divided by 60 to get minutes
      hash as the survey identifier
      survey_session_id used to construct the start URL
    Previously ALL surveys were being silently rejected because none of the
    old field names (reward, cpi, value, payout) exist in this API.
  - CRITICAL FIX: LOI > 60 filter now applies AFTER seconds→minutes conversion.
    A survey with loi=70 (70 seconds = 1.2 min) was being rejected as "too long".
  - CRITICAL FIX: Start URL constructed as
    https://app.topsurveys.app/survey/{hash}?session={survey_session_id}
    because the API returns no direct link field.
  - FIX: _extract_from_network now handles the TopSurveys wrapper shape
    {"result":"success","surveys":[...]} before generic traversal.
  - FIX: Offer-wall keyword filter only applied when a name/title field exists.
    The TopSurveys API does not return survey names — skipping name-based
    rejection prevents all surveys being dropped.
  - Everything else unchanged from v12.
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

# Keys we look for in parsed API JSON to find the survey list
_LIST_KEYS = [
    "surveys", "offers", "items", "results", "data",
    "placements", "walls", "available", "list",
]

# ---------------------------------------------------------------------------
# JS: find survey cards (DOM fallback — unchanged from v7)
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
# JS: click the start button for a specific card (fallback only)
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
# JS: get survey name from the current survey page
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
        .replace(/\\s*([-|]\\s*)?(TopSurveys|Survey|Screener)\\s*$/i, '')
        .trim() || 'Unknown Survey';
}
"""

# ---------------------------------------------------------------------------
# JS: extract questions
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
# JS: DQ check
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
# JS: advance to next page
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
# Diagnostic logger — structured events for the UI log panel
# ---------------------------------------------------------------------------

class DiagnosticLogger:
    """
    Collects structured log events during extraction.
    Each event is a dict: {level, phase, msg, detail, ts}
    Levels: INFO, OK, WARN, ERROR, DEBUG
    Phases: INIT, NETWORK, DISCOVERY, NAV, IFRAME, EXTRACT, DB, SURVEY
    """

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
        entry = {
            "level":  level,
            "phase":  phase,
            "msg":    msg,
            "detail": detail,
            "ts":     ts,
        }
        self.events.append(entry)
        # Also forward to Python logger so server logs stay useful
        log_msg = f"[{phase}] {msg}" + (f" — {detail}" if detail else "")
        if level == "ERROR":
            logger.error(log_msg)
        elif level == "WARN":
            logger.warning(log_msg)
        else:
            logger.info(log_msg)

    def info(self,  phase: str, msg: str, detail: str = ""): self._emit("INFO",  phase, msg, detail)
    def ok(self,    phase: str, msg: str, detail: str = ""): self._emit("OK",    phase, msg, detail)
    def warn(self,  phase: str, msg: str, detail: str = ""): self._emit("WARN",  phase, msg, detail)
    def error(self, phase: str, msg: str, detail: str = ""): self._emit("ERROR", phase, msg, detail)
    def debug(self, phase: str, msg: str, detail: str = ""): self._emit("DEBUG", phase, msg, detail)
    def step(self,  phase: str, msg: str, detail: str = ""): self._emit("STEP",  phase, msg, detail)

    def section(self, title: str):
        """Visual separator event."""
        self.events.append({"level": "SECTION", "phase": "", "msg": title, "detail": "", "ts": time.strftime("%H:%M:%S")})


# ---------------------------------------------------------------------------
# Known external survey providers
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
# Extractor class
# ---------------------------------------------------------------------------

class TopSurveysExtractor(BaseExtractor):

    MAX_SCREENER_PAGES = 3

    def __init__(self, db_manager=None):
        super().__init__(db_manager)
        self._info = {
            "site_name":        "Top Surveys",
            "description":      "Survey panel — extracts all surveys from the dashboard",
            "version":          "13.0.0",
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
        self.diag = DiagnosticLogger()  # fresh logger per run
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
            logger.info(f"[TopSurveys] HTTP Basic Auth detected — user: '{http_user}'")

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
                logger.info("[TopSurveys] HTTP credentials injected into browser context")
            except Exception as e:
                self.diag.warn("INIT", "Could not set HTTP credentials via context", str(e))
                logger.warning(f"[TopSurveys] Could not set HTTP credentials: {e}")
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
            # ── 1. Attach network listener ───────────────────────────────
            self._attach_network_listener(page)
            self.diag.section("NETWORK CAPTURE")
            self.diag.step("NETWORK", "Network listener attached — waiting for API calls")

            # ── 2. Load dashboard ────────────────────────────────────────
            self.diag.step("INIT", f"Navigating to dashboard", listing_url)
            logger.info(f"[TopSurveys] Navigating to: {listing_url}")
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
                logger.warning("[TopSurveys] Timed out waiting for buttons — proceeding")

            # Wait + trigger interaction so lazy APIs that fire on scroll/hover fire too
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

            logger.info(f"[TopSurveys] Landed on: {page.url} | "
                        f"Captured {len(self._network_data)} API responses")

            # ── 3a. PRIMARY: extract from network API ────────────────────
            self.diag.section("SURVEY DISCOVERY")
            self.diag.step("DISCOVERY", "Trying API-first extraction from network data…")
            surveys_found = self._extract_from_network()

            if surveys_found:
                discovery_source = "api"
                self.diag.ok("DISCOVERY", f"API extraction found {len(surveys_found)} survey(s)", "source: network intercept")
                for i, s in enumerate(surveys_found[:20]):
                    reward = f"${s.get('reward')}" if s.get("reward") else "reward=?"
                    loi    = f"{s.get('loi_minutes')}min" if s.get("loi_minutes") else "loi=?"
                    self.diag.debug("DISCOVERY", f"  [{i+1}] {s.get('text','?')[:70]}", f"{reward}  {loi}")
                logger.info(f"[TopSurveys] API extraction found {len(surveys_found)} surveys")
            else:
                # ── 3b. FALLBACK: scroll + DOM scrape ───────────────────
                self.diag.warn("DISCOVERY", "No API survey data found — falling back to DOM scraper")
                self.diag.step("DISCOVERY", "Scrolling page to trigger virtualised list rendering…")
                logger.info("[TopSurveys] No API data — falling back to DOM scraper")
                self._scroll_to_load(page)
                surveys_found = page.evaluate(_FIND_SURVEYS_JS)
                discovery_source = "dom"
                if surveys_found:
                    self.diag.ok("DISCOVERY", f"DOM found {len(surveys_found)} survey card(s)", "source: DOM heuristic")
                else:
                    self.diag.error("DISCOVERY", "DOM scraper also found nothing", "check login state and page content")
                logger.info(f"[TopSurveys] DOM extraction found {len(surveys_found)} survey cards")

            if not surveys_found:
                return self._empty_result(account_id, t0,
                    "No surveys found via API or DOM. "
                    "Ensure you are logged in and surveys are visible.")

            if max_surveys:
                surveys_found = surveys_found[:max_surveys]

            total = len(surveys_found)

            # ── 4. Process each survey ───────────────────────────────────
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

                    self.diag.step("NAV", f"Opening survey", survey_entry.get("href", "no href — will click")[:80])
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
                    page.wait_for_timeout(2500)

                    provider = self._detect_provider(page)
                    self.diag.info("SURVEY", f"Provider detected: {provider}")
                    logger.info(f"[TopSurveys] Provider: {provider}")

                    if self._is_dq(page):
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

                    try:
                        survey_name = page.evaluate(_SURVEY_NAME_JS) or survey_label
                    except Exception:
                        survey_name = survey_label

                    self.diag.info("SURVEY", f"Survey name resolved: '{survey_name}'")
                    logger.info(f"[TopSurveys] Survey name: '{survey_name}'")

                    questions: List[Dict] = []
                    page_num = 0

                    while page_num < self.MAX_SCREENER_PAGES:
                        page_num += 1
                        self.diag.step("EXTRACT", f"Extracting screener page {page_num}…")

                        if self._is_dq(page):
                            self.diag.warn("EXTRACT", f"DQ detected on page {page_num} — stopping")
                            logger.info(f"[TopSurveys] DQ on page {page_num}")
                            break

                        active_ctx = self._get_active_context(page)
                        new_qs = active_ctx.evaluate(_EXTRACT_JS)

                        if not isinstance(new_qs, list) or not new_qs:
                            self.diag.info("EXTRACT", f"No questions found on page {page_num} — done with this survey")
                            logger.info(f"[TopSurveys] No questions on page {page_num}")
                            break

                        self.diag.ok("EXTRACT", f"Page {page_num}: {len(new_qs)} question(s) found")
                        for q in new_qs:
                            self.diag.debug("EXTRACT", f"  Q: {q.get('question_text','?')[:80]}", f"type={q.get('question_type','?')}")
                        logger.info(f"[TopSurveys] Page {page_num}: {len(new_qs)} questions")
                        questions.extend(new_qs)

                        if not self._advance_page(page):
                            self.diag.info("EXTRACT", "No Next/Continue button — end of screener")
                            break
                        page.wait_for_timeout(1800)

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

                    logger.info(f"[TopSurveys] Done: '{survey_name}' "
                                f"— {len(questions)} Qs, {inserted} inserted")
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
        """
        Capture ALL JSON responses that pass _is_valid_survey_payload.
        We no longer filter by URL keywords — that was missing endpoints
        served from CDNs, fetch() calls, and delayed hydration paths.
        Request URLs are still logged at DEBUG level for diagnostics.
        """
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
        """
        PRIMARY survey discovery method.

        Walks every captured network response and tries to find a list of
        survey/offer objects.  Returns a list of survey_entry dicts in the
        same shape that _FIND_SURVEYS_JS produces, so the rest of the pipeline
        is unchanged.
        """
        surveys: List[Dict] = []
        seen_ids: set = set()

        for entry in self._network_data:
            data = entry.get("data")
            if not data:
                continue

            # ── TopSurveys-specific shape: {"result":"success","surveys":[...]} ──
            # Check this FIRST before generic traversal to avoid double-counting.
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
                continue  # skip generic traversal for this entry

            # ── Generic GraphQL / other shapes ───────────────────────────────
            graphql_items = self._extract_graphql_surveys(data)
            candidate_lists = [graphql_items] if graphql_items else self._find_all_lists(data)

            for candidate_list in candidate_lists:
                for item in candidate_list:
                    if not isinstance(item, dict):
                        continue
                    survey_entry = self._build_survey_from_api_item(item, entry["url"])
                    if not survey_entry:
                        continue

                    # Composite key — much harder to collide than a single field
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

        # Sort: highest reward first, then shortest LOI — process best surveys first
        # Use safe helpers — reward may arrive as "1.20 USD" string
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

        surveys.sort(
            key=lambda x: (
                -_to_float(x.get("reward")),
                _to_int(x.get("loi_minutes")),
            )
        )

        return surveys

    def _is_valid_survey_payload(self, data: Any) -> bool:
        """
        Return True if the JSON payload contains at least one dict that
        looks like a survey/offer item.  Deliberately broad — we'd rather
        store a few extra responses and discard junk items during normalisation
        than silently miss a real survey feed.
        Signal keys include identity fields (id, survey_id, url) so APIs that
        send reward later (after click) are not rejected at capture time.
        """
        _SIGNAL_KEYS = {
            "reward", "payout", "cpi", "loi", "minutes",
            "time", "duration", "estimated_time", "value",
            "points", "amount",
            # identity / link signals — enough to confirm a survey list
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
        """
        Pull survey items from a GraphQL-style response:
            { "data": { "getSurveys": [...] | { "items": [...] } } }
        Returns a flat list of item dicts, or [] if this doesn't look like
        a GraphQL response.
        """
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
        """
        Recursively collect ALL arrays-of-dicts in a JSON blob.
        Returns a list of lists, ordered by discovery depth (shallow first).
        This replaces the old _find_list_in_json which stopped at the first hit.
        """
        results: List[List[Any]] = []

        if isinstance(data, list):
            if data and isinstance(data[0], dict):
                results.append(data)
            # Also recurse into list members (list-of-lists edge case)
            for item in data:
                results.extend(self._find_all_lists(item))
            return results

        if isinstance(data, dict):
            # Prioritise well-known list keys
            for key in _LIST_KEYS:
                val = data.get(key)
                if isinstance(val, list) and val and isinstance(val[0], dict):
                    results.append(val)
            # Then recurse into all values
            for val in data.values():
                results.extend(self._find_all_lists(val))

        return results

    @staticmethod
    def _clean_money(val) -> Optional[float]:
        """Strip currency symbols/text and return a float, or None."""
        if val is None:
            return None
        try:
            cleaned = re.sub(r"[^\d.]", "", str(val))
            return float(cleaned) if cleaned else None
        except Exception:
            return None

    @staticmethod
    def _clean_int(val) -> Optional[int]:
        """Strip non-digits and return an int, or None."""
        if val is None:
            return None
        try:
            cleaned = re.sub(r"[^\d]", "", str(val))
            return int(cleaned) if cleaned else None
        except Exception:
            return None

    def _build_survey_from_api_item(self, item: Dict, source_url: str) -> Optional[Dict]:
        """
        Normalise a single API item into the survey_entry shape.

        Handles BOTH the confirmed TopSurveys API shape:
            {"hash":..., "loi": <seconds>, "user_reward": <cents>, "survey_session_id":...}
        AND generic provider shapes (Cint, CPX, Bitlabs, etc.).

        LOI from TopSurveys is in SECONDS — we convert to minutes.
        Reward from TopSurveys is in CENTS — we convert to dollars.
        Start URL is constructed from hash + survey_session_id since the API
        returns no direct link field.
        """
        # ── Identity / dedup ─────────────────────────────────────────────
        api_id = (
            item.get("hash")           # TopSurveys primary key
            or item.get("id")
            or item.get("survey_id")
            or item.get("offer_id")
            or item.get("placement_id")
        )

        survey_session_id = item.get("survey_session_id")

        # ── Start URL ────────────────────────────────────────────────────
        # TopSurveys: construct from hash + session (no direct link in API)
        # Generic providers: look for any direct link field
        if api_id and survey_session_id and not item.get("url"):
            href = f"https://app.topsurveys.app/survey/{api_id}?session={survey_session_id}"
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
            if not href and api_id and survey_session_id:
                href = f"https://app.topsurveys.app/survey/{api_id}?session={survey_session_id}"

        # ── Reward ───────────────────────────────────────────────────────
        # TopSurveys: user_reward is in CENTS (e.g. 100 = $1.00)
        # Generic providers: various field names, usually in dollars
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
        # TopSurveys: loi is in SECONDS (e.g. 70 = ~1 min, 249 = ~4 min)
        # Generic providers: usually in minutes already
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
        # Generic providers use minutes already, so don't convert those.
        if loi is not None and item.get("user_reward") is not None:
            loi = max(1, round(loi / 60))

        # ── Name / title ─────────────────────────────────────────────────
        # TopSurveys API does NOT return a name — that's fine, we use hash
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
        # (TopSurveys API has no name field, so this filter is skipped for
        # native TopSurveys items — avoids blanket rejection)
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
        # Applied AFTER seconds→minutes conversion so loi=70s (~1min) is not rejected
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
        }

    def _trigger_lazy_apis(self, page) -> None:
        """
        Simulate light user interaction — scroll + mouse-move — to trigger
        APIs that only fire in response to activity, not on page load.
        Also clicks visible "earn/survey/refresh/load" buttons to trigger
        click-to-load survey feeds.
        """
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
        """
        Click visible buttons whose text suggests they load surveys:
        'earn', 'survey', 'refresh', 'load', 'start'.
        This catches dashboards that only fire the survey API after a tab click.
        """
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
        """
        Scroll the listing page to trigger virtualized list rendering.
        Called only when the API path yields nothing.
        """
        logger.info(f"[TopSurveys] Scrolling to trigger lazy rendering ({scrolls} passes)")
        for i in range(scrolls):
            page.mouse.wheel(0, 2000)
            page.wait_for_timeout(1200)
        # Scroll back to top so _FIND_SURVEYS_JS sees cards in natural order
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
        try:
            inputs = page.query_selector_all("input, textarea, select")
            if inputs:
                return page
        except Exception:
            pass
        for frame in page.frames:
            try:
                if frame.query_selector("input, textarea, select"):
                    logger.info(f"[TopSurveys] Switching to iframe: {frame.url[:80]}")
                    try:
                        frame.wait_for_selector(
                            "input, textarea, select",
                            timeout=5_000,
                        )
                    except Exception:
                        pass  # already present; proceed
                    return frame
            except Exception:
                continue
        return page

    def _open_survey(self, page, survey_entry: Dict, listing_url: str) -> bool:
        """
        Navigate into a survey. Retries once on failure.
        Patches window.open before clicking so provider pop-outs are
        intercepted and stay in the same tab.
        """
        # Patch window.open so any provider pop-out attempt becomes same-tab nav
        try:
            page.evaluate("() => { window.open = (url) => { window.location.href = url; }; }")
        except Exception:
            pass

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


    def _is_dq(self, page) -> bool:
        try:
            return bool(page.evaluate(_DQ_JS))
        except Exception:
            return False

    def _advance_page(self, page) -> bool:
        try:
            for label in ["Next", "Continue", "Proceed"]:
                try:
                    btn = page.get_by_role("button", name=re.compile(label, re.IGNORECASE))
                    btn.first.click(timeout=2000)
                    page.wait_for_timeout(1000)
                    return True
                except Exception:
                    continue
            found = page.evaluate(_ADVANCE_PAGE_JS)
            if found:
                page.wait_for_timeout(1000)
            return bool(found)
        except Exception:
            return False

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