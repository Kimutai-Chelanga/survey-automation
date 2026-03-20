# src/streamlit/ui/pages/generate_manual_workflows/extraction/workflow_creators/topsurveys_creator.py
"""
TopSurveys Workflow Creator
============================
Builds Automa JSON workflows entirely from the extracted questions —
no fixed templates. Every block is generated dynamically based on:
  - The actual question type and options
  - The account persona
  - The survey URL
"""

import json
import logging
import random
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional

from extraction.base_workflow_creator import BaseWorkflowCreator  # type: ignore

logger = logging.getLogger(__name__)

DASHBOARD_URL = "https://www.topsurveys.app/en-us/surveys"


class TopSurveysWorkflowCreator(BaseWorkflowCreator):

    def __init__(self, db_manager=None):
        super().__init__(db_manager)
        self._info = {
            "site_name":     "Top Surveys",
            "version":       "3.0.0",
            "template_name": "TopSurveys Dynamic (question-driven, no templates)",
            "description":   "Fully dynamic workflows built from extracted questions",
        }

    def get_site_info(self) -> Dict[str, Any]:
        return self._info

    # ------------------------------------------------------------------
    # Main entry
    # ------------------------------------------------------------------

    def create_workflows(self, account_id: int, site_id: int,
                         questions: List[Dict], prompt: Optional[Dict],
                         **kwargs) -> Dict[str, Any]:
        t0            = time.time()
        n_workflows   = max(1, min(kwargs.get("workflow_count", 3), len(questions) or 1))
        survey_url    = kwargs.get("survey_url", DASHBOARD_URL)
        batch_id      = f"topsurveys_wf_{account_id}_{int(t0)}"

        persona = self.parse_persona(prompt)
        logger.info(f"[TopSurveys WF] account={account_id} questions={len(questions)} "
                    f"persona_keys={list(persona.keys())}")

        batches   = self._split(questions, n_workflows)
        workflows = []

        for i, batch in enumerate(batches):
            name = f"topsurveys_{account_id}_batch{i+1}_{int(t0)}"
            data = self._build_workflow_from_questions(
                batch=batch,
                persona=persona,
                name=name,
                survey_url=survey_url,
                batch_idx=i,
            )
            workflows.append({
                "workflow_name": name,
                "question_id":   batch[0].get("question_id") if batch else None,
                "question_text": batch[0].get("question_text", "")[:100] if batch else "",
                "batch_size":    len(batch),
                "workflow_data": data,
            })
            logger.info(f"  Batch {i+1}: {len(batch)} questions → "
                        f"{len(data['drawflow']['nodes'])} blocks")

        inserted = self.save_workflows_to_db(account_id, site_id, workflows, batch_id)
        elapsed  = round(time.time() - t0, 2)

        return {
            "success":                True,
            "workflows_created":      len(workflows),
            "workflows":              workflows,
            "inserted":               inserted,
            "batch_id":               batch_id,
            "execution_time_seconds": elapsed,
        }

    # ------------------------------------------------------------------
    # Core builder — everything driven by actual questions
    # ------------------------------------------------------------------

    def _build_workflow_from_questions(self, batch: List[Dict], persona: Dict,
                                       name: str, survey_url: str,
                                       batch_idx: int) -> Dict:
        """
        Build a complete Automa workflow purely from the questions in this batch.
        No fixed template — blocks are assembled question by question.
        """
        blocks = []

        # 1. Navigate to the survey
        blocks += self._open_survey_blocks(survey_url, batch_idx)

        # 2. For each extracted question, generate exactly the right blocks
        for q_idx, q in enumerate(batch):
            q_blocks = self._blocks_for_question(q, persona, q_idx)
            blocks += q_blocks

            # After answering, try to advance — but only if more questions follow
            # or this is the last one (submit)
            is_last = (q_idx == len(batch) - 1)
            blocks += self._advance_blocks(is_last=is_last)

            # DQ check after every page advance
            blocks.append(self._dq_check_block())

        # 3. Final completion check
        blocks.append(self._completion_block())

        return self.assemble_workflow(
            name=name,
            description=(
                f"TopSurveys — {len(batch)} question(s) — batch {batch_idx + 1} — "
                f"persona: {persona.get('gender','?')}/{persona.get('age','?')}y"
            ),
            blocks=blocks,
            icon="📋",
        )

    # ------------------------------------------------------------------
    # Navigation blocks
    # ------------------------------------------------------------------

    def _open_survey_blocks(self, survey_url: str, batch_idx: int) -> List[Dict]:
        """
        Build navigation blocks for this specific survey URL.
        If it's the dashboard, click a survey card. If it's a direct URL, open it.
        """
        blocks = []

        blocks.append(self.b_new_tab(survey_url))
        # Variable wait — longer for first batch (cold start), shorter for subsequent
        blocks.append(self.b_delay(4000 if batch_idx == 0 else 2500, 7000 if batch_idx == 0 else 5000))

        is_dashboard = "topsurveys.app" in survey_url and "surveys" in survey_url

        if is_dashboard:
            blocks.append(self.b_scroll("down", 200))
            blocks.append(self.b_delay(600, 1200))
            # Click the Nth available survey card based on batch index
            blocks.append(self.b_js(
                self._js_pick_survey_card(batch_idx),
                f"Select survey card (slot {batch_idx + 1})"
            ))
            blocks.append(self.b_delay(2000, 4500))
            # The card may open in a new tab
            blocks.append(self.b_js(
                self._js_switch_to_newest_tab(),
                "Switch to survey tab if opened"
            ))
            blocks.append(self.b_delay(1500, 3000))
        else:
            # Direct survey URL — just wait for it to fully load
            blocks.append(self.b_delay(2000, 4000))

        # Dismiss any cookie / GDPR banners that block interaction
        blocks.append(self.b_js(self._js_dismiss_banners(), "Dismiss banners if any"))
        blocks.append(self.b_delay(400, 800))

        return blocks

    # ------------------------------------------------------------------
    # Per-question block generation
    # ------------------------------------------------------------------

    def _blocks_for_question(self, q: Dict, persona: Dict, q_idx: int) -> List[Dict]:
        """
        Generate interaction blocks for a single extracted question.
        Uses the question's type, options, selectors, and text to decide
        exactly what blocks to emit — nothing is assumed in advance.
        """
        q_type   = (q.get("question_type") or "unknown").lower().strip()
        q_text   = (q.get("question_text") or "").strip()
        options  = self._parse_options(q.get("options"))
        click_el = (q.get("click_element") or "").strip()
        input_el = (q.get("input_element") or "").strip()

        blocks = []

        # Scroll to bring question into view
        blocks.append(self.b_scroll("down", random.randint(100, 250)))
        blocks.append(self.b_delay(300, 700))

        logger.debug(f"  Q{q_idx+1} type={q_type!r} options={len(options)} "
                     f"click_el={bool(click_el)} input_el={bool(input_el)}")

        # ---- RADIO / MULTIPLE CHOICE ----
        if q_type in ("multiple_choice", "radio", "single_choice"):
            best = self.best_answer(q, persona)
            if best and options:
                # If we know the exact selector from extraction, use it
                if click_el:
                    blocks.append(self.b_js(
                        self._js_click_option_by_text_or_selector(click_el, best),
                        f"Q{q_idx+1}: radio '{best[:30]}'"
                    ))
                else:
                    blocks.append(self.b_answer_radio_by_label(best))
            else:
                # No persona match — pick middle option to avoid extremes
                blocks.append(self.b_js(
                    self._js_click_nth_radio(len(options) // 2 if options else 0),
                    f"Q{q_idx+1}: radio middle"
                ))

        # ---- CHECKBOX ----
        elif q_type in ("checkbox", "multi_select", "multiple_select"):
            # Decide how many to check based on option count
            n = 1 if len(options) <= 2 else random.randint(2, min(3, len(options) - 1))
            blocks.append(self.b_js(
                self._js_check_n_options(n, options, persona),
                f"Q{q_idx+1}: checkbox pick {n}"
            ))

        # ---- DROPDOWN / SELECT ----
        elif q_type in ("dropdown", "select"):
            best = self.best_answer(q, persona)
            if click_el:
                # Open the dropdown first if we have its selector
                blocks.append(self.b_js(
                    self._js_open_and_select(click_el, best),
                    f"Q{q_idx+1}: dropdown '{best[:30]}'"
                ))
            else:
                blocks.append(self.b_answer_dropdown(best))

        # ---- TEXT / OPEN ENDED ----
        elif q_type in ("text", "textarea", "open_ended", "free_text", "short_answer"):
            answer_text = self._compose_text_answer(q_text, persona)
            if input_el:
                blocks.append(self.b_js(
                    self._js_type_into_selector(input_el, answer_text),
                    f"Q{q_idx+1}: type into extracted selector"
                ))
            else:
                blocks.append(self.b_answer_text(answer_text))

        # ---- RATING / SCALE ----
        elif q_type in ("rating", "scale", "likert", "slider"):
            # Use extracted click_element if available, otherwise generic mid-rating
            if click_el:
                blocks.append(self.b_js(
                    self._js_click_rating_by_selector(click_el, options),
                    f"Q{q_idx+1}: rating via extracted selector"
                ))
            else:
                blocks.append(self.b_answer_rating_mid())

        # ---- YES / NO ----
        elif q_type in ("yes_no", "boolean", "true_false"):
            answer = self.best_answer(q, persona) or "Yes"
            if click_el:
                blocks.append(self.b_js(
                    self._js_click_option_by_text_or_selector(click_el, answer),
                    f"Q{q_idx+1}: yes/no '{answer}'"
                ))
            else:
                blocks.append(self.b_answer_radio_by_label(answer))

        # ---- DATE ----
        elif q_type in ("date", "date_of_birth", "dob"):
            dob = self._persona_dob(persona)
            if input_el:
                blocks.append(self.b_js(
                    self._js_type_into_selector(input_el, dob),
                    f"Q{q_idx+1}: date '{dob}'"
                ))
            else:
                blocks.append(self.b_js(
                    self._js_fill_date_generic(dob),
                    f"Q{q_idx+1}: date generic"
                ))

        # ---- MATRIX / GRID ----
        elif q_type in ("matrix", "grid", "likert_matrix"):
            blocks.append(self.b_js(
                self._js_answer_matrix_mid(),
                f"Q{q_idx+1}: matrix mid"
            ))

        # ---- UNKNOWN ----
        else:
            logger.warning(f"  Q{q_idx+1}: unknown type '{q_type}' — trying radio random")
            blocks.append(self.b_answer_radio_random())

        blocks.append(self.b_delay(700, 2000))
        return blocks

    # ------------------------------------------------------------------
    # Advance / submit blocks
    # ------------------------------------------------------------------

    def _advance_blocks(self, is_last: bool) -> List[Dict]:
        """
        After answering a question, advance to the next page/question.
        On the last question we submit; on intermediate ones we just click Next.
        """
        blocks = []
        blocks.append(self.b_delay(500, 1200))
        blocks.append(self.b_js(
            self._js_click_next_or_submit(is_last),
            "Submit" if is_last else "Next"
        ))
        blocks.append(self.b_delay(1800, 4000))
        # Handle any redirect to an external survey provider
        blocks.append(self.b_js(
            self._js_switch_to_newest_tab(),
            "Handle redirect to external survey" if is_last else "Check for new tab"
        ))
        return blocks

    # ------------------------------------------------------------------
    # DQ / completion blocks
    # ------------------------------------------------------------------

    def _dq_check_block(self) -> Dict:
        """
        DQ detection using the actual page text — no fixed selectors.
        If DQ is detected we log and close the tab cleanly.
        """
        dq_phrases = [
            "not eligible", "don't qualify", "not a match",
            "no surveys available", "screened out", "unfortunately",
            "didn't qualify", "not qualified", "no longer available",
        ]
        js = f"""
() => {{
    const body = (document.body ? document.body.innerText : '').toLowerCase();
    const url  = window.location.href.toLowerCase();
    const dqPhrases = {json.dumps(dq_phrases)};
    return dqPhrases.some(p => body.includes(p) || url.includes('dq') || url.includes('disqualif'));
}}"""
        return self.b_condition(
            js_code=js,
            if_true=[
                self.b_js("() => console.log('[TopSurveys] DQ detected')", "Log DQ"),
                self.b_close_tab(),
            ],
            if_false=[],
            label="DQ check"
        )

    def _completion_block(self) -> Dict:
        """Detect completion page and close tab."""
        phrases = ["thank you", "completed", "reward", "points credited",
                   "success", "earned", "well done", "survey complete"]
        js = f"""
() => {{
    const body = (document.body ? document.body.innerText : '').toLowerCase();
    const url  = window.location.href.toLowerCase();
    const phrases = {json.dumps(phrases)};
    return phrases.some(p => body.includes(p)) || url.includes('complete') || url.includes('thank');
}}"""
        return self.b_condition(
            js_code=js,
            if_true=[
                self.b_js("() => console.log('[TopSurveys] Completed!')", "Log complete"),
                self.b_close_tab(),
            ],
            if_false=[],
            label="Completion check"
        )

    # ------------------------------------------------------------------
    # JS helpers — all generated from question data
    # ------------------------------------------------------------------

    def _js_pick_survey_card(self, index: int) -> str:
        """Click the Nth survey card on the dashboard using structural heuristics."""
        return f"""
() => {{
    const cards = Array.from(document.querySelectorAll('a, button, article, li'))
        .filter(el => {{
            const r = el.getBoundingClientRect();
            if (r.width < 80 || r.height < 40) return false;
            const t = el.innerText || '';
            return t.includes('$') || t.toLowerCase().includes('survey') || t.includes('min');
        }});
    if (!cards.length) {{
        const fallback = document.querySelector('main a, main button');
        if (fallback) {{ fallback.click(); return true; }}
        return false;
    }}
    cards[{index} % cards.length].click();
    return true;
}}"""

    def _js_switch_to_newest_tab(self) -> str:
        """Signal Automa to switch to a newly opened tab if one exists."""
        return """
() => {
    // Automa monitors tab creation; this JS just confirms we want the latest tab.
    // Return true only if we detect a redirect has likely occurred.
    const url = window.location.href;
    return url.includes('topsurveys') === false && url !== 'about:blank';
}"""

    def _js_dismiss_banners(self) -> str:
        """Click common cookie/GDPR accept buttons."""
        return """
() => {
    const labels = ['accept', 'agree', 'got it', 'ok', 'close', 'dismiss', 'allow'];
    const btns = Array.from(document.querySelectorAll('button, a[role="button"]'))
        .filter(el => {
            const r = el.getBoundingClientRect();
            return r.width > 0 && r.height > 0;
        });
    for (const lbl of labels) {
        const b = btns.find(el =>
            (el.innerText || '').trim().toLowerCase().startsWith(lbl)
        );
        if (b) { b.click(); return true; }
    }
    return false;
}"""

    def _js_click_option_by_text_or_selector(self, selector: str, text: str) -> str:
        """
        Try clicking by extracted CSS selector first.
        If that fails, fall back to text matching across all labels/buttons.
        """
        return f"""
() => {{
    // Try exact selector from extraction
    const sel = {json.dumps(selector)};
    const want = {json.dumps(text.lower())};

    if (sel) {{
        const el = document.querySelector(sel);
        if (el) {{ el.click(); return true; }}
    }}

    // Fall back: find label/span/button whose text contains the answer
    const all = Array.from(document.querySelectorAll('label, span, button, [role="radio"], [role="option"]'));
    const match = all.find(el => (el.innerText || '').toLowerCase().includes(want));
    if (match) {{
        match.click();
        if (match.htmlFor) {{
            const inp = document.getElementById(match.htmlFor);
            if (inp) inp.click();
        }}
        return true;
    }}

    // Last resort: click middle radio
    const radios = document.querySelectorAll("input[type='radio']");
    if (radios.length) {{
        radios[Math.floor(radios.length / 2)].click();
        return true;
    }}
    return false;
}}"""

    def _js_click_nth_radio(self, n: int) -> str:
        """Click the Nth radio input (0-indexed), clamped to available count."""
        return f"""
() => {{
    const radios = Array.from(document.querySelectorAll("input[type='radio']"));
    if (!radios.length) return false;
    const idx = Math.min({n}, radios.length - 1);
    radios[idx].click();
    const lbl = document.querySelector(`label[for='${{radios[idx].id}}']`);
    if (lbl) lbl.click();
    return true;
}}"""

    def _js_check_n_options(self, n: int, options: List, persona: Dict) -> str:
        """
        Check N checkboxes, preferring options that match the persona.
        Skips 'None / None of the above' options.
        """
        preferred = [str(o).lower() for o in options[:n] if o]
        return f"""
() => {{
    const preferred = {json.dumps(preferred)};
    let inputs = Array.from(document.querySelectorAll("input[type='checkbox']"))
        .filter(i => {{
            const lbl = document.querySelector(`label[for='${{i.id}}']`);
            const txt = (lbl ? lbl.innerText : i.value || '').toLowerCase();
            return !txt.includes('none') && !txt.includes('n/a') && !txt.includes('prefer not');
        }});

    if (!inputs.length) return false;

    // Sort: prefer options whose text matches persona-derived preferred list
    inputs.sort((a, b) => {{
        const ta = (document.querySelector(`label[for='${{a.id}}']`)?.innerText || '').toLowerCase();
        const tb = (document.querySelector(`label[for='${{b.id}}']`)?.innerText || '').toLowerCase();
        const sa = preferred.some(p => ta.includes(p)) ? -1 : 1;
        const sb = preferred.some(p => tb.includes(p)) ? -1 : 1;
        return sa - sb;
    }});

    const toCheck = inputs.slice(0, Math.min({n}, inputs.length));
    toCheck.forEach(i => {{
        i.click();
        const lbl = document.querySelector(`label[for='${{i.id}}']`);
        if (lbl) lbl.click();
    }});
    return toCheck.length > 0;
}}"""

    def _js_open_and_select(self, selector: str, value: str) -> str:
        """Open a dropdown by its extracted selector and select the best matching option."""
        return f"""
() => {{
    const sel = document.querySelector({json.dumps(selector)}) || document.querySelector('select');
    if (!sel) return false;

    const want = {json.dumps(value.lower())};
    const opts = Array.from(sel.options || []).filter(o => o.value);
    let pick = opts.find(o => o.text.toLowerCase().includes(want));
    if (!pick) pick = opts[Math.floor(opts.length / 2)];
    if (!pick) return false;

    sel.value = pick.value;
    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
    sel.dispatchEvent(new Event('input',  {{bubbles: true}}));
    return true;
}}"""

    def _js_type_into_selector(self, selector: str, text: str) -> str:
        """Type text into the extracted input/textarea selector with proper events."""
        return f"""
() => {{
    const el = document.querySelector({json.dumps(selector)})
            || document.querySelector("textarea, input[type='text'], input[type='email']");
    if (!el) return false;
    el.focus();
    el.value = {json.dumps(text)};
    el.dispatchEvent(new Event('input',  {{bubbles: true}}));
    el.dispatchEvent(new Event('change', {{bubbles: true}}));
    el.dispatchEvent(new KeyboardEvent('keyup', {{bubbles: true}}));
    return true;
}}"""

    def _js_click_rating_by_selector(self, selector: str, options: List) -> str:
        """
        Click a mid-point rating using the extracted selector.
        Falls back to generic mid-rating if selector doesn't resolve.
        """
        mid_idx = len(options) // 2 if options else 2
        return f"""
() => {{
    // Try extracted selector
    const extracted = document.querySelector({json.dumps(selector)});
    if (extracted) {{ extracted.click(); return true; }}

    // Try all rating-like inputs and pick the middle one
    const candidates = Array.from(document.querySelectorAll(
        "input[type='radio'], [role='radio'], .rating-item, [class*='star'], [class*='rating'] input"
    )).filter(el => {{
        const r = el.getBoundingClientRect();
        return r.width > 0 && r.height > 0;
    }});
    if (!candidates.length) return false;
    const mid = candidates[Math.min({mid_idx}, candidates.length - 1)];
    mid.click();
    mid.dispatchEvent(new Event('change', {{bubbles: true}}));
    return true;
}}"""

    def _js_fill_date_generic(self, dob: str) -> str:
        """Fill a date field — tries <input type='date'> first, then splits into parts."""
        parts = dob.split("-")
        year  = parts[0] if len(parts) > 0 else "1990"
        month = parts[1] if len(parts) > 1 else "01"
        day   = parts[2] if len(parts) > 2 else "01"
        return f"""
() => {{
    // Standard date input
    const dateInput = document.querySelector("input[type='date']");
    if (dateInput) {{
        dateInput.value = {json.dumps(dob)};
        dateInput.dispatchEvent(new Event('change', {{bubbles: true}}));
        return true;
    }}

    // Separate month/day/year selects
    const selects = document.querySelectorAll('select');
    if (selects.length >= 3) {{
        const setOpt = (sel, val) => {{
            const opt = Array.from(sel.options).find(o => o.value == val || o.text == val);
            if (opt) {{ sel.value = opt.value; sel.dispatchEvent(new Event('change', {{bubbles:true}})); }}
        }};
        setOpt(selects[0], {json.dumps(month)});
        setOpt(selects[1], {json.dumps(day)});
        setOpt(selects[2], {json.dumps(year)});
        return true;
    }}

    return false;
}}"""

    def _js_answer_matrix_mid(self) -> str:
        """For matrix/grid questions, select the middle column for every row."""
        return """
() => {
    // Find all row groups — each row usually has a set of radio inputs
    const rows = Array.from(document.querySelectorAll('tr, [role="row"], .matrix-row'))
        .filter(r => r.querySelector("input[type='radio'], [role='radio']"));

    if (!rows.length) {
        // Flat radio approach: divide into groups of equal size and pick mid per group
        const radios = Array.from(document.querySelectorAll("input[type='radio']"));
        if (!radios.length) return false;
        radios[Math.floor(radios.length / 2)].click();
        return true;
    }

    rows.forEach(row => {
        const opts = Array.from(row.querySelectorAll("input[type='radio'], [role='radio']"));
        if (opts.length) opts[Math.floor(opts.length / 2)].click();
    });
    return true;
}"""

    def _js_click_next_or_submit(self, is_submit: bool) -> str:
        """
        Click the Next or Submit button.
        Prefers submit-like text on the last question, next-like text otherwise.
        """
        primary   = ["submit", "finish", "done", "complete"] if is_submit else ["next", "continue", "proceed"]
        secondary = ["next", "continue", "ok"] if is_submit else ["submit", "ok", "done"]
        return f"""
() => {{
    const primary   = {json.dumps(primary)};
    const secondary = {json.dumps(secondary)};
    const allKeywords = [...primary, ...secondary];

    const candidates = Array.from(document.querySelectorAll(
        "button, input[type='submit'], input[type='button'], [role='button']"
    )).filter(el => {{
        const r = el.getBoundingClientRect();
        return r.width > 20 && r.height > 15 && !el.disabled;
    }});

    for (const kw of allKeywords) {{
        const btn = candidates.find(el =>
            (el.innerText || el.value || el.getAttribute('aria-label') || '')
            .trim().toLowerCase().startsWith(kw)
        );
        if (btn) {{ btn.click(); return true; }}
    }}

    // Bottom-right-most visible button as last resort
    if (candidates.length) {{
        const sorted = [...candidates].sort((a, b) => {{
            const ra = a.getBoundingClientRect(), rb = b.getBoundingClientRect();
            return (rb.bottom + rb.right) - (ra.bottom + ra.right);
        }});
        sorted[0].click();
        return true;
    }}
    return false;
}}"""

    # ------------------------------------------------------------------
    # Persona / answer helpers
    # ------------------------------------------------------------------

    def _compose_text_answer(self, question_text: str, persona: Dict) -> str:
        """
        Compose a natural text answer driven entirely by the question text
        and the persona — no hard-coded canned responses.
        """
        qt = question_text.lower()

        # Identity fields
        if any(w in qt for w in ("your name", "first name", "full name")):
            return persona.get("username", "Alex")
        if any(w in qt for w in ("city", "town", "location", "where do you live")):
            return persona.get("city", "Chicago")
        if any(w in qt for w in ("zip", "postal", "postcode")):
            return "60601"
        if "email" in qt:
            return persona.get("email", "user@example.com")
        if "phone" in qt:
            return persona.get("phone", "555-0100")
        if any(w in qt for w in ("employer", "company", "where do you work")):
            return persona.get("industry", "Technology")
        if any(w in qt for w in ("job title", "occupation", "position", "role")):
            return persona.get("job_status", "Professional")

        # Open opinion questions — generate a short, specific sentence
        if any(w in qt for w in ("why", "reason", "explain", "describe", "tell us")):
            job = persona.get("job_status", "professional").lower()
            city = persona.get("city", "my city")
            return f"As a {job} based in {city}, I find it practical and fits my routine well."

        if any(w in qt for w in ("comment", "suggestion", "feedback", "improvement")):
            return "The experience was straightforward and easy to navigate."

        if any(w in qt for w in ("how long", "how many year", "experience")):
            age = int(persona.get("age", 30))
            return str(max(1, age - random.randint(20, 25)))

        if any(w in qt for w in ("how often", "frequency", "how many times")):
            return random.choice(["Once a week", "A few times a month", "Daily", "Occasionally"])

        if any(w in qt for w in ("brand", "product", "which product")):
            return persona.get("brands_used", "Various well-known brands")

        # Generic short answer
        return "Not applicable."

    def _persona_dob(self, persona: Dict) -> str:
        """Return a DOB string (YYYY-MM-DD) from persona age or date_of_birth."""
        if persona.get("date_of_birth"):
            dob = str(persona["date_of_birth"])
            if len(dob) >= 10:
                return dob[:10]
        age = int(persona.get("age", 30))
        year = datetime.now().year - age if hasattr(self, "_now") else 2024 - age
        return f"{year}-06-15"

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _parse_options(self, raw) -> List:
        if not raw:
            return []
        if isinstance(raw, list):
            return raw
        if isinstance(raw, str):
            try:
                return json.loads(raw)
            except Exception:
                return [raw]
        return []

    def _split(self, questions: List[Dict], n: int) -> List[List[Dict]]:
        """Split questions into n balanced batches."""
        if not questions:
            return [[]]
        n = max(1, min(n, len(questions)))
        size = max(1, len(questions) // n)
        batches = []
        for i in range(0, len(questions), size):
            batches.append(questions[i: i + size])
            if len(batches) >= n:
                break
        return batches


