# src/streamlit/ui/pages/generate_manual_workflows/extraction/base_workflow_creator.py
"""
BaseWorkflowCreator — inherit from this for every new survey site.

Provides a complete library of Automa JSON block builders so subclasses
only need to compose blocks, not write raw JSON.
"""

import json
import logging
import random
import re
import uuid
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BaseWorkflowCreator(ABC):

    def __init__(self, db_manager=None):
        self.db_manager = db_manager

    @abstractmethod
    def get_site_info(self) -> Dict[str, Any]:
        ...

    @abstractmethod
    def create_workflows(self, account_id, site_id, questions, prompt, **kwargs) -> Dict[str, Any]:
        ...

    # ------------------------------------------------------------------
    # ID helper
    # ------------------------------------------------------------------

    def uid(self) -> str:
        return str(uuid.uuid4())

    # ------------------------------------------------------------------
    # Automa block library
    # ------------------------------------------------------------------

    def b_new_tab(self, url: str) -> Dict:
        return {"id": self.uid(), "label": f"Open {url[:60]}", "name": "new-tab",
                "data": {"url": url, "active": True}}

    def b_close_tab(self) -> Dict:
        return {"id": self.uid(), "label": "Close tab", "name": "close-tab", "data": {}}

    def b_wait(self, selector: str, timeout: int = 10000) -> Dict:
        return {"id": self.uid(), "label": f"Wait: {selector[:40]}", "name": "wait-element",
                "data": {"selector": selector, "timeout": timeout, "waitType": "visible"}}

    def b_click(self, selector: str, label: str = "") -> Dict:
        return {"id": self.uid(), "label": label or f"Click: {selector[:40]}",
                "name": "event-click",
                "data": {"selector": selector, "waitForSelector": True, "multiple": False}}

    def b_type(self, selector: str, text: str, clear: bool = True) -> Dict:
        return {"id": self.uid(), "label": f"Type: {text[:30]}", "name": "forms",
                "data": {"selector": selector, "type": "text-field",
                         "value": text, "clearValue": clear}}

    def b_select(self, selector: str, value: str) -> Dict:
        return {"id": self.uid(), "label": f"Select: {value[:30]}", "name": "forms",
                "data": {"selector": selector, "type": "select", "value": value}}

    def b_delay(self, min_ms: int = 1200, max_ms: int = 4000) -> Dict:
        ms = random.randint(min_ms, max_ms)
        return {"id": self.uid(), "label": f"Pause {ms}ms", "name": "delay",
                "data": {"time": ms}}

    def b_scroll(self, direction: str = "down", px: int = 300) -> Dict:
        y = px if direction == "down" else -px
        return {"id": self.uid(), "label": f"Scroll {direction}", "name": "scroll-element",
                "data": {"selector": "window", "scrollY": y, "scrollX": 0}}

    def b_js(self, code: str, label: str = "Run JS") -> Dict:
        return {"id": self.uid(), "label": label, "name": "javascript-code",
                "data": {"code": code, "evPage": False}}

    def b_reload(self) -> Dict:
        return {"id": self.uid(), "label": "Reload page", "name": "reload-tab", "data": {}}

    def b_switch_tab(self) -> Dict:
        """Switch to the most recently opened tab (handles external survey redirects)."""
        return {"id": self.uid(), "label": "Switch to new tab", "name": "switch-tab",
                "data": {"createIfNoMatch": False, "matchPattern": ".*"}}

    def b_condition(self, js_code: str, if_true: List[Dict],
                    if_false: Optional[List[Dict]] = None, label: str = "Condition") -> Dict:
        return {
            "id": self.uid(), "label": label, "name": "conditions",
            "data": {
                "conditions": [{"type": "code", "code": js_code}],
                "ifTrue":  if_true,
                "ifFalse": if_false or [],
            }
        }

    # ------------------------------------------------------------------
    # Higher-level helpers
    # ------------------------------------------------------------------

    def b_dq_check(self, on_dq: Optional[List[Dict]] = None) -> Dict:
        """
        Detect disqualification by looking for common DQ phrases in body text.
        TopSurveys claims low DQ rates but we still handle it gracefully.
        """
        dq_phrases = [
            "not eligible", "don't qualify", "not a match",
            "no surveys", "screened out", "unfortunately",
            "didn't qualify", "not qualified",
        ]
        js = f"""
() => {{
    const body = (document.body.innerText || '').toLowerCase();
    return {json.dumps(dq_phrases)}.some(p => body.includes(p));
}}"""
        return self.b_condition(
            js_code=js,
            if_true=on_dq or [self.b_close_tab()],
            label="DQ check"
        )

    def b_completion_check(self, on_complete: Optional[List[Dict]] = None) -> Dict:
        """Detect survey completion page."""
        phrases = ["thank you", "completed", "reward", "success", "earned", "credited"]
        js = f"""
() => {{
    const body = (document.body.innerText || '').toLowerCase();
    return {json.dumps(phrases)}.some(p => body.includes(p));
}}"""
        return self.b_condition(
            js_code=js,
            if_true=on_complete or [self.b_close_tab()],
            label="Completion check"
        )

    def b_answer_radio_by_label(self, label_text: str) -> Dict:
        """Click the radio whose label contains label_text (case-insensitive)."""
        js = f"""
() => {{
    const want = {json.dumps(label_text.lower())};
    for (const lbl of document.querySelectorAll('label')) {{
        if (lbl.innerText.trim().toLowerCase().includes(want)) {{
            lbl.click();
            const inp = document.getElementById(lbl.htmlFor) || lbl.querySelector('input');
            if (inp) inp.click();
            return true;
        }}
    }}
    const inputs = document.querySelectorAll("input[type='radio']");
    if (inputs.length) {{ inputs[Math.floor(inputs.length / 2)].click(); return true; }}
    return false;
}}"""
        return self.b_js(js, f"Radio: {label_text[:40]}")

    def b_answer_radio_random(self, exclude_last: bool = True) -> Dict:
        """Click a random radio button. If exclude_last=True avoids the last option (often 'None')."""
        js = """
() => {
    const inputs = Array.from(document.querySelectorAll("input[type='radio']"));
    if (!inputs.length) return false;
    const pool = inputs.length > 2 ? inputs.slice(0, -1) : inputs;
    const pick = pool[Math.floor(Math.random() * pool.length)];
    pick.click();
    const lbl = document.querySelector(`label[for='${pick.id}']`);
    if (lbl) lbl.click();
    return true;
}"""
        return self.b_js(js, "Radio: random")

    def b_answer_checkbox_n(self, n: int = 2, exclude_none: bool = True) -> Dict:
        """Select N random checkboxes. Skips 'None/None of the above' option."""
        js = f"""
() => {{
    let inputs = Array.from(document.querySelectorAll("input[type='checkbox']"));
    if ({str(exclude_none).lower()}) {{
        inputs = inputs.filter(i => {{
            const lbl = document.querySelector(`label[for='${{i.id}}']`);
            const txt = (lbl ? lbl.innerText : i.value || '').toLowerCase();
            return !txt.includes('none') && !txt.includes('n/a');
        }});
    }}
    if (!inputs.length) return false;
    const n = Math.min({n}, inputs.length);
    const shuffled = inputs.sort(() => Math.random() - 0.5).slice(0, n);
    shuffled.forEach(i => {{ i.click(); const lbl = document.querySelector(`label[for='${{i.id}}']`); if(lbl) lbl.click(); }});
    return true;
}}"""
        return self.b_js(js, f"Checkbox: pick {n}")

    def b_answer_dropdown(self, preferred_value: str = "") -> Dict:
        """Select an option from a <select> — prefer preferred_value if given, else pick middle."""
        js = f"""
() => {{
    const sel = document.querySelector('select');
    if (!sel) return false;
    const opts = Array.from(sel.options).filter(o => o.value && o.value !== '');
    if (!opts.length) return false;
    const preferred = {json.dumps(preferred_value.lower())};
    let pick = opts.find(o => o.text.toLowerCase().includes(preferred));
    if (!pick) pick = opts[Math.floor(opts.length / 2)];
    sel.value = pick.value;
    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
    return true;
}}"""
        return self.b_js(js, f"Dropdown: {preferred_value or 'mid-option'}")

    def b_answer_rating_mid(self) -> Dict:
        """Click the middle rating option (avoids extreme high/low which looks bot-like)."""
        js = """
() => {
    // Try star ratings, number ratings, radio-based ratings
    const candidates = Array.from(document.querySelectorAll(
        "input[type='radio'], [role='radio'], .rating-item, .star, [class*='rating'] input"
    )).filter(el => {
        const rect = el.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
    });
    if (!candidates.length) return false;
    const mid = candidates[Math.floor(candidates.length / 2)];
    mid.click();
    mid.dispatchEvent(new Event('change', {bubbles: true}));
    return true;
}"""
        return self.b_js(js, "Rating: mid")

    def b_answer_text(self, text: str) -> Dict:
        """Type text into the first visible text input or textarea."""
        js = f"""
() => {{
    const el = document.querySelector(
        "textarea:not([disabled]), input[type='text']:not([disabled]), input[type='email']:not([disabled])"
    );
    if (!el) return false;
    el.focus();
    el.value = {json.dumps(text)};
    el.dispatchEvent(new Event('input', {{bubbles: true}}));
    el.dispatchEvent(new Event('change', {{bubbles: true}}));
    return true;
}}"""
        return self.b_js(js, f"Text: {text[:30]}")

    def b_click_next(self) -> Dict:
        """
        Click the Next / Continue / Submit button.
        Uses text-matching so it works across different layouts.
        """
        js = """
() => {
    const labels = ['next', 'continue', 'submit', 'proceed', 'ok', 'done', 'finish'];
    const btns = Array.from(document.querySelectorAll(
        "button, input[type='submit'], input[type='button'], [role='button'], a.btn"
    )).filter(el => {
        const rect = el.getBoundingClientRect();
        return rect.width > 0 && rect.height > 0;
    });
    for (const lbl of labels) {
        const b = btns.find(el => (el.innerText || el.value || '').trim().toLowerCase().startsWith(lbl));
        if (b) { b.click(); return true; }
    }
    // fallback: last visible button
    if (btns.length) { btns[btns.length - 1].click(); return true; }
    return false;
}"""
        return self.b_js(js, "Click Next/Submit")

    def b_handle_external_survey(self) -> Dict:
        """
        TopSurveys sometimes redirects to external survey providers in a new tab.
        This block switches to the newest tab automatically.
        """
        js = """
() => {
    // Signal Automa to switch to the most recently opened tab
    return window.__automa_tabs ? window.__automa_tabs.length > 1 : false;
}"""
        return self.b_condition(
            js_code=js,
            if_true=[self.b_switch_tab()],
            label="Handle external redirect"
        )

    # ------------------------------------------------------------------
    # Workflow assembler
    # ------------------------------------------------------------------

    def assemble_workflow(self, name: str, description: str,
                          blocks: List[Dict], icon: str = "📋") -> Dict:
        return {
            "id":          self.uid(),
            "name":        name,
            "description": description,
            "icon":        icon,
            "version":     "1.0.0",
            "createdAt":   datetime.now().isoformat(),
            "drawflow":    {"nodes": blocks, "edges": self._chain(blocks)},
            "settings":    {"blockDelay": 300, "tabLoadTimeout": 30000, "onError": "keep-running"},
            "globalData":  "{}",
        }

    def _chain(self, blocks: List[Dict]) -> List[Dict]:
        return [
            {"id": self.uid(), "source": blocks[i]["id"], "target": blocks[i + 1]["id"]}
            for i in range(len(blocks) - 1)
        ]

    # ------------------------------------------------------------------
    # Persona helpers
    # ------------------------------------------------------------------

    def parse_persona(self, prompt: Optional[Dict]) -> Dict:
        """
        Extract persona fields from the prompt dict.
        Accepts:
          prompt['demographic_data']  → already-structured dict  (preferred)
          prompt['content']           → free text  (parsed best-effort)
          top-level account fields merged into prompt by the UI
        """
        if not prompt:
            return {}

        # Structured demographic_data field
        if isinstance(prompt.get("demographic_data"), dict):
            p = dict(prompt["demographic_data"])
        else:
            p = {}

        # Free-text content parsing
        content = prompt.get("content", "")
        if content:
            def find(pattern):
                m = re.search(pattern, content, re.I)
                return m.group(1).strip() if m else None

            if not p.get("age"):
                v = find(r"\bage[:\s]+(\d{1,3})\b")
                if v: p["age"] = int(v)

            if not p.get("gender"):
                v = find(r"\bgender[:\s]+(male|female|non-binary|other)\b")
                if v: p["gender"] = v.capitalize()

            if not p.get("job_status"):
                for s in ["student","employed full-time","employed part-time",
                          "self-employed","unemployed","retired","homemaker"]:
                    if s in content.lower():
                        p["job_status"] = s.title()
                        break

            if not p.get("income_range"):
                v = find(r"\bincome[:\s]+([^\n,]{3,40})")
                if v: p["income_range"] = v

            if not p.get("education_level"):
                for e in ["high school","associate","bachelor","master","doctorate","phd"]:
                    if e in content.lower():
                        p["education_level"] = e.title()
                        break

            if not p.get("city"):
                v = find(r"\bcity[:\s]+([A-Za-z\s]{2,30})")
                if v: p["city"] = v

            if p.get("has_children") is None:
                if re.search(r"\bhas_children[:\s]+(yes|true|1)\b", content, re.I):
                    p["has_children"] = True
                elif re.search(r"\bhas_children[:\s]+(no|false|0)\b", content, re.I):
                    p["has_children"] = False

        # Merge account-level fields that UI passes through the prompt dict
        for f in ("age","gender","city","education_level","job_status","income_range",
                  "marital_status","has_children","household_size","username","email"):
            if f in prompt and prompt[f] is not None:
                p.setdefault(f, prompt[f])

        return p

    def best_answer(self, question: Dict, persona: Dict) -> str:
        """
        Return the best string answer for a question given a persona.
        Used by radio / dropdown / yes-no block builders.
        """
        text    = (question.get("question_text") or "").lower()
        q_type  = (question.get("question_type") or "").lower()
        options = question.get("options") or []
        if isinstance(options, str):
            try:    options = json.loads(options)
            except: options = [options]

        def pick_opt(keyword):
            for o in options:
                if keyword.lower() in str(o).lower():
                    return str(o)
            return None

        # Age
        if "age" in text and persona.get("age"):
            age = int(persona["age"])
            for o in options:
                m = re.search(r"(\d+)\s*[-–]\s*(\d+)", str(o))
                if m and int(m.group(1)) <= age <= int(m.group(2)):
                    return str(o)

        # Gender
        if "gender" in text and persona.get("gender"):
            r = pick_opt(persona["gender"])
            if r: return r

        # Employment
        if any(w in text for w in ("employment","job","occupation","work status")):
            if persona.get("job_status"):
                r = pick_opt(persona["job_status"].split()[0])
                if r: return r

        # Income
        if "income" in text and persona.get("income_range"):
            r = pick_opt(persona["income_range"].split("-")[0].strip("$").strip())
            if r: return r

        # Education
        if "education" in text and persona.get("education_level"):
            r = pick_opt(persona["education_level"])
            if r: return r

        # Marital
        if "marital" in text and persona.get("marital_status"):
            r = pick_opt(persona["marital_status"])
            if r: return r

        # Children
        if "children" in text and persona.get("has_children") is not None:
            r = pick_opt("yes" if persona["has_children"] else "no")
            if r: return r

        # Yes/No default
        if q_type in ("yes_no", "boolean"):
            return "Yes"

        # Rating: return middle
        if q_type == "rating" and options:
            return str(options[len(options) // 2])

        return str(options[0]) if options else ""

    # ------------------------------------------------------------------
    # DB helper
    # ------------------------------------------------------------------

    def save_workflows_to_db(self, account_id, site_id, workflows, batch_id) -> int:
        if not self.db_manager or not workflows:
            return 0
        inserted = 0
        for wf in workflows:
            try:
                self.db_manager.execute_query(
                    """INSERT INTO workflows
                       (account_id,site_id,workflow_name,workflow_data,question_id,batch_id,
                        is_active,created_time)
                       VALUES (%s,%s,%s,%s::jsonb,%s,%s,TRUE,CURRENT_TIMESTAMP)""",
                    (account_id, site_id,
                     wf.get("workflow_name","unnamed"),
                     json.dumps(wf.get("workflow_data",{})),
                     wf.get("question_id"), batch_id)
                )
                if wf.get("question_id"):
                    self.db_manager.execute_query(
                        "UPDATE questions SET used_in_workflow=TRUE,used_at=CURRENT_TIMESTAMP "
                        "WHERE question_id=%s",
                        (wf["question_id"],)
                    )
                inserted += 1
            except Exception as exc:
                logger.error(f"save_workflows_to_db: {exc}")
        return inserted