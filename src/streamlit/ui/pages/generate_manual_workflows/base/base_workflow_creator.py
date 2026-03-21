# src/streamlit/ui/pages/generate_manual_workflows/base/base_workflow_creator.py
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
    def create_workflows(
        self, account_id, site_id, questions, prompt, **kwargs
    ) -> Dict[str, Any]:
        ...

    # ------------------------------------------------------------------
    # ID helper
    # ------------------------------------------------------------------

    def uid(self) -> str:
        return uuid.uuid4().hex[:8]

    # ------------------------------------------------------------------
    # Automa block library
    # ------------------------------------------------------------------

    def b_new_tab(self, url: str) -> Dict:
        return {
            "id": self.uid(), "label": f"Open {url[:60]}", "name": "new-tab",
            "data": {"url": url, "active": True, "updatePrevTab": False},
            "outputs": {"output-1": {"connections": []}},
        }

    def b_close_tab(self) -> Dict:
        return {
            "id": self.uid(), "label": "Close tab", "name": "close-tab",
            "data": {},
            "outputs": {"output-1": {"connections": []}},
        }

    def b_wait_element(self, selector: str, timeout: int = 10000) -> Dict:
        return {
            "id": self.uid(), "label": f"Wait: {selector[:40]}", "name": "wait-element",
            "data": {"selector": selector, "timeout": timeout, "waitType": "visible"},
            "outputs": {"output-1": {"connections": []}},
        }

    def b_click(self, selector: str, label: str = "", optional: bool = True) -> Dict:
        return {
            "id": self.uid(),
            "label": label or f"Click: {selector[:40]}",
            "name": "click-element",
            "data": {
                "selector": selector,
                "waitForSelector": True,
                "timeout": 5000,
                "optional": optional,
                "markEleAsError": False,
            },
            "outputs": {"output-1": {"connections": []}},
        }

    def b_type(self, selector: str, text: str, clear: bool = True) -> Dict:
        return {
            "id": self.uid(), "label": f"Type: {text[:30]}", "name": "forms",
            "data": {
                "form": {"selector": selector, "type": "text-field", "value": text},
                "clearValue": clear,
            },
            "outputs": {"output-1": {"connections": []}},
        }

    def b_select(self, selector: str, value: str) -> Dict:
        return {
            "id": self.uid(), "label": f"Select: {value[:30]}", "name": "forms",
            "data": {"form": {"selector": selector, "type": "select", "value": value}},
            "outputs": {"output-1": {"connections": []}},
        }

    def b_delay(self, min_ms: int = 1200, max_ms: int = 4000) -> Dict:
        ms = random.randint(min_ms, max_ms)
        return {
            "id": self.uid(), "label": f"Pause {ms}ms", "name": "delay",
            "data": {"time": ms},
            "outputs": {"output-1": {"connections": []}},
        }

    def b_delay_fixed(self, ms: int) -> Dict:
        return {
            "id": self.uid(), "label": f"Wait {ms}ms", "name": "delay",
            "data": {"time": ms},
            "outputs": {"output-1": {"connections": []}},
        }

    def b_scroll(self, direction: str = "down", px: int = 300) -> Dict:
        y = px if direction == "down" else -px
        return {
            "id": self.uid(), "label": f"Scroll {direction}", "name": "scroll-element",
            "data": {"selector": "window", "scrollY": y, "scrollX": 0},
            "outputs": {"output-1": {"connections": []}},
        }

    def b_js(self, code: str, label: str = "Run JS") -> Dict:
        return {
            "id": self.uid(), "label": label, "name": "javascript-code",
            "data": {"code": code, "everyNewTab": False},
            "outputs": {"output-1": {"connections": []}},
        }

    def b_js_condition(self, code: str, label: str = "Condition") -> Dict:
        """JS block whose return value routes output-1 (true) or output-2 (false)."""
        return {
            "id": self.uid(), "label": label, "name": "javascript-code",
            "data": {"code": code, "everyNewTab": False},
            "outputs": {
                "output-1": {"connections": []},   # truthy → this path
                "output-2": {"connections": []},   # falsy  → this path
            },
        }

    def b_reload(self) -> Dict:
        return {
            "id": self.uid(), "label": "Reload page", "name": "reload-tab",
            "data": {},
            "outputs": {"output-1": {"connections": []}},
        }

    def b_switch_tab(self) -> Dict:
        """Switch to the most recently opened tab (handles external survey redirects)."""
        return {
            "id": self.uid(), "label": "Switch to new tab", "name": "switch-tab",
            "data": {"createIfNoMatch": False, "matchPattern": ".*"},
            "outputs": {"output-1": {"connections": []}},
        }

    # ------------------------------------------------------------------
    # Higher-level composite blocks
    # ------------------------------------------------------------------

    def b_dq_check(self) -> Dict:
        """JS condition block — true if DQ phrases found on page."""
        dq_phrases = [
            "not eligible", "don't qualify", "not a match",
            "no surveys", "screened out", "unfortunately",
            "didn't qualify", "not qualified", "disqualified",
            "quota full", "survey is full",
        ]
        js = (
            "const body = (document.body.innerText || '').toLowerCase();\n"
            f"return {json.dumps(dq_phrases)}.some(p => body.includes(p));"
        )
        return self.b_js_condition(js, "DQ check")

    def b_completion_check(self) -> Dict:
        """JS condition block — true if survey completion phrases found."""
        phrases = [
            "thank you", "survey complete", "you have completed",
            "submission received", "rewards credited", "points added",
            "congratulations", "well done", "earned",
        ]
        js = (
            "const body = (document.body.innerText || '').toLowerCase();\n"
            f"return {json.dumps(phrases)}.some(p => body.includes(p));"
        )
        return self.b_js_condition(js, "Completion check")

    def b_answer_radio_by_label(self, label_text: str) -> Dict:
        """Click the radio whose label contains label_text (case-insensitive)."""
        js = (
            f"const want = {json.dumps(label_text.lower())};\n"
            "for (const lbl of document.querySelectorAll('label')) {\n"
            "  if (lbl.innerText.trim().toLowerCase().includes(want)) {\n"
            "    lbl.click();\n"
            "    const inp = document.getElementById(lbl.htmlFor) || lbl.querySelector('input');\n"
            "    if (inp) inp.click();\n"
            "    return true;\n"
            "  }\n"
            "}\n"
            "const inputs = document.querySelectorAll(\"input[type='radio']\");\n"
            "if (inputs.length) { inputs[Math.floor(inputs.length / 2)].click(); return true; }\n"
            "return false;"
        )
        return self.b_js(js, f"Radio: {label_text[:40]}")

    def b_answer_radio_random(self) -> Dict:
        """Click a random (non-last) radio button."""
        js = (
            "const inputs = Array.from(document.querySelectorAll(\"input[type='radio']\"));\n"
            "if (!inputs.length) return false;\n"
            "const pool = inputs.length > 2 ? inputs.slice(0, -1) : inputs;\n"
            "const pick = pool[Math.floor(Math.random() * pool.length)];\n"
            "pick.click();\n"
            "const lbl = document.querySelector(`label[for='${pick.id}']`);\n"
            "if (lbl) lbl.click();\n"
            "return true;"
        )
        return self.b_js(js, "Radio: random")

    def b_answer_checkbox_n(self, n: int = 2) -> Dict:
        """Select N random checkboxes, skipping 'None' options."""
        js = (
            "let inputs = Array.from(document.querySelectorAll(\"input[type='checkbox']\"));\n"
            "inputs = inputs.filter(i => {\n"
            "  const lbl = document.querySelector(`label[for='${i.id}']`);\n"
            "  const txt = (lbl ? lbl.innerText : i.value || '').toLowerCase();\n"
            "  return !txt.includes('none') && !txt.includes('n/a');\n"
            "});\n"
            "if (!inputs.length) return false;\n"
            f"const n = Math.min({n}, inputs.length);\n"
            "const picks = inputs.sort(() => Math.random() - 0.5).slice(0, n);\n"
            "picks.forEach(i => {\n"
            "  i.click();\n"
            "  const lbl = document.querySelector(`label[for='${i.id}']`);\n"
            "  if (lbl) lbl.click();\n"
            "});\n"
            "return true;"
        )
        return self.b_js(js, f"Checkbox: pick {n}")

    def b_answer_dropdown(self, preferred_value: str = "") -> Dict:
        """Select an option from a <select>."""
        js = (
            "const sel = document.querySelector('select');\n"
            "if (!sel) return false;\n"
            "const opts = Array.from(sel.options).filter(o => o.value && o.value !== '');\n"
            "if (!opts.length) return false;\n"
            f"const preferred = {json.dumps(preferred_value.lower())};\n"
            "let pick = opts.find(o => o.text.toLowerCase().includes(preferred));\n"
            "if (!pick) pick = opts[Math.floor(opts.length / 2)];\n"
            "sel.value = pick.value;\n"
            "sel.dispatchEvent(new Event('change', {bubbles: true}));\n"
            "return true;"
        )
        return self.b_js(js, f"Dropdown: {preferred_value or 'mid-option'}")

    def b_answer_rating_mid(self) -> Dict:
        """Click the middle rating option."""
        js = (
            "const candidates = Array.from(document.querySelectorAll(\n"
            "  \"input[type='radio'], [role='radio'], [class*='rating'] input\"\n"
            ")).filter(el => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; });\n"
            "if (!candidates.length) return false;\n"
            "const mid = candidates[Math.floor(candidates.length / 2)];\n"
            "mid.click();\n"
            "mid.dispatchEvent(new Event('change', {bubbles: true}));\n"
            "return true;"
        )
        return self.b_js(js, "Rating: mid")

    def b_answer_text(self, text: str) -> Dict:
        """Type text into the first visible text input or textarea."""
        js = (
            "const el = document.querySelector(\n"
            "  \"textarea:not([disabled]), input[type='text']:not([disabled])\"\n"
            ");\n"
            "if (!el) return false;\n"
            "el.focus();\n"
            f"el.value = {json.dumps(text)};\n"
            "el.dispatchEvent(new Event('input', {bubbles: true}));\n"
            "el.dispatchEvent(new Event('change', {bubbles: true}));\n"
            "return true;"
        )
        return self.b_js(js, f"Text: {text[:30]}")

    def b_click_next(self) -> Dict:
        """Click the Next / Continue / Submit button."""
        js = (
            "const labels = ['next','continue','submit','proceed','ok','done','finish'];\n"
            "const btns = Array.from(document.querySelectorAll(\n"
            "  \"button, input[type='submit'], input[type='button'], [role='button']\"\n"
            ")).filter(el => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; });\n"
            "for (const lbl of labels) {\n"
            "  const b = btns.find(el => (el.innerText || el.value || '').trim().toLowerCase().startsWith(lbl));\n"
            "  if (b) { b.click(); return 'clicked:' + lbl; }\n"
            "}\n"
            "if (btns.length) { btns[btns.length - 1].click(); return 'clicked:last'; }\n"
            "return 'no-button-found';"
        )
        return self.b_js(js, "Click Next/Submit")

    # ------------------------------------------------------------------
    # Workflow assembler
    # ------------------------------------------------------------------

    def assemble_workflow(
        self,
        name: str,
        description: str,
        blocks: List[Dict],
        icon: str = "riIcons:survey-line",
        color: str = "#2563eb",
    ) -> Dict:
        """
        Assemble blocks into a complete Automa workflow object.
        Blocks are stored as a dict keyed by block ID (Automa's drawflow format).
        Connections are NOT auto-wired here — callers must set
        block["outputs"]["output-1"]["connections"] before calling assemble_workflow,
        or use connect_blocks() below.
        """
        return {
            "name":        name,
            "description": description,
            "icon":        icon,
            "color":       color,
            "version":     "1.0.0",
            "createdAt":   datetime.now().isoformat(),
            "drawflow": {
                "nodes": {b["id"]: b for b in blocks}
            },
            "settings": {
                "blockDelay":           500,
                "tabLoadTimeout":       30000,
                "onError":              "keep-running",
                "executedBlockOnWeb":   False,
                "publicId":             "",
                "restartTimes":         0,
                "notification":         False,
                "reuseLastState":       False,
            },
            "globalData": "{}",
        }

    @staticmethod
    def connect_blocks(blocks: List[Dict], from_idx: int, to_idx: int,
                       port: str = "output-1") -> None:
        """
        Wire from_idx → to_idx using the given output port.
        Mutates blocks in-place.
        """
        src = blocks[from_idx]
        dst = blocks[to_idx]
        if port not in src.get("outputs", {}):
            src.setdefault("outputs", {})[port] = {"connections": []}
        src["outputs"][port]["connections"].append(
            {"node": dst["id"], "output": "output-1"}
        )

    @staticmethod
    def connect_by_id(blocks: List[Dict], from_id: str, to_id: str,
                      port: str = "output-1") -> None:
        """Wire from_id → to_id. Mutates the matching block in-place."""
        for b in blocks:
            if b["id"] == from_id:
                b.setdefault("outputs", {}).setdefault(port, {"connections": []})
                b["outputs"][port]["connections"].append(
                    {"node": to_id, "output": "output-1"}
                )
                return

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

        p = {}

        if isinstance(prompt.get("demographic_data"), dict):
            p = dict(prompt["demographic_data"])

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

        # Merge top-level account fields the UI passes through
        for f in ("age","gender","city","education_level","job_status","income_range",
                  "marital_status","has_children","household_size","username","email"):
            if f in prompt and prompt[f] is not None:
                p.setdefault(f, prompt[f])

        return p

    def best_answer(self, question: Dict, persona: Dict) -> str:
        """Return the best string answer for a question given a persona."""
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

        if "age" in text and persona.get("age"):
            age = int(persona["age"])
            for o in options:
                m = re.search(r"(\d+)\s*[-–]\s*(\d+)", str(o))
                if m and int(m.group(1)) <= age <= int(m.group(2)):
                    return str(o)

        if "gender" in text and persona.get("gender"):
            r = pick_opt(persona["gender"])
            if r: return r

        if any(w in text for w in ("employment","job","occupation","work status")):
            if persona.get("job_status"):
                r = pick_opt(persona["job_status"].split()[0])
                if r: return r

        if "income" in text and persona.get("income_range"):
            r = pick_opt(persona["income_range"].split("-")[0].strip("$").strip())
            if r: return r

        if "education" in text and persona.get("education_level"):
            r = pick_opt(persona["education_level"])
            if r: return r

        if "marital" in text and persona.get("marital_status"):
            r = pick_opt(persona["marital_status"])
            if r: return r

        if "children" in text and persona.get("has_children") is not None:
            r = pick_opt("yes" if persona["has_children"] else "no")
            if r: return r

        if q_type in ("yes_no", "boolean"):
            return "Yes"

        if q_type == "rating" and options:
            return str(options[len(options) // 2])

        return str(options[0]) if options else ""

    # ------------------------------------------------------------------
    # DB helper — matches actual workflows table schema (no batch_id column)
    # ------------------------------------------------------------------

    def save_workflows_to_db(
        self,
        account_id: int,
        site_id: int,
        workflows: List[Dict],
        question_id: Optional[int] = None,
    ) -> int:
        """
        Persist a list of workflow dicts to the workflows table.
        Each dict must have at minimum: workflow_name, workflow_data.
        Returns count of successfully inserted rows.
        """
        if not self.db_manager or not workflows:
            return 0

        inserted = 0
        for wf in workflows:
            try:
                rows = self.db_manager.execute_query(
                    """
                    INSERT INTO workflows
                        (account_id, site_id, workflow_name, workflow_data,
                         question_id, is_active, uploaded_to_chrome)
                    VALUES (%s, %s, %s, %s::jsonb, %s, TRUE, FALSE)
                    RETURNING workflow_id
                    """,
                    (
                        account_id,
                        site_id,
                        wf.get("workflow_name", "unnamed"),
                        json.dumps(wf.get("workflow_data", {})),
                        wf.get("question_id") or question_id,
                    ),
                    fetch=True,
                )
                if rows:
                    wf_id = rows[0]["workflow_id"] if hasattr(rows[0], "keys") else rows[0][0]
                    wf["workflow_id"] = wf_id

                # Mark question as used
                qid = wf.get("question_id") or question_id
                if qid:
                    self.db_manager.execute_query(
                        "UPDATE questions SET used_in_workflow=TRUE, used_at=CURRENT_TIMESTAMP "
                        "WHERE question_id=%s",
                        (qid,),
                    )

                inserted += 1
            except Exception as exc:
                logger.error(f"save_workflows_to_db row error: {exc}")

        return inserted