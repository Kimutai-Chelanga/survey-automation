"""
src/streamlit/ui/pages/generate_manual_workflows/gemini_answer_service.py

Gemini-powered answer generation service.
Uses the official google-generativeai SDK.
Sends each question + the user's persona prompt to Gemini and saves answers to DB.
"""

import json
import logging
import os
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import google.generativeai as genai
from psycopg2.extras import RealDictCursor

from src.core.database.postgres.connection import get_postgres_connection

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gemini client — configured once per call (stateless, safe for Streamlit)
# ---------------------------------------------------------------------------

def call_gemini(
    system_prompt: str,
    user_message: str,
    api_key: str,
    model_name: str = "gemini-2.0-flash",
) -> Optional[str]:
    """
    Call Gemini API using the official SDK.

    system_prompt  → the user's full persona text (from the Prompts page)
    user_message   → the question + instructions for this specific question
    Returns the raw text response, or None on failure.
    """
    try:
        genai.configure(api_key=api_key)

        model = genai.GenerativeModel(
            model_name=model_name,
            system_instruction=system_prompt,   # persona sent as system context
        )

        response = model.generate_content(
            user_message,
            generation_config=genai.types.GenerationConfig(
                temperature=0.3,        # low = consistent persona answers
                max_output_tokens=256,
                top_p=0.8,
            ),
        )

        return response.text.strip() if response.text else None

    except Exception as exc:
        logger.error(f"Gemini API error: {exc}")
        return None


# ---------------------------------------------------------------------------
# Prompt builder — constructs the per-question user message
# ---------------------------------------------------------------------------

def build_question_prompt(question: Dict[str, Any]) -> str:
    """
    Build the user-turn message sent to Gemini for a single question.
    Gemini must reply with ONLY the chosen answer — nothing else.
    """
    q_type  = question.get("question_type", "text")
    q_text  = question.get("question_text", "")
    options = question.get("options") or []

    # Normalise options — DB may store them as a JSON string or a list
    if isinstance(options, str):
        try:
            options = json.loads(options)
        except Exception:
            options = []

    lines = [
        "Answer the following survey question as your persona.",
        "",
        f"Question: {q_text}",
        f"Question type: {q_type}",
    ]

    if options:
        lines.append(f"Available options: {', '.join(str(o) for o in options)}")
        lines.append("")
        lines.append(
            "You MUST choose EXACTLY ONE option from the list above. "
            "Reply with only that option text — nothing else, no explanation."
        )
    elif q_type == "yes_no":
        lines.append("")
        lines.append("Reply with only 'Yes' or 'No' — nothing else.")
    elif q_type == "rating":
        lines.append("")
        lines.append(
            "Reply with only a single integer that fits naturally for your persona. "
            "Typical range is 1–10 unless context suggests otherwise. "
            "No explanation — just the number."
        )
    else:
        # text / textarea / open-ended
        lines.append("")
        lines.append(
            "Reply with a short, natural answer (1–2 sentences maximum). "
            "No preamble, no explanation — just the answer itself."
        )

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Answer parser — converts raw Gemini text → typed DB columns
# ---------------------------------------------------------------------------

def parse_gemini_answer(raw: str, question: Dict[str, Any]) -> Dict[str, Any]:
    """
    Return a dict with keys matching the answers table columns:
        answer_text            TEXT
        answer_value_numeric   NUMERIC  (or None)
        answer_value_boolean   BOOLEAN  (or None)
    """
    q_type  = question.get("question_type", "text")
    cleaned = raw.strip().strip('"').strip("'")

    result: Dict[str, Any] = {
        "answer_text":          cleaned,
        "answer_value_numeric": None,
        "answer_value_boolean": None,
    }

    if q_type == "yes_no":
        lower = cleaned.lower()
        if lower in ("yes", "true", "1"):
            result["answer_value_boolean"] = True
            result["answer_text"]          = "Yes"
        else:
            result["answer_value_boolean"] = False
            result["answer_text"]          = "No"

    elif q_type == "rating":
        try:
            result["answer_value_numeric"] = float(cleaned.split()[0])
        except (ValueError, IndexError):
            pass

    return result


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def _pg():
    return get_postgres_connection()


def save_answer(
    question_id:          int,
    account_id:           int,
    answer_text:          str,
    answer_value_numeric: Optional[float],
    answer_value_boolean: Optional[bool],
    batch_id:             str,
    workflow_id:          Optional[int] = None,
) -> Optional[int]:
    """Insert one answer row and return the new answer_id, or None on failure."""
    try:
        with _pg() as conn:
            with conn.cursor() as c:
                c.execute(
                    """
                    INSERT INTO answers
                        (question_id, account_id, answer_text,
                         answer_value_numeric, answer_value_boolean,
                         submission_batch_id, workflow_id, submitted_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                    RETURNING answer_id
                    """,
                    (
                        question_id, account_id, answer_text,
                        answer_value_numeric, answer_value_boolean,
                        batch_id, workflow_id,
                    ),
                )
                answer_id = c.fetchone()[0]
                conn.commit()
                return answer_id
    except Exception as exc:
        logger.error(f"save_answer failed: {exc}")
        return None


def log_workflow_generation(
    account_id:          int,
    site_id:             int,
    prompt_id:           Optional[int],
    username:            str,
    batch_id:            str,
    questions_processed: int,
    answers_generated:   int,
    status:              str = "success",
    error_message:       Optional[str] = None,
) -> None:
    """Write one row to workflow_generation_log for audit purposes."""
    try:
        with _pg() as conn:
            with conn.cursor() as c:
                c.execute(
                    """
                    INSERT INTO workflow_generation_log
                        (workflow_type, workflow_name, account_id, site_id,
                         prompt_id, username, status,
                         questions_processed, answers_generated,
                         error_message, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        "answer_generation",
                        f"Gemini answers {batch_id}",
                        account_id, site_id, prompt_id, username, status,
                        questions_processed, answers_generated,
                        error_message,
                        json.dumps({"batch_id": batch_id}),
                    ),
                )
                conn.commit()
    except Exception as exc:
        logger.error(f"log_workflow_generation failed: {exc}")


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_answers_with_gemini(
    questions:          List[Dict[str, Any]],
    prompt:             Dict[str, Any],
    account_id:         int,
    site_id:            int,
    progress_callback:  Optional[Callable[[int, int, str], None]] = None,
) -> Dict[str, Any]:
    """
    For each question in the list:
      1. Build the user message (question + type + options)
      2. Call Gemini with the persona as system instruction
      3. Parse the answer into the correct DB column types
      4. Save to the answers table

    progress_callback signature: (current: int, total: int, message: str) -> None

    Returns:
    {
        success:           bool,
        answers_generated: int,
        failed:            int,
        batch_id:          str,
        details:           list[dict],   — one entry per question
        error:             str | None,
    }
    """
    api_key = os.environ.get("GEMINI_API_KEY", "")
    if not api_key:
        return {
            "success":           False,
            "error":             "GEMINI_API_KEY not found in environment variables.",
            "answers_generated": 0,
            "failed":            0,
            "batch_id":          None,
            "details":           [],
        }

    # The full persona text the user wrote in the Prompts page
    system_prompt = prompt.get("content", "You are a survey respondent.")
    prompt_id     = prompt.get("prompt_id")
    username      = prompt.get("username", str(account_id))

    batch_id = (
        f"gemini_{uuid.uuid4().hex[:12]}_"
        f"{datetime.now().strftime('%Y%m%d%H%M%S')}"
    )

    results_detail: List[Dict[str, Any]] = []
    answers_saved  = 0
    failed         = 0
    total          = len(questions)

    for idx, question in enumerate(questions):
        q_id   = question["question_id"]
        q_text = question["question_text"]

        if progress_callback:
            progress_callback(idx + 1, total, f"Answering: {q_text[:60]}…")

        # ---- 1. Ask Gemini ----
        user_msg = build_question_prompt(question)
        raw      = call_gemini(system_prompt, user_msg, api_key)

        if not raw:
            failed += 1
            results_detail.append({
                "question_id":   q_id,
                "question_text": q_text,
                "status":        "failed",
                "error":         "Gemini returned no response",
                "answer":        None,
            })
            logger.warning(f"Gemini no response for question_id={q_id}")
            continue

        # ---- 2. Parse ----
        parsed = parse_gemini_answer(raw, question)

        # ---- 3. Save ----
        answer_id = save_answer(
            question_id          = q_id,
            account_id           = account_id,
            answer_text          = parsed["answer_text"],
            answer_value_numeric = parsed["answer_value_numeric"],
            answer_value_boolean = parsed["answer_value_boolean"],
            batch_id             = batch_id,
        )

        if answer_id:
            answers_saved += 1
            results_detail.append({
                "question_id":   q_id,
                "question_text": q_text,
                "status":        "success",
                "answer":        parsed["answer_text"],
                "answer_id":     answer_id,
            })
            logger.debug(
                f"Saved answer_id={answer_id} "
                f"q_id={q_id} answer='{parsed['answer_text'][:60]}'"
            )
        else:
            failed += 1
            results_detail.append({
                "question_id":   q_id,
                "question_text": q_text,
                "status":        "db_error",
                "error":         "Failed to save to DB",
                "answer":        parsed["answer_text"],
            })

    # ---- 4. Audit log ----
    log_workflow_generation(
        account_id           = account_id,
        site_id              = site_id,
        prompt_id            = prompt_id,
        username             = username,
        batch_id             = batch_id,
        questions_processed  = total,
        answers_generated    = answers_saved,
        status               = "success" if answers_saved > 0 else "failed",
        error_message        = f"{failed} questions failed" if failed else None,
    )

    return {
        "success":           answers_saved > 0,
        "answers_generated": answers_saved,
        "failed":            failed,
        "batch_id":          batch_id,
        "details":           results_detail,
        "error":             None if answers_saved > 0 else "All questions failed",
    }