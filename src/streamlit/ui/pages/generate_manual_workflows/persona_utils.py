"""
Build persona system message for the AI agent.
"""

from typing import Dict


def build_persona_system_message(prompt: Dict, acct: Dict) -> str:
    lines = ["You are a specific person answering survey questions. Embody this identity:", ""]
    for field, label in [
        ("age", "Age"), ("gender", "Gender"), ("city", "City/Location"),
        ("education_level", "Education"), ("job_status", "Employment"),
        ("income_range", "Income"), ("marital_status", "Marital status"),
        ("household_size", "Household size"), ("industry", "Industry"),
    ]:
        if acct.get(field):
            lines.append(f"• {label}: {acct[field]}")
    if acct.get("has_children") is not None:
        lines.append(f"• Has children: {'Yes' if acct['has_children'] else 'No'}")
    if prompt and prompt.get("content"):
        lines += ["", "Additional persona details:", prompt["content"].strip()]
    lines += [
        "", "Rules:",
        "- Stay consistent throughout the survey",
        "- For free-text: 1–2 natural sentences",
        "- Pick the most characteristic answer for this persona",
    ]
    return "\n".join(lines)