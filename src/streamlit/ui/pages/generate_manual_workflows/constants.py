"""
Constants for Generate Manual Workflows page.
"""

import os

# Status constants
STATUS_COMPLETE = "complete"
STATUS_PASSED   = "passed"
STATUS_FAILED   = "failed"
STATUS_PENDING  = "pending"
STATUS_ERROR    = "error"

# Model registry
MODEL_REGISTRY = {
    "openai — GPT-4o": {
        "cls": "ChatOpenAI",
        "kwargs": {"model": "gpt-4o", "temperature": 0.7},
    },
    "anthropic — Claude 3.5": {
        "cls": "ChatAnthropic",
        "kwargs": {"model": "claude-3-5-sonnet-20241022", "temperature": 0.7},
    },
    "gemini — Gemini 2.5 Flash": {
        "cls": "ChatGoogleGenerativeAI",
        "kwargs": {"model": "gemini-2.5-flash", "temperature": 0.7},
    },
}

MODEL_ENV_KEYS = {
    "openai — GPT-4o":           "OPENAI_API_KEY",
    "anthropic — Claude 3.5":    "ANTHROPIC_API_KEY",
    "gemini — Gemini 2.5 Flash": "GEMINI_API_KEY",
}

# Keyword detection
COMPLETE_KEYWORDS = [
    "thank you", "thank-you", "thankyou", "survey complete", "survey completed",
    "you have completed", "submission received", "response recorded",
    "reward", "points added", "earned", "credited",
    "all done", "finished", "successfully submitted",
]

DISQUALIFIED_KEYWORDS = [
    "disqualif", "screen out", "screened out", "not eligible",
    "don't qualify", "do not qualify", "unfortunately", "not a match",
    "not selected", "quota full", "quota reached", "sorry, ", "we're sorry",
]

# Screenshots – added "navigation_error"
ALLOWED_SCREENSHOTS = frozenset({
    "01_survey_tab_open", "02_qualification_start", "03_qualification_done",
    "04_survey_started", "05_survey_complete", "navigation_error"
})
SCREENSHOT_LABELS = {
    "01_survey_tab_open":      "1️⃣ Survey Tab Open",
    "02_qualification_start":  "2️⃣ Qualification Started",
    "03_qualification_done":   "3️⃣ Qualification Done",
    "04_survey_started":       "4️⃣ Survey Started",
    "05_survey_complete":      "5️⃣ Survey Complete",
    "navigation_error":        "🚨 Navigation Error (ERR_EMPTY_RESPONSE)",
}

# Default BrightData proxy (USA)
DEFAULT_PROXY = {
    "proxy_type": "http",
    "host": "brd.superproxy.io",
    "port": 33335,
    "username": "brd-customer-hl_9f5def12-zone-residential_proxy1",
    "password": "mviek6dqzsbm",
    "country": "US"
}

# List of common country codes for BrightData
PROXY_COUNTRIES = [
    "US", "GB", "CA", "AU", "DE", "FR", "JP", "BR", "IN", "IT", "ES", "NL",
    "SE", "NO", "DK", "FI", "BE", "CH", "AT", "IE", "PL", "CZ", "PT", "GR",
    "HU", "RO", "ZA", "IL", "AE", "SG", "HK", "KR", "TW", "MX", "CO", "CL", "AR", "PE", "VE"
]

# CAPTCHA solver (Capsolver) – set environment variable CAPSOLVER_API_KEY
CAPSOLVER_API_KEY = os.environ.get("CAPSOLVER_API_KEY", "")
CAPSOLVER_API_URL = "https://api.capsolver.com"