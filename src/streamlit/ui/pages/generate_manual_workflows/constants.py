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

# Screenshots – new flow (v7.0)
ALLOWED_SCREENSHOTS = frozenset({
    "browser_launched",
    "dashboard_loaded",
    "navigation_error",
    "navigation_error_final",
    "start_survey",
    "midpoint_survey",
    "after_completion",
})

SCREENSHOT_LABELS = {
    "browser_launched":    "🌐 Browser Launched (new tab)",
    "dashboard_loaded":    "📊 Dashboard Loaded",
    "navigation_error":    "🚨 Navigation Error",
    "navigation_error_final": "🚨 Navigation Error (Final Attempt)",
    "start_survey":        "🚀 Survey Started",
    "midpoint_survey":     "⏸️ Midpoint of Survey",
    "after_completion":    "✅ Survey Completed",
}

# ---------------------------------------------------------------------------
# Default proxy — Proxy-Cheap rotating residential SOCKS5 (USA)
#
# PORT GUIDE:
# ┌─────────┬──────────────────────────────────────────────────────────────┐
# │  Port   │ Purpose                                                      │
# ├─────────┼──────────────────────────────────────────────────────────────┤
# │  5959   │ Rotating residential — HTTP only. Cannot tunnel HTTPS via   │
# │         │ CONNECT. Do NOT use for browser automation against HTTPS.    │
# ├─────────┼──────────────────────────────────────────────────────────────┤
# │  9595   │ Rotating residential — SOCKS5. Proxies the full TCP         │
# │         │ connection, works for both HTTP and HTTPS targets without    │
# │         │ needing HTTP CONNECT support. Use this port.                 │
# │         │ Chrome receives --proxy-server=socks5://host:9595 directly. │
# └─────────┴──────────────────────────────────────────────────────────────┘
#
# HOW THE PROXY IS USED IN BROWSER AUTOMATION:
# - Chrome is launched WITH --proxy-server=socks5://proxy-us.proxy-cheap.com:9595
# - Credentials are percent-encoded and embedded in the --proxy-server flag URL.
# - Chrome handles SOCKS5 auth natively at the process level.
# - Playwright contexts are created plain (no proxy dict) — Playwright does NOT
#   support SOCKS5 proxy authentication at the context level.
#
# HOW THE PRE-FLIGHT TEST WORKS:
# - _test_upstream_proxy() hits http://ipv4.icanhazip.com via socks5h:// proxy.
# - A successful test returns the external IP assigned by Proxy-Cheap.
#
# USERNAME FORMAT (Proxy-Cheap credential from dashboard):
# - pcwS65b60G-resfix-us-nnid-0  →  fixed US residential, session 0
# - pcwS65b60G-res-any           →  rotating, any country
# Country/session targeting is baked into the username at credential-generation
# time on the Proxy-Cheap dashboard. Do NOT append -country-XX suffixes
# (that is BrightData convention only).
#
# BANDWIDTH NOTE:
# - This proxy is bandwidth-based. Monitor usage on the Proxy-Cheap dashboard.
# - When bandwidth is exhausted the proxy returns connection errors.
# - Top up at: https://app.proxy-cheap.com
# ---------------------------------------------------------------------------
DEFAULT_PROXY = {
    "proxy_type": "socks5",
    "host":       "proxy-us.proxy-cheap.com",
    "port":       9595,                          # ← SOCKS5 port
    "username":   "pcwS65b60G-resfix-us-nnid-0",
    "password":   "PC_7HMCucvPdRZZ5kcvn",
    "country":    "US",
}

# Proxy-Cheap does not use country-suffix usernames like BrightData.
# Country targeting is baked into the username at credential-generation time
# on the Proxy-Cheap dashboard (e.g. pcwS65b60G-res-any for any country).
PROXY_COUNTRIES = [
    "US", "GB", "CA", "AU", "DE", "FR", "JP", "BR", "IN", "IT", "ES", "NL",
    "SE", "NO", "DK", "FI", "BE", "CH", "AT", "IE", "PL", "CZ", "PT", "GR",
    "HU", "RO", "ZA", "IL", "AE", "SG", "HK", "KR", "TW", "MX", "CO", "CL",
    "AR", "PE", "VE",
]

# CAPTCHA solver (Capsolver) — set environment variable CAPSOLVER_API_KEY
CAPSOLVER_API_KEY = os.environ.get("CAPSOLVER_API_KEY", "")
CAPSOLVER_API_URL = "https://api.capsolver.com"