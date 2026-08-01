"""
Stub out third-party modules that aren't installed in the test environment
(twilio, supabase, google-generativeai, etc.) so imports don't blow up.

Goal: the test suite runs hermetically with ONLY pytest + stdlib installed —
no Supabase, no Twilio, no Gemini API calls. CI installs nothing else.
"""

import os
import sys
from unittest.mock import MagicMock

# Heavy third-party stubs — must be inserted before any service module is imported.
# Anything we touch in `services/` or `workers/` and don't want to install in CI
# goes here.
_STUBS = [
    "twilio",
    "twilio.rest",
    "twilio.base",
    "twilio.base.exceptions",
    "supabase",
    "supabase.client",
    "google",
    "google.generativeai",
    "google.auth",
    "gspread",
    "psycopg2",
    "psycopg2.extras",
    "resend",
    "fpdf",
    "fpdf.fpdf",
    "num2words",
    "dotenv",
    "requests",
    "requests.auth",
    "openpyxl",
    "openpyxl.styles",
    "openpyxl.utils",
    "pandas",
    "google_api_python_client",
    "httpx",
    "fastapi",
]

def _stub_if_missing(mod_name: str) -> None:
    """Only stub a module that ISN'T already installed. Real deps win.
    In CI we install requirements.txt and so most real packages are
    present; the stubs only fill in what's still missing (e.g. resend,
    which isn't in our requirements). Locally without deps, all stubs apply.
    """
    if mod_name in sys.modules:
        return
    try:
        __import__(mod_name)
        # Real module imported cleanly — no stub needed.
        return
    except Exception:
        # Real module unavailable — install a stub so imports succeed.
        sys.modules[mod_name] = MagicMock()


for mod in _STUBS:
    _stub_if_missing(mod)


# Make TwilioRestException catchable as a real Exception subclass so service
# code doing `except TwilioRestException` doesn't blow up at runtime.
class _StubTwilioRestException(Exception):
    def __init__(self, *args, code=None, msg=None, **kw):
        super().__init__(*args)
        self.code = code
        self.msg = msg or (args[0] if args else "")


sys.modules["twilio.base.exceptions"].TwilioRestException = _StubTwilioRestException


# Make dotenv.load_dotenv a no-op so worker imports succeed.
sys.modules["dotenv"].load_dotenv = lambda *a, **kw: False


# Placeholder credential values injected below so service constructors don't
# error on import. Anything checking "do I have a REAL credential?" must
# compare against these, NOT just `os.getenv(...)` truthiness — see
# has_real_ai_key() for why that distinction is load-bearing.
PLACEHOLDER_AI_KEY = "test_ai_key"

# Provide minimal config env vars so service constructors don't error on import.
os.environ.setdefault("TWILIO_ACCOUNT_SID", "test_sid")
os.environ.setdefault("TWILIO_AUTH_TOKEN", "test_token")
os.environ.setdefault("TWILIO_WHATSAPP_NUMBER", "whatsapp:+10000000000")
os.environ.setdefault("BASE_URL", "https://test.example.com")
os.environ.setdefault("AI_KEY", PLACEHOLDER_AI_KEY)
os.environ.setdefault("RESEND_API", "test_resend_key")
os.environ.setdefault("RESEND_FROM_EMAIL", "test@example.com")


def has_real_ai_key() -> bool:
    """True only when a GENUINE AI_KEY is present.

    Live-LLM tests used to guard with a bare `if not os.getenv("AI_KEY")`,
    which never fired: the setdefault above (and the identical value in
    .github/workflows/tests.yml) means AI_KEY is ALWAYS set. So those tests
    never skipped — they ran against a bogus key and failed on every run,
    every time, for everyone. Eleven permanent red tests trained us to read
    "11 failed" as the expected baseline, which is exactly how a real
    regression sneaks past unnoticed.

    Checking the value rather than mere presence is what makes the skip
    honest: absent OR placeholder → skip; anything else → really run.
    """
    key = (os.getenv("AI_KEY") or "").strip()
    return bool(key) and key != PLACEHOLDER_AI_KEY


def has_real_db_url() -> bool:
    """True only when a genuine SUPABASE_DB_URL is present. Same discipline
    as has_real_ai_key — no placeholder is injected for this one today, but
    guarding through one helper means a future setdefault can't silently
    defeat the e2e suite's skip the way it did the live-LLM suite's."""
    url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    return bool(url) and url.startswith("postgres")


# ── `live` marker (TODO.md Phase 4.2) ──────────────────────────────────
# The e2e suite hits a REAL database and makes REAL (paid) AI calls, so it
# must never run as part of the default `pytest tests/` that CI executes.
#
# Deliberately implemented as conftest hooks rather than a new pytest.ini
# with `addopts = -m "not live"`: CLAUDE.md's working style prefers a
# one-line fix over a new config file, tests/conftest.py already exists,
# and this repo has no pytest config at all today. It also degrades more
# safely — a plain `-m live` with no config would silently select nothing
# AND warn about an unknown marker; here, opting in is an explicit --live.

def pytest_addoption(parser):
    parser.addoption(
        "--live",
        action="store_true",
        default=False,
        help="Run @pytest.mark.live e2e tests (real DB + real paid AI calls).",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "live: hits a real database and makes real paid AI calls; "
        "skipped unless --live is passed.",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--live"):
        return
    skip_live = __import__("pytest").mark.skip(
        reason="needs --live (real DB + real paid AI calls)"
    )
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)
