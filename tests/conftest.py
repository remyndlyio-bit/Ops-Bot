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


# Provide minimal config env vars so service constructors don't error on import.
os.environ.setdefault("TWILIO_ACCOUNT_SID", "test_sid")
os.environ.setdefault("TWILIO_AUTH_TOKEN", "test_token")
os.environ.setdefault("TWILIO_WHATSAPP_NUMBER", "whatsapp:+10000000000")
os.environ.setdefault("BASE_URL", "https://test.example.com")
os.environ.setdefault("AI_KEY", "test_ai_key")
os.environ.setdefault("RESEND_API", "test_resend_key")
os.environ.setdefault("RESEND_FROM_EMAIL", "test@example.com")
