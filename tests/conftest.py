"""
Stub out third-party modules that aren't installed in the test environment
(twilio, supabase, google-generativeai, etc.) so imports don't blow up.
"""

import sys
from unittest.mock import MagicMock

# Heavy third-party stubs — must be inserted before any service module is imported
_STUBS = [
    "twilio",
    "twilio.rest",
    "supabase",
    "supabase.client",
    "google.generativeai",
    "google.auth",
    "gspread",
    "psycopg2",
    "psycopg2.extras",
    "resend",
]

for mod in _STUBS:
    if mod not in sys.modules:
        sys.modules[mod] = MagicMock()
