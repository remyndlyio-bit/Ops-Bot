"""
TODO.md Phase 4.1 — seeded fixture account.

Problem this solves (quoting the plan): "all live testing so far ran against
one shared WhatsApp account. State leaked between runs and between scenarios;
roughly HALF the 'failures' in every run were contamination, not bugs."

So every e2e run gets its OWN synthetic user id (`e2etest:<uuid>`) with a
known, fully-deterministic set of rows. No shared state, no leakage between
runs, and a teardown that removes exactly what was created.

Design notes
------------
* **Expected values are DERIVED from the fixture data, never hardcoded.**
  `TOTAL_FEES`, `UNPAID_TOTAL`, `CLIENT_COUNT` etc. are computed from
  `FIXTURE_ROWS` at import time, so a scenario asserting "total billing is
  ₹X" cannot silently drift out of sync with the seed. Change a row, the
  expectation changes with it.

* **Dates are relative to today, with fixed offsets.** Absolute dates would
  make "this month" / "last month" scenarios rot within weeks. Offsets keep
  temporal queries meaningful forever while staying reproducible within a
  run. The computed dates are exported so assertions can reference them.

* **Teardown is prefix-guarded.** `teardown()` refuses to run against any
  user_id lacking the `e2etest:` prefix. This is the one piece of the
  harness that issues DELETEs against a production table, so it is written
  to be incapable of touching a real user's rows even if called wrongly.

Requires SUPABASE_DB_URL. Callers should gate on `tests.conftest.has_real_db_url()`.
"""

from __future__ import annotations

import os
import uuid
from datetime import date, timedelta
from typing import Any, Dict, List, Optional

# Every synthetic e2e user id starts with this. The teardown guard keys off
# it — see _assert_synthetic().
E2E_USER_PREFIX = "e2etest:"


# ─────────────────────────────────────────────────────────────────────
# Fixture data
# ─────────────────────────────────────────────────────────────────────

def _d(days_ago: int) -> str:
    """A date `days_ago` days before today, as YYYY-MM-DD."""
    return (date.today() - timedelta(days=days_ago)).isoformat()


# Offsets chosen so the set spans several calendar months regardless of when
# it runs: something in the last few days, something ~5 weeks back, and
# something ~3 months back.
RECENT = 3        # this month (unless run in the first 3 days of a month)
LAST_MONTH = 35   # reliably a different month than today
OLD = 95          # ~3 months back

# 15 deterministic rows. Coverage is deliberate — each row exists to make
# some class of scenario answerable:
#   * paid AND unpaid rows                  → payment-status queries
#   * three distinct clients                → group-by / "biggest client"
#   * three distinct months                 → date-range queries
#   * a client with NO poc_email            → "clients with no email"
#   * a row with NO job_date                → null-date handling
#   * bill_sent yes/no mix                  → "invoices yet to send"
FIXTURE_ROWS: List[Dict[str, Any]] = [
    # ── Acme Studios: 5 jobs, fully invoiced, mixed payment ──────────
    {"client_name": "Acme Studios", "job_date": _d(RECENT),     "fees": 25000, "paid": "Yes", "bill_sent": "Yes", "poc_email": "accounts@acme.test",  "poc_name": "Rhea Menon",  "job_description_details": "Master film 30s"},
    {"client_name": "Acme Studios", "job_date": _d(RECENT + 1), "fees": 18000, "paid": "Yes", "bill_sent": "Yes", "poc_email": "accounts@acme.test",  "poc_name": "Rhea Menon",  "job_description_details": "Cutdown 15s"},
    {"client_name": "Acme Studios", "job_date": _d(LAST_MONTH), "fees": 42000, "paid": "No",  "bill_sent": "Yes", "poc_email": "accounts@acme.test",  "poc_name": "Rhea Menon",  "job_description_details": "Brand campaign"},
    {"client_name": "Acme Studios", "job_date": _d(LAST_MONTH + 2), "fees": 12000, "paid": "No", "bill_sent": "Yes", "poc_email": "accounts@acme.test", "poc_name": "Rhea Menon", "job_description_details": "Social edit"},
    {"client_name": "Acme Studios", "job_date": _d(OLD),        "fees": 30000, "paid": "Yes", "bill_sent": "Yes", "poc_email": "accounts@acme.test",  "poc_name": "Rhea Menon",  "job_description_details": "Launch film"},

    # ── Bridgestone: 5 jobs, NO poc_email anywhere (the "no email" case) ──
    {"client_name": "Bridgestone", "job_date": _d(RECENT + 2),  "fees": 55000, "paid": "No",  "bill_sent": "Yes", "poc_email": None, "poc_name": "Vikram Shah", "job_description_details": "TVC master"},
    {"client_name": "Bridgestone", "job_date": _d(RECENT + 4),  "fees": 22000, "paid": "No",  "bill_sent": "No",  "poc_email": None, "poc_name": "Vikram Shah", "job_description_details": "Radio spot"},
    {"client_name": "Bridgestone", "job_date": _d(LAST_MONTH + 1), "fees": 17000, "paid": "Yes", "bill_sent": "Yes", "poc_email": None, "poc_name": "Vikram Shah", "job_description_details": "Dubbing"},
    {"client_name": "Bridgestone", "job_date": _d(OLD + 3),     "fees": 26000, "paid": "Yes", "bill_sent": "Yes", "poc_email": None, "poc_name": "Vikram Shah", "job_description_details": "Print shoot"},
    {"client_name": "Bridgestone", "job_date": _d(OLD + 5),     "fees": 9000,  "paid": "No",  "bill_sent": "No",  "poc_email": None, "poc_name": "Vikram Shah", "job_description_details": "Voiceover"},

    # ── Nordic Films: 4 jobs, invoices largely NOT sent ───────────────
    {"client_name": "Nordic Films", "job_date": _d(RECENT + 5), "fees": 64000, "paid": "No",  "bill_sent": "No",  "poc_email": "pay@nordic.test", "poc_name": "Ingrid Olsen", "job_description_details": "Documentary edit"},
    {"client_name": "Nordic Films", "job_date": _d(LAST_MONTH + 4), "fees": 38000, "paid": "No", "bill_sent": "No", "poc_email": "pay@nordic.test", "poc_name": "Ingrid Olsen", "job_description_details": "Colour grade"},
    {"client_name": "Nordic Films", "job_date": _d(OLD + 1),    "fees": 15000, "paid": "Yes", "bill_sent": "Yes", "poc_email": "pay@nordic.test", "poc_name": "Ingrid Olsen", "job_description_details": "Sound mix"},
    {"client_name": "Nordic Films", "job_date": _d(OLD + 7),    "fees": 21000, "paid": "Yes", "bill_sent": "Yes", "poc_email": "pay@nordic.test", "poc_name": "Ingrid Olsen", "job_description_details": "Subtitling"},

    # ── The no-job-date row (null-date handling) ──────────────────────
    {"client_name": "Nordic Films", "job_date": None,           "fees": 7000,  "paid": "No",  "bill_sent": "No",  "poc_email": "pay@nordic.test", "poc_name": "Ingrid Olsen", "job_description_details": "Undated retainer"},
]

# Bank details, so invoice-generation scenarios have something to render.
FIXTURE_BANK = {
    "bank_account_name":   "E2E Test Account",
    "bank_account_number": "000111222333",
    "bank_ifsc":           "HDFC0001234",
    "bank_name":           "HDFC Bank",
    "upi_id":              "e2etest@upi",
}


# ─────────────────────────────────────────────────────────────────────
# Derived expectations — assertions reference THESE, not literals, so the
# fixture and its expected values can never drift apart.
# ─────────────────────────────────────────────────────────────────────

def _is_yes(v: Optional[str]) -> bool:
    return str(v or "").strip().lower() in ("yes", "true", "1", "paid")


ROW_COUNT     = len(FIXTURE_ROWS)
TOTAL_FEES    = sum(r["fees"] for r in FIXTURE_ROWS)
PAID_TOTAL    = sum(r["fees"] for r in FIXTURE_ROWS if _is_yes(r["paid"]))
UNPAID_TOTAL  = sum(r["fees"] for r in FIXTURE_ROWS if not _is_yes(r["paid"]))
PAID_COUNT    = sum(1 for r in FIXTURE_ROWS if _is_yes(r["paid"]))
UNPAID_COUNT  = ROW_COUNT - PAID_COUNT
CLIENTS       = sorted({r["client_name"] for r in FIXTURE_ROWS})
CLIENT_COUNT  = len(CLIENTS)
AVG_FEES      = TOTAL_FEES / ROW_COUNT
BILL_SENT_COUNT     = sum(1 for r in FIXTURE_ROWS if _is_yes(r["bill_sent"]))
BILL_NOT_SENT_COUNT = ROW_COUNT - BILL_SENT_COUNT
NO_EMAIL_COUNT      = sum(1 for r in FIXTURE_ROWS if not r["poc_email"])
NO_DATE_COUNT       = sum(1 for r in FIXTURE_ROWS if not r["job_date"])

# Per-client totals — for "biggest client" / "how much does X owe me".
FEES_BY_CLIENT: Dict[str, int] = {
    c: sum(r["fees"] for r in FIXTURE_ROWS if r["client_name"] == c)
    for c in CLIENTS
}
UNPAID_BY_CLIENT: Dict[str, int] = {
    c: sum(r["fees"] for r in FIXTURE_ROWS
           if r["client_name"] == c and not _is_yes(r["paid"]))
    for c in CLIENTS
}
BIGGEST_CLIENT = max(FEES_BY_CLIENT, key=FEES_BY_CLIENT.get)


# ─────────────────────────────────────────────────────────────────────
# DB plumbing
# ─────────────────────────────────────────────────────────────────────

def _connect():
    """Open a psycopg2 connection to SUPABASE_DB_URL.

    Mirrors SupabaseService's own connection settings (short connect
    timeout + statement timeout) so a misconfigured URL fails fast in a
    test run instead of hanging the suite.
    """
    db_url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    if not db_url:
        raise RuntimeError(
            "SUPABASE_DB_URL is not set — e2e seeding needs a real database. "
            "Gate the caller on tests.conftest.has_real_db_url()."
        )
    import psycopg2  # imported lazily: the offline suite stubs this module
    return psycopg2.connect(
        db_url, connect_timeout=5, options="-c statement_timeout=15000",
    )


def _assert_synthetic(user_id: str) -> None:
    """Refuse to operate on anything that isn't a synthetic e2e id.

    This is the guard that makes teardown safe to keep in the repo: the
    only DELETE statements in the test suite are scoped by user_id, and
    this makes it impossible to scope one at a real user (a Telegram
    chat_id or a WhatsApp phone number) by accident or by a bad caller.
    """
    if not user_id or not str(user_id).startswith(E2E_USER_PREFIX):
        raise ValueError(
            f"refusing to touch non-synthetic user_id {user_id!r} — "
            f"e2e fixtures only operate on ids prefixed {E2E_USER_PREFIX!r}"
        )


def new_user_id() -> str:
    """A fresh synthetic user id, unique per call."""
    return f"{E2E_USER_PREFIX}{uuid.uuid4()}"


def seed(user_id: Optional[str] = None) -> str:
    """Create a fresh fixture account and return its user_id.

    Inserts FIXTURE_ROWS into job_entries and FIXTURE_BANK into
    user_config. Idempotent per user_id in the sense that each call with a
    fresh id produces an identical dataset; pass an explicit user_id only
    if you need to reseed a known account (it is cleared first).
    """
    uid = user_id or new_user_id()
    _assert_synthetic(uid)

    conn = _connect()
    try:
        with conn:
            with conn.cursor() as cur:
                # Clear first so an explicit reseed is deterministic.
                cur.execute("DELETE FROM public.job_entries WHERE user_id = %s", (uid,))
                cur.execute("DELETE FROM public.user_config WHERE user_id = %s", (uid,))

                for row in FIXTURE_ROWS:
                    cur.execute(
                        """
                        INSERT INTO public.job_entries
                            (user_id, client_name, job_date, fees, paid,
                             bill_sent, poc_email, poc_name,
                             job_description_details)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            uid, row["client_name"], row["job_date"], row["fees"],
                            row["paid"], row["bill_sent"], row["poc_email"],
                            row["poc_name"], row["job_description_details"],
                        ),
                    )

                cur.execute(
                    """
                    INSERT INTO public.user_config
                        (user_id, bank_account_name, bank_account_number,
                         bank_ifsc, bank_name, upi_id)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (user_id) DO UPDATE SET
                        bank_account_name   = EXCLUDED.bank_account_name,
                        bank_account_number = EXCLUDED.bank_account_number,
                        bank_ifsc           = EXCLUDED.bank_ifsc,
                        bank_name           = EXCLUDED.bank_name,
                        upi_id              = EXCLUDED.upi_id
                    """,
                    (
                        uid, FIXTURE_BANK["bank_account_name"],
                        FIXTURE_BANK["bank_account_number"], FIXTURE_BANK["bank_ifsc"],
                        FIXTURE_BANK["bank_name"], FIXTURE_BANK["upi_id"],
                    ),
                )
    finally:
        conn.close()
    return uid


def teardown(user_id: str) -> None:
    """Delete every row belonging to a synthetic e2e user.

    Raises ValueError (before opening a connection) if user_id isn't
    prefixed `e2etest:` — see _assert_synthetic.
    """
    _assert_synthetic(user_id)

    conn = _connect()
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM public.job_entries  WHERE user_id = %s", (user_id,))
                cur.execute("DELETE FROM public.user_config  WHERE user_id = %s", (user_id,))
                cur.execute("DELETE FROM public.user_profiles WHERE user_id = %s", (user_id,))
    finally:
        conn.close()
