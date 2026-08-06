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

import json
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

# Year to use for the sheet's hardcoded April rows (below). Last year when
# the suite runs before May, this year otherwise — so "10 April" is always a
# date that has already happened. A future-dated fixture row would quietly
# break every "this year" / "last quarter" aggregate.
_APRIL_YEAR = date.today().year - (1 if date.today().month < 5 else 0)

# Deterministic rows. Coverage is deliberate — each row exists to make some
# class of scenario answerable:
#   * paid AND unpaid rows                  → payment-status queries
#   * five distinct clients                 → group-by / "biggest client"
#   * several distinct months               → date-range queries
#   * a client with NO poc_email            → "clients with no email"
#   * a row with NO job_date                → null-date handling
#   * bill_sent yes/no mix                  → "invoices yet to send"
#   * clients WITH and WITHOUT billing details → invoice generation both
#     proceeds (Acme / Nike / Star Studios) and correctly stops to ask
#     (Bridgestone / Nordic Films). Without a client that HAS them, every
#     invoice row in the sheet stalls on "I need their billing details" and
#     the whole invoice flow goes untested.
FIXTURE_ROWS: List[Dict[str, Any]] = [
    # ── Acme Studios: 5 jobs, fully invoiced, mixed payment ──────────
    {"client_name": "Acme Studios", "job_date": _d(RECENT),     "fees": 25000, "paid": "Yes", "bill_sent": "Yes", "poc_email": "accounts@acme.test",  "poc_name": "Rhea Menon",  "job_description_details": "Master film 30s", "client_billing_details": "Acme Studios Pvt Ltd, 4 Linking Road, Mumbai 400050, GST: 27AAAAA0000A1Z5"},
    {"client_name": "Acme Studios", "job_date": _d(RECENT + 1), "fees": 18000, "paid": "Yes", "bill_sent": "Yes", "poc_email": "accounts@acme.test",  "poc_name": "Rhea Menon",  "job_description_details": "Cutdown 15s", "client_billing_details": "Acme Studios Pvt Ltd, 4 Linking Road, Mumbai 400050, GST: 27AAAAA0000A1Z5"},
    {"client_name": "Acme Studios", "job_date": _d(LAST_MONTH), "fees": 42000, "paid": "No",  "bill_sent": "Yes", "poc_email": "accounts@acme.test",  "poc_name": "Rhea Menon",  "job_description_details": "Brand campaign", "client_billing_details": "Acme Studios Pvt Ltd, 4 Linking Road, Mumbai 400050, GST: 27AAAAA0000A1Z5"},
    {"client_name": "Acme Studios", "job_date": _d(LAST_MONTH + 2), "fees": 12000, "paid": "No", "bill_sent": "Yes", "poc_email": "accounts@acme.test", "poc_name": "Rhea Menon", "job_description_details": "Social edit", "client_billing_details": "Acme Studios Pvt Ltd, 4 Linking Road, Mumbai 400050, GST: 27AAAAA0000A1Z5"},
    {"client_name": "Acme Studios", "job_date": _d(OLD),        "fees": 30000, "paid": "Yes", "bill_sent": "Yes", "poc_email": "accounts@acme.test",  "poc_name": "Rhea Menon",  "job_description_details": "Launch film", "client_billing_details": "Acme Studios Pvt Ltd, 4 Linking Road, Mumbai 400050, GST: 27AAAAA0000A1Z5"},

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

    # ── Sheet-vocabulary rows: Nike / Star Studios, dated in April ────
    #
    # The Intent_Test_Matrix asks about "Nike", "Star Studios" and "10
    # April" by name, because it was written against a real account. Against
    # the relative-date clients above, 41 of its 148 rows ask for data that
    # does not exist: the bot correctly answers "no such client", the judge
    # correctly calls that a miss, and the run measures the fixture instead
    # of the product. These rows make the sheet answerable.
    #
    # They are the ONE place absolute dates are allowed, and only because
    # the sheet hardcodes April. Everything above stays relative so that
    # "this month" / "last quarter" keep working whenever the suite runs.
    # _APRIL_YEAR keeps them in the past rather than silently becoming
    # future-dated when the suite runs in Jan–Apr.
    {"client_name": "Nike", "job_date": f"{_APRIL_YEAR}-04-10", "fees": 2500,  "paid": "Yes", "bill_sent": "Yes", "poc_email": "ap@nike.test",  "poc_name": "Karan Rao",   "job_description_details": "Shooting", "client_billing_details": "Nike India Pvt Ltd, 12 MG Road, Bengaluru 560001, GST: 29BBBBB1111B1Z5"},
    {"client_name": "Nike", "job_date": f"{_APRIL_YEAR}-04-18", "fees": 15000, "paid": "No",  "bill_sent": "Yes", "poc_email": "ap@nike.test",  "poc_name": "Karan Rao",   "job_description_details": "Dubbing", "client_billing_details": "Nike India Pvt Ltd, 12 MG Road, Bengaluru 560001, GST: 29BBBBB1111B1Z5"},
    {"client_name": "Nike", "job_date": f"{_APRIL_YEAR}-03-22", "fees": 25000, "paid": "No",  "bill_sent": "No",  "poc_email": "ap@nike.test",  "poc_name": "Karan Rao",   "job_description_details": "Brand film", "client_billing_details": "Nike India Pvt Ltd, 12 MG Road, Bengaluru 560001, GST: 29BBBBB1111B1Z5"},
    {"client_name": "Star Studios", "job_date": f"{_APRIL_YEAR}-04-12", "fees": 40000, "paid": "No", "bill_sent": "Yes", "poc_email": "rahul@starstudios.test", "poc_name": "Rahul Nair", "job_description_details": "Studio shoot", "client_billing_details": "Star Studios Pvt Ltd, 8 Andheri West, Mumbai 400053, GST: 27CCCCC2222C1Z5"},
    {"client_name": "Star Studios", "job_date": f"{_APRIL_YEAR}-05-02", "fees": 32000, "paid": "Yes", "bill_sent": "Yes", "poc_email": "rahul@starstudios.test", "poc_name": "Rahul Nair", "job_description_details": "Edit + grade", "client_billing_details": "Star Studios Pvt Ltd, 8 Andheri West, Mumbai 400053, GST: 27CCCCC2222C1Z5"},
]

# A COMPLETED profile. Load-bearing, not decoration: process_request() gates
# on `onboarded_at` and routes anything from a user without it into
# _start_onboarding, which answers every message with the welcome greeting.
# The first live run failed 21/29 for exactly this reason — every scenario
# got "👋 Hi, I'm Remyndly!" instead of an answer.
FIXTURE_PROFILE = {
    "platform":     "whatsapp",
    "name":         "E2E Tester",
    "company_name": "E2E Test Co",
}

# preferences.invoice_address gates invoice generation: without it, every
# invoice request stops at "what's your business address for the invoice
# header?" and returns ACTION_TRIGGER instead of a PDF. Eight sheet rows
# (64, 67, 72, 73, 81, 82, 86, 143) died on that prompt before this was
# seeded — the invoice flow was effectively untestable.
FIXTURE_PREFERENCES = {
    "invoice_address": "E2E Test Co\n21 Test Lane\nMumbai 400001",
}

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

def _connect(attempts: int = 3):
    """Open a psycopg2 connection to SUPABASE_DB_URL.

    Mirrors SupabaseService's own connection settings (short connect
    timeout + statement timeout) so a misconfigured URL fails fast in a
    test run instead of hanging the suite.

    Retries on transient failures. A full matrix run re-seeds before nearly
    every row — well over a hundred short-lived connections against a
    pooler — and a single "server closed the connection unexpectedly"
    killed a 122-row run outright. A connection blip is not a test result,
    so it should cost a second, not the whole run.
    """
    db_url = (os.getenv("SUPABASE_DB_URL") or "").strip()
    if not db_url:
        raise RuntimeError(
            "SUPABASE_DB_URL is not set — e2e seeding needs a real database. "
            "Gate the caller on tests.conftest.has_real_db_url()."
        )
    import time

    import psycopg2  # imported lazily: the offline suite stubs this module

    last = None
    for attempt in range(attempts):
        try:
            return psycopg2.connect(
                db_url, connect_timeout=5, options="-c statement_timeout=15000",
            )
        except Exception as e:            # OperationalError and friends
            last = e
            if attempt < attempts - 1:
                time.sleep(1.5 * (attempt + 1))
    raise last


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

    try:
        return _seed_once(uid)
    except Exception:
        # One retry on a fresh connection: a pooler can also drop the link
        # mid-transaction, which _connect's retry cannot see.
        return _seed_once(uid)


def _seed_once(uid: str) -> str:
    conn = _connect()
    try:
        with conn:
            with conn.cursor() as cur:
                # Clear first so an explicit reseed is deterministic.
                cur.execute("DELETE FROM public.job_entries WHERE user_id = %s", (uid,))
                cur.execute("DELETE FROM public.user_config WHERE user_id = %s", (uid,))

                # Onboarded profile. Without onboarded_at, process_request
                # routes EVERY message to the welcome greeting and no
                # scenario can pass — see FIXTURE_PROFILE.
                cur.execute(
                    """
                    INSERT INTO public.user_profiles
                        (user_id, platform, name, company_name, onboarded_at, preferences)
                    VALUES (%s, %s, %s, %s, now(), %s::jsonb)
                    ON CONFLICT (user_id) DO UPDATE SET
                        platform     = EXCLUDED.platform,
                        name         = EXCLUDED.name,
                        company_name = EXCLUDED.company_name,
                        onboarded_at = EXCLUDED.onboarded_at,
                        preferences  = EXCLUDED.preferences
                    """,
                    (
                        uid, FIXTURE_PROFILE["platform"], FIXTURE_PROFILE["name"],
                        FIXTURE_PROFILE["company_name"],
                        json.dumps(FIXTURE_PREFERENCES),
                    ),
                )

                for row in FIXTURE_ROWS:
                    cur.execute(
                        """
                        INSERT INTO public.job_entries
                            (user_id, client_name, job_date, fees, paid,
                             bill_sent, poc_email, poc_name,
                             job_description_details, client_billing_details)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """,
                        (
                            uid, row["client_name"], row["job_date"], row["fees"],
                            row["paid"], row["bill_sent"], row["poc_email"],
                            row["poc_name"], row["job_description_details"],
                            row.get("client_billing_details"),
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
