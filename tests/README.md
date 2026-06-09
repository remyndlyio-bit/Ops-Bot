# Remyndly Test Suite — the Product Baseline

**~348 tests across 7 files. CI fails the build on any unexpected failure.**

This file is the contract: every test below is a behaviour the product
guarantees. Adding a feature is fine; breaking one of these is not.

Run locally:

    python -m pytest tests/ -v

Run a specific category:

    python -m pytest tests/test_planner_boundary.py -v
    python -m pytest tests/test_reminder_worker.py -v

CI runs everything on every push and PR via `.github/workflows/tests.yml`.

---

## How to add a test

When you patch a production bug, ADD A TEST for it. The pattern:

1. Reproduce locally (real input → wrong output).
2. Add a test that fails because of the bug.
3. Ship the fix.
4. Verify the test passes.
5. Commit both together.

If you skip step 2-4 the bug WILL come back. Most of the patches in this
repo's history are evidence of that. The test suite below exists to break
that pattern.

---

## What's covered (237 tests, organised by file)

### `tests/test_planner_boundary.py` — 44 tests
The architectural contract for the AI → SQL boundary. Every past
production bug in column handling lives here as a permanent regression.

- **TestBillSent (9)** — Hinglish "kiska invoice baki", list/null/string
  shapes, sent-requires-POC, never ILIKE.
- **TestPaid (6)** — Unpaid includes NULLs, mirror of bill_sent.
- **TestDateColumns (17)** — invoice_date / job_date etc. NEVER ILIKE'd.
- **TestPocEmail (3)** — NULL / IS NOT NULL / real-email matches.
- **TestGenericShapes (4)** — Lists → IN, numeric equality, text ILIKE.
- **TestV2VerdictBeatsLegacyInvoiceCheck (3)** — Confident READ_QUERY
  short-circuits legacy invoice keyword check.
- **TestColumnRegistry (5)** — Registry loads; prompts compose; unknown
  columns fall back safely.

### `tests/test_scenarios_from_matrix.py` — 48 tests
Derived from the Intent Test Matrix Excel.

- **TestFeeParsing (21)** — 25k, 1.5L, 25 hazaar, 1.5 lakh, 1cr,
  Rs.25000, ₹25,000 etc.
- **TestEmailValidation (12)** — Valid + invalid email patterns.
- **TestSqlInjectionSafety (1)** — Quote escaping in filter values.
- **TestClientListFilter (1)** — Multi-client SQL IN-clause.
- **TestWhatsAppExportPicker (4)** — PDF preferred over CSV/xlsx for
  WhatsApp delivery. CSV when no PDF. xlsx as last resort.
- **TestExcelExport (1)** — Generator writes .xlsx + .csv + .pdf
  siblings. PDF survives long values + Unicode (em-dash, '…', etc.).
- **TestNonStandardInputs (13)** — NULL / "IS NOT NULL" / junk-text
  values never produce ILIKE on date columns.

### `tests/test_user_queries.py` — 26 tests

- **TestResolveResponseMode (7)** — Correct response shape per query.
- **TestBuildFilterContext (4)** — Filter-context strings for replies.
- **TestFormatSqlResult (6)** — Empty rows; single / multi row; truncation
  at 20; nulls as N/A.
- **TestIsFollowupFieldRequest (5)** — "what about", "how much",
  short follow-ups detected; long unrelated NOT.
- **TestReconstructMessage (4)** — "April" reply joins with pending
  client context.
- **TestFormatUscfResult (4)** — Count/sum/error formatting.

### `tests/test_edge_cases.py` — 40 tests

- **TestSQLInjection (4)** — DROP/DELETE/semicolons blocked.
- **TestNoData (3)** — Zero rows → friendly; keyword fallback.
- **TestTypoInCommand (5)** — genrate, invoce, invoise detected.
- **TestDeleteLastJob (3)** — Soft delete only; hard DELETE blocked.
- **TestMultiRowDisambiguation (1)** — Numbered list shown.
- **TestDisambiguationReply (3)** — Reply "2" hits correct row; bounds
  checked; non-numeric prompts.
- **TestCancelDisambiguation (4)** — Cancel clears state; no SQL run.
- **TestStaleCachedInvoice (3)** — 30-min cache boundary.
- **TestContextPronounResolution (3)** — "them" / "this client" /
  month reply with context.
- **TestContextThisMonth (2)** — "this month" / "last month" join.
- **TestShortFollowUp (5)** — "Fees?" detected; aggregates not mistaken.
- **TestHindiWithEnglishNames (4)** — Hinglish invoice routing.

### `tests/test_invoice_flow.py` — 25 tests

- **TestSanitizePdfText (6)** — ₹ → Rs; em/en dashes; smart quotes;
  None → "".
- **TestParseFees (7)** — Plain / rupee / commas / float / empty / invalid.
- **TestGeneratePdf (4)** — PDF created; filename includes client+month;
  bank-details placeholders.
- **TestNormalizeEmails (6)** — Single, semicolon, comma, list, empty, None.
- **TestSendEmailDryRun (3)** — Dry-run skips HTTP; reminder dry; missing
  recipient → False.
- **TestSendEmailLive (3)** — 2xx → True; 4xx → False; network exception
  → False.
- **TestSendInvoiceEmail (3)** — False when PDF missing; subject has
  client+month.

### `tests/test_plan_model.py` — ~70 tests (Path 3)
The architectural contract that eliminates the "AI emitted a shape we
didn't anticipate" bug class. Every semantic concept (NULL, NOT NULL,
"sent", "not sent", "paid", "unpaid") has ALL plausible variants
enumerated and asserted to collapse to the SAME `CanonicalFilter`.

- **TestGenericNullNormalisation (~30)** — Every whitespace / case /
  underscore variant of NULL and NOT NULL collapses identically.
- **TestBillSentNormalisation (~22)** — Truthy / falsy variants;
  list shapes; `not_null` underscore.
- **TestPaidNormalisation (~13)** — Mirror of bill_sent.
- **TestPocEmailNormalisation (~12)** — NullCheck / TextMatch.
- **TestDateColumnNormalisation (~9)** — Equality / Comparison /
  NullCheck only — junk values return None (never ILIKE).
- **TestGenericFallback (4)** — Numeric / list / text / operator.
- **TestPlanFromRaw (6)** — End-to-end validation contract.
- **TestCrossVariantConsistency (4)** — Set-equality lemmas: every
  variant of the same concept collapses to ONE canonical form.

See `PATH_3.md` at the repo root for the architecture write-up and
Phase 3b roadmap.

### `tests/test_reminder_worker.py` — 34 tests

- **TestDetermineReminderLevel (4)** — First/second/third escalation.
- **TestFormatAmount (6)** — ₹X,XXX for ints/floats/strings/None/invalid.
- **TestIsTelegramUser (4)** — Numeric ID → Telegram; "whatsapp:" / "+"
  → WhatsApp.
- **TestBuildReminderText (4)** — Client+bill# in text; numbered; "Final"
  label for third tier.
- **TestGroupByUser (3)** — Groups by user_id; attaches reminder_level.
- **TestScanReminders (2)** — Rows on success; empty on DB failure.
- **TestNotifyUserTelegram (2)** — Sends with buttons; Send All + Skip All
  buttons present.
- **TestNotifyUserWhatsapp (3)** — Sends text; no markdown asterisks;
  saves pending state.
- **TestMarkRemindersSent (4)** — Stamps right flag per tier; skips
  invalid rows.
- **TestRun (3)** — Exits early on empty; Telegram path; WhatsApp path.

---

## Known gaps (honest)

- **No live Twilio integration test.** We test which file the picker
  chooses, not that Twilio actually delivers it. Twilio rejection codes
  surface via `[WHATSAPP_DEFERRED_FAILURE]` log lines in production.
- **No real Gemini call test.** AI classifier behaviour is mocked.
  Adding scenario-level tests with a mocked Gemini is future work.
- **No browser test for the invoice PDF in WhatsApp viewer.** We assert
  the PDF magic bytes and content; whether the user's device renders
  it nicely is observed manually.

These gaps are real but every one of them is a known-known. Production
logs surface failures within seconds via the deferred status checker.
