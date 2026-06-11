# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally (http://localhost:8080)
python main.py

# Health check
curl http://localhost:8080/health

# Validate AI key setup
python test_ai_key.py

# Run tests (352 tests across 8 files)
python -m pytest tests/ -v

# Run a specific test file
python -m pytest tests/test_planner_boundary.py -v
python -m pytest tests/test_plan_model.py -v
python -m pytest tests/test_plan_retry.py -v

# Run reminder worker standalone (normally scheduled via Railway cron)
python workers/reminder_worker.py

# Load Excel data to Supabase (creates job_entries table)
python scripts/load_excel_to_supabase.py path/to/data.xlsx
```

CI runs `pytest tests/` on every push and PR via `.github/workflows/tests.yml`.

## Architecture

**Remyndly (Ops Bot)** is a FastAPI backend for a multi-platform conversational bot (Telegram + WhatsApp via Twilio). Users interact via chat to manage job entries, generate invoices, and track payments. Data lives in Supabase Postgres.

### Request Flow

```
POST /webhooks/{whatsapp,telegram}
  → _handle_bot_message(user_id, message, platform)   [main.py]
    → IntentService.process_request()                  [services/intent_service.py]
      → GeminiService (OpenRouter/Gemini 2.5 Flash)   [services/gemini_service.py]
      → SupabaseService (DB read/write)               [services/supabase_service.py]
      → InvoiceGenerationService (PDF via fpdf2)      [services/invoice_generation_service.py]
      → ResendEmailService / WhatsAppService / TelegramService
```

### Query Pipeline (the most complex path)

```
User message
  → classify_operation()              [services/query_planner.py]  — keyword + LLM fallback
  → build_operation_plan()            [services/query_planner.py]  — LLM emits structured JSON plan
  → Plan.from_raw() validation        [services/plan.py]           — Path 3 canonical filter check
    → on failure: retry LLM once with feedback, then clarification
  → resolve_rows()                    [services/query_planner.py]  — context resolution for updates
  → validate_plan_columns()           [services/query_planner.py]  — schema check
  → plan_to_sql()                     [services/query_planner.py]  — deterministic SQL generation
    → _build_filter_clause()          — consults column registry first, generic builder fallback
  → execute SQL on Supabase
  → synthesize response               [services/gemini_service.py] — LLM converts result to natural language
```

### Path 3 — Typed Plan + Canonical Filter Normalisation

**The architectural fix for the recurring "AI emitted a shape we didn't anticipate → wrong SQL → wrong answer" bug class.** See `PATH_3.md` for the full write-up.

Key components:
- **`services/plan.py`** — `CanonicalFilter` hierarchy (`NullCheck`, `BoolCheck`, `Equality`, `InList`, `Comparison`, `TextMatch`), centralised generic normaliser, `Plan.from_raw()` validator, `PlanResult.feedback_for_retry()`
- **`services/columns/`** — Column registry. Each column has ONE file with semantic docs, prompt fragment, SQL filter handler, and canonical normaliser. Adding a new column = add a module here.
  - `bill_sent.py` — invoice email delivery status (BoolCheck)
  - `paid.py` — payment status (BoolCheck)
  - `poc_email.py` — client contact email (NullCheck / TextMatch)
  - `date_columns.py` — all date/timestamp columns (NullCheck / Equality / Comparison, NEVER TextMatch)
- **Strict validation** — `STRICT_PLAN_VALIDATION=1` (default). On failure: retry LLM once with typed feedback. Second failure → friendly clarification to user. Escape hatch: `STRICT_PLAN_VALIDATION=0` reverts to log-only shadow mode.

### Intent Routing (intent_service.py)

The main `process_request()` method classifies user input into these flows:
- **form_step**: Multi-step job entry (state stored in MemoryService per user)
- **smart_capture**: NL field extraction to add a job in one message
- **invoice_request**: Generate PDF invoice, email it, update `invoice_date` in DB
- **query**: NL→SQL via the query pipeline above
- **reminder_handling**: Payment escalation prompts (15/30/45-day tiers)
- **bank_details**: CRUD on `user_config` table
- **account_linking**: Cross-platform user linking via `user_profiles.preferences.linked_user_id`

### Database (Supabase Postgres, `supabase/schema.sql`)

Three tables:
- **`job_entries`** – core data: jobs, fees, invoice state, reminder timestamps (`first_reminder_sent`, `second_reminder_sent`, `third_reminder_sent`), `user_id`, `bill_sent`, `bill_sent_at`, `paid`
- **`user_config`** – bank details per user (account, IFSC, UPI)
- **`user_profiles`** – onboarding state, platform, cross-platform linking via `preferences` JSONB

`user_id` is either a Telegram `chat_id` or WhatsApp phone number. Cross-platform linking stores `linked_user_id` in `preferences` so both platforms can query the same `job_entries` rows.

### AI Layer (gemini_service.py)

All AI calls go through OpenRouter to Gemini 2.5 Flash (`AI_KEY` env var). Used for:
- Intent classification
- NL→SQL plan generation (with schema context from `COLUMN_SCHEMA` env var)
- Field extraction from free-form messages
- Response synthesis (includes AGGREGATE ANSWERS rule — never refuse when the DB returned a number)

### Background Workers

`workers/reminder_worker.py` scans `job_entries` for unpaid invoices past 15/30/45-day thresholds and sends Telegram inline-button messages or WhatsApp text. It runs as a Railway cron job.

### Platform Differences

- **Telegram**: inline buttons (payment reminder actions)
- **WhatsApp (Twilio)**: text + PDF attachments. Media delivery: PDF works reliably, xlsx/csv rejected by Twilio (63019/63005). Deferred status polling at 30s with `[WHATSAPP_DEFERRED_FAILURE]` log.
- **PDF generation**: fpdf2 with Helvetica (Latin-1 only). Unicode chars (₹, em-dash, smart quotes, ellipsis) are normalised to ASCII before PDF rendering.

### Environment Variables

See `.env.example`. Required: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_NUMBER`, `BASE_URL`, `TELEGRAM_BOT_TOKEN`, `AI_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_DB_URL` (use pooler connection, port 6543). Optional: `RESEND_API`, `RESEND_FROM_EMAIL`, `COLUMN_SCHEMA`, `STRICT_PLAN_VALIDATION` (default `1`).

### Deployment

Railway with `Procfile`: `web: uvicorn main:app --host 0.0.0.0 --port $PORT`. Python version is managed by Railpack's default (do NOT add `.python-version` — it triggers `mise` downloads that 404).

## Test Suite (352 tests, 8 files)

See `tests/README.md` for the full inventory. Key files:

| File | Tests | What it covers |
|---|---|---|
| `test_planner_boundary.py` | ~46 | AI→SQL boundary regressions (column registry, filter shapes, date ILIKE prevention) |
| `test_plan_model.py` | ~70 | Path 3 canonical filter normalisation (property-based: every variant of NULL/NOT NULL/truthy/falsy collapses to the same canonical form) |
| `test_plan_retry.py` | 4 | Strict-mode retry contract (invalid→retry→valid, exhaustion→clarification, escape hatch, no-retry-when-valid) |
| `test_scenarios_from_matrix.py` | ~48 | Fee parsing (k/L/lakh/crore), email validation, SQL injection, WhatsApp export picker |
| `test_user_queries.py` | 26 | Response modes, filter context, follow-ups, result formatting |
| `test_edge_cases.py` | 40 | SQL injection blocking, typos, disambiguation, context resolution, Hinglish routing |
| `test_invoice_flow.py` | 25 | PDF sanitisation, fee parsing, email send dry/live, invoice email flow |
| `test_reminder_worker.py` | 34 | Reminder tiers, grouping, Telegram/WhatsApp dispatch, mark-sent |

## Known Bugs (from live WhatsApp testing, June 2026)

These were found by sending 29 messages to the live WhatsApp bot. Fix in this priority order:

### Bug 1 (HIGH): Planner refuses valid aggregate queries
- **Messages that fail**: "Who is my biggest client?", "Average fees per job", "How much does Star Studios owe me?", "Star Studios se paisa aaya kya?"
- **Root cause**: The planner LLM won't emit GROUP BY, AVG, or compound filter plans despite structural support in the SQL builder.
- **Fix**: Add explicit examples in `_build_planner_prompt()` in `services/query_planner.py` for "biggest/top client" → group_by + sum + desc + limit 1, "average" → metric avg, "owe me" → client filter + paid=no + sum.

### Bug 2 (HIGH): Unfiltered COUNT synth crash
- **Messages that fail**: "How many jobs have I done?", "How many total jobs do I have?"
- **Response**: "I found matching records but couldn't format the reply."
- **Root cause**: Planner emits metric=null instead of metric=count for unfiltered "how many" → SELECT * runs → synthesis chokes on 25 raw rows instead of a single count.
- **Fix**: Grep for "couldn't format the reply" to find the failing code path. Ensure "how many" without qualifiers maps to metric=count.

### Bug 3 (MEDIUM): "Earnings" defaults to list instead of SUM
- **Message**: "Earnings last quarter" → lists 22 rows instead of a total.
- **Note**: "Total billing this year" → ✅ works because "total" triggers SUM.
- **Fix**: In planner prompt, when phrasing is value-oriented ("earnings", "kamai", "billing", "revenue") without "list/show", default metric to "sum".

### Bug 4 (MEDIUM): Smart-capture misses "paid" keyword
- **Message**: "Add a job for Acme, 25k, shoot, paid" → extracts brand/details/fees but ignores "paid".
- **Fix**: Update smart_capture extraction prompt to recognise "paid/unpaid" as a field value. Make POC fields optional. Default job_date to today.

### Bug 5 (LOW): Zero-result aggregate phrasing
- **Message**: "What about this month?" (after earnings query, when 0 jobs) → "No matching records — total is 0" reads like an error.
- **Fix**: When aggregate result=0, phrase as "₹0 for [period]" not "No matching records."

## WhatsApp Production Test Suite (29 messages)

Run these on live WhatsApp to verify the product works end-to-end. Target: 29/29 pass.

| # | Message | Category | Expected |
|---|---|---|---|
| 1 | Add a job for Acme, 25k, shoot, paid | Smart capture | Row created with fees=25000, paid=yes |
| 2 | Show my last 5 jobs | Basic query | 5 results + PDF |
| 3 | List all unpaid invoices | Basic query | Results + PDF |
| 4 | Who is my biggest client? | Grouped aggregate | Client name + total amount |
| 5 | How many invoices have I sent? | Count + filter | A number (e.g. "10 invoices") |
| 6 | Total billing this year | Sum + date range | ₹ amount |
| 7 | Average fees per job | Average | ₹ amount |
| 8 | Isme se invoice kitne logon ko bheja hai | Hinglish count | A number (e.g. "10 clients") |
| 9 | Kiska payment baki hai | Hinglish unpaid | Results + PDF |
| 10 | Pichle mahine ki total kamai kitni thi | Hinglish date + sum | ₹ amount or "₹0 for [month]" |
| 11 | Jobs in Q1 this year | Date range | Results + PDF |
| 12 | Earnings last quarter | Date range sum | ₹ total (NOT a row list) |
| 13 | What about this month? | Context follow-up | Inherits previous query's intent |
| 14 | Show jobs from around then | Path 3 clarification | "Could you specify the date?" |
| 15 | How many invoices sent to clients with no email | Multi-filter count | A number (should be 0) |
| 16 | Show my bank details | user_config read | Shows stored bank details |
| 17 | genrate invoce for Acme | Typo detection | Recognised as invoice request |
| 18 | Can you book me an Uber? | Out-of-scope | Friendly on-brand refusal |
| 19 | Show Samsung jobs | Client filter | Matching rows |
| 20 | Mark this as paid | Context update | Updates row from #19 |
| 21 | Show Pedigree and Garnier jobs | Multi-client | Both brands returned |
| 22 | What did I do last week? | Natural date | Results or "no jobs for [dates]" |
| 23 | Kiska invoice bhejna baki hai | Hinglish pending | Unsent invoices + PDF |
| 24 | How much does Star Studios owe me? | Client + unpaid sum | ₹ amount |
| 25 | Total fees for Star Studios | Client sum | ₹ amount |
| 26 | Star Studios se paisa aaya kya? | Hinglish paid check | Payment status for that client |
| 27 | Show me all my jobs | Full list | All results + PDF |
| 28 | How many jobs have I done? | Simple count | A number |
| 29 | How many total jobs do I have? | Simple count | Same number as #28 |

### Last run results (June 2026): 16 ✅, 7 ⚠️, 6 ❌

## Working Style

- **Always try the simplest fix first.** Remove/tweak config before adding new files. One-line fix > new config file > new tool (e.g. Dockerfile). Don't introduce new infrastructure (Docker, CI configs, build tools) unless the simple path is truly exhausted.
- **Don't jump to workarounds.** Diagnose the root cause fully before proposing a fix. If the first attempt fails, re-examine assumptions rather than escalating to a heavier solution.
- **Keep changes minimal.** Touch the fewest files possible. Prefer deleting over adding.
- **Commit directly to main.** No PRs or feature branches — merge straight into main and push. Commit messages start with `C C : ...`.
- **Every bug fix ships with a regression test.** See `tests/README.md` for the protocol.
- **When adding a new column handler**, add a module under `services/columns/` with semantic docs, prompt fragment, filter handler, and canonical normaliser. Add tests in `tests/test_planner_boundary.py` and `tests/test_plan_model.py`.
