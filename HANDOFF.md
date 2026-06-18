# Session Handoff — 2026-06-17 / 18

Big session. Three arcs: (1) a regression-fix sprint off a 156-row WhatsApp test
sheet, (2) a deterministic query router that takes common queries off the LLM
planner, and (3) a full invoice-generation overhaul plus two infra fixes
(DB-backed memory, reminder-hijack). All 16 commits are on `main` and
**auto-deploy to Railway on push** (see "Deploy" below — this is NOT midnight-only).

Test suite: **457 passing** (was ~280 at session start). New: `tests/test_query_router.py`,
`tests/test_memory_service.py`, and many additions to `test_edge_cases.py` /
`test_invoice_flow.py`.

---

## Commits shipped (newest first)

| Commit | What |
|---|---|
| `2a30608` | "update my address" command — set/correct saved business address any time |
| `02b2d2c` | **DB-backed MemoryService** — conversation/awaiting state → Supabase (survives redeploys + multi-instance) |
| `76b19da` | **Mandatory-fields gate** for invoices — prompt for every required field, then generate |
| `8756fef` | Close address-check gap on the bank-resume path |
| `1eb838a` | **Hard guard** — never generate a bankless (unpayable) invoice |
| `b61cab7` | Invoice PDF + generation: 7 client-feedback fixes |
| `627d2c0` | Stop a stale pending reminder hijacking numeric / mid-flow messages |
| `aecfb2c` | Don't rewrite aggregate/GROUP BY SQL to `SELECT *` for history questions |
| `d66b5b8` | Keep POC email example in add-job prompt, drop "(optional)" label |
| `1870f61` | Remove email from add-job prompt + warmer synthesizer tone |
| `47f7ee8` | Two router bugs from re-testing the sheet (output/ dir crash; scope-qualifier) |
| `19050e2` | Fix two incorrect E2E assertions (C9-01, C10-01) → suite effectively 33/33 |
| `c661622` | Regression FAILs 37-45 (delete-last, pronoun resolution, declines, OOS) |
| `9bf05a5` | Fix 36 regression FAILs + wire the query router into the pipeline |
| `3a0e51a` | Add the deterministic-first query router (`services/query_router.py`) |
| `1ee2ed9` | Harden E2E suite with `expected_db_rows` SQL-level validation |

---

## 1. Deterministic query router — `services/query_router.py` (NEW)

**The architectural shift this session.** `route_common_query(message, user_id)` is a
PURE function (no DB/LLM) mapping the ~20 common query shapes straight to SQL,
run BEFORE the planner in `intent_service`. The planner is now the **fallback**
for the long tail.

- Routes: count_jobs, total_fees, average_fees, list_jobs, last_job, unpaid_list,
  list_clients, biggest_client, earnings_by_client, top_bottom_job,
  payment_status ("has X paid"), client_owes, clients_paid_list, date_lookup,
  hinglish_earnings.
- Wired in `intent_service._execute_routed_query`; `_keyword_sql_fallback` now
  delegates to the same router (single source of truth).
- `_has_scope_qualifier()` guard: the unfiltered routes (total/count/avg/list)
  **defer to the planner** when a date period or specific client is present —
  otherwise "Total billing for Nike" returned the grand total and "how many jobs
  this quarter" ignored the date (both real bugs caught by re-testing).
- Tests: `tests/test_query_router.py` (34 pure tests, no mocks).

Why it exists: the planner (LLM → JSON plan → deterministic SQL) is fragile for
simple queries. e.g. "highest paying job" came back sorted by **date** not fees,
because the planner left `column` null. For unambiguous shapes we encode the
mapping as code.

---

## 2. Regression sprint — 45 FAILs off the WhatsApp sheet

Source: `~/Downloads/regression_results_20260611_202734 copy.xlsx` (156 rows,
audited; a **"Fix Tracker"** tab tracks all 45). Result: **36 FIXED, 9 PARTIAL**.

- The 9 PARTIAL (#14, 21, 22, 27, 30, 31, 32, 36, 43) were transient live-API
  blips / cascades; code verified, **not independently reproduced** → they need a
  live re-test to close.
- Notable fixes: CRITICAL "Yes deletes a job" (a bare "yes" in a numbered
  disambiguation was treated as delete-all), bank-parser silently dropping
  account number/holder, highest-paying-job sorting by date, Hinglish routing,
  client/paid lists, "Has X paid", delete-last treating "last" as a client,
  pronoun resolution ("invoice for them"), out-of-scope refusals.

**Live verification:** E2E suite **33/33** (after fixing 2 bad assertions). A
retest harness (`tests/retest_sheet.py`, hardcoded path to the Downloads sheet —
left untracked) replayed the Query category live → **37/37**, and *that* run
surfaced the two router bugs in `47f7ee8`.

---

## 3. Invoice overhaul

### PDF rendering — `services/invoice_generation_service.py` (`b61cab7`)
Seven issues from a client-annotated invoice:
1. Stray "Billing infor is" label → `_strip_billing_label()` (capture + render).
2. Sender address missing → now rendered when on file (see gate below for capture).
3. Job line was `client|brand|poc|fees|date|bill` dump with no description →
   rebuilt as a **Description | Date | Amount** table.
4. Bank section blank → main.py fetched bank/profile by raw login id while the
   pre-check used the resolved (linked) `data_user_id`; now both resolve the link.
5. "Payment Terms: Immediate" contradicted the 30-day T&C → "Within 30 days".
6. Always-"NA" GST / Job No. rows → omitted unless real.
7. Brand printed twice → removed from the job line.

### Mandatory-fields gate (`76b19da`) — the big behavior change
`intent_service._invoice_readiness_check(user_id, data_user_id, invoice_data, rows)`
is the single ordered gate. It returns a prompt for the FIRST missing field and
arms the matching awaiting-state; the field's handler saves it and **re-enters
the flow** (`_resume_invoice_flow`), so prompts chain until complete, then it
generates. Runs for BOTH generate and email paths.

**Mandatory** (confirmed with user): client billing details, POC name, a
description per job, bank account number, business address. **Optional**: GST,
POC email. The old per-field "skip → generate anyway" shortcuts are GONE;
`cancel` aborts. New `_handle_job_description_response` + routing.

### Hard guard (`1eb838a`)
`has_usable_bank_details()` (account number must be non-empty) is enforced at the
generation point in `main.py` — even if the gate is bypassed, it aborts + tells
the user instead of emitting a bankless PDF.

### Update address command (`2a30608`)
"update my address [to X]" / "my business address is X" / "wrong address" →
`_handle_address_update` → `_persist_invoice_address` (shared with the gate).

---

## 4. Infra fixes

### DB-backed MemoryService (`02b2d2c`) — important
Per-user state (awaiting_* flags, pending_invoice, conversation, form) was a
**per-instance local JSON file** (`user_memory.json`). On Railway a redeploy = fresh
disk, and multiple instances each have their own file → an in-flight
`awaiting_invoice_address` set while prompting could vanish before the reply
arrived, **orphaning the reply** (it then fell into `answer_feature_question`,
which leaked its "USER ASKED:/Your reply:" template + hallucinated a question).

Now backed by **`public.user_memory`** (jsonb per user, auto-created on init),
file kept only as a dev fallback when `SUPABASE_DB_URL` is unset. Same public
API; one reused, lock-guarded connection. Tests: `tests/test_memory_service.py`
(incl. the redeploy/cross-instance case). Startup log to confirm it's live:
`[MEMORY] Using Supabase-backed user memory`.

### Reminder hijack (`627d2c0`)
A stale `pending_reminders` row (persistent, no TTL) intercepted any numeric
message: "add a job … 5 May 2025, 20k" was read as reminder selection #5.
`_handle_pending_reminder` now (a) yields when any sub-flow is active and (b)
requires a STANDALONE number (`re.fullmatch`), not a digit buried in free text.

### History-rewrite vs aggregates (`aecfb2c`)
The "history question → `SELECT *`" rewrite destroyed `SELECT ..., AVG(fees) AS
result ... ORDER BY result` → Postgres `column "result" does not exist`.
`_is_aggregate_sql()` now skips the rewrite for GROUP BY / aggregate SQL.

---

## Open items / known gaps

1. **9 PARTIAL regression rows** — need a live re-test to confirm/close.
2. **`answer_feature_question` prompt leak (Bug #2, NOT done)** — it can still echo
   "USER ASKED:/Your reply:" and hallucinate. `02b2d2c` removes the main trigger
   (orphaned replies) but the leak itself is unhardened. Prompt at
   `gemini_service.py:~252/308/936`.
3. **UPI "(optional)" not stripped** — users copy the prompt's example
   "UPI: you@upi (optional)" verbatim and "(optional)" gets saved as the UPI. The
   bank parser should strip a trailing "(optional)".
4. **Invoice cache** — `process_and_send_invoice` reuses a cached PDF unless the
   message has a "regenerate" keyword. After fixing data (address/poc), users must
   say "regenerate invoice for X" to rebuild. Consider auto-invalidating a cached
   PDF that predates having bank/address on file.
5. **`direct SQL > planner` for complex queries** — user observed the
   `generate_sql` fallback often beats the planner on long-tail queries (the
   planner's JSON breaks). Option on the table: try direct SQL before the planner.
6. Pre-existing AVG-synthesizer-refusal latent bug (from prior handoff) —
   largely mitigated by synthesizer-prompt rules added this session, not formally
   re-verified.

---

## Deploy & ops

- **Railway auto-deploys on every push to `main`** — confirmed via the Railway
  API (all of today's commits deployed within minutes on `web-production-02c14`).
  The "we deploy at midnight" assumption is NOT how it's currently configured; if
  midnight-only is wanted, disable auto-deploy on the prod environment in Railway.
- Logs: `RAILWAY_API_TOKEN=… ./scripts/railway_logs.sh [limit]` (pulls the latest
  SUCCESS deployment's logs only — a redeploy resets what's visible).
- **Rotate secrets shared in chat this session**: the GitHub PAT used for pushes
  (`ghp_…`), the two OpenRouter AI keys (`sk-or-v1-…`), and consider the Railway
  token. These are in the conversation history.

## Test / run
```
python3 -m pytest tests/ -q                       # 457 passing
AI_KEY=sk-or-... python3 tests/test_e2e_live.py   # live, 33/33, ~$0.25–0.50/run
```
Note: the prior "4 pre-existing PDF failures" were just **fpdf2 missing locally** —
with it installed they pass; CI/prod always had it.

---

## Next session priorities

1. Live re-test the 9 PARTIAL regression rows; update the Fix Tracker.
2. Harden `answer_feature_question` so it can never leak its template (Bug #2).
3. Strip "(optional)" (and similar) from captured bank/UPI values.
4. Decide on direct-SQL-before-planner for complex queries (user's observation).
5. Optional: auto-invalidate cached invoices that predate bank/address on file.
