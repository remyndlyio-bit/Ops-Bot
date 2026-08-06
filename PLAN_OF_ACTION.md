# PLAN_OF_ACTION.md — Remyndly Production-Readiness Roadmap

*Produced 2026-08-06 from: full repo analysis (45k LOC), the 148-scenario Intent Test Matrix
live run (80 PASS / 46 FAIL / 2 UNCLEAR / 20 MANUAL = 62.5% of graded), per-turn telemetry
from 649 live turns, and industry-pattern research. No code was changed for this document.*

---

## 1. Executive Summary

The bot's core machinery is sound: the deterministic query router, Path 3 typed-plan
validation, the column registry, and the answer ledger are genuinely good engineering, and
the categories they own score well (Payments 90%, Earnings 81%, Small Talk 100%). The
product fails not because the AI is bad at SQL — it fails in the **plumbing around the AI**:

1. **Two routing brains coexist, and neither fully owns the product.** The FlowMachine v2
   migration is half-finished. With `FLOW_MACHINE_V2=true`, write intents
   (`WRITE_CREATE`, `WRITE_INVOICE`, …) are classified correctly and then **dropped** —
   `dispatch_idle()` treats them as shadow-only, while the legacy keyword handlers for the
   same intents are gated OFF (`if not _flow_machine_v2_enabled_for(...)`). There are intent
   classes with **no owner at all** under the production-default flag. Worse: the e2e suite
   that produced the 62.5% figure ran with v2 **off** (zero `V2_DISPATCH` lines in any run
   log), so the graded behavior is not the deployed behavior.

2. **Conversation state is scattered and leaks between turns.** Pending state lives in at
   least six places (`pending_disambiguation`, form state, FlowMachine, `awaiting_*`
   remnants, `last_generated_invoice`, `uscf_context`) with different owners and TTLs. In
   the live run, one stale delete-disambiguation swallowed **five consecutive turns** of the
   invoice-email flow (rows 87, 90, 91, 92, 97 all got "reply with a number, 'all' to
   delete every match, or 'cancel'").

3. **The fallback for "I don't understand" is a full table dump.** Unrecognized input —
   Hindi job entries, a link-ID reply, "send all", even an SQL-injection string — falls
   through to the planner, which builds an unfiltered `SELECT *` and replies "Found 20
   results — here's a spreadsheet." This is the single worst trust-destroying behavior in
   the product.

4. **It is slow.** Measured across 649 live turns: **p50 = 6.9s, p90 = 12.1s, p95 = 15.5s,
   max = 26.3s**. 200 turns made 3+ sequential LLM calls. Every `SupabaseService` call opens
   a fresh TCP+TLS connection to the pooler. And the WhatsApp webhook holds Twilio's HTTP
   request open for the whole turn — Twilio times out at 15 seconds, so every p95 turn
   risks a webhook retry and duplicate processing.

**The 62.5% number is better than it looks.** Roughly 10 of the 46 fails are harness/corpus
artifacts (stale sheet dates, fixture-precondition mismatches, onboarding spec drift, judge
noise), not product bugs. The remaining ~36 fails collapse into the four root-cause families
above plus invoice-entry parsing. Fixing the eight root causes below addresses essentially
every real failure without a rewrite.

---

## 2. Root Causes (each mapped to observed failures)

### RC-1 — Half-finished v2 migration: intents with no owner
`services/flow_dispatcher.py:49` declares `WRITE_CREATE / WRITE_UPDATE / WRITE_DELETE /
WRITE_INVOICE` shadow-only. Meanwhile `services/intent_service.py:4479` (add-job triggers),
`:4402` (small talk), `:4455` (modify), and the whole invoice keyword branch (`:4890` area,
`is_retrieval = False` when v2 on) are gated on v2 being OFF. So under v2, a classified
`WRITE_CREATE` falls through the entire cascade into `parse_user_intent`/the query pipeline.
Under legacy, routing depends on English keyword lists. Either path has holes; both paths
must be maintained; only one was tested.
- **Failures**: #9, #10, #20 (job one-liners → "Describe the job" instead of extraction),
  #19 (Hindi job entry → spreadsheet dump), #81/#143 (Hindi invoice → READ query), #21
  (compound add+invoice loses the second half).

### RC-2 — Scattered pending-state with no exclusivity invariant
Six state stores, each armed and cleared by different code. Nothing enforces "at most one
pending interaction per user," and pending handlers do not validate that the incoming reply
is plausibly *for them* before consuming it.
- **Failures**: #87, #90, #91, #92, #97 (stale delete-disambiguation hijacks the whole
  email flow), #75 ("skip" after billing prompt → job list), #110 (link-ID digits →
  spreadsheet), #116–#118 (reminder replies "send all"/"1"/"skip" mis-claimed), #141
  ("This month" follow-up lost), #143 (leftover address prompt claims a new invoice request).

### RC-3 — Fallback = unfiltered SELECT * + spreadsheet
The planner accepts garbage and emits a valid-but-empty plan (no filters, no metric) which
becomes `SELECT *`. `_process_request_impl` then attaches an Excel export for >4 rows.
- **Failures**: #19, #110, #116, #132 (injection string → data dump), #133, #144. Also the
  root of "Found 20 results" appearing as the answer to *five different questions*.

### RC-4 — Scope disclosure decoupled from executed SQL
`services/answer_ledger.py:89` (`scope_from_sql`) only extracts `paid` and a single
`client_name ILIKE` — no dates, no brand/production-house widening — and the plan-path
`build_entry` can disagree with what `plan_to_sql` actually emitted. The bot then *narrates
a scope that is not the query it ran*.
- **Failures**: #46 ("Nike jobs in April" answered correctly but narrated as "every
  client"), #51 (date filter applied, narrated as "no date filter — all time").

### RC-5 — Language support is keyword-gated, not classifier-gated
Every deterministic trigger list (invoice verbs, add-job phrases, settings commands,
negative responses) is English-only with a few Hinglish patches. Hindi/Devanagari works only
where it happens to contain an English keyword. The one component that understands all three
registers — the v2 classifier — is not authoritative for writes (RC-1).
- **Failures**: #8 (Hindi onboarding partial), #19, #81, #143. The market context makes
  this a product risk, not a polish item: WhatsApp-native bookkeeping competitors
  (e.g. Munimji/HelloBooks) are Hindi-first.

### RC-6 — Latency: sequential LLM chains + per-call DB connections + blocking webhook
- Planner path = up to 4 sequential LLM round-trips (classify_operation → build_plan →
  optional retry → synthesize), each ~1.5–2.5s through OpenRouter.
- Every `SupabaseService` method does `psycopg2.connect(...)` fresh (TCP + TLS + auth
  ≈ 100–300ms each); a single turn does 5–10 of these (profile, memory reads ×3, store
  conversation, execute, updates).
- `MemoryService` uses ONE global `threading.Lock` around its single shared connection —
  all users serialize on it.
- `main.py` webhooks `await` the full turn before returning 204 to Twilio (15s timeout →
  retries → duplicate replies at p95).
- **Measured**: p50 6.9s / p95 15.5s; 200/649 turns ≥3 LLM calls.

### RC-7 — Eval/config drift: the report doesn't measure production
The graded run had `FLOW_MACHINE_V2` unset (legacy path); production notes describe v2 as
default-on. The corpus itself has stale expectations: #27 expects "last quarter = Jan–Mar
2026" (true in April, false in August — the bot's Apr–Jun answer was *correct*); #2/#5/#6/#8
expect name→company while the shipped flow is name→email→company; 6 rows assert data
preconditions ("no bank saved", "0 records") the shared fixture contradicts.
- **Effect**: ~10 of 46 "fails" are measurement error. Decisions made on this report
  without correcting for it would misallocate effort.

### RC-8 — Security posture below SaaS bar
All SQL is built by string interpolation with hand-rolled `'` escaping (consistent, but one
missed call site = injection; #132 shows injection *text* already reaches the planner). The
service-role key bypasses RLS by design, so app-layer `user_id` scoping is the only tenant
boundary. Credentials were pasted into chat/shell during testing (rotation pending).

---

## 3. Prioritized Problem List

| # | Problem | Severity | Root cause | Evidence |
|---|---------|----------|-----------|----------|
| P0-1 | Write intents unowned under v2; tested path ≠ deployed path | **Critical** | RC-1, RC-7 | dead zone in dispatch_idle; run logs v2-off |
| P0-2 | Stale pending-state hijacks unrelated turns | **Critical** | RC-2 | rows 87–97, 75, 110, 116–118, 143 |
| P0-3 | Unrecognized input → full table dump | **Critical** | RC-3 | rows 19, 110, 116, 132, 133 |
| P0-4 | Webhook blocks past Twilio timeout | **Critical** | RC-6 | p95 15.5s vs 15s timeout |
| P1-1 | Scope disclosure lies | High | RC-4 | rows 46, 51 |
| P1-2 | Invoice entry parsing (bill-no, bare "generate invoice", regenerate, pronouns) | High | RC-1 | rows 65, 70, 79, 140 |
| P1-3 | Hindi/Hinglish parity for writes | High | RC-5 | rows 19, 81, 143 |
| P1-4 | Turn latency p50 6.9s | High | RC-6 | telemetry, 649 turns |
| P1-5 | Structured non-job input captured as job (bank/billing → job row) | High | RC-2/RC-3 | rows 76, 99 |
| P2-1 | Context follow-ups ("them", "this month") not resolved | Medium | RC-1 | rows 140, 141 |
| P2-2 | Calendar-range semantics + phrasing ("2026-08-01 to 2026-08-05") | Medium | — | rows 28, 33 |
| P2-3 | "Show all jobs" = spreadsheet only, nothing in chat | Medium | — | rows 43, 144 |
| P2-4 | Eval harness drift (env, stale expectations, preconditions) | Medium | RC-7 | rows 2–8, 27, 72–74, 89, 102, 133 |
| P2-5 | Smart-capture misc: date silently defaults to today; compound intent dropped | Medium | — | rows 14, 21 |
| P3-1 | String-interpolated SQL; no parameterization | Low* | RC-8 | codebase-wide |
| P3-2 | Credential rotation + secret hygiene | Low* | RC-8 | session history |
| P3-3 | `intent_service.py` at 7,629 lines / `_process_request_impl` ~2,300 lines | Low | RC-1 | maintainability |
| P3-4 | Observability gaps (no dashboards/alerts on fallback rate, route mix, dup webhooks) | Low | RC-6/7 | — |

\* Low *urgency* because current escaping is consistent and tenant scoping works — but P3-2
(rotation) should be done **today** since keys were exposed in chat; it's listed P3 only
because it's an ops task, not an engineering project.

---

## 4. Fixes, Implementation Strategy, Impact, Risks

### P0-1 · One brain: make the classifier authoritative, wire writes into it
**Strategy** (incremental — no rewrite):
1. In `flow_dispatcher.dispatch_idle()`, stop shadowing writes:
   - `WRITE_CREATE` → `intent_service._start_smart_capture(user_id, raw)`
   - `WRITE_INVOICE` → the existing invoice branch, extracted into a callable
     `_handle_invoice_request(user_id, message, params)` so both brains call one function.
     Seed it from the verdict's `parameters` (client, month) before regex fallback.
   - `WRITE_UPDATE` → `_handle_modify_intent`.
   - `WRITE_DELETE` → `_handle_soft_delete`.
   Each keeps the current "return None → legacy fallback" contract on exception.
2. Keep legacy keyword blocks as the v2-off path (unchanged), but treat v2-on as the only
   configuration you test and ship. Freeze new features on the legacy path.
3. Flip `FLOW_MACHINE_V2=true` for the canary cohort first (env already supports
   comma-separated IDs), watch `[ROUTE]`/`[V2_DISPATCH]` telemetry for a week, then global.
4. Re-run the matrix suite **with v2 on** (see P2-4) before the global flip.

**Impact**: closes the largest failure family (job entry, Hindi writes, invoice routing
inconsistency); makes one LLM call the single routing decision; deletes the need for most
keyword lists over time. Expected matrix lift: +10–14 rows.
**Risk**: behavior change on the deployed path → mitigate with canary + matrix re-run.
**Effort**: ~1–2 days including tests.

### P0-2 · Pending-state exclusivity invariant
**Strategy**:
1. Single choke point: an `arm_pending(user_id, kind, payload)` helper in
   `intent_service` (or FlowMachine) that **clears every other pending store** when arming
   (disambiguation, forms, awaiting remnants, FlowMachine state, cached-invoice
   send-intent). Most arm sites already exist (`_arm_*`); route them through it.
2. Reply-shape validation on consume: `_handle_disambiguation_reply` (and each flow's
   `handle_response`) must check the reply is plausible for the prompt (a number ≤ N /
   "all" / "cancel" for disambiguation; an email-shaped string for POC prompts). On
   mismatch: **release the state and fall through** instead of re-prompting. The
   disambiguation handler already half-does this; make it the rule everywhere.
3. One TTL: FlowMachine's `IDLE_TTL_MINUTES` already exists — apply it to
   `pending_disambiguation` and form state via the same `is_timestamp_stale` check at the
   top of the cascade (the code already does this for cached invoices; extend it).
4. Regression tests: replay the exact row-87→97 sequence offline with a fake LLM.

**Impact**: fixes the invoice-email cluster (7 rows), reminder replies, link-ID; this is
the #1 "bot feels broken" fix. Expected lift: +8–10 rows.
**Risk**: low — tightens behavior; the fall-through path already exists.
**Effort**: ~1 day.

### P0-3 · Replace the table-dump fallback with typed clarification
**Strategy**:
1. In `plan.py` / `query_planner.py`: a SELECT plan with **no filters, no metric, no
   time_range, no order/limit** produced from a message that the classifier did not call a
   READ intent → do not execute; return a clarification ("I didn't catch that — try 'show
   my jobs' or 'total earnings this month'").
2. Gate by verdict: when v2 says `UNKNOWN`/low-confidence, never enter the query pipeline
   at all (dispatch_idle already answers via `answer_feature_question` — the gap is the
   v2-off path and non-idle fallthroughs; add the same guard at `note_route("query_pipeline")`).
3. Injection-shaped input (`';`, `DROP`, `UNION SELECT`, `1=1`): match early, reply with
   the on-brand refusal, log a security event. (The SQL layer already survives it; the
   *reply* is the bug.)
4. Keep the spreadsheet behavior only for genuine list queries.

**Impact**: kills the most embarrassing failure mode; +4–6 rows; large trust gain.
**Risk**: over-blocking real queries → mitigate: only trigger when *both* classifier and
plan are empty-signal.
**Effort**: ~half a day.

### P0-4 · Ack-first webhooks
**Strategy**: in `main.py`, both webhooks should validate + enqueue and return 204
immediately; run `_handle_bot_message` via `asyncio.create_task` (or BackgroundTasks) with
per-`MessageSid` dedupe (an in-process TTL set is fine for one instance; move to a DB
uniqueness check when scaling out). The reply already goes out via the Twilio/Telegram send
APIs, not the webhook response, so nothing user-visible changes.
**Impact**: eliminates duplicate-processing risk at p95; decouples user latency from
Twilio's timeout. **Risk**: minimal; verify Railway doesn't kill in-flight tasks on scale-
down (it drains on deploy; acceptable). **Effort**: ~2 hours + a test.

### P1-1 · Truthful scope disclosure
**Strategy**: stop reconstructing scope from heuristics. `plan_to_sql` knows the exact
filters it emitted — return them alongside the SQL (`{"sql": ..., "applied_scope": {...}}`)
and build the ledger entry from `applied_scope`. For the router path, each `RoutedQuery`
already knows its own semantics — add a `scope` field to the route table instead of
regexing the SQL after the fact. Delete `scope_from_sql` once both paths pass scope
explicitly. Add the client/brand-widening (`_expand_client_filters`) to the scope so "just
Nike" appears when the filter was applied.
**Impact**: rows 46, 51 + every future scope line is trustworthy. **Risk**: low;
mechanical. **Effort**: ~half a day.

### P1-2 · Invoice entry hardening (single entry function)
**Strategy** (builds on P0-1's extracted `_handle_invoice_request`):
- **Bill-no lookup**: the regex at `intent_service.py:~5010` already detects bill numbers —
  wire it to a `SELECT ... WHERE bill_no ILIKE` lookup before client-name extraction; if
  found, generate for that row's client+month; if not, say "no bill BB2 on file" (never
  "client named Bill Bb2").
- **Bare "generate invoice"**: candidate client extraction must never yield the verb
  itself; when no client in message, resolve from context in order: verdict
  `resolved_query.client_name` → `last_generated_invoice` → `uscf_context` rows → ledger
  scope → then ask.
- **"Regenerate ..."**: `_is_regenerate_request` exists — when true and
  `last_generated_invoice` (or the DB invoice cache) has client+month, skip the month
  question and force `force_regenerate=True`.
- **Pronouns** ("for them"): same context ladder as bare generate.
**Impact**: rows 65, 70, 79, 140; invoice UX stops feeling brittle. **Risk**: context
mis-resolution → always echo the resolution ("Regenerating **Nike — April 2026**…") so the
user can catch a wrong guess. **Effort**: ~1 day.

### P1-3 · Hindi/Hinglish parity
**Strategy**: parity comes free once the classifier owns writes (P0-1) — it already reads
all three registers. Additionally: add the top Hindi invoice/job phrases to the
deterministic router's regexes (`invoice bhejo/banao/bana do`, `kaam kiya`, `ka bill`) so
common cases skip the LLM entirely; add 10 Hindi/Hinglish rows to the offline harness
tests. Do **not** attempt translation layers — classify natively (this is also what the
successful India-market bots do).
**Impact**: rows 19, 81, 143; market credibility. **Effort**: ~half a day after P0-1.

### P1-4 · Latency package — target p50 < 3s, p95 < 8s
Ordered by measured payoff:
1. **DB connection pool** (`psycopg_pool.ConnectionPool`, min 1 / max 5–10, with
   `check=ConnectionPool.check_connection`): replaces every per-call
   `psycopg2.connect`. This is the correct fix for the earlier "persistent connection
   hung" incident — a pool health-checks and replaces dead connections instead of hanging
   on one. Keep `connect_timeout=5` + `statement_timeout`. Savings: ~0.5–2s/turn on
   multi-query turns. Also remove `MemoryService`'s global lock in favor of the pool.
2. **One turn = one memory read**: `get_user_memory`, `get_conversation_history`, and
   `get_form_state` all read the same `user_memory` row; the turn-cache pattern already
   exists for the profile (`_turn_cache`) — extend it to the memory payload with
   write-through. Savings: 2–4 round-trips/turn.
3. **Collapse classify→plan into one LLM call**: extend the v2 classifier's JSON verdict
   with an optional `plan` object for READ intents (it already emits `resolved_query` with
   metric hints). The planner then only runs when the verdict's plan fails Path 3
   validation. Savings: one full LLM round-trip (~1.5–2.5s) on most query turns.
4. **Deterministic synthesis for aggregates and short lists**: `response_synthesis.py` +
   `format_inr` already produce the aggregate sentence; make deterministic rendering the
   default (LLM synthesis only for >4-row narrative summaries). Savings: another full LLM
   call on aggregate turns — combined with (3), a typical aggregate query goes 3 calls → 1.
5. **Tight output budgets + `temperature:0`** on all classifier/planner calls (mostly done;
   audit stragglers like `parse_user_intent`).
6. Keep the router-first design — 268/649 turns already resolve with zero LLM calls; grow
   that share with the P1-3 Hindi routes.
**Impact**: p50 6.9s → ~2.5–3.5s; p95 under the Twilio window even before P0-4.
**Risk**: (3) changes the planner contract — do it behind a flag with the matrix suite as
the gate; (1) is drop-in but soak it on canary for pooler behavior.
**Effort**: (1)+(2) ~1 day; (3)+(4) ~2 days behind a flag.

### P1-5 · Structured-input detector before capture
**Strategy**: before smart capture consumes a message, run a cheap structural check: ≥2
`Field: value` lines matching bank/billing vocabulary (`account`, `ifsc`, `upi`, `gst`,
`bank`) → route to the bank/billing handler for whatever flow is pending; if none pending,
ask which the user meant. Never let account numbers become `job_entries` rows.
**Impact**: rows 76, 99; data-integrity class fix. **Effort**: ~2 hours + tests.

### P2 items (after the above)
- **P2-1 context follow-ups**: flip the WP-2 shadow (`resolved_query`,
  `references_last_answer`) to authoritative once its shadow-agreement telemetry looks
  good — the scaffolding and eval hooks already exist.
- **P2-2 calendar semantics**: `_clamp_time_range` should clamp *data* to today but
  disclose the *period* ("August so far: ₹43,000"), and "this quarter/month" should use
  full calendar bounds in the SQL range. Small change in `_time_range_conditions` +
  disclosure wording.
- **P2-3 list rendering**: for list queries, always show the top 5 rows as cards in chat
  *and* attach the spreadsheet when >4. One formatting change.
- **P2-4 harness integrity**: pin the harness env to the production flag set (assert
  `FLOW_MACHINE_V2` state at run start and record it in the workbook header); fix stale
  sheet expectations (quarter row, onboarding order — pick option A: sheet matches code);
  give precondition rows dedicated fixture users (fresh user for "0 records", no-bank user
  for #73/#102). ~1 day, and it makes every future score trustworthy.
- **P2-5**: when smart capture defaults `job_date` to today, say so in the confirmation
  card ("Date: today — 2026-08-06 (change?)"); re-arm the compound-intent
  `suggested_next_action` under v2 (it's only wired in the legacy trigger block).

### P3 items
- **P3-1**: migrate `execute_sql` call sites to parameterized queries incrementally
  (new/changed call sites first; the planner's generated SQL keeps validation via
  `sql_validator` + `query_guard`). Not urgent, but stops the class permanently.
- **P3-2**: rotate the Supabase service-role JWT, DB password, and OpenRouter key **now**;
  move Railway env management to sealed variables; add a pre-commit secret scanner
  (`gitleaks`) to CI.
- **P3-3**: after the v2 flip is stable, delete the dead legacy blocks and split
  `intent_service.py` into `dispatch.py` / `invoice_flow.py` / `capture_flow.py` /
  `settings_flow.py`. Do this *after* behavior stabilizes — moving code while both brains
  are live multiplies risk.
- **P3-4**: dashboardize the existing telemetry (route mix, llm_calls/turn, fallback rate,
  turn_ms percentiles, dup-webhook count). It's already logged; pipe it to Railway
  metrics/Grafana or even a daily cron summary to Telegram.

---

## 5. Competitor & Industry Patterns (what to adopt, what to skip)

- **Deterministic-first routing with LLM fallback** is the 2026 consensus pattern
  ([Redis LLM-router best practices](https://redis.io/blog/llm-router-architecture-best-practices/),
  [Rasa's architecture](https://rasa.com/blog/llm-chatbot-architecture),
  [IrisAgent intent guide](https://irisagent.com/blog/building-chatbots-with-intent-detection-guide/)).
  You already built this (query_router). The plan extends it (Hindi routes, verdict-gated
  fallback) rather than adding anything new.
- **Function-calling / typed plans beat free-form NL2SQL** on reliability in production
  studies ([function-calling vs NL2SQL comparison](https://arxiv.org/pdf/2506.08757);
  raw-prompt NL2SQL accuracy collapses on real schemas per
  [BlazeSQL's 2026 guide](https://www.blazesql.com/blog/natural-language-to-sql)). Path 3
  is exactly this pattern — P1-4(3) completes it by making the typed verdict the single
  LLM artifact.
- **Dialogue state as a single state machine** (Rasa-style) rather than scattered flags —
  FlowMachine v2 is the right shape; the failure mode here is *finishing the migration*,
  which is why P0-1/P0-2 lead the roadmap.
- **India-market WhatsApp bookkeeping** ([Munimji/HelloBooks](https://hellobooks.ai/bookkeeping-on-whatsapp),
  [VoiceKhata comparisons](https://voicekhata.com/compare/),
  [Vyapar](https://vyapar.com/)): table stakes are Hindi/Hinglish-native understanding,
  instant acknowledgment ("typing…" then reply), and never exposing raw data dumps.
  Differentiators worth keeping: your bill_no system, reminder tiers, and cross-platform
  linking are ahead of the low-end competition; the gap is conversational reliability, not
  features.
- **Skip**: multi-agent orchestration frameworks, vector/RAG layers, and prompt-rule
  "knowledge books" — your own A/B evidence (KnowledgeBook, 2026-06) showed deterministic
  guards beat prompt-grounding for this bot. The plan follows that lesson.

---

## 6. Implementation Order (dependency-safe)

**Week 0 (today): ops**
0. P3-2 credential rotation. No code dependency; overdue.

**Week 1: stop the bleeding (no routing-brain change yet)**
1. P0-4 ack-first webhooks (independent, de-risks everything after).
2. P0-2 pending-state exclusivity + reply-shape validation (+ regression tests).
3. P0-3 fallback clarification + injection refusal.
4. P1-5 structured-input detector.
   *These four are all safe on both brains and fix ~18 failed rows.*

**Week 2: one brain**
5. P0-1 wire WRITE_* into dispatch_idle; extract `_handle_invoice_request`.
6. P1-2 invoice entry hardening (inside the extracted function).
7. P1-3 Hindi router routes.
8. P2-4 harness integrity → **re-run the matrix with v2 on** (gate for the canary flip).

**Week 3: speed**
9. P1-4(1)(2) connection pool + turn-cache (pure infra, both brains benefit).
10. P1-4(3)(4) merged classify+plan and deterministic synthesis, behind a flag, matrix-gated.

**Week 4: polish + flip**
11. P2-1 context follow-ups authoritative; P2-2 calendar semantics; P2-3 list rendering;
    P2-5 capture polish; P1-1 scope-from-plan.
12. Canary flip → global flip → P3-3 dead-code deletion → P3-4 dashboards.

Every step lands with regression tests (per the repo's own protocol) and none breaks the
currently-working categories: Payments/Earnings/Small Talk paths are untouched until
week 3's flagged changes, which the matrix suite gates.

---

## 7. Expected Outcomes

| Metric | Now | After weeks 1–2 | After week 4 |
|---|---|---|---|
| Matrix pass (graded) | 62.5% | ~78–82% | **≥90%** |
| p50 turn | 6.9s | ~5s (pool+cache) | **<3s** |
| p95 turn | 15.5s | <10s | **<8s** |
| LLM calls, typical query turn | 3 | 3 | **1** |
| Duplicate-webhook risk | at p95 | none | none |
| Table-dump fallbacks | common | zero | zero |
| Routing brains in production | 1.5 | 1.5 | **1** |

## 8. Production Checklist (before public launch)

- [ ] Credentials rotated (service-role JWT, DB password, AI key); gitleaks in CI
- [ ] Matrix suite ≥90% on the production flag set, run recorded with env fingerprint
- [ ] p50 < 3s, p95 < 8s over a 100-turn soak
- [ ] Webhook ack < 1s; dedupe verified with a forced Twilio retry
- [ ] Stale-state regression pack green (rows 87–97 replay)
- [ ] Injection corpus replied with refusal, zero data returned
- [ ] Hindi/Hinglish write parity spot-checked live (10 messages)
- [ ] Reminder worker + invoice email verified end-to-end on staging numbers
- [ ] Rollback plan: `FLOW_MACHINE_V2` flag flip documented as the instant revert
