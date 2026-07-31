# TODO — Path to a production-grade conversational bot

**Context for whoever picks this up:** Every live bug we've fixed recently falls into
three families: (1) keyword checkpoints hijacking messages before anything understands
them, (2) conversational state scattered across ~10 parallel stores that go stale and
contaminate later turns, (3) raw LLM output trusted without validation. The phases below
attack those root causes in order of risk. Phase 0 is safe and immediate; Phases 1–2 are
the real architectural fix; Phases 3–4 make quality measurable and keep it that way.

Rules that apply to EVERY task below:
- Run `python -m pytest tests/ -v` before AND after your change. Zero failures allowed.
- Every task ships with a regression test (see `tests/README.md` for the protocol).
- Commit directly to `main`, message prefix `C C : `.
- Touch the fewest files possible. If a task says "delete", delete — don't comment out.

---

## Phase 0 — Stop the live flakes (do these first, ~1–2 days total)

### 0.1 Retry the planner once when the LLM returns invalid JSON
- **File:** `services/query_planner.py`, in `build_operation_plan()` (~line 396–414).
- The call already requests JSON mode (`responseMimeType: application/json`), but the
  upstream model still occasionally returns malformed JSON (seen live: `JSON parse
  error: Expecting property name enclosed in double quotes`). Today that error
  immediately falls back to legacy SQL generation, which is lower quality.
- **What to do:** wrap the `_call_api` + `json.loads` block in a `for attempt in range(2)`
  loop. On `json.JSONDecodeError` in attempt 0, log `[PLANNER] JSON parse error, retrying
  once` and call again (same prompt). Only return `{"_error": ...}` after the second
  failure.
- **Test:** in `tests/test_plan_retry.py`, add a test where the mocked
  `gemini_service._call_api` returns invalid JSON on call 1 and valid JSON on call 2 —
  assert the plan comes back valid and `_call_api` was called exactly twice. Add a second
  test where both calls are invalid — assert `_error` is set and calls == 2 (no infinite
  retry).

### 0.2 Generalize the truncated-synthesis guard
- **File:** `services/intent_service.py` — search for `_looks_truncated`.
- We shipped a narrow guard (short + digit-free ⇒ fall back to deterministic renderer)
  at ONE call site after a live incident where the bot replied literally `"You've had"`.
  But `synthesize_response()` has other call sites (search for `self.gemini.synthesize_response(`)
  that still trust the output raw.
- **What to do:** move the check into a small module-level helper in
  `services/intent_service.py`:
  ```python
  def _synthesis_looks_broken(response: Optional[str]) -> bool:
      """A reply to a data question that is very short AND contains no digit is
      almost certainly an upstream truncation (live example: "You've had")."""
      if not response or not response.strip():
          return True
      r = response.strip()
      return len(r) < 15 and not any(ch.isdigit() for ch in r)
  ```
  Replace the inline check at the existing site with a call to this helper, then apply it
  at every other `synthesize_response` call site that answers a data query (NOT the
  small-talk/feature-question sites — a short digit-free reply is fine there). Each site
  already has some fallback string; keep using that site's existing fallback.
- **Test:** extend `tests/test_answer_ledger_integration.py::TestTruncatedSynthesisFallsBackToDeterministicAnswer`
  with one test per newly-guarded call site.

### 0.3 Log every routing decision with one grep-able prefix
- **File:** `services/intent_service.py`, `_process_request_impl`.
- Today, when a checkpoint (audit-reply, small-talk, form-step, awaiting_* handler,
  disambiguation, etc.) claims a message, some log and some don't — diagnosing a hijack
  from Railway logs takes an hour.
- **What to do:** at every `return` point inside `_process_request_impl` that hands the
  message to a specific handler, add ONE log line immediately before dispatch, uniform
  format:
  `logger.info(f"[ROUTE] {checkpoint_name} claimed message: {message[:60]!r}")`
  Checkpoint names: `form_step`, `pending_reminder`, `audit_reply`, `bank_command`,
  `small_talk`, `disambiguation`, `compound_followup`, `modify`, `add_job`,
  `awaiting_<flag>` (one per awaiting handler), `no_op_cancel`, `negative_response`,
  `invoice_action`, `cached_invoice_send`, `overdue`, `query_pipeline`.
- Do NOT change any behavior in this task. Log lines only.
- **Test:** one test that runs a plain query through `process_request` with everything
  mocked and asserts (via `caplog`) that exactly one `[ROUTE]` line was emitted.

### 0.4 Add a per-turn route field to telemetry
- **File:** `services/telemetry.py` (the `[TELEMETRY]` line already prints `route=unclassified`
  for every turn — it's never being set).
- **What to do:** find where `Turn` is created/finished in `services/telemetry.py` and
  `process_request`. Set `route` to the same checkpoint name logged in 0.3 (pass it via
  the existing turn object — look at how `note_llm_call()` reaches the current turn and
  mirror that with a `note_route(name)` helper).
- **Why:** lets us answer "what % of turns get hijacked before the query pipeline"
  directly from logs.
- **Test:** unit test on the telemetry module: start a turn, call `note_route("query_pipeline")`,
  finish, assert the line contains `route=query_pipeline`.

---

## Phase 1 — Understanding before routing (the core fix, ~1–2 weeks)

**Goal:** ONE understanding step runs first on every message; everything after it is a
deterministic dispatch on the verdict. The v2 classifier (`services/classifier.py`) and
dispatcher (`services/flow_dispatcher.py`) already exist and already run — but only
AFTER ~10 legacy keyword checkpoints have had a chance to steal the message, and they
fall back to legacy (`SHADOW_ONLY`) for most branches. The work is to move them to the
top and port checkpoints behind them one at a time.

**Order matters.** Do the tasks in this sequence — each one is independently shippable
and independently revertible.

### 1.1 Kill the audit-reply keyword hijack
- **Files:** `services/intent_service.py` (`_handle_pending_audit_reply`, and its call
  site in `_handle_pending_reminder` ~line 3208), `services/classifier.py`.
- This checkpoint intercepts any message containing "paid" while an overdue-audit nudge
  is pending — BEFORE the classifier runs. It has needed three rounds of guard patches
  (question-shape regex, filler-word list, mid-sentence question words) and it will need
  more.
- **What to do:**
  1. In `services/classifier.py`, add `AUDIT_REPLY` to the intent enum the classifier
     can return, with 3–4 few-shot examples in the prompt ("paid 2", "all paid",
     "later"), plus counter-examples ("how many have paid" → READ_AGGREGATE).
  2. The classifier already receives flow context; make sure a pending audit list is
     passed in (add `audit_pending: true/false` to the context dict built in
     `_process_request_impl` before `_v2_classify`).
  3. In `services/flow_dispatcher.py`, route `intent == "AUDIT_REPLY"` to the existing
     `_handle_pending_audit_reply`.
  4. In `_process_request_impl`, delete the early call to `_handle_pending_reminder`'s
     audit-reply branch (keep the reminder-sending branch if it's a different concern —
     read `_handle_pending_reminder` carefully first; only the audit-REPLY interception
     moves).
- **Gate:** this only takes effect when `FLOW_MACHINE_V2` is on (it is, in production).
  Keep the legacy call as a fallback when v2 is off.
- **Tests:** port the whole `TestAuditReplyDoesNotHijackQuestions` class to go through
  the classifier path (mock classifier verdict), and keep the originals passing for the
  v2-off path.

### 1.2 Kill the small-talk keyword checkpoint (same recipe)
- `_detect_small_talk` runs on raw keyword/trigger lists before understanding. The
  classifier already has a small-talk-ish intent — verify what it's called in
  `services/classifier.py` (grep for `SMALL_TALK` / `GREETING`), route it in the
  dispatcher to the existing canned-response generator, and remove the early checkpoint
  when v2 is on. Same gating + test recipe as 1.1.

### 1.3 Kill the invoice keyword check (`_INVOICE_CHECK`)
- **File:** `services/intent_service.py` — grep `[INVOICE_CHECK]`.
- This regex/verb check decides "is this an invoice action?" before understanding and
  has already been overridden once by a guard (grep `TestV2VerdictBeatsLegacyInvoiceCheck`
  in `tests/test_planner_boundary.py` for the story). The classifier has
  `INVOICE_ACTION`-type intents.
- Same recipe: classifier verdict routes to the existing invoice flow entry point;
  legacy check only runs when v2 is off. This one is the biggest single win — most
  misroutes we saw in live testing passed through this gate.

### 1.4 Port the remaining pre-classifier checkpoints
- One PR per checkpoint, same recipe, in this order (easiest → hardest):
  1. `add_job` trigger list (classifier: `CREATE_ENTRY`)
  2. `modify` verb triggers (classifier: `UPDATE_ENTRY`)
  3. bank/name/link explicit commands (classifier: `SETTINGS_COMMAND` — may need adding)
  4. `_reconstruct_message` (replace with the classifier's `resolved_query` field — it
     already computes one; grep `resolved_query` in `services/classifier.py` and
     `[UNDERSTAND_V2_SHADOW]` in intent_service to see it being logged-but-unused)
- After each port, delete the corresponding keyword list. The end state for
  `_process_request_impl`'s top half: onboarding gate → state TTL check → classifier →
  dispatcher. Nothing else.

### 1.5 Delete `SHADOW_ONLY` fallbacks one branch at a time
- **File:** `services/flow_dispatcher.py`. Grep `SHADOW_ONLY`.
- Each `return SHADOW_ONLY` means "v2 understood the message but legacy still handles
  it". For each one: implement the real dispatch (most just call an existing
  `intent_service` method — see how `InvoiceAwaitSendConfirm.handle_response` delegates
  in `services/flows.py`), then delete the fallback.
- Priority: `SIDE_QUESTION` for READ paths (currently loses the resume-nudge), then
  `NEW_FLOW`.

---

## Phase 2 — One conversation state object (~1 week, can overlap Phase 1)

**Goal:** replace the ~10 parallel state stores with the FlowMachine as the single
source of truth. The stores as of Phase 2.2/2.3 kickoff (all in user memory unless noted):
1. ~~14 `awaiting_*` booleans (`_AWAITING_FLAGS` in intent_service)~~ — **migrated.**
   `_AWAITING_FLAGS` is an empty tuple now; every flag has its own FlowMachine flow.
2. `pending_*` payload keys (`pending_send_invoice`, `pending_invoice`,
   `pending_poc_email_client`, `pending_billing_client`, …) — kept as FlowMachine
   *context* payload, not deleted (they're data, not routing flags).
3. ~~`pending_disambiguation` dict~~ — **migrated** to `DISAMBIGUATION` (payload stays).
4. form state (`memory.get_form_state` — smart capture) — **migrated** to
   `SMART_CAPTURE_CONFIRM_PENDING`; both arm sites write FlowMachine directly now.
5. audit-reminder pending list (`get_pending` in the reminder module) — stays outside
   FlowMachine by design (cron-armed, not `process_request`-armed).
6. `last_intent` (drives `_reconstruct_message`) — Phase 2.4 candidate for outright
   retirement (overlaps the classifier's `resolved_query`), not ported.
7. `uscf_context` (last rows / last SQL) — Phase 2.4 candidate for outright retirement
   (overlaps `answer_ledger`), not ported.
8. `last_generated_invoice` cache — untouched, Phase 2.4's TTL-unification territory.
9. `answer_ledger` (WP-1 — this one is fine, keep it)
10. FlowMachine v2 state (`services/flow_machine.py`) — now the sole source of truth
    for every conversational-state flow; see `services/FLOW_MACHINE_V2.md`.

### 2.1 Write the state inventory doc ✅ DONE
- Table lives in `services/FLOW_MACHINE_V2.md`, mapping every legacy store → the
  FlowMachine flow + context keys that replace it.

### 2.2 Make FlowMachine the writer, legacy flags the mirror ✅ DONE
### 2.3 Port readers, then delete the mirror ✅ DONE

All 12 originally-mirrored flows (`INVOICE_AWAIT_SEND_CONFIRM`, `BANK_DETAILS`,
`NAME_CHANGE`, `LINK_ACCOUNT`, `INVOICE_NEED_POC_EMAIL`, `INVOICE_NEED_BILLING`,
`INVOICE_NEED_POC_NAME`, `INVOICE_ADDRESS`, `INVOICE_NEED_JOB_DESCRIPTION`,
`SMART_CAPTURE_NEED_DESCRIPTION`, `SMART_CAPTURE_CONFIRM_PENDING`, `DISAMBIGUATION`)
AND all 6 originally-legacy-only flows (`INVOICE_READINESS_POC_EMAIL`,
`INVOICE_NEED_MONTH`, `COMPOUND_RESPONSE`, `MODIFY_FIELD`, plus the two
short-lived same-turn caches `pending_value_fork`/`pending_reminder_offer`
left as-is by design — not blocking gates, no Flow class needed) are now
FlowMachine-owned. `_AWAITING_FLAGS` is an empty tuple — no boolean legacy
conversational-state flag remains anywhere in the codebase. See
`services/FLOW_MACHINE_V2.md`'s "Progress update" and "Post-2.3" sections for
the full per-flow writeup, including two real pre-existing bugs the
migration surfaced and fixed (`LINK_ACCOUNT`'s and `SMART_CAPTURE_CONFIRM_PENDING`'s
check-after retry-loop bug) and three flagged-but-not-fixed out-of-scope bugs
(`task_ab7501b3`, `task_1b10c22c`, `task_e16e90fb`).

### 2.4 One TTL for everything
- FlowMachine already has a TTL check (grep `TTL` in intent_service around the v2
  block). Once state lives in one place, delete the ad-hoc staleness checks:
  the 30-min form check in `_handle_form_step`, the 30-min `last_generated_invoice`
  cache expiry, and the stale-clear dict in the v2 TTL block. One expiry policy,
  defined in `services/flow_machine.py`.

---

## Phase 3 — Fewer, cheaper, safer LLM calls (~2–3 days, after Phase 1.3)

Target: ≤2 LLM calls per normal turn (today: 3–5, with 20–35s turns seen live).

### 3.1 Deterministic answers for aggregates — no synthesis call
- **File:** `services/intent_service.py`, the query result handling (~line 5800s).
- When the result is a single aggregate row (`rows == [{"result": N}]`), do NOT call
  `synthesize_response`. Build the reply with `render_answer_payload(build_answer_payload(...))`
  (already exists in `services/response_synthesis.py`, already used as the fallback).
  This removes an entire LLM call AND the truncation/refusal flake surface from the most
  common question type. Keep the LLM synthesis for multi-row prose answers.
- **Config flag:** `DETERMINISTIC_AGGREGATES=1` default on, so it can be flipped off if
  the phrasing feels too dry. (The renderer's phrasing can be improved in place —
  it's plain Python.)
- **Tests:** for each aggregate shape (count, sum, avg, distinct-count, ₹0 result):
  assert `synthesize_response` was NOT called and the reply contains the number.

### 3.2 Merge classify + plan for READ intents
- Today a READ query pays: classifier call → planner call → synthesis call.
- The classifier already extracts `params` (metric/column/group_by — see the
  `[CLASSIFIER]` log line). For READ_AGGREGATE / READ_QUERY verdicts with high
  confidence and complete params, build the plan dict directly from the verdict and skip
  `build_operation_plan` entirely. Validate through the same `Plan.from_raw()` path
  (Path 3) so malformed verdicts still get caught. Fall back to the full planner when
  params are missing or validation fails.
- With 3.1 + 3.2 a simple "how many clients have paid?" costs exactly ONE LLM call.

### 3.3 Cap and monitor
- `[TELEMETRY_ALERT]` already fires at >2 calls/turn. After 3.1/3.2, treat that alert as
  a CI-able regression: add a test that runs the 10 most common query shapes through
  `process_request` (all AI mocked) and asserts `llm_calls <= 2` per turn via the
  telemetry counter.

---

## Phase 4 — A real evaluation harness (~2–3 days, then it runs forever)

**Problem it solves:** all live testing so far ran against one shared WhatsApp account.
State leaked between runs and between scenarios; roughly HALF the "failures" in every
run were contamination, not bugs. That noise cost more time than the bugs themselves.

### 4.1 Seeded fixture account
- **New file:** `tests/e2e/seed.py`. Creates a fresh synthetic user id (e.g.
  `e2etest:<uuid>`), inserts ~15 deterministic `job_entries` rows covering: paid/unpaid,
  multiple clients, multiple months, a no-poc-email client, a no-job-date row, bank
  details in `user_config`. Returns the user id. Include a `teardown(user_id)` that
  deletes all rows for that id.
- Needs `SUPABASE_DB_URL` — skip (pytest `skipif`) when not set, so CI without secrets
  still passes.

### 4.2 Scenario runner with graded assertions
- **New file:** `tests/e2e/test_scenarios.py`. Port the 134-scenario sheet
  (`Intent_Test_Matrix`) into a Python list of
  `(message, [assertion, ...])` where assertions are machine-checkable:
  `contains_number`, `contains("₹")`, `operation == "query"`, `not contains("couldn't")`,
  `row_created(client="...")` (checks the DB), etc. NO eyeball grading.
- Scenarios that depend on a previous turn's state must declare it explicitly
  (`requires: fresh_state` / `requires: after("Show Samsung jobs")`) — the runner resets
  memory state between independent scenarios (one `MemoryService` wipe per group).
- Each scenario runs against a REAL `IntentService` with real AI + the seeded fixture
  account. Budget: this suite is slow and costs API money — mark it
  `@pytest.mark.live`, excluded from default runs, run nightly / on demand:
  `python -m pytest tests/e2e -m live -v`.

### 4.3 Score tracking
- The runner writes `tests/e2e/last_run.json`: per-scenario pass/fail + overall %.
  Committing it on each run gives a pass-rate history in git log. Fail the run (exit
  nonzero) if the pass rate drops below the previous committed run — regressions become
  impossible to miss.

---

## Explicitly OUT of scope (don't get pulled into these)
- Rewriting `intent_service.py` from scratch in one PR. The phases above get to the same
  place in shippable steps; a big-bang rewrite will break flows we don't have tests for.
- Switching AI providers/models. Measure first (Phase 0.4 + Phase 4 give the data);
  decide after.
- New features. Nothing here adds capability; it makes the existing capability reliable.

## Definition of done for the whole effort
- `_process_request_impl` top half = gate → TTL → classifier → dispatcher (no keyword
  checkpoints ahead of understanding).
- Exactly one conversational state store (FlowMachine) + the answer ledger.
- ≤2 LLM calls per normal turn; aggregates answered deterministically.
- Nightly e2e pass rate ≥95% on the seeded account, tracked in git.
