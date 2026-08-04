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

### 0.1 Retry the planner once when the LLM returns invalid JSON ✅ DONE
*Audited 2026-08-04: `for attempt in range(2)` at `services/query_planner.py:396`; both
required tests present in `tests/test_plan_retry.py`
(`test_malformed_json_first_try_valid_json_second_try`,
`test_malformed_json_both_attempts_returns_error`).*
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

### 0.2 Generalize the truncated-synthesis guard ✅ DONE
*Audited then fixed 2026-08-04. The helper existed but guarded only 3 of 10
`synthesize_response` call sites; the other 7 were ALL data-query sites still using
`if not response or not response.strip():` — which catches an EMPTY reply but not the
short-and-digit-free truncation ("You've had") the guard exists for. All 10 now go
through `_synthesis_looks_broken`, each keeping its own existing fallback. (One site,
line 2025, uses the inverted form `if not _synthesis_looks_broken(response): return`.)*
*Tests: `TestEverySynthesisSiteIsGuardedAgainstTruncation` in
`tests/test_answer_ledger_integration.py` asserts the invariant at SOURCE level rather
than one brittle behavioural test per path — it also catches an 11th site added later,
which is exactly how the original gap appeared. Plus a behavioural test on the
newly-guarded router ROWS path. Both were verified to actually FAIL when a guard is
reverted; the behavioural one initially passed while regressed (its rows were "full job
rows", so they rendered as cards and never reached synthesis) and now asserts
`synthesize_response.called` so it can't silently stop covering the path again.*

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

### 0.3 Log every routing decision with one grep-able prefix ✅ DONE
*Audited then fixed 2026-08-04. Most names from the spec's list are correctly ABSENT —
those checkpoints no longer exist as legacy return points after Phases 1–2. Three real
gaps found and closed:*
1. *`small_talk` and `no_op_cancel` returned silently — now logged + `note_route`d.*
2. ***The deterministic router emitted no `[ROUTE]` line at all.** It answers the ~15
   most common query shapes, so the single most common SUCCESSFUL path was the one
   invisible to the grep this task exists to enable (turns logged as
   `route=unclassified`). Now `[ROUTE] router:<name>` + `note_route("router:<name>")`,
   named per-route so telemetry shows WHICH route absorbed the turn.*
3. *`[ROUTE] compound_response declined:` (added during Phase 2) broke the uniform
   format and had no `note_route` — log and metric disagreed. Now conforms as
   `compound_declined`.*
*Tests: `tests/test_route_logging.py` — the spec's "exactly one [ROUTE] line per turn"
test (which is what caught the router gap), per-checkpoint tests for the new lines,
telemetry `route=` assertions, plus two source-level invariants: every `[ROUTE]` log
matches the uniform format, and every one has a `note_route()` beside it so 0.3 and 0.4
can't drift apart.*

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

### 0.4 Add a per-turn route field to telemetry ✅ DONE
*Audited 2026-08-04: `note_route()` at `utils/telemetry.py:81` (the spec said
`services/telemetry.py` — the module actually lives under `utils/`), `route=` emitted on
the `[TELEMETRY]` line (:138), 12 call sites in intent_service, and
`tests/test_telemetry.py::test_note_route_visible_within_the_turn` covers it.*
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

### 1.1 Kill the audit-reply keyword hijack ✅ DONE
*Audited 2026-08-04: `AUDIT_REPLY` is in the classifier's intent enum and prompt, `audit_pending` is passed through the idle context block, and `services/flow_dispatcher.py` routes the intent. Legacy path retained for v2-off.*
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

### 1.2 Kill the small-talk keyword checkpoint (same recipe) ✅ DONE
*Audited 2026-08-04: legacy `_detect_small_talk` is gated behind `if not _flow_machine_v2_enabled_for(user_id)` (`services/intent_service.py:4280`); `dispatch_idle` owns SMALL_TALK when v2 is on.*
- `_detect_small_talk` runs on raw keyword/trigger lists before understanding. The
  classifier already has a small-talk-ish intent — verify what it's called in
  `services/classifier.py` (grep for `SMALL_TALK` / `GREETING`), route it in the
  dispatcher to the existing canned-response generator, and remove the early checkpoint
  when v2 is on. Same gating + test recipe as 1.1.

### 1.3 Kill the invoice keyword check (`_INVOICE_CHECK`) ✅ DONE
*Audited 2026-08-04: gated behind `if not _flow_machine_v2_enabled_for(user_id)` (`services/intent_service.py:4774`), plus the `_v2_says_read` override so a confident v2 READ verdict beats the legacy keyword check.*
- **File:** `services/intent_service.py` — grep `[INVOICE_CHECK]`.
- This regex/verb check decides "is this an invoice action?" before understanding and
  has already been overridden once by a guard (grep `TestV2VerdictBeatsLegacyInvoiceCheck`
  in `tests/test_planner_boundary.py` for the story). The classifier has
  `INVOICE_ACTION`-type intents.
- Same recipe: classifier verdict routes to the existing invoice flow entry point;
  legacy check only runs when v2 is off. This one is the biggest single win — most
  misroutes we saw in live testing passed through this gate.

### 1.4 Port the remaining pre-classifier checkpoints — ⚠️ PARTIAL (3 of 4)
*Audited 2026-08-04:*
1. *`add_job` triggers — ✅ gated on v2-off (`intent_service.py:4351`).*
2. *`modify` verb triggers — ✅ gated on v2-off (`intent_service.py:4331`).*
3. *bank/name/link/address/user-id commands — ✅ **FIXED 2026-08-04** (was a live bug).
   The block was gated behind `if not _flow_machine_v2_enabled_for(...)` on the grounds
   that "the classifier handles these as SETTINGS_COMMAND" — but that intent was never
   added to the classifier, and each handler had exactly ONE caller, inside that gate.
   Reproduced with v2 ON: all six commands fell through to the QUERY PIPELINE
   (`operation='query'`), so "update my bank details" was handed to the SQL planner as
   though it were a data question. Fixed by un-gating the block rather than building a
   SETTINGS_COMMAND intent — these are explicit unambiguous keyword commands, so the
   simplest fix that restores correct behaviour (CLAUDE.md: "one-line fix > new
   config"). Un-gating re-exposed a latent hijack: `"my id"` was substring-matched, so
   "what is my idea for the Nike shoot" became the user-id command — now word-boundary
   matched. Tests: `tests/test_settings_commands_reachable.py` (18) cover reachability
   with v2 both on AND off, non-swallowing by the query pipeline, and the hijack guard.*
   *If SETTINGS_COMMAND is still wanted as the "proper" port, it is now a refactor on
   top of working behaviour rather than a fix for an outage.*
4. *`_reconstruct_message` → `resolved_query` — ⚠️ **PARTIAL, by design.** A full swap is unsafe and was rejected with evidence: the classifier emits `resolved_query` as null for anything that isn't READ_QUERY/READ_AGGREGATE, but FOUR of `_reconstruct_message`'s six cases are WRITE_INVOICE paths ("which month?" → "March" → Generate invoice) — replacing wholesale deletes them with no replacement. And the messages `resolved_query` exists for ("what about this month?", "and last quarter?") never reach the function: `_looks_like_a_question` returns early and the answer_ledger handles them. Both verified empirically.*
   *What DID land: where reconstruction already fires, a client freshly resolved by the classifier for THIS turn now beats the stored `last_intent.client_name` — nearly every incident in that function's own comments is the stale-client shape. Gated to READ intents AND to turns with no pending clarification (while `pending` is set the user is answering the bot's own question about a specific flow, so the stored client is authoritative). That second guard exists because without it a stray READ verdict rewrote a mid-invoice "March" into "Generate invoice for Nike for March" — an invoice against the WRONG CLIENT, caught by its own test before release. Tests: `tests/test_resolved_query_reconstruction.py` (30).*
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

### 1.5 Delete `SHADOW_ONLY` fallbacks one branch at a time — ✅ DONE (as far as is safe)
*Audited then completed 2026-08-04. Both STATED priorities are now genuinely done:*
- *`NEW_FLOW` — commit 7165bec.*
- *`SIDE_QUESTION` for READ paths — the router-answerable half landed in 11077cd, but the
  spec's actual complaint ("currently loses the resume-nudge") still applied to the OTHER
  half: a side question the deterministic router can't answer is handed to legacy's
  planner, which knows nothing about the active flow, so the "still waiting on X" reminder
  was dropped and the user was stranded mid-flow. Fixed by carrying the nudge across the
  handoff — stashed thread-locally for the turn, appended by `process_request` to whatever
  legacy produced. Same user-visible result as fully porting the branch, without lifting
  the ~1000-line query cascade (follow-ups, ledger, invoice check, router, planner,
  synthesis, export) into the dispatcher. Tests: `tests/test_resume_nudge_handoff.py` (11).*

*The 8 remaining `return SHADOW_ONLY` sites are NOT work items — audited individually:*
- *4 are defensive fallbacks that must stay (flow not in registry; unknown/missing
  `flow_compatible`; the outer exception handler; AUDIT_REPLY with no pending list).
  Deleting them leaves an unhandled state with nowhere to go.*
- *1 is intentional design: `NEW_FLOW` clears the flow then returns SHADOW_ONLY *so that*
  the message is handled fresh by the normal cascade — that IS the correct behaviour.*
- *1 is a deliberate correctness choice, already documented inline: a scope-shaped
  question with an empty ledger must NOT go to the router, because a stray "unpaid"
  inside "does this include unpaid?" false-matches the unpaid_list route.*
- *2 are the genuine remainder — `dispatch_idle`'s READ/WRITE catch-all, and the
  SIDE_QUESTION handoff above. Owning the first means extracting the whole query cascade
  AND routing destructive WRITE_DELETE/WRITE_UPDATE on an LLM verdict with no legacy
  safety net, which is precisely what the dispatcher's docstring says shadow mode exists
  to avoid. Out of scope for "one branch at a time"; revisit only with the Phase 4
  evaluation harness in place to measure it.*

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

### 2.4 One TTL for everything ✅ DONE
- Added `is_timestamp_stale(iso_str, minutes=IDLE_TTL_MINUTES)` to
  `services/flow_machine.py` — one shared, conservative (missing/malformed
  timestamp = stale) policy, reused by:
  - `FlowMachine.expire_if_stale` itself, now implemented in terms of it.
  - `_handle_form_step`'s smart-capture form staleness check — deleted the
    hand-rolled 30-min check against the form's own `created_at`; delegates
    to `flow_machine.expire_if_stale()` directly (the form is always
    FlowMachine-tracked via `SMART_CAPTURE_CONFIRM_PENDING` since Phase
    2.3), with a defensive fallback straight to `is_timestamp_stale` for
    the should-never-happen case where FlowMachine and `form_state` have
    desynced.
  - The `last_generated_invoice` cache's own 30-min expiry — deleted the
    hand-rolled check, now calls `is_timestamp_stale` against `cached_at`
    directly (stays a standalone cache, not FlowMachine-owned state, since
    no flow ever arms it).
  - The `_ALL_AWAITING_CLEAR_PATCH` dict in the v2 TTL block (`_clear_flow_state`)
    was NOT deleted — post-Phase-2.3 it's pure `pending_*` payload cleanup
    (no boolean flags left in it at all), not a staleness check, and
    FlowMachine's own `reset()` doesn't touch those memory keys.
  - Two deliberate behavior changes from unifying onto the shared
    conservative policy: the form's staleness is now IDLE-based (resets on
    every retry, since FlowMachine's `started_at` refreshes on every
    `set_state()`) rather than a fixed lifetime cap from the form's
    original `created_at`; the invoice cache's missing/malformed `cached_at`
    is now stale immediately instead of silently treated as fresh forever.
  - Tests: `tests/test_ttl_unification.py`.

---

## Phase 3 — Fewer, cheaper, safer LLM calls ✅ DONE

Target: ≤2 LLM calls per normal turn (was 3–5, with 20–35s turns seen live).
**Met.** Measured per-turn cost today:

| Shape | Calls | Path |
|---|---|---|
| Router-covered (~15 common shapes: "how many jobs", "average fees", "total billing", "who's unpaid", …) | **1** | classifier only — `route_common_query` is pure regex, planner never runs, 3.1 skips synthesis |
| Planner-path scalar aggregate (filtered/date-scoped) | **2** | classifier + planner; 3.1 skips synthesis |
| Planner-path multi-row list | 3 | classifier + planner + synthesis (prose genuinely needs the LLM) |

### 3.1 Deterministic answers for aggregates — no synthesis call ✅ DONE
- `_deterministic_aggregates_enabled()` + `_is_single_scalar_aggregate()` in
  `services/intent_service.py`; wired into both the planner-result path and the
  deterministic-router path. `DETERMINISTIC_AGGREGATES=1` default-on escape hatch.
- Deliberately scoped to UNGROUPED scalars (`rows == [{"result": N}]`) — GROUP BY rows
  carry a dimension column and read better as LLM prose.
- **Tests:** `tests/test_deterministic_aggregates.py` (20).

### 3.2 Merge classify + plan for READ intents — ⏭️ INVESTIGATED, INTENTIONALLY SKIPPED
The premise above ("a READ query pays classifier → planner → synthesis") went stale
before this was picked up. Two things landed first that already capture the win:
- **3.1** removed the synthesis call for scalar aggregates.
- **`services/query_router.py`** (the deterministic router, a separate earlier change)
  answers the ~15 most common shapes with *zero* LLM calls beyond the classifier —
  already the "exactly ONE LLM call" ideal 3.2 was aiming for.

Building the shortcut anyway would be nearly all cost, no benefit:
- The router *already owns* every unfiltered aggregate, so a shortcut restricted to
  the safe (unfiltered) case would essentially never fire.
- The router deliberately **punts anything client- or date-qualified to the planner**
  (see `_has_scope_qualifier`) because those need compound business-rule reasoning —
  e.g. "how much does X owe me" must imply `paid='no'`; "se paisa aaya kya" must imply
  `paid='yes'`. Those rules live in the planner prompt, not the classifier's.
- The classifier's `parameters` is **not** plan-shaped except for the READ_AGGREGATE
  branch. `Plan.filters` is `Dict[str, CanonicalFilter]` (every value normalising to
  `NullCheck`/`BoolCheck`/`Equality`/`InList`/`Comparison`/`TextMatch` against the
  column registry); the classifier emits loose scalars under semantic keys, with no
  `operation`, no `limit`/`order`, and `field` naming a column without saying what to
  test on it. Closing that gap means teaching the classifier prompt the whole column
  registry — reintroducing precisely the "AI emits a shape we didn't anticipate →
  wrong SQL → wrong number shown to the user" bug class `PATH_3.md` exists to prevent.

The remaining 2-call case is already inside the 3.3 budget. Revisit only if profiling
shows planner latency actually hurting, and prefer **adding deterministic routes to
`query_router.py`** (same zero-LLM-trust pattern as the existing 15) over trusting
classifier params to build SQL.

### 3.3 Cap and monitor ✅ DONE
- **Tests:** `tests/test_llm_call_budget.py` (5) — runs the common query shapes through
  `process_request` with AI mocked and asserts `llm_calls <= 2` per turn off the real
  telemetry log line, plus a negative test proving the assertion actually fails at 3.

---

## Phase 4 — A real evaluation harness (~2–3 days, then it runs forever)

**Problem it solves:** all live testing so far ran against one shared WhatsApp account.
State leaked between runs and between scenarios; roughly HALF the "failures" in every
run were contamination, not bugs. That noise cost more time than the bugs themselves.

### 4.1 Seeded fixture account ✅ DONE
*`tests/e2e/seed.py` — synthetic `e2etest:<uuid>` account, 15 deterministic rows + bank details, prefix-guarded `teardown()` (guard runs BEFORE any DB connection opens). Expected values are DERIVED from the fixture so assertions can't drift from the data; dates are relative-with-fixed-offsets so "this month" scenarios don't rot. 28 offline tests in `tests/e2e/test_seed_guards.py` (not marked live, so they run in CI).*
- **New file:** `tests/e2e/seed.py`. Creates a fresh synthetic user id (e.g.
  `e2etest:<uuid>`), inserts ~15 deterministic `job_entries` rows covering: paid/unpaid,
  multiple clients, multiple months, a no-poc-email client, a no-job-date row, bank
  details in `user_config`. Returns the user id. Include a `teardown(user_id)` that
  deletes all rows for that id.
- Needs `SUPABASE_DB_URL` — skip (pytest `skipif`) when not set, so CI without secrets
  still passes.

### 4.2 Scenario runner with graded assertions ✅ DONE
*Corpus: the 29-message WhatsApp suite from CLAUDE.md (the `Intent_Test_Matrix` sheet
named in the plan was never in the repo). `tests/e2e/scenarios.py` holds the definitions,
`tests/e2e/test_scenarios.py` the runner. Run with `python -m pytest tests/e2e -m live --live -v`.*

*Two adaptations, both deliberate: client names are rewritten to the 4.1 fixture's three
clients (the original suite ran against a real account), and every numeric expectation is
DERIVED from FIXTURE_ROWS rather than copied from the sheet — the sheet's "should be 0"
was true of that account, not this fixture. Where the original expectation was vague
("a number") the assertion stays loose on purpose: over-specifying measures LLM wording,
not correctness.*

*State discipline: `requires="fresh"` (default) RE-SEEDS the account and wipes memory
first; `requires="after:<id>"` replays the prerequisite in the same memory for genuine
multi-turn tests (13 after 12, 20 after 19). Re-seeding is load-bearing, not caution —
scenario 1 writes a row and 20 updates one, so without it every later count assertion
would be off and the corpus would silently depend on execution order. Scenarios that
write are flagged `mutates=True` and the runner re-seeds after them too.*

*Nine of the scenarios carry `note=` linking them to the CLAUDE.md bug list (Bugs 1–5),
so a failure says WHY it matters rather than just which row broke.*

*A second, OFFLINE layer (`TestScenarioCorpusIsWellFormed`, 9 tests, runs in normal CI)
validates the corpus itself — ids contiguous, dependencies resolve and point backwards,
no chains deeper than one, every assertion is a real Assertion, write scenarios declared,
derived expectations still agree with the fixture. A typo in a definition fails CI in two
seconds instead of 40 minutes into a paid nightly run.*

### 4.3 Score tracking — ⏸️ BLOCKED on 4.2
*`run_assertions()` already emits JSON-serialisable `{assertion, passed, detail}` records shaped for `last_run.json`; needs the runner to exist before there's anything to score.*
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
