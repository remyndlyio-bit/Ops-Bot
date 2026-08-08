# RELIABILITY_ROADMAP.md — from 67% to 90%+, task by task

*Written 2026-08-08. Self-contained implementation guide: every task below carries its
evidence (matrix row numbers from `tests/e2e/matrix_results_20260807_2253.xlsx`), exact
code anchors (grep patterns, not raw line numbers — the file shifts), a concrete fix
shape, and acceptance criteria. An implementer should be able to pick any task and
complete it without reading anything else, though PLAN_OF_ACTION.md §9 has the deeper
architecture context.*

---

## 0. Ground rules (read once, apply to every task)

**Repo conventions (from CLAUDE.md):**
- Commit directly to main, message prefix `C C : `. No PRs, no feature branches.
- Every bug fix ships with a regression test in the same commit.
- Simplest fix first. Touch the fewest files. Prefer deleting over adding.

**Environment:**
- Hermetic test suite (no deps needed): `python3 -m pytest tests/ -q`
  - Expected baseline: **1930 passed, 8 failed, 30 skipped** with the system python
    (the 8 failures are `tests/test_live_llm_bugs.py` — stale call signatures, a
    separately-tracked fix; NOT caused by your change. If you see 9+ failures or any
    failure outside that file, you regressed something.)
- Real-deps venv (already built): `./venv/bin/python3 -m pytest tests/ -q`
- Live matrix run (paid AI calls + real DB writes under `e2etest:` users):
  ```bash
  FLOW_MACHINE_V2=true ./venv/bin/python3 -m tests.e2e.run_matrix
  ```
  Needs real `AI_KEY` and `SUPABASE_DB_URL` in the environment. Run it ONLY when a task
  says to; per-task acceptance uses the cheaper targeted runs below.
- Targeted matrix slice (cheaper): add `--category "Invoice Email"` etc. Categories:
  Onboarding, Job Entry, "Queries - Earnings", "Queries - Jobs", "Queries - Payments",
  "Invoice Generation", "Invoice Email", "Bank Details", "Profile & Settings",
  Reminders, "Small Talk", "Edge Cases", "Cross-Platform".

**Key files:**
- `services/intent_service.py` (~7700 lines) — dispatch cascade + all handlers.
  `_process_request_impl` is the main turn function; `_handle_query_request` is the
  extracted NL→SQL path; `_handle_form_step` handles active-form replies.
- `services/flow_dispatcher.py` — v2 dispatch (`dispatch_idle`, `dispatch_in_flow`).
- `services/flows.py` — 16 Flow classes + `REGISTRY` (per-flow `handle_response`,
  `on_cancel`, `resume_nudge`).
- `services/flow_machine.py` — FlowMachine state store (current_flow, context, TTL).
- `services/query_router.py` — deterministic message→SQL routes, `_ROUTES` order matters.
- `services/classifier.py` — LLM intent classifier (Verdict dict).
- `tests/e2e/` — matrix harness (`run_matrix.py`, `seed.py` fixture data).

**The one architectural fact that explains the failure data:** single-turn messages
pass at 80% (72/90); multi-turn replies ("Yes", "April", "cancel", "skip" after a bot
prompt) pass at 37% (14/38). "What is the bot waiting for" lives in three places —
legacy `awaiting_*` flags in the memory blob, FlowMachine state, and `form_state` —
and ~20 competing claim-sites in the cascade decide who gets the reply. Most tasks
below are narrow, safe repairs to that layer; task P2-1 is the structural fix.

---

## P0-A. Universal cancel — "cancel" must cancel, not explain

**Evidence:** matrix rows 13, 100, 111, 138 — four identical failures. User says
`cancel` during a form / bank prompt / link prompt / disambiguation, and the bot
replies with a canned *explanation* ("You can use 'cancel' to exit any ongoing flow")
instead of cancelling.

**Root cause:** the bare-cancel branch (grep anchor: `# 0b4b. A bare cancel/stop word`
in `services/intent_service.py`) assumes "every awaiting_*/pending_disambiguation/form
check above already had its own chance to consume it." That assumption is false: with
`FLOW_MACHINE_V2=true` (production default), several of those legacy checks are gated
off or the pending state is only in FlowMachine, so `cancel` falls through to 0b4b,
which thinks nothing is pending and replies with the no-op message. Meanwhile, the
actual pending state survives and swallows the NEXT message too.

**Fix shape (simplest that works):** make the 0b4b branch state-aware instead of
assuming. Before returning the no-op reply, check whether ANYTHING is actually
pending, and if so, clear it all and acknowledge:

```python
if msg_lower in ("cancel", "stop", "nevermind", "never mind", "nvm", "abort", "quit", "exit"):
    _something_pending = (
        self.memory.get_form_state(user_id)
        or user_mem.get("pending_disambiguation")
        or user_mem.get("pending_send_invoice")
        or self.flow_machine.current_flow(user_id) != FLOW_IDLE   # import FLOW_IDLE from services.flow_machine
        or any(user_mem.get(k) for k in user_mem if str(k).startswith("awaiting_"))
    )
    if _something_pending:
        self._clear_flow_state(user_id)          # already exists; clears flags + form + FlowMachine
        response = "Cancelled 👍 What else can I help with?"
        self._store_conversation(user_id, message, response)
        return {"operation": "cancelled", "response": response,
                "trigger_invoice": False, "invoice_data": {}}
    # ... existing no-op reply below stays for the genuinely-nothing-pending case
```

Note: `user_mem` at this point may be stale if a handler above wrote memory this turn;
re-fetch with `user_mem = self.memory.get_user_memory(user_id)` at the top of the
branch to be safe. `_clear_flow_state` already exists and does the full multi-store
clear — do NOT hand-roll a partial clear.

Also check `dispatch_in_flow` (`services/flow_dispatcher.py`): CANCEL verdicts route
to `flow.on_cancel(...)` per-flow — that path works. The failures happen when the
classifier does NOT emit `flow_compatible=CANCEL` (low confidence, or flow state only
in legacy flags), so the message falls to the cascade. The 0b4b fix is the backstop.

**Regression tests** (new file `tests/test_universal_cancel.py`, copy the
`_svc()`/`FakeMemory` pattern from `tests/test_handle_query_request_extraction.py`):
1. Arm a form (`svc.memory.start_form(...)`), send "cancel" → operation == "cancelled",
   `get_form_state` returns None afterward.
2. Set `pending_disambiguation` in memory, send "cancel" → cancelled + flag cleared.
3. Nothing pending, send "cancel" → the existing `no_op_cancel` reply (don't break it).
4. After a cancel, the NEXT message routes normally (send "how many jobs" with mocked
   `execute_sql`; assert it's not swallowed).

**Acceptance:** the 4 tests pass; full suite has no new failures; matrix rows 13, 100,
111, 138 pass on a targeted re-run of their categories.

---

## P0-B. Invoice-email sub-flow — one missing prompt poisons four turns

**Evidence:** rows 86, 87, 89, 90, 91, 92, 97 (Invoice Email category: 1/8 = 12%,
worst in the product). The chain: row 89 "Send invoice for Nike (no poc_email)"
should ask for the client's email but doesn't → rows 90-92 (user supplies email /
bad email / "skip") all land in `invoice_month_retry` ("I couldn't detect a month")
because the bot is still in month-selection state from earlier. Row 97 ("Send that to
the client") also drowns in `invoice_month_retry`.

**Root cause (two parts):**
1. The email-collection state never arms. Grep `awaiting_invoice_poc_email` in
   `services/intent_service.py` — comments at the top of the file say its legacy arm
   site was removed during the v2 migration ("awaiting_invoice_poc_email removed (its
   arm site, ...)"), and the v2 replacement (`InvoiceReadinessPocEmail` /
   `InvoiceNeedPocEmail` flows in `services/flows.py`) is only entered from paths that
   row 89's message doesn't hit. Result: "send invoice to client" for a client with no
   poc_email generates the PDF (or tries) and never asks for an address.
2. The month-await state (`_arm_invoice_month_v2`, grep it) has no escape hatch: once
   armed, EVERY subsequent message gets the "couldn't detect a month" retry, including
   emails and "skip". Find the handler that produces `"operation": "invoice_month_retry"`
   (grep `invoice_month_retry`): it must (a) treat `skip`/`cancel` as abort-and-clear,
   (b) detect an email-shaped reply (`self._is_valid_email(message.strip())`) and at
   minimum say "that looks like an email — I was asking which month" instead of the
   canned month retry, and ideally hand off to the email flow if one is pending.

**Fix order:** do part 2 first (containment — stops one wrong state from eating four
turns), then part 1 (arm the email ask). For part 1, trace row 89's actual path:
`_handle_invoice_retrieval_request` (grep the def) is the entry; find where it decides
the invoice is ready to send/email and add the check: if the target job's `poc_email`
is empty and the user asked to SEND (not just generate), arm the poc-email flow the
same way other flows arm (`self.flow_machine.set_state(...)` — copy the arm pattern
from `_arm_invoice_month_v2`, which is the reference implementation for a correctly
v2-armed prompt) and return the ask ("What's the client's email address? ...").

**Regression tests** (extend `tests/test_flow_invoice_gates.py` or new file):
1. Month-state armed + email-shaped reply → NOT the canned month retry.
2. Month-state armed + "skip" → state cleared, friendly abort.
3. Send-invoice request, target job has no poc_email → response asks for email AND
   FlowMachine current_flow is the poc-email flow.
4. Then reply with a valid email → email saved (mock `supabase`), flow advances.

**Acceptance:** tests pass; no new failures; matrix "Invoice Email" category ≥5/8 on a
targeted run (`--category "Invoice Email"`).

---

## P0-C. Defaulted-date disclosure misfires after the date is provided

**Evidence:** row 15 — user answers a missing-fields prompt with "10 April, dubbing".
Bot correctly parses `2026-04-10`, but the confirmation card STILL shows
"(today — no date in your message; reply with the real date to change)".

**Root cause (confirmed by code read):** in `_extract_and_confirm` (grep the def), when
job_date is missing on FIRST extraction it sets `extracted["_job_date_defaulted"] = True`
and stores it in the form. Later, `_handle_smart_capture_missing` (grep the def) merges
new fields via `for k, v in new_data.items(): if v is not None: extracted[k] = v` —
job_date gets overwritten with the real parsed date, but the stale
`_job_date_defaulted` flag is never cleared, so `_job_date_display_value` (grep it)
still appends the caveat.

**Fix (2 lines):** in `_handle_smart_capture_missing`, immediately after the merge loop:

```python
if new_data and new_data.get("job_date"):
    extracted.pop("_job_date_defaulted", None)
```

**Regression test** (add to `TestSmartCaptureDefaultedDateDisclosure` in
`tests/test_smart_capture_flow.py`, which already tests this feature):
- Start capture with no date (flag set) → supply "10 April, dubbing" via the
  missing-fields path → assert the confirmation text does NOT contain "no date in
  your message" and DOES contain the parsed date.

**Acceptance:** test passes; the 6 existing tests in that class still pass.

---

## P1-A. Ontology routes — unpaid ≠ unsent ≠ overdue

**Evidence:** rows 56, 60, 61, 122. "Show unpaid invoices" returns rows whose invoice
was never SENT; "overdue" ignores the 30-day terms; "follow up on payments" and
"remind clients about payments" miss the overdue handler.

**Root cause:** `services/query_router.py`'s `_route_unpaid_list` maps every unpaid-ish
phrase to `paid='No'` only. The schema distinguishes `bill_sent` (invoice sent?),
`invoice_date` (when), `paid` (settled?) — the router's vocabulary doesn't.

**Fix shape:** in `services/query_router.py`:
1. New route `_route_unpaid_invoices` — trigger: message contains "invoice"/"bill" AND
   an unpaid word ("unpaid", "not paid", "outstanding", "pending payment"). SQL adds
   `AND bill_sent = 'Yes' AND paid = 'No'` (copy quoting/`isDeleted` guard style from
   `BILL_NOT_SENT`). Scope: `{"filters": {"bill_sent": "yes", "paid": "no"}, "time_range": None}`.
   ORDER it BEFORE `_route_unpaid_list` in `_ROUTES` (first match wins), and make
   `_route_unpaid_list`'s trigger NOT fire when "invoice"/"bill" is present.
2. New route `_route_overdue` — trigger: "overdue" or "past due" or ("due" + "date").
   SQL: `... AND bill_sent = 'Yes' AND paid = 'No' AND invoice_date IS NOT NULL AND
   invoice_date < (CURRENT_DATE - INTERVAL '30 days')`. Scope filters:
   `{"bill_sent": "yes", "paid": "no", "overdue": "yes"}`.
3. Rows 61/122 ("follow up on payments", "remind clients"): these should hit the
   EXISTING overdue handler in `_process_request_impl` (grep `# 3. Overdue / payment
   followup`). Row 122 was classified FEATURE_QUESTION by v2 and answered as a
   capability description. Fix in `services/flow_dispatcher.py` `dispatch_idle`: before
   the FEATURE_QUESTION branch returns, if the raw message matches the manual-reminder
   pattern (grep `_wants_remind` in intent_service.py for the exact regex — reuse it,
   don't re-derive), return SHADOW_ONLY so the legacy overdue handler answers.

**Regression tests:** add a `TestOntologyRoutes` class to `tests/test_query_router.py`
(follow `TestRouteScope` pattern): "show unpaid invoices" → SQL contains both
`bill_sent = 'Yes'` and `paid = 'No'`; "show overdue invoices" → SQL contains the
interval comparison; "kiska payment baki hai" still hits the OLD unpaid route
(no bill_sent clause — Hinglish regression guard); scope dicts assert exact shape.

**Acceptance:** tests pass; matrix rows 56/60/61/122 pass on targeted "Queries -
Payments" + Reminders runs; rows 9/57 (existing unpaid passes) still pass.

---

## P1-B. Flip `resolved_query` from shadow to authoritative (P2-1)

**Evidence:** rows 70, 140, 141 — "Generate invoice for them", bare "Generate invoice"
after a Nike conversation, "This month" after a client question. The classifier
already computes a `resolved_query` (the message rewritten with context, e.g.
"Generate invoice for Nike") but it is logged and thrown away.

**Where:** `services/intent_service.py`, grep `UNDERSTAND_V2_SHADOW`. The verdict dict
carries `resolved_query` and `references_last_answer`. The shadow log line compares
the classifier vs. the regex (`answer_ledger_is_scope_question`) — that telemetry now
exists in the matrix run logs (`/tmp/matrix_run_week5_final.log`: every
`[UNDERSTAND_V2_SHADOW]` line showed `agree=True`).

**Fix shape (conservative flip):** immediately after the shadow log, add:

```python
_rq = (_verdict.get("resolved_query") or "").strip()
if (_rq and _rq.lower() != raw_message_lower  # actually rewrote something
        and (_verdict.get("confidence") or 0) >= 0.8
        and len(_rq) < 200):
    logger.info(f"[UNDERSTAND_V2] using resolved_query: {message!r} -> {_rq!r}")
    message = _rq
    msg_lower = message.strip().lower()
```

Gate it behind an env flag `RESOLVED_QUERY_AUTHORITATIVE` (default `"1"`, escape hatch
`"0"`) using the exact pattern of `STRICT_PLAN_VALIDATION` (grep it in
`services/query_planner.py` for the reference). Apply BEFORE `dispatch_idle` is called
so both v2 branches and the legacy fallback see the resolved message.

**Risk & guard:** a hallucinated rewrite corrupts a good message. The confidence gate
plus "must differ from original" plus the flag cover it. Do NOT apply when a flow is
active (`dispatch_in_flow` path) — flow replies like "April" must stay verbatim.

**Regression tests** (new `tests/test_resolved_query_authoritative.py`): stub
`services.classifier.classify` to return a verdict with
`resolved_query="Generate invoice for Nike"` for input "generate invoice for them";
assert the invoice handler receives the resolved text. Second test: flag `"0"` →
original message used. Third: confidence 0.5 → original used.

**Acceptance:** tests pass; matrix rows 70/140/141 pass on targeted Edge Cases +
Invoice Generation runs; rows 64/66/71/72 (invoice happy paths) still pass.

---

## P1-C. Decline acknowledgment ("No" / "Maybe later" closes the thread)

**Evidence:** rows 128, 129 — after the bot asks "Would you like...?", the user says
"No" / "Maybe later" and gets the generic greeting ("Hey 👋 — what's on your plate
today?") instead of a simple acknowledgment.

**Root cause:** the legacy negative-intent branch (grep `_NEGATIVE_RESPONSES` in
`services/intent_service.py`) handles exactly this — but with FLOW_MACHINE_V2 on, the
classifier claims these messages first as SMALL_TALK and `dispatch_idle`'s SMALL_TALK
branch (in `services/flow_dispatcher.py`) returns the canned greeting, so the cascade
branch is never reached.

**Fix shape:** in `dispatch_idle`'s SMALL_TALK branch, before calling
`_detect_small_talk`: if the raw message (lowered, stripped) is in a decline set
(reuse `_NEGATIVE_RESPONSES` — import or hoist it to module level in intent_service
and reference it, don't duplicate the list) AND the last assistant message in
`conversation_history` contains a follow-up marker (reuse `_FOLLOWUP_MARKERS`, same
hoisting), return a short acknowledgment instead:

```python
{"operation": "decline_ack", "response": "No problem 👍", "trigger_invoice": False, "invoice_data": {}}
```

**Regression tests** (extend `tests/test_dispatch_idle_write_intents.py` pattern in a
new class): SMALL_TALK verdict + history ending in "Would you like a breakdown?" +
message "no" → decline_ack; same verdict + history ending in a normal answer +
message "no" → falls through to normal small talk (greeting is fine there).

**Acceptance:** tests pass; rows 128/129 pass on a targeted "Small Talk" run; rows
123-127, 130-131 (greetings) still pass.

---

## P1-D. Ground the matrix judge in today's date (eval fix — stop losing ~4 points to noise)

**Evidence:** rows 46 and 51 failed with "the bot returned 2026 instead of the current
year" — 2026 IS the current year. Both were 2-of-3 split votes. Row 11 and 21 also
encode expectation mismatches (see P2-B/P2-C).

**Fix:** `tests/e2e/grade.py` (grep the prompt template inside `grade()`): add one line
to the judge prompt: `Today's date is {date.today().isoformat()}. The current year is
{date.today().year}.` Also add the instruction: "If the bot's dates are consistent
with today's date, do not fail it for the year."

**Acceptance:** `./venv/bin/python3 -m tests.e2e.run_matrix --regrade
tests/e2e/matrix_results_20260807_2253.xlsx --out /tmp/regraded.xlsx` (regrade mode:
re-judges stored replies, no bot calls, no DB) — rows 46/51 flip to PASS, and no
previously-passing row flips to FAIL for date reasons. The harness self-tests
(`python3 -m pytest tests/e2e/ -q`) stay green.

---

## P2-A. The structural fix: one typed "expected reply" per prompt (do AFTER all P0/P1)

This is the redesign that takes multi-turn from ~37% to 90%+. Do not start it until
the P0/P1 patches above are merged and a fresh matrix run confirms ~80%+, because it
touches the same code paths and you want a clean baseline.

**Principle:** every time the bot sends a message that EXPECTS a reply (a question, a
confirmation, a menu), the sending site must arm exactly ONE FlowMachine state naming
(a) the flow, (b) the expected reply type (yes_no / month / email / number_pick /
free_text), and (c) the context to resume. The next turn checks FlowMachine FIRST —
before small talk, before the router, before the planner. `cancel`/`skip`/`stop` are
handled by the state machine generically (P0-A already gives you the backstop).

**Migration recipe, one flow at a time (repeat ~6 times):**
1. Pick the worst remaining flow from the matrix (after P0-B, likely Reminders —
   rows 116-118: the WhatsApp reminder list's "send all"/"1"/"skip" replies).
2. Find where the prompt is SENT (for reminders: grep `pending_reminder_offer` — the
   overdue handler stores it but nothing arms a FlowMachine state, which is why the
   replies fall through).
3. Add/extend a Flow class in `services/flows.py` (copy `InvoiceNeedMonth` — it's the
   cleanest reference: `handle_response` parses the reply, `on_cancel` clears,
   `resume_nudge` re-prompts). Register it in `REGISTRY`.
4. Arm it at the prompt site: `self.flow_machine.set_state(user_id, FLOW_X, context)`.
5. Delete the legacy `awaiting_*` flag for that flow ONLY after the matrix rows for
   that flow pass through the new path (check `[V2_DISPATCH]` log lines confirm
   `dispatch_in_flow` handled them).
6. Regression tests per flow: reply lands in `handle_response`; cancel works; an
   unrelated message mid-flow (a side question) doesn't destroy the flow.

**Order of migration by matrix impact:** Reminders (3 rows) → "More details" yes/no
(row 48) → Edit-during-confirmation (row 12) → Payment-received disambiguation
(row 59: after P0-A, "Payment received from Nike" asking "which one?" is fine, but
the numbered reply must work) → remaining stragglers.

**Hard rule:** never migrate two flows in one commit. Full suite + targeted matrix
category between each.

---

## P2-B. Bill numbers at save time (product decision required)

Row 11 expects "Saves to DB, confirms with bill number". The product currently
assigns `bill_no` at invoice time, not job-save time. Either (a) auto-assign a
sequential bill_no on insert (schema + `_handle_smart_capture_*` + invoice generation
dedupe implications — real work), or (b) change the matrix expectation to match the
design. Decide with the product owner; do not silently pick (a).

Row 65 ("Generate invoice for bill BB2") is related: the regex matches BB2 but the
lookup finds nothing — check whether `tests/e2e/seed.py` fixture rows carry `bill_no`
values at all; if not, this is a FIXTURE gap, and the fix is seeding a row with a
bill_no and re-running, before touching product code.

## P2-C. Compound-intent expectation (row 21)

"Add job ... and send invoice" — bot confirms the save first (by design, the
suggested_next_action fires AFTER "Yes"). The sheet expects the invoice offer in the
same turn. Either append the pending-action note to the confirmation card ("After
saving, I'll ask about: send invoice") — one line in
`_show_smart_capture_confirmation` — or fix the sheet. The one-liner is cheap and
honest; prefer it.

## P2-D. Operational leftovers (not code)

- **Rotate Railway's `AI_KEY`** — the old key died 2026-08-06 (401 "User not found").
  The local shell was fixed on 2026-08-07; production Railway still needs the new key
  set in the Variables tab. Until then the LIVE bot's AI calls fail.
- `tests/test_live_llm_bugs.py` stale signatures (8 permanently-failing tests) — call
  `build_operation_plan` with its current signature (see the def in
  `services/query_planner.py`); a spawned task for this already exists.
- Latency: the plan's p50<3s target has never been re-measured post-pool/cache.
  Matrix telemetry lines (`[TELEMETRY] turn_ms=...`) in `/tmp/matrix_run_week5_final.log`
  are a free dataset — p50 there was ~4-5s with llm_calls=1 dominating.

---

## Scorecard math (what each fix is worth, on the 128-graded-row denominator)

| Task | Rows fixed | Cumulative expected |
|---|---|---|
| baseline (2026-08-07 run) | — | 86 (67.2%) |
| P0-A universal cancel | 13, 100, 111, 138 | 90 (70%) |
| P0-B invoice email | 86, 87, 89, 90, 91, 92, 97 (some partial) | ~95-97 (~75%) |
| P0-C date disclosure | 15 | ~97 (~76%) |
| P1-A ontology | 56, 60, 61, 122 | ~101 (~79%) |
| P1-B resolved_query | 70, 140, 141 (+ maybe 79) | ~104 (~81%) |
| P1-C decline ack | 128, 129 | ~106 (~83%) |
| P1-D judge grounding | 46, 51 (+ 142 likely) | ~108-109 (~85%) |
| P2-A state redesign | 12, 48, 59, 116, 117, 118, 45 + stragglers | **115+ (90%+)** |

Numbers are honest estimates, not promises — cascaded rows (P0-B) may not all clear
until P2-A. Re-run the FULL matrix after P1-D and after each P2-A flow migration;
record every run's workbook in `tests/e2e/` with the config fingerprint it stamps.
