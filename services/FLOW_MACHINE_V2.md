# FlowMachine v2 — migration map

Living doc for the architectural refactor described in the chat thread that
landed this directory's `classifier.py` + `flow_dispatcher.py`. Sessions are
shipped behind the `FLOW_MACHINE_V2=true` env var so production stays on the
legacy path until each session is verified.

## What this is for

The legacy `intent_service.process_request` does intent classification 5+
times per message (keyword regexes, multiple AI calls, planner classification).
Each mini-classifier has its own heuristic and writes to a shared bag of
`awaiting_*` flags. Recurring bugs (off-brand fallbacks, hallucinated columns,
sticky form state, duplicate prompts) trace back to this design.

v2 replaces the cascade with:
  1. ONE classifier (`services/classifier.py`) returning a typed `Verdict`.
  2. ONE dispatcher (`services/flow_dispatcher.py`) routing the Verdict.
  3. (sessions 2+) ONE flow machine owning user state transitions.

## Session 1 — landed

**Scope**: Replace fresh-message (IDLE-state) classification with one AI call.

**Touched**:
- NEW `services/classifier.py` — Verdict type + classify() + prompt + parser tests.
- NEW `services/flow_dispatcher.py` — dispatch_idle() handling SMALL_TALK,
  FEATURE_QUESTION, UNKNOWN. Read/write intents return SHADOW_ONLY so legacy
  code keeps running — we get verdict telemetry without behaviour change.
- `services/intent_service.py` (~50 lines added at top of process_request):
  flag check + idle check + v2 call + fall-through on shadow result.

**Behind**: `FLOW_MACHINE_V2=true` env var. Default off.

**Wins (when flag is on)**:
- Off-brand "I'm a spreadsheet assistant" clarifications can't leak through —
  UNKNOWN now routes to `answer_feature_question` (REMYNDLY_FEATURES.md grounded).
- "Are you back?" / similar small talk handled before the planner ever runs.
- Smart-capture can no longer trigger on a READ_QUERY question — the classifier
  separates them with explicit definitions.

**Telemetry**: every classification logs
  `[CLASSIFIER] intent=… conf=… hist=… bulk=… params=…`
so we can see in production whether intent distribution matches expectations.

## Session 2 — landed

**Scope shipped**: FlowMachine + first flow migrated end-to-end. The other
two from the original plan (`INVOICE_NEED_*`, `SMART_CAPTURE_*`) deferred to
session 2.5 / 3 to keep this session's blast radius contained.

**Touched**:
- NEW `services/flow_machine.py` — single owner of v2 state. Methods:
  `get_state`, `set_state`, `update_context`, `push`, `pop`, `reset`,
  `expire_if_stale`. Persists via `MemoryService` under `flow_v2` key.
  Stack depth capped at 2. 30-min idle TTL applied at entry of
  `process_request`.
- NEW `services/flows.py` — `Flow` base class + `InvoiceAwaitSendConfirm`
  concrete flow. `handle_response` / `on_cancel` delegate to existing
  `intent_service._handle_send_confirmation` (reuse, not rewrite). After
  flow completes, `flow_machine.reset(user_id)` clears v2 state.
- `services/classifier.py` — `Verdict` extended with `flow_compatible`
  field (FLOW_RESPONSE | SIDE_QUESTION | NEW_FLOW | CANCEL | null).
  `classify()` accepts `current_flow` + `current_context`; the prompt
  includes a per-flow guidance block when in-flow.
- `services/flow_dispatcher.py` — new `dispatch_in_flow()` routes the four
  compatibility cases. Owned today: CANCEL, FLOW_RESPONSE,
  SIDE_QUESTION-for-FEATURE_QUESTION. Shadow today: SIDE_QUESTION-for-READ
  and NEW_FLOW (legacy keeps deciding).
- `services/intent_service.py` — `IntentService.__init__` initialises
  `self.flow_machine`. `process_request` checks TTL, then routes to
  `dispatch_in_flow` when in an owned flow, or `dispatch_idle` when IDLE
  (existing session 1 path).
- `main.py` — `process_and_send_invoice` mirrors `awaiting_send_confirmation`
  state into `flow_machine.set_state(INVOICE_AWAIT_SEND_CONFIRM, …)` so
  dispatch_in_flow can recognise it. Legacy flag still drives behaviour;
  FlowMachine is a parallel writer.

**Behind**: same `FLOW_MACHINE_V2=true` env var.

**What changes when flag is on** (vs session 1 alone):
- After a v2-mirrored invoice flow arms, a user's "yes" / "no" / "skip" is
  classified WITH flow context — the AI sees the active flow + context
  and returns `flow_compatible: FLOW_RESPONSE | CANCEL`. Same delegated
  handler runs, but the classification is auditable in one log line.
- Stale flows (30 min idle) auto-reset on next message, so users aren't
  trapped in old state from yesterday's interaction.
- "what was Garnier's fee?" while in invoice confirm → classifier marks
  `flow_compatible: SIDE_QUESTION`. For FEATURE_QUESTION side asks the
  answer comes with a flow-resume nudge appended. For READ side asks we
  shadow to legacy in session 2 (typed plan layer in session 3 will own).

**Tests in repo** (run with `python3 -c`):
- 8 FlowMachine state paths (set/update/push/pop/TTL/unknown).
- 8 classifier parser paths (incl. `flow_compatible` values).
- 1 MemoryService round-trip persistence.

**Telemetry additions**: `[FLOW_V2] set_state → ... ctx_keys=...`,
`[FLOW_V2] popped → resumed ...`, `[V2_DISPATCH] in_flow=...`,
`[CLASSIFIER] ... fc=...` on every message.

## Session 2.5 — landed

**Scope shipped**: 5 more flows migrated, bringing v2 to **6 owned flows**
(was 1 after session 2). All multi-turn user flows now have a Flow class.

**Touched**:
- `services/flow_machine.py` — added 5 new flow constants and grew
  `KNOWN_FLOWS`. New ownership:
    INVOICE_NEED_BILLING
    INVOICE_NEED_POC_NAME
    INVOICE_NEED_POC_EMAIL
    SMART_CAPTURE_NEED_DESCRIPTION
    SMART_CAPTURE_CONFIRM_PENDING
- `services/flows.py` — 5 new Flow classes added to REGISTRY. Same
  delegation pattern as session 2: `handle_response` and `on_cancel`
  call the existing `_handle_*_response` / `_extract_and_confirm` /
  `_handle_form_step` methods, then `flow_machine.reset(user_id)`.
  SmartCaptureNeedDescription also transitions into
  SMART_CAPTURE_CONFIRM_PENDING after a successful extract, so the
  two-step add-job flow stays consistent on both sides.
- `services/classifier.py` — per-flow guidance blocks for each of the
  5 new flows in `_flow_compat_block`, telling the AI what counts as
  FLOW_RESPONSE / CANCEL / SIDE_QUESTION for each.
- `services/intent_service.py` — new method
  `_reconcile_legacy_to_flow_machine` runs once per message at the
  top of `process_request` (when v2 is enabled). If FlowMachine is
  IDLE but a legacy `awaiting_*` flag is armed, it syncs FlowMachine
  to match. This avoids touching 10+ legacy arm sites individually.
  Stale-flow TTL cleanup extended to clear ALL 6 legacy flag groups,
  not just `awaiting_send_confirmation`.

**Behavioural effect when flag is on** (vs session 2 alone):
- The "skip" → junk Redmi job bug class is permanently dead for the
  smart-capture flow. The Flow's `handle_response` is called only when
  the classifier sees `flow_compatible: FLOW_RESPONSE`; "skip" routes
  to `on_cancel` which clears `awaiting_job_input` + resets v2.
- Same for the 3 invoice-detail-collection flows. Each understands
  what "skip" means in its own context.
- Side questions during any of the 6 owned flows now get the right
  resume-nudge — "Still waiting on the {client} contact email…", etc.
- 30-min idle TTL applies uniformly across all 6 owned flows.

**Tests in repo**:
- All 6 flows registered in KNOWN_FLOWS.
- All 6 Flow classes have full surface (handle_response / resume_nudge
  / on_cancel) and resume_nudge is safe to call standalone.
- Per-flow classifier guidance appears in built prompts for each.
- MemoryService round-trip persistence verified for all 6 flow names.
- set_state still rejects unknown flow names.

## Session 3 — planned

**Scope**: Read-side SIDE_QUESTION ownership + typed query plan + cleanup.

- Take ownership of `SIDE_QUESTION` for `READ_QUERY` / `READ_AGGREGATE` —
  needs the typed query plan to land first so we can run a read without
  going through the full legacy pipeline.
- Implement `NEW_FLOW` push/pop properly now that all flows live in
  FlowMachine.
- Replace `query_planner.py`'s free-form JSON output with a typed `Plan`
  dataclass that goes through a schema validator BEFORE it ever reaches
  SQL generation (kills the `bill_sent` hallucination class).
- Migrate remaining flag-bag flows: onboarding, bank-details,
  disambiguation, audit reply, reminder reply.
- Delete `awaiting_*` flags entirely. Delete the intent-shift guard
  (subsumed by `flow_compatible`). Delete the smart-capture trigger
  keyword list.

**Expected net code reduction**: ~30-40% smaller `intent_service.py`.

## Rollback

Any session: `unset FLOW_MACHINE_V2` (or set to `false`) on Railway and restart.
Legacy code path is preserved end-to-end until session 3 starts deleting it.
