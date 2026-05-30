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

## Session 2 — planned

**Scope**: Migrate the 3 most-used flows from `awaiting_*` flag bag to an
explicit `flow + context` state owned by a FlowMachine class.

**Order** (by frequency, easy → hard):
1. `INVOICE_AWAIT_SEND_CONFIRM` — currently `awaiting_send_confirmation` +
   `pending_send_invoice`. Smallest state surface, one Yes/No transition.
2. `INVOICE_NEED_BILLING` / `INVOICE_NEED_POC_NAME` / `INVOICE_NEED_POC_EMAIL` —
   the three "missing detail" prompts during invoice generation.
3. `SMART_CAPTURE_NEED_DESCRIPTION` + `SMART_CAPTURE_CONFIRM_PENDING` — the
   add-job form. Sticky-state bug from session 1 fixes here permanently.

For each: classifier returns `Verdict`; new `flow_compatible` field tells
dispatcher whether to consume as FLOW_RESPONSE, queue as SIDE_QUESTION (read
inline + stay in flow), push as NEW_WRITE_FLOW, or CANCEL.

**Defaults agreed**:
- Side-question UX: push/pop (answer inline, then nudge back to current flow).
- Idle-flow TTL: 30 minutes of silence → auto-reset to IDLE with a "I'd dropped
  that, want to restart?" greeting.
- Cancel: both natural language (`skip`, `cancel`, `nevermind`) AND a `/cancel`
  Telegram command.

## Session 3 — planned

**Scope**: Remaining flows + typed query plan.

- Migrate onboarding, bank-details, disambiguation, audit reply, reminder reply.
- Replace `query_planner.py`'s free-form JSON output with a typed `Plan` dataclass
  that goes through a schema validator BEFORE it ever reaches SQL generation.
- Delete `awaiting_*` flags. Delete the intent-shift guard (subsumed by
  `flow_compatible`). Delete the smart-capture trigger keyword list.

**Expected net code reduction**: ~30-40% smaller `intent_service.py`.

## Rollback

Any session: `unset FLOW_MACHINE_V2` (or set to `false`) on Railway and restart.
Legacy code path is preserved end-to-end until session 3 starts deleting it.
