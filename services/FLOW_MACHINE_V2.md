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

## WP-3 slices 1–3 — landed

**Scope shipped**: 6 more flows migrated (bringing v2 to 12 owned flows total),
plus the pre-classifier keyword cascade gated behind v2 (TODO.md Phase 1.1–1.4).

- New flows: `DISAMBIGUATION`, `BANK_DETAILS`, `NAME_CHANGE`, `LINK_ACCOUNT`,
  `INVOICE_ADDRESS`, `INVOICE_NEED_JOB_DESCRIPTION` — all with concrete
  `Flow` classes in `services/flows.py`, same delegation pattern as session
  2.5 (wrap the existing `_handle_*` method; no rewrites).
- `services/classifier.py` — `AUDIT_REPLY` intent added; audit-pending
  context passed into the classifier so audit replies are classified
  instead of keyword-hijacked.
- `services/intent_service.py` — the ~10 pre-classifier keyword checkpoints
  (small-talk, invoice check, add_job, modify, bank/name/link commands) are
  now each gated on `not _flow_machine_v2_enabled_for(user_id)`, so v2's
  classifier gets first look at every message when the flag is on.
  `_reconcile_legacy_to_flow_machine` extended to cover all 12 flows.

## Phase 1.5 — landed (partial)

**Scope shipped**: two of `flow_dispatcher.py`'s `SHADOW_ONLY` branches
replaced with real handling; the rest deliberately deferred (see below).

- `SIDE_QUESTION` for `READ_QUERY` / `READ_AGGREGATE`: tried against the
  deterministic router (`services/query_router.route_common_query`) first —
  zero LLM calls. A match answers inline WITH the flow's `resume_nudge`
  (previously lost). Scope-clarifying questions ("does this include paid
  and unpaid?") are checked FIRST via `answer_ledger.answer_scope_question`
  so a stray keyword like "unpaid" can't false-match the router. No router
  match (or a WRITE-shaped side question) still shadows to legacy exactly
  as before — the LLM planner is untouched.
- `NEW_FLOW`: a high-confidence (≥0.7) verdict now applies the classifier's
  decision directly — clears the current flow's legacy mirror flags via
  the new `IntentService._clear_flow_state()` helper — instead of letting
  legacy's intent-shift guard ask the SAME question again via its own LLM
  call (`gemini_service.is_new_query_not_response`). Saves one LLM call per
  turn on this path. Low-confidence verdicts are untouched.
- **NOT done**: real push/pop for `NEW_FLOW` (resume the abandoned flow
  later with a nudge) and full `SIDE_QUESTION`/read-write ownership in
  `dispatch_idle`. Both need every flow's completion point to know how to
  pop back — see "Session 3 — remaining" below.

## Session 3 — remaining

**Scope**: typed query plan + real push/pop + final cleanup.

- Implement `NEW_FLOW` push/pop properly (the FlowMachine `push()`/`pop()`
  API already exists, stack capped at 2) — requires wiring a pop-check into
  every flow's completion point, which Phase 1.5 deliberately deferred.
- Take full ownership of `SIDE_QUESTION` / `dispatch_idle`'s READ intents
  beyond what the deterministic router covers — needs the typed query plan
  (below) so a read can run without the full legacy pipeline.
- Replace `query_planner.py`'s free-form JSON output with a typed `Plan`
  dataclass that goes through a schema validator BEFORE it ever reaches
  SQL generation (kills the `bill_sent` hallucination class). Note: Path 3
  (`services/plan.py`, see `PATH_3.md`) already ships this for the
  canonical-filter layer — this item is about extending that discipline to
  the rest of the plan shape.
- Migrate remaining flag-bag flows not yet in FlowMachine: onboarding,
  `awaiting_invoice_month`, `awaiting_compound_response`,
  `awaiting_modify_field`, `awaiting_invoice_poc_email` (see the state
  inventory below for what each does).
- Delete `awaiting_*` flags entirely. Delete the intent-shift guard
  (subsumed by `flow_compatible`). Delete the smart-capture trigger
  keyword list.

**Expected net code reduction**: ~30-40% smaller `intent_service.py`.

## Rollback

Any session: `unset FLOW_MACHINE_V2` (or set to `false`) on Railway and restart.
Legacy code path is preserved end-to-end until session 3 starts deleting it.

---

## State inventory (TODO.md Phase 2.1)

Every legacy conversational-state store as of this writing, what it holds,
and its FlowMachine v2 equivalent (or lack of one). This is the map Phase
2.2–2.4 execute against: "flip one flow at a time" means picking a row
below, making FlowMachine the writer, and deleting the legacy mirror once
every reader is ported.

### 1. `awaiting_*` boolean flags (`IntentService._AWAITING_FLAGS` + 2 more)

`_arm_awaiting()` clears every OTHER flag in `_AWAITING_FLAGS` before
setting one — flags are mutually exclusive by construction. 10 of 14 are
already mirrored into FlowMachine via `_reconcile_legacy_to_flow_machine`;
4 are legacy-only with no FlowMachine flow yet.

| Legacy flag | Companion `pending_*` keys | FlowMachine flow | Status |
|---|---|---|---|
| ~~`awaiting_send_confirmation`~~ | `pending_send_invoice` (payload, stays) | `INVOICE_AWAIT_SEND_CONFIRM` | ✅✅ **Deleted** (Phase 2.3) — FlowMachine is the sole source of truth; no legacy flag, no reconciliation branch, no mirror. First flow to complete the full migration. |
| `awaiting_client_billing` | `pending_billing_client`, `pending_billing_user_id` | `INVOICE_NEED_BILLING` | ✅ Mirrored + owned (2.5) |
| `awaiting_poc_name` | `pending_poc_client`, `pending_poc_user_id`, `pending_poc_row_ids` | `INVOICE_NEED_POC_NAME` | ✅ Mirrored + owned (2.5) |
| `awaiting_poc_email` | `pending_send_invoice`, `poc_email_client` | `INVOICE_NEED_POC_EMAIL` | ✅ Mirrored + owned (2.5) |
| `awaiting_job_input` | — | `SMART_CAPTURE_NEED_DESCRIPTION` | ✅ Mirrored + owned (2.5) |
| `awaiting_bank_details` | — | `BANK_DETAILS` | ✅ Mirrored + owned (WP-3.2) |
| `awaiting_name_change` | — | `NAME_CHANGE` | ✅ Mirrored + owned (WP-3.2) |
| `awaiting_link_id` | — | `LINK_ACCOUNT` | ✅ Mirrored + owned (WP-3.2) |
| `awaiting_invoice_address` | `pending_invoice`, `pending_address_user_id` | `INVOICE_ADDRESS` | ✅ Mirrored + owned (WP-3.3) |
| `awaiting_job_description` | `pending_jobdesc_row_id`, `pending_jobdesc_user_id` | `INVOICE_NEED_JOB_DESCRIPTION` | ✅ Mirrored + owned (WP-3.3) |
| `awaiting_invoice_month` | — (reads `pending_invoice_client` / conversation) | *none* | ❌ Legacy-only. Not in `_reconcile_*`; a user mid-this-flow reads as FlowMachine-IDLE, so `dispatch_idle` (not `dispatch_in_flow`) handles their next message. |
| `awaiting_compound_response` | `suggested_next_action` | *none* | ❌ Legacy-only. "Yes" after "You also mentioned: …" follow-up offer. Single-shot, would be a trivial Flow class. |
| `awaiting_modify_field` | `modify_row_id` (in flag's own `extra` dict) | *none* | ❌ Legacy-only. Mid-modify field-value prompt (distinct from `DISAMBIGUATION`'s row-pick). Also has a `_cancelled` companion (`awaiting_modify_field_cancelled`) tracking a declined confirm, itself never migrated. |
| `awaiting_invoice_poc_email` | `pending_poc_email_client`, `pending_poc_email_user_id`, `pending_poc_email_row_ids` | *none* | ❌ Legacy-only, and easy to confuse with the migrated `awaiting_poc_email` — this is the PRE-generation readiness gate; `awaiting_poc_email` is the SEND-time flow. Two different prompts, same shape. |

### 2. Other `pending_*` keys (no boolean gate — read directly)

| Key | Purpose | FlowMachine equivalent | Status |
|---|---|---|---|
| `pending_disambiguation` | Numbered "which one did you mean?" / bulk-delete-confirm list | `DISAMBIGUATION` | ✅ Mirrored + owned (WP-3.1) |
| `pending_clarification` | Tag on `last_intent` for a specific ambiguous-month/alt-suggestion confirm | *none* | ❌ Read as part of `last_intent` (row 6 below), not independently flagged. |
| `pending_value_fork` | Disambiguates "billed vs received" when a client's numbers diverge mid-query | *none* | ❌ Legacy-only, short-lived (resolved same-turn via `_resolve_value_fork`), never actually blocks a NEXT message the way an `awaiting_*` flag does — lower priority to migrate. |
| `pending_reminder_offer` | Rows offered when a reminder-adjacent query surfaces unpaid jobs | *none* | ❌ Legacy-only, informational cache for a possible follow-up offer, not a blocking gate. |

### 3. Form state (smart-capture confirm)

| Store | Purpose | FlowMachine equivalent | Status |
|---|---|---|---|
| `memory.get_form_state()` / `memory.cancel_form()` | Extracted-job confirmation card ("Save this job? Yes/Edit") | `SMART_CAPTURE_CONFIRM_PENDING` | ✅ Mirrored (highest reconciliation precedence — "deepest" state) + owned (2.5) |

### 4. Audit-reminder pending list

| Store | Purpose | FlowMachine equivalent | Status |
|---|---|---|---|
| `utils.pending_reminders.get_pending()` | Overdue-invoice nudge awaiting a reply ("paid" / "paid 2" / "later") | *none* | ❌ **Deliberately not migrated.** Armed by `workers/reminder_worker.py`, a separate Railway cron process outside any `process_request` call — doesn't fit the reconciliation pattern (nothing to reconcile FROM inside a request). Classified via `AUDIT_REPLY` intent instead (WP-3 slice 1), routed in `dispatch_idle`, not `dispatch_in_flow`. Already P0-hardened; stays on its own path by design. |

### 5. Follow-up / context caches (not gates — read on the next relevant message, never block one)

| Store | Purpose | FlowMachine equivalent | Status |
|---|---|---|---|
| `last_intent` | Drives `_reconstruct_message` (pronoun/ellipsis resolution: "what about him") + carries `pending_clarification` | *none* — candidate: classifier's `resolved_query` field (WP-2, computed but shadow-logged only, see `[UNDERSTAND_V2_SHADOW]`) | ❌ Superseding mechanism already exists in `services/classifier.py`, just not wired to act (shadow mode, per ASSISTANT_PLAN.md WP-2's accuracy-gate-before-flip requirement). |
| `uscf_context` | Last SQL + last rows shown, for "mark this as paid" / "the first one" resolution | *none* — candidate: `answer_ledger`'s `row_ids` field already carries this | ❌ Overlaps with `answer_ledger` (row 9), which is the intended eventual replacement per TODO.md ("this one is fine, keep it"). |
| `last_generated_invoice` | 30-min cache of the last PDF generated, avoids regenerating on "send it" | *none* | ❌ Not flow-gating state (doesn't block message routing), just a cache. Own ad-hoc TTL — the one Phase 2.4 wants folded into FlowMachine's single TTL. |

### 6. Already-correct — no migration needed

| Store | Purpose | Status |
|---|---|---|
| `answer_ledger` (`services/answer_ledger.py`) | Bot's memory of its own claims (question → scope → value), zero-LLM scope-question answering | ✅ TODO.md: "this one is fine, keep it." Already the intended replacement for `uscf_context`. |
| `flow_v2` (`services/flow_machine.py`) | FlowMachine's own state: `{flow, context, started_at, stack}` | ✅ This IS the target architecture — everything above migrates INTO this. |

### Reading this table for Phase 2.2–2.4

- **12 rows are `✅ Mirrored + owned`** — FlowMachine already tracks these
  via `_reconcile_legacy_to_flow_machine`, and each has a `Flow` class. These
  are Phase 2.2's easiest targets: flip the arm-site to call
  `flow_machine.set_state()` directly, keep a legacy-flag shim for
  as-yet-unported readers, delete the shim once every reader is moved.
- **6 rows are `❌ Legacy-only`** — no FlowMachine flow exists yet. Four are
  simple single-prompt gates (`awaiting_invoice_month`,
  `awaiting_compound_response`, `awaiting_modify_field`,
  `awaiting_invoice_poc_email`) that fit the exact pattern WP-3 slices 2–3
  already proved out; the other two (`pending_value_fork`,
  `pending_reminder_offer`) are short-lived same-turn caches, not blocking
  gates, and may not need a Flow class at all.
- **`get_pending()` (audit reminders) stays outside FlowMachine by design** —
  cron-armed, not `process_request`-armed.
- **`last_intent` / `uscf_context` are the two rows Phase 2.4 should retire
  outright** rather than port: `last_intent`'s job overlaps the classifier's
  already-built (shadow-only) `resolved_query`, and `uscf_context` overlaps
  `answer_ledger`, which TODO.md already names as the keeper.
