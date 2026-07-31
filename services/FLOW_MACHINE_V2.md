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
| ~~`awaiting_client_billing`~~ | `pending_billing_client`, `pending_billing_user_id` | `INVOICE_NEED_BILLING` | ✅✅ **Deleted** (Phase 2.3) — FlowMachine is the sole source of truth. `_arm_client_billing_v2` bypasses the shared `_prompt()` helper. |
| ~~`awaiting_poc_name`~~ | `pending_poc_client`, `pending_poc_user_id`, `pending_poc_row_ids` | `INVOICE_NEED_POC_NAME` | ✅✅ **Deleted** (Phase 2.3) — FlowMachine is the sole source of truth. `_arm_poc_name_v2` bypasses the shared `_prompt()` helper. Migrating this + billing uncovered a pre-existing bug (task_1b10c22c, not fixed here): `on_cancel` passes "skip" to handlers whose cancel-word set doesn't include it, so a CANCEL-classified message ends up saving the literal string "skip" as billing text / POC name. |
| ~~`awaiting_poc_email`~~ | `pending_send_invoice` | `INVOICE_NEED_POC_EMAIL` | ✅✅ **Deleted** (Phase 2.3) — FlowMachine is the sole source of truth. Most arm-site-heavy migration yet: 4 sites (3 via a new `_arm_poc_email_v2` helper, plus main.py's own). Uncovered a pre-existing gap (task_ab7501b3, not fixed here): main.py's site writes `poc_email_client`/`poc_email_pdf_path`/etc., which `_handle_poc_email_response` never actually reads (only `pending_send_invoice`) — preserved as-is. |
| `awaiting_job_input` | — | `SMART_CAPTURE_NEED_DESCRIPTION` | ✅ Mirrored + owned (2.5) |
| ~~`awaiting_bank_details`~~ | — (`pending_invoice` shared with other checkpoints) | `BANK_DETAILS` | ✅✅ **Deleted** (Phase 2.3) — FlowMachine is the sole source of truth. Second flow migrated; more involved than send-confirm due to a shared `_prompt()` helper (5 other checkpoints) and a check-after retry pattern, both worked around without touching the other checkpoints. |
| ~~`awaiting_name_change`~~ | — | `NAME_CHANGE` | ✅✅ **Deleted** (Phase 2.3) — FlowMachine is the sole source of truth. Simplest migration in this batch: single arm site, no retry loop. |
| ~~`awaiting_link_id`~~ | — | `LINK_ACCOUNT` | ✅✅ **Deleted** (Phase 2.3) — FlowMachine is the sole source of truth. Uncovered a real pre-existing bug during migration: `LinkAccount.handle_response`'s docstring claimed "always completes in one turn," but `_process_link_id` has an invalid-ID retry path that only "worked" pre-migration via the legacy flag re-arming + reconciliation-on-next-message side door. Fixed to check the returned operation directly (`link_invalid_id`), same pattern as `BankDetails`. |
| ~~`awaiting_invoice_address`~~ | `pending_invoice`, `pending_address_user_id` | `INVOICE_ADDRESS` | ✅✅ **Deleted** (Phase 2.3) — FlowMachine is the sole source of truth. Two arm sites (standalone command + gate checkpoint), both via `_arm_invoice_address_v2`. |
| ~~`awaiting_job_description`~~ | `pending_jobdesc_row_id`, `pending_jobdesc_user_id` | `INVOICE_NEED_JOB_DESCRIPTION` | ✅✅ **Deleted** (Phase 2.3) — FlowMachine is the sole source of truth. `_arm_job_description_v2` replaces the last-but-one `_prompt()` caller. |
| ~~`awaiting_invoice_month`~~ | `pending_invoice_client`, `pending_invoice_send_email` | `INVOICE_NEED_MONTH` | ✅✅ **Deleted.** Three arm sites (direct "send invoice for X" with no month, the planner-clarification redirect, and the retry branch). `_handle_invoice_month_reply` has no cancel branch of its own — CANCEL is handled entirely in `InvoiceNeedMonth.on_cancel`. Migrating this deleted the "Universal intent-shift guard" (`_PENDING_STATES` + surface-shape gate + `is_new_query_not_response` escape hatch) outright — this was its last member. |
| ~~`awaiting_compound_response`~~ | `suggested_next_action` | `COMPOUND_RESPONSE` | ✅✅ **Deleted.** "Yes" after "You also mentioned: …" follow-up offer. Two arm sites (after smart-capture save, after deterministic-query INSERT), both via `_arm_compound_response_v2`. No retry loop; `_handle_compound_response` resets FlowMachine FIRST (not after) since every branch recurses into `process_request` or is terminal. |
| ~~`awaiting_modify_field`~~ | `modify_row_id` | `MODIFY_FIELD` | ✅✅ **Deleted.** The last boolean legacy flag — `_AWAITING_FLAGS` is empty now. Single arm site, genuine 3-way outcome once armed (re-prompt / hand off to `DISAMBIGUATION` via `_arm_disambiguation` / apply-and-finish). The explicit verb-trigger entry point ("modify ...") for a BRAND NEW message stays legacy-only (v2-off fallback) — a fresh WRITE_UPDATE-shaped message is shadow-only when v2 is on and never routes through `_handle_modify_intent` at all. |
| ~~`awaiting_invoice_poc_email`~~ | `pending_poc_email_client`, `pending_poc_email_user_id`, `pending_poc_email_row_ids` | `INVOICE_READINESS_POC_EMAIL` | ✅✅ **Deleted.** Easy to confuse with the earlier-migrated `awaiting_poc_email`/`INVOICE_NEED_POC_EMAIL` — this is the PRE-generation readiness gate; that one is the SEND-time flow. Was the shared `_prompt()` helper's LAST caller — with `_arm_invoice_readiness_poc_email_v2` replacing it, `_prompt()` itself was deleted entirely. Retry loop on invalid email, same shape as `BANK_DETAILS`. |

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

### Progress update (Phase 2.3, complete)

**All 12 of the originally-`✅ Mirrored + owned` rows are now `✅✅ Deleted`**:
`INVOICE_AWAIT_SEND_CONFIRM`, `BANK_DETAILS`, `NAME_CHANGE`, `LINK_ACCOUNT`,
`INVOICE_NEED_POC_EMAIL`, `INVOICE_NEED_BILLING`, `INVOICE_NEED_POC_NAME`,
`INVOICE_ADDRESS`, `INVOICE_NEED_JOB_DESCRIPTION`, `SMART_CAPTURE_NEED_DESCRIPTION`,
`SMART_CAPTURE_CONFIRM_PENDING`, `DISAMBIGUATION` — FlowMachine is their sole
source of truth, no legacy flag or reconciliation branch left for any of
them. `_reconcile_legacy_to_flow_machine` itself is now an empty no-op (kept
as the mechanism for any future flow that wants the same "arm now, sync
eagerly" pattern). The shared `_prompt()` helper (used by 6 of the
invoice-readiness-gate checkpoints originally) now has exactly **one caller
left**: `awaiting_invoice_poc_email` — still legacy, no migration needed yet.

`DISAMBIGUATION` was the 12th and last migration. Its legacy mirror
(`pending_disambiguation`) is payload data, not a routing-only boolean flag
— it can't be deleted the way the other 11 flags' boolean mirrors were,
since it carries the actual numbered row list. What *was* migrated: its one
arm site, `_arm_disambiguation`, used to go through the same
`_sync_flow_machine_now` → `_reconcile_legacy_to_flow_machine` eager-sync
path `_arm_awaiting` uses; now it writes `flow_machine.set_state()`
directly, matching every other migrated flow's arm site. With that done,
`_reconcile_legacy_to_flow_machine`'s disambiguation branch — the very last
branch left in that function — was deleted, leaving the function an empty
no-op. The legacy read/dispatch block in `_process_request_impl`
(`if user_mem.get("pending_disambiguation"): ...`) was deliberately **not**
gated behind "v2 off" or deleted: `Disambiguation.handle_response`'s own
docstring explains it's intentionally kept as the SIDE_QUESTION fallback
path — when `dispatch_in_flow` returns `SHADOW_ONLY` for a scope-clarifying
question, this legacy block's own heuristics are what let the message keep
falling through to be answered elsewhere, instead of being misread as a
numbered pick.

`SMART_CAPTURE_CONFIRM_PENDING` was the first migration whose legacy
"mirror" was never a boolean flag but `memory.get_form_state()` (a dict from
`memory.start_form()`) — reconciliation treated any truthy form_state as
this flow regardless of `form_type` (`smart_capture_confirm` or
`smart_capture_missing` both mapped here), which meant FlowMachine always
lagged one full turn behind the form itself. Fixed by making both
fresh-entry sites (`_show_smart_capture_confirmation` and
`_extract_and_confirm`'s missing-fields branch) write
`flow_machine.set_state()` directly the same moment they call
`memory.start_form()`; every other `start_form()` call in the module just
re-arms the same form while already inside this flow (retry counters,
invalid-email re-prompts), so needed no new write. With both fresh-entry
sites eager, the reconciliation branch was deleted —
`pending_disambiguation` is now the **only** thing left with a
reconciliation branch at all.

`SMART_CAPTURE_NEED_DESCRIPTION` (formerly `awaiting_job_input`) was the most
involved of the ten: three arm sites (`_start_smart_capture`,
`_extract_and_confirm`'s retry branch, `_handle_smart_capture_confirm`'s
"Edit" branch), a genuine 3-way transition in
`SmartCaptureNeedDescription.handle_response` (stay / advance to confirm-
pending / reset), and a real bug fixed in a *different* flow class —
`SmartCaptureConfirmPending.handle_response`'s blanket
`if not form_state: reset()` would have clobbered the Edit transition back to
`SMART_CAPTURE_NEED_DESCRIPTION` (form_state IS gone right after
`cancel_form()`), the same class of bug `LINK_ACCOUNT` had. The legacy
dispatch block's `is_new_query_not_response` escape-hatch LLM call was
deleted outright (not ported) — the v2 classifier's per-flow guidance
already makes the identical distinction, and `dispatch_in_flow`'s
SIDE_QUESTION handling routes those messages correctly earlier in the
cascade.

**Remaining unmigrated: none — all 12 of the originally-mirrored flows are
now FlowMachine-only.**

### Post-2.3: INVOICE_READINESS_POC_EMAIL (formerly `awaiting_invoice_poc_email`)

With Phase 2.3 complete, `awaiting_invoice_poc_email` — one of the six
`❌ Legacy-only` rows, not one of the original 12 — got its own FlowMachine
flow too, since it was specifically the LAST caller of the shared invoice-
readiness-gate `_prompt()` helper. Migrating it let `_prompt()` be deleted
outright (every other checkpoint had already moved to its own `_arm_*_v2`
helper). Same retry-loop shape as `BANK_DETAILS`: an invalid email re-arms
and re-asks (via the new `_arm_invoice_readiness_poc_email_v2`, called from
both the gate's own checkpoint and the handler's own retry branch); a valid
one saves and resumes the invoice flow. Deliberately named distinctly from
`INVOICE_NEED_POC_EMAIL` (the SEND-time flow, asks for the address to
deliver an already-generated PDF) — this one runs BEFORE generation, asking
for an email on the job rows themselves.

### Post-2.3: INVOICE_NEED_MONTH (formerly `awaiting_invoice_month`)

Next of the six `❌ Legacy-only` rows to get its own FlowMachine flow.
THREE arm sites: the direct "send invoice for X" path with no month given,
the planner-clarification redirect (a query that leaked an invoice ask into
the query pipeline), and `_handle_invoice_month_reply`'s own
unrecognised-month retry branch — added an `"invoice_month_retry"` operation
name (replacing a bare `"ACTION_TRIGGER"`) so `InvoiceNeedMonth.handle_response`
can tell "stay in this flow" apart from "the reconstructed synthetic message
re-entered `process_request` and landed somewhere else entirely" (which
needs an unconditional reset, unlike the retry case). Unlike most gates,
the handler has no cancel branch of its own — any text is read as an
attempted month — so CANCEL is handled entirely in `InvoiceNeedMonth.on_cancel`
instead of delegating to it.

Migrating this let the entire "Universal intent-shift guard" be deleted
outright: `_PENDING_STATES` + a surface-shape gate + an
`is_new_query_not_response` escape-hatch LLM call, all built to protect
exactly the single-question legacy flags that by this point had all
already moved to FlowMachine except `awaiting_invoice_month` — its last
member. The identical "question-shaped → don't treat as a reply"
distinction is made by the v2 classifier's per-flow `flow_compatible`
guidance now, reached via `dispatch_in_flow` before this legacy block was
ever consulted.

### Post-2.3: COMPOUND_RESPONSE (formerly `awaiting_compound_response`)

Two arm sites (after a smart-capture job save, after a
deterministic-query INSERT), both via the new `_arm_compound_response_v2` —
`suggested_next_action` itself is written by the CALLER before arming (set
earlier during compound-intent detection), so the arm helper only
transitions FlowMachine and clears other legacy flags defensively.

The inline legacy dispatch block was extracted into `_handle_compound_response`
(called by `CompoundResponse.handle_response`), and the legacy dispatch
site inside `_process_request_impl` was deleted outright — v2 is
unconditionally the router for this flow now. Notably,
`_handle_compound_response` resets FlowMachine **first**, not after: every
branch either recurses into `process_request`
(the "yes" case, and the "anything else, fall through as a new message"
case) or is terminal (the "no" case) — leaving `COMPOUND_RESPONSE` active
into a recursive `process_request` call would make `dispatch_in_flow` try
to route that recursive call through this same flow again.

### Post-2.3: MODIFY_FIELD (formerly `awaiting_modify_field`) — the last one

`awaiting_modify_field` was the LAST boolean legacy flag left anywhere —
migrating it left `_AWAITING_FLAGS` an empty tuple. All 12 originally-`✅
Mirrored + owned` flows AND all 6 originally-`❌ Legacy-only` flows are now
FlowMachine-owned; there is no boolean legacy conversational-state flag
left in the codebase at all. `_arm_awaiting` and `_sync_flow_machine_now`
are left in place (with zero callers) as the mechanism for any FUTURE flag
that wants the same "arm now, sync eagerly" pattern before its own direct
FlowMachine write is wired up.

Single arm site (`_handle_modify_intent`'s "no field/value parsed, but a
row is pinned" branch), and a genuine 3-way outcome once armed:
re-prompt (still no field/value; re-arms itself), hand off to
`DISAMBIGUATION` (a client/bill filter supplied alongside the field/value
matched multiple rows — `_arm_disambiguation` already transitioned
FlowMachine, so `ModifyField.handle_response` must not clobber it back to
IDLE), or apply the update and finish (success, or a parse/write failure,
both terminal).

Deliberately did NOT touch the explicit verb-trigger entry point ("modify
...", "update ...") that starts this flow for a BRAND NEW message — that
stays legacy-only, firing only when v2 is off. When v2 is on, a fresh
WRITE_UPDATE-shaped message is shadow-only (see `flow_dispatcher.dispatch_idle`'s
own docstring) and the legacy update/query pipeline handles it directly,
never through `_handle_modify_intent` at all — so the only thing that
needed migrating was the CONTINUATION (the field-value reply to an
already-pinned row).

**Three pre-existing bugs surfaced during these migrations** (flagged
separately, not fixed as part of state-ownership changes):
`task_ab7501b3` (main.py's poc_email arm site writes a memory shape
`_handle_poc_email_response` never reads), `task_1b10c22c`
(`InvoiceNeedBilling`/`InvoiceNeedPocName`'s `on_cancel` passes the literal
string `"skip"`, which isn't in their handlers' cancel-word set, so a
CANCEL-classified message ends up saving "skip" as real data instead of
aborting the invoice), and `task_e16e90fb` (`_handle_pending_reminder`'s
`_active_subflow` check never accounted for `SMART_CAPTURE_CONFIRM_PENDING`
or an active form_state at all — a pending reminder can hijack a reply
meant for the "Save this job? (Yes/Edit)" confirmation card; pre-existing,
not introduced by this migration).

### Reading this table for Phase 2.2–2.4

- **12 rows are `✅ Mirrored + owned`** — FlowMachine already tracks these
  via `_reconcile_legacy_to_flow_machine`, and each has a `Flow` class. These
  are Phase 2.2's easiest targets: flip the arm-site to call
  `flow_machine.set_state()` directly, keep a legacy-flag shim for
  as-yet-unported readers, delete the shim once every reader is moved.
  **(All 12 of these are now done — see "Progress update" above.)**
- **All 6 originally-`❌ Legacy-only` rows are now migrated too — no
  boolean legacy flag remains in the codebase at all**:
  `awaiting_invoice_poc_email` → `INVOICE_READINESS_POC_EMAIL` (the last
  caller of the shared invoice-readiness-gate `_prompt()` helper — migrating
  it let that helper be deleted outright), `awaiting_invoice_month` →
  `INVOICE_NEED_MONTH` (the last member of the "Universal intent-shift
  guard" — migrating it let that whole mechanism be deleted outright too),
  `awaiting_compound_response` → `COMPOUND_RESPONSE`, and
  `awaiting_modify_field` → `MODIFY_FIELD` (the very last boolean legacy
  flag — `_AWAITING_FLAGS` is an empty tuple now). `pending_value_fork` and
  `pending_reminder_offer` are short-lived same-turn caches, not blocking
  gates, and may not need a Flow class at all.
- **`get_pending()` (audit reminders) stays outside FlowMachine by design** —
  cron-armed, not `process_request`-armed.
- **`last_intent` / `uscf_context` are the two rows Phase 2.4 should retire
  outright** rather than port: `last_intent`'s job overlaps the classifier's
  already-built (shadow-only) `resolved_query`, and `uscf_context` overlaps
  `answer_ledger`, which TODO.md already names as the keeper.
