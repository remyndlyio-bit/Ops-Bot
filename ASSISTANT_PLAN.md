# Remyndly — Assistant-Grade Architecture: Implementation Plan

**Status**: approved for implementation · **Owner**: (assign) · **Written**: 2026-07
**Goal**: close the gap between "keyword bot that answers queries" and "assistant that
holds a conversation" — fixing, by construction: weak follow-up understanding, no
proactive summaries, missing supporting details, inaccurate historical answers,
generic fallbacks, and WhatsApp latency.

---

## 0. Is this how the industry does it? (yes)

The target architecture below is the standard production pattern for
conversational assistants, not an invention:

| Pattern in this plan | Industry equivalent |
|---|---|
| One LLM "understand" call returning a typed verdict, deterministic execution after | OpenAI/Anthropic tool-calling agent loop; how Sierra, Decagon, Intercom Fin structure turns |
| Explicit conversation state machine owning flows | LangGraph state graphs; Rasa dialogue stack; every IVR/support bot at scale |
| Structured outputs validated before execution | JSON-schema/structured-output APIs; our Path 3 typed `Plan` is exactly this (already shipped) |
| Answer ledger (bot remembers its own claims) | "conversation memory / grounding state" in production assistants; the piece hobby bots skip |
| Shadow mode → eval gate → flag flip → canary | standard LLM-product rollout (we already do this: KnowledgeBook A/B, STRICT_PLAN_VALIDATION escape hatch) |

**Deliberate choice: no new framework.** LangGraph/Assistants API would re-house the
loop, not improve it — and CLAUDE.md's working style (simplest fix first, no new
infrastructure while the simple path works) plus the fact that **FlowMachine v2 is
already half-built in this repo** make "complete what exists" strictly cheaper and
lower-risk than adopting a framework.

---

## 1. What already exists (do NOT rebuild)

| Asset | Where | State |
|---|---|---|
| One-call classifier with typed `Verdict` (+ `flow_compatible`) | `services/classifier.py` | Landed, flag OFF |
| Single dispatcher (idle + in-flow) | `services/flow_dispatcher.py` | Landed, flag OFF |
| FlowMachine: single state owner, TTL, stack(2), 6 owned flows | `services/flow_machine.py`, `services/flows.py` | Landed, flag OFF |
| Legacy→v2 flag reconciliation | `intent_service._reconcile_legacy_to_flow_machine` | Landed |
| Typed query plan + strict validation + retry | `services/plan.py`, Path 3 (`PATH_3.md`) | **Live in prod** |
| Column registry (semantic docs + SQL per column) | `services/columns/` | Live |
| Message↔SQL consistency guard (incl. dispatch vocab) | `services/query_guard.py` | Live |
| KnowledgeBook grounding + 1,340-exemplar corpus | `services/knowledge_book.py`, `knowledge/` | Live (default ON) |
| Oracle-graded eval harness (200 held-out cases, A/B runner) | `knowledge/eval_hard.py`, `knowledge/ab_run.py` | Live |
| DB-backed per-user memory (JSONB, per-user locks, file fallback) | `utils/memory_service.py` → `public.user_memory` | Live |
| Beta-gate allowlist (usable as canary mechanism) | `intent_service.process_request` top | Live |
| 898-test offline suite + live e2e scripts | `tests/` | Live |

**The problem layer** is `intent_service.process_request`'s legacy cascade: ~20
ordered keyword checks + 15 `awaiting_*` flags + 7 parallel context mechanisms
(`last_intent`, `uscf_context`, `last_saved_job`, `form_state`,
`pending_disambiguation`, `pending_reminders`, conversation history). Every
production bug fixed in the 2026-07 hardening sessions (audit-reply hijack,
id/"paid" substring misroute, modify-disambiguation silent no-op, aggregate-scope
misread) lived in this layer. FlowMachine v2 sessions 1–2.5 were built to replace
it and stopped before the finish line.

---

## 2. Target architecture

```
 WhatsApp / Telegram webhook
        │  (immediate ack — WP-5)
        ▼
 ┌─────────────────────────────────────────────────────────┐
 │ process_request                                          │
 │                                                          │
 │  1. load ConversationState  (FlowMachine + ledger+focus) │
 │  2. UNDERSTAND — one classifier call, full state in      │
 │     prompt → typed Verdict v2                            │
 │       · intent                                           │
 │       · flow_compatible (existing)                       │
 │       · resolved_query {entities, time_range, inherits}  │
 │       · references_last_answer: bool                     │
 │       · confidence                                       │
 │  3. route:                                               │
 │       reply-to-flow  → Flow.handle_response (existing)   │
 │       ledger hit     → answer from AnswerLedger (no SQL) │
 │       read/aggregate → Path 3 plan → guard → SQL (as-is) │
 │       mutation       → modify/delete handlers (as-is)    │
 │       action         → invoice/reminder flows (as-is)    │
 │  4. RESPOND — typed AnswerPayload → synthesis contract   │
 │  5. write-back: ledger entry + focus + flow transition   │
 └─────────────────────────────────────────────────────────┘
```

Execution layer (Path 3, columns, guard, KB, PDF, reminders) is untouched.

---

## 3. Work packages

Each WP states: scope, files, exact shapes, tests, acceptance gate, rollback,
estimate. Ship strictly in order; every WP leaves prod shippable.

---

### WP-0 — Turn on what exists; instrument the baseline  *(1–2 days)*

The prerequisite for everything: v2 has never run in production, and we have no
latency numbers to beat.

**Do**
1. Run the full offline suite + `tests/test_e2e_live.py` with
   `FLOW_MACHINE_V2=true` locally. Fix anything that only fails under the flag.
2. Enable `FLOW_MACHINE_V2=true` on Railway for the beta-gate allowlist cohort
   only (guard the v2 branch with the same `is_user_allowed` check used by
   BETA_GATE — ~5 lines) → then all users after 3 quiet days.
3. Add per-turn telemetry (one structured log line per message):
   `turn_ms`, `llm_calls`, `llm_ms_total`, `route` (flow/ledger/planner/…),
   `fallback` (bool: any generic clarify/couldn't-format reply),
   `verdict_intent`, `verdict_confidence`.
   File: small helper in `utils/telemetry.py`; call from `process_request` exit
   paths (wrap in try/except — telemetry must never break a turn).
4. Baseline dashboard: pull 1 week of logs; record p50/p95 `turn_ms`,
   mean `llm_calls`/turn, `fallback` rate. These are the numbers WP-2/4/5 must move.

**Acceptance**: v2 on for all users; baseline table committed to this doc.
**Rollback**: unset `FLOW_MACHINE_V2` (documented, already supported).

---

### WP-1 — AnswerLedger + Focus  *(2–3 days)*

The single highest-leverage piece: the bot starts remembering **its own claims**,
which is what follow-ups and "historical accuracy" actually require.

**Shapes** (new file `services/conversation_state.py`; persisted inside the
existing FlowMachine context blob under `user_memory` — no new table):

```python
@dataclass
class LedgerEntry:
    turn_id: str            # uuid
    ts: str                 # iso
    question: str           # user's message verbatim
    kind: str               # "aggregate" | "list" | "field" | "action"
    plan: dict | None       # the EXACT Path-3 plan/SQL filters used
    scope: dict             # {"filters": {...}, "time_range": {...}} resolved
    value: Any              # the number / count / client name given
    row_ids: list[str]      # rows shown (for "the first one", "mark this paid")
    surface: str            # first 200 chars of the reply actually sent

@dataclass
class Focus:
    client: str | None      # entity in play
    time_range: dict | None
    row_ids: list[str]      # last shown rows
    updated_at: str

# ConversationState = {"ledger": [LedgerEntry × ≤10], "focus": Focus}
```

**Do**
1. Write-through: at every answer exit point that today calls
   `_update_uscf_context` / `_save_last_intent` / `_update_sql_context`, ALSO
   append a `LedgerEntry` and update `Focus`. (Do not remove the legacy writes
   yet — WP-3 deletes them.) Exit points: aggregate answers, list answers,
   field-answers-from-context, modify/delete confirmations, invoice sends.
2. Cap ledger at 10 entries, trim on write. Schema-version the blob
   (`{"v": 1, ...}`) so future migrations are a read-time upgrade, not a wipe.
3. Read path (small, deterministic, this WP only): scope questions about the
   immediately previous answer — messages matching "does/do these include…",
   "is that only…", "paid and unpaid?" — answered directly from
   `ledger[-1].scope` ("That ₹11,75,000 is everything — paid and unpaid, no
   date filter."). This kills the worst transcript failure class with zero LLM
   dependency; WP-2 generalises it.

**Tests** (`tests/test_answer_ledger.py`): write-through from each exit point;
trim; schema-version upgrade; scope-question read path (the exact production
transcript: total → "Do these include, paid and unpaid?"); focus row_ids feed
"mark this as paid" resolution identically to today's `uscf_context`.

**Acceptance**: transcript scenario passes offline; no legacy test regresses.
**Rollback**: ledger writes are additive; read path behind `ANSWER_LEDGER=1`.

---

### WP-2 — Understand v2: full state into the one classifier  *(3–4 days)*

Absorb the five colliding heuristics into the classifier that already exists,
with the ledger + focus in its prompt.

**Do**
1. Extend `Verdict` (`services/classifier.py`) — add:
   `resolved_query: dict | None` — the query with inherited context filled in
   ({"client": "Nike", "time_range": {...}, "metric_hint": "sum"|"count"|"list"}),
   `references_last_answer: bool`, `history_question: bool`.
2. Extend the prompt: render `Focus`, `ledger[-3:]` (question → scope → value,
   compact one-liners — never raw JSON, same discipline as KnowledgeBook), and
   the active flow block (exists). Keep total added tokens ≤ ~400.
3. On READ intents, hand `resolved_query` to the planner as pre-resolved
   context (planner keeps its own retry/validation — unchanged). On
   `references_last_answer`, route to the ledger answerer from WP-1.
4. **Shadow first**: for 1–2 weeks the new fields are logged but legacy
   `_reconstruct_message` / `_is_followup_field_request` / `is_history_question`
   keep deciding. Build `knowledge/understand_eval.py` in the style of
   `ab_run.py`: ~60 labelled multi-turn cases (pull from live transcripts +
   this repo's bug history: "what about this month?", "the first one",
   "mark this as paid", "Do these include…", Hinglish follow-ups). Grade
   verdict fields against gold.
5. Flip `UNDERSTAND_V2=1` when shadow accuracy ≥ 90% on the eval AND disagrees
   with legacy in the *right* direction on disagreement sampling. Legacy
   heuristics stay in the tree until WP-3 deletes them.

**Tests**: parser paths for new fields; prompt renders state; shadow logging;
eval harness itself deterministic.

**Acceptance**: eval ≥ 90%; fallback rate (WP-0 metric) drops ≥ 30% relative
for the canary cohort with flag on.
**Rollback**: `UNDERSTAND_V2=0` reverts to legacy heuristics wholesale.

---

### WP-3 — Finish the flow migration; delete the flag bag  *(3–4 days)*

FlowMachine v2 session 3, as sketched in `services/FLOW_MACHINE_V2.md`, minus
what's already shipped elsewhere (typed Plan = Path 3, already live).

**Do**
1. Migrate the remaining flag-bag flows into `flows.py` Flow classes:
   onboarding (3-step), bank-details, disambiguation (**including the
   modify-type pending — reuse `_apply_modify_update`**), audit-reply
   (**the P0 hijack path — its question-shape guard moves into
   `flow_compatible` classification, where it can't be bypassed**), reminder
   reply, name-change, link-account, invoice-address/job-description gates.
   Same delegation pattern as sessions 2/2.5: Flow wraps the existing
   `_handle_*` method; no handler rewrites.
2. Implement `NEW_FLOW` push/pop properly (stack already exists, capped at 2):
   e.g. mid invoice-gate, "add a job for Acme 25k" pushes capture flow, pops
   back with the invoice-gate resume nudge.
3. Delete: `awaiting_*` flags and every arm/check/reset site, the
   `_reconcile_legacy_to_flow_machine` bridge, `_is_followup_field_request`,
   `_reconstruct_message`, the smart-capture trigger keyword list, the
   intent-shift guard — each deletion in its own commit, suite green between.
4. Port the regression suites that pinned this layer's bugs
   (`TestAuditReplyDoesNotHijackQuestions`, `TestModifyDisambiguationReply`,
   `TestReminderDoesNotHijack`, form escape-hatches) to drive the Flow
   classes instead — the *scenarios* are the asset, keep every one.

**Acceptance**: zero `awaiting_*` references left; `intent_service.py` shrinks
~30–40% (doc's own estimate); all ported scenario tests green.
**Rollback**: per-commit; this WP is why sessions 1–2.5 kept legacy intact.

---

### WP-4 — Answer contract: summaries + supporting details, everywhere  *(2 days)*

**Do**
1. New `AnswerPayload` (extend `services/response_synthesis.py`):

```python
@dataclass
class AnswerPayload:
    headline: str            # "₹11,75,000" / "7 jobs" / "Maruti Suzuki"
    scope_note: str          # "all jobs, paid + unpaid, all time" — from the plan, deterministic
    support: list[dict]      # ≤3 rows: {client, bill_no, amount, paid, bill_sent}
    remainder: int           # rows not shown
    followup: str | None     # ONE contextual suggestion ("Want the unpaid split?")
```

2. Build it deterministically from plan + rows in `build_clean_payload`'s
   callers (scope_note comes from the Path-3 plan — the same source WP-1
   ledgers, so the spoken scope ALWAYS matches the executed SQL — this is the
   structural fix for the "said earnings, meant unpaid" transcript bug).
3. Synthesis prompt: render payload; require headline + scope in sentence one;
   support lines formatted `Client · bill_no · ₹amt · paid/unpaid ·
   invoiced/not`; ≤ 900 chars for WhatsApp.
4. Replace the raw `_format_job_cards` dump for query results with the
   contract renderer (keep cards for explicit "show/export" asks). No more
   4-job card dumps for a status question.
5. `followup` from a small deterministic table keyed on plan shape
   (aggregate→offer split; unpaid list→offer reminders; client total→offer
   invoice status). Not an LLM call.

**Tests**: payload construction per plan shape; scope_note == executed filters
(property test over the oracle dataset); renderer length caps; card-dump only
on explicit list intents.
**Acceptance**: every read answer carries headline + scope + support in live
e2e; "Improper response" transcript case (IMG-3) renders as one scoped card.
**Rollback**: `ANSWER_CONTRACT=0` reverts synthesis prompt only.

---

### WP-5 — Latency  *(2 days)*

Baseline from WP-0; target: **p95 ≤ 6s WhatsApp perceived, ≤2 LLM calls/turn.**

**Do**
1. Immediate ack: webhook returns fast; on turns that will call the planner,
   send "On it 👍" via the existing WhatsApp sender before the LLM work
   (skip when ledger/flow answers — those are sub-second).
2. Call-count audit: with Understand v2, classify+reconstruct+follow-up
   detection are ONE call; synthesis is the second. Assert `llm_calls ≤ 2` in
   telemetry; alert at 3+.
3. Ledger fast-path answers: 0 LLM calls (template render) — measure share.
4. Cache per-process: schema block, known-client list (exists), KB index
   (exists — verify it isn't re-built per request), one shared HTTP client
   with keep-alive for OpenRouter + Twilio.
5. Kill sequential retries where a deterministic check can pre-empt (JSON
   retry only on actual parse failure — exists; keep).

**Acceptance**: telemetry shows p95 target met for canary cohort; llm_calls
histogram ≤2 at p99.

---

### WP-6 — Production hardening  *(2 days, parallelizable)*

1. **Webhook idempotency**: dedupe on Twilio `MessageSid` / Telegram
   `update_id` (10-min window in `user_memory`) — retries must not double-run
   flows or double-write ledger.
2. **Concurrency**: per-user turn lock (MemoryService already has per-user
   locks — extend to the whole turn) so a double-send can't interleave two
   `process_request`s mid-state-write.
3. **State hygiene**: TTLs already exist (30-min flows); add ledger max-age
   (7 days), versioned blob (WP-1), and a `/health` deep-check that reads+
   writes a sentinel `user_memory` row.
4. **Observability**: ship the WP-0 log line to a dashboard (Railway logs →
   simple grafana/logtail); alerts: fallback rate > 10%, p95 > 8s,
   llm_calls/turn > 2.5, any `[FLOW_V2]` unknown-state error.
5. **Runbook**: one page in this doc — every flag, its safe value, and the
   one-line rollback (pattern already proven: FLOW_MACHINE_V2,
   STRICT_PLAN_VALIDATION, KNOWLEDGE_BOOK, KB_VALUE_FORK).

---

## 4. Rollout protocol (every WP)

1. Offline suite green (898+ and growing).
2. Live e2e scripts green on a fresh OpenRouter key (`test_e2e_live.py`,
   `test_e2e_context_followup.py`, plus WP-2's `understand_eval.py`).
3. Shadow in prod where applicable (classifier fields, ledger reads) — logs only.
4. Flag ON for beta-gate allowlist cohort ≥ 3 days; watch WP-0 metrics.
5. Flag ON globally; previous stage's flag becomes the rollback.

## 5. Success metrics (from WP-0 baseline)

| Metric | Baseline (WP-0) | Target |
|---|---|---|
| Fallback rate (generic clarify / couldn't-format) | measure | **< 5%** |
| Follow-up success (understand_eval, live sample) | measure | **≥ 90%** |
| p95 perceived WhatsApp latency | measure | **≤ 6s** (ack ≤ 1.5s) |
| LLM calls / turn (p99) | measure (est. 3–4) | **≤ 2** |
| Answers carrying scope + support | ~0% | **100% of reads** |
| `awaiting_*` flags in code | 15+ | **0** |

## 6. Risks

| Risk | Mitigation |
|---|---|
| v2 flag-on surfaces latent bugs (never ran in prod) | WP-0 canary cohort + telemetry before any new code |
| Understand call misroutes vs. legacy heuristics | shadow mode + labelled eval gate before flip; per-WP flags |
| Ledger answers go stale after a mutation | mutations append their own ledger entry + invalidate `focus.row_ids`; scope answers only ever reference `ledger[-1]` |
| Prompt growth slows the classifier | ≤400-token budget for state block; compact one-liners, never JSON |
| Flow migration breaks a multi-turn edge | port ALL existing scenario tests first (they encode every 2026-07 bug), migrate one flow per commit |
| Single engineer bus-factor | this doc + FLOW_MACHINE_V2.md + PATH_3.md are the spec; each WP lands independently |

## 7. Sequencing & effort

| WP | Scope | Est. |
|---|---|---|
| 0 | Enable v2, telemetry, baseline | 1–2 d |
| 1 | AnswerLedger + Focus + scope answers | 2–3 d |
| 2 | Understand v2 + shadow eval + flip | 3–4 d |
| 3 | Flow migration completion, delete flag bag | 3–4 d |
| 4 | Answer contract | 2 d |
| 5 | Latency | 2 d |
| 6 | Hardening (parallel) | 2 d |
| **Total** | | **~15–19 dev-days** (3–4 wks for one engineer incl. rollout soak) |

Order is deliberate: WP-0/1 deliver visible product improvement in week 1
(scope follow-ups + summaries of what exists), WP-2/3 remove the bug-factory,
WP-4/5 deliver the feel, WP-6 makes it boring to operate.
