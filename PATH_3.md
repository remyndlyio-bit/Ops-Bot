# Path 3 — Typed Plan + Canonical Filter Normalisation

The architectural answer to "the AI emitted a shape we didn't anticipate,
the SQL builder ILIKE'd it, the user got a wrong answer." Tracks the work
to eliminate that entire bug class, not just patch individual variants.

## Why Path 3 exists

Every bug in `tests/test_planner_boundary.py` started the same way: the
planner LLM emitted a filter value (`"not_null"`, `["no","false","0"]`,
`null`, `"IS NOT NULL"`) that didn't match the shape the SQL builder
expected. The fix was always local — patch the builder, add a test.
The class kept recurring because nothing in the architecture said
*every* filter value must reduce to one of N known canonical shapes
before SQL ever runs.

Path 3 says that.

## Phase 3a — landed (this commit)

| Component | File | Purpose |
| --- | --- | --- |
| Canonical filter types | `services/plan.py` | `NullCheck` / `BoolCheck` / `Equality` / `InList` / `Comparison` / `TextMatch` — every filter value normalises to exactly one |
| Centralised generic normaliser | `services/plan.py:generic_normalize_filter` | Handles all whitespace / case / underscore variants of NULL / NOT NULL / numeric / date / operator-prefixed strings ONCE. New variant = one-line add here, every column benefits. |
| Per-column normalisers | `services/columns/{bill_sent,paid,poc_email,date_columns}.py` | Authoritative. Returning `None` means "this value is invalid for this column" — surfaced as a `NormalisationError`, never silently coerced. |
| Plan dataclass | `services/plan.py:Plan` | Typed contract. `Plan.from_raw(raw, allowed_columns)` returns `PlanResult(plan, errors)`. |
| Shadow-mode wiring | `services/query_planner.py:execute_query_plan` | Validates every plan; logs mismatches as `[PLAN_VALIDATOR_SHADOW]`. Does NOT reject yet. |
| Property-based test contract | `tests/test_plan_model.py` (~70 tests) | For each semantic concept, EVERY plausible variant is asserted to collapse to the SAME `CanonicalFilter`. |

### What we get today

1. **Every production query that hits a shape the normaliser can't handle
   logs `[PLAN_VALIDATOR_SHADOW]`** with the column, raw value, and
   reason. We can grep production logs for the next 24-48 hours and
   discover any shapes the planner uses that we haven't anticipated —
   **without** risking false rejections of legitimate queries.
2. **A property-based suite that catches the bug class lexically.**
   ~70 variants per concept asserted to produce the same canonical
   filter. Adding a new variant the AI invents = one line in the
   `_NULL_VARIANTS` / `_BILL_SENT_TRUTHY` lists.
3. **No behaviour change.** The legacy SQL builder still runs. Shipping
   this commit cannot break anything in production.

## Phase 3b — next session

1. **Flip strict mode.** Set `STRICT_PLAN_VALIDATION=1` after observing
   shadow-mode logs for 24h. Any unanticipated shape is now a hard
   rejection.
2. **LLM retry loop.** When validation fails, re-prompt the planner with
   `PlanResult.feedback_for_retry()` as additional context. The planner
   gets one chance to correct itself; second failure → clarification
   question to user.
3. **Refactor `_build_filter_clause`** to consume `CanonicalFilter`
   directly. Every branch becomes a switch on the type:
   - `NullCheck(True)` → `"{col} IS NULL"`
   - `BoolCheck(True)` on bill_sent → the existing truthy SQL
   - `Equality(value)` → `"{col} = {literal}"`
   - etc.
   Delete the in-builder normalisation guesswork. Delete the legacy
   list-of-falsy-markers special cases — they live in the canonical
   types now.
4. **Add `column_name`, `client_name`, `fees`, `brand_name` column
   modules** so they participate in the registry. Today they fall
   through to the generic normaliser, which is correct but not
   semantic.

## Failure modes Path 3 still does NOT fix

Honesty check — Path 3 eliminates ONE class. It does not fix:

- **Semantically wrong filters** (planner adds a `poc_email` filter when
  none was asked). That's a prompt issue. The bill_sent prompt fragment
  already covers this case; we'd need to add similar guidance for new
  columns as they appear.
- **Synthesizer hallucination** ("I can't tell you" when the count IS
  the answer). That's the explicit AGGREGATE ANSWERS rule we added in
  the previous session — separate from Path 3.
- **Wholly new intents** (a user wants a feature we haven't categorised).
  Neither path helps; that's product work.

The win is bounded but real: **the SQL boundary stops being a guessing
game.** Every filter value crossing into SQL has a known canonical
shape, or the request gets rejected with an actionable error.

## Where to look first if Path 3 misbehaves

- `[PLAN_VALIDATOR_SHADOW]` lines in production logs — these are the
  shapes the planner is emitting that we haven't normalised. Each one
  is either (a) a new variant to add to the centralised normaliser, or
  (b) a genuine planner error we want strict mode to reject.
- `tests/test_plan_model.py::TestCrossVariantConsistency` — the
  property-style lemmas. If one of these fails, the centralised
  normaliser has a hole.
- `services/plan.py:normalize_filter` — the single entry point
  consulted by everything.
