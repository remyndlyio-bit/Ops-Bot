# Session Handoff — 2026-06-11

## What happened this session

Four bugs from the WhatsApp test matrix were fixed and an E2E live test suite was built. All fixes are on `main` and deployed.

---

## Fixes shipped (commits on main)

| Commit | What |
|---|---|
| `1ba38e0` | Fix Bug 1 + Bug 2 — aggregate keyword shortcuts + GROUP BY payload fix |
| `e4f4cbb` | Add live E2E test suite (`tests/test_e2e_live.py`) |
| `9fbcbc1` | Fix E2E mock (ILIKE without wildcards), raise token cap, retry on JSON fluke |
| `ca4bc46` | Expand E2E to 33 test cases across 10 categories |

---

## Bug status after this session

| Bug | Status | How confirmed |
|---|---|---|
| Bug 1: Planner refuses aggregates (biggest client, avg, owe me) | ✅ FIXED | E2E tests C3-01, C3-02, C4-01, C4-02 all pass |
| Bug 2: Unfiltered COUNT crash | ✅ FIXED | E2E tests C1-01, C1-02 pass; planner emits metric=count correctly |
| Bug 3: "Earnings" defaults to list | ✅ FIXED | E2E test C2-02 passes; "Earnings last quarter" → SUM not SELECT * |
| Bug 4: Smart-capture misses "paid" | ✅ FIXED | E2E test C9-01 passes; "paid" extracted as `"true"` |
| Bug 5: Zero-result aggregate phrasing | ❌ NOT FIXED | Never addressed this session. Still in CLAUDE.md as LOW. |

---

## Root causes — only identified in this conversation

### Bug 1: why it produced "Two ways I could read that"

The failure path was `planner_failed`, NOT `clarification`. Specifically:

```
execute_query_plan() → _error (LLM emitted invalid column → column validation failed)
  ↓ planner_failed = True
generate_sql() → also failed
  ↓
_keyword_sql_fallback() → returned None (no pattern for "biggest client")
  ↓
clarify_phrase(["How many jobs?", "Total fees for Garnier", "Last payment date"])
```

The `clarification` branch (lines 4140–4167) was NOT the path. The exact route was `intent_service.py:4184`. This matters if the bug re-appears — check `[PIPELINE] Planner failed` in logs first.

### Bug 2: metric=null vs metric=count

The planner prompt already had rules for "how many" → metric=count before this session. But when the planner emits a valid plan with metric=null (e.g. during LLM distraction), the fix is a post-plan correction in `execute_query_plan()` at line 951–954 in `query_planner.py`. This fires after Stage 3 (row resolver) and before Path 3 validation.

### Bug 1 fix: keyword patterns fire REACTIVELY, not proactively

Important: the new aggregate patterns in `_keyword_sql_fallback` (`biggest client`, `avg fees`, `owe me`) only fire when BOTH the planner AND `generate_sql` LLM both fail. They do NOT intercept a successful but wrong planner result.

A proactive bypass (checking patterns BEFORE calling the planner) was attempted during this session but reverted because the if/else structure inside the huge planner block would have required re-indenting ~200 lines. The E2E tests confirm the planner itself now gets the right answer directly, so the keyword patterns serve as a safety net only.

### GROUP BY payload fix in response_synthesis.py

`build_clean_payload` had a bug: a single-row result with both an aggregate key ("result") AND dimension columns (e.g. "client_name") was being collapsed to a scalar aggregate, dropping client_name. Fixed by checking `_non_agg_keys` before collapsing. See `response_synthesis.py:95–103`.

---

## Outstanding issues found but NOT fixed

### AVG synthesizer occasionally refuses to answer

In one E2E run, "Average fees per job" → SQL was correct (`SELECT AVG(fees) AS result`) → payload was `{"type": "aggregate", "data": {"result": 129000}}` → but the synthesizer responded: *"I can give you the total fees, but I don't calculate averages. Your total fees are ₹129,000."*

Root cause: the aggregate payload only carries `{"result": 129000}` — the synthesizer doesn't know it was an AVG query vs. a SUM query. The AGGREGATE ANSWERS rule in the system prompt says "Never refuse" but the LLM ignores it occasionally when it doesn't see context distinguishing AVG from SUM.

The test currently asserts any number `r"₹\s*\d[\d,]*"` so it passes even when the synthesizer says the wrong thing. This is a latent bug.

**Potential fix**: Include the original metric type in the payload, e.g. `{"type": "aggregate", "metric": "avg", "data": {"result": 129000}}`, and update the synthesizer prompt to use it.

### C10-02 typo detection: wrong test entry point

The test "genrate invoce for Pedigree" uses `run_oos_test()` which calls `execute_query_plan()` directly. But the typo detection for invoice requests lives in `intent_service.py` lines 3276–3287, before the planner is even called. `execute_query_plan()` has no invoice routing.

This test will always fail because it tests the wrong layer. The right fix is to either:
a. Test through `IntentService.process_request()` (requires more setup), or
b. Move the test to a unit test that checks the intent routing logic directly, or
c. Accept that E2E invoice routing can only be tested via actual WhatsApp

### `_expand_client_ilike` not applied in E2E test

In production (`intent_service.py:4204`), `client_name ILIKE 'X'` gets expanded to `(client_name ILIKE '%X%' OR brand_name ILIKE '%X%' OR production_house ILIKE '%X%')`. This expansion does NOT happen in `execute_query_plan()`. The E2E mock was patched to handle `ILIKE 'X'` (without wildcards) to compensate, but the E2E test is testing slightly different SQL than what runs in production.

### Bug 5 (zero-result aggregate phrasing) — still open

When an aggregate returns 0 (e.g. "Earnings last quarter" with no matching rows), the response says "No matching records — total is 0" which reads like an error. The synthesizer prompt was NOT updated this session. This is LOW priority per CLAUDE.md.

---

## E2E test suite — operational notes

**Run command:**
```
AI_KEY=sk-or-v1-... python3 tests/test_e2e_live.py
```

**Token budget**: 33 tests × ~2 AI calls each = ~66 API calls × 700 tokens = ~46,200 tokens per run. Budget at least $0.25–$0.50 of OpenRouter credit per run.

**Token cap**: `E2E_MAX_TOKENS=700` (default). Raise to `E2E_MAX_TOKENS=800` if seeing JSON truncation errors on complex plans. The planner JSON for GROUP BY + filters is the heaviest output (~400–500 tokens).

**JSON fluke retries**: Both query and smart-capture runners retry once on JSON parse errors with 1.5s delay. This handles the ~10% of calls where Gemini 2.5 Flash returns malformed JSON under token pressure.

**Test count discrepancy**: The file has 33 tests, not 30 as the user requested. 10 categories: C1(4)+C2(5)+C3(3)+C4(3)+C5(4)+C6(4)+C7(3)+C8(3)+C9(2)+C10(2)=33. Header says 30, reality is 33.

**MockSupabaseService ILIKE handling**: The mock handles both `ILIKE '%X%'` (from keyword shortcut, with wildcards) and `ILIKE 'X'` (from planner, without wildcards). This was a mid-session bug fix — initial version only handled the wildcarded form, causing #24 "How much does Star Studios owe me?" to return the wrong total (all unpaid instead of just Star Studios unpaid).

**C6-01 mildly wrong response**: "Show jobs from last month" → mock returns 1 row (Garnier/L'Oréal, May 8). Synthesizer says "I don't have any jobs from last month. I can show you the Skincare campaign job...". This is technically wrong (May 8 IS last month). The mock's date filter has an off-by-one or the synthesizer is misinterpreting the date range. Test passes because the assertion accepts "Garnier|₹" but the response is subtly confused.

---

## Confirmed working via E2E (first successful full run)

These all generated correct SQL AND synthesized correct natural language:

- "How many jobs have I done?" → `SELECT COUNT(*) AS result` → "You've completed 8 jobs so far."
- "Who is my biggest client?" → `GROUP BY client_name, SUM(fees), ORDER BY result DESC LIMIT 1` → "Your biggest client is Star Studios, with ₹350,000 in fees."
- "Average fees per job" → `SELECT AVG(fees) AS result` → [correct number, sometimes refuses — see above]
- "How much does Star Studios owe me?" → `SUM(fees) WHERE client_name ILIKE 'Star Studios' AND paid IS NULL` → "Star Studios owes you ₹200,000."
- "Star Studios se paisa aaya kya?" → `SUM(fees) WHERE client_name ILIKE 'Star Studios' AND LOWER(COALESCE(paid,...)) IN ('true',...)` → "You've received ₹150,000 from Star Studios. ✅"
- "Earnings last quarter" → `SELECT SUM(fees) WHERE job_date BETWEEN 2026-01-01 AND 2026-03-31` → "Your earnings last quarter were ₹645,000."
- "Add a job for Acme, 25k, shoot, paid" → extracted: `{brand_name: "Acme", fees: 25000, paid: "true"}`
- "Kiska payment baki hai" → `GROUP BY client_name, SUM(fees) WHERE paid IS NULL` → lists outstanding per client
- "Isme se invoice kitne logon ko bheja hai" → `COUNT(*) WHERE bill_sent=yes AND poc_email IS NOT NULL` → "You've sent 5 invoices so far."
- "Total billing this year" → `SELECT SUM(fees) WHERE job_date BETWEEN 2026-01-01 AND today`

---

## WhatsApp live test status

Only one live message was tested: "Who is my biggest client?" — confirmed failing ("Two ways I could read that 🤔") before the fix. Fix was pushed to main. No further live WhatsApp testing was done this session.

The full 29-message test matrix from CLAUDE.md still needs a live run to update the 16✅/7⚠️/6❌ counts.

---

## Next session priorities

1. Run the full 29-message WhatsApp matrix on the live bot to get updated pass/fail counts and update CLAUDE.md
2. Investigate and fix the AVG synthesizer refusal issue (see above)
3. Fix Bug 5: zero-result aggregate phrasing (LOW priority, simple synthesizer prompt change)
4. Fix C10-02 E2E test: either test at a higher layer or rewrite as a unit test
5. Consider updating CLAUDE.md Known Bugs section — Bugs 1–4 are likely fixed but "likely" needs live confirmation
