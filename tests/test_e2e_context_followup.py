#!/usr/bin/env python3
"""
Live E2E — Category 6 gap: an aggregate-scope clarifying question reaching the
query planner WITH conversation history attached.

The specific untested shape (per HANDOFF discussion, 2026-07):
    Turn 1: "What's my total earning so far?"          -> an aggregate (SUM, no filter)
    Turn 2: "Do these include, paid and unpaid?"        -> a scope question about Turn 1

classify_operation() tags Turn 2 "query" (it just matches the trailing "?"),
so it flows into execute_query_plan() same as any fresh data request. Unlike
a fresh request, though, it's meaningless without Turn 1's context — "these"
has no referent on its own. execute_query_plan() DOES accept
conversation_history and forwards it to both classify_operation and
build_operation_plan, so the LLM has a chance to resolve it correctly. This
script checks whether it actually does, against the real model.

Reuses MOCK_ROWS / MockSupabaseService / TEST_USER_ID from test_e2e_live.py so
the dataset and grading assumptions stay consistent with the rest of the e2e
suite (unfiltered SUM of all 8 mock rows = 1,175,000; a "does this include
paid and unpaid" question about that number should be answered YES for both,
since no paid filter was ever applied).

Usage:
    AI_KEY=sk-or-v1-... python3 tests/test_e2e_context_followup.py
"""
import os
import re
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

AI_KEY = os.environ.get("AI_KEY", "").strip()
if not AI_KEY:
    print("ERROR: Set AI_KEY env variable before running.\n"
          "  AI_KEY=sk-or-v1-... python3 tests/test_e2e_context_followup.py")
    sys.exit(1)
os.environ["AI_KEY"] = AI_KEY
os.environ["STRICT_PLAN_VALIDATION"] = "1"

from services.gemini_service import GeminiService
from services.query_planner import execute_query_plan
from services.response_synthesis import build_clean_payload

# NOTE: do NOT cap maxOutputTokens below production's real per-call values
# (build_operation_plan uses 800). An earlier version of this script copied
# test_e2e_live.py's 700-token "budget-constrained key" cap and it produced a
# FALSE FAILURE here: the KnowledgeBook-grounded, history-attached prompt for
# this exact message needs ~150 tokens of visible JSON but apparently more
# thinking budget than 700 allows, and gets cut off mid-string at 700/1500 but
# completes cleanly (3/3) at the real production cap of 800. Confirmed this
# was a test-harness artifact, not a production bug, before writing this note.

from tests.test_e2e_live import MOCK_ROWS, MockSupabaseService, TEST_USER_ID

GREEN = "\033[92m"; RED = "\033[91m"; YELLOW = "\033[93m"
BOLD = "\033[1m"; RESET = "\033[0m"

EXPECTED_TOTAL = sum(r["fees"] for r in MOCK_ROWS)  # 1,175,000 — no paid filter


def run_turn(message, gemini, supabase, conversation_history=None):
    plan_result = execute_query_plan(message, gemini, supabase,
                                      conversation_history=conversation_history,
                                      user_id=TEST_USER_ID)
    if plan_result.get("_error") and "JSON" in str(plan_result.get("_error", "")):
        # Same one-retry-on-parse-flake convention as test_e2e_live.py — an
        # occasional truncated/garbled JSON response is a live-model flake,
        # not the thing under test here.
        time.sleep(1.5)
        plan_result = execute_query_plan(message, gemini, supabase,
                                          conversation_history=conversation_history,
                                          user_id=TEST_USER_ID)
    if plan_result.get("clarification"):
        return {"kind": "clarification", "text": plan_result["clarification"],
                "plan": plan_result.get("plan")}
    if plan_result.get("_error"):
        return {"kind": "error", "text": plan_result["_error"], "plan": plan_result.get("plan")}

    sql = plan_result.get("sql", "")
    db = supabase.execute_sql(sql)
    rows = db.get("rows", []) if db.get("ok") else []
    payload = build_clean_payload(rows, "select")
    response = gemini.synthesize_response(payload, message, conversation_history=conversation_history)
    return {"kind": "answer", "text": response, "sql": sql, "rows": rows, "plan": plan_result.get("plan")}


def grade_turn2(result):
    """Turn 2 must not silently narrow scope (paid-only or unpaid-only SQL) and
    must not deny that both are included. Neutral/deflecting-but-not-wrong
    responses (e.g. re-stating the total without a direct yes/no) are a WARN,
    not a FAIL — only an actual wrong claim or a silently narrowed SQL fails."""
    if result["kind"] != "answer":
        return "FAIL", f"Did not produce an answer ({result['kind']}): {result['text'][:150]}"

    sql = (result.get("sql") or "").upper()
    if re.search(r"\bPAID\s*=\s*'YES'|\bPAID\s+IS\s+NOT\s+NULL\b", sql) and "NOT IN" not in sql:
        return "FAIL", f"Turn 2 silently re-scoped to PAID-only: {sql[:200]}"
    if re.search(r"\bPAID\s+IS\s+NULL\b|\bPAID\s+NOT\s+IN\b", sql):
        return "FAIL", f"Turn 2 silently re-scoped to UNPAID-only: {sql[:200]}"

    text = (result["text"] or "").lower()
    denies_scope = bool(re.search(r"\bonly\s+(paid|unpaid)\b|\bjust\s+(the\s+)?(paid|unpaid)\b|\bdoes\s*n'?t\s+include\b|\bexcludes?\b", text))
    if denies_scope:
        return "FAIL", f"Response denies full scope: {result['text'][:200]}"

    confirms_scope = bool(re.search(r"\bboth\b.*\b(paid|unpaid)\b|\ball\s+jobs\b|\byes\b.*\binclud", text))
    if confirms_scope:
        return "PASS", result["text"][:200]
    return "WARN", f"Neutral — doesn't explicitly confirm scope, but doesn't deny it either: {result['text'][:200]}"


def main():
    print(f"\n{BOLD}{'='*76}{RESET}")
    print(f"{BOLD}  Live E2E — aggregate-scope follow-up with conversation history{RESET}")
    print(f"  Mock dataset total (unfiltered): {EXPECTED_TOTAL:,}")
    print(f"{'='*76}{RESET}\n")

    gemini = GeminiService()
    supabase = MockSupabaseService()

    turn1_msg = "What's my total earning so far?"
    print(f"{BOLD}Turn 1:{RESET} {turn1_msg}")
    t0 = time.time()
    r1 = run_turn(turn1_msg, gemini, supabase)
    print(f"  kind={r1['kind']}  ({time.time()-t0:.1f}s)")
    if r1["kind"] == "answer":
        print(f"  SQL:  {r1['sql'][:140]}")
        print(f"  Resp: {r1['text']}")
        t1_ok = str(EXPECTED_TOTAL) in r1["text"].replace(",", "") or f"{EXPECTED_TOTAL:,}" in r1["text"]
        print(f"  Turn 1 total correct: {GREEN + 'yes' + RESET if t1_ok else RED + 'NO — ' + str(EXPECTED_TOTAL) + RESET}")
    else:
        print(f"  {RED}{r1['text']}{RESET}")
        t1_ok = False

    conversation_history = [
        {"role": "user", "content": turn1_msg},
        {"role": "assistant", "content": r1.get("text") or ""},
    ]

    turn2_msg = "Do these include, paid and unpaid?"
    print(f"\n{BOLD}Turn 2:{RESET} {turn2_msg}  (with Turn 1 in conversation_history)")
    t0 = time.time()
    r2 = run_turn(turn2_msg, gemini, supabase, conversation_history=conversation_history)
    print(f"  kind={r2['kind']}  ({time.time()-t0:.1f}s)")
    if r2["kind"] == "answer":
        print(f"  Plan: {r2.get('plan')}")
        print(f"  SQL:  {r2['sql'][:200]}")
        print(f"  Resp: {r2['text']}")
    else:
        print(f"  Plan: {r2.get('plan')}")
        print(f"  {YELLOW}{r2['text']}{RESET}")

    verdict, detail = grade_turn2(r2)
    color = GREEN if verdict == "PASS" else (YELLOW if verdict == "WARN" else RED)
    print(f"\n{BOLD}  Verdict: {color}{verdict}{RESET}")
    print(f"  {detail}")
    print(f"{'='*76}\n")

    sys.exit(0 if verdict != "FAIL" else 1)


if __name__ == "__main__":
    main()
