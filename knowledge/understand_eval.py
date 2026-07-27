"""
WP-2 of ASSISTANT_PLAN.md — Understand v2 shadow eval.

Calls the REAL classifier (services.classifier.classify) over ~60 hand-labelled
multi-turn cases and grades the two WP-2 fields — references_last_answer and
resolved_query — plus a sanity check that intent/historical didn't regress.
Mirrors knowledge/ab_run.py's conventions (checkpointing, retry-on-JSON-error,
CLI limit/arm flags) but there is no "oracle" here: verdict quality is a
judgment call, so cases are hand-labelled gold, not computed.

Cases are pulled from three sources, same as the plan specifies:
  - the exact production transcript that motivated WP-1/WP-2
    ("Do these include, paid and unpaid?")
  - this repo's own bug history (context-follow-up tests already written in
    tests/test_context_followup.py, tests/test_answer_ledger.py — the SAME
    scenarios, now graded against the LIVE model instead of the deterministic
    fallback)
  - fresh coverage for context-inheritance shapes (resolved_query) that
    nothing else in the suite exercises live

  AI_KEY=sk-or-... python -m knowledge.understand_eval                # full run
  AI_KEY=sk-or-... python -m knowledge.understand_eval --limit 10
  AI_KEY=sk-or-... python -m knowledge.understand_eval --category scope

Gate (ASSISTANT_PLAN.md WP-2 acceptance): >= 90% overall before UNDERSTAND_V2=1
is ever set. This file measures it; it does not flip anything.
"""
import os
import sys
import json
import time
import argparse
from typing import Any, Dict, List, Optional

from services.classifier import classify
from services.gemini_service import GeminiService
from services.answer_ledger import LedgerEntry

RESULTS_PATH = os.path.join(os.path.dirname(__file__), "understand_eval_results.json")


def _entry(question: str, kind: str, filters: Optional[Dict] = None,
           time_range: Optional[Dict] = None, value: Any = None) -> LedgerEntry:
    return LedgerEntry(question=question, kind=kind,
                        scope={"filters": filters or {}, "time_range": time_range},
                        value=value)


def _hist(*pairs):
    """pairs of (role, content) -> conversation_history shape."""
    return [{"role": r, "content": c} for r, c in pairs]


# ── Cases ─────────────────────────────────────────────────────────────────
# Each case: id, category, message, optional history/ledger, gold expectations.
# gold fields left as None are not checked (keeps cases focused).

def cases() -> List[Dict[str, Any]]:
    C = []

    # ══ Category: scope — references_last_answer must be True ═══════════
    C.append({
        "id": "scope-01", "category": "scope",
        "history": _hist(("user", "What's my total earning so far?"),
                          ("assistant", "Your total billing comes to ₹11,75,000!")),
        "ledger": [_entry("What's my total earning so far?", "aggregate", {}, None, 1175000)],
        "message": "Do these include, paid and unpaid?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-02", "category": "scope",
        "history": _hist(("user", "What's my total outstanding payment"),
                          ("assistant", "Your total outstanding payment is ₹75,000.")),
        "ledger": [_entry("What's my total outstanding payment", "aggregate", {"paid": "no"}, None, 75000)],
        "message": "are these both paid and unpaid?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-03", "category": "scope",
        "history": _hist(("user", "How much does Star Studios owe me?"),
                          ("assistant", "Star Studios owes you ₹200,000.")),
        "ledger": [_entry("How much does Star Studios owe me?", "aggregate", {"client_name": "Star Studios", "paid": "no"}, None, 200000)],
        "message": "is that only unpaid, or everything?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-04", "category": "scope",
        "history": _hist(("user", "total billing this year"), ("assistant", "₹5,00,000")),
        "ledger": [_entry("total billing this year", "aggregate", {}, {"type": "absolute", "value": {"start": "2026-01-01", "end": "2026-12-31"}}, 500000)],
        "message": "does that include this whole year or just part of it?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-05", "category": "scope",
        "history": _hist(("user", "how much have I billed Nike"), ("assistant", "₹3,00,000")),
        "ledger": [_entry("how much have I billed Nike", "aggregate", {"client_name": "Nike"}, None, 300000)],
        "message": "is that just Nike or all my clients?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-06-hi", "category": "scope",
        "history": _hist(("user", "meri total kamai kitni hai"), ("assistant", "₹11,75,000")),
        "ledger": [_entry("meri total kamai kitni hai", "aggregate", {}, None, 1175000)],
        "message": "isme paid aur unpaid dono shamil hai kya?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-07", "category": "scope",
        "history": _hist(("user", "show me unpaid invoices"), ("assistant", "You have 4 unpaid invoices.")),
        "ledger": [_entry("show me unpaid invoices", "list", {"paid": "no"}, None, 4)],
        "message": "does that include ones I haven't even sent yet?",
        "gold": {"references_last_answer": True},
    })

    C.append({
        "id": "scope-08", "category": "scope",
        "history": _hist(("user", "how many invoices have I sent"), ("assistant", "You've sent 8 invoices.")),
        "ledger": [_entry("how many invoices have I sent", "aggregate", {"bill_sent": "yes"}, None, 8)],
        "message": "does that count the ones I haven't been paid for too?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-09", "category": "scope",
        "history": _hist(("user", "average fee per job"), ("assistant", "₹1,46,875 on average.")),
        "ledger": [_entry("average fee per job", "aggregate", {}, None, 146875)],
        "message": "is that across every client or just recent ones?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-10", "category": "scope",
        "history": _hist(("user", "who is my biggest client"), ("assistant", "Maruti Suzuki, ₹9,50,000 total.")),
        "ledger": [_entry("who is my biggest client", "client", {}, None, "Maruti Suzuki")],
        "message": "is that by paid amount or total billed?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-11-hi", "category": "scope",
        "history": _hist(("user", "total unpaid kitna hai"), ("assistant", "₹50,000 unpaid hai")),
        "ledger": [_entry("total unpaid kitna hai", "aggregate", {"paid": "no"}, None, 50000)],
        "message": "ye sirf is saal ka hai ya hamesha ka",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "scope-12", "category": "scope",
        "history": _hist(("user", "show unpaid jobs for Nike"), ("assistant", "2 unpaid jobs for Nike.")),
        "ledger": [_entry("show unpaid jobs for Nike", "list", {"client_name": "Nike", "paid": "no"}, None, 2)],
        "message": "just Nike, or all my unpaid clients?",
        "gold": {"references_last_answer": True},
    })

    # ══ Category: not-scope — surface-similar but must be False ═════════
    # These guard against over-triggering: a question mentioning "paid" or
    # sounding reflective isn't automatically about the prior answer.
    C.append({
        "id": "notscope-01", "category": "not_scope",
        "history": _hist(("user", "What's my total earning so far?"), ("assistant", "₹11,75,000")),
        "ledger": [_entry("What's my total earning so far?", "aggregate", {}, None, 1175000)],
        "message": "show me Nike jobs",
        "gold": {"references_last_answer": False},
    })
    C.append({
        "id": "notscope-02", "category": "not_scope",
        "history": _hist(("user", "What's my total earning so far?"), ("assistant", "₹11,75,000")),
        "ledger": [_entry("What's my total earning so far?", "aggregate", {}, None, 1175000)],
        "message": "mark the Nike job as paid",
        "gold": {"references_last_answer": False, "intent": "WRITE_UPDATE"},
    })
    C.append({
        "id": "notscope-03", "category": "not_scope",
        "history": _hist(("user", "How many unpaid jobs do I have?"), ("assistant", "You have 3.")),
        "ledger": [_entry("How many unpaid jobs do I have?", "aggregate", {"paid": "no"}, None, 3)],
        "message": "how many paid jobs do I have",
        "gold": {"references_last_answer": False},
    })
    C.append({
        "id": "notscope-04", "category": "not_scope",
        "history": _hist(("user", "total for Nike"), ("assistant", "₹3,00,000")),
        "ledger": [_entry("total for Nike", "aggregate", {"client_name": "Nike"}, None, 300000)],
        "message": "what about Garnier",
        "gold": {"references_last_answer": False},
    })
    C.append({
        "id": "notscope-05", "category": "not_scope",
        "history": [],
        "ledger": None,
        "message": "Do these include, paid and unpaid?",
        # No prior answer exists at all -- nothing to reference.
        "gold": {"references_last_answer": False},
    })

    C.append({
        "id": "notscope-06", "category": "not_scope",
        "history": _hist(("user", "How much does Star Studios owe me?"), ("assistant", "₹200,000.")),
        "ledger": [_entry("How much does Star Studios owe me?", "aggregate", {"client_name": "Star Studios", "paid": "no"}, None, 200000)],
        "message": "generate an invoice for them",
        "gold": {"references_last_answer": False, "intent": "WRITE_INVOICE"},
    })
    C.append({
        "id": "notscope-07", "category": "not_scope",
        "history": _hist(("user", "total billing this year"), ("assistant", "₹5,00,000")),
        "ledger": [_entry("total billing this year", "aggregate", {}, None, 500000)],
        "message": "thanks!",
        "gold": {"references_last_answer": False, "intent": "SMALL_TALK"},
    })
    C.append({
        "id": "notscope-08", "category": "not_scope",
        "history": _hist(("user", "average fee per job"), ("assistant", "₹1,46,875")),
        "ledger": [_entry("average fee per job", "aggregate", {}, None, 146875)],
        "message": "what's the average for Nike specifically",
        # A NEW, differently-scoped aggregate question -- not asking about the
        # composition of the prior number.
        "gold": {"references_last_answer": False},
    })
    C.append({
        "id": "notscope-09-hi", "category": "not_scope",
        "history": _hist(("user", "kitne unpaid jobs hai"), ("assistant", "3 unpaid jobs hai")),
        "ledger": [_entry("kitne unpaid jobs hai", "aggregate", {"paid": "no"}, None, 3)],
        "message": "Nike ka invoice bhejo",
        "gold": {"references_last_answer": False, "intent": "WRITE_INVOICE"},
    })

    # ══ Category: historical vs references_last_answer (must be distinct) ═
    C.append({
        "id": "hist-01", "category": "historical",
        "history": _hist(("user", "what's Nike's fee"), ("assistant", "₹50,000")),
        "ledger": [_entry("what's Nike's fee", "field", {"client_name": "Nike"}, None, 50000)],
        "message": "what was it before we changed it?",
        "gold": {"historical": True, "references_last_answer": False},
    })
    C.append({
        "id": "hist-02", "category": "historical",
        "history": _hist(("user", "Nike job status"), ("assistant", "Unpaid, ₹50,000")),
        "ledger": [_entry("Nike job status", "field", {"client_name": "Nike"}, None, "Unpaid")],
        "message": "when did this become unpaid",
        "gold": {"historical": True},
    })
    C.append({
        "id": "hist-03", "category": "historical",
        "history": _hist(("user", "What's my total earning so far?"), ("assistant", "₹11,75,000")),
        "ledger": [_entry("What's my total earning so far?", "aggregate", {}, None, 1175000)],
        "message": "does that include paid and unpaid?",
        # This IS references_last_answer, NOT historical -- asks about the
        # CURRENT figure's composition, not a past value.
        "gold": {"references_last_answer": True, "historical": False},
    })

    C.append({
        "id": "hist-04", "category": "historical",
        "history": _hist(("user", "Garnier ka fee kitna hai"), ("assistant", "₹80,000")),
        "ledger": [_entry("Garnier ka fee kitna hai", "field", {"client_name": "Garnier"}, None, 80000)],
        "message": "pehle kitna tha",
        "gold": {"historical": True},
    })
    C.append({
        "id": "hist-05", "category": "historical",
        "history": _hist(("user", "is this job paid"), ("assistant", "Yes, paid.")),
        "ledger": [_entry("is this job paid", "field", {}, None, "Yes")],
        "message": "when did it get marked paid",
        "gold": {"historical": True},
    })
    C.append({
        "id": "hist-06", "category": "historical",
        "history": _hist(("user", "what's the fee for the Nike job"), ("assistant", "₹40,000")),
        "ledger": [_entry("what's the fee for the Nike job", "field", {"client_name": "Nike"}, None, 40000)],
        "message": "has it always been that amount?",
        "gold": {"historical": True},
    })

    # ══ Category: resolved_query — context inheritance ═══════════════════
    C.append({
        "id": "resolve-01", "category": "resolve",
        "history": _hist(("user", "show jobs for Nike"), ("assistant", "3 jobs for Nike.")),
        "ledger": [_entry("show jobs for Nike", "list", {"client_name": "Nike"}, None, 3)],
        "message": "what about this month?",
        "gold": {"intent": "READ_QUERY", "resolved_query_client": "Nike"},
    })
    C.append({
        "id": "resolve-02", "category": "resolve",
        "history": _hist(("user", "total billing for Garnier"), ("assistant", "₹4,00,000")),
        "ledger": [_entry("total billing for Garnier", "aggregate", {"client_name": "Garnier"}, None, 400000)],
        "message": "and last quarter?",
        "gold": {"intent": "READ_AGGREGATE", "resolved_query_client": "Garnier",
                  "resolved_query_metric": "sum"},
    })
    C.append({
        "id": "resolve-03", "category": "resolve",
        "history": _hist(("user", "how many jobs for Samsung"), ("assistant", "5 jobs.")),
        "ledger": [_entry("how many jobs for Samsung", "aggregate", {"client_name": "Samsung"}, None, 5)],
        "message": "what about unpaid ones",
        "gold": {"resolved_query_client": "Samsung"},
    })
    C.append({
        "id": "resolve-04", "category": "resolve",
        "history": _hist(("user", "show all my jobs"), ("assistant", "Here are your 12 jobs...")),
        "ledger": [_entry("show all my jobs", "list", {}, None, 12)],
        "message": "the first one, mark it paid",
        "gold": {"intent": "WRITE_UPDATE"},
    })
    C.append({
        "id": "resolve-05", "category": "resolve",
        "history": [],
        "ledger": None,
        "message": "total billing for Nike in March",
        # Fully self-contained -- nothing to inherit, resolved_query should
        # stay null (this guards against the classifier over-eagerly filling
        # it in for every READ_AGGREGATE regardless of need).
        "gold": {"intent": "READ_AGGREGATE", "resolved_query_is_none_or_matches": True},
    })

    C.append({
        "id": "resolve-06", "category": "resolve",
        "history": _hist(("user", "unpaid jobs for Samsung"), ("assistant", "2 unpaid jobs.")),
        "ledger": [_entry("unpaid jobs for Samsung", "list", {"client_name": "Samsung", "paid": "no"}, None, 2)],
        "message": "and paid ones?",
        "gold": {"resolved_query_client": "Samsung"},
    })
    C.append({
        "id": "resolve-07", "category": "resolve",
        "history": _hist(("user", "jobs for Garnier in Q1"), ("assistant", "5 jobs.")),
        "ledger": [_entry("jobs for Garnier in Q1", "list",
                          {"client_name": "Garnier"},
                          {"type": "absolute", "value": {"start": "2026-01-01", "end": "2026-03-31"}}, 5)],
        "message": "what about Q2",
        "gold": {"resolved_query_client": "Garnier"},
    })
    C.append({
        "id": "resolve-08-hi", "category": "resolve",
        "history": _hist(("user", "Nike ke kitne jobs hai"), ("assistant", "3 jobs hai")),
        "ledger": [_entry("Nike ke kitne jobs hai", "aggregate", {"client_name": "Nike"}, None, 3)],
        "message": "is mahine ka kya",
        "gold": {"resolved_query_client": "Nike"},
    })
    C.append({
        "id": "resolve-09", "category": "resolve",
        "history": _hist(("user", "how much has Nike paid me"), ("assistant", "₹1,50,000")),
        "ledger": [_entry("how much has Nike paid me", "aggregate", {"client_name": "Nike", "paid": "yes"}, None, 150000)],
        "message": "and how much do they still owe",
        "gold": {"resolved_query_client": "Nike"},
    })
    C.append({
        "id": "resolve-10", "category": "resolve",
        "history": [],
        "ledger": None,
        "message": "how many jobs this quarter",
        "gold": {"intent": "READ_AGGREGATE", "resolved_query_is_none_or_matches": True},
    })

    # ══ Category: baseline — plain new queries, no context needed ════════
    for i, (msg, intent) in enumerate([
        ("how many jobs do I have?", "READ_AGGREGATE"),
        ("show me Nike jobs", "READ_QUERY"),
        ("total billing this year", "READ_AGGREGATE"),
        ("mark this as paid", "WRITE_UPDATE"),
        ("add a job for Acme, 25k, shoot", "WRITE_CREATE"),
        ("delete my last job", "WRITE_DELETE"),
        ("generate invoice for Nike", "WRITE_INVOICE"),
        ("can you send recurring invoices", "FEATURE_QUESTION"),
        ("hi", "SMALL_TALK"),
        ("tell me a joke", "UNKNOWN"),
        ("who is my biggest client", "READ_AGGREGATE"),
        ("can you book me an Uber", "UNKNOWN"),
    ], start=1):
        C.append({
            "id": f"base-{i:02d}", "category": "baseline",
            "history": [], "ledger": None, "message": msg,
            "gold": {"intent": intent, "references_last_answer": False},
        })

    # ══ Category: Hinglish follow-ups ═════════════════════════════════════
    C.append({
        "id": "hi-01", "category": "hinglish",
        "history": _hist(("user", "Nike ka total kitna hai"), ("assistant", "₹3,00,000")),
        "ledger": [_entry("Nike ka total kitna hai", "aggregate", {"client_name": "Nike"}, None, 300000)],
        "message": "isme paid wale bhi hai?",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "hi-02", "category": "hinglish",
        "history": _hist(("user", "kitne unpaid jobs hai"), ("assistant", "3 unpaid jobs hai")),
        "ledger": [_entry("kitne unpaid jobs hai", "aggregate", {"paid": "no"}, None, 3)],
        "message": "aur is mahine ka kya",
        "gold": {"intent": "READ_AGGREGATE"},
    })
    C.append({
        "id": "hi-03", "category": "hinglish",
        "history": _hist(("user", "Star Studios ka total kitna hai"), ("assistant", "₹4,50,000")),
        "ledger": [_entry("Star Studios ka total kitna hai", "aggregate", {"client_name": "Star Studios"}, None, 450000)],
        "message": "sirf Star Studios ka ya sab clients ka",
        "gold": {"references_last_answer": True},
    })
    C.append({
        "id": "hi-04", "category": "hinglish",
        "history": _hist(("user", "kitna baki hai"), ("assistant", "₹75,000 baki hai")),
        "ledger": [_entry("kitna baki hai", "aggregate", {"paid": "no"}, None, 75000)],
        "message": "Nike ko invoice bhej do",
        "gold": {"references_last_answer": False, "intent": "WRITE_INVOICE"},
    })
    C.append({
        "id": "hi-05", "category": "hinglish",
        "history": _hist(("user", "Garnier ke jobs dikhao"), ("assistant", "Garnier ke 6 jobs hai")),
        "ledger": [_entry("Garnier ke jobs dikhao", "list", {"client_name": "Garnier"}, None, 6)],
        "message": "pichle mahine ka kya",
        "gold": {"resolved_query_client": "Garnier"},
    })

    return C


# ── Grading ───────────────────────────────────────────────────────────────

def grade(verdict: Optional[Dict], gold: Dict[str, Any]) -> List[str]:
    """Return a list of failure reasons (empty = pass)."""
    if verdict is None:
        return ["classifier returned None (call failed or unparseable)"]
    fails = []
    if "intent" in gold and gold["intent"] is not None:
        if verdict.get("intent") != gold["intent"]:
            fails.append(f"intent: got {verdict.get('intent')!r} want {gold['intent']!r}")
    if "references_last_answer" in gold:
        if bool(verdict.get("references_last_answer")) != gold["references_last_answer"]:
            fails.append(
                f"references_last_answer: got {verdict.get('references_last_answer')} "
                f"want {gold['references_last_answer']}"
            )
    if "historical" in gold:
        if bool(verdict.get("historical")) != gold["historical"]:
            fails.append(f"historical: got {verdict.get('historical')} want {gold['historical']}")
    if "resolved_query_client" in gold:
        rq = verdict.get("resolved_query") or {}
        got_client = (rq.get("client_name") or "").strip().lower()
        want_client = (gold["resolved_query_client"] or "").strip().lower()
        # Empty-string substring check ("" in "anything" is True in Python)
        # would silently pass a missing/empty client_name against any
        # non-empty gold — must fail explicitly when the model returned
        # nothing but a client was expected.
        if not got_client or (want_client not in got_client and got_client not in want_client):
            fails.append(f"resolved_query.client_name: got {rq.get('client_name')!r} want {gold['resolved_query_client']!r}")
    if "resolved_query_metric" in gold:
        rq = verdict.get("resolved_query") or {}
        if rq.get("metric_hint") != gold["resolved_query_metric"]:
            fails.append(f"resolved_query.metric_hint: got {rq.get('metric_hint')!r} want {gold['resolved_query_metric']!r}")
    return fails


def run_case(case: Dict, gemini) -> Dict:
    t0 = time.time()
    verdict = classify(
        case["message"], gemini,
        conversation_history=case.get("history"),
        schema_summary="id, client_name, brand_name, job_date, fees, paid, bill_sent, invoice_date, poc_email",
        ledger_entries=case.get("ledger"),
    )
    if verdict is None:
        # One retry on a JSON flake, matching ab_run.py / test_e2e_live.py convention.
        time.sleep(1.5)
        verdict = classify(
            case["message"], gemini,
            conversation_history=case.get("history"),
            schema_summary="id, client_name, brand_name, job_date, fees, paid, bill_sent, invoice_date, poc_email",
            ledger_entries=case.get("ledger"),
        )
    fails = grade(verdict, case["gold"])
    return {
        "id": case["id"], "category": case["category"], "message": case["message"],
        "gold": case["gold"], "verdict": verdict, "pass": not fails, "fails": fails,
        "elapsed": time.time() - t0,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--category", type=str, default=None)
    args = ap.parse_args()

    ai_key = os.environ.get("AI_KEY", "").strip()
    if not ai_key:
        print("ERROR: Set AI_KEY.\n  AI_KEY=sk-or-... python -m knowledge.understand_eval")
        sys.exit(1)

    gemini = GeminiService()
    all_cases = cases()
    if args.category:
        all_cases = [c for c in all_cases if c["category"] == args.category]
    if args.limit:
        all_cases = all_cases[: args.limit]

    results = []
    for i, c in enumerate(all_cases, 1):
        r = run_case(c, gemini)
        results.append(r)
        mark = "PASS" if r["pass"] else "FAIL"
        print(f"[{i}/{len(all_cases)}] {mark} {c['id']:<14} ({c['category']:<10}) {c['message'][:60]}")
        if not r["pass"]:
            for f in r["fails"]:
                print(f"      -> {f}")
        with open(RESULTS_PATH, "w") as f:
            json.dump(results, f, indent=2, default=str)

    passed = sum(1 for r in results if r["pass"])
    total = len(results)
    print(f"\n{'='*70}")
    print(f"OVERALL: {passed}/{total} ({100*passed/total:.0f}%)" if total else "no cases run")
    by_cat: Dict[str, List[Dict]] = {}
    for r in results:
        by_cat.setdefault(r["category"], []).append(r)
    for cat, rs in sorted(by_cat.items()):
        cp = sum(1 for r in rs if r["pass"])
        print(f"  {cat:<12} {cp}/{len(rs)}")
    print(f"\nGATE (ASSISTANT_PLAN.md WP-2): >= 90% required before UNDERSTAND_V2=1.")
    print(f"Results written to {RESULTS_PATH}")


if __name__ == "__main__":
    main()
