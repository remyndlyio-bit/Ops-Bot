"""
KnowledgeBook — HARD held-out evaluation set (50 cases).

Purpose
-------
A/B the KnowledgeBook: run each question through the planner with
``KNOWLEDGE_BOOK`` OFF vs ON, then grade the planner's final answer against
the oracle-computed gold answer here. If KB grounding helps, KB-on should
beat KB-off on this set.

Why held-out
------------
These questions are DELIBERATELY NOT in ``knowledge/examples.jsonl`` (the KB
corpus). If they were in-corpus, KB-on could win by memorisation rather than
generalisation. Every phrasing here is chosen to be *hard*:

  * unpaid = paid IS NULL, never the literal 'No'            (owe/baaki/fasa)
  * a brand term must widen to its billing client            (Pepsi -> Content Lab)
  * "sent" needs bill_sent alone, NOT poc_email IS NOT NULL  (the over-reasoning bug)
  * indirect value verbs ("owe me", "outstanding", "into me for") -> unpaid SUM
  * Hinglish idioms the model mis-parses                     (kitna fasa hai, baaki)
  * compound filters (client + unpaid + date) the planner tends to drop
  * relative dates resolved against 2026-07-02 (today)

Ground truth is COMPUTED, never hand-labelled: each case carries a gold
``plan`` (planner-native shape, see ``oracle.py``) and the oracle fills the
answer from the seeded dataset. Change the dataset and the gold answers stay
correct.

Usage
-----
    python -m knowledge.eval_hard              # validate + print the table
    python -m knowledge.eval_hard --dump       # write knowledge/eval_hard.jsonl

    from knowledge.eval_hard import cases
    for c in cases():
        planner_answer = run_planner(c["question"], kb=...)  # your A/B harness
        grade(planner_answer, c["answer"])                   # vs oracle gold
"""
import os
import json
from typing import Any, Dict, List

from knowledge.dataset import build_dataset
from knowledge.oracle import compute_answer

JSONL_PATH = os.path.join(os.path.dirname(__file__), "eval_hard.jsonl")

# Today is 2026-07-02 (see currentDate). Relative windows resolved to absolute
# ranges so the oracle (which only understands absolute time_range) can grade.
_LAST_QUARTER = {"start": "2026-04-01", "end": "2026-06-30"}   # Q2 2026
_Q1_2026 = {"start": "2026-01-01", "end": "2026-03-31"}
_THIS_YEAR = {"start": "2026-01-01", "end": "2026-12-31"}
_LAST_YEAR = {"start": "2025-01-01", "end": "2025-12-31"}
_LAST_MONTH = {"start": "2026-06-01", "end": "2026-06-30"}     # June 2026
_THIS_MONTH = {"start": "2026-07-01", "end": "2026-07-31"}     # July 2026 -> empty
_FIRST_HALF = {"start": "2026-01-01", "end": "2026-06-30"}


def _abs(rng: Dict) -> Dict:
    return {"type": "absolute", "value": rng}


def _plan(metric=None, column=None, filters=None, time_range=None,
          group_by=None, order=None, limit=None) -> Dict:
    return {"metric": metric, "column": column, "filters": filters or {},
            "time_range": time_range, "group_by": group_by,
            "order": order, "limit": limit}


# ── The 50 hard cases: (id, question, gold_plan, tags, lang) ──────────────────
# Answers are computed by the oracle at load time (see cases()).
_CASES = [
    # ── A. Unpaid semantics / indirect "owe me" verbs (7) ────────────────────
    ("owe-01", "How much does Star Studios owe me?",
     _plan("sum", "fees", {"client_name": "Star Studios", "paid": "no"}),
     ["unpaid", "client", "sum", "indirect"], "en"),
    ("owe-02", "What's still outstanding from Samsung?",
     _plan("sum", "fees", {"client_name": "Samsung", "paid": "no"}),
     ["unpaid", "client", "sum", "indirect"], "en"),
    ("owe-03", "How much is Maruti into me for?",
     _plan("sum", "fees", {"client_name": "Maruti", "paid": "no"}),
     ["unpaid", "client", "sum", "idiom"], "en"),
    ("owe-04", "Total money I'm still waiting on",
     _plan("sum", "fees", {"paid": "no"}),
     ["unpaid", "sum", "bare"], "en"),
    ("owe-05", "How many jobs haven't been paid yet?",
     _plan("count", None, {"paid": "no"}),
     ["unpaid", "count"], "en"),
    ("owe-06", "Which of my Garnier jobs are still unpaid?",
     _plan(None, None, {"client_name": "Garnier", "paid": "no"}),
     ["unpaid", "client", "list"], "en"),
    ("owe-07", "How much have I actually collected from Content Lab?",
     _plan("sum", "fees", {"client_name": "Content Lab", "paid": "yes"}),
     ["paid", "client", "sum", "indirect"], "en"),

    # ── B. Brand term must widen to billing client (6) ───────────────────────
    ("brand-01", "Total billing for Pepsi",
     _plan("sum", "fees", {"client_name": "Pepsi"}),
     ["brand-vs-client", "sum"], "en"),
    ("brand-02", "How many Nike jobs have I done?",
     _plan("count", None, {"client_name": "Nike"}),
     ["brand-vs-client", "count"], "en"),
    ("brand-03", "What do I still need to collect on the Swiggy work?",
     _plan("sum", "fees", {"client_name": "Swiggy", "paid": "no"}),
     ["brand-vs-client", "unpaid", "sum"], "en"),
    ("brand-04", "Cadbury ka total kitna hua",
     _plan("sum", "fees", {"client_name": "Cadbury"}),
     ["brand-vs-client", "sum"], "hi"),
    ("brand-05", "Show me every Lays project",
     _plan(None, None, {"client_name": "Lays"}),
     ["brand-vs-client", "list"], "en"),
    ("brand-06", "Average fee on Adidas shoots",
     _plan("avg", "fees", {"client_name": "Adidas"}),
     ["brand-vs-client", "avg"], "en"),

    # ── C. Compound filters the planner tends to drop (8) ────────────────────
    ("comp-01", "Unpaid Star Studios jobs from this year",
     _plan(None, None, {"client_name": "Star Studios", "paid": "no"}, _abs(_THIS_YEAR)),
     ["compound", "client", "unpaid", "date"], "en"),
    ("comp-02", "How much does Garnier still owe me this year?",
     _plan("sum", "fees", {"client_name": "Garnier", "paid": "no"}, _abs(_THIS_YEAR)),
     ["compound", "client", "unpaid", "date", "sum"], "en"),
    ("comp-03", "Count of unpaid jobs in the last quarter",
     _plan("count", None, {"paid": "no"}, _abs(_LAST_QUARTER)),
     ["compound", "unpaid", "date", "count"], "en"),
    ("comp-04", "Paid Samsung work in Q1",
     _plan(None, None, {"client_name": "Samsung", "paid": "yes"}, _abs(_Q1_2026)),
     ["compound", "client", "paid", "date"], "en"),
    ("comp-05", "How much did I bill Maruti last year?",
     _plan("sum", "fees", {"client_name": "Maruti"}, _abs(_LAST_YEAR)),
     ["compound", "client", "date", "sum"], "en"),
    ("comp-06", "Unpaid invoices I've already sent",
     _plan(None, None, {"bill_sent": "yes", "paid": "no"}),
     ["compound", "bill_sent", "unpaid"], "en"),
    ("comp-07", "Total owed to me on invoices I've already raised this year",
     _plan("sum", "fees", {"bill_sent": "yes", "paid": "no"}, _abs(_THIS_YEAR)),
     ["compound", "bill_sent", "unpaid", "date", "sum"], "en"),
    ("comp-08", "How many paid Pepsi jobs in the first half of this year?",
     _plan("count", None, {"client_name": "Pepsi", "paid": "yes"}, _abs(_FIRST_HALF)),
     ["compound", "brand-vs-client", "paid", "date", "count"], "en"),

    # ── D. bill_sent vs paid distinction, incl. the over-reasoning trap (6) ──
    ("sent-01", "How many invoices have I sent?",
     _plan("count", None, {"bill_sent": "yes"}),
     ["bill_sent", "count", "no-poc_email-trap"], "en"),
    ("sent-02", "How many invoices are still pending to send?",
     _plan("count", None, {"bill_sent": "no"}),
     ["bill_sent", "count"], "en"),
    ("sent-03", "Show jobs where the invoice went out but I haven't been paid",
     _plan(None, None, {"bill_sent": "yes", "paid": "no"}),
     ["bill_sent", "unpaid", "list"], "en"),
    ("sent-04", "Total value of invoices I've raised",
     _plan("sum", "fees", {"bill_sent": "yes"}),
     ["bill_sent", "sum", "no-poc_email-trap"], "en"),
    ("sent-05", "Which jobs haven't been invoiced yet?",
     _plan(None, None, {"bill_sent": "no"}),
     ["bill_sent", "list"], "en"),
    ("sent-06", "How many Samsung invoices have gone out?",
     _plan("count", None, {"client_name": "Samsung", "bill_sent": "yes"}),
     ["bill_sent", "client", "count", "no-poc_email-trap"], "en"),

    # ── E. Hinglish idioms (9) ───────────────────────────────────────────────
    ("hi-01", "Star Studios ka kitna paisa fasa hua hai",
     _plan("sum", "fees", {"client_name": "Star Studios", "paid": "no"}),
     ["hinglish", "unpaid", "client", "sum", "idiom"], "hi"),
    ("hi-02", "Kiska kiska payment abhi baaki hai",
     _plan(None, None, {"paid": "no"}),
     ["hinglish", "unpaid", "list"], "hi"),
    ("hi-03", "Is saal ki total kamai kitni hui",
     _plan("sum", "fees", {}, _abs(_THIS_YEAR)),
     ["hinglish", "date", "sum"], "hi"),
    ("hi-04", "Pichhle mahine kitne kaam kiye",
     _plan("count", None, {}, _abs(_LAST_MONTH)),
     ["hinglish", "date", "count"], "hi"),
    ("hi-05", "Garnier ke kitne projects ka paisa nahi aaya",
     _plan("count", None, {"client_name": "Garnier", "paid": "no"}),
     ["hinglish", "unpaid", "client", "count"], "hi"),
    ("hi-06", "Kitne logon ko invoice bhej diya hai",
     _plan("count", None, {"bill_sent": "yes"}),
     ["hinglish", "bill_sent", "count", "no-poc_email-trap"], "hi"),
    ("hi-07", "Pepsi wale kaam ka total kitna banta hai",
     _plan("sum", "fees", {"client_name": "Pepsi"}),
     ["hinglish", "brand-vs-client", "sum"], "hi"),
    ("hi-08", "Sabse zyada paisa kaun sa client deta hai",
     _plan("sum", "fees", {}, None, "client_name", "desc", 1),
     ["hinglish", "group_by", "top-client"], "hi"),
    ("hi-09", "Ek job ka average kitna milta hai",
     _plan("avg", "fees", {}),
     ["hinglish", "avg"], "hi"),

    # ── F. Aggregates: biggest client, average, ranking (6) ──────────────────
    ("agg-01", "Who is my biggest client by revenue?",
     _plan("sum", "fees", {}, None, "client_name", "desc", 1),
     ["group_by", "top-client"], "en"),
    ("agg-02", "Which client has paid me the most?",
     _plan("sum", "fees", {"paid": "yes"}, None, "client_name", "desc", 1),
     ["group_by", "top-client", "paid"], "en"),
    ("agg-03", "Rank my clients by how much they still owe",
     _plan("sum", "fees", {"paid": "no"}, None, "client_name", "desc", None),
     ["group_by", "ranking", "unpaid"], "en"),
    ("agg-04", "What's my average job fee?",
     _plan("avg", "fees", {}),
     ["avg"], "en"),
    ("agg-05", "Average fee on the jobs I've already been paid for",
     _plan("avg", "fees", {"paid": "yes"}),
     ["avg", "paid"], "en"),
    ("agg-06", "Which client brings me the least total revenue?",
     _plan("sum", "fees", {}, None, "client_name", "asc", 1),
     ["group_by", "bottom-client"], "en"),

    # ── G. Date ranges incl. zero-result phrasing (5) ────────────────────────
    ("date-01", "Earnings last quarter",
     _plan("sum", "fees", {}, _abs(_LAST_QUARTER)),
     ["date", "sum", "value-word"], "en"),
    ("date-02", "How much did I make this month?",
     _plan("sum", "fees", {}, _abs(_THIS_MONTH)),
     ["date", "sum", "zero-result"], "en"),
    ("date-03", "Jobs in Q1 this year",
     _plan(None, None, {}, _abs(_Q1_2026)),
     ["date", "list"], "en"),
    ("date-04", "Total billing for the first half of the year",
     _plan("sum", "fees", {}, _abs(_FIRST_HALF)),
     ["date", "sum"], "en"),
    ("date-05", "How many jobs did I do last year?",
     _plan("count", None, {}, _abs(_LAST_YEAR)),
     ["date", "count"], "en"),

    # ── H. poc_email edges (3) ───────────────────────────────────────────────
    ("poc-01", "Which clients don't have an email on file?",
     _plan(None, None, {"poc_email": "null"}),
     ["poc_email", "null", "list"], "en"),
    ("poc-02", "How many jobs are missing a contact email?",
     _plan("count", None, {"poc_email": "null"}),
     ["poc_email", "null", "count"], "en"),
    ("poc-03", "How many invoices did I send to clients that have no email?",
     _plan("count", None, {"bill_sent": "yes", "poc_email": "null"}),
     ["poc_email", "null", "bill_sent", "count", "multi-filter"], "en"),
]


def cases(seed: int = 42, n: int = 120) -> List[Dict[str, Any]]:
    """Return the 50 hard cases with oracle-computed gold answers.

    Each item: {id, question, plan, answer, tags, lang}. The A/B harness runs
    the planner on ``question`` (KB off vs on) and grades its answer against
    ``answer`` (the oracle's ground truth over the seeded dataset)."""
    rows = build_dataset(seed=seed, n=n)
    out = []
    for cid, q, plan, tags, lang in _CASES:
        out.append({
            "id": cid,
            "question": q,
            "plan": plan,
            "answer": compute_answer(plan, rows),
            "tags": tags,
            "source": "hard-eval",
            "lang": lang,
        })
    return out


def dump_jsonl(path: str = JSONL_PATH) -> str:
    data = cases()
    with open(path, "w") as f:
        for c in data:
            f.write(json.dumps(c) + "\n")
    return path


def _validate(data: List[Dict]) -> List[str]:
    """Cheap sanity checks so a malformed gold plan is caught before an A/B run.
    Flags: duplicate ids, and answers that are degenerate for their type in a
    way that usually means the plan is wrong (empty ranking, zero count on a
    non-zero-result case, etc.). Zero is legitimate for the tagged case."""
    warnings = []
    seen = set()
    for c in data:
        if c["id"] in seen:
            warnings.append(f"{c['id']}: duplicate id")
        seen.add(c["id"])
        a = c["answer"]
        zero_ok = "zero-result" in c["tags"]
        if a["type"] in ("count", "money") and a["value"] == 0 and not zero_ok:
            warnings.append(f"{c['id']}: {a['type']} == 0 (plan likely wrong): {c['question']!r}")
        if a["type"] == "list" and a["value"] == 0 and not zero_ok:
            warnings.append(f"{c['id']}: empty list (plan likely wrong): {c['question']!r}")
        if a["type"] == "ranking" and not a["value"]:
            warnings.append(f"{c['id']}: empty ranking: {c['question']!r}")
        if a["type"] == "client" and not a.get("value"):
            warnings.append(f"{c['id']}: no top client: {c['question']!r}")
    return warnings


if __name__ == "__main__":
    import sys

    data = cases()
    if "--dump" in sys.argv:
        print(f"Wrote {len(data)} cases -> {dump_jsonl()}")
        sys.exit(0)

    def _fmt(a):
        t = a["type"]
        if t == "count":
            return f"count={a['value']}"
        if t == "money":
            return f"Rs {a['value']:,}"
        if t == "client":
            return f"top={a['value']} (Rs {a['amount']:,})"
        if t == "ranking":
            return f"ranking[{len(a['value'])}] top={a['value'][0]['client_name'] if a['value'] else '-'}"
        if t == "list":
            return f"list rows={a['value']} clients={len(a['clients'])}"
        return str(a)

    print(f"{'ID':<10} {'LANG':<4} {'GOLD ANSWER':<34} QUESTION")
    print("-" * 100)
    for c in data:
        print(f"{c['id']:<10} {c['lang']:<4} {_fmt(c['answer']):<34} {c['question']}")

    warns = _validate(data)
    print("-" * 100)
    print(f"{len(data)} cases.", end=" ")
    if warns:
        print(f"{len(warns)} WARNING(S):")
        for w in warns:
            print("  ⚠ ", w)
        sys.exit(1)
    print("All gold plans produce sane answers. ✓")
