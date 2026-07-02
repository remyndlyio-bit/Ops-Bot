"""
KnowledgeBook — HARD held-out evaluation set (200 cases).

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
_Q2_2026 = {"start": "2026-04-01", "end": "2026-06-30"}
_Q3_2025 = {"start": "2025-07-01", "end": "2025-09-30"}
_Q4_2025 = {"start": "2025-10-01", "end": "2025-12-31"}
_H2_2025 = {"start": "2025-07-01", "end": "2025-12-31"}
_MAR_2026 = {"start": "2026-03-01", "end": "2026-03-31"}
_NEXT_YEAR = {"start": "2027-01-01", "end": "2027-12-31"}       # future -> empty


def _abs(rng: Dict) -> Dict:
    return {"type": "absolute", "value": rng}


def _plan(metric=None, column=None, filters=None, time_range=None,
          group_by=None, order=None, limit=None) -> Dict:
    return {"metric": metric, "column": column, "filters": filters or {},
            "time_range": time_range, "group_by": group_by,
            "order": order, "limit": limit}


# ── The hard cases: (id, question, gold_plan, tags, lang) — 200 total ─────────
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

    # ══════════════════════════════════════════════════════════════════════
    # EXTENSION — 150 more hard held-out cases (total 200). Same rules:
    # oracle-gradeable shapes only (count/sum/avg, group_by=client_name SUM with
    # limit 1 or no limit; NEVER per-group avg or limit>1). Phrasings kept
    # indirect / idiomatic so they are NOT template matches of the corpus.
    # ══════════════════════════════════════════════════════════════════════

    # ── A2. Indirect unpaid / "owe" verbs, more clients & phrasings (18) ─────
    ("owe-08", "What's the damage still pending from Content Lab?",
     _plan("sum", "fees", {"client_name": "Content Lab", "paid": "no"}),
     ["unpaid", "client", "sum", "idiom"], "en"),
    ("owe-09", "How much is Ogilvy yet to clear?",
     _plan("sum", "fees", {"client_name": "Ogilvy", "paid": "no"}),
     ["unpaid", "client", "sum", "indirect"], "en"),
    ("owe-10", "Money Lowe Lintas hasn't settled yet",
     _plan("sum", "fees", {"client_name": "Lowe Lintas", "paid": "no"}),
     ["unpaid", "client", "sum", "indirect"], "en"),
    ("owe-11", "How much am I yet to receive from Famous Innovations?",
     _plan("sum", "fees", {"client_name": "Famous Innovations", "paid": "no"}),
     ["unpaid", "client", "sum", "indirect"], "en"),
    ("owe-12", "What's Dentsu's unpaid balance with me?",
     _plan("sum", "fees", {"client_name": "Dentsu", "paid": "no"}),
     ["unpaid", "client", "sum", "indirect"], "en"),
    ("owe-13", "Total sitting unpaid across everyone",
     _plan("sum", "fees", {"paid": "no"}),
     ["unpaid", "sum", "bare"], "en"),
    ("owe-14", "How many of my invoices are still not cleared?",
     _plan("count", None, {"paid": "no"}),
     ["unpaid", "count", "bare"], "en"),
    ("owe-15", "Which Samsung jobs are still on credit?",
     _plan(None, None, {"client_name": "Samsung", "paid": "no"}),
     ["unpaid", "client", "list"], "en"),
    ("owe-16", "Show me everything Pedigree still owes on",
     _plan(None, None, {"client_name": "Pedigree", "paid": "no"}),
     ["unpaid", "client", "list", "brand-vs-client"], "en"),
    ("owe-17", "How much has actually landed from Garnier?",
     _plan("sum", "fees", {"client_name": "Garnier", "paid": "yes"}),
     ["paid", "client", "sum", "indirect"], "en"),
    ("owe-18", "What have I banked from Maruti so far?",
     _plan("sum", "fees", {"client_name": "Maruti", "paid": "yes"}),
     ["paid", "client", "sum", "idiom"], "en"),
    ("owe-19", "How many jobs has Star Studios actually paid up?",
     _plan("count", None, {"client_name": "Star Studios", "paid": "yes"}),
     ["paid", "client", "count"], "en"),
    ("owe-20", "Count everything that's been cleared",
     _plan("count", None, {"paid": "yes"}),
     ["paid", "count", "bare"], "en"),
    ("owe-21", "How much has come in overall?",
     _plan("sum", "fees", {"paid": "yes"}),
     ["paid", "sum", "bare"], "en"),
    ("owe-22", "Which invoices have cleared so far?",
     _plan(None, None, {"paid": "yes"}),
     ["paid", "list"], "en"),
    ("owe-23", "How much is Content Lab short on paying me?",
     _plan("sum", "fees", {"client_name": "Content Lab", "paid": "no"}),
     ["unpaid", "client", "sum", "idiom"], "en"),
    ("owe-24", "Anything Ogilvy still needs to pay?",
     _plan(None, None, {"client_name": "Ogilvy", "paid": "no"}),
     ["unpaid", "client", "list"], "en"),
    ("owe-25", "How many unpaid gigs does Lowe Lintas have?",
     _plan("count", None, {"client_name": "Lowe Lintas", "paid": "no"}),
     ["unpaid", "client", "count"], "en"),

    # ── B2. Brand term must widen to billing client, all brands (20) ─────────
    ("brand-07", "Total I've done on Adidas",
     _plan("sum", "fees", {"client_name": "Adidas"}),
     ["brand-vs-client", "sum"], "en"),
    ("brand-08", "How many Puma shoots have I had?",
     _plan("count", None, {"client_name": "Puma"}),
     ["brand-vs-client", "count"], "en"),
    ("brand-09", "Show me the Galaxy work",
     _plan(None, None, {"client_name": "Galaxy"}),
     ["brand-vs-client", "list"], "en"),
    ("brand-10", "What's outstanding on Swift?",
     _plan("sum", "fees", {"client_name": "Swift", "paid": "no"}),
     ["brand-vs-client", "unpaid", "sum"], "en"),
    ("brand-11", "Total billed to Tropicana",
     _plan("sum", "fees", {"client_name": "Tropicana"}),
     ["brand-vs-client", "sum"], "en"),
    ("brand-12", "How many Zomato jobs?",
     _plan("count", None, {"client_name": "Zomato"}),
     ["brand-vs-client", "count"], "en"),
    ("brand-13", "Whiskas ka total kitna",
     _plan("sum", "fees", {"client_name": "Whiskas"}),
     ["brand-vs-client", "sum"], "hi"),
    ("brand-14", "Average fee on Bournvita",
     _plan("avg", "fees", {"client_name": "Bournvita"}),
     ["brand-vs-client", "avg"], "en"),
    ("brand-15", "Everything for Surf Excel",
     _plan(None, None, {"client_name": "Surf Excel"}),
     ["brand-vs-client", "list"], "en"),
    ("brand-16", "How much has Lifebuoy paid me?",
     _plan("sum", "fees", {"client_name": "Lifebuoy", "paid": "yes"}),
     ["brand-vs-client", "paid", "sum"], "en"),
    ("brand-17", "Royal Enfield ka kitna kaam hua",
     _plan("count", None, {"client_name": "Royal Enfield"}),
     ["brand-vs-client", "count"], "hi"),
    ("brand-18", "Bajaj se kitna paisa aaya",
     _plan("sum", "fees", {"client_name": "Bajaj", "paid": "yes"}),
     ["brand-vs-client", "paid", "sum"], "hi"),
    ("brand-19", "Total on the L'Oreal projects",
     _plan("sum", "fees", {"client_name": "L'Oreal"}),
     ["brand-vs-client", "sum"], "en"),
    ("brand-20", "How many Garnier Men jobs are unpaid?",
     _plan("count", None, {"client_name": "Garnier Men", "paid": "no"}),
     ["brand-vs-client", "unpaid", "count"], "en"),
    ("brand-21", "Brezza work — show it all",
     _plan(None, None, {"client_name": "Brezza"}),
     ["brand-vs-client", "list"], "en"),
    ("brand-22", "What's the average on Lays?",
     _plan("avg", "fees", {"client_name": "Lays"}),
     ["brand-vs-client", "avg"], "en"),
    ("brand-23", "Nike se kitna baki hai",
     _plan("sum", "fees", {"client_name": "Nike", "paid": "no"}),
     ["brand-vs-client", "unpaid", "sum"], "hi"),
    ("brand-24", "Total unpaid on Pepsi",
     _plan("sum", "fees", {"client_name": "Pepsi", "paid": "no"}),
     ["brand-vs-client", "unpaid", "sum"], "en"),
    ("brand-25", "How many invoices went out to Cadbury?",
     _plan("count", None, {"client_name": "Cadbury", "bill_sent": "yes"}),
     ["brand-vs-client", "bill_sent", "count"], "en"),
    ("brand-26", "Swiggy — how many jobs total?",
     _plan("count", None, {"client_name": "Swiggy"}),
     ["brand-vs-client", "count"], "en"),

    # ── C2. Compound filters (client + status + date, etc.) (25) ─────────────
    ("comp-09", "Unpaid Samsung work from the first half of this year",
     _plan(None, None, {"client_name": "Samsung", "paid": "no"}, _abs(_FIRST_HALF)),
     ["compound", "client", "unpaid", "date"], "en"),
    ("comp-10", "How much does Maruti owe me this year?",
     _plan("sum", "fees", {"client_name": "Maruti", "paid": "no"}, _abs(_THIS_YEAR)),
     ["compound", "client", "unpaid", "date", "sum"], "en"),
    ("comp-11", "Paid Star Studios jobs in Q1",
     _plan(None, None, {"client_name": "Star Studios", "paid": "yes"}, _abs(_Q1_2026)),
     ["compound", "client", "paid", "date"], "en"),
    ("comp-12", "How much did I bill Content Lab last year?",
     _plan("sum", "fees", {"client_name": "Content Lab"}, _abs(_LAST_YEAR)),
     ["compound", "client", "date", "sum"], "en"),
    ("comp-13", "Count of Garnier jobs in the first half of this year",
     _plan("count", None, {"client_name": "Garnier"}, _abs(_FIRST_HALF)),
     ["compound", "client", "date", "count"], "en"),
    ("comp-14", "How many unpaid jobs in Q1 this year?",
     _plan("count", None, {"paid": "no"}, _abs(_Q1_2026)),
     ["compound", "unpaid", "date", "count"], "en"),
    ("comp-15", "Total paid last quarter",
     _plan("sum", "fees", {"paid": "yes"}, _abs(_LAST_QUARTER)),
     ["compound", "paid", "date", "sum"], "en"),
    ("comp-16", "Invoices I've sent but not been paid for, this year",
     _plan(None, None, {"bill_sent": "yes", "paid": "no"}, _abs(_THIS_YEAR)),
     ["compound", "bill_sent", "unpaid", "date"], "en"),
    ("comp-17", "How many invoices are sent and still unpaid?",
     _plan("count", None, {"bill_sent": "yes", "paid": "no"}),
     ["compound", "bill_sent", "unpaid", "count"], "en"),
    ("comp-18", "Total on Samsung jobs that are paid",
     _plan("sum", "fees", {"client_name": "Samsung", "paid": "yes"}),
     ["compound", "client", "paid", "sum"], "en"),
    ("comp-19", "Unpaid Maruti count this year",
     _plan("count", None, {"client_name": "Maruti", "paid": "no"}, _abs(_THIS_YEAR)),
     ["compound", "client", "unpaid", "date", "count"], "en"),
    ("comp-20", "How much have I invoiced Garnier that's still due?",
     _plan("sum", "fees", {"client_name": "Garnier", "bill_sent": "yes", "paid": "no"}),
     ["compound", "client", "bill_sent", "unpaid", "sum"], "en"),
    ("comp-21", "Paid jobs for Content Lab in the first half",
     _plan("count", None, {"client_name": "Content Lab", "paid": "yes"}, _abs(_FIRST_HALF)),
     ["compound", "client", "paid", "date", "count"], "en"),
    ("comp-22", "Total billed to Star Studios last year",
     _plan("sum", "fees", {"client_name": "Star Studios"}, _abs(_LAST_YEAR)),
     ["compound", "client", "date", "sum"], "en"),
    ("comp-23", "How many jobs did Ogilvy pay in Q4 2025?",
     _plan("count", None, {"client_name": "Ogilvy", "paid": "yes"}, _abs(_Q4_2025)),
     ["compound", "client", "paid", "date", "count"], "en"),
    ("comp-24", "Unpaid total for Lowe Lintas this year",
     _plan("sum", "fees", {"client_name": "Lowe Lintas", "paid": "no"}, _abs(_THIS_YEAR)),
     ["compound", "client", "unpaid", "date", "sum"], "en"),
    ("comp-25", "Show paid Nike jobs from this year",
     _plan(None, None, {"client_name": "Nike", "paid": "yes"}, _abs(_THIS_YEAR)),
     ["compound", "brand-vs-client", "paid", "date"], "en"),
    ("comp-26", "How much has Samsung cleared in the last three months?",
     _plan("sum", "fees", {"client_name": "Samsung", "paid": "yes"}, _abs(_LAST_QUARTER)),
     ["compound", "client", "paid", "date", "sum"], "en"),
    ("comp-27", "Count unpaid Pepsi jobs",
     _plan("count", None, {"client_name": "Pepsi", "paid": "no"}),
     ["compound", "brand-vs-client", "unpaid", "count"], "en"),
    ("comp-28", "Total value of Maruti invoices already sent out",
     _plan("sum", "fees", {"client_name": "Maruti", "bill_sent": "yes"}),
     ["compound", "client", "bill_sent", "sum"], "en"),
    ("comp-29", "How many Garnier invoices haven't gone out yet?",
     _plan("count", None, {"client_name": "Garnier", "bill_sent": "no"}),
     ["compound", "client", "bill_sent", "count"], "en"),
    ("comp-30", "Paid amount from Star Studios in the second half of 2025",
     _plan("sum", "fees", {"client_name": "Star Studios", "paid": "yes"}, _abs(_H2_2025)),
     ["compound", "client", "paid", "date", "sum"], "en"),
    ("comp-31", "How many paid jobs overall this year?",
     _plan("count", None, {"paid": "yes"}, _abs(_THIS_YEAR)),
     ["compound", "paid", "date", "count"], "en"),
    ("comp-32", "Total unpaid in the first half of the year",
     _plan("sum", "fees", {"paid": "no"}, _abs(_FIRST_HALF)),
     ["compound", "unpaid", "date", "sum"], "en"),
    ("comp-33", "Unpaid Content Lab jobs, how many?",
     _plan("count", None, {"client_name": "Content Lab", "paid": "no"}),
     ["compound", "client", "unpaid", "count"], "en"),

    # ── D2. bill_sent vs paid + more over-reasoning traps (18) ───────────────
    ("sent-07", "How many bills went out the door?",
     _plan("count", None, {"bill_sent": "yes"}),
     ["bill_sent", "count", "no-poc_email-trap"], "en"),
    ("sent-08", "How many invoices are still sitting unsent?",
     _plan("count", None, {"bill_sent": "no"}),
     ["bill_sent", "count"], "en"),
    ("sent-09", "Total worth of everything I've invoiced",
     _plan("sum", "fees", {"bill_sent": "yes"}),
     ["bill_sent", "sum", "no-poc_email-trap"], "en"),
    ("sent-10", "Which jobs are still waiting to be billed?",
     _plan(None, None, {"bill_sent": "no"}),
     ["bill_sent", "list"], "en"),
    ("sent-11", "Star Studios — how many invoices have I fired off?",
     _plan("count", None, {"client_name": "Star Studios", "bill_sent": "yes"}),
     ["bill_sent", "client", "count", "no-poc_email-trap"], "en"),
    ("sent-12", "How many Garnier invoices are pending to send?",
     _plan("count", None, {"client_name": "Garnier", "bill_sent": "no"}),
     ["bill_sent", "client", "count"], "en"),
    ("sent-13", "Value of unsent invoices",
     _plan("sum", "fees", {"bill_sent": "no"}),
     ["bill_sent", "sum"], "en"),
    ("sent-14", "Show me the jobs where the bill already went out",
     _plan(None, None, {"bill_sent": "yes"}),
     ["bill_sent", "list", "no-poc_email-trap"], "en"),
    ("sent-15", "How many invoices have I raised for Samsung?",
     _plan("count", None, {"client_name": "Samsung", "bill_sent": "yes"}),
     ["bill_sent", "client", "count", "no-poc_email-trap"], "en"),
    ("sent-16", "Count of invoices dispatched to Maruti",
     _plan("count", None, {"client_name": "Maruti", "bill_sent": "yes"}),
     ["bill_sent", "client", "count", "no-poc_email-trap"], "en"),
    ("sent-17", "Invoices sent out but money not in yet",
     _plan(None, None, {"bill_sent": "yes", "paid": "no"}),
     ["bill_sent", "unpaid", "list"], "en"),
    ("sent-18", "How many jobs are billed but unpaid?",
     _plan("count", None, {"bill_sent": "yes", "paid": "no"}),
     ["bill_sent", "unpaid", "count"], "en"),
    ("sent-19", "Total of invoices sent that are still unpaid",
     _plan("sum", "fees", {"bill_sent": "yes", "paid": "no"}),
     ["bill_sent", "unpaid", "sum"], "en"),
    ("sent-20", "How many invoices have I emailed out in total?",
     _plan("count", None, {"bill_sent": "yes"}),
     ["bill_sent", "count", "no-poc_email-trap"], "en"),
    ("sent-21", "Content Lab — invoices sent count",
     _plan("count", None, {"client_name": "Content Lab", "bill_sent": "yes"}),
     ["bill_sent", "client", "count", "no-poc_email-trap"], "en"),
    ("sent-22", "How many of my jobs still need an invoice raised?",
     _plan("count", None, {"bill_sent": "no"}),
     ["bill_sent", "count"], "en"),
    ("sent-23", "Total already invoiced to Star Studios",
     _plan("sum", "fees", {"client_name": "Star Studios", "bill_sent": "yes"}),
     ["bill_sent", "client", "sum", "no-poc_email-trap"], "en"),
    ("sent-24", "Which unsent invoices belong to Garnier?",
     _plan(None, None, {"client_name": "Garnier", "bill_sent": "no"}),
     ["bill_sent", "client", "list"], "en"),

    # ── E2. Hinglish idioms, expanded (25) ───────────────────────────────────
    ("hi-10", "Content Lab ka kitna paisa atka hua hai",
     _plan("sum", "fees", {"client_name": "Content Lab", "paid": "no"}),
     ["hinglish", "unpaid", "client", "sum", "idiom"], "hi"),
    ("hi-11", "Maruti se kitna vasool karna baki hai",
     _plan("sum", "fees", {"client_name": "Maruti", "paid": "no"}),
     ["hinglish", "unpaid", "client", "sum", "idiom"], "hi"),
    ("hi-12", "Ogilvy ne kitna pay kiya",
     _plan("sum", "fees", {"client_name": "Ogilvy", "paid": "yes"}),
     ["hinglish", "paid", "client", "sum"], "hi"),
    ("hi-13", "Samsung ke kitne kaam ho chuke",
     _plan("count", None, {"client_name": "Samsung"}),
     ["hinglish", "client", "count"], "hi"),
    ("hi-14", "Sabse kam paisa kaun sa client deta hai",
     _plan("sum", "fees", {}, None, "client_name", "asc", 1),
     ["hinglish", "group_by", "bottom-client"], "hi"),
    ("hi-15", "Total kitna outstanding hai sabka",
     _plan("sum", "fees", {"paid": "no"}),
     ["hinglish", "unpaid", "sum", "bare"], "hi"),
    ("hi-16", "Is saal kitne kaam hue",
     _plan("count", None, {}, _abs(_THIS_YEAR)),
     ["hinglish", "date", "count"], "hi"),
    ("hi-17", "Pichle saal ki total kamai",
     _plan("sum", "fees", {}, _abs(_LAST_YEAR)),
     ["hinglish", "date", "sum"], "hi"),
    ("hi-18", "Garnier ke kitne invoice bhejne baki hain",
     _plan("count", None, {"client_name": "Garnier", "bill_sent": "no"}),
     ["hinglish", "bill_sent", "client", "count"], "hi"),
    ("hi-19", "Kitne jobs ka paisa aa gaya",
     _plan("count", None, {"paid": "yes"}),
     ["hinglish", "paid", "count"], "hi"),
    ("hi-20", "Nike ka average fee kitna hai",
     _plan("avg", "fees", {"client_name": "Nike"}),
     ["hinglish", "brand-vs-client", "avg"], "hi"),
    ("hi-21", "Star Studios ne ab tak kitna diya",
     _plan("sum", "fees", {"client_name": "Star Studios", "paid": "yes"}),
     ["hinglish", "paid", "client", "sum"], "hi"),
    ("hi-22", "Kaunse client ka sabse zyada paisa baki hai",
     _plan("sum", "fees", {"paid": "no"}, None, "client_name", "desc", 1),
     ["hinglish", "group_by", "top-client", "unpaid"], "hi"),
    ("hi-23", "Pepsi ka total billing dikha",
     _plan("sum", "fees", {"client_name": "Pepsi"}),
     ["hinglish", "brand-vs-client", "sum"], "hi"),
    ("hi-24", "Kitne invoice abhi tak nahi bheje",
     _plan("count", None, {"bill_sent": "no"}),
     ["hinglish", "bill_sent", "count"], "hi"),
    ("hi-25", "Maruti ke kitne unpaid pade hain",
     _plan("count", None, {"client_name": "Maruti", "paid": "no"}),
     ["hinglish", "unpaid", "client", "count"], "hi"),
    ("hi-26", "Total kitna invoice bhej chuka hoon",
     _plan("count", None, {"bill_sent": "yes"}),
     ["hinglish", "bill_sent", "count", "no-poc_email-trap"], "hi"),
    ("hi-27", "Samsung se pichle teen mahine mein kitna aaya",
     _plan("sum", "fees", {"client_name": "Samsung", "paid": "yes"}, _abs(_LAST_QUARTER)),
     ["hinglish", "compound", "client", "paid", "date"], "hi"),
    ("hi-28", "Cadbury ke saare kaam dikhao",
     _plan(None, None, {"client_name": "Cadbury"}),
     ["hinglish", "brand-vs-client", "list"], "hi"),
    ("hi-29", "Ek job pe average kitna banta hai",
     _plan("avg", "fees", {}),
     ["hinglish", "avg", "bare"], "hi"),
    ("hi-30", "Kis client ka kaam sabse zyada kiya paise ke hisaab se",
     _plan("sum", "fees", {}, None, "client_name", "desc", 1),
     ["hinglish", "group_by", "top-client"], "hi"),
    ("hi-31", "Garnier se kitna paisa clear hua",
     _plan("sum", "fees", {"client_name": "Garnier", "paid": "yes"}),
     ["hinglish", "paid", "client", "sum"], "hi"),
    ("hi-32", "Is saal kitni kamai hui total",
     _plan("sum", "fees", {}, _abs(_THIS_YEAR)),
     ["hinglish", "date", "sum"], "hi"),
    ("hi-33", "Kitne kaam abhi unpaid hain",
     _plan("count", None, {"paid": "no"}),
     ["hinglish", "unpaid", "count"], "hi"),
    ("hi-34", "Lowe Lintas ka kitna baki hai abhi tak",
     _plan("sum", "fees", {"client_name": "Lowe Lintas", "paid": "no"}),
     ["hinglish", "unpaid", "client", "sum"], "hi"),

    # ── F2. Aggregates: top/bottom/ranking, paid-most (15) ───────────────────
    ("agg-07", "Who owes me the most money?",
     _plan("sum", "fees", {"paid": "no"}, None, "client_name", "desc", 1),
     ["group_by", "top-client", "unpaid"], "en"),
    ("agg-08", "Which client has sent me the least business?",
     _plan("sum", "fees", {}, None, "client_name", "asc", 1),
     ["group_by", "bottom-client"], "en"),
    ("agg-09", "Rank everyone by what they still owe",
     _plan("sum", "fees", {"paid": "no"}, None, "client_name", "desc"),
     ["group_by", "ranking", "unpaid"], "en"),
    ("agg-10", "Top client by money actually received",
     _plan("sum", "fees", {"paid": "yes"}, None, "client_name", "desc", 1),
     ["group_by", "top-client", "paid"], "en"),
    ("agg-11", "List my clients from biggest to smallest earner",
     _plan("sum", "fees", {}, None, "client_name", "desc"),
     ["group_by", "ranking"], "en"),
    ("agg-12", "What's the average fee across everything I've been paid for?",
     _plan("avg", "fees", {"paid": "yes"}),
     ["avg", "paid"], "en"),
    ("agg-13", "Average value of an unpaid job",
     _plan("avg", "fees", {"paid": "no"}),
     ["avg", "unpaid"], "en"),
    ("agg-14", "My single biggest client overall",
     _plan("sum", "fees", {}, None, "client_name", "desc", 1),
     ["group_by", "top-client"], "en"),
    ("agg-15", "Which client brings the least unpaid balance?",
     _plan("sum", "fees", {"paid": "no"}, None, "client_name", "asc", 1),
     ["group_by", "bottom-client", "unpaid"], "en"),
    ("agg-16", "Average fee on invoices I've actually sent",
     _plan("avg", "fees", {"bill_sent": "yes"}),
     ["avg", "bill_sent"], "en"),
    ("agg-17", "Top earner among the paid jobs",
     _plan("sum", "fees", {"paid": "yes"}, None, "client_name", "desc", 1),
     ["group_by", "top-client", "paid"], "en"),
    ("agg-18", "Rank clients by total revenue this year",
     _plan("sum", "fees", {}, _abs(_THIS_YEAR), "client_name", "desc"),
     ["group_by", "ranking", "date"], "en"),
    ("agg-19", "Who is my biggest client for the first half of the year?",
     _plan("sum", "fees", {}, _abs(_FIRST_HALF), "client_name", "desc", 1),
     ["group_by", "top-client", "date"], "en"),
    ("agg-20", "Average job size in Q1",
     _plan("avg", "fees", {}, _abs(_Q1_2026)),
     ["avg", "date"], "en"),
    ("agg-21", "Smallest client by paid revenue",
     _plan("sum", "fees", {"paid": "yes"}, None, "client_name", "asc", 1),
     ["group_by", "bottom-client", "paid"], "en"),

    # ── G2. Date ranges incl. zero-result (17) ───────────────────────────────
    ("date-06", "Total billing in Q1 this year",
     _plan("sum", "fees", {}, _abs(_Q1_2026)),
     ["date", "sum"], "en"),
    ("date-07", "How many jobs in the second half of 2025?",
     _plan("count", None, {}, _abs(_H2_2025)),
     ["date", "count"], "en"),
    ("date-08", "Earnings in Q3 2025",
     _plan("sum", "fees", {}, _abs(_Q3_2025)),
     ["date", "sum", "value-word"], "en"),
    ("date-09", "How much did I make in March 2026?",
     _plan("sum", "fees", {}, _abs(_MAR_2026)),
     ["date", "sum"], "en"),
    ("date-10", "Jobs in the last three months",
     _plan(None, None, {}, _abs(_LAST_QUARTER)),
     ["date", "list"], "en"),
    ("date-11", "Total revenue for last quarter",
     _plan("sum", "fees", {}, _abs(_LAST_QUARTER)),
     ["date", "sum"], "en"),
    ("date-12", "How many jobs did I do in Q4 2025?",
     _plan("count", None, {}, _abs(_Q4_2025)),
     ["date", "count"], "en"),
    ("date-13", "What did I earn in the first half of the year?",
     _plan("sum", "fees", {}, _abs(_FIRST_HALF)),
     ["date", "sum", "value-word"], "en"),
    ("date-14", "How many jobs so far this year?",
     _plan("count", None, {}, _abs(_THIS_YEAR)),
     ["date", "count"], "en"),
    ("date-15", "Average fee in 2025",
     _plan("avg", "fees", {}, _abs(_LAST_YEAR)),
     ["date", "avg"], "en"),
    ("date-16", "How much did I earn next year?",
     _plan("sum", "fees", {}, _abs(_NEXT_YEAR)),
     ["date", "sum", "zero-result"], "en"),
    ("date-17", "How many jobs are booked for 2027?",
     _plan("count", None, {}, _abs(_NEXT_YEAR)),
     ["date", "count", "zero-result"], "en"),
    ("date-18", "Total earned this month",
     _plan("sum", "fees", {}, _abs(_THIS_MONTH)),
     ["date", "sum", "zero-result"], "en"),
    ("date-19", "Revenue in Q2 2026",
     _plan("sum", "fees", {}, _abs(_Q2_2026)),
     ["date", "sum"], "en"),
    ("date-20", "How many jobs last month?",
     _plan("count", None, {}, _abs(_LAST_MONTH)),
     ["date", "count"], "en"),
    ("date-21", "Total billing for 2025",
     _plan("sum", "fees", {}, _abs(_LAST_YEAR)),
     ["date", "sum"], "en"),
    ("date-22", "How many jobs in the second half of last year?",
     _plan("count", None, {}, _abs(_H2_2025)),
     ["date", "count"], "en"),

    # ── H2. poc_email edges + multi-filter (10) ──────────────────────────────
    ("poc-04", "Which of my clients are missing an email?",
     _plan(None, None, {"poc_email": "null"}),
     ["poc_email", "null", "list"], "en"),
    ("poc-05", "How many jobs have a contact email on file?",
     _plan("count", None, {"poc_email": "not_null"}),
     ["poc_email", "not_null", "count"], "en"),
    ("poc-06", "Total fees on jobs that have no email",
     _plan("sum", "fees", {"poc_email": "null"}),
     ["poc_email", "null", "sum"], "en"),
    ("poc-07", "How many invoices went to clients that DO have an email?",
     _plan("count", None, {"bill_sent": "yes", "poc_email": "not_null"}),
     ["poc_email", "not_null", "bill_sent", "count", "multi-filter"], "en"),
    ("poc-08", "Show the jobs with an email on record",
     _plan(None, None, {"poc_email": "not_null"}),
     ["poc_email", "not_null", "list"], "en"),
    ("poc-09", "How many unpaid jobs are for clients with no email?",
     _plan("count", None, {"paid": "no", "poc_email": "null"}),
     ["poc_email", "null", "unpaid", "count", "multi-filter"], "en"),
    ("poc-10", "Count of contactable clients I've invoiced",
     _plan("count", None, {"bill_sent": "yes", "poc_email": "not_null"}),
     ["poc_email", "not_null", "bill_sent", "count", "multi-filter"], "en"),
    ("poc-11", "How much is unpaid from clients I can't email?",
     _plan("sum", "fees", {"paid": "no", "poc_email": "null"}),
     ["poc_email", "null", "unpaid", "sum", "multi-filter"], "en"),
    ("poc-12", "Which jobs have neither been sent nor have an email?",
     _plan(None, None, {"bill_sent": "no", "poc_email": "null"}),
     ["poc_email", "null", "bill_sent", "list", "multi-filter"], "en"),
    ("poc-13", "How many jobs are missing contact details?",
     _plan("count", None, {"poc_email": "null"}),
     ["poc_email", "null", "count"], "en"),

    # ── I2. A couple more compound / Hinglish to round out to 200 (2) ────────
    ("owe-26", "How much has Famous Innovations cleared so far?",
     _plan("sum", "fees", {"client_name": "Famous Innovations", "paid": "yes"}),
     ["paid", "client", "sum", "indirect"], "en"),
    ("hi-35", "Dentsu ka kitna paisa aana baki hai",
     _plan("sum", "fees", {"client_name": "Dentsu", "paid": "no"}),
     ["hinglish", "unpaid", "client", "sum", "idiom"], "hi"),
]


def cases(seed: int = 42, n: int = 120) -> List[Dict[str, Any]]:
    """Return the 200 hard cases with oracle-computed gold answers.

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
