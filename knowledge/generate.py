"""
KnowledgeBook — examples generator.

Expands templates × entities × paraphrases into {question, plan} pairs, then the
oracle fills the correct `answer` from the seeded dataset. Run it to (re)build
knowledge/examples.jsonl — deterministic, so the examples are reproducible.

    python -m knowledge.generate         # writes knowledge/examples.jsonl

Scaling 100 → 10k = add templates/paraphrases/entities here (and grow the
dataset). Answers never need hand-labelling: the oracle computes them.
"""
import os
import json
import calendar

from knowledge.dataset import build_dataset, entities
from knowledge.oracle import compute_answer

CORPUS_PATH = os.path.join(os.path.dirname(__file__), "examples.jsonl")


def _month_range(month_name: str, year: int):
    mnum = list(calendar.month_name).index(month_name)
    last = calendar.monthrange(year, mnum)[1]
    return {"type": "absolute", "value": {"start": f"{year}-{mnum:02d}-01",
                                          "end": f"{year}-{mnum:02d}-{last:02d}"}}


# Slot pools.
_CLIENTS = entities()["clients"][:6]            # billing clients
_BRANDS = ["Pepsi", "Nike", "Samsung", "Swiggy", "Cadbury", "Maruti"]  # brand-vs-client
_ENTS = _CLIENTS + _BRANDS                      # everything we slot into client cats
_MONTHS = [("March", 2026), ("January", 2026), ("February", 2026), ("June", 2025)]

# Paraphrase templates per intent: (template, lang). Scaling 100 -> 10k mostly
# means growing these lists (and the entity pools) — answers stay free (oracle).
_T_COUNT_CLIENT = [("how many jobs for {c}?", "en"), ("how many {c} jobs", "en"),
                   ("number of {c} projects", "en"), ("{c} ke kitne jobs", "hi")]
_T_TOTAL_CLIENT = [("total fees for {c}", "en"), ("how much have I billed {c}", "en"),
                   ("{c} ka total billing", "hi"), ("{c} se total kitna aaya", "hi")]
_T_LIST_CLIENT = [("show me {c} jobs", "en"), ("list all {c} work", "en"),
                  ("{c} ka kaam dikhao", "hi")]
_T_OWES = [("how much does {c} owe me", "en"), ("total unpaid for {c}", "en"),
           ("{c} ka kitna baki hai", "hi")]
_T_UNPAID_COUNT_CLIENT = [("how many unpaid jobs for {c}", "en"), ("{c} ke kitne unpaid hain", "hi")]
_T_PAID_COUNT_CLIENT = [("how many paid jobs for {c}", "en")]
_T_SENT_CLIENT = [("how many invoices sent to {c}", "en")]


def _plan(metric=None, column=None, filters=None, time_range=None,
          group_by=None, order=None, limit=None):
    return {"metric": metric, "column": column, "filters": filters or {},
            "time_range": time_range, "group_by": group_by, "order": order, "limit": limit}


def _entries():
    """Yield (category, question, plan, tags, lang) tuples."""
    P = _plan

    # ── bare aggregates ──────────────────────────────────────────────────
    for q, l in [("how many jobs do I have?", "en"), ("total number of jobs", "en"),
                 ("how many projects in total", "en"), ("count my jobs", "en"),
                 ("kitne jobs hue", "hi")]:
        yield "count_all", q, P(metric="count"), ["count", "bare"], l
    for q, l in [("what's my total billing?", "en"), ("overall revenue so far", "en"),
                 ("what have I billed in total", "en"), ("sum of all my fees", "en"),
                 ("total kitna kamaya", "hi")]:
        yield "total_all", q, P(metric="sum", column="fees"), ["sum", "bare"], l
    for q in ["average fee per job", "what's my average billing", "avg job value",
              "mean fee per project"]:
        yield "avg_all", q, P(metric="avg", column="fees"), ["avg", "bare"], "en"

    # ── client-scoped (clients + brands) × paraphrases ───────────────────
    for c in _ENTS:
        f = {"client_name": c}
        for t, l in _T_COUNT_CLIENT:
            yield "count_client", t.format(c=c), P(metric="count", filters=f), ["count", "client"], l
        for t, l in _T_TOTAL_CLIENT:
            yield "total_client", t.format(c=c), P(metric="sum", column="fees", filters=f), ["sum", "client"], l
        for t, l in _T_LIST_CLIENT:
            yield "list_client", t.format(c=c), P(filters=f), ["list", "client"], l

    # ── payment status (bare) ────────────────────────────────────────────
    for status, w in [("yes", "paid"), ("no", "unpaid")]:
        for q, l in [(f"how many {w} jobs do I have", "en"), (f"kitne {w} hain", "hi")]:
            yield f"count_{w}", q, P(metric="count", filters={"paid": status}), ["count", "status"], l
        for q, l in [(f"how much is {w} in total", "en"), (f"total {w} amount", "en")]:
            yield f"total_{w}", q, P(metric="sum", column="fees", filters={"paid": status}), ["sum", "status"], l
    for q, l in [("list my unpaid invoices", "en"), ("show unpaid jobs", "en"), ("kaunse unpaid hain", "hi")]:
        yield "list_unpaid", q, P(filters={"paid": "no"}), ["list", "status"], l

    # ── invoices sent (bill_sent) ────────────────────────────────────────
    for q, l in [("how many invoices have I sent", "en"), ("how many bills have I sent", "en"),
                 ("kitne invoice bheje", "hi")]:
        yield "count_sent", q, P(metric="count", filters={"bill_sent": "yes"}), ["count", "bill_sent"], l

    # ── client + status / bill_sent (multi-filter) ───────────────────────
    for c in _ENTS:
        for t, l in _T_OWES:
            yield "client_owes", t.format(c=c), P(metric="sum", column="fees", filters={"client_name": c, "paid": "no"}), ["sum", "client", "status"], l
        for t, l in _T_UNPAID_COUNT_CLIENT:
            yield "count_client_unpaid", t.format(c=c), P(metric="count", filters={"client_name": c, "paid": "no"}), ["count", "client", "status"], l
        for t, l in _T_PAID_COUNT_CLIENT:
            yield "count_client_paid", t.format(c=c), P(metric="count", filters={"client_name": c, "paid": "yes"}), ["count", "client", "status"], l
        for t, l in _T_SENT_CLIENT:
            yield "count_client_sent", t.format(c=c), P(metric="count", filters={"client_name": c, "bill_sent": "yes"}), ["count", "client", "bill_sent"], l

    # ── Hinglish idioms, client-scoped (the phrasings real users send) ───
    for c in _CLIENTS[:4] + _BRANDS[:3]:
        f = {"client_name": c}
        yield "h_client_sum", f"{c} se kitna kamaya", P(metric="sum", column="fees", filters=f), ["sum", "client"], "hi"
        yield "h_client_unpaid", f"{c} ka paisa baki hai", P(metric="sum", column="fees", filters={"client_name": c, "paid": "no"}), ["sum", "client", "status"], "hi"
        yield "h_client_aana_baki", f"{c} se kitna aana baki hai", P(metric="sum", column="fees", filters={"client_name": c, "paid": "no"}), ["sum", "client", "status"], "hi"
        yield "h_client_sent", f"{c} ko kitne invoice bheje", P(metric="count", filters={"client_name": c, "bill_sent": "yes"}), ["count", "client", "bill_sent"], "hi"
        yield "h_client_count", f"{c} ka kitna kaam hua", P(metric="count", filters=f), ["count", "client"], "hi"

    # ── biggest client (grouped) ─────────────────────────────────────────
    for q, l in [("who is my biggest client?", "en"), ("top client by revenue", "en"),
                 ("sabse bada client kaun hai", "hi")]:
        yield "biggest_client", q, P(metric="sum", column="fees", group_by="client_name", order="desc", limit=1), ["group", "client"], l

    # ── date ranges (month) ──────────────────────────────────────────────
    for mname, yr in _MONTHS:
        tr = _month_range(mname, yr)
        yield "count_month", f"how many jobs in {mname} {yr}", P(metric="count", time_range=tr), ["count", "date"], "en"
        yield "total_month", f"total billing in {mname} {yr}", P(metric="sum", column="fees", time_range=tr), ["sum", "date"], "en"


def build_corpus(seed: int = 42):
    rows = build_dataset(seed=seed)
    corpus = []
    seen = set()
    for i, (cat, q, plan, tags, lang) in enumerate(_entries(), 1):
        if q.lower() in seen:
            continue
        seen.add(q.lower())
        corpus.append({
            "id": f"{cat}-{i:04d}",
            "question": q,
            "plan": plan,
            "answer": compute_answer(plan, rows),
            "tags": tags,
            "source": "synthetic",
            "lang": lang,
        })
    return corpus


def main():
    corpus = build_corpus()
    with open(CORPUS_PATH, "w") as f:
        for e in corpus:
            f.write(json.dumps(e, ensure_ascii=False) + "\n")
    print(f"wrote {len(corpus)} golden entries to {CORPUS_PATH}")


if __name__ == "__main__":
    main()
