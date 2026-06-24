"""
Golden corpus generator.

Expands templates × entities × paraphrases into {question, plan} pairs, then the
oracle fills the correct `answer` from the seeded dataset. Run it to (re)build
golden/corpus.jsonl — deterministic, so the corpus is reproducible.

    python -m golden.generate            # writes golden/corpus.jsonl

Scaling 100 → 10k = add templates/paraphrases/entities here (and grow the
dataset). Answers never need hand-labelling: the oracle computes them.
"""
import os
import json
import calendar

from golden.dataset import build_dataset, entities
from golden.oracle import compute_answer

CORPUS_PATH = os.path.join(os.path.dirname(__file__), "corpus.jsonl")


def _month_range(month_name: str, year: int):
    mnum = list(calendar.month_name).index(month_name)
    last = calendar.monthrange(year, mnum)[1]
    return {"type": "absolute", "value": {"start": f"{year}-{mnum:02d}-01",
                                          "end": f"{year}-{mnum:02d}-{last:02d}"}}


# Slot pools (kept small per category so the 100-seed stays balanced).
_CLIENTS = entities()["clients"][:5]            # Star Studios, Garnier India, ...
_BRANDS = ["Pepsi", "Nike", "Samsung", "Swiggy"]  # teach brand-vs-client matching
_MONTHS = [("March", 2026), ("January", 2026), ("October", 2025), ("June", 2025)]


def _plan(metric=None, column=None, filters=None, time_range=None,
          group_by=None, order=None, limit=None):
    return {"metric": metric, "column": column, "filters": filters or {},
            "time_range": time_range, "group_by": group_by, "order": order, "limit": limit}


def _entries():
    """Yield (category, question, plan, tags, lang) tuples."""
    # 1–3 — bare aggregates
    for q, lang in [("how many jobs do I have?", "en"), ("total number of jobs", "en"),
                    ("kitne jobs hue", "hi")]:
        yield "count_all", q, _plan(metric="count"), ["count", "bare"], lang
    for q, lang in [("what's my total billing?", "en"), ("overall revenue so far", "en"),
                    ("total kitna kamaya", "hi")]:
        yield "total_all", q, _plan(metric="sum", column="fees"), ["sum", "bare"], lang
    for q in ["average fee per job", "what's my average billing", "avg job value"]:
        yield "avg_all", q, _plan(metric="avg", column="fees"), ["avg", "bare"], "en"

    # 4–6 — client-scoped (clients + brands, formal/casual/Hinglish)
    for c in _CLIENTS + _BRANDS:
        f = {"client_name": c}
        yield "count_client", f"how many jobs for {c}?", _plan(metric="count", filters=f), ["count", "client"], "en"
        yield "total_client", f"total fees for {c}", _plan(metric="sum", column="fees", filters=f), ["sum", "client"], "en"
        yield "list_client", f"show me {c} jobs", _plan(filters=f), ["list", "client"], "en"
    for c in _CLIENTS[:3]:
        yield "total_client", f"{c} se total kitna aaya", _plan(metric="sum", column="fees", filters={"client_name": c}), ["sum", "client"], "hi"

    # 7–8 — payment status
    for status, words in [("yes", "paid"), ("no", "unpaid")]:
        yield f"count_{words}", f"how many {words} jobs do I have", _plan(metric="count", filters={"paid": status}), ["count", "status"], "en"
        yield f"total_{words}", f"how much is {words} in total", _plan(metric="sum", column="fees", filters={"paid": status}), ["sum", "status"], "en"
    yield "count_unpaid", "kitne unpaid hain", _plan(metric="count", filters={"paid": "no"}), ["count", "status"], "hi"
    yield "total_unpaid", "total outstanding amount", _plan(metric="sum", column="fees", filters={"paid": "no"}), ["sum", "status"], "en"

    # 9 — biggest client (grouped)
    for q in ["who is my biggest client?", "top client by revenue", "sabse bada client kaun hai"]:
        yield "biggest_client", q, _plan(metric="sum", column="fees", group_by="client_name", order="desc", limit=1), ["group", "client"], ("hi" if "kaun" in q else "en")

    # 10–11 — date ranges (month)
    for mname, yr in _MONTHS:
        tr = _month_range(mname, yr)
        yield "count_month", f"how many jobs in {mname} {yr}", _plan(metric="count", time_range=tr), ["count", "date"], "en"
        yield "total_month", f"total billing in {mname} {yr}", _plan(metric="sum", column="fees", time_range=tr), ["sum", "date"], "en"

    # 12 — list unpaid
    for q, lang in [("list my unpaid invoices", "en"), ("show unpaid jobs", "en"), ("kaunse unpaid hain", "hi")]:
        yield "list_unpaid", q, _plan(filters={"paid": "no"}), ["list", "status"], lang

    # client + status (owes)
    for c in _CLIENTS[:4]:
        yield "client_owes", f"how much does {c} owe me", _plan(metric="sum", column="fees", filters={"client_name": c, "paid": "no"}), ["sum", "client", "status"], "en"


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
