"""
Golden-dataset answer-correctness net (CI, no LLM).

Runs real user phrasings through the deterministic router against a seeded dataset
with known answers. Two contracts:

  • MUST_ANSWER  — the router handles it AND returns the correct answer.
  • MUST_DEFER   — the router ABSTAINS (returns None) so the LLM planner can build
                   the filtered query. These are the qualified shapes the router
                   used to answer WRONG ("how many unpaid", "show me Garnier jobs").

This is the suite that catches the whole "confidently wrong" class — a route that
silently drops a qualifier flips a MUST_DEFER case from None to a wrong answer.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from services.query_router import route_common_query
from tests.golden_dataset import GoldenDB

DB = GoldenDB()
UID = "golden_user"


def _answer(q):
    r = route_common_query(q, UID)
    if r is None:
        return None, None
    return r, DB.execute_sql(r.sql)["rows"]


# (query, predicate over the executed rows)
MUST_ANSWER = [
    ("How many jobs do I have?", lambda rs: rs[0]["result"] == 8),
    ("What's my total billing?", lambda rs: rs[0]["result"] == 1175000),
    ("What's my average fee per job?", lambda rs: rs[0]["result"] == 146875),
    ("Who is my biggest client?", lambda rs: rs[0]["client_name"] == "Star Studios" and rs[0]["result"] == 350000),
    ("Show me total earnings per client", lambda rs: len(rs) == 5 and rs[0]["client_name"] == "Star Studios" and rs[0]["result"] == 350000),
    ("How much does Star Studios owe me?", lambda rs: rs[0]["result"] == 200000),
    ("List my unpaid invoices", lambda rs: len(rs) == 5),
    ("Has Maruti paid me?", lambda rs: len(rs) == 1 and rs[0]["fees"] == 175000),
    ("Who are my clients?", lambda rs: len(rs) == 5),
]

MUST_DEFER = [
    "How many unpaid jobs do I have?",     # count + status  → planner
    "How much money is still unpaid?",     # value + status  → planner (sum)
    "Show me all Garnier jobs",            # list + client   → planner
    "Show me Samsung jobs",                # list + client   → planner
    "Show me Star Studios jobs",           # list + client   → planner
    "How many jobs did I do in March 2026?",  # count + date → planner
    "What are my total fees for Garnier?",    # sum + client  → planner
    "How many jobs do I have for Garnier?",   # count + client→ planner
]


@pytest.mark.parametrize("q,check", MUST_ANSWER)
def test_router_answers_correctly(q, check):
    r, rows = _answer(q)
    assert r is not None, f"router should handle {q!r} but abstained"
    assert rows, f"no rows for {q!r}"
    assert check(rows), f"wrong answer for {q!r}: {rows[:3]}"


@pytest.mark.parametrize("q", MUST_DEFER)
def test_router_abstains_on_qualified_queries(q):
    r = route_common_query(q, UID)
    assert r is None, (
        f"router must abstain on {q!r} so the planner can honour the qualifier; "
        f"instead it answered via route {r.name!r}: {r.sql}"
    )
