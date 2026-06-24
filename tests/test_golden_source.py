"""
Tests for the golden source (Phase 1): oracle correctness, corpus integrity,
generator reproducibility, and lexical retrieval relevance. All offline (no LLM).
"""
import os
import sys
import json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from golden.dataset import build_dataset
from golden.oracle import compute_answer, _is_paid, _client_of
from golden.generate import build_corpus, CORPUS_PATH
from services.golden_retriever import GoldenRetriever


ROWS = build_dataset()


def _plan(**kw):
    base = {"metric": None, "column": None, "filters": {}, "time_range": None,
            "group_by": None, "order": None, "limit": None}
    base.update(kw)
    return base


class TestOracle:
    """The oracle is the ground truth — cross-check it against an independent
    computation over the same dataset."""

    def test_count_all(self):
        assert compute_answer(_plan(metric="count"), ROWS)["value"] == len(ROWS)

    def test_total_all(self):
        assert compute_answer(_plan(metric="sum", column="fees"), ROWS)["value"] == sum(r["fees"] for r in ROWS)

    def test_avg_all(self):
        fees = [r["fees"] for r in ROWS]
        assert compute_answer(_plan(metric="avg", column="fees"), ROWS)["value"] == round(sum(fees) / len(fees))

    def test_count_unpaid_uses_messy_semantics(self):
        # unpaid = NOT in the paid-set (incl. null) — the whole point of the oracle.
        expected = len([r for r in ROWS if not _is_paid(r)])
        assert compute_answer(_plan(metric="count", filters={"paid": "no"}), ROWS)["value"] == expected
        assert expected > 0

    def test_paid_plus_unpaid_equals_all(self):
        paid = compute_answer(_plan(metric="count", filters={"paid": "yes"}), ROWS)["value"]
        unpaid = compute_answer(_plan(metric="count", filters={"paid": "no"}), ROWS)["value"]
        assert paid + unpaid == len(ROWS)

    def test_client_matches_brand(self):
        # "Pepsi" is a brand whose billing client is "Content Lab" — must still match.
        got = compute_answer(_plan(metric="count", filters={"client_name": "Pepsi"}), ROWS)["value"]
        expected = len([r for r in ROWS if "pepsi" in
                        " ".join(str(r.get(k) or "") for k in ("client_name", "brand_name", "production_house")).lower()])
        assert got == expected and got > 0

    def test_biggest_client(self):
        agg = {}
        for r in ROWS:
            agg[_client_of(r)] = agg.get(_client_of(r), 0) + r["fees"]
        top = max(agg.items(), key=lambda kv: kv[1])
        ans = compute_answer(_plan(metric="sum", column="fees", group_by="client_name", order="desc", limit=1), ROWS)
        assert ans["type"] == "client" and ans["value"] == top[0] and ans["amount"] == top[1]

    def test_month_range_filters(self):
        tr = {"type": "absolute", "value": {"start": "2026-03-01", "end": "2026-03-31"}}
        expected = len([r for r in ROWS if "2026-03-01" <= r["job_date"][:10] <= "2026-03-31"])
        assert compute_answer(_plan(metric="count", time_range=tr), ROWS)["value"] == expected


class TestCorpus:
    def test_corpus_file_present_and_valid(self):
        assert os.path.exists(CORPUS_PATH), "run `python -m golden.generate`"
        with open(CORPUS_PATH) as f:
            entries = [json.loads(l) for l in f if l.strip()]
        assert len(entries) >= 50
        ids = [e["id"] for e in entries]
        assert len(ids) == len(set(ids)), "duplicate ids"
        for e in entries:
            assert e["question"] and isinstance(e["plan"], dict) and e["answer"]

    def test_generator_is_reproducible(self):
        assert build_corpus() == build_corpus()

    def test_answers_match_oracle_on_current_dataset(self):
        # Every stored answer must equal a fresh oracle computation (corpus honest).
        for e in build_corpus():
            assert e["answer"] == compute_answer(e["plan"], ROWS), f"stale answer: {e['id']}"


class TestRetriever:
    def setup_method(self):
        self.r = GoldenRetriever()

    def test_loads_corpus(self):
        assert len(self.r.entries) >= 50

    def test_retrieves_relevant_status_query(self):
        hits = self.r.retrieve("how many unpaid jobs do I have", k=5)
        assert hits and any("status" in h["tags"] for h in hits[:3])

    def test_retrieves_client_query(self):
        hits = self.r.retrieve("show me all Garnier work", k=5)
        assert hits and any("garnier" in h["question"].lower() for h in hits[:3])

    def test_fewshot_block_is_prompt_ready(self):
        block = self.r.fewshot_block("what's my total billing this period", k=3)
        assert "PLAN:" in block and "Q:" in block
        # the PLAN lines must be valid JSON
        for line in block.splitlines():
            if line.startswith("PLAN:"):
                json.loads(line[len("PLAN:"):].strip())

    def test_no_match_returns_empty(self):
        assert self.r.fewshot_block("xyzzy plugh frobnicate", k=3) == ""
