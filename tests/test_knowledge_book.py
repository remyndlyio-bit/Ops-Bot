"""
Tests for the KnowledgeBook (Phase 1): oracle correctness, examples integrity,
generator reproducibility, rules/glossary rendering, lexical retrieval relevance,
and the assembled grounding context. All offline (no LLM).
"""
import os
import sys
import json
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from knowledge.dataset import build_dataset
from knowledge.oracle import compute_answer, _is_paid, _client_of
from knowledge.generate import build_corpus, CORPUS_PATH
from knowledge import rules as kb_rules
from services.knowledge_book import ExampleIndex, knowledge_context


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
        assert os.path.exists(CORPUS_PATH), "run `python -m knowledge.generate`"
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
        self.r = ExampleIndex()

    def test_loads_examples(self):
        assert len(self.r.entries) >= 50

    def test_retrieves_relevant_status_query(self):
        hits = self.r.retrieve("how many unpaid jobs do I have", k=5)
        assert hits and any("status" in h["tags"] for h in hits[:3])

    def test_retrieves_client_query(self):
        hits = self.r.retrieve("show me all Garnier work", k=5)
        assert hits and any("garnier" in h["question"].lower() for h in hits[:3])

    def test_examples_block_is_compact_hints_not_json(self):
        block = self.r.examples_block("what's my total billing this period", k=3)
        assert "->" in block and '"' in block
        assert "PLAN:" not in block and "{" not in block  # no raw JSON to echo

    def test_no_match_returns_empty(self):
        assert self.r.examples_block("xyzzy plugh frobnicate", k=3) == ""


class TestRules:
    def test_render_has_rules_and_glossary(self):
        block = kb_rules.render()
        assert "KnowledgeBook" in block
        # load-bearing conventions are present
        assert "paid IS NULL" in block
        assert "client_name OR brand_name OR production_house" in block
        assert "glossary" in block.lower()
        assert "earnings" in block.lower()


class TestKnowledgeContext:
    def test_context_combines_rules_and_examples(self):
        ctx = knowledge_context("how much is still unpaid?", k=3)
        assert "KnowledgeBook" in ctx          # rules always on
        assert "Reference — how to read" in ctx  # examples retrieved
        assert "paid IS NULL" in ctx
        assert "{" not in ctx                  # never injects raw JSON
        assert "output ONLY the single" in ctx  # re-asserts the JSON contract

    def test_rules_present_even_with_no_example_match(self):
        ctx = knowledge_context("xyzzy plugh frobnicate", k=3)
        assert "KnowledgeBook" in ctx
        assert "Reference — how to read" not in ctx  # no example matched, rules still there


class TestPlannerWiring:
    """The KnowledgeBook grounds the planner only when the flag is on."""

    def _fake_gemini(self, cap):
        class G:
            def _call_api(self, prompt, generation_config=None):
                cap["prompt"] = prompt
                return '{"metric":"sum","column":"fees","filters":{"paid":"no"}}'
        return G()

    def test_injected_when_enabled(self, monkeypatch):
        from services.query_planner import build_operation_plan
        monkeypatch.setenv("KNOWLEDGE_BOOK", "1")
        cap = {}
        build_operation_plan("how much is unpaid", "query", "schema",
                             ["fees", "paid"], gemini_service=self._fake_gemini(cap))
        assert "KnowledgeBook" in cap["prompt"] and "output ONLY the single" in cap["prompt"]

    def test_not_injected_when_disabled(self, monkeypatch):
        from services.query_planner import build_operation_plan
        monkeypatch.delenv("KNOWLEDGE_BOOK", raising=False)
        cap = {}
        build_operation_plan("how much is unpaid", "query", "schema",
                             ["fees", "paid"], gemini_service=self._fake_gemini(cap))
        assert "KnowledgeBook" not in cap["prompt"]
