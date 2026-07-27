"""
Offline coverage for knowledge/understand_eval.py — the WP-2 shadow-eval
harness. The eval itself needs a live AI_KEY to produce a real accuracy
number (same constraint as knowledge/ab_run.py); this pins the harness's own
logic (grading, case integrity) so a bug in the harness can't silently
produce a fake pass/fail rate.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from knowledge.understand_eval import cases, grade


class TestCaseIntegrity:
    def test_at_least_50_cases(self):
        assert len(cases()) >= 50

    def test_unique_ids(self):
        ids = [c["id"] for c in cases()]
        assert len(ids) == len(set(ids)), "duplicate case ids"

    def test_every_case_has_required_keys(self):
        for c in cases():
            assert "id" in c and "category" in c and "message" in c and "gold" in c
            assert c["message"].strip(), f"{c['id']}: empty message"
            assert c["gold"], f"{c['id']}: empty gold — nothing graded"

    def test_categories_cover_the_plan_shapes(self):
        """ASSISTANT_PLAN.md WP-2 names: scope-clarifying questions, historical
        vs references_last_answer, context-inheritance (resolved_query),
        Hinglish follow-ups, and a baseline sanity set."""
        present = {c["category"] for c in cases()}
        assert {"scope", "not_scope", "historical", "resolve", "baseline", "hinglish"} <= present

    def test_ledger_shape_when_present(self):
        from services.answer_ledger import LedgerEntry
        for c in cases():
            if c.get("ledger"):
                assert all(isinstance(e, LedgerEntry) for e in c["ledger"]), c["id"]

    def test_history_shape_when_present(self):
        for c in cases():
            for turn in c.get("history") or []:
                assert turn["role"] in ("user", "assistant"), c["id"]
                assert "content" in turn


class TestGrading:
    def test_none_verdict_always_fails(self):
        fails = grade(None, {"intent": "READ_QUERY"})
        assert fails and "None" in fails[0]

    def test_matching_intent_passes(self):
        assert grade({"intent": "READ_QUERY"}, {"intent": "READ_QUERY"}) == []

    def test_mismatched_intent_fails_with_reason(self):
        fails = grade({"intent": "WRITE_UPDATE"}, {"intent": "READ_QUERY"})
        assert fails and "intent" in fails[0]

    def test_intent_not_checked_when_gold_omits_it(self):
        assert grade({"intent": "anything"}, {"references_last_answer": False}) == []

    def test_references_last_answer_match(self):
        assert grade({"references_last_answer": True}, {"references_last_answer": True}) == []

    def test_references_last_answer_mismatch(self):
        fails = grade({"references_last_answer": False}, {"references_last_answer": True})
        assert fails and "references_last_answer" in fails[0]

    def test_historical_checked_independently_of_references_last_answer(self):
        v = {"historical": True, "references_last_answer": False}
        assert grade(v, {"historical": True, "references_last_answer": False}) == []
        assert grade(v, {"historical": False}) != []

    def test_resolved_query_client_substring_match(self):
        v = {"resolved_query": {"client_name": "Nike India"}}
        assert grade(v, {"resolved_query_client": "Nike"}) == []

    def test_resolved_query_client_mismatch(self):
        v = {"resolved_query": {"client_name": "Garnier"}}
        fails = grade(v, {"resolved_query_client": "Nike"})
        assert fails and "resolved_query" in fails[0]

    def test_resolved_query_missing_when_expected_fails(self):
        v = {"resolved_query": None}
        fails = grade(v, {"resolved_query_client": "Nike"})
        assert fails

    def test_resolved_query_metric_hint_match(self):
        v = {"resolved_query": {"metric_hint": "sum"}}
        assert grade(v, {"resolved_query_metric": "sum"}) == []

    def test_multiple_failures_all_reported(self):
        v = {"intent": "WRITE_UPDATE", "references_last_answer": False}
        fails = grade(v, {"intent": "READ_QUERY", "references_last_answer": True})
        assert len(fails) == 2

    def test_empty_gold_always_passes(self):
        assert grade({"intent": "anything"}, {}) == []
