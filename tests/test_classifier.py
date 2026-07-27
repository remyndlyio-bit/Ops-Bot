"""
services/classifier.py — the FlowMachine v2 Verdict classifier.

Session 1-2.5 shipped this file with test coverage only ever run ad-hoc via
`python3 -c` (per services/FLOW_MACHINE_V2.md's own notes) — nothing durable
in the suite. This backfills real coverage for the existing parser AND covers
the WP-2 additions (references_last_answer, resolved_query, the ledger
context block) together, since they're the same file and the same contract.

PURE tests only: classify() itself calls a real Gemini instance, so these
test _parse_verdict / _build_prompt / _ledger_block directly, and classify()
only with a mocked gemini object (no network).
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
import pytest
from unittest.mock import MagicMock

from services.classifier import (
    _parse_verdict, _build_prompt, _ledger_block, _flow_compat_block, classify,
    VALID_INTENTS, VALID_FLOW_COMPAT,
)
from services.answer_ledger import LedgerEntry


def _raw(**kw):
    base = {"intent": "READ_QUERY", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False, "flow_compatible": None}
    base.update(kw)
    return json.dumps(base)


class TestParseVerdictExistingFields:
    """Backfilled coverage for the pre-WP-2 parser contract."""

    def test_valid_intent_parsed(self):
        v = _parse_verdict(_raw(intent="READ_AGGREGATE"), "how many jobs")
        assert v["intent"] == "READ_AGGREGATE"

    def test_invalid_intent_coerced_to_unknown(self):
        v = _parse_verdict(_raw(intent="MADE_UP_INTENT"), "x")
        assert v["intent"] == "UNKNOWN"

    def test_lowercase_intent_normalised(self):
        v = _parse_verdict(_raw(intent="read_query"), "x")
        assert v["intent"] == "READ_QUERY"

    @pytest.mark.parametrize("intent", sorted(VALID_INTENTS))
    def test_every_valid_intent_accepted(self, intent):
        v = _parse_verdict(_raw(intent=intent), "x")
        assert v["intent"] == intent

    def test_confidence_clamped_above_one(self):
        v = _parse_verdict(_raw(confidence=5.0), "x")
        assert v["confidence"] == 1.0

    def test_confidence_clamped_below_zero(self):
        v = _parse_verdict(_raw(confidence=-3.0), "x")
        assert v["confidence"] == 0.0

    def test_missing_confidence_defaults_zero(self):
        raw = json.dumps({"intent": "SMALL_TALK", "parameters": {}})
        v = _parse_verdict(raw, "hi")
        assert v["confidence"] == 0.0

    def test_non_dict_parameters_coerced_to_empty(self):
        raw = json.dumps({"intent": "READ_QUERY", "parameters": "not a dict", "confidence": 0.9})
        v = _parse_verdict(raw, "x")
        assert v["parameters"] == {}

    def test_malformed_json_returns_none(self):
        assert _parse_verdict("not json at all {{{", "x") is None

    def test_empty_string_returns_none(self):
        assert _parse_verdict("", "x") is None

    def test_strips_markdown_code_fences(self):
        """_parse_verdict itself strips ```json fences — a model that ignores
        the 'no markdown' instruction must not produce a hard failure."""
        fenced = "```json\n" + _raw() + "\n```"
        v = _parse_verdict(fenced, "x")
        assert v is not None and v["intent"] == "READ_QUERY"

    @pytest.mark.parametrize("fc", sorted(VALID_FLOW_COMPAT))
    def test_valid_flow_compatible_values(self, fc):
        v = _parse_verdict(_raw(flow_compatible=fc), "x")
        assert v["flow_compatible"] == fc

    def test_invalid_flow_compatible_becomes_none(self):
        v = _parse_verdict(_raw(flow_compatible="MADE_UP"), "x")
        assert v["flow_compatible"] is None

    def test_historical_and_bulk_coerced_to_bool(self):
        v = _parse_verdict(_raw(historical=1, bulk=0), "x")
        assert v["historical"] is True and v["bulk"] is False


class TestParseVerdictWP2Fields:
    def test_references_last_answer_true(self):
        v = _parse_verdict(_raw(references_last_answer=True), "does this include paid and unpaid?")
        assert v["references_last_answer"] is True

    def test_references_last_answer_defaults_false(self):
        v = _parse_verdict(_raw(), "how many jobs")
        assert v["references_last_answer"] is False

    def test_references_last_answer_coerced_from_truthy(self):
        v = _parse_verdict(_raw(references_last_answer=1), "x")
        assert v["references_last_answer"] is True

    def test_resolved_query_parsed_when_dict(self):
        rq = {"client_name": "Nike", "time_range": None, "metric_hint": "sum"}
        v = _parse_verdict(_raw(resolved_query=rq), "what about Nike")
        assert v["resolved_query"] == rq

    def test_resolved_query_none_when_absent(self):
        v = _parse_verdict(_raw(), "x")
        assert v["resolved_query"] is None

    def test_resolved_query_coerced_to_none_when_not_a_dict(self):
        v = _parse_verdict(_raw(resolved_query="not a dict"), "x")
        assert v["resolved_query"] is None

    def test_resolved_query_coerced_to_none_when_null_literal(self):
        v = _parse_verdict(_raw(resolved_query=None), "x")
        assert v["resolved_query"] is None


class TestLedgerBlock:
    """The classifier prompt's rendering of AnswerLedger context — must be
    compact one-liners, never raw JSON (same discipline as KnowledgeBook's
    examples_block, for the same reason: a JSON blob in the prompt gets
    echoed/garbled instead of taught from)."""

    def test_empty_when_no_entries(self):
        assert _ledger_block(None) == ""
        assert _ledger_block([]) == ""

    def test_renders_question_kind_value(self):
        entries = [LedgerEntry(question="What's my total earning so far?",
                                kind="aggregate", scope={"filters": {}, "time_range": None},
                                value=1175000)]
        block = _ledger_block(entries)
        assert "What's my total earning so far?" in block
        assert "aggregate" in block
        assert "1175000" in block

    def test_never_contains_raw_json_braces_for_scope(self):
        entries = [LedgerEntry(question="q", kind="aggregate",
                                scope={"filters": {"paid": "no", "client_name": "Nike"}, "time_range": None},
                                value=5000)]
        block = _ledger_block(entries)
        # The block must show filters as compact key=value, not a {"paid": "no"} dump.
        assert "paid=no" in block
        assert '{"paid"' not in block

    def test_only_last_three_entries_included(self):
        entries = [LedgerEntry(question=f"q{i}", kind="list", scope={}) for i in range(6)]
        block = _ledger_block(entries)
        assert "q5" in block and "q0" not in block
        assert block.count(" -> ") == 3

    def test_time_range_shown_when_present(self):
        entries = [LedgerEntry(
            question="q1 total", kind="aggregate",
            scope={"filters": {}, "time_range": {"type": "absolute",
                   "value": {"start": "2026-01-01", "end": "2026-03-31"}}},
            value=100000)]
        block = _ledger_block(entries)
        assert "2026-01-01" in block

    def test_no_time_range_says_all_time(self):
        entries = [LedgerEntry(question="q", kind="aggregate", scope={"filters": {}, "time_range": None}, value=1)]
        assert "all-time" in _ledger_block(entries)


class TestBuildPromptIncludesLedger:
    def test_ledger_block_appears_in_full_prompt(self):
        entries = [LedgerEntry(question="What's my total earning so far?",
                                kind="aggregate", scope={"filters": {}, "time_range": None}, value=1175000)]
        prompt = _build_prompt("does this include paid and unpaid?", "id,fees,paid", "",
                                ledger_entries=entries)
        assert "RECENT ANSWERS" in prompt
        assert "What's my total earning so far?" in prompt

    def test_no_ledger_block_when_no_entries(self):
        # "RECENT ANSWERS" also appears in the always-on schema documentation
        # for resolved_query, so assert on the ledger block's actual content
        # (an entry line) rather than that literal substring anywhere in the
        # prompt.
        prompt = _build_prompt("how many jobs", "id,fees", "", ledger_entries=None)
        assert "most recent last" not in prompt
        assert " -> aggregate " not in prompt and " -> list " not in prompt

    def test_verdict_schema_documents_new_fields(self):
        prompt = _build_prompt("x", "id,fees", "")
        assert "references_last_answer" in prompt
        assert "resolved_query" in prompt

    def test_schema_distinguishes_from_historical(self):
        """The prompt must explicitly tell the model these are different
        concepts, or it'll conflate 'what was it before' with 'what does
        this cover' — exactly the ambiguity WP-2 exists to resolve."""
        prompt = _build_prompt("x", "id,fees", "")
        assert "Distinct from" in prompt or "distinct from" in prompt.lower()


class TestClassifyPassesLedgerThrough:
    """classify() itself never hits the network in these tests — gemini is a
    MagicMock. Verifies the ledger_entries argument actually reaches the
    prompt sent to _call_api, and that the parsed verdict logs the new
    fields without raising."""

    def _gemini(self, response_json):
        g = MagicMock()
        g._ensure_initialized.return_value = None
        g._initialized = True
        g.api_key = "fake"
        g._load_features_doc.return_value = ""
        g._call_api.return_value = json.dumps(response_json)
        return g

    def test_ledger_entries_reach_the_prompt(self):
        entries = [LedgerEntry(question="What's my total earning so far?",
                                kind="aggregate", scope={"filters": {}, "time_range": None}, value=1175000)]
        gemini = self._gemini({"intent": "READ_QUERY", "parameters": {}, "confidence": 0.9,
                                "historical": False, "bulk": False, "flow_compatible": None,
                                "references_last_answer": True, "resolved_query": None})
        classify("does this include paid and unpaid?", gemini, ledger_entries=entries)
        sent_prompt = gemini._call_api.call_args.args[0]
        assert "What's my total earning so far?" in sent_prompt

    def test_verdict_references_last_answer_surfaces(self):
        gemini = self._gemini({"intent": "READ_QUERY", "parameters": {}, "confidence": 0.9,
                                "historical": False, "bulk": False, "flow_compatible": None,
                                "references_last_answer": True, "resolved_query": None})
        v = classify("is that only Nike?", gemini, ledger_entries=[
            LedgerEntry(question="q", kind="aggregate", scope={"filters": {"client_name": "Nike"}}, value=1)
        ])
        assert v["references_last_answer"] is True

    def test_no_ledger_entries_still_works(self):
        """ledger_entries is optional — omitting it must not break classify()."""
        gemini = self._gemini({"intent": "SMALL_TALK", "parameters": {}, "confidence": 0.95,
                                "historical": False, "bulk": False, "flow_compatible": None})
        v = classify("hi", gemini)
        assert v["intent"] == "SMALL_TALK"
        assert v["references_last_answer"] is False
        assert v["resolved_query"] is None

    def test_call_api_gets_bumped_token_budget(self):
        gemini = self._gemini({"intent": "SMALL_TALK", "parameters": {}, "confidence": 0.9,
                                "historical": False, "bulk": False, "flow_compatible": None})
        classify("hi", gemini)
        gc = gemini._call_api.call_args.kwargs.get("generation_config") or gemini._call_api.call_args.args[1]
        assert gc["maxOutputTokens"] >= 400
