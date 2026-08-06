"""
P0-3 (PLAN_OF_ACTION.md): the table-dump fallback.

Root cause: classify_operation's own last-resort fallback
(`return {"operation": "query", "confidence": "low"}`) fires whenever
NEITHER a keyword pattern NOR a confident LLM call recognised the message
as a genuine data request — exactly the shape of an SQL-injection string,
a bare link-ID reply, or a Hindi job entry the classifier doesn't
understand. That confidence signal used to be logged and then silently
discarded: execute_query_plan let the planner build a plan from it anyway,
which — with no filters to work from — produced an unfiltered
`SELECT * ... LIMIT 25`. intent_service.py then attaches an Excel export
for any >4-row result, so garbage input got answered with "Found 20
results — here's a spreadsheet" instead of a refusal.

_plan_has_no_real_signal + the low-confidence guard in execute_query_plan
close this: low confidence + a signal-less plan now returns a clarification
instead of executing. A genuine unfiltered request ("show all my jobs")
is unaffected — it matches a _QUERY_PATTERNS keyword, so classify_operation
reports confidence="high" via the keyword path, without ever calling the
LLM classifier at all.
"""

import json
import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import services.columns  # noqa: F401 — loads the column registry
from services.query_planner import execute_query_plan, _plan_has_no_real_signal


def _fake_supabase():
    fake = MagicMock()
    fake.get_schema.return_value = {
        "description": "- job_date (date)\n- client_name (text)\n- "
                       "fees (numeric)\n- paid (text)\n- poc_email (text)",
        "columns": ["job_date", "client_name", "fees", "paid", "poc_email"],
    }
    return fake


def _gemini_with_scripted_responses(*responses):
    fake = MagicMock()
    iterator = iter(responses)

    def _call_api(prompt, generation_config=None):
        try:
            return next(iterator)
        except StopIteration:
            raise AssertionError(
                "Gemini called more times than scripted responses provided"
            )
    fake._call_api.side_effect = _call_api
    return fake


class TestPlanHasNoRealSignal:
    def test_fully_empty_query_plan_has_no_signal(self):
        plan = {"operation": "query", "filters": {}, "metric": None,
                "group_by": None, "time_range": None, "order": None, "limit": None}
        assert _plan_has_no_real_signal(plan) is True

    def test_filters_present_has_signal(self):
        plan = {"operation": "query", "filters": {"client_name": "Nike"}}
        assert _plan_has_no_real_signal(plan) is False

    def test_metric_present_has_signal(self):
        plan = {"operation": "query", "filters": {}, "metric": "count"}
        assert _plan_has_no_real_signal(plan) is False

    def test_group_by_present_has_signal(self):
        plan = {"operation": "query", "filters": {}, "group_by": "client_name"}
        assert _plan_has_no_real_signal(plan) is False

    def test_time_range_present_has_signal(self):
        plan = {"operation": "query", "filters": {}, "time_range": {"start": "2026-01-01"}}
        assert _plan_has_no_real_signal(plan) is False

    def test_order_present_has_signal(self):
        plan = {"operation": "query", "filters": {}, "order": "fees DESC"}
        assert _plan_has_no_real_signal(plan) is False

    def test_limit_present_has_signal(self):
        plan = {"operation": "query", "filters": {}, "limit": 5}
        assert _plan_has_no_real_signal(plan) is False

    def test_zero_limit_still_counts_as_signal(self):
        """limit=0 is falsy but explicitly SET — `is not None` is the
        correct check here, not truthiness."""
        plan = {"operation": "query", "filters": {}, "limit": 0}
        assert _plan_has_no_real_signal(plan) is False

    def test_non_query_operation_never_flagged(self):
        """An update/create plan isn't the 'dumps every row' risk this
        guard exists for, regardless of how sparse it looks."""
        plan = {"operation": "update", "filters": {}}
        assert _plan_has_no_real_signal(plan) is False


class TestLowConfidenceEmptySignalReturnsClarification:
    def test_injection_shaped_input_does_not_execute_unfiltered_select(self, monkeypatch):
        monkeypatch.setenv("STRICT_PLAN_VALIDATION", "1")
        gemini = _gemini_with_scripted_responses(
            # classify_operation's LLM fallback (no keyword pattern matched):
            json.dumps({"operation": "query", "confidence": "low"}),
            # build_operation_plan — the planner tries its best and comes
            # up with nothing to actually filter on:
            json.dumps({
                "operation": "query", "filters": {}, "metric": None,
                "group_by": None, "time_range": None,
            }),
        )
        result = execute_query_plan(
            "'; DROP TABLE job_entries; --", gemini, _fake_supabase(),
            conversation_history=[], user_id="u1",
        )
        assert result["sql"] is None, "must not execute an unfiltered SELECT for unrecognised input"
        assert result["clarification"], "must give the user an honest 'I didn't understand' reply"

    def test_unrecognised_hindi_text_does_not_execute_unfiltered_select(self, monkeypatch):
        monkeypatch.setenv("STRICT_PLAN_VALIDATION", "1")
        gemini = _gemini_with_scripted_responses(
            json.dumps({"operation": "query", "confidence": "low"}),
            json.dumps({
                "operation": "query", "filters": {}, "metric": None,
                "group_by": None, "time_range": None,
            }),
        )
        result = execute_query_plan(
            "Nike ka kaam kiya 10 April ko, shooting, 25 hazaar",
            gemini, _fake_supabase(), conversation_history=[], user_id="u1",
        )
        assert result["sql"] is None
        assert result["clarification"]

    def test_low_confidence_but_real_signal_still_executes(self, monkeypatch):
        """Guard against over-blocking: low confidence alone isn't enough —
        if the planner DID manage to extract a real filter, that's still a
        usable answer and must execute normally."""
        monkeypatch.setenv("STRICT_PLAN_VALIDATION", "1")
        gemini = _gemini_with_scripted_responses(
            json.dumps({"operation": "query", "confidence": "low"}),
            json.dumps({
                "operation": "query",
                "filters": {"client_name": "Nike"},
                "metric": None, "group_by": None, "time_range": None,
            }),
        )
        result = execute_query_plan(
            "ambiguous nike thing", gemini, _fake_supabase(),
            conversation_history=[], user_id="u1",
        )
        assert result["sql"] is not None
        assert result["clarification"] is None


class TestHighConfidenceUnfilteredRequestStillWorks:
    def test_genuine_show_all_jobs_executes_normally(self, monkeypatch):
        """'show all my jobs' matches a _QUERY_PATTERNS keyword ('show'),
        so classify_operation never even calls the LLM — confidence is
        'high' via the keyword path. The empty-filter plan this produces
        is a legitimate unfiltered listing, not garbage input, and must
        still execute."""
        monkeypatch.setenv("STRICT_PLAN_VALIDATION", "1")
        gemini = _gemini_with_scripted_responses(
            # Only ONE scripted response — classify_operation's keyword
            # match means the planner's LLM call is the only one made.
            json.dumps({
                "operation": "query", "filters": {}, "metric": None,
                "group_by": None, "time_range": None,
            }),
        )
        result = execute_query_plan(
            "show all my jobs", gemini, _fake_supabase(),
            conversation_history=[], user_id="u1",
        )
        assert result["sql"] is not None, "a genuine unfiltered request must not be blocked"
        assert result["clarification"] is None
