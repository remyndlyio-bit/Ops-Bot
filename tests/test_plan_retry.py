"""
Path 3 Phase 3b — the strict-mode retry loop.

When the planner emits a plan that the canonical filter validator
rejects, execute_query_plan should:

  1. Re-call the planner ONCE with feedback_for_retry() injected.
  2. If the retry produces a valid plan → continue to SQL with the
     retry plan (user never sees the bug).
  3. If the retry STILL fails → return a clarification message rather
     than ship known-wrong SQL.

These tests stub Gemini + Supabase to exercise both paths
deterministically. They are the regression contract for the retry loop.
"""

import json
import os
import sys
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Ensure the column registry is loaded.
import services.columns  # noqa: F401
from services.query_planner import execute_query_plan


# ─────────────────────────────────────────────────────────────────────
# Helpers — minimal fakes for Gemini + Supabase
# ─────────────────────────────────────────────────────────────────────

def _fake_supabase():
    """Just enough surface for execute_query_plan."""
    fake = MagicMock()
    fake.get_schema.return_value = {
        "description": "- job_date (date)\n- invoice_date (date)\n- "
                       "bill_sent (text)\n- client_name (text)\n- "
                       "fees (numeric)\n- paid (text)\n- poc_email (text)",
        "columns": ["job_date", "invoice_date", "bill_sent",
                    "client_name", "fees", "paid", "poc_email"],
    }
    return fake


def _gemini_with_scripted_responses(*responses):
    """A Gemini stub whose `_call_api` returns the scripted responses
    in order. Used to simulate 'first call returns bad plan, second
    call returns good plan' etc."""
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


# ─────────────────────────────────────────────────────────────────────
# Phase 3b contract tests
# ─────────────────────────────────────────────────────────────────────

class TestStrictModeRetrySuccess:
    """First plan is invalid (junk date filter) → retry returns a valid
    plan → pipeline produces SQL using the retry plan."""

    def test_invalid_then_valid(self, monkeypatch):
        monkeypatch.setenv("STRICT_PLAN_VALIDATION", "1")

        # Classifier (1 call) + invalid plan + retry plan.
        gemini = _gemini_with_scripted_responses(
            # First planner call — junk date filter:
            json.dumps({
                "operation": "query",
                "metric": "count",
                "filters": {"invoice_date": "sometime last week"},
                "confidence": "high",
            }),
            # Retry — corrected:
            json.dumps({
                "operation": "query",
                "metric": "count",
                "filters": {
                    "invoice_date": {"operator": ">=", "value": "2026-06-01"}
                },
                "confidence": "high",
            }),
        )

        result = execute_query_plan(
            message="how many invoices since June",
            gemini_service=gemini,
            supabase_service=_fake_supabase(),
            user_id="test-user",
        )

        # Retry succeeded → we get SQL, not a clarification.
        assert result["_error"] is None
        assert result["clarification"] is None
        assert result["sql"] is not None
        # The retry plan's date filter is what made it into SQL.
        assert "invoice_date >= '2026-06-01'" in result["sql"]


class TestStrictModeRetryExhausted:
    """Both attempts are invalid → user gets a clarification, NOT a
    broken SQL query."""

    def test_two_invalid_plans_yields_clarification(self, monkeypatch):
        monkeypatch.setenv("STRICT_PLAN_VALIDATION", "1")

        gemini = _gemini_with_scripted_responses(
            # Classifier fallback:
            # First attempt — junk:
            json.dumps({
                "operation": "query",
                "filters": {"invoice_date": "sometime last week"},
                "confidence": "high",
            }),
            # Second attempt — still junk:
            json.dumps({
                "operation": "query",
                "filters": {"invoice_date": "around then"},
                "confidence": "high",
            }),
        )

        result = execute_query_plan(
            message="show invoices from around then",
            gemini_service=gemini,
            supabase_service=_fake_supabase(),
            user_id="test-user",
        )

        # No SQL — we refused to ship a query we know is broken.
        assert result["sql"] is None
        # User-facing clarification, not an internal error.
        assert result["_error"] is None
        assert result["clarification"] is not None
        assert "rephrase" in result["clarification"].lower()


class TestStrictModeBypassedWhenFlagOff:
    """STRICT_PLAN_VALIDATION=0 reverts to shadow behaviour — invalid
    plans pass through to the legacy SQL builder. Escape hatch for
    emergency rollback."""

    def test_flag_off_does_not_retry(self, monkeypatch):
        monkeypatch.setenv("STRICT_PLAN_VALIDATION", "0")

        # Only ONE planner response — if a retry happened the test
        # would AssertionError (StopIteration in _gemini_with_scripted_responses).
        gemini = _gemini_with_scripted_responses(
            json.dumps({
                "operation": "query",
                "metric": "count",
                "filters": {"invoice_date": "sometime last week"},
                "confidence": "high",
            }),
        )

        result = execute_query_plan(
            message="how many invoices",
            gemini_service=gemini,
            supabase_service=_fake_supabase(),
            user_id="test-user",
        )

        # Legacy SQL builder ran. _error is None; SQL is non-None.
        # (The legacy builder will produce a NULL match — semantically
        # wrong, but that's what shadow mode means.)
        assert result["_error"] is None


class TestValidPlanFirstTryNoRetry:
    """A clean plan must not trigger a retry — the LLM is only re-called
    when validation actually fails."""

    def test_no_retry_when_valid(self, monkeypatch):
        monkeypatch.setenv("STRICT_PLAN_VALIDATION", "1")

        gemini = _gemini_with_scripted_responses(
            json.dumps({
                "operation": "query",
                "metric": "count",
                "filters": {"bill_sent": "no"},
                "confidence": "high",
            }),
            # No second response scripted — a retry would AssertionError.
        )

        result = execute_query_plan(
            message="how many pending invoices",
            gemini_service=gemini,
            supabase_service=_fake_supabase(),
            user_id="test-user",
        )

        assert result["_error"] is None
        assert result["sql"] is not None
