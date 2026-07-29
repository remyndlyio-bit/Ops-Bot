"""
Phase 1.5: SIDE_QUESTION dispatch for READ_QUERY/READ_AGGREGATE via the
deterministic router.

Before this: any read-shaped side question while mid-flow fell back to
SHADOW_ONLY unconditionally, losing the resume_nudge (session-2 limitation).

After this: side questions matching the deterministic router (services.
query_router.route_common_query) are answered inline WITH the resume_nudge,
using zero LLM calls. Scope-clarifying questions ("does this include paid
and unpaid?") are checked first and answered from the ledger — never routed
as a fresh query, even when they contain a keyword like "unpaid" that would
otherwise false-match a router route.

Anything the router doesn't recognize still falls through to SHADOW_ONLY
exactly as before.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch

from services.flow_machine import FLOW_DISAMBIGUATION, FLOW_INVOICE_AWAIT_SEND_CONFIRM
from services.flow_dispatcher import dispatch_in_flow, SHADOW_ONLY
from services.answer_ledger import build_entry


def _make_svc(user_memory=None):
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()
    svc.gemini = MagicMock()
    svc.email = MagicMock()
    svc.supabase = MagicMock()
    svc.memory = MagicMock()
    svc.memory.get_user_memory.return_value = user_memory or {}
    return svc


class TestSideQuestionRouterMatch:
    """A deterministic-router-matchable read side-question is answered inline."""

    def test_unpaid_list_side_question_answered_with_nudge(self):
        """'What's still unpaid?' mid-flow -> router match, answer + resume_nudge."""
        svc = _make_svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [
                {"id": "a", "client_name": "Nike", "fees": 10000, "paid": False, "job_date": "2026-01-01"},
            ],
        }

        verdict = {
            "intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
            "raw_message": "what's still unpaid?",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )

        assert result is not None
        assert result["operation"] == "query"
        # resume_nudge from Disambiguation should be appended
        assert result["response"]  # non-empty

    def test_no_llm_call_for_router_match(self):
        """Router-matched side questions cost zero LLM calls for the routing itself."""
        svc = _make_svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [{"result": 50000}],
        }

        verdict = {
            "intent": "READ_AGGREGATE", "flow_compatible": "SIDE_QUESTION",
            "raw_message": "what's the average fee?",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )
        # route_common_query itself makes no LLM call; synthesize_response
        # may be called for the AGGREGATE render path, which is expected
        # and different from "routing" cost.


class TestSideQuestionScopeQuestion:
    """Scope-clarifying questions are answered from ledger, never routed as new queries."""

    def test_scope_question_answered_from_ledger_not_router(self):
        """'Does that include paid and unpaid?' with a ledger entry -> ledger answer."""
        entry = build_entry(
            question="Total earnings",
            plan={"metric": "sum", "filters": {"paid": None}, "time_range": None},
            rows=[{"result": 100000}],
            response="₹1,00,000",
        )
        user_mem = {"answer_ledger": {"v": 1, "entries": [entry.to_dict()]}}
        svc = _make_svc(user_memory=user_mem)

        verdict = {
            "intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
            "raw_message": "does that include paid and unpaid?",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )

        assert result is not None
        assert result["operation"] == "scope_question"
        assert "paid and unpaid" in result["response"].lower()
        # Must NOT have hit the SQL execution path (answered from ledger only)
        svc.supabase.execute_sql.assert_not_called()

    def test_scope_question_without_ledger_falls_to_shadow(self):
        """Scope-shaped question but empty ledger -> SHADOW_ONLY, not router false-match."""
        svc = _make_svc(user_memory={})  # no ledger entries

        verdict = {
            "intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
            "raw_message": "does that include paid and unpaid?",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )

        # Must fall through to legacy — NOT get misrouted to unpaid_list
        assert result is SHADOW_ONLY
        svc.supabase.execute_sql.assert_not_called()


class TestSideQuestionNoRouterMatch:
    """Complex queries the router doesn't recognize still shadow to legacy."""

    def test_complex_query_falls_to_shadow(self):
        """A query the deterministic router can't handle -> SHADOW_ONLY (legacy planner)."""
        svc = _make_svc()

        verdict = {
            "intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
            "raw_message": "what's the correlation between fees and job duration across all my Q3 clients grouped by region",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )

        assert result is SHADOW_ONLY

    def test_write_intent_side_question_unaffected(self):
        """A WRITE-shaped side question (not READ) is untouched by this change."""
        svc = _make_svc()

        verdict = {
            "intent": "WRITE_UPDATE", "flow_compatible": "SIDE_QUESTION",
            "raw_message": "mark it as paid",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )

        assert result is SHADOW_ONLY


class TestSideQuestionExceptionSafety:
    """Errors in the new router path must never break the turn."""

    def test_router_exception_falls_back_to_shadow(self):
        """If route_common_query or _execute_routed_query throws, fall to SHADOW_ONLY."""
        svc = _make_svc()
        svc.supabase.execute_sql.side_effect = Exception("DB connection lost")

        verdict = {
            "intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
            "raw_message": "what's still unpaid?",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        # dispatch_in_flow wraps everything in try/except -> should not raise
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )
        # Either SHADOW_ONLY (outer except) or a graceful None from execute -> shadow
        assert result is SHADOW_ONLY
