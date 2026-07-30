"""
Phase 3.3 (TODO.md): "Cap and monitor... treat [TELEMETRY_ALERT] as a
CI-able regression: add a test that runs the 10 most common query shapes
through process_request (all AI mocked) and asserts llm_calls <= 2 per
turn via the telemetry counter."

Every existing test in this repo mocks execute_query_plan's OUTPUT rather
than exercising the real classify->plan LLM call inside it (build_operation_
plan calls gemini_service._call_api directly, with no schema/JSON fixture
harness anywhere to drive it safely offline). This test follows the same
convention, but makes the LLM-call ACCOUNTING faithful: the execute_query_
plan mock's side_effect calls note_llm_call() once, mirroring what the real
function costs for a normal high-confidence query (classify_operation is
keyword-matched -- 0 calls -- then build_operation_plan makes exactly ONE
_call_api round-trip). synthesize_response's mock does the same. The turn's
REPORTED llm_calls (read from the real telemetry log line, same technique
as TestProcessRequestWiring in test_telemetry.py) then reflects exactly
what intent_service.py's OWN orchestration logic decided to do with those
two calls -- which is precisely what this test needs to catch a regression
in: some future change re-adding a synthesis call for a shape Phase 3.1
made deterministic, or adding an extra call site nobody budgeted for.

route_common_query (the deterministic router, a SEPARATE earlier-in-the-
cascade optimization) is forced to None in every test here. Without that,
several of these exact query shapes ("average fees", "show my jobs") get
answered by the router before the planner is ever reached -- genuinely
BETTER (0 calls, discovered while writing this test), but it means the
router would silently absorb what this test is specifically trying to
isolate: the planner+synthesis budget. Router-path call counts are the
router's own concern, exercised by tests/test_deterministic_aggregates.py's
TestRouterPathSkipsSynthesisForAggregate instead.

This does NOT catch a regression inside build_operation_plan itself (e.g.
a change that makes it retry twice instead of once) -- that needs a live
AI_KEY harness (tests/test_live_llm_bugs.py) or a dedicated planner-level
call-count test with its own JSON-response fixture, out of scope here.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import patch, MagicMock

from utils.telemetry import note_llm_call


class FakeMemory:
    def __init__(self):
        self._store = {}

    def get_user_memory(self, uid):
        return dict(self._store.get(uid, {}))

    def update_user_memory(self, uid, patch):
        self._store.setdefault(uid, {}).update(patch)

    def get_form_state(self, uid):
        return None

    def get_conversation_history(self, uid):
        return []

    def cancel_form(self, uid):
        pass

    def add_message(self, uid, role, content):
        pass


def _svc():
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()
    svc.memory = FakeMemory()
    svc.supabase = MagicMock()
    svc.gemini = MagicMock()
    svc.supabase.get_user_profile.return_value = {
        "ok": True, "data": {"onboarded_at": "2024-01-01", "name": "D"},
    }
    svc.supabase.get_schema.return_value = {
        "table": "job_entries", "schema_name": "public",
        "columns": ["id", "client_name", "fees", "paid"], "description": "x",
    }
    svc.gemini.is_invoice_action_request.return_value = False
    return svc


def _mock_planner_result(sql, plan):
    """Attach a note_llm_call()-counting side_effect to execute_query_plan,
    mirroring the ONE real _call_api round-trip a normal high-confidence
    plan costs."""
    def _side_effect(*args, **kwargs):
        note_llm_call()
        return {
            "sql": sql, "plan": plan,
            "classification": {"operation": "query", "confidence": "high"},
            "clarification": None, "_error": None,
        }
    return _side_effect


def _mock_synthesis(text):
    def _side_effect(*args, **kwargs):
        note_llm_call()
        return text
    return _side_effect


def _run_and_get_llm_calls(svc, message):
    """Runs through the planner path unconditionally -- the deterministic
    router is forced to abstain (return None) so it can't intercept the
    message before execute_query_plan is reached."""
    calls = []
    with patch("services.intent_service.route_common_query", return_value=None), \
         patch("utils.telemetry.log_turn", side_effect=lambda **kw: calls.append(kw)):
        result = svc.process_request("u1", message)
    assert len(calls) == 1, "expected exactly one telemetry line per turn"
    return calls[0]["llm_calls"], result


class TestScalarAggregateShapesCostOneCall:
    """Phase 3.1 made these deterministic — planner call only, no synthesis."""

    def test_how_many_jobs(self):
        svc = _svc()
        with patch("services.intent_service.execute_query_plan",
                    side_effect=_mock_planner_result(
                        "SELECT COUNT(*) AS result FROM public.job_entries WHERE user_id='u1'",
                        {"metric": "count", "column": None, "filters": None,
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                    )):
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 12}]}
            llm_calls, result = _run_and_get_llm_calls(svc, "How many jobs have I done?")
        assert llm_calls <= 2
        assert llm_calls == 1, "scalar count should cost exactly one call (planner only)"
        svc.gemini.synthesize_response.assert_not_called()

    def test_total_billing(self):
        svc = _svc()
        with patch("services.intent_service.execute_query_plan",
                    side_effect=_mock_planner_result(
                        "SELECT SUM(fees) AS result FROM public.job_entries WHERE user_id='u1'",
                        {"metric": "sum", "column": "fees", "filters": None,
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                    )):
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 500000}]}
            llm_calls, result = _run_and_get_llm_calls(svc, "Total billing")
        assert llm_calls == 1

    def test_average_fee(self):
        svc = _svc()
        with patch("services.intent_service.execute_query_plan",
                    side_effect=_mock_planner_result(
                        "SELECT AVG(fees) AS result FROM public.job_entries WHERE user_id='u1'",
                        {"metric": "avg", "column": "fees", "filters": None,
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                    )):
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 25000}]}
            llm_calls, result = _run_and_get_llm_calls(svc, "Average fee per job")
        assert llm_calls == 1

    def test_count_paid_clients(self):
        svc = _svc()
        with patch("services.intent_service.execute_query_plan",
                    side_effect=_mock_planner_result(
                        "SELECT COUNT(DISTINCT client_name) AS result FROM public.job_entries "
                        "WHERE user_id='u1' AND paid = true",
                        {"metric": "count", "column": "client_name", "filters": {"paid": "yes"},
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                    )):
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 4}]}
            llm_calls, result = _run_and_get_llm_calls(svc, "How many clients have paid?")
        assert llm_calls == 1


class TestListShapesCostAtMostTwoCalls:
    """A non-aggregate result still needs synthesis -- planner + synthesis
    is the expected/budgeted TWO calls, not a regression."""

    def test_show_last_jobs(self):
        svc = _svc()
        with patch("services.intent_service.execute_query_plan",
                    side_effect=_mock_planner_result(
                        "SELECT * FROM public.job_entries WHERE user_id='u1' ORDER BY job_date DESC LIMIT 5",
                        {"metric": None, "column": None, "filters": {},
                         "time_range": None, "group_by": None, "order": "desc", "limit": 5},
                    )), \
             patch.object(svc.gemini, "synthesize_response",
                           side_effect=_mock_synthesis("Here are your last 5 jobs.")):
            svc.supabase.execute_sql.return_value = {
                "ok": True,
                "rows": [{"id": str(i), "client_name": "Nike", "fees": 1000 * i} for i in range(2)],
            }
            llm_calls, result = _run_and_get_llm_calls(svc, "list my jobs summary")
        assert llm_calls <= 2

    def test_client_specific_field_lookup(self):
        svc = _svc()
        with patch("services.intent_service.execute_query_plan",
                    side_effect=_mock_planner_result(
                        "SELECT fees FROM public.job_entries WHERE user_id='u1' AND client_name ILIKE '%Garnier%' "
                        "ORDER BY job_date DESC LIMIT 1",
                        {"metric": None, "column": "fees", "filters": {"client_name": "Garnier"},
                         "time_range": None, "group_by": None, "order": "desc", "limit": 1},
                    )), \
             patch.object(svc.gemini, "synthesize_response",
                           side_effect=_mock_synthesis("The last Garnier fee was ₹25,000.")):
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"fees": 25000}]}
            llm_calls, result = _run_and_get_llm_calls(svc, "what was the last fee on Garnier")
        assert llm_calls <= 2

    def test_grouped_aggregate_biggest_client(self):
        """Grouped aggregates are explicitly OUT of Phase 3.1's scope (row
        carries a dimension column) -- still costs a synthesis call, still
        within budget."""
        svc = _svc()
        with patch("services.intent_service.execute_query_plan",
                    side_effect=_mock_planner_result(
                        "SELECT client_name, SUM(fees) AS result FROM public.job_entries "
                        "WHERE user_id='u1' GROUP BY 1 ORDER BY result DESC LIMIT 1",
                        {"metric": "sum", "column": "fees", "filters": {},
                         "time_range": None, "group_by": "client_name", "order": "desc", "limit": 1},
                    )), \
             patch.object(svc.gemini, "synthesize_response",
                           side_effect=_mock_synthesis("Nike is your biggest client at ₹5,00,000.")):
            svc.supabase.execute_sql.return_value = {
                "ok": True, "rows": [{"client_name": "Nike", "result": 500000}],
            }
            llm_calls, result = _run_and_get_llm_calls(svc, "who is my biggest client")
        assert llm_calls <= 2


class TestBudgetAssertionCatchesRegressions:
    """Sanity check on the test infrastructure itself: if orchestration
    logic DID cost 3 calls, this harness must actually catch it (proving
    the assertion isn't vacuously true)."""

    def test_three_calls_fails_the_budget_assertion(self):
        svc = _svc()

        def _triple_call_side_effect(*args, **kwargs):
            note_llm_call()
            note_llm_call()
            return {
                "sql": "SELECT * FROM public.job_entries WHERE user_id='u1'",
                "plan": {"metric": None, "column": None, "filters": {},
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }

        with patch("services.intent_service.execute_query_plan", side_effect=_triple_call_side_effect), \
             patch.object(svc.gemini, "synthesize_response", side_effect=_mock_synthesis("ok")):
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"id": "1", "fees": 100}]}
            llm_calls, result = _run_and_get_llm_calls(svc, "show me stuff")

        assert llm_calls == 3, "harness sanity check: extra calls must be visible in the count"
        assert llm_calls > 2, "this deliberately exceeds budget -- proves the assertion has teeth"
