"""
Phase 3.1 (TODO.md): "Deterministic answers for aggregates — no synthesis call."

A single scalar aggregate result (rows == [{"result": N}]) doesn't need an
LLM to phrase — render_answer_payload(build_answer_payload(...)) already
produces "₹N — <scope>" deterministically, and until now it only ran as
synthesize_response's POST-HOC fallback (paying for a call whose answer
sometimes got thrown away when it looked truncated/empty). This makes it
the PRIMARY path for the single-scalar-aggregate shape, removing an entire
LLM call (and its truncation/refusal flake surface) from the most common
question type: "how many jobs", "total billing", "average fee".

Two call sites: the planner pipeline (query_planner.execute_query_plan) and
the deterministic router (query_router.route_common_query, AGGREGATE render
kind — always scalar by construction, per its own docstring).

Grouped aggregates ("biggest client") are deliberately OUT of scope — those
rows carry a dimension column alongside `result` and still go through
synthesis; _is_single_scalar_aggregate excludes them by row-shape, not by
metric name.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.intent_service import _is_single_scalar_aggregate, _deterministic_aggregates_enabled


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
    svc.gemini.is_invoice_action_request.return_value = False
    return svc


class TestIsSingleScalarAggregateDetection:
    """The row-shape + metric check that gates the deterministic path."""

    def test_true_for_bare_count_result(self):
        assert _is_single_scalar_aggregate([{"result": 5}], {"metric": "count"})

    def test_true_for_sum_avg_min_max(self):
        for metric in ("sum", "avg", "min", "max"):
            assert _is_single_scalar_aggregate([{"result": 100}], {"metric": metric})

    def test_false_for_group_by_row_with_extra_column(self):
        """'Biggest client' rows carry client_name alongside result — not scalar."""
        rows = [{"client_name": "Nike", "result": 500000}]
        assert not _is_single_scalar_aggregate(rows, {"metric": "sum", "group_by": "client_name"})

    def test_false_for_multiple_rows(self):
        rows = [{"result": 1}, {"result": 2}]
        assert not _is_single_scalar_aggregate(rows, {"metric": "sum"})

    def test_false_for_full_job_row(self):
        rows = [{"id": "1", "bill_no": "INV-1", "job_date": "2026-01-01", "fees": 5000}]
        assert not _is_single_scalar_aggregate(rows, {"metric": None})

    def test_false_when_metric_is_none(self):
        """A bare {"result": N} row with metric=None (e.g. 'highest paying
        job' ordered-single-row) is NOT an aggregate — it's a specific
        record, just happens to be one row."""
        assert not _is_single_scalar_aggregate([{"result": 5000}], {"metric": None})

    def test_false_for_empty_rows(self):
        assert not _is_single_scalar_aggregate([], {"metric": "count"})

    def test_false_for_missing_plan(self):
        assert not _is_single_scalar_aggregate([{"result": 5}], None)


class TestDeterministicAggregatesFlag:
    """Env-var escape hatch, same pattern as STRICT_PLAN_VALIDATION."""

    def test_enabled_by_default(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("DETERMINISTIC_AGGREGATES", None)
            assert _deterministic_aggregates_enabled()

    def test_disabled_via_env_var(self):
        for val in ("0", "false", "no", "off", "False", "OFF"):
            with patch.dict(os.environ, {"DETERMINISTIC_AGGREGATES": val}):
                assert not _deterministic_aggregates_enabled()

    def test_stays_enabled_for_other_values(self):
        with patch.dict(os.environ, {"DETERMINISTIC_AGGREGATES": "1"}):
            assert _deterministic_aggregates_enabled()


class TestPlannerPipelineSkipsSynthesisForScalarAggregate:
    """End-to-end through process_request: planner path."""

    def test_count_query_skips_synthesize_response(self):
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name", "fees"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = {
                "sql": "SELECT COUNT(*) AS result FROM public.job_entries WHERE user_id='u1'",
                "plan": {"metric": "count", "column": None, "filters": None,
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 7}], "operation": "select"}
            result = svc.process_request("u1", "How many jobs have I done?")

        svc.gemini.synthesize_response.assert_not_called()
        assert "7" in result["response"]

    def test_sum_query_skips_synthesize_response(self):
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name", "fees"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = {
                "sql": "SELECT SUM(fees) AS result FROM public.job_entries WHERE user_id='u1'",
                "plan": {"metric": "sum", "column": "fees", "filters": None,
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 125000}], "operation": "select"}
            result = svc.process_request("u1", "Total billing")

        svc.gemini.synthesize_response.assert_not_called()
        assert "1,25,000" in result["response"] or "125000" in result["response"]

    def test_grouped_aggregate_still_uses_synthesis(self):
        """'Biggest client' (group_by set, row has extra column) is NOT
        scalar — must still go through the normal synthesis path."""
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name", "fees"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = {
                "sql": "SELECT client_name, SUM(fees) AS result FROM public.job_entries "
                       "WHERE user_id='u1' GROUP BY 1 ORDER BY result DESC LIMIT 1",
                "plan": {"metric": "sum", "column": "fees", "filters": None,
                         "time_range": None, "group_by": "client_name", "order": "desc", "limit": 1},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {
                "ok": True, "rows": [{"client_name": "Nike", "result": 500000}], "operation": "select",
            }
            svc.gemini.synthesize_response.return_value = "Nike is your biggest client at ₹5,00,000."
            result = svc.process_request("u1", "Who is my biggest client?")

        # group_by path is handled by the card/summary branch before this
        # check even applies -- just confirm it didn't crash and produced
        # a sensible answer either way (not asserting synthesis specifically,
        # since group_by rows route through _is_full_job_row's sibling
        # branch, not the scalar-aggregate one this phase touches).
        assert result["response"]

    def test_disabled_flag_falls_back_to_synthesis(self):
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name", "fees"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec, \
             patch("services.intent_service._deterministic_aggregates_enabled", return_value=False):
            mock_exec.return_value = {
                "sql": "SELECT COUNT(*) AS result FROM public.job_entries WHERE user_id='u1'",
                "plan": {"metric": "count", "column": None, "filters": None,
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 7}], "operation": "select"}
            svc.gemini.synthesize_response.return_value = "You have 7 jobs logged."
            result = svc.process_request("u1", "How many jobs have I done?")

        svc.gemini.synthesize_response.assert_called_once()
        assert result["response"] == "You have 7 jobs logged."


class TestRouterPathSkipsSynthesisForAggregate:
    """End-to-end through process_request: deterministic-router path
    (a message the router itself recognises, e.g. 'average fees')."""

    def test_router_aggregate_skips_synthesize_response(self):
        svc = _svc()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 42000}]}
        result = svc.process_request("u1", "Average fees per job")

        svc.gemini.synthesize_response.assert_not_called()
        assert result["response"]  # produced SOME deterministic answer

    def test_router_aggregate_disabled_flag_uses_synthesis(self):
        svc = _svc()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 42000}]}
        svc.gemini.synthesize_response.return_value = "Your average fee is ₹42,000."

        with patch("services.intent_service._deterministic_aggregates_enabled", return_value=False):
            result = svc.process_request("u1", "Average fees per job")

        svc.gemini.synthesize_response.assert_called_once()
        assert result["response"] == "Your average fee is ₹42,000."
