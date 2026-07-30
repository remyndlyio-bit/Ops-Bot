"""
WP-1 integration tests — the full write->read loop through the REAL
process_request cascade (not just the answer_ledger module in isolation, and
not with _process_request_impl mocked out). This is what actually proves the
production transcript is fixed: "What's my total earning so far?" ->
"Do these include, paid and unpaid?" answered with ZERO new SQL and ZERO new
LLM call, from state written by the FIRST turn alone.

Uses a real dict-backed fake memory (not MagicMock) because the whole point
is verifying state round-trips between two separate process_request calls —
a MagicMock's get_user_memory would just return another MagicMock, which
would defeat the test.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock


class FakeMemory:
    """Minimal in-process stand-in for MemoryService: a plain dict per user,
    round-tripping exactly like the real DB-backed JSONB store does."""

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


class TestRouterPathToScopeAnswer:
    """The exact production transcript, via the deterministic router (the
    'total_fees' route 'What's my total earning so far?' actually hits)."""

    def test_scope_question_answered_with_no_new_sql_or_llm_call(self):
        svc = _svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True, "rows": [{"result": 1175000}], "operation": "select",
        }
        svc.gemini.synthesize_response.return_value = "Your total billing comes to ₹11,75,000!"

        r1 = svc.process_request("u1", "What's my total earning so far?")
        assert r1["operation"] == "query"
        assert "11,75,000" in r1["response"]

        svc.supabase.execute_sql.reset_mock()
        svc.gemini.synthesize_response.reset_mock()

        r2 = svc.process_request("u1", "Do these include, paid and unpaid?")
        assert svc.supabase.execute_sql.called is False, "must not hit the DB again"
        assert svc.gemini.synthesize_response.called is False, "must not call the LLM again"
        assert "11,75,000" in r2["response"]
        assert "both paid and unpaid" in r2["response"].lower()

    def test_router_path_writes_to_ledger(self):
        svc = _svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True, "rows": [{"result": 500000}], "operation": "select",
        }
        svc.gemini.synthesize_response.return_value = "₹5,00,000"
        svc.process_request("u1", "What is my total billing?")
        ledger = svc.memory.get_user_memory("u1").get("answer_ledger")
        assert ledger is not None and len(ledger["entries"]) == 1
        assert ledger["entries"][0]["value"] == 500000


class TestPlannerPathToScopeAnswer:
    """Same behaviour via the LLM planner path (execute_query_plan), for a
    message unusual enough not to match the deterministic router."""

    def test_unpaid_filtered_answer_reads_back_correctly(self):
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name", "fees", "paid"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = {
                "sql": "SELECT SUM(fees) AS result FROM public.job_entries WHERE user_id='u1' AND paid = 'no'",
                "plan": {"metric": "sum", "column": "fees", "filters": {"paid": "no"},
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 50000}], "operation": "select"}
            svc.gemini.synthesize_response.return_value = "Your outstanding total is ₹50,000."
            r1 = svc.process_request("u1", "What is the sum of fees still pending across all my active accounts")
        assert "50,000" in r1["response"]

        r2 = svc.process_request("u1", "does this include the paid ones too?")
        assert "only the unpaid" in r2["response"].lower()
        assert "50,000" in r2["response"]

    def test_client_scoped_planner_answer_names_the_client_on_followup(self):
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name", "fees"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = {
                "sql": "SELECT SUM(fees) AS result FROM public.job_entries WHERE user_id='u1' AND client_name ILIKE '%Nike%'",
                "plan": {"metric": "sum", "column": "fees", "filters": {"client_name": "Nike"},
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 300000}], "operation": "select"}
            svc.gemini.synthesize_response.return_value = "₹3,00,000 total for Nike."
            svc.process_request("u1", "total for the shoe brand account this whole relationship")

        r2 = svc.process_request("u1", "is that only Nike?")
        assert "nike" in r2["response"].lower()


class TestScopeCheckDoesNotMisfireOnNormalQueries:
    def test_normal_second_query_still_runs_sql(self):
        """A genuinely new question after an aggregate answer must still hit
        the pipeline normally — the ledger short-circuit is scope-question-
        shaped messages only."""
        svc = _svc()
        svc.gemini.is_history_question.return_value = False
        svc.supabase.execute_sql.return_value = {
            "ok": True, "rows": [{"result": 1175000}], "operation": "select",
        }
        svc.gemini.synthesize_response.return_value = "₹11,75,000"
        svc.process_request("u1", "What's my total earning so far?")

        svc.supabase.execute_sql.reset_mock()
        # "how many unpaid jobs" doesn't match a router route -> goes to the
        # LLM planner, so mock that stage explicitly rather than relying on
        # default MagicMock behaviour for gemini's JSON-returning methods.
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = {
                "sql": "SELECT COUNT(*) AS result FROM public.job_entries WHERE user_id='u1' AND (paid IS NULL OR LOWER(paid) NOT IN ('yes'))",
                "plan": {"metric": "count", "column": None, "filters": {"paid": "no"},
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 3}], "operation": "select"}
            svc.gemini.synthesize_response.return_value = "You have 3 unpaid jobs."
            svc.process_request("u1", "How many unpaid jobs do I have?")
        assert svc.supabase.execute_sql.called, "a genuinely new question must still query the DB"


class TestTruncatedSynthesisFallsBackToDeterministicAnswer:
    """Live production bug (found right after switching FLOW_MACHINE_V2 on):
    a flaky upstream AI call returned "You've had" — 10 characters, cut off
    mid-sentence, no number — as the synthesized reply to "How many have
    paid" (an aggregate count query). It wasn't EMPTY, so the existing
    empty-response fallback didn't catch it, and the incoherent fragment
    went straight out to the user over WhatsApp. A short, digit-free
    response to a data query is essentially always cut off — this must be
    caught and replaced with the deterministic renderer, same as an empty
    response already is."""

    def test_short_digit_free_response_falls_back(self):
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name", "fees", "paid"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = {
                "sql": "SELECT COUNT(DISTINCT client_name) AS result FROM public.job_entries WHERE user_id='u1' AND paid = 'Yes'",
                "plan": {"metric": "count", "column": "client_name", "filters": {"paid": "yes"},
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 12}], "operation": "select"}
            svc.gemini.synthesize_response.return_value = "You've had"
            result = svc.process_request("u1", "How many have paid")
        assert result["response"] != "You've had"
        assert "12" in result["response"]

    def test_normal_short_response_with_a_digit_is_not_treated_as_truncated(self):
        """Over-correction guard: a legitimately short, valid answer that
        happens to contain a number must pass through untouched.

        This row shape (single scalar aggregate) is now answered
        deterministically by default (Phase 3.1, DETERMINISTIC_AGGREGATES) —
        synthesize_response is skipped entirely, so its mocked return value
        would never be exercised. Disabling the flag for this one test keeps
        it testing what it was written to test: the truncation guard's own
        precision on synthesis output, not the deterministic renderer."""
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
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 0}], "operation": "select"}
            svc.gemini.synthesize_response.return_value = "₹0 so far."
            result = svc.process_request("u1", "how much total work have I logged this whole relationship")
        assert result["response"] == "₹0 so far."

    def test_reasonably_long_digit_free_response_is_not_treated_as_truncated(self):
        """Over-correction guard: a genuinely complete prose answer with no
        digits (e.g. a client-name list) must not be flagged as truncated
        just because it lacks numbers."""
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = {
                "sql": "SELECT DISTINCT client_name AS result FROM public.job_entries WHERE user_id='u1'",
                "plan": {"metric": None, "column": "client_name", "filters": None,
                         "time_range": None, "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": "Nike"}], "operation": "select"}
            svc.gemini.synthesize_response.return_value = "Nice — you're working with Nike right now."
            result = svc.process_request("u1", "which single account have I been doing the most work for lately")
        assert result["response"] == "Nice — you're working with Nike right now."
