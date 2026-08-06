"""
Week 5.1 (PLAN_OF_ACTION.md §9) — the NL->SQL query-answering path was
extracted verbatim out of _process_request_impl's step 4 into its own method,
_handle_query_request, so dispatch_idle can call it directly for
READ_QUERY/READ_AGGREGATE at idle (Week 5.2) the same way it already calls
_handle_invoice_retrieval_request / _handle_create_entry_request for writes.

This is a pure move (same logic, same try/except contract, same return
shape) — these tests pin that contract so Week 5.2's dispatch wiring has a
known-good extraction to build on.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import inspect
from unittest.mock import patch, MagicMock


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


class TestExtractionExists:
    """The method must exist with the exact signature dispatch_idle (Week 5.2)
    will call: (user_id, message, data_user_id, conversation_history, user_mem)."""

    def test_handle_query_request_method_exists(self):
        from services.intent_service import IntentService
        assert hasattr(IntentService, "_handle_query_request")

    def test_handle_query_request_signature(self):
        from services.intent_service import IntentService
        sig = inspect.signature(IntentService._handle_query_request)
        params = list(sig.parameters)
        assert params == [
            "self", "user_id", "message", "data_user_id",
            "conversation_history", "user_mem",
        ]


class TestLegacyCascadeStillCallsTheExtractedMethod:
    """process_request's legacy path must still answer a plain query
    correctly, proving the one-line call site (_process_request_impl's step
    4 now just returns self._handle_query_request(...)) is wired right."""

    def test_router_path_query_still_answered_end_to_end(self):
        svc = _svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True, "rows": [{"result": 500000}], "operation": "select",
        }
        svc.gemini.synthesize_response.return_value = "₹5,00,000"
        result = svc.process_request("u1", "What is my total billing?")
        assert result["operation"] == "query"
        assert "5,00,000" in result["response"]

    def test_can_be_called_directly_with_the_documented_signature(self):
        svc = _svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True, "rows": [{"result": 42}], "operation": "select",
        }
        svc.gemini.synthesize_response.return_value = "42 jobs"
        result = svc._handle_query_request(
            "u1", "How many jobs have I done?", "u1", [], {},
        )
        assert result["operation"] == "query"
        assert result["trigger_invoice"] is False
        assert result["invoice_data"] == {}


class TestExtractedMethodOwnsItsOwnExceptionHandling:
    """The extraction moved the try/except tail (log + calm error phrase)
    into the new method itself, so an exception raised anywhere in the query
    path is caught there — never leaks past _handle_query_request, and never
    needs the outer _process_request_impl try to catch it."""

    def test_exception_in_query_path_returns_calm_error_not_a_crash(self):
        svc = _svc()
        with patch("services.intent_service.JOB_ENTRIES_COLUMNS", new=None):
            # JOB_ENTRIES_COLUMNS=None breaks the very first line of the
            # extracted try block (`[c for c in JOB_ENTRIES_COLUMNS ...]`),
            # guaranteeing the exception is raised inside _handle_query_request.
            result = svc._handle_query_request(
                "u1", "How many jobs have I done?", "u1", [], {},
            )
        assert result["operation"] == "query"
        assert result["response"]
        assert result["trigger_invoice"] is False
        assert result["invoice_data"] == {}
        # No raw exception text — the calm, on-brand error phrase, not a stack trace.
        assert "Traceback" not in result["response"]
