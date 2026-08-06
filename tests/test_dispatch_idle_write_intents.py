"""
P0-1 (PLAN_OF_ACTION.md Week 2): flow_dispatcher.dispatch_idle used to treat
every WRITE_* verdict as shadow-only — logged for telemetry, then always
handed to legacy. With FLOW_MACHINE_V2 on, legacy's OWN write-trigger
checks are themselves gated off for CREATE and INVOICE (they only run
`if not _flow_machine_v2_enabled_for(user_id)`), so those two intents had
NO owner at all under v2 — classified correctly, then silently dropped.

These tests exercise dispatch_idle directly (not through the full
process_request cascade) so each WRITE_* branch's wiring is verified in
isolation: the right handler gets called with the right arguments, a
None/falling-through result correctly shadows to legacy, and an exception
never propagates out of the dispatcher.

Week 5.2 (PLAN_OF_ACTION.md §9) adds the same coverage for READ_QUERY /
READ_AGGREGATE, which used to be unconditionally shadow-only here — see
TestReadQueryDispatch below.
"""

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.flow_dispatcher import dispatch_idle, SHADOW_ONLY


class _FakeMemory:
    """Same minimal real (non-Mock) store used by
    tests/test_settings_commands_reachable.py — process_request needs to
    actually read/write memory as it walks the cascade, which a MagicMock
    can't do meaningfully."""

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


def _make_svc():
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
    svc.memory.get_user_memory.return_value = {}
    return svc


def _verdict(intent: str, raw: str, **overrides) -> dict:
    base = {
        "intent": intent,
        "parameters": {},
        "confidence": 0.9,
        "raw_message": raw,
        "historical": False,
        "bulk": False,
        "flow_compatible": None,
        "references_last_answer": False,
        "resolved_query": None,
    }
    base.update(overrides)
    return base


class TestWriteCreateDispatch:
    def test_routes_to_handle_create_entry_request(self):
        svc = _make_svc()
        svc._handle_create_entry_request = MagicMock(
            return_value={"operation": "smart_capture_prompt", "response": "ok",
                          "trigger_invoice": False, "invoice_data": {}}
        )
        verdict = _verdict("WRITE_CREATE", "add a job for Nike 10 April shoot 25k")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=[], data_user_id="u1", user_mem={},
        )
        svc._handle_create_entry_request.assert_called_once_with(
            "u1", "add a job for Nike 10 April shoot 25k",
        )
        assert result["operation"] == "smart_capture_prompt"

    def test_exception_falls_through_to_shadow(self):
        svc = _make_svc()
        svc._handle_create_entry_request = MagicMock(side_effect=Exception("boom"))
        verdict = _verdict("WRITE_CREATE", "add a job")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=[], data_user_id="u1", user_mem={},
        )
        assert result is SHADOW_ONLY


class TestWriteUpdateDispatch:
    def test_routes_to_handle_modify_intent_with_user_mem(self):
        svc = _make_svc()
        svc._handle_modify_intent = MagicMock(
            return_value={"operation": "modify_success", "response": "done",
                          "trigger_invoice": False, "invoice_data": {}}
        )
        user_mem = {"uscf_context": {"last_row_data": {"id": "a"}}}
        verdict = _verdict("WRITE_UPDATE", "mark this as paid")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=[], data_user_id="u1", user_mem=user_mem,
        )
        svc._handle_modify_intent.assert_called_once_with("u1", "mark this as paid", user_mem)
        assert result["operation"] == "modify_success"

    def test_none_result_falls_through_to_shadow(self):
        """_handle_modify_intent returns None when there's no field/value
        AND no row context — 'let the real pipeline have a go', the same
        contract every other SHADOW_ONLY branch relies on."""
        svc = _make_svc()
        svc._handle_modify_intent = MagicMock(return_value=None)
        verdict = _verdict("WRITE_UPDATE", "update")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=[], data_user_id="u1", user_mem={},
        )
        assert result is SHADOW_ONLY

    def test_exception_falls_through_to_shadow(self):
        svc = _make_svc()
        svc._handle_modify_intent = MagicMock(side_effect=Exception("boom"))
        verdict = _verdict("WRITE_UPDATE", "mark this as paid")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=[], data_user_id="u1", user_mem={},
        )
        assert result is SHADOW_ONLY


class TestWriteDeleteDispatch:
    def test_routes_to_handle_soft_delete_with_data_user_id(self):
        svc = _make_svc()
        svc._handle_soft_delete = MagicMock(
            return_value={"operation": "query", "response": "deleted",
                          "trigger_invoice": False, "invoice_data": {}}
        )
        history = [{"role": "user", "content": "hi"}]
        verdict = _verdict("WRITE_DELETE", "delete the last job")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=history, data_user_id="linked_u1", user_mem={},
        )
        svc._handle_soft_delete.assert_called_once_with(
            "u1", "delete the last job", "linked_u1", history,
        )
        assert result["operation"] == "query"

    def test_exception_falls_through_to_shadow(self):
        svc = _make_svc()
        svc._handle_soft_delete = MagicMock(side_effect=Exception("boom"))
        verdict = _verdict("WRITE_DELETE", "delete the last job")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=[], data_user_id="u1", user_mem={},
        )
        assert result is SHADOW_ONLY


class TestWriteInvoiceDispatch:
    def test_routes_to_handle_invoice_retrieval_request_as_definite(self):
        """The classifier's own WRITE_INVOICE verdict IS the confidence
        check legacy's _is_definite_invoice_action regex exists to
        approximate — so the v2 call always passes
        _invoice_action_definite=True, skipping straight to the direct
        regex-extraction path instead of a redundant parse_user_intent call."""
        svc = _make_svc()
        svc._handle_invoice_retrieval_request = MagicMock(
            return_value={"operation": "ACTION_TRIGGER", "response": "On it…",
                          "trigger_invoice": True, "invoice_data": {}}
        )
        history = [{"role": "user", "content": "hi"}]
        user_mem = {"last_saved_job": {"db_client_name": "Nike"}}
        verdict = _verdict("WRITE_INVOICE", "Generate Invoice For Nike April")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=history, data_user_id="linked_u1", user_mem=user_mem,
        )
        svc._handle_invoice_retrieval_request.assert_called_once_with(
            "u1", "Generate Invoice For Nike April", "generate invoice for nike april",
            "linked_u1", history, user_mem, None, True,
        )
        assert result["trigger_invoice"] is True

    def test_exception_falls_through_to_shadow(self):
        svc = _make_svc()
        svc._handle_invoice_retrieval_request = MagicMock(side_effect=Exception("boom"))
        verdict = _verdict("WRITE_INVOICE", "Generate invoice for Nike")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=[], data_user_id="u1", user_mem={},
        )
        assert result is SHADOW_ONLY


class TestReadQueryDispatch:
    """Week 5.2 (PLAN_OF_ACTION.md §9): READ_QUERY/READ_AGGREGATE used to be
    unconditionally shadow-only here — the query pipeline stayed 100%
    legacy even with v2 "on" globally. Now routes to the method Week 5.1
    extracted from legacy's own step 4, the same shape as every WRITE_*
    branch above."""

    def test_read_query_routes_to_handle_query_request(self):
        svc = _make_svc()
        svc._handle_query_request = MagicMock(
            return_value={"operation": "query", "response": "5 jobs found",
                          "trigger_invoice": False, "invoice_data": {}}
        )
        history = [{"role": "user", "content": "hi"}]
        user_mem = {"uscf_context": {"last_row_data": {"id": "a"}}}
        verdict = _verdict("READ_QUERY", "show my jobs")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=history, data_user_id="linked_u1", user_mem=user_mem,
        )
        svc._handle_query_request.assert_called_once_with(
            "u1", "show my jobs", "linked_u1", history, user_mem,
        )
        assert result["response"] == "5 jobs found"

    def test_read_aggregate_routes_to_handle_query_request(self):
        svc = _make_svc()
        svc._handle_query_request = MagicMock(
            return_value={"operation": "query", "response": "₹5,00,000",
                          "trigger_invoice": False, "invoice_data": {}}
        )
        verdict = _verdict("READ_AGGREGATE", "total earnings this month")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=[], data_user_id="u1", user_mem={},
        )
        svc._handle_query_request.assert_called_once_with(
            "u1", "total earnings this month", "u1", [], {},
        )
        assert result["response"] == "₹5,00,000"

    def test_exception_falls_through_to_shadow(self):
        svc = _make_svc()
        svc._handle_query_request = MagicMock(side_effect=Exception("boom"))
        verdict = _verdict("READ_QUERY", "show my jobs")
        result = dispatch_idle(
            verdict, intent_service=svc, user_id="u1",
            conversation_history=[], data_user_id="u1", user_mem={},
        )
        assert result is SHADOW_ONLY


class TestHandleCreateEntryRequest:
    """P0-1: extracted from the legacy add-job trigger site so both legacy
    and dispatch_idle's WRITE_CREATE branch share one implementation."""

    def test_short_message_skips_compound_check_and_captures_directly(self):
        svc = _make_svc()
        svc._start_smart_capture = MagicMock(return_value={"operation": "smart_capture_prompt"})
        svc._handle_create_entry_request("u1", "add a job")
        svc.gemini.decompose_compound_intent.assert_not_called()
        svc._start_smart_capture.assert_called_once_with("u1", "add a job")

    def test_compound_intent_splits_and_stashes_suggested_next_action(self):
        svc = _make_svc()
        svc.gemini.decompose_compound_intent.return_value = [
            "add a job for Nike 10 April shoot 25k",
            "send invoice for Nike",
        ]
        svc._start_smart_capture = MagicMock(return_value={"operation": "smart_capture_prompt"})
        svc._handle_create_entry_request(
            "u1", "add a job for Nike 10 April shoot 25k and send invoice",
        )
        svc._start_smart_capture.assert_called_once_with(
            "u1", "add a job for Nike 10 April shoot 25k",
        )
        stashed = svc.memory.update_user_memory.call_args.args[1]
        assert stashed["suggested_next_action"] == "send invoice for Nike"

    def test_compound_split_returning_one_part_uses_original_message(self):
        svc = _make_svc()
        svc.gemini.decompose_compound_intent.return_value = ["add a job for Nike 10 April shoot 25k"]
        svc._start_smart_capture = MagicMock(return_value={"operation": "smart_capture_prompt"})
        original = "add a job for Nike 10 April shoot 25000 rupees today"
        svc._handle_create_entry_request("u1", original)
        svc._start_smart_capture.assert_called_once_with("u1", original)


class TestEndToEndProcessRequestReachesWriteHandlers:
    """Drives the real process_request cascade (not dispatch_idle in
    isolation) with FLOW_MACHINE_V2 forced on, proving the full chain:
    process_request -> the v2 IDLE block -> classify (stubbed) ->
    dispatch_idle -> the real handler. This is the same shape of
    regression as test_settings_commands_reachable.py: a handler existing
    and being individually correct doesn't help if nothing on the
    production-default v2 path ever reaches it."""

    def _svc(self):
        with patch("services.intent_service.GeminiService"), \
             patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), \
             patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.memory = _FakeMemory()
        svc.supabase = MagicMock()
        svc.gemini = MagicMock()
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"onboarded_at": "2024-01-01", "name": "D"},
        }
        svc.gemini.is_invoice_action_request.return_value = False
        svc.gemini.is_new_query_not_response.return_value = False
        svc.supabase.execute_sql.return_value = {"ok": True, "operation": "select", "rows": []}
        return svc

    def _verdict_patch(self, verdict):
        return patch("services.classifier.classify", return_value=verdict)

    def test_write_create_verdict_reaches_handle_create_entry_request(self):
        svc = self._svc()
        svc._handle_create_entry_request = MagicMock(return_value={
            "operation": "smart_capture_prompt", "response": "ok",
            "trigger_invoice": False, "invoice_data": {},
        })
        verdict = _verdict("WRITE_CREATE", "add a job for Nike 10 April shoot 25k")
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True), \
             self._verdict_patch(verdict):
            result = svc.process_request("u1", "add a job for Nike 10 April shoot 25k")
        svc._handle_create_entry_request.assert_called_once()
        assert result["operation"] == "smart_capture_prompt"

    def test_write_invoice_verdict_reaches_handle_invoice_retrieval_request(self):
        svc = self._svc()
        svc._handle_invoice_retrieval_request = MagicMock(return_value={
            "operation": "ACTION_TRIGGER", "response": "On it…",
            "trigger_invoice": True, "invoice_data": {},
        })
        verdict = _verdict("WRITE_INVOICE", "Generate invoice for Nike April")
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True), \
             self._verdict_patch(verdict):
            result = svc.process_request("u1", "Generate invoice for Nike April")
        svc._handle_invoice_retrieval_request.assert_called_once()
        assert result["trigger_invoice"] is True

    def test_read_query_verdict_reaches_handle_query_request(self):
        """Week 5.2: proves the full chain reaches the extracted query
        method under v2 at idle, not just that dispatch_idle's own branch
        is individually correct."""
        svc = self._svc()
        svc._handle_query_request = MagicMock(return_value={
            "operation": "query", "response": "5 jobs found",
            "trigger_invoice": False, "invoice_data": {},
        })
        verdict = _verdict("READ_QUERY", "show my jobs")
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True), \
             self._verdict_patch(verdict):
            result = svc.process_request("u1", "show my jobs")
        svc._handle_query_request.assert_called_once()
        assert result["response"] == "5 jobs found"
