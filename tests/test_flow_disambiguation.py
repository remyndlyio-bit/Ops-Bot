"""
WP-3 of ASSISTANT_PLAN.md — migrating disambiguation into FlowMachine v2.

Disambiguation was chosen for this WP-3 slice (over the plan's original
audit-reply suggestion) because it's armed SYNCHRONOUSLY within
process_request (both delete- and modify-type disambiguation write
pending_disambiguation mid-turn), matching every flow FlowMachine v2 already
owns. The overdue-audit reminder flow is armed by a SEPARATE cron process
(workers/reminder_worker.py) outside any process_request call, which doesn't
fit the same reconciliation pattern — it stays on its own (already P0-hardened)
path.

No test file existed for services/flow_machine.py or services/flows.py before
this one, despite three prior migration sessions landing that code.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch

from services.flow_machine import FlowMachine, FLOW_DISAMBIGUATION, FLOW_IDLE, KNOWN_FLOWS
from services.flows import Disambiguation, get_flow


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


DELETE_PENDING = {
    "sql": "UPDATE public.job_entries SET \"isDeleted\" = true WHERE id = '{id}' RETURNING id",
    "rows": [
        {"id": "a", "client_name": "Nike", "bill_no": "INV-1", "fees": 10000},
        {"id": "b", "client_name": "Nike", "bill_no": "INV-2", "fees": 20000},
    ],
    "data_user_id": "u1",
}

MODIFY_PENDING = {
    "type": "modify", "field": "fees", "value": 30000,
    "rows": [
        {"id": "a", "client_name": "Nike", "bill_no": "INV-1", "fees": 10000},
        {"id": "b", "client_name": "Nike", "bill_no": "INV-2", "fees": 20000},
    ],
}


class TestFlowMachineRegistration:
    def test_disambiguation_in_known_flows(self):
        assert FLOW_DISAMBIGUATION in KNOWN_FLOWS

    def test_get_flow_returns_disambiguation_instance(self):
        flow = get_flow(FLOW_DISAMBIGUATION)
        assert isinstance(flow, Disambiguation)

    def test_set_state_accepts_disambiguation(self):
        mem = MagicMock()
        mem.get_user_memory.return_value = {}
        fm = FlowMachine(mem)
        fm.set_state("u1", FLOW_DISAMBIGUATION, {"count": 2, "type": "delete"})
        written = mem.update_user_memory.call_args.args[1]["flow_v2"]
        assert written["flow"] == FLOW_DISAMBIGUATION
        assert written["context"] == {"count": 2, "type": "delete"}


class TestReconciliation:
    """_reconcile_legacy_to_flow_machine's new branch: sync FlowMachine to
    DISAMBIGUATION when pending_disambiguation is armed but v2 is still IDLE."""

    def test_reconciles_delete_type_pending(self):
        svc = _make_svc()
        user_mem = {"pending_disambiguation": DELETE_PENDING}
        svc.flow_machine = MagicMock()
        svc.flow_machine.current_flow.return_value = FLOW_IDLE
        svc.memory.get_form_state.return_value = None
        svc._reconcile_legacy_to_flow_machine("u1", user_mem)
        svc.flow_machine.set_state.assert_called_once()
        args = svc.flow_machine.set_state.call_args.args
        assert args[1] == FLOW_DISAMBIGUATION
        assert args[2]["count"] == 2
        assert args[2]["type"] == "delete"

    def test_reconciles_modify_type_pending(self):
        svc = _make_svc()
        user_mem = {"pending_disambiguation": MODIFY_PENDING}
        svc.flow_machine = MagicMock()
        svc.flow_machine.current_flow.return_value = FLOW_IDLE
        svc.memory.get_form_state.return_value = None
        svc._reconcile_legacy_to_flow_machine("u1", user_mem)
        args = svc.flow_machine.set_state.call_args.args
        assert args[2]["type"] == "modify"

    def test_no_pending_disambiguation_no_op(self):
        svc = _make_svc()
        svc.flow_machine = MagicMock()
        svc.flow_machine.current_flow.return_value = FLOW_IDLE
        svc.memory.get_form_state.return_value = None
        svc._reconcile_legacy_to_flow_machine("u1", {})
        svc.flow_machine.set_state.assert_not_called()

    def test_active_form_state_wins_over_disambiguation(self):
        """Mirrors the legacy precedence at the pending_disambiguation call
        site: an active form_state (smart-capture confirm) takes priority
        over a (potentially stale) disambiguation. Ordering in the reconcile
        if-chain gives this for free -- this test locks that down.

        Uses form_state as the example (checked before disambiguation in
        reconcile's own order — the only thing still checked before it,
        after Phase 2.3's migrations moved every boolean legacy flag,
        including awaiting_job_input, out of reconciliation entirely). The
        equivalent precedence for awaiting_poc_email specifically moved out
        of reconciliation entirely in Phase 2.3 -- INVOICE_NEED_POC_EMAIL
        is FlowMachine-only now (no reconcile branch), and its own
        invoice-email-flow-beats-stale-disambiguation guarantee lives in
        _process_request_impl's _invoice_await_active check instead
        (services/intent_service.py, checks flow_machine.current_flow
        directly)."""
        svc = _make_svc()
        user_mem = {"pending_disambiguation": DELETE_PENDING}
        svc.flow_machine = MagicMock()
        svc.flow_machine.current_flow.return_value = FLOW_IDLE
        svc.memory.get_form_state.return_value = {"step": "confirm"}
        svc._reconcile_legacy_to_flow_machine("u1", user_mem)
        from services.flow_machine import FLOW_SMART_CAPTURE_CONFIRM_PENDING
        args = svc.flow_machine.set_state.call_args.args
        assert args[1] == FLOW_SMART_CAPTURE_CONFIRM_PENDING

    def test_already_tracking_a_flow_is_a_no_op(self):
        svc = _make_svc()
        user_mem = {"pending_disambiguation": DELETE_PENDING}
        svc.flow_machine = MagicMock()
        svc.flow_machine.current_flow.return_value = "SOME_OTHER_FLOW"
        svc._reconcile_legacy_to_flow_machine("u1", user_mem)
        svc.flow_machine.set_state.assert_not_called()


class TestTTLStalenessClearsDisambiguation:
    """Regression: before this WP, a TTL-based FlowMachine reset didn't clear
    the legacy pending_disambiguation flag, so the NEXT message's
    reconciliation would silently re-arm FlowMachine against data the TTL
    logic had just decided was stale."""

    def test_stale_clear_includes_pending_disambiguation(self):
        # _stale_clear was factored out into the shared _ALL_AWAITING_CLEAR_PATCH
        # module constant (services/intent_service.py) — used by both the TTL
        # site and dispatch_in_flow's NEW_FLOW branch via
        # IntentService._clear_flow_state.
        from services.intent_service import _ALL_AWAITING_CLEAR_PATCH
        assert "pending_disambiguation" in _ALL_AWAITING_CLEAR_PATCH


class TestDisambiguationFlowHandleResponse:
    def _svc_with_pending(self, pending):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"pending_disambiguation": pending}
        svc.flow_machine = MagicMock()
        return svc

    def test_numbered_pick_delegates_and_resets_flow(self):
        svc = self._svc_with_pending(DELETE_PENDING)
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"id": "a"}]}
        flow = Disambiguation()
        result = flow.handle_response(svc, "u1", "1", {"count": 2, "type": "delete"})
        assert result["operation"] == "query"
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_modify_type_numbered_pick_actually_writes(self):
        """The exact bug fixed earlier this session (modify-disambiguation
        silent no-op) -- now reachable through the v2 Flow wrapper too."""
        svc = self._svc_with_pending(MODIFY_PENDING)
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"fees": 10000, "notes": ""}]},
            {"ok": True, "rows": [{"id": "a", "fees": 30000}]},
        ]
        flow = Disambiguation()
        result = flow.handle_response(svc, "u1", "1", {"count": 2, "type": "modify"})
        assert result["operation"] == "modify_success"
        update_calls = [c.args[0] for c in svc.supabase.execute_sql.call_args_list
                        if c.args[0].strip().upper().startswith("UPDATE")]
        assert len(update_calls) == 1 and "30000" in update_calls[0]

    def test_cancel_message_delegates_and_resets(self):
        svc = self._svc_with_pending(DELETE_PENDING)
        flow = Disambiguation()
        result = flow.handle_response(svc, "u1", "cancel", {"count": 2, "type": "delete"})
        assert result["operation"] == "query"
        assert "cancel" in result["response"].lower()
        svc.flow_machine.reset.assert_called_once_with("u1")
        svc.supabase.execute_sql.assert_not_called()

    def test_no_pending_state_handled_gracefully(self):
        """pending_disambiguation missing (raced/already resolved elsewhere)
        -- must not crash, must reset v2, must say something honest."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        svc.flow_machine = MagicMock()
        flow = Disambiguation()
        result = flow.handle_response(svc, "u1", "1", {"count": 2, "type": "delete"})
        assert result is not None
        assert result["operation"] == "disambiguation_stale"
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_legacy_none_return_never_propagates_as_none(self):
        """If the legacy handler decides this wasn't really a pick after all
        (ambiguous bare 'yes', or a question that slipped through) and
        returns None, Flow.handle_response must NEVER return None itself --
        that's not a contract any other flow uses and risks the caller
        re-running stale legacy state a second time."""
        svc = self._svc_with_pending(DELETE_PENDING)
        svc._handle_disambiguation_reply = MagicMock(return_value=None)
        flow = Disambiguation()
        result = flow.handle_response(svc, "u1", "what about Garnier", {"count": 2, "type": "delete"})
        assert result is not None
        assert result["operation"] == "disambiguation_cleared"
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_out_of_range_number_reprompts_without_resetting_incorrectly(self):
        svc = self._svc_with_pending(DELETE_PENDING)
        flow = Disambiguation()
        result = flow.handle_response(svc, "u1", "99", {"count": 2, "type": "delete"})
        assert "between 1 and 2" in result["response"]
        svc.supabase.execute_sql.assert_not_called()


class TestDisambiguationFlowResumeNudge:
    def test_mentions_the_count(self):
        flow = Disambiguation()
        nudge = flow.resume_nudge({"count": 3, "type": "delete"})
        assert "3" in nudge

    def test_safe_with_no_context(self):
        flow = Disambiguation()
        nudge = flow.resume_nudge({})
        assert nudge  # non-empty, doesn't crash


class TestDisambiguationFlowOnCancel:
    def test_delegates_to_cancel_and_resets(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"pending_disambiguation": DELETE_PENDING}
        svc.flow_machine = MagicMock()
        flow = Disambiguation()
        result = flow.on_cancel(svc, "u1", "cancel", {"count": 2, "type": "delete"})
        assert result["operation"] == "query"
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_on_cancel_with_no_pending_state_still_safe(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        svc.flow_machine = MagicMock()
        flow = Disambiguation()
        result = flow.on_cancel(svc, "u1", "cancel", {})
        assert result["operation"] == "disambiguation_cancelled"
        svc.flow_machine.reset.assert_called_once_with("u1")


class TestClassifierGuidanceForDisambiguation:
    """The core value-add of this migration: the classifier prompt must teach
    the model that a QUESTION is never a numbered pick, regardless of
    surface content -- replacing a fragile keyword/regex guard with real
    semantic classification."""

    def test_disambiguation_guidance_present_in_flow_compat_block(self):
        from services.classifier import _flow_compat_block
        block = _flow_compat_block("DISAMBIGUATION", {"count": 2})
        assert "SIDE_QUESTION" in block
        assert "scope" in block.lower() or "paid and unpaid" in block.lower()

    def test_no_guidance_block_when_idle(self):
        from services.classifier import _flow_compat_block
        assert _flow_compat_block(None, None) == ""
        assert _flow_compat_block("IDLE", None) == ""


class TestDispatchInFlowIntegration:
    """The end-to-end payoff: dispatch_in_flow routes a scope-clarifying
    question as SIDE_QUESTION (never touching Disambiguation.handle_response
    at all), while a genuine numbered pick reaches it."""

    def test_flow_response_verdict_reaches_handle_response(self):
        from services.flow_dispatcher import dispatch_in_flow
        svc = self._svc()
        verdict = {"intent": "READ_QUERY", "flow_compatible": "FLOW_RESPONSE",
                   "raw_message": "1", "parameters": {}, "confidence": 0.9,
                   "historical": False, "bulk": False}
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )
        assert result is not None
        assert result["operation"] == "query"

    def test_side_question_verdict_never_reaches_handle_response(self):
        """A scope question classified as SIDE_QUESTION must fall to legacy
        (SHADOW_ONLY) for a READ intent -- never call
        Disambiguation.handle_response, which would misinterpret it as a
        pick attempt."""
        from services.flow_dispatcher import dispatch_in_flow
        svc = self._svc()
        with patch.object(Disambiguation, "handle_response") as mock_handle:
            verdict = {"intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
                       "raw_message": "does that include paid and unpaid?",
                       "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False}
            result = dispatch_in_flow(
                verdict, intent_service=svc, user_id="u1",
                current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
                conversation_history=[],
            )
        mock_handle.assert_not_called()
        assert result is None  # SHADOW_ONLY -- legacy pipeline answers it

    def test_cancel_verdict_reaches_on_cancel(self):
        from services.flow_dispatcher import dispatch_in_flow
        svc = self._svc()
        verdict = {"intent": "UNKNOWN", "flow_compatible": "CANCEL",
                   "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
                   "historical": False, "bulk": False}
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )
        assert result is not None and result["operation"] == "query"

    def _svc(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"pending_disambiguation": DELETE_PENDING}
        svc.flow_machine = MagicMock()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"id": "a"}]}
        return svc
