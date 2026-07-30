"""
Phase 1.5: NEW_FLOW dispatch — apply the classifier's verdict instead of
re-asking the same question via legacy's is_new_query_not_response LLM call.

Full push/pop (resume the abandoned flow later with a nudge) is deferred —
that needs every flow's completion point to know how to pop back
(ASSISTANT_PLAN.md WP-3, "once more flows are migrated and stack invariants
can be guaranteed everywhere"). This lands a safe, testable slice: a
high-confidence NEW_FLOW verdict clears the current flow's legacy mirror
flags immediately, so legacy's own intent-shift guard sees no pending state
and skips its redundant LLM call — same outcome, one fewer call per turn.
A low-confidence verdict changes nothing (falls through exactly as before).
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import MagicMock, patch

from services.flow_machine import FLOW_DISAMBIGUATION, FLOW_BANK_DETAILS
from services.flow_dispatcher import dispatch_in_flow, SHADOW_ONLY


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
    svc.memory.get_form_state.return_value = None
    svc.flow_machine = MagicMock()
    return svc


class TestNewFlowHighConfidence:
    """High-confidence NEW_FLOW clears state and always returns SHADOW_ONLY."""

    def test_clears_flow_state_on_high_confidence(self):
        svc = _make_svc()
        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "NEW_FLOW",
            "raw_message": "add a job for Acme, 25k, shoot",
            "parameters": {}, "confidence": 0.95, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )

        assert result is SHADOW_ONLY
        svc.flow_machine.reset.assert_called_once_with("u1")
        # Legacy mirror flags cleared via update_user_memory
        svc.memory.update_user_memory.assert_called()
        clear_patch = svc.memory.update_user_memory.call_args.args[1]
        assert clear_patch.get("pending_disambiguation") is None

    def test_still_returns_shadow_only(self):
        """The message itself always proceeds through legacy — only the
        clear happens differently."""
        svc = _make_svc()
        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "NEW_FLOW",
            "raw_message": "add a job for Nike, 50k",
            "parameters": {}, "confidence": 0.99, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_BANK_DETAILS, current_context={},
            conversation_history=[],
        )
        assert result is SHADOW_ONLY

    def test_confidence_exactly_at_threshold_clears(self):
        """0.7 (the threshold) should clear — boundary is inclusive."""
        svc = _make_svc()
        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "NEW_FLOW",
            "raw_message": "log a new job",
            "parameters": {}, "confidence": 0.7, "historical": False, "bulk": False,
        }
        dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={},
            conversation_history=[],
        )
        svc.flow_machine.reset.assert_called_once()


class TestNewFlowLowConfidence:
    """Low-confidence NEW_FLOW changes nothing — legacy's guard decides alone."""

    def test_does_not_clear_flow_state_below_threshold(self):
        svc = _make_svc()
        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "NEW_FLOW",
            "raw_message": "maybe add a job? not sure",
            "parameters": {}, "confidence": 0.5, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={"count": 2, "type": "delete"},
            conversation_history=[],
        )

        assert result is SHADOW_ONLY
        svc.flow_machine.reset.assert_not_called()
        svc.memory.update_user_memory.assert_not_called()

    def test_missing_confidence_treated_as_zero(self):
        """No confidence field at all -> defaults to 0.0, below threshold."""
        svc = _make_svc()
        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "NEW_FLOW",
            "raw_message": "add a job",
            "parameters": {}, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={},
            conversation_history=[],
        )
        assert result is SHADOW_ONLY
        svc.flow_machine.reset.assert_not_called()


class TestNewFlowExceptionSafety:
    """A failure inside _clear_flow_state must never break the turn."""

    def test_clear_flow_state_exception_falls_back_gracefully(self):
        svc = _make_svc()
        svc._clear_flow_state = MagicMock(side_effect=Exception("memory write failed"))

        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "NEW_FLOW",
            "raw_message": "add a job for Acme",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_DISAMBIGUATION, current_context={},
            conversation_history=[],
        )
        # Must still return SHADOW_ONLY, not raise
        assert result is SHADOW_ONLY


class TestClearFlowStateHelper:
    """IntentService._clear_flow_state itself, in isolation."""

    def test_clear_flow_state_resets_flow_machine(self):
        with patch("services.intent_service.GeminiService"), \
             patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), \
             patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.flow_machine = MagicMock()
        svc.memory = MagicMock()
        svc.memory.get_form_state.return_value = None

        svc._clear_flow_state("u1")

        svc.flow_machine.reset.assert_called_once_with("u1")
        svc.memory.update_user_memory.assert_called_once()

    def test_clear_flow_state_cancels_active_form(self):
        with patch("services.intent_service.GeminiService"), \
             patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), \
             patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.flow_machine = MagicMock()
        svc.memory = MagicMock()
        svc.memory.get_form_state.return_value = {"step": "confirm"}  # active form

        svc._clear_flow_state("u1")

        svc.memory.cancel_form.assert_called_once_with("u1")

    def test_clear_flow_state_never_raises_on_memory_failure(self):
        with patch("services.intent_service.GeminiService"), \
             patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), \
             patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.flow_machine = MagicMock()
        svc.flow_machine.reset.side_effect = Exception("boom")
        svc.memory = MagicMock()
        svc.memory.update_user_memory.side_effect = Exception("boom")
        svc.memory.get_form_state.side_effect = Exception("boom")

        # Must not raise
        svc._clear_flow_state("u1")

    def test_clear_patch_covers_all_known_legacy_flags(self):
        """Every flag the reconciliation function reads FROM must also be
        clearable here, or a NEW_FLOW clear could leave a stale flag that
        re-arms the SAME flow on the very next message.

        awaiting_send_confirmation is deliberately excluded (Phase 2.3):
        FlowMachine is now the sole source of truth for
        INVOICE_AWAIT_SEND_CONFIRM, so there's no legacy flag left to clear.
        """
        from services.intent_service import _ALL_AWAITING_CLEAR_PATCH
        expected_flags = {
            "awaiting_client_billing",
            "awaiting_poc_name", "awaiting_poc_email", "awaiting_invoice_poc_email",
            "awaiting_job_input", "pending_disambiguation",
            "awaiting_bank_details", "awaiting_name_change", "awaiting_link_id",
            "awaiting_invoice_address", "awaiting_job_description",
        }
        for flag in expected_flags:
            assert flag in _ALL_AWAITING_CLEAR_PATCH, f"missing {flag}"
        assert "awaiting_send_confirmation" not in _ALL_AWAITING_CLEAR_PATCH, \
            "should be removed — FlowMachine owns this flow exclusively now"
