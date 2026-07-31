"""
WP-3 slice 3 of ASSISTANT_PLAN.md — migrating the last two invoice-readiness-
gate prompts (_invoice_readiness_check) into FlowMachine v2: business
address and per-job description. Same shape as the already-migrated
INVOICE_NEED_BILLING/POC_NAME/POC_EMAIL — single-shot, always resumes the
invoice flow via _resume_invoice_flow.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch

from services.flow_machine import (
    FLOW_INVOICE_ADDRESS, FLOW_INVOICE_NEED_JOB_DESCRIPTION, FLOW_IDLE, KNOWN_FLOWS,
)
from services.flows import InvoiceAddress, InvoiceNeedJobDescription, get_flow


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
    svc.flow_machine = MagicMock()
    return svc


class TestFlowMachineRegistration:
    @pytest.mark.parametrize("flow_name,cls", [
        (FLOW_INVOICE_ADDRESS, InvoiceAddress),
        (FLOW_INVOICE_NEED_JOB_DESCRIPTION, InvoiceNeedJobDescription),
    ])
    def test_registered(self, flow_name, cls):
        assert flow_name in KNOWN_FLOWS
        assert isinstance(get_flow(flow_name), cls)

    def test_distinct_from_smart_capture_description_flow(self):
        """INVOICE_NEED_JOB_DESCRIPTION (an existing job missing a
        description at invoice time) must be a different flow name from
        SMART_CAPTURE_NEED_DESCRIPTION (a brand new job being logged) --
        they serve genuinely different prompts."""
        from services.flow_machine import FLOW_SMART_CAPTURE_NEED_DESCRIPTION
        assert FLOW_INVOICE_NEED_JOB_DESCRIPTION != FLOW_SMART_CAPTURE_NEED_DESCRIPTION


class TestReconciliation:
    # Phase 2.3: awaiting_invoice_address and awaiting_job_description are
    # both FlowMachine-only now — no reconciliation branch left for either
    # (their arm sites, _arm_invoice_address_v2 / _arm_job_description_v2,
    # write flow_machine.set_state() directly). Arm-site behavior itself is
    # covered by tests/test_invoice_address_and_job_description_v2_only.py.
    def test_no_flags_no_op(self):
        svc = _make_svc()
        svc.flow_machine.current_flow.return_value = FLOW_IDLE
        svc.memory.get_form_state.return_value = None
        svc._reconcile_legacy_to_flow_machine("u1", {})
        svc.flow_machine.set_state.assert_not_called()

    def test_still_legacy_flag_reconciles_normally(self):
        """A flow NOT yet migrated (awaiting_job_input) still reconciles
        the normal way -- confirms this module's migration didn't touch
        the reconciliation mechanism itself."""
        svc = _make_svc()
        svc.flow_machine.current_flow.return_value = FLOW_IDLE
        svc.memory.get_form_state.return_value = None
        from services.flow_machine import FLOW_SMART_CAPTURE_NEED_DESCRIPTION
        svc._reconcile_legacy_to_flow_machine("u1", {"awaiting_job_input": True})
        args = svc.flow_machine.set_state.call_args.args
        assert args[1] == FLOW_SMART_CAPTURE_NEED_DESCRIPTION


class TestNoLegacyMirrorForTheseTwoFlows:
    """Phase 2.3: awaiting_invoice_address and awaiting_job_description are
    both gone -- neither exists in _ALL_AWAITING_CLEAR_PATCH (the TTL/
    NEW_FLOW clear patch) or _AWAITING_FLAGS anymore. Their pending_* payload
    keys stay (still read by the handlers regardless of entry point)."""

    def test_awaiting_flags_removed_but_payload_keys_stay(self):
        from services.intent_service import _ALL_AWAITING_CLEAR_PATCH
        assert "awaiting_invoice_address" not in _ALL_AWAITING_CLEAR_PATCH
        assert "awaiting_job_description" not in _ALL_AWAITING_CLEAR_PATCH
        for payload_key in ("pending_address_user_id", "pending_jobdesc_row_id",
                            "pending_jobdesc_user_id"):
            assert payload_key in _ALL_AWAITING_CLEAR_PATCH, f"missing {payload_key}"

    def test_none_in_awaiting_flags(self):
        svc = _make_svc()
        assert "awaiting_invoice_address" not in svc._AWAITING_FLAGS
        assert "awaiting_job_description" not in svc._AWAITING_FLAGS


class TestInvoiceAddressFlow:
    def test_saves_address_and_resumes_pending_invoice(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"pending_invoice": {"client_name": "Nike", "month": "March", "year": 2026}}
        svc._resume_invoice_flow = MagicMock(return_value={"operation": "resumed", "response": "ok"})
        flow = InvoiceAddress()
        result = flow.handle_response(svc, "u1", "12 MG Road, Bangalore", {"client_name": "Nike"})
        assert result["operation"] == "resumed"
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_standalone_update_no_pending_invoice(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        flow = InvoiceAddress()
        result = flow.handle_response(svc, "u1", "12 MG Road, Bangalore", {})
        assert result["operation"] == "address_saved"
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_cancel_aborts_pending_invoice(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"pending_invoice": {"client_name": "Nike"}}
        flow = InvoiceAddress()
        result = flow.on_cancel(svc, "u1", "cancel", {"client_name": "Nike"})
        assert result["operation"] == "invoice_cancelled"
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_resume_nudge_mentions_client_when_present(self):
        assert "Nike" in InvoiceAddress().resume_nudge({"client_name": "Nike"})

    def test_resume_nudge_safe_with_no_client(self):
        assert InvoiceAddress().resume_nudge({})


class TestInvoiceNeedJobDescriptionFlow:
    def test_saves_description_and_resumes_invoice(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "pending_invoice": {"client_name": "Nike"},
            "pending_jobdesc_row_id": "row-1",
        }
        svc.supabase.execute_sql.return_value = {"ok": True}
        svc._resume_invoice_flow = MagicMock(return_value={"operation": "resumed", "response": "ok"})
        flow = InvoiceNeedJobDescription()
        result = flow.handle_response(svc, "u1", "2 master films, English VO", {"row_id": "row-1"})
        assert result["operation"] == "resumed"
        update_sql = svc.supabase.execute_sql.call_args.args[0]
        assert "master films" in update_sql and "row-1" in update_sql
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_cancel_aborts_invoice(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"pending_invoice": {"client_name": "Nike"}}
        flow = InvoiceNeedJobDescription()
        result = flow.on_cancel(svc, "u1", "cancel", {"row_id": "row-1"})
        assert result["operation"] == "invoice_cancelled"
        svc.supabase.execute_sql.assert_not_called()
        svc.flow_machine.reset.assert_called_once_with("u1")


class TestClassifierGuidance:
    @pytest.mark.parametrize("flow_name", ["INVOICE_ADDRESS", "INVOICE_NEED_JOB_DESCRIPTION"])
    def test_guidance_present(self, flow_name):
        from services.classifier import _flow_compat_block
        block = _flow_compat_block(flow_name, {})
        assert "SIDE_QUESTION" in block and "FLOW_RESPONSE" in block and "CANCEL" in block


class TestDispatchInFlowIntegration:
    @pytest.mark.parametrize("flow_name,flow_cls", [
        (FLOW_INVOICE_ADDRESS, InvoiceAddress),
        (FLOW_INVOICE_NEED_JOB_DESCRIPTION, InvoiceNeedJobDescription),
    ])
    def test_side_question_never_reaches_handle_response(self, flow_name, flow_cls):
        from services.flow_dispatcher import dispatch_in_flow
        svc = _make_svc()
        with patch.object(flow_cls, "handle_response") as mock_handle:
            verdict = {"intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
                       "raw_message": "does that include everything?",
                       "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False}
            result = dispatch_in_flow(
                verdict, intent_service=svc, user_id="u1",
                current_flow=flow_name, current_context={}, conversation_history=[],
            )
        mock_handle.assert_not_called()
        assert result is None
