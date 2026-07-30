"""
WP-3 slice 2 of ASSISTANT_PLAN.md — migrating bank-details, name-change, and
link-account into FlowMachine v2.

All three are simple, self-contained "prompted once, single reply completes
it" flows — unlike disambiguation (slice 1), none carry dual-shape pending
state or a bug to fix. They round out the set of flows armed synchronously
within process_request, matching the existing reconciliation pattern.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch

from services.flow_machine import (
    FLOW_BANK_DETAILS, FLOW_NAME_CHANGE, FLOW_LINK_ACCOUNT, FLOW_IDLE, KNOWN_FLOWS,
)
from services.flows import BankDetails, NameChange, LinkAccount, get_flow


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
        (FLOW_BANK_DETAILS, BankDetails),
        (FLOW_NAME_CHANGE, NameChange),
        (FLOW_LINK_ACCOUNT, LinkAccount),
    ])
    def test_registered(self, flow_name, cls):
        assert flow_name in KNOWN_FLOWS
        assert isinstance(get_flow(flow_name), cls)


class TestReconciliation:
    # awaiting_bank_details removed from this parametrization (Phase 2.3):
    # FlowMachine is BANK_DETAILS' sole source of truth now, with no legacy
    # flag left to reconcile FROM — see TestBankDetailsIsFlowMachineOnly
    # below instead.
    @pytest.mark.parametrize("legacy_flag,flow_name", [
        ("awaiting_name_change", FLOW_NAME_CHANGE),
        ("awaiting_link_id", FLOW_LINK_ACCOUNT),
    ])
    def test_reconciles_each_flag(self, legacy_flag, flow_name):
        svc = _make_svc()
        svc.flow_machine.current_flow.return_value = FLOW_IDLE
        svc.memory.get_form_state.return_value = None
        svc._reconcile_legacy_to_flow_machine("u1", {legacy_flag: True})
        args = svc.flow_machine.set_state.call_args.args
        assert args[1] == flow_name

    def test_no_flags_no_op(self):
        svc = _make_svc()
        svc.flow_machine.current_flow.return_value = FLOW_IDLE
        svc.memory.get_form_state.return_value = None
        svc._reconcile_legacy_to_flow_machine("u1", {})
        svc.flow_machine.set_state.assert_not_called()

    def test_disambiguation_checked_before_these_three(self):
        """If both happen to be set (shouldn't normally happen), the
        disambiguation branch runs first and returns before reaching these."""
        svc = _make_svc()
        svc.flow_machine.current_flow.return_value = FLOW_IDLE
        svc.memory.get_form_state.return_value = None
        svc._reconcile_legacy_to_flow_machine("u1", {
            "pending_disambiguation": {"rows": [], "type": "delete"},
            "awaiting_name_change": True,
        })
        from services.flow_machine import FLOW_DISAMBIGUATION
        args = svc.flow_machine.set_state.call_args.args
        assert args[1] == FLOW_DISAMBIGUATION


class TestTTLStalenessClearsAllThree:
    # awaiting_bank_details removed from this parametrization (Phase 2.3) —
    # it's no longer in _ALL_AWAITING_CLEAR_PATCH since there's no legacy
    # flag left to clear (FlowMachine.reset handles BANK_DETAILS directly).
    @pytest.mark.parametrize("flag", ["awaiting_name_change", "awaiting_link_id"])
    def test_stale_clear_includes_flag(self, flag):
        # _stale_clear was factored out into the shared _ALL_AWAITING_CLEAR_PATCH
        # constant (also used by dispatch_in_flow's NEW_FLOW branch) — both the
        # TTL site and NEW_FLOW now clear via IntentService._clear_flow_state,
        # which applies this same patch.
        from services.intent_service import _ALL_AWAITING_CLEAR_PATCH
        assert flag in _ALL_AWAITING_CLEAR_PATCH


class TestBankDetailsFlow:
    def test_successful_save_resets_flow(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        svc.supabase.upsert_user_config.return_value = {"ok": True}
        flow = BankDetails()
        result = flow.handle_response(
            svc, "u1",
            "Account Name: D\nBank Name: HDFC\nAccount Number: 123\nIFSC: HDFC0001234",
            {},
        )
        assert result["operation"] == "bank_config_complete"
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_unparseable_message_stays_in_flow(self):
        # Phase 2.3: no legacy awaiting_bank_details flag exists anymore —
        # BankDetails.handle_response decides whether to stay in the flow
        # purely from the returned operation name ("bank_details_retry"),
        # not a memory re-read. get_user_memory's value here is irrelevant
        # to that decision; kept empty to make that explicit.
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        flow = BankDetails()
        result = flow.handle_response(svc, "u1", "uhh not sure what you need", {})
        assert result["operation"] == "bank_details_retry"
        svc.flow_machine.reset.assert_not_called()

    def test_cancel_saves_nothing(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        flow = BankDetails()
        result = flow.on_cancel(svc, "u1", "cancel", {})
        assert result["operation"] == "bank_details_cancelled"
        svc.supabase.upsert_user_config.assert_not_called()
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_resume_nudge_non_empty(self):
        assert BankDetails().resume_nudge({})


class TestNameChangeFlow:
    def test_applies_new_name_and_resets(self):
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        flow = NameChange()
        result = flow.handle_response(svc, "u1", "Akshaj Kasliwal", {})
        assert result["operation"] == "name_change_complete" or "name" in result["response"].lower()
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_cancel_keeps_name_unchanged(self):
        svc = _make_svc()
        flow = NameChange()
        result = flow.on_cancel(svc, "u1", "nevermind", {})
        assert result["operation"] == "name_change_cancelled"
        svc.supabase.upsert_user_profile.assert_not_called()
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_always_resets_no_retry_loop(self):
        """Unlike bank-details, name-change has no re-prompt path -- any
        non-cancel reply becomes the new name, so the flow always ends."""
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        flow = NameChange()
        flow.handle_response(svc, "u1", "X", {})
        svc.flow_machine.reset.assert_called_once()


class TestLinkAccountFlow:
    def test_valid_id_links_and_resets(self):
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        flow = LinkAccount()
        result = flow.handle_response(svc, "u1", "751256859", {})
        assert result["operation"] == "account_linked"
        svc.flow_machine.reset.assert_called_once_with("u1")

    def test_cancel_does_not_link(self):
        svc = _make_svc()
        flow = LinkAccount()
        result = flow.on_cancel(svc, "u1", "cancel", {})
        assert result["operation"] == "link_cancelled"
        svc.supabase.upsert_user_profile.assert_not_called()
        svc.flow_machine.reset.assert_called_once_with("u1")


class TestClassifierGuidance:
    @pytest.mark.parametrize("flow_name", ["BANK_DETAILS", "NAME_CHANGE", "LINK_ACCOUNT"])
    def test_guidance_present_and_distinguishes_side_questions(self, flow_name):
        from services.classifier import _flow_compat_block
        block = _flow_compat_block(flow_name, {})
        assert "SIDE_QUESTION" in block
        assert "FLOW_RESPONSE" in block
        assert "CANCEL" in block


class TestDispatchInFlowIntegration:
    """A scope-clarifying question during any of these three flows must be
    classified SIDE_QUESTION and never reach handle_response."""

    @pytest.mark.parametrize("flow_name,flow_cls", [
        (FLOW_BANK_DETAILS, BankDetails),
        (FLOW_NAME_CHANGE, NameChange),
        (FLOW_LINK_ACCOUNT, LinkAccount),
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
        assert result is None  # SHADOW_ONLY

    @pytest.mark.parametrize("flow_name", [FLOW_BANK_DETAILS, FLOW_NAME_CHANGE, FLOW_LINK_ACCOUNT])
    def test_flow_response_reaches_handle_response(self, flow_name):
        from services.flow_dispatcher import dispatch_in_flow
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        svc.supabase.upsert_user_config.return_value = {"ok": True}
        verdict = {"intent": "WRITE_UPDATE", "flow_compatible": "FLOW_RESPONSE",
                   "raw_message": "some reply", "parameters": {}, "confidence": 0.9,
                   "historical": False, "bulk": False}
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=flow_name, current_context={}, conversation_history=[],
        )
        assert result is not None
