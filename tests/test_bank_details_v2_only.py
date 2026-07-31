"""
Phase 2.3 (TODO.md): "Port readers, then delete the mirror."

BANK_DETAILS is the second flow fully migrated off its legacy flag —
FlowMachine is now its ONLY source of truth. This flow was more involved
than INVOICE_AWAIT_SEND_CONFIRM (the first migration) because:

1. Its own retry loop used a "check-after" pattern: the handler re-armed
   the legacy flag on an unparseable message, and the Flow wrapper
   re-checked memory afterward to decide whether to reset FlowMachine or
   stay in the flow. Fixed by switching the signal to the handler's
   returned `operation` field ("bank_details_retry") instead — see
   flows.py's BankDetails.handle_response.
2. It has THREE arm sites, not one: the invoice-readiness gate's own
   checkpoint (_invoice_readiness_check, "4. Bank account number"), the
   standalone "update my bank details" command (_prompt_bank_details_
   format), and — discovered mid-migration — a SHARED `_prompt()` helper
   used by five OTHER still-legacy checkpoints that looks up the target
   flag via `k in self._AWAITING_FLAGS` membership, which would have
   crashed (StopIteration) for the bank-details checkpoint once removed
   from that tuple. Both real arm sites now go through a new shared
   `_arm_bank_details_v2()` helper instead, bypassing `_prompt()` entirely
   for this one flow while leaving `_prompt()` untouched for the other five.

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not
mocked), matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import FlowMachine, FLOW_IDLE, FLOW_BANK_DETAILS
from services.flow_dispatcher import dispatch_in_flow
from services.flows import BankDetails, get_flow


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
    svc.flow_machine = FlowMachine(svc.memory)
    svc.gemini = MagicMock()
    svc.supabase = MagicMock()
    return svc


class TestArmBankDetailsV2Helper:
    """The shared _arm_bank_details_v2 helper both real arm sites use."""

    def test_writes_flow_machine_state(self):
        svc = _svc()
        svc._arm_bank_details_v2("u1", pending_invoice=None)
        assert svc.flow_machine.current_flow("u1") == FLOW_BANK_DETAILS

    def test_carries_pending_invoice_payload(self):
        svc = _svc()
        invoice_data = {"client_name": "Nike", "bill_number": "INV-1"}
        svc._arm_bank_details_v2("u1", pending_invoice=invoice_data)
        assert svc.memory.get_user_memory("u1")["pending_invoice"] == invoice_data

    def test_clears_pending_invoice_for_standalone_command(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {
            "pending_invoice": {"client_name": "StaleClient"},
        })
        svc._arm_bank_details_v2("u1", pending_invoice=None)
        assert svc.memory.get_user_memory("u1")["pending_invoice"] is None

    def test_defensively_clears_other_legacy_flags(self):
        """_AWAITING_FLAGS is empty now -- awaiting_modify_field (the last
        boolean legacy flag) was migrated to its own FlowMachine flow
        (MODIFY_FIELD). This test now just confirms arming doesn't crash
        or write anything unexpected when _AWAITING_FLAGS has nothing to
        iterate."""
        svc = _svc()
        svc._arm_bank_details_v2("u1", pending_invoice=None)
        assert svc.flow_machine.current_flow("u1") == FLOW_BANK_DETAILS

    def test_no_legacy_bank_details_flag_written(self):
        svc = _svc()
        svc._arm_bank_details_v2("u1", pending_invoice=None)
        assert "awaiting_bank_details" not in svc.memory.get_user_memory("u1")


class TestInvoiceReadinessGateArmSite:
    """_invoice_readiness_check's bank-details checkpoint no longer goes
    through the shared _prompt() helper (which depends on _AWAITING_FLAGS
    membership)."""

    def test_prompts_and_arms_flow_machine(self):
        svc = _svc()
        svc.supabase.get_user_bank_details.return_value = {"ok": True, "data": {}}
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        invoice_data = {"client_name": "Nike", "month": "March", "year": 2026}
        rows = [{
            "id": "1", "client_billing_details": "Nike Inc, Mumbai",
            "poc_name": "Raj", "poc_email": "raj@nike.com",
            "job_description_details": "Shoot",
        }]
        result = svc._invoice_readiness_check("u1", "u1", invoice_data, rows)
        assert result is not None
        assert result["operation"] == "ACTION_TRIGGER"
        assert "bank details" in result["response"].lower()
        assert svc.flow_machine.current_flow("u1") == FLOW_BANK_DETAILS
        assert svc.memory.get_user_memory("u1")["pending_invoice"] == invoice_data


class TestFlowResponseSuccess:
    """dispatch_in_flow -> BankDetails.handle_response -> successful save."""

    def test_valid_details_saves_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_BANK_DETAILS, {})
        svc.supabase.upsert_user_config = MagicMock(return_value={"ok": True})

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "Account Name: D\nBank Name: HDFC\nAccount Number: 123\nIFSC: HDFC0001234",
            "parameters": {}, "confidence": 0.95, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_BANK_DETAILS, current_context={},
            conversation_history=[],
        )

        assert result is not None
        assert result["operation"] == "bank_config_complete"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestFlowResponseRetry:
    """dispatch_in_flow -> BankDetails.handle_response -> unparseable message."""

    def test_unparseable_message_stays_in_flow(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_BANK_DETAILS, {})

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "uhh not sure what you need",
            "parameters": {}, "confidence": 0.8, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_BANK_DETAILS, current_context={},
            conversation_history=[],
        )

        assert result is not None
        assert result["operation"] == "bank_details_retry"
        # Flow must stay active for the retry -- NOT reset to IDLE.
        assert svc.flow_machine.current_flow("u1") == FLOW_BANK_DETAILS

    def test_cancel_word_in_message_cancels_not_retries(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_BANK_DETAILS, {})

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "cancel",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_BANK_DETAILS, current_context={},
            conversation_history=[],
        )

        assert result is not None
        assert result["operation"] == "bank_details_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestCancel:
    """dispatch_in_flow -> BankDetails.on_cancel."""

    def test_cancel_verdict_declines_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_BANK_DETAILS, {})

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "CANCEL",
            "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_BANK_DETAILS, current_context={},
            conversation_history=[],
        )

        assert result is not None
        assert result["operation"] == "bank_details_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        svc.supabase.upsert_user_config.assert_not_called()


class TestResumeNudge:
    def test_resume_nudge_mentions_bank_details(self):
        flow = get_flow(FLOW_BANK_DETAILS)
        nudge = flow.resume_nudge({})
        assert "bank details" in nudge.lower()
