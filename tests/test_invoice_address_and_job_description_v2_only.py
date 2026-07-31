"""
Phase 2.3 (TODO.md): "Port readers, then delete the mirror."

INVOICE_ADDRESS and INVOICE_NEED_JOB_DESCRIPTION are the eighth and ninth
flows fully migrated off their legacy flags — FlowMachine is now their sole
source of truth. Both are invoice-readiness-gate checkpoints using the same
_prompt()-bypass pattern as billing/poc_name/bank_details.

INVOICE_ADDRESS is unusual among the gate checkpoints: it has TWO arm
sites, both now going through _arm_invoice_address_v2 -- the standalone
"update my address" command (_handle_address_update, pending_invoice=None)
and the gate's own checkpoint 5 (_invoice_readiness_check). This mirrors
how BANK_DETAILS had two arm sites for the analogous reason.

INVOICE_NEED_JOB_DESCRIPTION has a single arm site (checkpoint 3).

With this pair migrated, _prompt() (the shared invoice-readiness-gate
helper) has exactly ONE caller left: the still-legacy
awaiting_invoice_poc_email checkpoint. Every other checkpoint that used to
share it (billing, poc_name, poc_email, bank_details, address, job
description) now bypasses it via its own dedicated _arm_*_v2 helper.

Both flows have no retry loop -- any non-cancel reply is accepted verbatim
(the address text, or the job description text) -- matching NAME_CHANGE/
INVOICE_NEED_BILLING/INVOICE_NEED_POC_NAME's pattern, not BANK_DETAILS'/
LINK_ACCOUNT's/POC_EMAIL's check-after complexity. Both flows.py classes
correctly pass "cancel" (not "skip") to on_cancel, so neither repeats the
task_1b10c22c bug found in billing/poc_name.

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not
mocked), matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import (
    FlowMachine, FLOW_IDLE, FLOW_INVOICE_ADDRESS, FLOW_INVOICE_NEED_JOB_DESCRIPTION,
)
from services.flow_dispatcher import dispatch_in_flow
from services.flows import InvoiceAddress, InvoiceNeedJobDescription, get_flow


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


class TestArmInvoiceAddressV2Helper:
    def test_writes_flow_machine_state(self):
        svc = _svc()
        svc._arm_invoice_address_v2("u1", "Nike", "u1", {"client_name": "Nike"})
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_ADDRESS
        assert svc.flow_machine.get_state("u1")["context"]["client_name"] == "Nike"

    def test_none_client_name_for_standalone_update(self):
        svc = _svc()
        svc._arm_invoice_address_v2("u1", None, "u1", None)
        assert svc.flow_machine.get_state("u1")["context"]["client_name"] is None

    def test_carries_address_payload(self):
        svc = _svc()
        svc._arm_invoice_address_v2("u1", "Nike", "data_u1", {"client_name": "Nike"})
        mem = svc.memory.get_user_memory("u1")
        assert mem["pending_address_user_id"] == "data_u1"
        assert mem["pending_invoice"] == {"client_name": "Nike"}

    def test_defensively_clears_other_legacy_flags(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"awaiting_invoice_poc_email": True})
        svc._arm_invoice_address_v2("u1", "Nike", "u1", {})
        assert svc.memory.get_user_memory("u1")["awaiting_invoice_poc_email"] is False

    def test_no_legacy_flag_written(self):
        svc = _svc()
        svc._arm_invoice_address_v2("u1", "Nike", "u1", {})
        assert "awaiting_invoice_address" not in svc.memory.get_user_memory("u1")


class TestArmJobDescriptionV2Helper:
    def test_writes_flow_machine_state(self):
        svc = _svc()
        svc._arm_job_description_v2("u1", "row-1", "u1", {"client_name": "Nike"})
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_JOB_DESCRIPTION
        assert svc.flow_machine.get_state("u1")["context"]["row_id"] == "row-1"

    def test_carries_jobdesc_payload(self):
        svc = _svc()
        svc._arm_job_description_v2("u1", "row-1", "data_u1", {"client_name": "Nike"})
        mem = svc.memory.get_user_memory("u1")
        assert mem["pending_jobdesc_row_id"] == "row-1"
        assert mem["pending_jobdesc_user_id"] == "data_u1"

    def test_no_legacy_flag_written(self):
        svc = _svc()
        svc._arm_job_description_v2("u1", "row-1", "u1", {})
        assert "awaiting_job_description" not in svc.memory.get_user_memory("u1")


class TestStandaloneAddressUpdateArmSite:
    def test_bare_command_arms_flow_machine_with_no_client(self):
        svc = _svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"preferences": {}}}
        result = svc._handle_address_update("u1", "update my address", "u1")
        assert result["trigger_invoice"] is False
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_ADDRESS
        assert svc.flow_machine.get_state("u1")["context"]["client_name"] is None
        assert svc.memory.get_user_memory("u1")["pending_invoice"] is None

    def test_inline_address_bypasses_the_flow_entirely(self):
        svc = _svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"preferences": {}}}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._handle_address_update("u1", "update my address to 12 MG Road, Bangalore", "u1")
        assert result["operation"] == "address_updated"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestInvoiceReadinessGateArmSites:
    def _rows_missing_address_only(self):
        return [{
            "id": "1", "client_billing_details": "Nike Inc, Mumbai",
            "poc_name": "Raj", "poc_email": "raj@nike.com",
            "job_description_details": "Shoot",
        }]

    def _rows_missing_job_description(self):
        return [{
            "id": "1", "client_billing_details": "Nike Inc, Mumbai",
            "poc_name": "Raj", "poc_email": "raj@nike.com",
            "job_description_details": None, "job_date": "2026-01-15",
        }]

    def test_address_checkpoint_arms_flow_machine(self):
        svc = _svc()
        svc.supabase.get_user_bank_details.return_value = {
            "ok": True, "data": {"bank_account_number": "123456"},
        }
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"preferences": {}}}
        invoice_data = {"client_name": "Nike", "month": "March", "year": 2026}
        result = svc._invoice_readiness_check("u1", "u1", invoice_data, self._rows_missing_address_only())
        assert result is not None
        assert "business address" in result["response"].lower()
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_ADDRESS

    def test_job_description_checkpoint_arms_flow_machine(self):
        svc = _svc()
        invoice_data = {"client_name": "Nike", "month": "March", "year": 2026}
        result = svc._invoice_readiness_check("u1", "u1", invoice_data, self._rows_missing_job_description())
        assert result is not None
        assert "description" in result["response"].lower()
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_JOB_DESCRIPTION


class TestInvoiceAddressLifecycle:
    def test_flow_response_saves_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_ADDRESS, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {"pending_address_user_id": "u1"})
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"preferences": {}}}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "12 MG Road, Bangalore", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_ADDRESS, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_cancel_word_cancels_invoice_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_ADDRESS, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {"pending_invoice": {"client_name": "Nike"}})

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "cancel", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_ADDRESS, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result["operation"] == "invoice_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_cancel_verdict_cancels_correctly(self):
        """Unlike billing/poc_name (task_1b10c22c), InvoiceAddress.on_cancel
        passes the real word "cancel", which IS in
        _handle_invoice_address_response's cancel-word set -- so this
        actually cancels, no pre-existing bug here."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_ADDRESS, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {"pending_invoice": {"client_name": "Nike"}})

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "CANCEL",
            "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_ADDRESS, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result["operation"] == "invoice_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestInvoiceNeedJobDescriptionLifecycle:
    def test_flow_response_saves_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_JOB_DESCRIPTION, {"row_id": "row-1"})
        svc.memory.update_user_memory("u1", {
            "pending_jobdesc_row_id": "row-1", "pending_jobdesc_user_id": "u1",
        })
        svc.supabase.execute_sql.return_value = {"ok": True}

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "2 master films, English VO", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_JOB_DESCRIPTION, current_context={"row_id": "row-1"},
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_cancel_verdict_cancels_correctly(self):
        """Same as InvoiceAddress -- on_cancel passes the real word
        "cancel", correctly recognized, no task_1b10c22c-style bug here."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_JOB_DESCRIPTION, {"row_id": "row-1"})
        svc.memory.update_user_memory("u1", {"pending_invoice": {"client_name": "Nike"}})

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "CANCEL",
            "raw_message": "cancel", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_JOB_DESCRIPTION, current_context={"row_id": "row-1"},
            conversation_history=[],
        )

        assert result["operation"] == "invoice_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        svc.supabase.execute_sql.assert_not_called()


class TestResumeNudges:
    def test_address_nudge_mentions_client_when_present(self):
        flow = get_flow(FLOW_INVOICE_ADDRESS)
        assert "Nike" in flow.resume_nudge({"client_name": "Nike"})

    def test_address_nudge_safe_with_no_client(self):
        flow = get_flow(FLOW_INVOICE_ADDRESS)
        assert flow.resume_nudge({})

    def test_job_description_nudge_non_empty(self):
        flow = get_flow(FLOW_INVOICE_NEED_JOB_DESCRIPTION)
        assert flow.resume_nudge({})
