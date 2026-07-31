"""
Phase 2.3 (TODO.md): "Port readers, then delete the mirror."

INVOICE_NEED_BILLING and INVOICE_NEED_POC_NAME are the sixth and seventh
flows fully migrated off their legacy flags — FlowMachine is now their sole
source of truth. Both are invoice-readiness-gate checkpoints (like
BANK_DETAILS), so both had the same _prompt()-bypass requirement: the
shared _prompt() helper looks up its target flag via
`k in self._AWAITING_FLAGS` membership, which breaks once a flag is
removed from that tuple. New _arm_client_billing_v2 and _arm_poc_name_v2
helpers replace the _prompt() calls for these two checkpoints specifically
-- the remaining _prompt()-based checkpoints (job description, invoice
address) are untouched.

Both flows are the SIMPLEST kind in this whole migration series: single
arm site, no retry loop at all (any non-cancel reply is accepted verbatim
as the billing text / POC name -- there's no "that's not parseable"
branch), matching NAME_CHANGE's pattern rather than BANK_DETAILS'/
LINK_ACCOUNT'S/POC_EMAIL's check-after complexity.

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not
mocked), matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import (
    FlowMachine, FLOW_IDLE, FLOW_INVOICE_NEED_BILLING, FLOW_INVOICE_NEED_POC_NAME,
)
from services.flow_dispatcher import dispatch_in_flow
from services.flows import InvoiceNeedBilling, InvoiceNeedPocName, get_flow


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


class TestArmClientBillingV2Helper:
    def test_writes_flow_machine_state(self):
        svc = _svc()
        svc._arm_client_billing_v2("u1", "Nike", "u1", {"client_name": "Nike"})
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_BILLING
        assert svc.flow_machine.get_state("u1")["context"]["client_name"] == "Nike"

    def test_carries_billing_payload(self):
        svc = _svc()
        svc._arm_client_billing_v2("u1", "Nike", "data_u1", {"client_name": "Nike"})
        mem = svc.memory.get_user_memory("u1")
        assert mem["pending_billing_client"] == "Nike"
        assert mem["pending_billing_user_id"] == "data_u1"

    def test_defensively_clears_other_legacy_flags(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"awaiting_invoice_poc_email": True})
        svc._arm_client_billing_v2("u1", "Nike", "u1", {})
        assert svc.memory.get_user_memory("u1")["awaiting_invoice_poc_email"] is False

    def test_no_legacy_flag_written(self):
        svc = _svc()
        svc._arm_client_billing_v2("u1", "Nike", "u1", {})
        assert "awaiting_client_billing" not in svc.memory.get_user_memory("u1")


class TestArmPocNameV2Helper:
    def test_writes_flow_machine_state(self):
        svc = _svc()
        svc._arm_poc_name_v2("u1", "Nike", "u1", ["1", "2"], {"client_name": "Nike"})
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_POC_NAME
        assert svc.flow_machine.get_state("u1")["context"]["client_name"] == "Nike"

    def test_carries_poc_name_payload_including_row_ids(self):
        svc = _svc()
        svc._arm_poc_name_v2("u1", "Nike", "data_u1", ["1", "2"], {"client_name": "Nike"})
        mem = svc.memory.get_user_memory("u1")
        assert mem["pending_poc_client"] == "Nike"
        assert mem["pending_poc_user_id"] == "data_u1"
        assert mem["pending_poc_row_ids"] == ["1", "2"]

    def test_no_legacy_flag_written(self):
        svc = _svc()
        svc._arm_poc_name_v2("u1", "Nike", "u1", [], {})
        assert "awaiting_poc_name" not in svc.memory.get_user_memory("u1")


class TestInvoiceReadinessGateArmSites:
    """_invoice_readiness_check's billing and POC-name checkpoints, no
    longer going through the shared _prompt() helper."""

    def _rows_missing_billing(self):
        return [{
            "id": "1", "client_billing_details": None,
            "poc_name": "Raj", "poc_email": "raj@nike.com",
            "job_description_details": "Shoot",
        }]

    def _rows_missing_poc_name(self):
        return [{
            "id": "1", "client_billing_details": "Nike Inc, Mumbai",
            "poc_name": None, "poc_email": "raj@nike.com",
            "job_description_details": "Shoot",
        }]

    def test_billing_checkpoint_arms_flow_machine(self):
        svc = _svc()
        invoice_data = {"client_name": "Nike", "month": "March", "year": 2026}
        result = svc._invoice_readiness_check("u1", "u1", invoice_data, self._rows_missing_billing())
        assert result is not None
        assert result["operation"] == "ACTION_TRIGGER"
        assert "billing details" in result["response"].lower()
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_BILLING

    def test_poc_name_checkpoint_arms_flow_machine(self):
        svc = _svc()
        invoice_data = {"client_name": "Nike", "month": "March", "year": 2026}
        result = svc._invoice_readiness_check("u1", "u1", invoice_data, self._rows_missing_poc_name())
        assert result is not None
        assert result["operation"] == "ACTION_TRIGGER"
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_POC_NAME


class TestInvoiceNeedBillingLifecycle:
    def test_flow_response_saves_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_BILLING, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_billing_client": "Nike", "pending_billing_user_id": "u1",
        })
        svc.supabase.execute_sql.return_value = {"ok": True}

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "Nike Inc, Mumbai", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_BILLING, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_cancel_word_cancels_invoice_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_BILLING, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_billing_client": "Nike", "pending_invoice": {"client_name": "Nike"},
        })

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "cancel", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_BILLING, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result["operation"] == "invoice_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        assert svc.memory.get_user_memory("u1").get("pending_invoice") is None

    def test_cancel_verdict_resets_flow_machine(self):
        """NOTE: InvoiceNeedBilling.on_cancel calls
        _handle_client_billing_response(user_id, "skip") -- and that
        handler's own cancel-word set is {cancel, stop, abort, nevermind},
        which does NOT include "skip". This is a pre-existing behavior
        (unrelated to this migration, flagged separately as task_1b10c22c):
        the CANCEL path ends up saving the literal string "skip" as billing
        text rather than aborting the invoice. What THIS migration actually
        guarantees -- and what this test verifies -- is that FlowMachine
        still resets correctly regardless of what the delegated handler
        does with its own message."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_BILLING, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_billing_client": "Nike", "pending_invoice": {"client_name": "Nike"},
        })
        svc.supabase.execute_sql.return_value = {"ok": True}

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "CANCEL",
            "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_BILLING, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestInvoiceNeedPocNameLifecycle:
    def test_flow_response_saves_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_POC_NAME, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_poc_client": "Nike", "pending_poc_user_id": "u1", "pending_poc_row_ids": ["1"],
        })
        svc.supabase.execute_sql.return_value = {"ok": True}

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "Raj Kumar", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_POC_NAME, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_cancel_verdict_resets_flow_machine(self):
        """See TestInvoiceNeedBillingLifecycle's equivalent test docstring —
        same pre-existing "skip" gap in InvoiceNeedPocName.on_cancel,
        tracked as task_1b10c22c, not fixed here. This verifies FlowMachine
        resets correctly regardless."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_POC_NAME, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_poc_client": "Nike", "pending_invoice": {"client_name": "Nike"},
        })
        svc.supabase.execute_sql.return_value = {"ok": True}

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "CANCEL",
            "raw_message": "cancel", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_POC_NAME, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestResumeNudges:
    def test_billing_nudge_mentions_client(self):
        flow = get_flow(FLOW_INVOICE_NEED_BILLING)
        assert "Nike" in flow.resume_nudge({"client_name": "Nike"})

    def test_poc_name_nudge_mentions_client(self):
        flow = get_flow(FLOW_INVOICE_NEED_POC_NAME)
        assert "Nike" in flow.resume_nudge({"client_name": "Nike"})
