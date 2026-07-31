"""
Phase 2.3 (TODO.md): "Port readers, then delete the mirror."

INVOICE_NEED_POC_EMAIL is the fifth flow fully migrated off its legacy
flag — FlowMachine is now its sole source of truth. The most arm-site-heavy
migration yet: FOUR arm sites (three inside _process_request_impl, all
sharing the pending_send_invoice payload shape via a new _arm_poc_email_v2
helper, plus main.py's own separate arm site inside
process_and_send_invoice).

Discovered mid-migration, NOT fixed here (flagged separately,
task_ab7501b3): main.py's arm site writes a DIFFERENT memory shape
(poc_email_client/poc_email_pdf_path/poc_email_month/poc_email_year)
than _handle_poc_email_response actually reads (pending_send_invoice) --
a pre-existing gap independent of this migration. Preserved exactly as-is;
only added the FlowMachine write alongside it.

Same check-after retry pattern as BankDetails/LinkAccount:
_handle_poc_email_response signals "stay in the flow" via its returned
operation ("poc_email_retry") instead of a legacy flag re-read.

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not
mocked), matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import FlowMachine, FLOW_IDLE, FLOW_INVOICE_NEED_POC_EMAIL
from services.flow_dispatcher import dispatch_in_flow
from services.flows import InvoiceNeedPocEmail, get_flow


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
    svc.email = MagicMock()
    return svc


class TestArmPocEmailV2Helper:
    def test_writes_flow_machine_state_with_client_name(self):
        svc = _svc()
        svc._arm_poc_email_v2("u1", "Nike", {"client_name": "Nike", "month": "March"})
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_POC_EMAIL
        assert svc.flow_machine.get_state("u1")["context"]["client_name"] == "Nike"

    def test_carries_pending_send_invoice_payload(self):
        svc = _svc()
        payload = {"client_name": "Nike", "month": "March", "year": 2026, "row_ids": ["1"]}
        svc._arm_poc_email_v2("u1", "Nike", payload)
        assert svc.memory.get_user_memory("u1")["pending_send_invoice"] == payload

    def test_defensively_clears_other_legacy_flags(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"awaiting_invoice_poc_email": True})
        svc._arm_poc_email_v2("u1", "Nike", {"client_name": "Nike"})
        assert svc.memory.get_user_memory("u1")["awaiting_invoice_poc_email"] is False

    def test_no_legacy_poc_email_flag_written(self):
        svc = _svc()
        svc._arm_poc_email_v2("u1", "Nike", {"client_name": "Nike"})
        assert "awaiting_poc_email" not in svc.memory.get_user_memory("u1")


class TestFlowResponseSuccess:
    def test_valid_email_saves_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_POC_EMAIL, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_send_invoice": {
                "client_name": "Nike", "month": "March", "year": 2026, "row_ids": ["1"],
            },
        })
        svc.supabase.update_poc_email_for_client.return_value = {"ok": True, "updated": 1}

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "client@nike.com", "parameters": {}, "confidence": 0.95,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_POC_EMAIL, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        assert svc.memory.get_user_memory("u1").get("pending_send_invoice") is None

    def test_cancel_word_cancels_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_POC_EMAIL, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_send_invoice": {"client_name": "Nike"},
        })

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "skip", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_POC_EMAIL, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result["operation"] == "poc_email_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestFlowResponseRetry:
    """The core regression this migration had to preserve: an invalid
    email must keep the flow active."""

    def test_invalid_email_stays_in_flow(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_POC_EMAIL, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_send_invoice": {"client_name": "Nike"},
        })

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "not an email", "parameters": {}, "confidence": 0.8,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_POC_EMAIL, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result["operation"] == "poc_email_retry"
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_POC_EMAIL

    def test_second_attempt_after_invalid_email_still_works(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_POC_EMAIL, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_send_invoice": {
                "client_name": "Nike", "month": "March", "year": 2026, "row_ids": ["1"],
            },
        })
        svc.supabase.update_poc_email_for_client.return_value = {"ok": True, "updated": 1}

        bad_verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "nonsense", "parameters": {}, "confidence": 0.7,
            "historical": False, "bulk": False,
        }
        dispatch_in_flow(
            bad_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_POC_EMAIL, current_context={"client_name": "Nike"},
            conversation_history=[],
        )
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_POC_EMAIL

        good_verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "client@nike.com", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            good_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_POC_EMAIL, current_context={"client_name": "Nike"},
            conversation_history=[],
        )
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestCancel:
    def test_cancel_verdict_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_POC_EMAIL, {"client_name": "Nike"})
        svc.memory.update_user_memory("u1", {
            "pending_send_invoice": {"client_name": "Nike"},
        })

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "CANCEL",
            "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_POC_EMAIL, current_context={"client_name": "Nike"},
            conversation_history=[],
        )

        assert result["operation"] == "poc_email_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestResumeNudge:
    def test_nudge_mentions_client(self):
        flow = get_flow(FLOW_INVOICE_NEED_POC_EMAIL)
        nudge = flow.resume_nudge({"client_name": "Nike"})
        assert "Nike" in nudge
