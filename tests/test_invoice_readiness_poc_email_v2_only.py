"""
INVOICE_READINESS_POC_EMAIL — the invoice-readiness gate's checkpoint 2b,
migrated straight into FlowMachine v2 (no intermediate legacy-mirror phase;
this flow never existed before). It was the LAST caller of the shared
_prompt() helper (awaiting_invoice_poc_email) -- with it migrated, _prompt()
itself has zero callers left and was deleted from _invoice_readiness_check.

Distinct from FLOW_INVOICE_NEED_POC_EMAIL (the SEND-time flow, asks for the
address to deliver an already-generated PDF) -- this one runs BEFORE
generation, asking for an email on the job rows themselves so invoice
delivery and payment reminders can both reach the client. Same retry-loop
shape as BANK_DETAILS: an invalid email re-arms and re-asks; a valid one
saves and resumes the invoice flow.

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not mocked),
matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import FlowMachine, FLOW_IDLE, FLOW_INVOICE_READINESS_POC_EMAIL
from services.flow_dispatcher import dispatch_in_flow
from services.flows import InvoiceReadinessPocEmail, get_flow


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


class TestArmSiteReadinessGate:
    """The gate's own checkpoint (_invoice_readiness_check) -- the ORIGINAL
    _prompt() caller, now bypassing it entirely."""

    def test_missing_poc_email_arms_flow_machine(self):
        svc = _svc()
        rows = [{"id": "r1", "client_name": "Nike", "poc_email": None,
                  "client_billing_details": "x", "poc_name": "Rahul",
                  "job_description_details": "shoot"}]
        result = svc._invoice_readiness_check(
            "u1", "u1", {"client_name": "Nike", "month": "March", "year": 2026}, rows=rows,
        )
        assert result["operation"] == "ACTION_TRIGGER"
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_READINESS_POC_EMAIL

    def test_no_legacy_flag_written(self):
        svc = _svc()
        rows = [{"id": "r1", "client_name": "Nike", "poc_email": None,
                  "client_billing_details": "x", "poc_name": "Rahul",
                  "job_description_details": "shoot"}]
        svc._invoice_readiness_check(
            "u1", "u1", {"client_name": "Nike", "month": "March", "year": 2026}, rows=rows,
        )
        assert "awaiting_invoice_poc_email" not in svc.memory.get_user_memory("u1")

    def test_carries_row_ids_and_pending_invoice(self):
        svc = _svc()
        invoice_data = {"client_name": "Nike", "month": "March", "year": 2026}
        rows = [{"id": "r1", "client_name": "Nike", "poc_email": None,
                  "client_billing_details": "x", "poc_name": "Rahul",
                  "job_description_details": "shoot"}]
        svc._invoice_readiness_check("u1", "data_u1", invoice_data, rows=rows)
        mem = svc.memory.get_user_memory("u1")
        assert mem["pending_poc_email_row_ids"] == ["r1"]
        assert mem["pending_poc_email_user_id"] == "data_u1"
        assert mem["pending_invoice"] == invoice_data


class TestHandlerDirectly:
    def _mem_with_pending(self, svc, **extra):
        base = {
            "pending_invoice": {"client_name": "Spotify"},
            "pending_poc_email_client": "Spotify",
            "pending_poc_email_user_id": "u1",
            "pending_poc_email_row_ids": ["r1"],
        }
        base.update(extra)
        svc.memory.update_user_memory("u1", base)

    def test_valid_email_saves_and_resumes(self):
        svc = _svc()
        self._mem_with_pending(svc)
        svc.supabase.execute_sql.return_value = {"ok": True}
        svc._resume_invoice_flow = MagicMock(return_value={"operation": "resumed"})
        result = svc._handle_invoice_poc_email_response("u1", "karan@gmail.com")
        assert result["operation"] == "resumed"
        upd = svc.supabase.execute_sql.call_args.args[0]
        assert "karan@gmail.com" in upd and "r1" in upd

    def test_invalid_email_rearms_flow_machine_directly(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_READINESS_POC_EMAIL, {"client_name": "Spotify"})
        self._mem_with_pending(svc)
        result = svc._handle_invoice_poc_email_response("u1", "notanemail")
        assert result["operation"] == "invoice_poc_email_retry"
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_READINESS_POC_EMAIL
        assert svc.memory.get_user_memory("u1")["pending_poc_email_row_ids"] == ["r1"]

    def test_cancel_aborts_without_writing(self):
        svc = _svc()
        self._mem_with_pending(svc)
        result = svc._handle_invoice_poc_email_response("u1", "cancel")
        assert result["operation"] == "invoice_cancelled"
        svc.supabase.execute_sql.assert_not_called()
        assert svc.memory.get_user_memory("u1")["pending_invoice"] is None


class TestDispatchInFlowIntegration:
    def test_valid_email_reaches_handle_response_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_READINESS_POC_EMAIL, {"client_name": "Spotify"})
        svc.memory.update_user_memory("u1", {
            "pending_invoice": {"client_name": "Spotify"},
            "pending_poc_email_client": "Spotify",
            "pending_poc_email_user_id": "u1",
            "pending_poc_email_row_ids": ["r1"],
        })
        svc.supabase.execute_sql.return_value = {"ok": True}
        svc._resume_invoice_flow = MagicMock(return_value={"operation": "resumed"})

        verdict = {"intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
                   "raw_message": "karan@gmail.com", "parameters": {}, "confidence": 0.9,
                   "historical": False, "bulk": False}
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_READINESS_POC_EMAIL, current_context={"client_name": "Spotify"},
            conversation_history=[],
        )
        assert result["operation"] == "resumed"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_invalid_email_stays_in_flow_across_two_calls(self):
        """End-to-end: invalid email, then a valid one, across two separate
        dispatch_in_flow calls -- proves the retry loop genuinely stays
        active (same shape as BANK_DETAILS'/LINK_ACCOUNT's precedent)."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_READINESS_POC_EMAIL, {"client_name": "Spotify"})
        svc.memory.update_user_memory("u1", {
            "pending_invoice": {"client_name": "Spotify"},
            "pending_poc_email_client": "Spotify",
            "pending_poc_email_user_id": "u1",
            "pending_poc_email_row_ids": ["r1"],
        })

        bad_verdict = {"intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
                       "raw_message": "not an email", "parameters": {}, "confidence": 0.7,
                       "historical": False, "bulk": False}
        result = dispatch_in_flow(
            bad_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_READINESS_POC_EMAIL, current_context={"client_name": "Spotify"},
            conversation_history=[],
        )
        assert result["operation"] == "invoice_poc_email_retry"
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_READINESS_POC_EMAIL

        svc.supabase.execute_sql.return_value = {"ok": True}
        svc._resume_invoice_flow = MagicMock(return_value={"operation": "resumed"})
        good_verdict = {"intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
                        "raw_message": "karan@gmail.com", "parameters": {}, "confidence": 0.9,
                        "historical": False, "bulk": False}
        result = dispatch_in_flow(
            good_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_READINESS_POC_EMAIL, current_context={"client_name": "Spotify"},
            conversation_history=[],
        )
        assert result["operation"] == "resumed"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_cancel_verdict_reaches_on_cancel(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_READINESS_POC_EMAIL, {"client_name": "Spotify"})
        svc.memory.update_user_memory("u1", {"pending_invoice": {"client_name": "Spotify"}})

        verdict = {"intent": "UNKNOWN", "flow_compatible": "CANCEL",
                   "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
                   "historical": False, "bulk": False}
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_READINESS_POC_EMAIL, current_context={"client_name": "Spotify"},
            conversation_history=[],
        )
        assert result["operation"] == "invoice_cancelled"
        svc.supabase.execute_sql.assert_not_called()
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_side_question_never_reaches_handle_response(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_READINESS_POC_EMAIL, {"client_name": "Spotify"})
        with patch.object(InvoiceReadinessPocEmail, "handle_response") as mock_handle:
            verdict = {"intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
                       "raw_message": "does that include everything?", "parameters": {},
                       "confidence": 0.9, "historical": False, "bulk": False}
            result = dispatch_in_flow(
                verdict, intent_service=svc, user_id="u1",
                current_flow=FLOW_INVOICE_READINESS_POC_EMAIL, current_context={"client_name": "Spotify"},
                conversation_history=[],
            )
        mock_handle.assert_not_called()
        assert result is None


class TestFlowMachineRegistration:
    def test_registered(self):
        from services.flow_machine import KNOWN_FLOWS
        assert FLOW_INVOICE_READINESS_POC_EMAIL in KNOWN_FLOWS
        assert isinstance(get_flow(FLOW_INVOICE_READINESS_POC_EMAIL), InvoiceReadinessPocEmail)

    def test_distinct_from_send_time_poc_email_flow(self):
        from services.flow_machine import FLOW_INVOICE_NEED_POC_EMAIL
        assert FLOW_INVOICE_READINESS_POC_EMAIL != FLOW_INVOICE_NEED_POC_EMAIL


class TestResumeNudge:
    def test_mentions_client_when_present(self):
        flow = get_flow(FLOW_INVOICE_READINESS_POC_EMAIL)
        assert "Spotify" in flow.resume_nudge({"client_name": "Spotify"})

    def test_safe_with_no_client(self):
        flow = get_flow(FLOW_INVOICE_READINESS_POC_EMAIL)
        assert flow.resume_nudge({})


class TestClassifierGuidance:
    def test_guidance_present(self):
        from services.classifier import _flow_compat_block
        block = _flow_compat_block("INVOICE_READINESS_POC_EMAIL", {})
        assert "SIDE_QUESTION" in block and "FLOW_RESPONSE" in block and "CANCEL" in block


class TestReconciliation:
    def test_no_reconciliation_branch_for_this_flow(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"pending_poc_email_row_ids": ["r1"]})
        svc._reconcile_legacy_to_flow_machine("u1", svc.memory.get_user_memory("u1"))
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_no_legacy_flag_anywhere(self):
        from services.intent_service import _ALL_AWAITING_CLEAR_PATCH
        svc = _svc()
        assert "awaiting_invoice_poc_email" not in svc._AWAITING_FLAGS
        assert "awaiting_invoice_poc_email" not in _ALL_AWAITING_CLEAR_PATCH
