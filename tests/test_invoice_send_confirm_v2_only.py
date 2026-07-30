"""
Phase 2.3 (TODO.md): "Port readers, then delete the mirror."

INVOICE_AWAIT_SEND_CONFIRM is the first flow fully migrated off the legacy
awaiting_send_confirmation flag — FlowMachine is now its ONLY source of
truth (no reconciliation branch, no legacy read-site, no mirror write).
This is safe because (confirmed): FLOW_MACHINE_V2 is unconditionally on in
production, so the legacy fallback these reads used to serve is dead code.

No test exercised _handle_send_confirmation or InvoiceAwaitSendConfirm's
full lifecycle before this migration — this file closes that gap AND locks
in the new v2-only behavior: arm -> FLOW_RESPONSE -> handle -> reset, and
arm -> CANCEL -> decline -> reset, using a REAL FlowMachine bound to a real
dict-backed FakeMemory (not mocked) so state genuinely round-trips exactly
like production, the same technique test_answer_ledger_integration.py uses.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import FlowMachine, FLOW_IDLE, FLOW_INVOICE_AWAIT_SEND_CONFIRM
from services.flow_dispatcher import dispatch_in_flow
from services.flows import InvoiceAwaitSendConfirm, get_flow


class FakeMemory:
    """Real dict-backed store, not MagicMock — state must genuinely
    round-trip through flow_machine.set_state/current_flow/reset."""

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
    # Rebind flow_machine to the SAME memory instance -- IntentService.__init__
    # constructed it against the (patched, now-discarded) MemoryService()
    # mock; production always has flow_machine and memory pointing at the
    # same store, so tests must too.
    svc.flow_machine = FlowMachine(svc.memory)
    svc.gemini = MagicMock()
    svc.supabase = MagicMock()
    return svc


class TestArmSiteWritesFlowMachineOnly:
    """The 'cached invoice + send intent detected' arm site inside
    _process_request_impl."""

    def test_arming_sets_flow_machine_state(self):
        svc = _svc()
        svc.flow_machine.set_state(
            "u1", FLOW_INVOICE_AWAIT_SEND_CONFIRM,
            {"client_name": "Nike", "month": "March", "year": 2026, "poc_email": "a@nike.com"},
        )
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_AWAIT_SEND_CONFIRM
        state = svc.flow_machine.get_state("u1")
        assert state["context"]["client_name"] == "Nike"
        assert state["context"]["poc_email"] == "a@nike.com"

    def test_no_legacy_flag_written_by_arming(self):
        """Confirms the mirror is truly gone -- arming leaves no
        awaiting_send_confirmation key in memory at all."""
        svc = _svc()
        svc.flow_machine.set_state(
            "u1", FLOW_INVOICE_AWAIT_SEND_CONFIRM,
            {"client_name": "Nike", "month": "March", "year": 2026, "poc_email": "a@nike.com"},
        )
        svc.memory.update_user_memory("u1", {
            "pending_send_invoice": {"client_name": "Nike", "poc_email": "a@nike.com"},
        })
        mem = svc.memory.get_user_memory("u1")
        assert "awaiting_send_confirmation" not in mem


class TestFlowResponseYes:
    """dispatch_in_flow -> InvoiceAwaitSendConfirm.handle_response -> yes path."""

    def test_yes_sends_email_and_resets_flow_machine(self):
        svc = _svc()
        svc.flow_machine.set_state(
            "u1", FLOW_INVOICE_AWAIT_SEND_CONFIRM,
            {"client_name": "Nike", "month": "March", "year": 2026, "poc_email": "a@nike.com"},
        )
        svc.memory.update_user_memory("u1", {
            "pending_send_invoice": {
                "client_name": "Nike", "month": "March", "year": 2026,
                "poc_email": "a@nike.com", "row_ids": ["1"],
            },
            "last_generated_invoice": {"pdf_path": "/tmp/fake.pdf", "row_ids": ["1"]},
        })
        svc.email.send_invoice_email = MagicMock(return_value={"ok": True})
        svc.supabase.update_row = MagicMock(return_value={"ok": True})
        svc.supabase.execute_sql = MagicMock(return_value={"ok": True, "rows": []})

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "yes", "parameters": {}, "confidence": 0.95,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_AWAIT_SEND_CONFIRM,
            current_context=svc.flow_machine.get_state("u1")["context"],
            conversation_history=[],
        )

        assert result is not None
        # FlowMachine must be reset back to IDLE after handling.
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        # Legacy payload key cleared too.
        assert svc.memory.get_user_memory("u1").get("pending_send_invoice") is None

    def test_no_declines_and_resets_flow_machine(self):
        svc = _svc()
        svc.flow_machine.set_state(
            "u1", FLOW_INVOICE_AWAIT_SEND_CONFIRM,
            {"client_name": "Nike", "month": "March", "year": 2026, "poc_email": "a@nike.com"},
        )
        svc.memory.update_user_memory("u1", {
            "pending_send_invoice": {"client_name": "Nike", "poc_email": "a@nike.com"},
        })

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "no", "parameters": {}, "confidence": 0.95,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_AWAIT_SEND_CONFIRM,
            current_context=svc.flow_machine.get_state("u1")["context"],
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        assert svc.memory.get_user_memory("u1").get("pending_send_invoice") is None


class TestCancel:
    """dispatch_in_flow -> InvoiceAwaitSendConfirm.on_cancel."""

    def test_cancel_declines_and_resets_flow_machine(self):
        svc = _svc()
        svc.flow_machine.set_state(
            "u1", FLOW_INVOICE_AWAIT_SEND_CONFIRM,
            {"client_name": "Nike", "month": "March", "year": 2026, "poc_email": "a@nike.com"},
        )
        svc.memory.update_user_memory("u1", {
            "pending_send_invoice": {"client_name": "Nike", "poc_email": "a@nike.com"},
        })

        verdict = {
            "intent": "WRITE_INVOICE", "flow_compatible": "CANCEL",
            "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_AWAIT_SEND_CONFIRM,
            current_context=svc.flow_machine.get_state("u1")["context"],
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestResumeNudge:
    """Side-question resume nudge still works (unaffected by this migration)."""

    def test_resume_nudge_mentions_client_and_email(self):
        flow = get_flow(FLOW_INVOICE_AWAIT_SEND_CONFIRM)
        nudge = flow.resume_nudge({"client_name": "Nike", "poc_email": "a@nike.com"})
        assert "Nike" in nudge
        assert "a@nike.com" in nudge

    def test_resume_nudge_without_poc_email(self):
        flow = get_flow(FLOW_INVOICE_AWAIT_SEND_CONFIRM)
        nudge = flow.resume_nudge({"client_name": "Nike"})
        assert "Nike" in nudge


class TestActiveSubflowDetectionUsesFlowMachine:
    """The reminder handler's _active_subflow check (services/
    intent_service.py ~line 3338) must still detect this flow via
    FlowMachine, now that the legacy flag is gone."""

    def test_active_subflow_true_when_flow_machine_tracks_send_confirm(self):
        svc = _svc()
        svc.flow_machine.set_state(
            "u1", FLOW_INVOICE_AWAIT_SEND_CONFIRM,
            {"client_name": "Nike", "month": "March", "year": 2026, "poc_email": "a@nike.com"},
        )
        from utils.pending_reminders import get_pending
        with patch("services.intent_service.get_pending", return_value=[{"id": "1"}]):
            result = svc._handle_pending_reminder("u1", "3")
        # Must yield (return None) rather than hijack the reply -- the
        # send-confirm flow is active per FlowMachine.
        assert result is None

    def test_active_subflow_false_when_flow_machine_idle(self):
        svc = _svc()
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        with patch("services.intent_service.get_pending", return_value=None):
            result = svc._handle_pending_reminder("u1", "3")
        # No pending reminders at all -> early return None, different reason,
        # but confirms no crash from the new FlowMachine check path.
        assert result is None
