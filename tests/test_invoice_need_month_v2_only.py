"""
INVOICE_NEED_MONTH — the "which month?" prompt for an invoice request with
no month specified, migrated straight into FlowMachine v2 (no intermediate
legacy-mirror phase; this flow never existed before). One of the four
originally `❌ Legacy-only` single-prompt gates (FLOW_MACHINE_V2.md), fitting
the exact pattern WP-3 slices 2-3 already proved out.

THREE arm sites: the direct "send invoice for X" path with no month given,
the planner-clarification redirect (a query that leaked an invoice ask into
the query pipeline), and _handle_invoice_month_reply's own unrecognised-
month retry branch (operation "invoice_month_retry" — added as part of this
migration, replacing the old bare "ACTION_TRIGGER" so the Flow wrapper can
tell a retry apart from a fully-resolved month that re-entered
process_request and landed somewhere else entirely).

Unlike most gates, _handle_invoice_month_reply has NO cancel branch of its
own (any text is read as an attempted month) — CANCEL is handled entirely
in InvoiceNeedMonth.on_cancel instead.

With this migration, the "Universal intent-shift guard" (_PENDING_STATES +
surface-shape gate + is_new_query_not_response escape hatch) was deleted
outright — awaiting_invoice_month was its last member.

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not mocked),
matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import FlowMachine, FLOW_IDLE, FLOW_INVOICE_NEED_MONTH
from services.flow_dispatcher import dispatch_in_flow
from services.flows import InvoiceNeedMonth, get_flow


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


class TestArmSiteRetry:
    """_handle_invoice_month_reply's own retry branch -- an unrecognised
    reply re-arms the same flow."""

    def test_unrecognised_month_arms_flow_machine(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {
            "pending_invoice_client": "DriveOne", "pending_invoice_send_email": False,
        })
        result = svc._handle_invoice_month_reply(
            "u1", "not sure honestly", svc.memory.get_user_memory("u1"), "u1", [],
        )
        assert result["operation"] == "invoice_month_retry"
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_MONTH

    def test_no_legacy_flag_written(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"pending_invoice_client": "DriveOne"})
        svc._handle_invoice_month_reply(
            "u1", "huh", svc.memory.get_user_memory("u1"), "u1", [],
        )
        assert "awaiting_invoice_month" not in svc.memory.get_user_memory("u1")

    def test_context_survives_retry(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {
            "pending_invoice_client": "DriveOne", "pending_invoice_send_email": True,
        })
        svc._handle_invoice_month_reply(
            "u1", "unclear", svc.memory.get_user_memory("u1"), "u1", [],
        )
        mem = svc.memory.get_user_memory("u1")
        assert mem["pending_invoice_client"] == "DriveOne"
        assert mem["pending_invoice_send_email"] is True


class TestValidMonthResolves:
    def test_valid_month_reenters_process_request(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {
            "pending_invoice_client": "DriveOne", "pending_invoice_send_email": False,
        })
        svc.process_request = MagicMock(return_value={"operation": "resumed"})
        result = svc._handle_invoice_month_reply(
            "u1", "March", svc.memory.get_user_memory("u1"), "u1", [],
        )
        assert result["operation"] == "resumed"
        synthetic = svc.process_request.call_args.kwargs.get("message") or svc.process_request.call_args.args[-1]
        assert "March" in synthetic and "DriveOne" in synthetic

    def test_send_email_flag_produces_send_phrasing(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {
            "pending_invoice_client": "DriveOne", "pending_invoice_send_email": True,
        })
        svc.process_request = MagicMock(return_value={"operation": "resumed"})
        svc._handle_invoice_month_reply(
            "u1", "March", svc.memory.get_user_memory("u1"), "u1", [],
        )
        synthetic = svc.process_request.call_args.kwargs.get("message") or svc.process_request.call_args.args[-1]
        assert synthetic.lower().startswith("send invoice")


class TestFlowClassHandleResponse:
    def test_unrecognised_reply_stays_in_flow(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_MONTH, {"client_name": "DriveOne"})
        svc.memory.update_user_memory("u1", {
            "pending_invoice_client": "DriveOne", "pending_invoice_send_email": False,
        })
        flow = InvoiceNeedMonth()
        result = flow.handle_response(svc, "u1", "huh what", {"client_name": "DriveOne"})
        assert result["operation"] == "invoice_month_retry"
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_MONTH

    def test_valid_month_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_MONTH, {"client_name": "DriveOne"})
        svc.memory.update_user_memory("u1", {
            "pending_invoice_client": "DriveOne", "pending_invoice_send_email": False,
        })
        svc.process_request = MagicMock(return_value={"operation": "resumed"})
        flow = InvoiceNeedMonth()
        result = flow.handle_response(svc, "u1", "March", {"client_name": "DriveOne"})
        assert result["operation"] == "resumed"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_second_attempt_after_retry_still_works(self):
        """End-to-end: unrecognised reply, then a valid month, across two
        separate handle_response calls -- proves the retry loop genuinely
        stays active."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_MONTH, {"client_name": "DriveOne"})
        svc.memory.update_user_memory("u1", {
            "pending_invoice_client": "DriveOne", "pending_invoice_send_email": False,
        })
        flow = InvoiceNeedMonth()
        flow.handle_response(svc, "u1", "dunno", {"client_name": "DriveOne"})
        assert svc.flow_machine.current_flow("u1") == FLOW_INVOICE_NEED_MONTH

        svc.process_request = MagicMock(return_value={"operation": "resumed"})
        result = flow.handle_response(svc, "u1", "March", {"client_name": "DriveOne"})
        assert result["operation"] == "resumed"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_on_cancel_clears_payload_without_calling_handler(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_MONTH, {"client_name": "DriveOne"})
        svc.memory.update_user_memory("u1", {
            "pending_invoice_client": "DriveOne", "pending_invoice_send_email": False,
        })
        flow = InvoiceNeedMonth()
        result = flow.on_cancel(svc, "u1", "cancel", {"client_name": "DriveOne"})
        assert result["operation"] == "invoice_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        assert svc.memory.get_user_memory("u1")["pending_invoice_client"] is None


class TestDispatchInFlowIntegration:
    def test_cancel_verdict_reaches_on_cancel(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_MONTH, {"client_name": "DriveOne"})
        verdict = {"intent": "UNKNOWN", "flow_compatible": "CANCEL",
                   "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
                   "historical": False, "bulk": False}
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_INVOICE_NEED_MONTH, current_context={"client_name": "DriveOne"},
            conversation_history=[],
        )
        assert result["operation"] == "invoice_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_side_question_never_reaches_handle_response(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_INVOICE_NEED_MONTH, {"client_name": "DriveOne"})
        with patch.object(InvoiceNeedMonth, "handle_response") as mock_handle:
            verdict = {"intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
                       "raw_message": "does that include everything?", "parameters": {},
                       "confidence": 0.9, "historical": False, "bulk": False}
            result = dispatch_in_flow(
                verdict, intent_service=svc, user_id="u1",
                current_flow=FLOW_INVOICE_NEED_MONTH, current_context={"client_name": "DriveOne"},
                conversation_history=[],
            )
        mock_handle.assert_not_called()
        assert result is None


class TestFlowMachineRegistration:
    def test_registered(self):
        from services.flow_machine import KNOWN_FLOWS
        assert FLOW_INVOICE_NEED_MONTH in KNOWN_FLOWS
        assert isinstance(get_flow(FLOW_INVOICE_NEED_MONTH), InvoiceNeedMonth)


class TestResumeNudge:
    def test_mentions_client_when_present(self):
        flow = get_flow(FLOW_INVOICE_NEED_MONTH)
        assert "DriveOne" in flow.resume_nudge({"client_name": "DriveOne"})

    def test_safe_with_no_client(self):
        flow = get_flow(FLOW_INVOICE_NEED_MONTH)
        assert flow.resume_nudge({})


class TestClassifierGuidance:
    def test_guidance_present(self):
        from services.classifier import _flow_compat_block
        block = _flow_compat_block("INVOICE_NEED_MONTH", {})
        assert "SIDE_QUESTION" in block and "FLOW_RESPONSE" in block and "CANCEL" in block


class TestReconciliation:
    def test_no_reconciliation_branch_for_this_flow(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"pending_invoice_client": "DriveOne"})
        svc._reconcile_legacy_to_flow_machine("u1", svc.memory.get_user_memory("u1"))
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_no_legacy_flag_anywhere(self):
        from services.intent_service import _ALL_AWAITING_CLEAR_PATCH
        svc = _svc()
        assert "awaiting_invoice_month" not in svc._AWAITING_FLAGS
        assert "awaiting_invoice_month" not in _ALL_AWAITING_CLEAR_PATCH
