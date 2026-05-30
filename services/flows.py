"""
Session 2 of the FlowMachine v2 migration.

Concrete flow classes. Each owns the behaviour for one v2 flow:
  - handle_response(): the user is answering the bot's pending question.
  - resume_nudge():    after a SIDE_QUESTION, what to append to the answer
                       reminding the user where we left off.
  - on_cancel():       the user wants out of the flow.

Session 2 ships exactly ONE flow class — InvoiceAwaitSendConfirm — proving
the pattern. Sessions 2.x / 3 add the rest from FLOW_MACHINE_V2.md.

All flow classes are thin shells over existing intent_service methods. The
goal is reuse, not rewrite. Future sessions can pull more logic in here.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from services.flow_machine import FLOW_INVOICE_AWAIT_SEND_CONFIRM
from utils.logger import logger


class Flow:
    """Base shape for every v2 flow. Subclasses override what they need."""

    name: str = ""  # one of services.flow_machine FLOW_* constants

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        """User is answering the bot's pending question. Return a
        process_request-shaped dict ({operation, response, trigger_invoice,
        invoice_data})."""
        raise NotImplementedError

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        """A short line appended after a SIDE_QUESTION answer, reminding the
        user what flow they're still in. Empty string = no nudge."""
        return ""

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        """User asked to bail. Default = brief confirmation; subclasses can
        override to do cleanup (clear pending state, etc.)."""
        intent_service._store_conversation(
            user_id, message, "OK, dropped it. Let me know if you need anything else."
        )
        return {
            "operation": "flow_cancelled",
            "response": "OK, dropped it. Let me know if you need anything else.",
            "trigger_invoice": False,
            "invoice_data": {},
        }


# ── INVOICE_AWAIT_SEND_CONFIRM ────────────────────────────────────────

class InvoiceAwaitSendConfirm(Flow):
    """User has been shown the generated invoice PDF; bot asked
    'Should I also email it to <poc_email>? Reply Yes / No'.

    Delegates handle_response / on_cancel to the existing
    intent_service._handle_send_confirmation, which already implements:
      - YES → send email, mark invoice_date, ack.
      - NO  → cancel, friendly note.
      - feedback ('missing client billing', etc.) → invoice_feedback path.
    """

    name = FLOW_INVOICE_AWAIT_SEND_CONFIRM

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        # The existing handler reads its state from user_mem['pending_send_invoice'],
        # which is kept in sync at flow entry. So we can just delegate.
        logger.info(
            f"[FLOW_V2] InvoiceAwaitSendConfirm.handle_response "
            f"user={user_id} ctx_client={context.get('client_name')!r}"
        )
        result = intent_service._handle_send_confirmation(user_id, message)
        # _handle_send_confirmation has already cleared the legacy
        # awaiting_send_confirmation flag. Tell the FlowMachine the flow's done.
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception as e:
            logger.warning(f"[FLOW_V2] FlowMachine.reset failed (non-fatal): {e}")
        return result

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        client = context.get("client_name", "your invoice")
        poc = context.get("poc_email")
        if poc:
            return f"\n\nStill waiting on the email confirmation for {client} ({poc}). Yes to send, No to skip."
        return f"\n\nStill waiting — should I email the {client} invoice? Yes / No."

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        # Reuse the existing decline path by simulating the "no" route through
        # _handle_send_confirmation — it already clears flags and emits a
        # friendly "got it, skipped" line.
        result = intent_service._handle_send_confirmation(user_id, "no")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# Registry — dispatcher uses this to look up the right Flow by name.
REGISTRY: Dict[str, Flow] = {
    FLOW_INVOICE_AWAIT_SEND_CONFIRM: InvoiceAwaitSendConfirm(),
}


def get_flow(flow_name: Optional[str]) -> Optional[Flow]:
    if not flow_name:
        return None
    return REGISTRY.get(flow_name)
