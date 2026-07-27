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

from services.flow_machine import (
    FLOW_INVOICE_AWAIT_SEND_CONFIRM,
    FLOW_INVOICE_NEED_BILLING,
    FLOW_INVOICE_NEED_POC_NAME,
    FLOW_INVOICE_NEED_POC_EMAIL,
    FLOW_SMART_CAPTURE_NEED_DESCRIPTION,
    FLOW_SMART_CAPTURE_CONFIRM_PENDING,
    FLOW_DISAMBIGUATION,
)
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


# ── INVOICE_NEED_BILLING ──────────────────────────────────────────────

class InvoiceNeedBilling(Flow):
    """Bot asked for client billing details (name, address, GST) before
    generating the invoice. User reply is either free-text billing details
    or a skip token. Delegates to existing _handle_client_billing_response
    which already accepts 'skip'/'cancel'/'no'/'none' internally."""

    name = FLOW_INVOICE_NEED_BILLING

    def handle_response(self, intent_service, user_id, message, context):
        logger.info(
            f"[FLOW_V2] InvoiceNeedBilling.handle_response user={user_id} "
            f"ctx_client={context.get('client_name')!r}"
        )
        result = intent_service._handle_client_billing_response(user_id, message)
        # _handle_client_billing_response clears the legacy flag. Mirror v2.
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result

    def resume_nudge(self, context):
        client = context.get("client_name", "the client")
        return f"\n\nStill waiting on billing details for {client} (or 'skip' to skip)."

    def on_cancel(self, intent_service, user_id, message, context):
        # Delegate to the existing skip path which knows to resume invoice generation.
        result = intent_service._handle_client_billing_response(user_id, "skip")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# ── INVOICE_NEED_POC_NAME ─────────────────────────────────────────────

class InvoiceNeedPocName(Flow):
    """Bot asked for the POC name to address the invoice to. Delegates to
    _handle_poc_name_response which accepts 'skip'/'cancel'/'no'/'none'."""

    name = FLOW_INVOICE_NEED_POC_NAME

    def handle_response(self, intent_service, user_id, message, context):
        logger.info(
            f"[FLOW_V2] InvoiceNeedPocName.handle_response user={user_id} "
            f"ctx_client={context.get('client_name')!r}"
        )
        result = intent_service._handle_poc_name_response(user_id, message)
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result

    def resume_nudge(self, context):
        client = context.get("client_name", "the client")
        return f"\n\nStill need a POC name for the {client} invoice (or 'skip' to use the brand/client name)."

    def on_cancel(self, intent_service, user_id, message, context):
        result = intent_service._handle_poc_name_response(user_id, "skip")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# ── INVOICE_NEED_POC_EMAIL ────────────────────────────────────────────

class InvoiceNeedPocEmail(Flow):
    """Bot asked for the client's contact email to send the invoice.
    Delegates to _handle_poc_email_response which validates the email
    format and supports 'cancel'/'skip'/'no'/'nevermind'."""

    name = FLOW_INVOICE_NEED_POC_EMAIL

    def handle_response(self, intent_service, user_id, message, context):
        logger.info(
            f"[FLOW_V2] InvoiceNeedPocEmail.handle_response user={user_id} "
            f"ctx_client={context.get('client_name')!r}"
        )
        result = intent_service._handle_poc_email_response(user_id, message)
        # NOTE: _handle_poc_email_response may RE-ARM awaiting_poc_email when
        # the email format is invalid. In that case the legacy flag is still
        # True, so we keep FlowMachine in this same state too (don't reset).
        try:
            user_mem_after = intent_service.memory.get_user_memory(user_id) or {}
            if not user_mem_after.get("awaiting_poc_email"):
                intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result

    def resume_nudge(self, context):
        client = context.get("client_name", "the client")
        return f"\n\nStill need the {client} contact email — send it (e.g. client@x.com) or 'skip'."

    def on_cancel(self, intent_service, user_id, message, context):
        result = intent_service._handle_poc_email_response(user_id, "cancel")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# ── SMART_CAPTURE_NEED_DESCRIPTION ────────────────────────────────────

class SmartCaptureNeedDescription(Flow):
    """The bot asked the user to describe a new job. Reply is free-text job
    details (brand, date, fees, etc.) which goes through field extraction.
    Delegates to _extract_and_confirm. This flow has historically been
    sticky — session 2.5 explicitly clears it on CANCEL so the user can
    always type a question and escape."""

    name = FLOW_SMART_CAPTURE_NEED_DESCRIPTION

    def handle_response(self, intent_service, user_id, message, context):
        logger.info(f"[FLOW_V2] SmartCaptureNeedDescription.handle_response user={user_id}")
        # _extract_and_confirm itself transitions to the confirm form state
        # (via memory.start_form), so we let it run and DON'T reset v2 — the
        # next message arrives in SMART_CAPTURE_CONFIRM_PENDING.
        result = intent_service._extract_and_confirm(user_id, message)
        # If extraction came back empty, _extract_and_confirm RE-PROMPTS for
        # job input. Stay in this flow. Otherwise transition to the confirm
        # state via flow_machine (parallel writer).
        try:
            from services.flow_machine import FLOW_SMART_CAPTURE_CONFIRM_PENDING
            user_mem_after = intent_service.memory.get_user_memory(user_id) or {}
            if intent_service.memory.get_form_state(user_id):
                # Form started → confirm pending.
                intent_service.flow_machine.set_state(
                    user_id, FLOW_SMART_CAPTURE_CONFIRM_PENDING, {"source": "smart_capture"}
                )
            elif not user_mem_after.get("awaiting_job_input"):
                # No form, no awaiting → done somehow → reset.
                intent_service.flow_machine.reset(user_id)
            # else: still awaiting more job input, stay in this flow.
        except Exception as e:
            logger.warning(f"[FLOW_V2] post-extract transition failed: {e}")
        return result

    def resume_nudge(self, context):
        return "\n\nStill waiting on the job description — send it in one message, or 'cancel' to drop the form."

    def on_cancel(self, intent_service, user_id, message, context):
        intent_service.memory.update_user_memory(user_id, {"awaiting_job_input": False})
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        ack = "OK, dropped the add-job form. Let me know if you need anything else."
        intent_service._store_conversation(user_id, message, ack)
        return {"operation": "smart_capture_cancelled", "response": ack,
                "trigger_invoice": False, "invoice_data": {}}


# ── SMART_CAPTURE_CONFIRM_PENDING ─────────────────────────────────────

class SmartCaptureConfirmPending(Flow):
    """User has been shown the extracted-job confirmation card with
    'Save this job? (Yes / Edit)'. Reply is Yes / Edit / extra fields /
    No. Delegates to _handle_form_step which routes to
    _handle_smart_capture_confirm. After completion the form_state is
    cleared and v2 transitions back to IDLE."""

    name = FLOW_SMART_CAPTURE_CONFIRM_PENDING

    def handle_response(self, intent_service, user_id, message, context):
        logger.info(f"[FLOW_V2] SmartCaptureConfirmPending.handle_response user={user_id}")
        result = intent_service._handle_form_step(user_id, message)
        # _handle_form_step may complete the form (form_state cleared) or
        # stay in confirm (still awaiting). Sync v2 to whichever.
        try:
            if not intent_service.memory.get_form_state(user_id):
                intent_service.flow_machine.reset(user_id)
            # else: still in confirm, leave v2 state as-is.
        except Exception:
            pass
        return result

    def resume_nudge(self, context):
        return "\n\nStill waiting on the Yes/Edit confirmation for the new job — or 'cancel' to drop it."

    def on_cancel(self, intent_service, user_id, message, context):
        try:
            intent_service.memory.cancel_form(user_id)
        except Exception:
            pass
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        ack = "OK, dropped the new job. Let me know if you need anything else."
        intent_service._store_conversation(user_id, message, ack)
        return {"operation": "smart_capture_cancelled", "response": ack,
                "trigger_invoice": False, "invoice_data": {}}


# ── DISAMBIGUATION (WP-3, ASSISTANT_PLAN.md) ───────────────────────────

class Disambiguation(Flow):
    """The bot showed a numbered 'which one did you mean?' list (or a bulk
    delete-confirmation) and is waiting for a pick. Delegates to the existing
    _handle_disambiguation_reply, which already implements the numbered-pick,
    'all'/bulk-confirm, and cancel logic for BOTH delete-type and modify-type
    pending state (services.answer_ledger-style row targeting reused via
    _apply_modify_update).

    The real value of migrating this flow specifically: flow_compatible
    classification (an LLM reading the whole message, not a regex) decides
    BEFORE handle_response is ever called whether a message is a genuine pick
    vs. a side question vs. a cancel -- see classifier.py's DISAMBIGUATION
    guidance. That is a real upgrade over the legacy method's own internal
    heuristics (a bare 'yes' being ambiguous, a '?'-or-question-word regex for
    detecting a new query), not just a relocation of the same logic. Those
    legacy heuristics still exist and still run (defence in depth / the
    non-v2 fallback path), but the classifier is what actually decides first
    now when FLOW_MACHINE_V2 is on.
    """

    name = FLOW_DISAMBIGUATION

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(
            f"[FLOW_V2] Disambiguation.handle_response user={user_id} "
            f"type={context.get('type')} count={context.get('count')}"
        )
        pending = (intent_service.memory.get_user_memory(user_id) or {}).get("pending_disambiguation")
        if not pending:
            # Raced with something that already cleared it (e.g. a
            # different code path resolved it first). Nothing left to do.
            intent_service.flow_machine.reset(user_id)
            response = "That list isn't active anymore — go ahead and ask again."
            intent_service._store_conversation(user_id, message, response)
            return {"operation": "disambiguation_stale", "response": response,
                    "trigger_invoice": False, "invoice_data": {}}

        result = intent_service._handle_disambiguation_reply(user_id, message, pending)
        if result is None:
            # The legacy handler decided this wasn't actually a pick after
            # all (an ambiguous bare 'yes', or a question-shaped message the
            # classifier still let through) and has ALREADY cleared
            # pending_disambiguation itself. Never propagate None out of a
            # Flow.handle_response (that's not a contract any other flow
            # uses, and would risk the caller re-running the legacy
            # disambiguation check a second time against now-stale state) --
            # resolve it here instead: reset v2 to match and ask plainly.
            intent_service.flow_machine.reset(user_id)
            response = "Got it — go ahead and send that as a new message."
            intent_service._store_conversation(user_id, message, response)
            return {"operation": "disambiguation_cleared", "response": response,
                    "trigger_invoice": False, "invoice_data": {}}

        # Both the numbered-pick and cancel paths already clear
        # pending_disambiguation inside _handle_disambiguation_reply.
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        count = context.get("count")
        if count:
            return f"\n\n(Still waiting on which of the {count} matches you meant — reply with a number, or 'cancel'.)"
        return "\n\n(Still waiting on your pick from the list above — a number, or 'cancel'.)"

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        pending = (intent_service.memory.get_user_memory(user_id) or {}).get("pending_disambiguation")
        if pending:
            result = intent_service._handle_disambiguation_reply(user_id, "cancel", pending)
        else:
            result = None
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        if result is not None:
            return result
        response = "OK, cancelled. Let me know if you need anything else."
        intent_service._store_conversation(user_id, message, response)
        return {"operation": "disambiguation_cancelled", "response": response,
                "trigger_invoice": False, "invoice_data": {}}


# Registry — dispatcher uses this to look up the right Flow by name.
REGISTRY: Dict[str, Flow] = {
    FLOW_INVOICE_AWAIT_SEND_CONFIRM:     InvoiceAwaitSendConfirm(),
    FLOW_INVOICE_NEED_BILLING:           InvoiceNeedBilling(),
    FLOW_INVOICE_NEED_POC_NAME:          InvoiceNeedPocName(),
    FLOW_INVOICE_NEED_POC_EMAIL:         InvoiceNeedPocEmail(),
    FLOW_SMART_CAPTURE_NEED_DESCRIPTION: SmartCaptureNeedDescription(),
    FLOW_SMART_CAPTURE_CONFIRM_PENDING:  SmartCaptureConfirmPending(),
    FLOW_DISAMBIGUATION:                 Disambiguation(),
}


def get_flow(flow_name: Optional[str]) -> Optional[Flow]:
    if not flow_name:
        return None
    return REGISTRY.get(flow_name)
