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
    FLOW_BANK_DETAILS,
    FLOW_NAME_CHANGE,
    FLOW_LINK_ACCOUNT,
    FLOW_INVOICE_ADDRESS,
    FLOW_INVOICE_NEED_JOB_DESCRIPTION,
    FLOW_INVOICE_READINESS_POC_EMAIL,
    FLOW_INVOICE_NEED_MONTH,
    FLOW_COMPOUND_RESPONSE,
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
        # _handle_send_confirmation has already cleared pending_send_invoice
        # (the payload). FlowMachine is the sole source of truth for whether
        # this flow is active (Phase 2.3 — no legacy awaiting_* flag exists
        # for it anymore), so THIS reset is what actually ends the flow.
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
        # Phase 2.3: no legacy flag left to clear — FlowMachine.reset is
        # what actually ends this flow now (no retry loop, always resets).
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
        # Phase 2.3: no legacy awaiting_poc_email flag exists anymore —
        # _handle_poc_email_response signals "stay in the flow, let the
        # user retry" via its returned operation ("poc_email_retry")
        # instead, same pattern as BankDetails / LinkAccount.
        if result.get("operation") != "poc_email_retry":
            try:
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
        # _extract_and_confirm's own success/missing-fields branches write
        # flow_machine.set_state(SMART_CAPTURE_CONFIRM_PENDING) directly
        # (Phase 2.3), and its "nothing extracted" retry branch re-arms this
        # same flow via _arm_smart_capture_description_v2 -- so the only
        # case left to handle here is the (should-not-normally-happen)
        # fallback: neither a form started nor a re-prompt, i.e. some other
        # outcome altogether -- reset defensively rather than get stuck.
        result = intent_service._extract_and_confirm(user_id, message)
        try:
            if (not intent_service.memory.get_form_state(user_id)
                    and result.get("operation") != "smart_capture_prompt"):
                intent_service.flow_machine.reset(user_id)
        except Exception as e:
            logger.warning(f"[FLOW_V2] post-extract transition failed: {e}")
        return result

    def resume_nudge(self, context):
        return "\n\nStill waiting on the job description — send it in one message, or 'cancel' to drop the form."

    def on_cancel(self, intent_service, user_id, message, context):
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
        # _handle_form_step may complete the form (form_state cleared), stay
        # in confirm (still awaiting), or route to "Edit" — which cancels
        # the form AND transitions v2 to SMART_CAPTURE_NEED_DESCRIPTION via
        # _arm_smart_capture_description_v2 (see _handle_smart_capture_confirm).
        # Don't let this handler's own form_state check clobber that.
        try:
            if result.get("operation") == "smart_capture_edit":
                pass  # already transitioned back to SMART_CAPTURE_NEED_DESCRIPTION
            elif not intent_service.memory.get_form_state(user_id):
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


# ── BANK_DETAILS (WP-3 slice 2) ─────────────────────────────────────────

class BankDetails(Flow):
    """Bot asked the user to send their own bank details in one structured
    message. Delegates to the existing _handle_bank_details_response, which
    already: accepts 'cancel'/'stop'/'nevermind'/'skip'; re-prompts on an
    unparseable message; and — if a pending_invoice was waiting on this —
    resumes the invoice flow after a successful save."""

    name = FLOW_BANK_DETAILS

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(f"[FLOW_V2] BankDetails.handle_response user={user_id}")
        result = intent_service._handle_bank_details_response(user_id, message)
        # Phase 2.3: no legacy awaiting_bank_details flag exists to check-
        # after anymore. _handle_bank_details_response signals "stay in the
        # flow, let the user retry" via its returned operation name instead
        # (it never touches FlowMachine on that path, so simply NOT
        # resetting here is what keeps BANK_DETAILS active).
        if result.get("operation") != "bank_details_retry":
            try:
                intent_service.flow_machine.reset(user_id)
            except Exception:
                pass
        return result

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        return "\n\n(Still waiting on your bank details — account name, bank, account number, IFSC — or 'cancel' to skip.)"

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        result = intent_service._handle_bank_details_response(user_id, "cancel")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# ── NAME_CHANGE (WP-3 slice 2) ──────────────────────────────────────────

class NameChange(Flow):
    """Bot asked what the user's new display name should be. Delegates to
    the existing _process_name_change, which accepts 'cancel'/'nevermind'/
    'no' and otherwise applies the reply as the new (title-cased) name —
    always completes in one turn, no retry loop."""

    name = FLOW_NAME_CHANGE

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(f"[FLOW_V2] NameChange.handle_response user={user_id}")
        result = intent_service._process_name_change(user_id, message)
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        return "\n\n(Still waiting on what to change your name to — or 'cancel' to keep it as-is.)"

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        result = intent_service._process_name_change(user_id, "cancel")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# ── LINK_ACCOUNT (WP-3 slice 2) ─────────────────────────────────────────

class LinkAccount(Flow):
    """Bot asked for the user's ID from the other platform (Telegram/
    WhatsApp) to link cross-platform data access. Delegates to the existing
    _process_link_id, which accepts 'cancel'/'nevermind'/'no', validates
    the ID shape and re-prompts on an invalid one, and otherwise treats the
    reply as the ID to link.

    Pre-Phase-2.3 note: this class used to unconditionally reset FlowMachine
    after every call, even on the invalid-ID retry path — that "worked" only
    because the legacy awaiting_link_id flag got re-armed by
    _process_link_id and reconciliation picked it back up on the NEXT
    message (FlowMachine bounced IDLE -> LINK_ACCOUNT again). With no legacy
    flag left to reconcile FROM, that trick no longer exists, so this now
    checks the returned operation directly, same pattern as BankDetails."""

    name = FLOW_LINK_ACCOUNT

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(f"[FLOW_V2] LinkAccount.handle_response user={user_id}")
        result = intent_service._process_link_id(user_id, message)
        if result.get("operation") != "link_invalid_id":
            try:
                intent_service.flow_machine.reset(user_id)
            except Exception:
                pass
        return result

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        return "\n\n(Still waiting on the other platform's user ID to link — or 'cancel' to skip.)"

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        result = intent_service._process_link_id(user_id, "cancel")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# ── INVOICE_ADDRESS (WP-3 slice 3) ──────────────────────────────────────

class InvoiceAddress(Flow):
    """Bot asked for the user's own business address for the invoice header
    — either the mandatory readiness-gate prompt mid invoice-generation, or
    a standalone 'update my address' ask (both call _arm_invoice_address_v2,
    Phase 2.3). Delegates to the existing _handle_invoice_address_response,
    which already: accepts 'cancel'/'stop'/'abort'/'nevermind'; resumes the
    invoice flow if one was pending; otherwise just confirms the standalone
    update. Always completes in one turn — no retry loop."""

    name = FLOW_INVOICE_ADDRESS

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(
            f"[FLOW_V2] InvoiceAddress.handle_response user={user_id} "
            f"ctx_client={context.get('client_name')!r}"
        )
        result = intent_service._handle_invoice_address_response(user_id, message)
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        client = context.get("client_name")
        if client:
            return f"\n\nStill need your business address for the {client} invoice header (or 'cancel' to stop)."
        return "\n\nStill waiting on your business address (or 'cancel' to skip)."

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        result = intent_service._handle_invoice_address_response(user_id, "cancel")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# ── INVOICE_NEED_JOB_DESCRIPTION (WP-3 slice 3) ─────────────────────────

class InvoiceNeedJobDescription(Flow):
    """Bot asked what the work was for one specific EXISTING job that has no
    description, before it can be invoiced. Distinct from
    SMART_CAPTURE_NEED_DESCRIPTION (a NEW job being logged). Delegates to
    the existing _handle_job_description_response, which saves the
    description to the pinned row and resumes the invoice flow. Always
    completes in one turn."""

    name = FLOW_INVOICE_NEED_JOB_DESCRIPTION

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(
            f"[FLOW_V2] InvoiceNeedJobDescription.handle_response user={user_id} "
            f"row_id={context.get('row_id')!r}"
        )
        result = intent_service._handle_job_description_response(user_id, message)
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        return "\n\nStill need a description for that job before I can invoice it (or 'cancel' to stop)."

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        result = intent_service._handle_job_description_response(user_id, "cancel")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# ── INVOICE_READINESS_POC_EMAIL ─────────────────────────────────────────

class InvoiceReadinessPocEmail(Flow):
    """Bot asked what email a client's invoice/reminders should go to,
    BEFORE generation — distinct from FLOW_INVOICE_NEED_POC_EMAIL, the
    SEND-time flow (asks for the address to deliver an already-generated
    PDF). Delegates to the existing _handle_invoice_poc_email_response,
    which: accepts 'cancel'/'stop'/'abort'/'nevermind'; re-prompts on an
    invalid email (retry loop, like BANK_DETAILS); saves and resumes the
    invoice flow on success."""

    name = FLOW_INVOICE_READINESS_POC_EMAIL

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(
            f"[FLOW_V2] InvoiceReadinessPocEmail.handle_response user={user_id} "
            f"ctx_client={context.get('client_name')!r}"
        )
        result = intent_service._handle_invoice_poc_email_response(user_id, message)
        # Phase 2.2 (post-2.3): no legacy awaiting_invoice_poc_email flag
        # exists to check-after anymore. The handler signals "stay in the
        # flow, let the user retry" via its returned operation name
        # instead (it re-arms FlowMachine itself on that path via
        # _arm_invoice_readiness_poc_email_v2, so simply NOT resetting here
        # is what keeps the flow active) — same pattern as BankDetails.
        if result.get("operation") != "invoice_poc_email_retry":
            try:
                intent_service.flow_machine.reset(user_id)
            except Exception:
                pass
        return result

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        client = context.get("client_name")
        if client:
            return f"\n\nStill need an email for {client}'s invoice (or 'cancel' to stop)."
        return "\n\nStill waiting on that email address (or 'cancel' to stop)."

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        result = intent_service._handle_invoice_poc_email_response(user_id, "cancel")
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        return result


# ── INVOICE_NEED_MONTH ───────────────────────────────────────────────────

class InvoiceNeedMonth(Flow):
    """Bot asked which month an invoice request should cover (no month was
    given in the original request). Delegates to the existing
    _handle_invoice_month_reply, which: extracts a month name from free
    text (re-prompting via its own operation "invoice_month_retry" if none
    is found — no cancel handling of its own, since ANY text is read as an
    attempted month); on success, reconstructs a synthetic message and
    re-enters process_request to actually generate/send the invoice."""

    name = FLOW_INVOICE_NEED_MONTH

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(
            f"[FLOW_V2] InvoiceNeedMonth.handle_response user={user_id} "
            f"ctx_client={context.get('client_name')!r}"
        )
        user_mem = intent_service.memory.get_user_memory(user_id) or {}
        result = intent_service._handle_invoice_month_reply(
            user_id, message, user_mem, user_id, [],
        )
        # _handle_invoice_month_reply signals "stay in the flow, let the
        # user retry" via its returned operation name (it re-arms
        # FlowMachine itself on that path via _arm_invoice_month_v2) --
        # same check-after pattern as BankDetails/InvoiceReadinessPocEmail.
        # On success it re-enters process_request via a synthetic message,
        # which may land in ANY other flow (or none) -- reset unconditionally
        # unless the retry path re-armed THIS SAME flow.
        if result.get("operation") != "invoice_month_retry":
            try:
                intent_service.flow_machine.reset(user_id)
            except Exception:
                pass
        return result

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        client = context.get("client_name")
        if client:
            return f"\n\nStill need the month for {client}'s invoice (or 'cancel' to stop)."
        return "\n\nStill waiting on that month (or 'cancel' to stop)."

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        # No delegate to call -- _handle_invoice_month_reply has no cancel
        # branch of its own (any text is read as an attempted month), so a
        # genuine CANCEL verdict is handled entirely here.
        intent_service.memory.update_user_memory(user_id, {
            "pending_invoice_client": None,
            "pending_invoice_send_email": None,
        })
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        response = "No problem — invoice cancelled. Nothing was generated."
        intent_service._store_conversation(user_id, message, response)
        return {"operation": "invoice_cancelled", "response": response,
                "trigger_invoice": False, "invoice_data": {}}


# ── COMPOUND_RESPONSE ────────────────────────────────────────────────────

class CompoundResponse(Flow):
    """Bot suggested a follow-up action after a job save/insert ("You also
    mentioned: X. Want me to do that now? (Yes / No)"). Delegates to the
    existing _handle_compound_response, which: resets FlowMachine FIRST
    (every branch either recurses into process_request or is terminal, so
    leaving this flow active into a recursive call would make
    dispatch_in_flow try to route it through this same flow again); 'yes'
    (+ optional qualifier) re-enters process_request with the merged
    action; a decline word ends cleanly; anything else falls through and is
    treated as a brand-new message. No retry loop."""

    name = FLOW_COMPOUND_RESPONSE

    def handle_response(self, intent_service, user_id: str, message: str,
                        context: Dict[str, Any]) -> Dict[str, Any]:
        logger.info(f"[FLOW_V2] CompoundResponse.handle_response user={user_id}")
        # _handle_compound_response resets FlowMachine itself (see its own
        # docstring) -- nothing left to do here.
        return intent_service._handle_compound_response(user_id, message)

    def resume_nudge(self, context: Dict[str, Any]) -> str:
        action = context.get("suggested_next_action")
        if action:
            return f"\n\nStill waiting: want me to also \"{action}\"? (Yes / No)"
        return "\n\nStill waiting on that Yes/No."

    def on_cancel(self, intent_service, user_id: str, message: str,
                  context: Dict[str, Any]) -> Dict[str, Any]:
        intent_service.memory.update_user_memory(user_id, {"suggested_next_action": None})
        try:
            intent_service.flow_machine.reset(user_id)
        except Exception:
            pass
        response = "👍 No problem. Let me know if you need anything else."
        intent_service._store_conversation(user_id, message, response)
        return {"operation": "compound_declined", "response": response,
                "trigger_invoice": False, "invoice_data": {}}


# Registry — dispatcher uses this to look up the right Flow by name.
REGISTRY: Dict[str, Flow] = {
    FLOW_INVOICE_AWAIT_SEND_CONFIRM:     InvoiceAwaitSendConfirm(),
    FLOW_INVOICE_NEED_BILLING:           InvoiceNeedBilling(),
    FLOW_INVOICE_NEED_POC_NAME:          InvoiceNeedPocName(),
    FLOW_INVOICE_NEED_POC_EMAIL:         InvoiceNeedPocEmail(),
    FLOW_INVOICE_READINESS_POC_EMAIL:    InvoiceReadinessPocEmail(),
    FLOW_INVOICE_NEED_MONTH:             InvoiceNeedMonth(),
    FLOW_COMPOUND_RESPONSE:              CompoundResponse(),
    FLOW_SMART_CAPTURE_NEED_DESCRIPTION: SmartCaptureNeedDescription(),
    FLOW_SMART_CAPTURE_CONFIRM_PENDING:  SmartCaptureConfirmPending(),
    FLOW_DISAMBIGUATION:                 Disambiguation(),
    FLOW_BANK_DETAILS:                   BankDetails(),
    FLOW_NAME_CHANGE:                    NameChange(),
    FLOW_LINK_ACCOUNT:                   LinkAccount(),
    FLOW_INVOICE_ADDRESS:                InvoiceAddress(),
    FLOW_INVOICE_NEED_JOB_DESCRIPTION:   InvoiceNeedJobDescription(),
}


def get_flow(flow_name: Optional[str]) -> Optional[Flow]:
    if not flow_name:
        return None
    return REGISTRY.get(flow_name)
