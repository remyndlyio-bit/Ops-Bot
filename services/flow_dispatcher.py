"""
Session 1 of the FlowMachine v2 migration.

Routes a classified Verdict to either:
  (a) a leaf handler we own directly here (small talk / feature questions /
      unknown — these are the safest paths to take over first), or
  (b) the legacy code path, by returning SHADOW_ONLY. The caller then
      proceeds with the existing cascade as if v2 wasn't there.

Session 1 only owns the LEAF paths. Read/write intents fall through to the
legacy pipeline so we get verdict telemetry without changing destructive
behaviour. Sessions 2 and 3 will progressively take over those branches.
"""

from __future__ import annotations

from typing import Any, Dict, Optional

from services.classifier import Verdict
from services.flow_machine import FlowMachine
from services.flows import get_flow
from services.response_formatter import unsupported_feature_phrase
from utils.logger import logger
from utils.telemetry import note_route

# Sentinel returned when the dispatcher decides NOT to handle a verdict in v2
# and the caller should fall through to the legacy code path.
SHADOW_ONLY = None  # explicit None for readability


def dispatch_idle(
    verdict: Verdict,
    *,
    intent_service,
    user_id: str,
    conversation_history,
) -> Optional[Dict[str, Any]]:
    """
    Returns a process_request-shaped dict if v2 handled the message,
    or None to indicate the caller should fall through to the legacy path.

    Owned (session 1):
      - SMALL_TALK
      - FEATURE_QUESTION
      - UNKNOWN (low-confidence or off-topic)

    Shadow-only (returns None, legacy handles):
      - READ_QUERY, READ_AGGREGATE
      - WRITE_CREATE, WRITE_UPDATE, WRITE_DELETE, WRITE_INVOICE
    """
    intent = verdict["intent"]
    raw = verdict["raw_message"]

    # ── SMALL_TALK ─────────────────────────────────────────────────────
    if intent == "SMALL_TALK":
        # Reuse the existing curated small-talk responder. It returns a
        # nice canned reply (greeting, thanks, etc.) and handles None
        # gracefully — if it returns None we drop down to a generic ack.
        try:
            canned = intent_service._detect_small_talk(raw, user_id)
        except Exception as e:
            logger.warning(f"[V2_DISPATCH] _detect_small_talk error: {e}")
            canned = None
        note_route("v2_leaf")  # WP-5 item 3: v2-owned leaf paths never touch the LLM synthesis pipeline
        if canned:
            intent_service._store_conversation(user_id, raw, canned)
            return {
                "operation": "small_talk",
                "response": canned,
                "trigger_invoice": False,
                "invoice_data": {},
            }
        # Fallback — keep it brief and on-brand.
        ack = "Hey 👋 — what's on your plate today? I'm good with jobs, invoices, and payments."
        intent_service._store_conversation(user_id, raw, ack)
        return {
            "operation": "small_talk",
            "response": ack,
            "trigger_invoice": False,
            "invoice_data": {},
        }

    # ── AUDIT_REPLY ────────────────────────────────────────────────────
    if intent == "AUDIT_REPLY":
        note_route("audit_reply_v2")
        try:
            from utils.pending_reminders import get_pending
            pending = get_pending(user_id) or []
            if pending:
                result = intent_service._handle_pending_audit_reply(user_id, raw, raw.lower(), pending)
                if result:
                    return result
        except Exception as e:
            logger.warning(f"[V2_DISPATCH] _handle_pending_audit_reply error: {e}")
        # Fallback if audit reply fails or no pending audit exists
        return SHADOW_ONLY

    # ── FEATURE_QUESTION & UNKNOWN ────────────────────────────────────
    # Both route through the feature-aware AI responder so the user gets
    # an on-brand reply grounded in REMYNDLY_FEATURES.md. UNKNOWN here is
    # genuinely off-topic / low confidence — exactly what the catalog was
    # built for.
    if intent in ("FEATURE_QUESTION", "UNKNOWN"):
        note_route("v2_leaf")
        try:
            reply = intent_service.gemini.answer_feature_question(
                raw, conversation_history=conversation_history
            )
        except Exception as e:
            logger.warning(f"[V2_DISPATCH] answer_feature_question error: {e}")
            reply = None
        if not reply or not reply.strip():
            reply = unsupported_feature_phrase(raw[:80])
        intent_service._store_conversation(user_id, raw, reply)
        return {
            "operation": "feature_q" if intent == "FEATURE_QUESTION" else "unknown",
            "response": reply,
            "trigger_invoice": False,
            "invoice_data": {},
        }

    # ── Everything else — shadow only ─────────────────────────────────
    # Read/write intents still flow through the existing cascade. We've
    # already logged the verdict in classifier.classify, so we get a
    # production telemetry trail of "what would v2 have decided" vs
    # "what the legacy code actually did", without behaviour change.
    return SHADOW_ONLY


def dispatch_in_flow(
    verdict: Verdict,
    *,
    intent_service,
    user_id: str,
    current_flow: str,
    current_context: Dict[str, Any],
    conversation_history,
) -> Optional[Dict[str, Any]]:
    """
    Routes a verdict that arrived while the user is in an ACTIVE flow.
    Reads `verdict['flow_compatible']` and acts accordingly.

    Returns a process_request-shaped dict if v2 handled it, or SHADOW_ONLY
    (None) to fall back to legacy. We fall back when:
      - flow_compatible is None or invalid
      - the flow isn't in our registry yet (session 2 only owns one)
      - any branch hits an exception
    """
    flow = get_flow(current_flow)
    if flow is None:
        logger.info(f"[V2_DISPATCH] in_flow={current_flow} not in registry — shadow only")
        return SHADOW_ONLY

    fc = verdict.get("flow_compatible")
    raw = verdict["raw_message"]

    try:
        # CANCEL — user wants out of the flow.
        if fc == "CANCEL":
            logger.info(f"[V2_DISPATCH] CANCEL in flow={current_flow}")
            return flow.on_cancel(intent_service, user_id, raw, current_context)

        # FLOW_RESPONSE — user is answering the bot's pending question.
        if fc == "FLOW_RESPONSE":
            logger.info(f"[V2_DISPATCH] FLOW_RESPONSE in flow={current_flow}")
            return flow.handle_response(intent_service, user_id, raw, current_context)

        # SIDE_QUESTION — answer inline, stay in flow.
        # Session 2 keeps this simple: only owns side questions that are
        # READ_QUERY / READ_AGGREGATE / FEATURE_QUESTION. For READ paths we
        # need an answer — easiest reliable way is to fall back to legacy
        # for the actual SQL pipeline, then append the resume_nudge.
        # For FEATURE_QUESTION we can answer here directly.
        if fc == "SIDE_QUESTION":
            if verdict["intent"] == "FEATURE_QUESTION":
                reply = intent_service.gemini.answer_feature_question(
                    raw, conversation_history=conversation_history
                ) or unsupported_feature_phrase(raw[:80])
                reply = reply + flow.resume_nudge(current_context)
                intent_service._store_conversation(user_id, raw, reply)
                logger.info(f"[V2_DISPATCH] SIDE_QUESTION (FEATURE) answered, staying in {current_flow}")
                return {
                    "operation": "side_q",
                    "response": reply,
                    "trigger_invoice": False,
                    "invoice_data": {},
                }
            # READ_QUERY / READ_AGGREGATE side questions — Phase 1.5: try the
            # deterministic router first (no LLM, no plan retry — the common
            # query shapes it covers are exactly the ones safe to answer
            # inline without risking a slow/expensive planner detour mid-flow).
            # On a match we get BOTH the answer and the resume_nudge, fixing
            # the session-2 limitation noted above. Anything the router
            # doesn't recognise falls through to SHADOW_ONLY exactly as
            # before — legacy's LLM planner still owns those, unchanged.
            if verdict["intent"] in ("READ_QUERY", "READ_AGGREGATE"):
                try:
                    # Scope-clarifying questions about the PREVIOUS answer
                    # ("does that include paid and unpaid?") must be checked
                    # BEFORE the router: a stray keyword like "unpaid" inside
                    # such a question matches the router's unpaid_list route
                    # and would answer the wrong thing entirely (a NEW list of
                    # unpaid jobs, not a description of the prior answer's
                    # scope). Same ordering rationale as the legacy cascade's
                    # AnswerLedger check (see intent_service.py ~line 5290).
                    from services.answer_ledger import answer_scope_question, is_scope_question
                    _scope_answer = answer_scope_question(
                        raw, intent_service.memory.get_user_memory(user_id)
                    )
                    if _scope_answer:
                        _nudge = flow.resume_nudge(current_context)
                        _reply = _scope_answer + (_nudge or "")
                        intent_service._store_conversation(user_id, raw, _reply)
                        logger.info(
                            f"[V2_DISPATCH] SIDE_QUESTION (READ) answered via ledger scope, "
                            f"staying in {current_flow}"
                        )
                        return {
                            "operation": "scope_question",
                            "response": _reply,
                            "trigger_invoice": False,
                            "invoice_data": {},
                        }

                    # Scope-shaped but nothing to answer from (empty/stale
                    # ledger) — do NOT hand this to the router. A stray word
                    # like "unpaid" inside "does this include unpaid?" would
                    # false-match the unpaid_list route and answer a totally
                    # different question. Fall through to legacy instead.
                    if is_scope_question(raw):
                        logger.info(
                            f"[V2_DISPATCH] SIDE_QUESTION (READ) scope-shaped but no ledger "
                            f"to answer from — shadow, flow={current_flow} preserved"
                        )
                        return SHADOW_ONLY

                    from services.query_router import route_common_query
                    _routed = route_common_query(raw, user_id)
                    if _routed is not None:
                        _result = intent_service._execute_routed_query(
                            _routed, user_id, user_id, raw, conversation_history,
                        )
                        if _result is not None:
                            _nudge = flow.resume_nudge(current_context)
                            if _nudge:
                                _result = dict(_result)
                                _result["response"] = (_result.get("response") or "") + _nudge
                                intent_service._store_conversation(user_id, raw, _result["response"])
                            logger.info(
                                f"[V2_DISPATCH] SIDE_QUESTION (READ) answered via router, "
                                f"staying in {current_flow}"
                            )
                            return _result
                except Exception as e:
                    logger.warning(f"[V2_DISPATCH] SIDE_QUESTION router attempt failed: {e}")
            # No deterministic match (or a WRITE-shaped side question) — fall
            # back to legacy. The active flow's awaiting_* legacy flag remains
            # set in parallel, so the next message still routes to the flow
            # handler. Legacy code appends no nudge for this path.
            logger.info(
                f"[V2_DISPATCH] SIDE_QUESTION (READ) → shadow (legacy answers, "
                f"flow={current_flow} preserved via legacy awaiting flag)"
            )
            return SHADOW_ONLY

        # NEW_FLOW — user is starting a different operation mid-flow.
        # Session 2 takes the safe path: fall back to legacy. The existing
        # intent-shift guard will decide whether to clear the legacy awaiting
        # flag based on the message. Push/pop semantics land in session 3
        # once more flows are migrated and we can guarantee stack invariants.
        if fc == "NEW_FLOW":
            logger.info(f"[V2_DISPATCH] NEW_FLOW in flow={current_flow} — shadow (legacy decides)")
            return SHADOW_ONLY

        # Missing or unknown flow_compatible — shadow.
        logger.info(f"[V2_DISPATCH] flow_compatible={fc!r} — shadow")
        return SHADOW_ONLY

    except Exception as e:
        logger.warning(f"[V2_DISPATCH] in_flow exception, falling back: {e}")
        return SHADOW_ONLY
