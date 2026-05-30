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
from services.response_formatter import unsupported_feature_phrase
from utils.logger import logger

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

    # ── FEATURE_QUESTION & UNKNOWN ────────────────────────────────────
    # Both route through the feature-aware AI responder so the user gets
    # an on-brand reply grounded in REMYNDLY_FEATURES.md. UNKNOWN here is
    # genuinely off-topic / low confidence — exactly what the catalog was
    # built for.
    if intent in ("FEATURE_QUESTION", "UNKNOWN"):
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
