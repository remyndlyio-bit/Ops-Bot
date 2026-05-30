"""
Session 1 of the FlowMachine v2 migration.

ONE Gemini call that returns a typed Verdict describing what the user wants.
Replaces the cascade of 13 mini-classifiers (keyword regexes, multiple AI
calls, planner classification) for IDLE-state messages only.

This file is intentionally small: it owns ONE prompt and ONE parser. The
dispatcher (services/flow_dispatcher.py) decides what to do with the Verdict.

Out of scope for session 1:
  - flow_compatible (push/pop side-question handling) — session 2.
  - CANCEL / FLOW_RESPONSE intents — session 2, when we own flow states.
  - Replacing the SQL planner — session 3.
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, List, Literal, Optional, TypedDict

from utils.logger import logger

Intent = Literal[
    "READ_QUERY",
    "READ_AGGREGATE",
    "WRITE_CREATE",
    "WRITE_UPDATE",
    "WRITE_DELETE",
    "WRITE_INVOICE",
    "FEATURE_QUESTION",
    "SMALL_TALK",
    "UNKNOWN",
]

VALID_INTENTS = {
    "READ_QUERY", "READ_AGGREGATE", "WRITE_CREATE", "WRITE_UPDATE",
    "WRITE_DELETE", "WRITE_INVOICE", "FEATURE_QUESTION", "SMALL_TALK", "UNKNOWN",
}

# How a fresh intent combines with whatever flow the user is currently in.
# Only meaningful when current_flow != IDLE.
FlowCompat = Literal[
    "FLOW_RESPONSE",     # user is answering the pending prompt
    "SIDE_QUESTION",     # read-only ask; answer inline, stay in flow
    "NEW_FLOW",          # user wants to start a different operation
    "CANCEL",            # user wants out ('skip', 'cancel', 'nevermind')
]

VALID_FLOW_COMPAT = {"FLOW_RESPONSE", "SIDE_QUESTION", "NEW_FLOW", "CANCEL"}


class Verdict(TypedDict):
    intent: Intent
    parameters: Dict[str, Any]
    confidence: float
    raw_message: str
    historical: bool
    bulk: bool
    # flow_compatible is None when the user was IDLE at classification time.
    # When set, it tells the dispatcher how to combine the new intent with the
    # active flow (push/pop, treat as response, cancel, etc.).
    flow_compatible: Optional[FlowCompat]


def _flow_compat_block(current_flow: Optional[str], current_context: Optional[Dict[str, Any]]) -> str:
    """Return prompt block describing the active flow (if any) and how the
    classifier should set flow_compatible. Empty string when IDLE."""
    if not current_flow or current_flow == "IDLE":
        return ""
    ctx_str = ""
    if current_context:
        try:
            ctx_str = json.dumps(current_context, default=str)[:300]
        except Exception:
            ctx_str = str(current_context)[:300]
    # Per-flow guidance: what counts as a FLOW_RESPONSE vs CANCEL.
    per_flow = {
        "INVOICE_AWAIT_SEND_CONFIRM": (
            "  - FLOW_RESPONSE: user is answering yes/no to 'should I email this invoice?'.\n"
            "    Treat 'yes', 'yep', 'sure', 'send it', 'go ahead', 'confirm' as FLOW_RESPONSE.\n"
            "    Treat 'no', 'nope', 'skip', 'cancel', 'don't send', 'not now' as CANCEL.\n"
        ),
        "INVOICE_NEED_BILLING": (
            "  - The bot just asked the user to provide CLIENT billing details (billing name,\n"
            "    address, GST). Any free-text reply describing those details is FLOW_RESPONSE.\n"
            "  - 'skip', 'cancel', 'no', 'none', 'don't have' = CANCEL.\n"
            "  - A clearly unrelated question (e.g. 'what was my last fee') is SIDE_QUESTION.\n"
        ),
        "INVOICE_NEED_POC_NAME": (
            "  - The bot just asked for the POC NAME on the invoice. Any short text that looks\n"
            "    like a person's name is FLOW_RESPONSE.\n"
            "  - 'skip', 'cancel', 'no', 'none', 'use the brand' = CANCEL.\n"
        ),
        "INVOICE_NEED_POC_EMAIL": (
            "  - The bot just asked for the client's contact EMAIL. Any email-looking string\n"
            "    (contains '@' and a domain) is FLOW_RESPONSE.\n"
            "  - 'skip', 'cancel', 'no', 'nevermind', \"don't have\" = CANCEL.\n"
        ),
        "SMART_CAPTURE_NEED_DESCRIPTION": (
            "  - The bot is waiting for a free-text JOB DESCRIPTION (brand, date, fees, client,\n"
            "    POC name, POC email). Any text that contains job-like fields is FLOW_RESPONSE.\n"
            "  - 'cancel', 'nevermind', 'drop it', 'never mind' = CANCEL.\n"
            "  - A clearly unrelated question (starts with who/what/show/list/etc., or contains\n"
            "    a '?') is SIDE_QUESTION — DO NOT classify those as FLOW_RESPONSE. The bot will\n"
            "    answer inline and prompt the user again for the job description.\n"
        ),
        "SMART_CAPTURE_CONFIRM_PENDING": (
            "  - The bot just showed an extracted-job confirmation card with 'Save this job?\n"
            "    (Yes / Edit)'. FLOW_RESPONSE for: 'yes', 'save', 'edit', 'no', or any text\n"
            "    that supplies missing fields (e.g. 'fee 4500', 'date 12 Mar').\n"
            "  - 'cancel', 'drop it', 'nevermind' = CANCEL.\n"
        ),
    }.get(current_flow, "")
    return (
        "\n\nACTIVE FLOW (the bot just asked a question and is waiting):\n"
        f"  current_flow: {current_flow}\n"
        f"  context:      {ctx_str}\n"
        "\n"
        "MUST set the 'flow_compatible' field to one of:\n"
        "  FLOW_RESPONSE - user is directly answering the bot's pending question.\n"
        "  CANCEL        - user wants out (any natural 'skip / cancel / nevermind' phrasing).\n"
        "  SIDE_QUESTION - user asks an unrelated READ_QUERY / READ_AGGREGATE / FEATURE_QUESTION\n"
        "                  that does NOT advance the current flow. Bot will answer inline\n"
        "                  and stay in the flow.\n"
        "  NEW_FLOW      - user is starting an unrelated WRITE_* operation (e.g. logging a\n"
        "                  new job, generating a different invoice). Bot may push/swap.\n"
        "\n"
        f"{per_flow}"
        "If unsure: prefer FLOW_RESPONSE over SIDE_QUESTION (the bot just asked a question;\n"
        "most replies are answers). Prefer SIDE_QUESTION over NEW_FLOW for any READ intent.\n"
    )


def _build_prompt(
    message: str,
    schema_summary: str,
    features_doc: str,
    conversation_history: Optional[List[Dict[str, str]]] = None,
    current_flow: Optional[str] = None,
    current_context: Optional[Dict[str, Any]] = None,
) -> str:
    recent = ""
    if conversation_history:
        lines = []
        for m in conversation_history[-4:]:
            role = "User" if m.get("role") == "user" else "You"
            content = (m.get("content") or "").strip()
            if content:
                lines.append(f"{role}: {content[:180]}")
        if lines:
            recent = "RECENT CHAT:\n" + "\n".join(lines) + "\n\n"

    feat_block = f"FEATURE CATALOG (truth source for FEATURE_QUESTION and UNKNOWN):\n{features_doc}\n\n" if features_doc else ""

    flow_block = _flow_compat_block(current_flow, current_context)
    flow_field_line = (
        '  "flow_compatible": one of [FLOW_RESPONSE, CANCEL, SIDE_QUESTION, NEW_FLOW]\n'
        '                     (required when ACTIVE FLOW is set below, null otherwise)\n'
    ) if flow_block else (
        '  "flow_compatible": null  (no active flow)\n'
    )

    return (
        "You are Remyndly's intent classifier. The user just sent a WhatsApp/Telegram message.\n"
        "Return ONLY a JSON Verdict matching the schema below. No prose, no markdown.\n\n"
        "VERDICT SCHEMA:\n"
        "{\n"
        '  "intent":      one of [READ_QUERY, READ_AGGREGATE, WRITE_CREATE, WRITE_UPDATE,\n'
        '                         WRITE_DELETE, WRITE_INVOICE, FEATURE_QUESTION,\n'
        '                         SMALL_TALK, UNKNOWN],\n'
        '  "parameters":  object — intent-specific (client_name, month, year, fees, etc.).\n'
        '                 Use null for unknown values. Never invent.\n'
        '  "confidence":  number 0.0–1.0,\n'
        '  "historical":  true ONLY if user asks about a PREVIOUS / OLD value\n'
        '                 ("what was the EARLIER fee on X", "the amount BEFORE we changed it"),\n'
        '  "bulk":        true ONLY if user said "all" / "every" with a write intent\n'
        '                 ("delete all Nike jobs", "mark all paid"),\n'
        f"{flow_field_line}"
        "}\n\n"
        "INTENT DEFINITIONS:\n"
        "- READ_QUERY: user wants to see specific job(s) or fields.\n"
        '    examples: "show my last 5 jobs", "what was the last fee on Garnier",\n'
        '              "who got invoices so far", "jobs older than 30 days"\n'
        "    parameters: {client_name?, brand_name?, month?, year?, field?, time_range?}\n\n"
        "- READ_AGGREGATE: user wants count/sum/avg/min/max.\n"
        '    examples: "total billing this quarter", "how many jobs this month",\n'
        '              "average fee per client"\n'
        "    parameters: {metric, column?, time_range?, group_by?}\n\n"
        "- WRITE_CREATE: user wants to LOG A NEW JOB. Requires at least one CONCRETE\n"
        "  field signal: a number, a date, a '+Client' prefix, or an explicit\n"
        '  "add a job for X" / "log a job" phrase. NEVER classify a question as\n'
        "  WRITE_CREATE just because it mentions 'job' or 'client'.\n"
        '    examples: "+Nike, dubbing, 5000", "add a job for Bisleri, 2 Feb, 15k",\n'
        '              "log a job"\n'
        "    parameters: {client_name?, brand_name?, job_date?, fees?, description?, poc_name?, poc_email?}\n\n"
        "- WRITE_UPDATE: modify an existing job's field.\n"
        '    examples: "mark Bisleri job paid", "change Nike fee to 7000",\n'
        '              "update POC email for Garnier to ash@brand.com"\n'
        "    parameters: {client_name?, field?, new_value?}\n\n"
        "- WRITE_DELETE: soft-delete one or more jobs.\n"
        '    examples: "delete my last job", "delete all Nike jobs", "remove this entry"\n'
        "    parameters: {client_name?, scope: 'last'|'this'|'all'|'specific'}\n\n"
        "- WRITE_INVOICE: generate/send a PDF invoice.\n"
        '    examples: "generate invoice for Bisleri", "send invoice for Nike for March",\n'
        '              "regenerate invoice for X" (set parameters.force_regenerate=true)\n'
        "    parameters: {client_name?, month?, year?, force_regenerate?}\n\n"
        "- FEATURE_QUESTION: user asks what Remyndly can do, how to do X,\n"
        "  or whether a feature is supported.\n"
        '    examples: "can you do OCR", "how do I update my bank details",\n'
        '              "do you support recurring invoices"\n'
        "    parameters: {}\n\n"
        "- SMALL_TALK: greetings, thanks, idle acknowledgements with no operational ask.\n"
        '    examples: "hi", "thanks!", "good morning", "are you back?"\n'
        "    parameters: {kind: 'greeting'|'thanks'|'check_in'|'other'}\n\n"
        "- UNKNOWN: anything off-topic, gibberish, or genuinely unmappable.\n"
        '    examples: "tell me a joke", "what\'s the weather", random text\n'
        "    parameters: {}\n\n"
        "RULES:\n"
        "- If unsure between two intents, prefer READ over WRITE (writes are destructive).\n"
        "- confidence < 0.5 → UNKNOWN. Do NOT write a custom clarification text;\n"
        "  the app picks an on-brand reply.\n"
        "- Never invent column names; reference only the schema below.\n"
        "- A message can mention 'invoice' or 'bill' without being WRITE_INVOICE\n"
        '  (e.g. "who got invoices" is READ_QUERY, "total invoices last month" is READ_AGGREGATE).\n'
        "- FORBIDDEN PARAMETER VALUES — never put any of these in `field`, `column`,\n"
        "  or `filters` keys/values: 'bill_sent', 'invoice_sent', 'sent', 'is_sent',\n"
        "  'paid_status' (use 'paid'), 'amount' (use 'fees'). These columns DO NOT EXIST.\n"
        "  Semantic mappings the planner (next stage) understands — use these instead:\n"
        "    'invoice sent / billed / invoiced clients' → set parameters.field = 'invoice_date'\n"
        "      and the planner will filter where invoice_date IS NOT NULL.\n"
        "    'unpaid / pending' → parameters.field = 'paid' (planner handles NULL/empty logic).\n"
        "    'how much / amount / earnings' → parameters.field = 'fees'.\n\n"
        f"{feat_block}"
        f"SCHEMA SUMMARY:\n{schema_summary}\n\n"
        f"{recent}"
        f"{flow_block}"
        f"USER MESSAGE: {message}\n\n"
        "Your JSON Verdict:"
    )


def _parse_verdict(raw: str, message: str) -> Optional[Verdict]:
    """Strip code fences, parse JSON, coerce field types, validate intent."""
    if not raw:
        return None
    text = raw.strip()
    # Strip markdown code fences if Gemini added them
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)
    text = text.strip()
    try:
        data = json.loads(text)
    except Exception as e:
        logger.warning(f"[CLASSIFIER] JSON parse failed: {e} | raw={raw[:200]!r}")
        return None
    intent = str(data.get("intent") or "").upper().strip()
    if intent not in VALID_INTENTS:
        logger.warning(f"[CLASSIFIER] Invalid intent {intent!r} — coercing to UNKNOWN")
        intent = "UNKNOWN"
    try:
        confidence = float(data.get("confidence") or 0.0)
    except Exception:
        confidence = 0.0
    confidence = max(0.0, min(1.0, confidence))
    params = data.get("parameters")
    if not isinstance(params, dict):
        params = {}
    fc_raw = data.get("flow_compatible")
    if isinstance(fc_raw, str):
        fc_up = fc_raw.upper().strip()
        flow_compatible = fc_up if fc_up in VALID_FLOW_COMPAT else None
    else:
        flow_compatible = None
    return Verdict(
        intent=intent,       # type: ignore[arg-type]
        parameters=params,
        confidence=confidence,
        raw_message=message,
        historical=bool(data.get("historical")),
        bulk=bool(data.get("bulk")),
        flow_compatible=flow_compatible,   # type: ignore[arg-type]
    )


def classify(
    message: str,
    gemini,
    conversation_history: Optional[List[Dict[str, str]]] = None,
    schema_summary: str = "",
    current_flow: Optional[str] = None,
    current_context: Optional[Dict[str, Any]] = None,
) -> Optional[Verdict]:
    """
    Single Gemini call that returns a Verdict.
    Returns None if the call fails or output is unparseable — caller MUST
    fall back to the legacy code path in that case.

    Pass `current_flow` (e.g. "INVOICE_AWAIT_SEND_CONFIRM") + `current_context`
    when the user is in a v2-owned flow; the classifier will then set
    `flow_compatible` so the dispatcher can route correctly.
    """
    if not message or not message.strip():
        return None
    try:
        gemini._ensure_initialized()
        if not gemini._initialized or not gemini.api_key:
            return None
    except Exception:
        return None

    features_doc = ""
    try:
        features_doc = gemini._load_features_doc() or ""
        if len(features_doc) > 8000:
            features_doc = features_doc[:8000]
    except Exception:
        pass

    prompt = _build_prompt(
        message, schema_summary or "", features_doc, conversation_history,
        current_flow=current_flow, current_context=current_context,
    )
    try:
        raw = gemini._call_api(
            prompt,
            generation_config={"temperature": 0.0, "maxOutputTokens": 300},
        )
    except Exception as e:
        logger.warning(f"[CLASSIFIER] _call_api failed: {e}")
        return None
    verdict = _parse_verdict(raw or "", message)
    if verdict:
        logger.info(
            f"[CLASSIFIER] intent={verdict['intent']} "
            f"conf={verdict['confidence']:.2f} "
            f"hist={verdict['historical']} bulk={verdict['bulk']} "
            f"fc={verdict.get('flow_compatible')} "
            f"params={json.dumps(verdict['parameters'], default=str)[:160]}"
        )
    return verdict
