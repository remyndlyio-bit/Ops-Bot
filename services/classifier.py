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


class Verdict(TypedDict):
    intent: Intent
    parameters: Dict[str, Any]
    confidence: float
    raw_message: str
    historical: bool
    bulk: bool


def _build_prompt(
    message: str,
    schema_summary: str,
    features_doc: str,
    conversation_history: Optional[List[Dict[str, str]]] = None,
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
        '                 ("delete all Nike jobs", "mark all paid")\n'
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
        '  (e.g. "who got invoices" is READ_QUERY, "total invoices last month" is READ_AGGREGATE).\n\n'
        f"{feat_block}"
        f"SCHEMA SUMMARY:\n{schema_summary}\n\n"
        f"{recent}"
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
    return Verdict(
        intent=intent,       # type: ignore[arg-type]
        parameters=params,
        confidence=confidence,
        raw_message=message,
        historical=bool(data.get("historical")),
        bulk=bool(data.get("bulk")),
    )


def classify(
    message: str,
    gemini,
    conversation_history: Optional[List[Dict[str, str]]] = None,
    schema_summary: str = "",
) -> Optional[Verdict]:
    """
    Single Gemini call that returns a Verdict.
    Returns None if the call fails or output is unparseable — caller MUST
    fall back to the legacy code path in that case.
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

    prompt = _build_prompt(message, schema_summary or "", features_doc, conversation_history)
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
            f"params={json.dumps(verdict['parameters'], default=str)[:160]}"
        )
    return verdict
