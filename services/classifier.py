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
    "AUDIT_REPLY",
    "FEATURE_QUESTION",
    "SMALL_TALK",
    "UNKNOWN",
]

VALID_INTENTS = {
    "READ_QUERY", "READ_AGGREGATE", "WRITE_CREATE", "WRITE_UPDATE",
    "WRITE_DELETE", "WRITE_INVOICE", "AUDIT_REPLY", "FEATURE_QUESTION", "SMALL_TALK", "UNKNOWN",
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
    # ── ASSISTANT_PLAN.md WP-2 additions ────────────────────────────────
    # references_last_answer: true when the message is asking about the
    # SCOPE or composition of the answer the bot JUST gave ("do these
    # include paid and unpaid?", "is that only Nike?") rather than a new
    # data request. Distinct from `historical` (which means "a PAST value",
    # e.g. "what was the fee BEFORE we changed it") — a references_last_answer
    # message wants clarification about a value already on screen, not a
    # different value from the past. The two are not mutually exclusive but
    # usually are.
    references_last_answer: bool
    # resolved_query: for a READ_QUERY/READ_AGGREGATE whose entities are
    # INHERITED from context ("what about this month?", "the first one"),
    # the fully resolved shape the planner can consume directly —
    # {"client_name": str|None, "time_range": dict|None, "metric_hint":
    # "sum"|"count"|"avg"|"list"|None}. None when the message is
    # self-contained (nothing to inherit) or not a READ intent.
    resolved_query: Optional[Dict[str, Any]]


def _idle_context_block(current_context: Optional[Dict[str, Any]]) -> str:
    """Return prompt guidance for IDLE-state context signals (audit_pending, etc.)."""
    if not current_context:
        return ""
    audit_pending = current_context.get("audit_pending", False)
    if not audit_pending:
        return ""
    return (
        "ACTIVE CONTEXT: The bot recently sent a payment audit reminder with a list\n"
        "of unpaid invoices. The user may be replying to that prompt.\n"
        "- If the user's message is a SHORT, IMPERATIVE reply to the audit\n"
        "  (e.g. 'paid', 'paid 2', 'all paid', 'later', 'remind me later'),\n"
        "  classify as AUDIT_REPLY.\n"
        "- REJECT anything question-shaped (contains '?' or words like 'how', 'do',\n"
        "  'what', 'can') — questions are not audit replies, they're new queries.\n"
        "  Examples of REJECTS: 'how many have paid?' (READ_AGGREGATE),\n"
        "  'do these include paid ones?' (READ_QUERY).\n\n"
    )


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
        "DISAMBIGUATION": (
            "  - The bot showed a NUMBERED LIST of matching records and asked which one the\n"
            "    user meant (or 'all' to act on every match, or 'cancel' to abort).\n"
            "  - FLOW_RESPONSE: a bare number (e.g. '2', '3'), 'all', 'all of them', or an\n"
            "    explicit bulk confirmation like 'delete all' / 'yes' ONLY when the bot's\n"
            "    prompt was itself a bulk-action confirmation (not a numbered pick).\n"
            "  - CANCEL: 'cancel', 'stop', 'nevermind', 'abort', 'no', or any natural phrasing\n"
            "    of backing out.\n"
            "  - SIDE_QUESTION: ANY genuinely different question — including a question about\n"
            "    the SCOPE of an earlier answer (e.g. 'does that include paid and unpaid?',\n"
            "    'is that only Nike?') — must NOT be read as picking a numbered option, even\n"
            "    if it happens to contain a digit or the word 'all'. A message that is a\n"
            "    QUESTION (ends in '?', or starts with a question word like who/what/why/\n"
            "    is/does/did/how) is SIDE_QUESTION, never FLOW_RESPONSE, regardless of what\n"
            "    words it contains. This is the exact bug class this flow was migrated to\n"
            "    fix — a fragile keyword/regex guard previously misread ordinary questions\n"
            "    as replies to a pending prompt.\n"
        ),
        "BANK_DETAILS": (
            "  - The bot asked the user to send their OWN bank details (account name,\n"
            "    bank name, account number, IFSC, optional UPI) in one structured message.\n"
            "  - FLOW_RESPONSE: any message that looks like it's supplying those fields\n"
            "    (contains words like account/bank/IFSC/UPI, or a mix of digits and labels).\n"
            "  - CANCEL: 'cancel', 'stop', 'nevermind', 'skip', or similar.\n"
            "  - SIDE_QUESTION: a genuinely unrelated question (including one about a PRIOR\n"
            "    answer's scope) is SIDE_QUESTION, never FLOW_RESPONSE.\n"
        ),
        "NAME_CHANGE": (
            "  - The bot asked what the user's new display name should be.\n"
            "  - FLOW_RESPONSE: any short text that reads as a name (1-4 words, no '?').\n"
            "  - CANCEL: 'cancel', 'nevermind', 'no', or similar.\n"
            "  - SIDE_QUESTION: a genuinely unrelated question is SIDE_QUESTION, never\n"
            "    FLOW_RESPONSE — even a short one.\n"
        ),
        "LINK_ACCOUNT": (
            "  - The bot asked for the user's ID from the OTHER platform (Telegram/WhatsApp)\n"
            "    to link accounts.\n"
            "  - FLOW_RESPONSE: a numeric ID (5+ digits) or a 'whatsapp:+...' string.\n"
            "  - CANCEL: 'cancel', 'nevermind', 'no', or similar.\n"
            "  - SIDE_QUESTION: a genuinely unrelated question is SIDE_QUESTION, never\n"
            "    FLOW_RESPONSE, even if it happens to contain a number.\n"
        ),
        "INVOICE_ADDRESS": (
            "  - The bot asked for the user's OWN business address, for the invoice header\n"
            "    (either mid invoice-generation, or a standalone 'update my address' ask).\n"
            "  - FLOW_RESPONSE: any free text that reads as an address (a location, multiple\n"
            "    lines, a pincode, etc.).\n"
            "  - CANCEL: 'cancel', 'stop', 'abort', 'nevermind'.\n"
            "  - SIDE_QUESTION: a genuinely unrelated question is SIDE_QUESTION, never\n"
            "    FLOW_RESPONSE.\n"
        ),
        "INVOICE_NEED_JOB_DESCRIPTION": (
            "  - The bot asked what the WORK WAS for one specific job that's missing a\n"
            "    description, before it can generate the invoice (e.g. '2 master films,\n"
            "    English VO').\n"
            "  - FLOW_RESPONSE: any free text describing work done.\n"
            "  - CANCEL: 'cancel', 'stop', 'nevermind', 'abort'.\n"
            "  - SIDE_QUESTION: a genuinely unrelated question — including one about a PRIOR\n"
            "    answer's scope — is SIDE_QUESTION, never FLOW_RESPONSE.\n"
        ),
        "INVOICE_READINESS_POC_EMAIL": (
            "  - The bot asked what EMAIL the invoice/reminders for a client should go to,\n"
            "    BEFORE generating the invoice (distinct from the SEND-time email prompt).\n"
            "  - FLOW_RESPONSE: any email-looking string (contains '@' and a domain).\n"
            "  - CANCEL: 'cancel', 'stop', 'abort', 'nevermind'.\n"
            "  - SIDE_QUESTION: a genuinely unrelated question is SIDE_QUESTION, never\n"
            "    FLOW_RESPONSE.\n"
        ),
        "INVOICE_NEED_MONTH": (
            "  - The bot asked WHICH MONTH an invoice request should cover (no month was\n"
            "    given in the original request).\n"
            "  - FLOW_RESPONSE: any text naming a month (e.g. 'March', 'last month', 'Feb 2025').\n"
            "  - CANCEL: 'cancel', 'stop', 'abort', 'nevermind'.\n"
            "  - SIDE_QUESTION: a genuinely unrelated question is SIDE_QUESTION, never\n"
            "    FLOW_RESPONSE.\n"
        ),
        "COMPOUND_RESPONSE": (
            "  - The bot just asked 'You also mentioned: X. Want me to do that now? (Yes / No)'\n"
            "    after saving a job that had a second, compound action mentioned in the same\n"
            "    message (e.g. 'add a job for Nike AND send the invoice').\n"
            "  - FLOW_RESPONSE: 'yes'/'yep'/'sure'/'ok' (optionally with a qualifier after it,\n"
            "    e.g. 'yes, include bill numbers') or 'no'/'nope'/'skip'/'not now'/'later'.\n"
            "  - CANCEL: treat the same as a 'no' answer -- there's nothing else pending to\n"
            "    cancel once this is declined.\n"
            "  - NEW_FLOW: a message that reads as a completely different, unrelated request\n"
            "    (not a yes/no answer) is NEW_FLOW, not FLOW_RESPONSE -- the bot should drop\n"
            "    this offer and just handle the new request.\n"
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


def _ledger_block(ledger_entries: Optional[List[Any]]) -> str:
    """Render the last up-to-3 AnswerLedger entries as compact one-liners —
    NEVER raw JSON (same discipline as KnowledgeBook's examples_block: a raw
    JSON blob in the prompt gets echoed/garbled by the model instead of
    taught from). Each entry carries the SCOPE (filters + time_range) the
    prior answer was actually computed under, straight from the plan/SQL
    that ran — this is what lets the classifier recognise "do these include
    paid and unpaid?" as a question about a SPECIFIC prior number instead of
    guessing from the raw text alone.

    Returns "" when there's no ledger yet (nothing to render).
    """
    if not ledger_entries:
        return ""
    lines = ["RECENT ANSWERS (most recent last — what the bot just told the user):"]
    for e in ledger_entries[-3:]:
        scope = e.scope or {}
        filters = scope.get("filters") or {}
        f_str = ", ".join(f"{k}={v}" for k, v in filters.items()) or "none"
        tr = scope.get("time_range")
        tr_str = "all-time" if not tr else str((tr or {}).get("value"))[:60]
        lines.append(
            f'  - "{e.question[:80]}" -> {e.kind} value={e.value!r} '
            f"filters=[{f_str}] time_range={tr_str}"
        )
    return "\n".join(lines) + "\n\n"


def _build_prompt(
    message: str,
    schema_summary: str,
    features_doc: str,
    conversation_history: Optional[List[Dict[str, str]]] = None,
    current_flow: Optional[str] = None,
    current_context: Optional[Dict[str, Any]] = None,
    ledger_entries: Optional[List[Any]] = None,
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

    idle_block = _idle_context_block(current_context) if not current_flow or current_flow == "IDLE" else ""
    flow_block = _flow_compat_block(current_flow, current_context)
    flow_field_line = (
        '  "flow_compatible": one of [FLOW_RESPONSE, CANCEL, SIDE_QUESTION, NEW_FLOW]\n'
        '                     (required when ACTIVE FLOW is set below, null otherwise)\n'
    ) if flow_block else (
        '  "flow_compatible": null  (no active flow)\n'
    )

    ledger_block = _ledger_block(ledger_entries)

    return (
        "You are Remyndly's intent classifier. The user just sent a WhatsApp/Telegram message.\n"
        "Return ONLY a JSON Verdict matching the schema below. No prose, no markdown.\n\n"
        "VERDICT SCHEMA:\n"
        "{\n"
        '  "intent":      one of [READ_QUERY, READ_AGGREGATE, WRITE_CREATE, WRITE_UPDATE,\n'
        '                         WRITE_DELETE, WRITE_INVOICE, AUDIT_REPLY, FEATURE_QUESTION,\n'
        '                         SMALL_TALK, UNKNOWN],\n'
        '  "parameters":  object — intent-specific (client_name, month, year, fees, etc.).\n'
        '                 Use null for unknown values. Never invent.\n'
        '  "confidence":  number 0.0–1.0,\n'
        '  "historical":  true ONLY if user asks about a PREVIOUS / OLD value\n'
        '                 ("what was the EARLIER fee on X", "the amount BEFORE we changed it"),\n'
        '  "bulk":        true ONLY if user said "all" / "every" with a write intent\n'
        '                 ("delete all Nike jobs", "mark all paid"),\n'
        '  "references_last_answer": true ONLY if the message asks about the SCOPE or\n'
        '                 composition of the answer the bot JUST gave — "do these include\n'
        '                 paid and unpaid?", "is that only Nike?", "does that include this\n'
        '                 month?" — not a request for a different/new number. Requires a\n'
        "                 RECENT ANSWER to be present below; false if there isn't one.\n"
        '                 Distinct from "historical": historical asks about a PAST value\n'
        '                 ("what was the fee BEFORE"), this asks about a value ALREADY SHOWN.\n'
        '  "resolved_query": for a READ_QUERY/READ_AGGREGATE whose entities are INHERITED\n'
        '                 from context ("what about this month?", "the first one", "and last\n'
        '                 quarter?") — the FULLY RESOLVED filters, combining what the message\n'
        "                 states with what carries over from the RECENT ANSWERS / RECENT CHAT\n"
        "                 below. Shape:\n"
        '                   {"client_name": string|null, "time_range": object|null,\n'
        '                    "metric_hint": "sum"|"count"|"avg"|"list"|null}\n'
        "                 null when the message is already self-contained (nothing to\n"
        "                 inherit) or the intent isn't READ_QUERY/READ_AGGREGATE. NEVER\n"
        "                 invent a client or date that isn't stated or carried over.\n"
        f"{flow_field_line}"
        "}\n\n"
        "INTENT DEFINITIONS:\n"
        "- READ_QUERY: user wants to see specific job(s) or fields.\n"
        '    examples: "show my last 5 jobs", "what was the last fee on Garnier",\n'
        '              "who got invoices so far", "jobs older than 30 days",\n'
        '              "list my clients", "i don\'t remember client names" → no filters,\n'
        '                                                                   just return\n'
        '                                                                   distinct clients\n'
        '              "kiska invoice baki hai bhejna" / "pending invoices" / "yet to send"\n'
        '                  → parameters.field=\'bill_sent\' (pending = not yet sent)\n'
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
        "- WRITE_INVOICE: generate/send a PDF invoice — OR ask the bot to re-deliver\n"
        "  a previously-generated invoice (the cached PDF is reused automatically).\n"
        '    examples: "generate invoice for Bisleri", "send invoice for Nike for March",\n'
        '              "share me the invoice for Schbang", "give me the invoice you sent",\n'
        '              "send me the latest invoice for X", "show me the X invoice again",\n'
        '              "the invoice file for Nike", "what was the last invoice you sent"\n'
        "              (the bot has a cached invoice store; any 'give me / share / send me\n"
        "               the invoice for X' triggers the same flow and re-delivers from\n"
        "               cache — no regeneration unless force_regenerate=true).\n"
        '              "regenerate invoice for X" / "fresh copy" → force_regenerate=true\n'
        "    parameters: {client_name?, month?, year?, force_regenerate?}\n\n"
        "- AUDIT_REPLY: user is replying to a payment audit reminder (only when\n"
        "  there is a pending audit list). Replies must be imperative and short —\n"
        "  rejects anything question-shaped or elaborated.\n"
        '    examples: "paid", "paid 2", "all paid", "later", "remind me later",\n'
        '              "mark paid", "mark 1 paid"\n'
        '    counter-examples: "how many have paid?" → READ_AGGREGATE\n'
        '                      "do these jobs include paid ones?" → READ_QUERY (question\n'
        "                       about an unrelated earlier answer, not a reply to audit)\n"
        "    parameters: {}\n\n"
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
        "  or `filters` keys/values: 'invoice_sent', 'is_sent', 'paid_status' (use 'paid'),\n"
        "  'amount' (use 'fees'). These columns DO NOT EXIST.\n"
        "  Semantic mappings the planner (next stage) understands — use these instead:\n"
        "    'invoiced / has invoice / billed (without sent)' → parameters.field = 'invoice_date'.\n"
        "      Planner filters where invoice_date IS NOT NULL (the PDF was generated).\n"
        "    'sent / emailed / delivered the invoice / who got the invoice email' →\n"
        "      parameters.field = 'bill_sent'. The bill_sent column EXISTS (text type) and\n"
        "      tracks actual email delivery. DO NOT use 'invoice_date' for sent queries —\n"
        "      a generated PDF that was never emailed has invoice_date set but bill_sent NULL.\n"
        "    'when was the invoice sent / sent date / when did you email it' →\n"
        "      parameters.field = 'bill_sent_at'. This is a timestamptz column populated\n"
        "      automatically when the bot successfully emails an invoice. NULL means we\n"
        "      never sent it.\n"
        "    'unpaid / pending' → parameters.field = 'paid' (planner handles NULL/empty logic).\n"
        "    'how much / amount / earnings' → parameters.field = 'fees'.\n\n"
        f"{feat_block}"
        f"SCHEMA SUMMARY:\n{schema_summary}\n\n"
        f"{recent}"
        f"{ledger_block}"
        f"{idle_block}"
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

    resolved_query = data.get("resolved_query")
    if not isinstance(resolved_query, dict):
        resolved_query = None

    return Verdict(
        intent=intent,       # type: ignore[arg-type]
        parameters=params,
        confidence=confidence,
        raw_message=message,
        historical=bool(data.get("historical")),
        bulk=bool(data.get("bulk")),
        flow_compatible=flow_compatible,   # type: ignore[arg-type]
        references_last_answer=bool(data.get("references_last_answer")),
        resolved_query=resolved_query,
    )


def classify(
    message: str,
    gemini,
    conversation_history: Optional[List[Dict[str, str]]] = None,
    schema_summary: str = "",
    current_flow: Optional[str] = None,
    current_context: Optional[Dict[str, Any]] = None,
    ledger_entries: Optional[List[Any]] = None,
) -> Optional[Verdict]:
    """
    Single Gemini call that returns a Verdict.
    Returns None if the call fails or output is unparseable — caller MUST
    fall back to the legacy code path in that case.

    Pass `current_flow` (e.g. "INVOICE_AWAIT_SEND_CONFIRM") + `current_context`
    when the user is in a v2-owned flow; the classifier will then set
    `flow_compatible` so the dispatcher can route correctly.

    Pass `ledger_entries` (services.answer_ledger.LedgerEntry list, most
    recent last — e.g. answer_ledger.get_entries(user_mem)) so the classifier
    can set `references_last_answer` / `resolved_query` (WP-2). Omit and
    those fields simply come back False/None — never required.
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
        ledger_entries=ledger_entries,
    )
    try:
        raw = gemini._call_api(
            prompt,
            # WP-2 added two fields (references_last_answer, resolved_query)
            # to the JSON Verdict — bump the output budget so a resolved_query
            # object doesn't get truncated into invalid JSON (see the earlier,
            # unrelated lesson from ASSISTANT_PLAN.md-adjacent work: a live
            # e2e test failed at 700 tokens purely from budget, not model
            # behaviour, and passed clean at production's real 800+).
            generation_config={"temperature": 0.0, "maxOutputTokens": 400},
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
            f"ref_last={verdict.get('references_last_answer')} "
            f"resolved_query={json.dumps(verdict.get('resolved_query'), default=str)[:160]} "
            f"params={json.dumps(verdict['parameters'], default=str)[:160]}"
        )
    return verdict
