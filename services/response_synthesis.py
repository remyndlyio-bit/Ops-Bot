"""
Clean structured payload builder for AI synthesis.
Transforms DB results into safe JSON, stripping internal/technical fields.
"""

import json
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional

from services.answer_ledger import format_inr, describe_scope

# Fields to strip before sending to AI (technical, internal)
INTERNAL_FIELDS = {
    "id", "created_at", "first_reminder_sent", "second_reminder_sent",
    "third_reminder_sent", "_row", "_full_rows",
}

# Human-friendly field name mapping (optional; AI can use original names)
FIELD_ALIASES = {
    "job_description_details": "job_description",
    "fees": "fee",
    "paid": "payment_status",
    "bill_no": "invoice_number",
}


def _clean_value(v: Any) -> Any:
    """Serialize date/datetime to ISO string; pass through scalars. None/pd.NaT → None."""
    if v is None:
        return None
    try:
        import pandas as pd
        if pd.isna(v):
            return None
    except ImportError:
        pass
    if hasattr(v, "isoformat"):
        s = getattr(v, "isoformat", lambda: None)()
        if s is None or s == "NaT" or not str(s).strip():
            return None
        return s[:10] if len(str(s)) >= 10 else s  # date only for readability
    if isinstance(v, (dict, list)):
        return v
    return v


def _clean_row(row: Dict) -> Dict:
    """Strip internal fields; alias keys; serialize values. Omit nulls for compact payload."""
    out = {}
    for k, v in row.items():
        k_lower = str(k).lower().strip()
        if k_lower in INTERNAL_FIELDS or k.startswith("_"):
            continue
        v_clean = _clean_value(v)
        if v_clean is None:
            # Special case: keep payment status semantically — null paid means unpaid.
            # Without this, "who hasn't paid" returns rows with no payment_status,
            # and the synthesizer says "I can't see payment statuses".
            if k_lower == "paid":
                out[FIELD_ALIASES.get("paid", "paid")] = "unpaid"
            continue
        # Normalize paid values for clarity
        if k_lower == "paid":
            sv = str(v_clean).strip().lower()
            if sv in ("yes", "true", "1", "y", "paid"):
                v_clean = "paid"
            elif sv in ("no", "false", "0", "n", "unpaid", ""):
                v_clean = "unpaid"
        alias = FIELD_ALIASES.get(k_lower, k)
        out[alias] = v_clean
    return out


def build_clean_payload(rows: List[Dict], operation: str = "select") -> Dict[str, Any]:
    """
    Transform DB result into safe structured payload for AI synthesis.
    Returns dict suitable for JSON serialization.
    """
    if not rows:
        return {"type": "empty", "data": None}

    if operation == "insert":
        return {"type": "insert_confirmation", "data": {"inserted": True}}

    # Aggregate detection must happen BEFORE _clean_row, which drops nulls.
    # The query planner aliases SUM/COUNT/AVG/etc. as "result"; we also accept
    # generic keys like count/sum/total. When the aggregate value is NULL
    # (no matching rows), surface that explicitly so the synthesizer says
    # "₹0 / no records this period" instead of "I can't see anything".
    _AGG_KEYS = ("count", "sum", "total", "avg", "average", "min", "max", "result")
    if len(rows) == 1:
        orig_keys = [str(k).lower() for k in rows[0].keys()]
        if any(c in orig_keys for c in _AGG_KEYS):
            raw_row = rows[0]
            # GROUP BY result: row has both a dimension column (e.g. client_name)
            # and an aggregate — don't drop the dimension. Route as job_summary so
            # the synthesizer sees the full row ("biggest client is X: ₹5L").
            _non_agg_keys = [k for k in orig_keys if k not in _AGG_KEYS]
            if _non_agg_keys:
                cleaned_row = _clean_row(raw_row)
                return {"type": "job_summary", "data": cleaned_row}
            agg_val = None
            for k in raw_row:
                if str(k).lower() in _AGG_KEYS:
                    agg_val = raw_row[k]
                    break
            if agg_val is None:
                return {
                    "type": "aggregate",
                    "data": {"result": 0},
                    "note": "zero",
                }
            return {"type": "aggregate", "data": {"result": agg_val}}

    cleaned = [_clean_row(r) for r in rows]

    if len(cleaned) == 1:
        row = cleaned[0]
        if len(row) == 1:
            field_name = list(row.keys())[0]
            return {
                "type": "field_answer",
                "field_name": field_name,
                "value": row[field_name],
                "related_context": {},
            }
        return {"type": "job_summary", "data": row}

    return {"type": "multi_record", "data": cleaned[:20], "total_count": len(rows)}


def build_field_answer_payload(
    field_name: str,
    value: Any,
    full_row: Dict,
) -> Dict[str, Any]:
    """
    Build structured payload for follow-up field extraction.
    Used when user asks for a single field from the last result (e.g. "what was the client?")
    related_context gives AI optional context for natural phrasing.
    """
    # Build related_context from other non-null fields (exclude the asked field).
    # Always include notes so Gemini can read change history for "earlier value" questions.
    related = _clean_row({k: v for k, v in full_row.items() if str(k).lower() != str(field_name).lower()})
    notes_val = related.pop("notes", None)
    related_context = dict(list(related.items())[:6])
    if notes_val:
        related_context["notes"] = notes_val
    v_clean = _clean_value(value)
    return {
        "type": "field_answer",
        "field_name": FIELD_ALIASES.get(str(field_name).lower(), field_name),
        "value": v_clean,
        "related_context": related_context,
    }


# ══════════════════════════════════════════════════════════════════════════
# ASSISTANT_PLAN.md WP-4 — the Answer contract.
#
# Today's answers get a bare number or a raw job-card dump with no summary
# and no indication of scope (the "said earnings, meant unpaid" class of
# confusion this whole hardening effort traces back to). AnswerPayload fixes
# that by CONSTRUCTION: headline + scope_note are built deterministically
# from the same {filters, time_range} shape WP-1's AnswerLedger already
# stores (reusing answer_ledger.describe_scope/format_inr rather than a
# third implementation of "describe filters in English" and "format ₹"), so
# the spoken scope can never disagree with what actually ran.
# ══════════════════════════════════════════════════════════════════════════

_MAX_SUPPORT_ROWS = 3
_MAX_RENDER_CHARS = 900


def _row_is_full_job(row: Dict) -> bool:
    """Same test as intent_service._is_full_job_row, duplicated (not
    imported) to avoid a response_synthesis <-> intent_service import cycle
    — intent_service already imports this module."""
    return "bill_no" in row or "job_date" in row


def _support_row(row: Dict) -> Dict[str, Any]:
    client = (row.get("client_name") or row.get("brand_name")
              or row.get("production_house") or "—")
    fees = row.get("fees")
    try:
        amount = f"₹{format_inr(int(float(fees)))}" if fees is not None else "—"
    except (TypeError, ValueError):
        amount = str(fees) if fees else "—"
    paid_raw = str(row.get("paid") or "").strip().lower()
    paid = "paid" if paid_raw in ("yes", "true", "1", "y", "paid") else "unpaid"
    bill_sent_raw = str(row.get("bill_sent") or "").strip().lower()
    bill_sent = "invoiced" if bill_sent_raw in ("yes", "true", "1", "y", "sent") else "not invoiced"
    return {"client": client, "bill_no": row.get("bill_no") or "—",
            "amount": amount, "paid": paid, "bill_sent": bill_sent}


# Deterministic follow-up suggestions keyed on plan shape — NOT an LLM call.
# Order matters: first matching rule wins.
def _suggest_followup(filters: Dict[str, Any], metric: Optional[str]) -> Optional[str]:
    if filters.get("paid") == "no":
        return "Want me to send reminders for these?"
    if filters.get("bill_sent") == "no":
        return "Want to generate invoices for these?"
    if filters.get("client_name") and "paid" not in filters:
        return "Want their payment status too?"
    if not filters and metric in ("sum", None):
        return "Want the paid vs. unpaid split?"
    return None


@dataclass
class AnswerPayload:
    headline: str
    scope_note: str
    support: List[Dict[str, Any]] = field(default_factory=list)
    remainder: int = 0
    followup: Optional[str] = None


def build_answer_payload(*, scope: Dict[str, Any], metric: Optional[str],
                          rows: List[Dict], group_by: Optional[str] = None) -> AnswerPayload:
    """Construct an AnswerPayload from the SAME filters/time_range/metric that
    produced the SQL (scope: {"filters": {...}, "time_range": {...}|None} —
    the exact shape answer_ledger.LedgerEntry.scope already uses) plus the
    rows it returned. Never calls an LLM; safe to build on every read."""
    filters = (scope or {}).get("filters") or {}
    scope_note = describe_scope(scope)
    job_rows = [r for r in rows if _row_is_full_job(r)]

    if group_by == "client_name" and rows:
        top = rows[0]
        client = top.get("client_name") or top.get("brand_name") or "—"
        headline = str(client)
    elif metric == "sum":
        val = rows[0].get("result") if rows else 0
        headline = f"₹{format_inr(int(val or 0))}"
    elif metric == "avg":
        val = rows[0].get("result") if rows else 0
        headline = f"₹{format_inr(int(val or 0))} average"
    elif metric == "count":
        val = int((rows[0].get("result") if rows else 0) or 0)
        headline = f"{val} job{'s' if val != 1 else ''}"
    else:
        n = len(job_rows) if job_rows else len(rows)
        headline = f"{n} result{'s' if n != 1 else ''}"

    support = [_support_row(r) for r in job_rows[:_MAX_SUPPORT_ROWS]]
    remainder = max(0, len(job_rows) - _MAX_SUPPORT_ROWS) if job_rows else 0

    return AnswerPayload(
        headline=headline, scope_note=scope_note, support=support,
        remainder=remainder, followup=_suggest_followup(filters, metric),
    )


def render_answer_payload(payload: AnswerPayload) -> str:
    """Deterministic (no LLM) text rendering of an AnswerPayload. Used as the
    guaranteed-quality fallback when LLM synthesis returns empty, and as the
    direct renderer for status/filtered queries that would otherwise dump raw
    job cards (Invoice No: / Invoice Date: fields) for a scoped question —
    the exact shape of the IMG-3 confusion this fixes: a raw card dump with
    no indication of what was actually asked or answered."""
    lines = [f"{payload.headline} — {payload.scope_note}."]
    for r in payload.support:
        lines.append(f"• {r['client']} · {r['bill_no']} · {r['amount']} · {r['paid']} · {r['bill_sent']}")
    if payload.remainder:
        lines.append(f"(+{payload.remainder} more)")
    if payload.followup:
        lines.append(payload.followup)
    text = "\n".join(lines)
    if len(text) > _MAX_RENDER_CHARS:
        text = text[: _MAX_RENDER_CHARS - 1].rstrip() + "…"
    return text
