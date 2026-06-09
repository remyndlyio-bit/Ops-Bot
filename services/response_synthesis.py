"""
Clean structured payload builder for AI synthesis.
Transforms DB results into safe JSON, stripping internal/technical fields.
"""

import json
from typing import List, Dict, Any, Optional

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
