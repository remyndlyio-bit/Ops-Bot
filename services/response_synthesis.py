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
            continue
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

    cleaned = [_clean_row(r) for r in rows]

    if len(cleaned) == 1:
        row = cleaned[0]
        keys = [str(k).lower() for k in row.keys()]
        # Count/aggregate result
        if len(row) == 1 or (len(row) <= 2 and any(c in keys for c in ("count", "sum", "total"))):
            return {"type": "aggregate", "data": row}
        return {"type": "job_summary", "data": row}

    return {"type": "multi_record", "data": cleaned[:20], "total_count": len(rows)}
