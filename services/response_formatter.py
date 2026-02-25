"""
Response-style layer: adaptive, context-aware formatting.
Preserves data accuracy; avoids rigid templates and key:value dump.
"""

from typing import List, Dict, Any, Optional, Tuple

# Result types for adaptive formatting
RESULT_EMPTY = "empty"
RESULT_COUNT = "count"
RESULT_SINGLE_FIELD = "single_field"
RESULT_FULL_RECORD = "full_record"
RESULT_MULTI_ROW = "multi_row"

# Tone modes (for non-query paths)
STRICT_DATA_MODE = "STRICT_DATA_MODE"
ASSISTANT_MODE = "ASSISTANT_MODE"
REMINDER_MODE = "REMINDER_MODE"
ERROR_MODE = "ERROR_MODE"

PAID_LIKE_KEYS = ("paid", "payment_status", "bill_sent")
FEE_LIKE_KEYS = ("fees", "fee", "amount", "fees_total")
CLIENT_LIKE_KEYS = ("client_name", "client", "production_house")
DATE_LIKE_KEYS = ("job_date", "date", "payment_date")
LANGUAGE_LIKE_KEYS = ("language", "lang")
COUNT_LIKE_KEYS = ("count", "total", "num", "n")


def _get_val(row: Dict, keys: tuple) -> Optional[Any]:
    row_lower = {str(k).lower(): v for k, v in row.items()}
    for k in keys:
        if k in row_lower and row_lower[k] is not None:
            return row_lower[k]
    return None


def _is_paid(row: Dict) -> Optional[bool]:
    v = _get_val(row, PAID_LIKE_KEYS)
    if v is None:
        return None
    s = str(v).strip().upper()
    if s in ("YES", "TRUE", "1", "PAID", "Y"):
        return True
    if s in ("NO", "FALSE", "0", "PENDING", "N"):
        return False
    return None


def _label(key: str) -> str:
    labels = {
        "client_name": "Client", "job_date": "Date", "payment_date": "Payment date",
        "fees": "Fee", "fee": "Fee", "language": "Language", "paid": "Payment status",
        "bill_no": "Bill no.", "job_description_details": "Job",
        "production_house": "Production house", "notes": "Notes",
    }
    return labels.get(key.lower(), key.replace("_", " ").title())


def _fmt_val(v: Any) -> str:
    if v is None:
        return "N/A"
    return str(v)


def detect_result_type(rows: List[Dict]) -> str:
    """
    Classify result for adaptive formatting.
    Returns: RESULT_EMPTY | RESULT_COUNT | RESULT_SINGLE_FIELD | RESULT_FULL_RECORD | RESULT_MULTI_ROW
    """
    if not rows:
        return RESULT_EMPTY
    if len(rows) > 1:
        return RESULT_MULTI_ROW
    row = rows[0]
    keys = [str(k).lower() for k in row.keys()]
    ncols = len(row)

    # Single column → count or single_field
    if ncols == 1:
        k = keys[0]
        if k in ("count", "total", "sum", "num") or "count" in k:
            return RESULT_COUNT
        return RESULT_SINGLE_FIELD

    # Two columns often: one is count/sum, one is label
    if ncols == 2 and any(c in keys for c in ("count", "sum", "total")):
        return RESULT_COUNT

    # One row, many columns → full record
    return RESULT_FULL_RECORD


def _format_single_field(rows: List[Dict]) -> str:
    """One row, one column → natural sentence."""
    row = rows[0]
    key = next(iter(row.keys()))
    val = row[key]
    k = str(key).lower()

    if val is None:
        return "No value for that."

    # Natural sentence templates (no key:value)
    if k in ("language", "lang"):
        return f"The last job was executed in {val}."
    if k in ("client_name", "client", "production_house"):
        return f"The client is {val}."
    if k in ("job_date", "date"):
        return f"That was on {val}."
    if k in ("payment_date",):
        return f"Payment date is {val}."
    if k in ("fees", "fee", "amount", "sum"):
        try:
            n = float(val)
            return f"The amount is ₹{n:,.0f}." if k != "sum" else f"Total is ₹{n:,.0f}."
        except (TypeError, ValueError):
            return f"The amount is {val}."
    if k in ("paid", "payment_status", "bill_sent"):
        s = str(val).strip().upper()
        if s in ("YES", "TRUE", "1", "PAID", "Y"):
            return "Paid."
        if s in ("NO", "FALSE", "0", "PENDING", "N"):
            return "Still pending."
        return f"Payment status: {val}."
    if k in ("job_description_details", "job", "notes"):
        v = str(val).strip()
        if len(v) > 80:
            v = v[:77] + "..."
        return v if v else "No description."
    if k in ("bill_no",):
        return f"Bill no. {val}."

    return f"{_label(key)}: {val}."


def _format_count(rows: List[Dict]) -> str:
    """Count/sum result → natural sentence."""
    row = rows[0]
    keys = [str(k).lower() for k in row.keys()]
    if "count" in keys or any("count" in k for k in keys):
        n = row.get("count") or list(row.values())[0]
        try:
            n = int(float(n))
            return f"You have {n} job entries." if n != 1 else "You have 1 job entry."
        except (TypeError, ValueError):
            pass
    if "sum" in keys or any("sum" in k for k in keys):
        v = list(row.values())[0]
        try:
            n = float(v)
            return f"Total is ₹{n:,.0f}."
        except (TypeError, ValueError):
            pass
    # Single numeric value
    v = list(row.values())[0]
    try:
        n = int(float(v))
        return f"There are {n} matching." if n != 1 else "There is 1 matching."
    except (TypeError, ValueError):
        return f"Total: {v}."


def _format_full_record(rows: List[Dict], add_payment_note: bool = True) -> str:
    """One row, many columns → structured block, no robotic prefix."""
    block = format_as_job_summary_block(rows)
    note = payment_status_note(rows[0]) if add_payment_note and rows else None
    if note:
        return block.rstrip() + "\n\n" + note
    return block


def _format_multi_row(rows: List[Dict], max_rows: int = 10) -> str:
    """Multiple rows → concise list, no 'Here's what I found'."""
    preferred = ["client_name", "job_date", "fees", "paid", "language"]
    lines = []
    for r in rows[:max_rows]:
        parts = []
        for k in preferred:
            if k in r and r[k] is not None:
                parts.append(f"{_label(k)}: {_fmt_val(r[k])}")
        if not parts:
            parts = [f"{_label(k)}: {_fmt_val(v)}" for k, v in list(r.items())[:5] if v is not None]
        lines.append("• " + ", ".join(parts))
    if len(rows) > max_rows:
        lines.append(f"... and {len(rows) - max_rows} more.")
    return "\n".join(lines)


def format_as_job_summary_block(rows: List[Dict], max_rows: int = 10) -> str:
    """Structured block for job/financial data. No prefix, no emojis."""
    if not rows:
        return ""
    lines = []
    preferred = ["client_name", "job_date", "language", "fees", "paid", "bill_no", "job_description_details", "notes"]
    for i, row in enumerate(rows[:max_rows]):
        if i > 0:
            lines.append("")
        keys = list(row.keys())
        ordered = [k for k in preferred if k in keys] + [k for k in keys if k not in preferred]
        for k in ordered[:12]:
            v = row.get(k)
            if v is None and k not in preferred:
                continue
            lines.append(f"• {_label(k)}: {_fmt_val(v)}")
    if len(rows) > max_rows:
        lines.append(f"\n... and {len(rows) - max_rows} more.")
    return "\n".join(lines)


def payment_status_note(row: Dict) -> Optional[str]:
    paid = _is_paid(row)
    if paid is True:
        return "Payment received and recorded."
    if paid is False:
        return "This payment is still pending. Would you like me to draft a reminder?"
    return None


def format_adaptive(
    rows: List[Dict],
    *,
    add_payment_note: bool = True,
) -> str:
    """
    Single entry point for query results: detect type and format naturally.
    No repetitive prefixes; natural sentences for single field and count.
    """
    result_type = detect_result_type(rows)
    if result_type == RESULT_EMPTY:
        return (
            "I couldn't find any records matching that. "
            "Would you like to adjust the filters?"
        )
    if result_type == RESULT_COUNT:
        return _format_count(rows)
    if result_type == RESULT_SINGLE_FIELD:
        return _format_single_field(rows)
    if result_type == RESULT_FULL_RECORD:
        return _format_full_record(rows, add_payment_note=add_payment_note)
    if result_type == RESULT_MULTI_ROW:
        return _format_multi_row(rows, max_rows=10)
    return format_as_job_summary_block(rows)


def format_response(
    mode: str,
    factual: str = "",
    *,
    rows: Optional[List[Dict]] = None,
    is_financial: bool = False,
    add_payment_note: bool = True,
    clarification_hint: str = "",
    error_detail: str = "",
    reminder_sent_count: int = 0,
    reminder_details: Optional[List[str]] = None,
    insert_confirmation: bool = False,
) -> str:
    """Legacy entry for non-query paths (reminders, errors, insert confirm)."""
    if mode == STRICT_DATA_MODE:
        if rows and is_financial:
            block = format_as_job_summary_block(rows)
            note = payment_status_note(rows[0]) if add_payment_note and rows else None
            if note:
                return block.rstrip() + "\n\n" + note
            return block
        return factual.strip() if factual else "No data to display."

    if mode == ASSISTANT_MODE:
        if insert_confirmation:
            return "Done. I've added that job."
        if rows:
            return format_adaptive(rows, add_payment_note=add_payment_note)
        if factual:
            return factual.strip()
        return factual.strip() or "No data to show."

    if mode == REMINDER_MODE:
        if reminder_sent_count and reminder_details:
            parts = ["All set.", f"Sent {reminder_sent_count} payment reminder(s).", ""]
            parts.append("Clients notified:")
            for d in reminder_details:
                parts.append(f"• {d}")
            return "\n".join(parts)
        if reminder_sent_count == 0 and clarification_hint:
            return f"I couldn't find any clients with payments due in the next few days that need a first reminder. {clarification_hint}"
        return "Reminders are up to date."

    if mode == ERROR_MODE:
        if not factual and not error_detail:
            return (
                "I couldn't find any records matching that. "
                "Would you like to adjust the filters?"
            )
        if error_detail:
            return error_detail.strip()
        if "couldn't find" in (factual or "").lower() or "no matching" in (factual or "").lower():
            return (
                "I couldn't find any records matching that. "
                "Would you like to adjust the filters?"
            )
        return factual.strip() if factual else "Something went wrong. Please try again."

    return factual.strip() if factual else ""


def _looks_like_job_row(row: Dict) -> bool:
    keys_lower = [str(k).lower() for k in row.keys()]
    return (
        any(c in keys_lower for c in ["client_name", "client", "production_house"])
        or any(f in keys_lower for f in ["fees", "fee", "amount"])
        or any(d in keys_lower for d in ["job_date", "date", "payment_date"])
    )


def clarify_phrase(examples: Optional[List[str]] = None) -> str:
    if examples:
        ex = ", ".join(f"'{e}'" for e in examples[:3])
        return f"I'm not quite sure what you're asking. Could you give a bit more detail? For example: {ex}."
    return "Could you rephrase or narrow that down? For example, try a date range or a specific client name."


def error_calm_phrase(technical: bool = False) -> str:
    if technical:
        return "Something went wrong on my side. Please try again in a moment."
    return "I couldn't complete that. Please try again in a moment."


def query_invalid_phrase() -> str:
    return (
        "I couldn't turn that into a safe query. "
        "Try rephrasing, e.g. 'Total billing last month' or 'Jobs for client X'."
    )


def no_result_phrase() -> str:
    return (
        "I couldn't find any records matching that. "
        "Would you like to adjust the filters?"
    )
