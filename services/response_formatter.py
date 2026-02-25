"""
Response-style layer: tone, structure, and phrasing for the ops bot.
Preserves precision and financial accuracy; no hallucination or fluff in data.
"""

from typing import List, Dict, Any, Optional

# Tone modes for context-appropriate phrasing
STRICT_DATA_MODE = "STRICT_DATA_MODE"   # Financial summaries, tables, payment details
ASSISTANT_MODE = "ASSISTANT_MODE"       # General query results, job lookups
REMINDER_MODE = "REMINDER_MODE"         # Payment reminders, proactive actions
ERROR_MODE = "ERROR_MODE"               # Errors, empty results, clarification


# Column name variants for payment status detection (case-insensitive)
PAID_LIKE_KEYS = ("paid", "payment_status", "bill_sent")
FEE_LIKE_KEYS = ("fees", "fee", "amount", "fees_total")
CLIENT_LIKE_KEYS = ("client_name", "client", "production_house")
DATE_LIKE_KEYS = ("job_date", "date", "payment_date")
LANGUAGE_LIKE_KEYS = ("language", "lang")


def _get_val(row: Dict, keys: tuple) -> Optional[Any]:
    """First matching key (case-insensitive) in row."""
    row_lower = {str(k).lower(): v for k, v in row.items()}
    for k in keys:
        if k in row_lower and row_lower[k] is not None:
            return row_lower[k]
    return None


def _is_paid(row: Dict) -> Optional[bool]:
    """True if paid, False if not paid, None if unknown."""
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
    """Human label for a key (no emojis)."""
    labels = {
        "client_name": "Client",
        "job_date": "Date",
        "payment_date": "Payment date",
        "fees": "Fee",
        "fee": "Fee",
        "language": "Language",
        "paid": "Payment status",
        "bill_no": "Bill no.",
        "job_description_details": "Job",
        "production_house": "Production house",
        "notes": "Notes",
    }
    return labels.get(key.lower(), key.replace("_", " ").title())


def _fmt_val(v: Any) -> str:
    if v is None:
        return "N/A"
    if isinstance(v, (int, float)) and "fee" not in str(v).lower():
        return str(v)
    return str(v)


def format_as_job_summary_block(rows: List[Dict], max_rows: int = 10) -> str:
    """
    Deterministic structured block for job/financial data. No emojis.
    Uses consistent labels and bullet style.
    """
    if not rows:
        return ""
    lines = []
    for i, row in enumerate(rows[:max_rows]):
        if i > 0:
            lines.append("")
        # Prefer a consistent field order when keys look like job summary
        preferred = ["client_name", "job_date", "language", "fees", "paid", "bill_no", "job_description_details", "notes"]
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
    """
    Contextual line for payment status. No fluff.
    Returns None if status unknown or not relevant.
    """
    paid = _is_paid(row)
    if paid is True:
        return "Payment received and recorded."
    if paid is False:
        return "This payment is still pending. Would you like me to draft a reminder?"
    return None


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
    """
    Produce a polished response string for the given mode and inputs.
    factual: raw data or preformatted text.
    rows: when provided and is_financial/ASSISTANT_MODE, format as job summary block.
    """
    if mode == STRICT_DATA_MODE:
        if rows and is_financial:
            block = format_as_job_summary_block(rows)
            note = payment_status_note(rows[0]) if add_payment_note and rows else None
            if note:
                return block.rstrip() + "\n\n" + note
            return block
        return factual.strip() if factual else "No data to display."

    if mode == ASSISTANT_MODE:
        if rows and (is_financial or _looks_like_job_row(rows[0])):
            block = format_as_job_summary_block(rows)
            note = payment_status_note(rows[0]) if add_payment_note and rows else None
            head = "Here's what I found:"
            out = f"{head}\n\n{block}"
            if note:
                out = out.rstrip() + "\n\n" + note
            return out
        if insert_confirmation:
            return "Done. I've added that job."
        if factual:
            return f"Here's what I found:\n\n{factual.strip()}"
        return factual.strip() or "No data to show."

    if mode == REMINDER_MODE:
        if reminder_sent_count and reminder_details:
            parts = ["All set."]
            parts.append(f"Sent {reminder_sent_count} payment reminder(s).")
            parts.append("")
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
                "I couldn't find any jobs matching that criteria. "
                "Would you like me to check a different date range or client?"
            )
        if error_detail:
            return f"{error_detail.strip()}"  # Already user-facing, keep calm
        if "couldn't find" in factual.lower() or "no matching" in factual.lower():
            return (
                "I couldn't find any jobs matching that criteria. "
                "Would you like me to check a different date range or client?"
            )
        return factual.strip() if factual else "Something went wrong. Please try again."

    return factual.strip() if factual else ""


def _looks_like_job_row(row: Dict) -> bool:
    """Heuristic: row has at least one of client/fee/date."""
    keys_lower = [str(k).lower() for k in row.keys()]
    return (
        any(c in keys_lower for c in ["client_name", "client", "production_house"])
        or any(f in keys_lower for f in ["fees", "fee", "amount"])
        or any(d in keys_lower for d in ["job_date", "date", "payment_date"])
    )


def clarify_phrase(examples: Optional[List[str]] = None) -> str:
    """When confidence is low or query is ambiguous."""
    if examples:
        ex = ", ".join(f"'{e}'" for e in examples[:3])
        return f"I'm not quite sure what you're asking. Could you give a bit more detail? For example: {ex}."
    return "Could you rephrase or narrow that down? For example, try a date range or a specific client name."


def error_calm_phrase(technical: bool = False) -> str:
    """Calm, clear error message. No technical leak unless requested."""
    if technical:
        return "Something went wrong on my side. Please try again in a moment."
    return "I couldn't complete that. Please try again in a moment."


def query_invalid_phrase() -> str:
    """When SQL/query validation fails."""
    return (
        "I couldn't turn that into a safe query. "
        "Try rephrasing, e.g. 'Total billing last month' or 'Jobs for client X'."
    )


def no_result_phrase() -> str:
    """Consistent no-result response."""
    return (
        "I couldn't find any jobs matching that criteria. "
        "Would you like me to check a different date range or client?"
    )
