"""
Non-query response helpers: reminders, errors, insert confirm, clarification.
Query results use the AI synthesis layer (response_synthesis + gemini.synthesize_response).
"""

from typing import Optional, List

STRICT_DATA_MODE = "STRICT_DATA_MODE"
ASSISTANT_MODE = "ASSISTANT_MODE"
REMINDER_MODE = "REMINDER_MODE"
ERROR_MODE = "ERROR_MODE"


def format_response(
    mode: str,
    factual: str = "",
    *,
    clarification_hint: str = "",
    error_detail: str = "",
    reminder_sent_count: int = 0,
    reminder_details: Optional[List[str]] = None,
    insert_confirmation: bool = False,
) -> str:
    """Non-query paths: reminders, errors, insert confirmation, follow-up factual."""
    if mode == ASSISTANT_MODE:
        if insert_confirmation:
            return "Done. I've added that job."
        return factual.strip() if factual else ""

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
        if error_detail:
            return error_detail.strip()
        if not factual:
            return (
                "I couldn't find any records matching that. "
                "Would you like to adjust the filters?"
            )
        if "couldn't find" in factual.lower() or "no matching" in factual.lower():
            return (
                "I couldn't find any records matching that. "
                "Would you like to adjust the filters?"
            )
        return factual.strip() if factual else "Something went wrong. Please try again."

    if mode == STRICT_DATA_MODE:
        return factual.strip() if factual else "No data to display."

    return factual.strip() if factual else ""


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
