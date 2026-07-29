"""
Scope question detector: answer "does this include X?" from ledger[-1].scope.

When the previous answer has filters (e.g., "paid invoices"), users ask scope
questions to clarify: "does this include unpaid?", "is that only Nike?", etc.

These are answered from the plan's filters deterministically, with zero LLM cost.
"""

import re
from typing import Optional, Dict, Any


def is_scope_question(message: str) -> bool:
    """
    Detect scope questions about the immediately previous answer.

    Scope questions ask about the COMPOSITION/FILTERS of the last answer:
    - "does/do these include…?" (both paid and unpaid, paid ones, unpaid?)
    - "is that only…?" (only Nike, only paid?)
    - "paid and unpaid?" (shorthand clarification)

    False for new queries: "what about this month?" (references_last_answer but
    asks for a DIFFERENT value, not scope clarification).
    """
    msg = message.strip().lower()

    # Pattern: "does/do these include…" variations
    if re.search(r"do(?:es)?\s+(?:these|that|those|they|it|the\s+\w+)\s+(?:also\s+)?include", msg):
        return True

    # Pattern: "is that only…" / "are those only…"
    if re.search(r"(?:is|are)\s+(?:that|these|those|it)\s+only", msg):
        return True

    # Pattern: specific scope questions without structure
    # "paid and unpaid?" / "paid or unpaid?" / "both paid and unpaid?"
    if re.search(r"\b(?:paid|invoice|billed?|bill_sent)\s+(?:and|or|\&)\s+(?:unpaid|not\s+invoice|unbilled|not\s+bill)", msg):
        return True

    # Pattern: clarifications like "including X?" / "with X included?"
    if re.search(r"(?:including|with|also)\s+\w+(?:s)?\s+(?:included?|added?|counted?)", msg):
        return True

    # Hinglish
    if re.search(r"(?:kya|kaise|toh)\s+\w+.*(?:bhi|include|count)", msg):
        return True

    return False


def answer_scope_from_ledger(
    last_ledger_entry: Optional[Dict[str, Any]],
    question: str,
) -> Optional[str]:
    """
    Construct a scope answer from ledger[-1] without calling LLM.

    Returns a reply like "That's all jobs — paid and unpaid, no date filter."
    or None if we can't answer from ledger (e.g., no filters in scope).
    """
    if not last_ledger_entry:
        return None

    try:
        scope = last_ledger_entry.get("scope", {})
        filters = scope.get("filters", {})
        time_range = scope.get("time_range")

        # Detect what the user is asking about
        msg = question.lower()

        # "does this include paid and unpaid?"
        if re.search(r"(?:paid|payment|invoice|bill)", msg):
            paid_filter = filters.get("paid")
            if paid_filter is None or paid_filter == "":
                return "That's all invoices — paid and unpaid."
            elif paid_filter == "yes" or paid_filter is True:
                return "That's invoices that have been paid."
            elif paid_filter == "no" or paid_filter is False:
                return "That's invoices still waiting for payment."

        # "is that only Nike?"
        if re.search(r"only\s+(\w+)", msg):
            client_filter = filters.get("client_name")
            if client_filter:
                return f"That's only for {client_filter}."
            else:
                return "That's all clients, not limited to one."

        # General scope: list all active filters
        parts = []
        if time_range:
            start = time_range.get("start", "")
            end = time_range.get("end", "")
            if start and end:
                parts.append(f"from {start} to {end}")
            elif start:
                parts.append(f"since {start}")
            elif end:
                parts.append(f"through {end}")

        if filters:
            for key, val in sorted(filters.items()):
                if key == "paid":
                    if val is None or val == "":
                        parts.append("paid and unpaid")
                    elif val == "yes" or val is True:
                        parts.append("paid")
                    elif val == "no" or val is False:
                        parts.append("unpaid")
                elif key == "client_name" and val:
                    parts.append(f"client: {val}")
                elif key == "bill_sent":
                    if val == "yes" or val is True:
                        parts.append("invoices sent")
                    elif val == "no" or val is False:
                        parts.append("invoices not sent")

        if parts:
            return "That's: " + ", ".join(parts) + "."

        return None

    except Exception:
        return None
