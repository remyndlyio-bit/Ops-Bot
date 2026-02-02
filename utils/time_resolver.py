"""
Resolves relative and absolute time ranges to concrete start/end dates.
All time calculations are done in backend code; the LLM only returns identifiers.
"""
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
from utils.logger import logger

# Allowed relative time_range.value strings from the LLM
RELATIVE_VALUES = frozenset({
    "last_quarter", "this_quarter", "last_month", "this_month",
    "ytd", "this_year", "last_7_days", "last_30_days", "last_90_days",
})


def resolve_time_range(time_range: Dict[str, Any], date_column: str) -> Optional[Dict[str, str]]:
    """
    Resolve time_range from the query plan to { "start": "YYYY-MM-DD", "end": "YYYY-MM-DD" }.
    - time_range["type"]: "relative" | "absolute"
    - time_range["value"]: for relative, a string (e.g. "last_quarter"); for absolute, { "start", "end" }.
    Returns None if invalid; caller should treat as "no date filter" or reject.
    """
    if not time_range or not isinstance(time_range, dict):
        return None
    t = time_range.get("type")
    v = time_range.get("value")
    if t == "absolute" and isinstance(v, dict):
        start = v.get("start")
        end = v.get("end")
        if start and end:
            return {"start": str(start)[:10], "end": str(end)[:10]}
        return None
    if t == "relative" and isinstance(v, str):
        v = v.strip().lower()
        if v not in RELATIVE_VALUES:
            logger.warning(f"Unknown relative time_range value: {v}")
            return None
        return _resolve_relative(v)
    return None


def _resolve_relative(value: str) -> Dict[str, str]:
    """Compute start/end for a relative time identifier."""
    now = datetime.now()
    today = now.date()

    if value == "last_7_days":
        start = today - timedelta(days=7)
        return {"start": start.isoformat(), "end": today.isoformat()}
    if value == "last_30_days":
        start = today - timedelta(days=30)
        return {"start": start.isoformat(), "end": today.isoformat()}
    if value == "last_90_days":
        start = today - timedelta(days=90)
        return {"start": start.isoformat(), "end": today.isoformat()}

    if value == "this_month":
        start = today.replace(day=1)
        # end = last day of this month
        next_month = (now.replace(day=28) + timedelta(days=4)).replace(day=1)
        end = next_month - timedelta(days=1)
        return {"start": start.isoformat(), "end": end.date().isoformat()}
    if value == "last_month":
        first_this = today.replace(day=1)
        end = first_this - timedelta(days=1)
        start = end.replace(day=1)
        return {"start": start.isoformat(), "end": end.isoformat()}

    if value == "ytd" or value == "this_year":
        start = today.replace(month=1, day=1)
        return {"start": start.isoformat(), "end": today.isoformat()}

    # Quarters: Q1=1-3, Q2=4-6, Q3=7-9, Q4=10-12
    if value == "this_quarter":
        q = (now.month - 1) // 3 + 1
        start_month = (q - 1) * 3 + 1
        start = today.replace(month=start_month, day=1)
        end_month = start_month + 2
        if end_month == 12:
            end = today.replace(month=12, day=31)
        else:
            next_first = today.replace(month=end_month, day=28) + timedelta(days=4)
            end = next_first.replace(day=1) - timedelta(days=1)
        return {"start": start.isoformat(), "end": end.isoformat()}
    if value == "last_quarter":
        current_q = (now.month - 1) // 3 + 1
        if current_q == 1:
            start = today.replace(year=now.year - 1, month=10, day=1)
            end = today.replace(year=now.year - 1, month=12, day=31)
        else:
            start_month = (current_q - 2) * 3 + 1
            end_month = start_month + 2
            start = today.replace(month=start_month, day=1)
            next_first = today.replace(month=end_month, day=28) + timedelta(days=4)
            end = next_first.replace(day=1) - timedelta(days=1)
        return {"start": start.isoformat(), "end": end.isoformat()}

    return {"start": today.isoformat(), "end": today.isoformat()}
