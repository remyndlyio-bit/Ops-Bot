"""
USCF Validator.
Validates and sanitizes USCF commands against schema.
"""
from typing import Dict, Any, List, Tuple, Optional
from utils.logger import logger

ALLOWED_OPERATIONS = frozenset({"create", "update", "query", "delete"})
ALLOWED_UPDATE_MODES = frozenset({"set", "increase", "decrease", "increase_percent", "decrease_percent", "append", "clear"})
ALLOWED_METRICS = frozenset({"count", "sum", "avg", "min", "max", "value"})


def _normalize_column(col: str, columns_set: set) -> Optional[str]:
    """Find matching column name (case-insensitive, space-insensitive)."""
    if not col:
        return None
    col_clean = str(col).strip()
    # Exact match
    if col_clean in columns_set:
        return col_clean
    # Case-insensitive match
    col_lower = col_clean.lower().replace(" ", "").replace("_", "")
    for c in columns_set:
        c_lower = c.lower().replace(" ", "").replace("_", "")
        if col_lower == c_lower:
            return c
    return None


def _parse_smart_number(val: Any) -> Any:
    """Parse smart numbers: 2k, ₹5000, fifty, etc."""
    if val is None:
        return val
    if isinstance(val, (int, float)):
        return val
    s = str(val).strip().lower()
    # Remove currency symbols
    s = s.replace("₹", "").replace("$", "").replace(",", "").replace(" ", "")
    # Handle k/m suffixes
    multiplier = 1
    if s.endswith("k"):
        s = s[:-1]
        multiplier = 1000
    elif s.endswith("m"):
        s = s[:-1]
        multiplier = 1000000
    elif s.endswith("lakh") or s.endswith("lac"):
        s = s.replace("lakh", "").replace("lac", "")
        multiplier = 100000
    elif s.endswith("cr") or s.endswith("crore"):
        s = s.replace("crore", "").replace("cr", "")
        multiplier = 10000000
    # Word numbers
    word_map = {
        "zero": 0, "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
        "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
        "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
        "hundred": 100, "thousand": 1000,
    }
    if s in word_map:
        return word_map[s] * multiplier
    try:
        return float(s) * multiplier
    except (ValueError, TypeError):
        return val  # Return original if not parseable


def validate_uscf(
    cmd: Dict[str, Any],
    columns: List[str],
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Validate USCF command.
    Returns (valid, sanitized_command, error_message).
    """
    if not isinstance(cmd, dict):
        return False, None, "Expected a JSON object."

    columns_set = {c.strip() for c in columns if c and str(c).strip()}

    operation = cmd.get("operation")
    if not operation or str(operation).lower() not in ALLOWED_OPERATIONS:
        return False, None, f"Invalid operation. Allowed: create, update, query, delete."

    operation = str(operation).lower()
    sanitized = {"operation": operation}

    # Validate filters (common to update, query, delete)
    filters = cmd.get("filters")
    if filters and isinstance(filters, dict):
        san_filters = {}
        for k, v in filters.items():
            col = _normalize_column(k, columns_set)
            if col:
                san_filters[col] = v
        sanitized["filters"] = san_filters
    else:
        sanitized["filters"] = {}

    # Operation-specific validation
    if operation == "create":
        data = cmd.get("data")
        if not data or not isinstance(data, dict):
            return False, None, "Create operation requires 'data' object with column values."
        san_data = {}
        for k, v in data.items():
            col = _normalize_column(k, columns_set)
            if col:
                san_data[col] = _parse_smart_number(v) if isinstance(v, (str, int, float)) else v
        if not san_data:
            return False, None, "No valid columns in 'data'. Check column names."
        sanitized["data"] = san_data

    elif operation == "update":
        updates = cmd.get("updates")
        if not updates or not isinstance(updates, list) or len(updates) == 0:
            return False, None, "Update operation requires 'updates' array with at least one update."
        san_updates = []
        for upd in updates:
            if not isinstance(upd, dict):
                continue
            field = upd.get("field")
            mode = str(upd.get("mode", "set")).lower()
            value = upd.get("value")
            col = _normalize_column(field, columns_set)
            if not col:
                continue
            if mode not in ALLOWED_UPDATE_MODES:
                mode = "set"
            # Parse smart numbers for numeric modes
            if mode in ("set", "increase", "decrease", "increase_percent", "decrease_percent"):
                value = _parse_smart_number(value)
            san_updates.append({"field": col, "mode": mode, "value": value})
        if not san_updates:
            return False, None, "No valid updates. Check field names and modes."
        sanitized["updates"] = san_updates
        if not sanitized["filters"]:
            return False, None, "Update requires filters to identify which row(s) to update."

    elif operation == "query":
        # metric
        metric = cmd.get("metric")
        if metric and str(metric).lower() in ALLOWED_METRICS:
            sanitized["metric"] = str(metric).lower()
        else:
            sanitized["metric"] = "count"  # Default
        # column (for aggregation)
        column = cmd.get("column")
        if column:
            col = _normalize_column(column, columns_set)
            sanitized["column"] = col if col else column
        # return_fields
        return_fields = cmd.get("return_fields")
        if return_fields and isinstance(return_fields, list):
            san_fields = [_normalize_column(f, columns_set) or f for f in return_fields]
            sanitized["return_fields"] = [f for f in san_fields if f]
        # group_by
        group_by = cmd.get("group_by")
        if group_by:
            col = _normalize_column(group_by, columns_set)
            sanitized["group_by"] = col
        # order
        order = cmd.get("order")
        if order and str(order).lower() in ("asc", "desc"):
            sanitized["order"] = str(order).lower()
        # limit
        limit = cmd.get("limit")
        if isinstance(limit, int) and limit > 0:
            sanitized["limit"] = min(limit, 100)
        elif isinstance(limit, str):
            try:
                sanitized["limit"] = min(int(limit), 100)
            except ValueError:
                pass
        # offset
        offset = cmd.get("offset")
        if isinstance(offset, int) and offset >= 0:
            sanitized["offset"] = min(offset, 1000)
        # time_range
        time_range = cmd.get("time_range")
        if time_range and isinstance(time_range, dict):
            start = time_range.get("start")
            end = time_range.get("end")
            if start and end:
                sanitized["time_range"] = {"start": str(start)[:10], "end": str(end)[:10]}

    elif operation == "delete":
        if not sanitized["filters"]:
            return False, None, "Delete requires filters to identify which row(s) to delete."

    return True, sanitized, None
