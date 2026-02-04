"""
Validates and sanitizes the LLM query plan against a strict schema.
Only allows columns from the schema and metrics from the approved list.
"""
from typing import Dict, Any, List, Tuple, Optional
from utils.logger import logger

ALLOWED_METRICS = frozenset({"sum", "avg", "min", "max", "count"})
ALLOWED_CONFIDENCE = frozenset({"high", "low"})
REQUIRED_KEYS = frozenset({"sheet", "metric", "column", "filters", "time_range", "group_by", "confidence"})


def validate_plan(
    plan: Dict[str, Any],
    allowed_columns: List[str],
    allowed_sheets: Optional[List[str]] = None,
) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str]]:
    """
    Validate the LLM query plan against schema.
    Returns (valid, sanitized_plan, error_message).
    - allowed_columns: list of column names from schema/env.
    - allowed_sheets: list of sheet names (default ["sheet1"]).
    """
    if not isinstance(plan, dict):
        return False, None, "Expected a JSON object."

    allowed_sheets = allowed_sheets or ["sheet1"]
    columns_set = {c.strip() for c in allowed_columns if c and str(c).strip()}

    # Check required keys
    missing = [k for k in REQUIRED_KEYS if k not in plan]
    if missing:
        return False, None, f"Missing required fields: {', '.join(missing)}. Please rephrase with a clear time period and column."

    sanitized = {}

    # sheet: must be string, must be in allowed list (or default)
    sheet = plan.get("sheet")
    if sheet is None or (isinstance(sheet, str) and not sheet.strip()):
        sanitized["sheet"] = "sheet1"
    elif isinstance(sheet, str) and sheet.strip().lower() in {s.lower() for s in allowed_sheets}:
        sanitized["sheet"] = sheet.strip()
    else:
        sanitized["sheet"] = "sheet1"
        logger.info(f"Plan sheet '{sheet}' not in allowed list; using sheet1")

    # metric: only from approved list
    metric = plan.get("metric")
    if metric is None or (isinstance(metric, str) and metric.strip().lower() not in ALLOWED_METRICS):
        return False, None, f"Invalid or missing 'metric'. Allowed: sum, avg, min, max, count."
    sanitized["metric"] = str(metric).strip().lower()

    # column: must be in schema
    column = plan.get("column")
    if not column or not str(column).strip():
        return False, None, "Missing 'column'. Please specify which column (e.g. Fees, Client Name)."
    col_str = str(column).strip()
    # Match case-insensitively against allowed columns
    col_match = next((c for c in columns_set if c.lower() == col_str.lower()), None)
    if not col_match and columns_set:
        return False, None, f"Column '{col_str}' is not in the schema. Allowed columns include: {', '.join(sorted(columns_set)[:15])}{'...' if len(columns_set) > 15 else ''}."
    sanitized["column"] = col_match if col_match else col_str

    # filters: keys must be in schema; values string, number, list of strings (for "in" match), or null
    filters = plan.get("filters")
    if filters is None:
        sanitized["filters"] = {}
    elif not isinstance(filters, dict):
        sanitized["filters"] = {}
    else:
        out = {}
        for k, v in filters.items():
            k_str = str(k).strip()
            key_match = next((c for c in columns_set if c.lower() == k_str.lower()), None)
            if key_match and v is not None:
                if isinstance(v, list):
                    out[key_match] = [str(x).strip() for x in v if x is not None]
                elif isinstance(v, (str, int, float)):
                    out[key_match] = v
                else:
                    out[key_match] = str(v)
        sanitized["filters"] = out

    # time_range: optional; if missing, treat as "no date filter". Only absolute (AI-computed) dates are allowed.
    time_range = plan.get("time_range")
    if time_range is None:
        sanitized["time_range"] = None
    else:
        if not isinstance(time_range, dict):
            return False, None, "Invalid 'time_range'. Use { \"type\": \"absolute\", \"value\": { \"start\": \"YYYY-MM-DD\", \"end\": \"YYYY-MM-DD\" } } or null."
        tr_type = time_range.get("type")
        tr_value = time_range.get("value")
        if tr_type != "absolute":
            return False, None, "time_range.type must be 'absolute' (AI computes dates). Use start/end in YYYY-MM-DD."
        if not isinstance(tr_value, dict) or "start" not in tr_value or "end" not in tr_value:
            return False, None, "time_range.value must be { \"start\": \"YYYY-MM-DD\", \"end\": \"YYYY-MM-DD\" }."
        sanitized["time_range"] = {"type": "absolute", "value": {"start": str(tr_value.get("start", ""))[:10], "end": str(tr_value.get("end", ""))[:10]}}

    # group_by: null or column from schema
    group_by = plan.get("group_by")
    if group_by is None or (isinstance(group_by, str) and not group_by.strip()):
        sanitized["group_by"] = None
    elif isinstance(group_by, str):
        gb_str = group_by.strip()
        gb_match = next((c for c in columns_set if c.lower() == gb_str.lower()), None)
        if gb_match:
            sanitized["group_by"] = gb_match
        else:
            sanitized["group_by"] = None
            logger.info(f"group_by '{group_by}' not in schema; ignoring")
    else:
        sanitized["group_by"] = None

    # limit: optional positive integer (top N, bottom N, etc.)
    limit_raw = plan.get("limit")
    if limit_raw is None:
        sanitized["limit"] = None
    elif isinstance(limit_raw, int) and limit_raw > 0:
        sanitized["limit"] = min(int(limit_raw), 100)
    elif isinstance(limit_raw, (float, str)):
        try:
            n = int(float(limit_raw))
            if n > 0:
                sanitized["limit"] = min(n, 100)
            else:
                sanitized["limit"] = None
        except (ValueError, TypeError):
            sanitized["limit"] = None
    else:
        sanitized["limit"] = None

    # order: optional "asc" | "desc" for sort order of grouped/list results
    order_raw = plan.get("order")
    if order_raw is None or (isinstance(order_raw, str) and not order_raw.strip()):
        sanitized["order"] = None
    elif isinstance(order_raw, str) and order_raw.strip().lower() in ("asc", "desc"):
        sanitized["order"] = order_raw.strip().lower()
    else:
        sanitized["order"] = None

    # offset: optional non-negative integer (skip first N)
    offset_raw = plan.get("offset")
    if offset_raw is None:
        sanitized["offset"] = None
    elif isinstance(offset_raw, int) and offset_raw >= 0:
        sanitized["offset"] = min(int(offset_raw), 1000)
    elif isinstance(offset_raw, (float, str)):
        try:
            n = int(float(offset_raw))
            if n >= 0:
                sanitized["offset"] = min(n, 1000)
            else:
                sanitized["offset"] = None
        except (ValueError, TypeError):
            sanitized["offset"] = None
    else:
        sanitized["offset"] = None

    # confidence
    confidence = plan.get("confidence")
    if confidence is None or (isinstance(confidence, str) and confidence.strip().lower() not in ALLOWED_CONFIDENCE):
        sanitized["confidence"] = "high"
    else:
        sanitized["confidence"] = str(confidence).strip().lower()

    # Optional: clarification_question (when confidence is low)
    if plan.get("clarification_question") and isinstance(plan.get("clarification_question"), str):
        sanitized["clarification_question"] = plan["clarification_question"].strip()
    else:
        sanitized["clarification_question"] = None

    return True, sanitized, None
