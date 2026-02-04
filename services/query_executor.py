"""
Executes a validated query plan against Google Sheets.
Resolves time range in code, filters rows, applies metric, optionally group_by.
"""
from typing import Dict, Any, List, Optional
from utils.date_utils import parse_sheet_date
from utils.time_resolver import resolve_time_range
from utils.logger import logger


def _row_matches_filters(row: Dict, filters: Dict[str, Any], date_column: str, date_range: Optional[Dict[str, str]]) -> bool:
    """Check if row matches filters and (if provided) date range."""
    for col, val in filters.items():
        if val is None:
            continue
        row_val = row.get(col)
        if row_val is None:
            return False
        row_str = str(row_val).strip().lower()
        val_str = str(val).strip().lower()
        if row_str != val_str and val_str not in row_str:
            return False
    if date_range and date_column:
        date_val = row.get(date_column)
        if not date_val:
            return False
        dt = parse_sheet_date(str(date_val))
        if not dt:
            return False
        start = date_range.get("start")
        end = date_range.get("end")
        if start and end:
            try:
                from datetime import datetime
                start_d = datetime.strptime(start[:10], "%Y-%m-%d").date()
                end_d = datetime.strptime(end[:10], "%Y-%m-%d").date()
                if not (start_d <= dt.date() <= end_d):
                    return False
            except ValueError:
                return False
    return True


def _numeric_value(val: Any) -> float:
    """Extract numeric value from cell (strip currency, commas)."""
    if val is None:
        return 0.0
    s = str(val).strip().replace("₹", "").replace(",", "").replace(" ", "").replace("$", "")
    try:
        return float(s) if s and s != "None" else 0.0
    except (ValueError, TypeError):
        return 0.0


def execute_plan(
    plan: Dict[str, Any],
    records: List[Dict],
    date_column: str,
) -> Dict[str, Any]:
    """
    Execute a validated query plan on the given records.
    - plan: sanitized plan (sheet, metric, column, filters, time_range, group_by, confidence).
    - records: list of row dicts from the sheet.
    - date_column: column name to use for time_range (e.g. Date or invoice_date).
    Returns { "ok": True, "value": number } or { "ok": True, "values": [...], "labels": [...] } for group_by,
    or { "ok": False, "message": "..." }.
    """
    if not records:
        return {"ok": False, "message": "I don't have any records to query yet."}

    time_range = plan.get("time_range")
    date_range = None
    if time_range and date_column:
        date_range = resolve_time_range(time_range, date_column)
        if not date_range:
            return {"ok": False, "message": "I couldn't resolve the time period. Please specify a period (e.g. last quarter, this month)."}

    filters = plan.get("filters") or {}
    column = plan.get("column")
    metric = plan.get("metric", "sum")
    group_by = plan.get("group_by")

    # Normalize column names: plan may use schema name; row keys might have spaces
    row_keys_lower = {str(k).strip().lower(): k for k in records[0].keys()}
    def get_col(key: str):
        if not key:
            return None
        k = str(key).strip()
        if k in records[0]:
            return k
        return row_keys_lower.get(k.lower())

    col_metric = get_col(column)
    col_group = get_col(group_by) if group_by else None
    col_date = get_col(date_column) if date_column else None

    if not col_metric and column:
        return {"ok": False, "message": f"I don't see a column matching '{column}' in the sheet."}

    filtered = [
        r for r in records
        if _row_matches_filters(r, filters, col_date or date_column, date_range)
    ]

    if not filtered:
        return {"ok": True, "value": 0, "count": 0, "message": "No rows match the filters or time period."}

    # group_by: grouped aggregation (sum/avg/min/max/count) per group
    if col_group:
        groups: Dict[str, List[float]] = {}
        for r in filtered:
            val = r.get(col_group)
            v = str(val).strip() if val is not None else ""
            if v:
                groups.setdefault(v, []).append(_numeric_value(r.get(col_metric)))

        agg_values: Dict[str, float] = {}
        for label, nums in groups.items():
            if metric == "count":
                agg_values[label] = float(len(nums))
            elif metric == "sum":
                agg_values[label] = float(sum(nums))
            elif metric == "avg":
                agg_values[label] = float(sum(nums) / len(nums)) if nums else 0.0
            elif metric == "min":
                agg_values[label] = float(min(nums)) if nums else 0.0
            elif metric == "max":
                agg_values[label] = float(max(nums)) if nums else 0.0
            else:
                agg_values[label] = float(sum(nums))

        # Sort groups by aggregated value descending (useful for "top clients" style questions)
        sorted_labels = sorted(agg_values.keys(), key=lambda k: agg_values[k], reverse=True)
        values = [agg_values[l] for l in sorted_labels]
        return {"ok": True, "labels": sorted_labels, "values": values, "count": len(filtered)}

    # Single metric on column
    numbers = [_numeric_value(r.get(col_metric)) for r in filtered]
    if metric == "sum":
        value = sum(numbers)
    elif metric == "avg":
        value = sum(numbers) / len(numbers) if numbers else 0
    elif metric == "min":
        value = min(numbers) if numbers else 0
    elif metric == "max":
        value = max(numbers) if numbers else 0
    elif metric == "count":
        value = len(filtered)
    else:
        value = sum(numbers)

    return {"ok": True, "value": value, "count": len(filtered)}
