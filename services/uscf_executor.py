"""
USCF Executor.
Executes validated USCF commands against Google Sheets.
Returns structured results for the response maker.
"""
from datetime import datetime
from typing import Dict, Any, List, Optional
from utils.date_utils import parse_sheet_date
from utils.logger import logger


def _normalize_keys(row: Dict) -> Dict[str, str]:
    """Create lowercase key map for case-insensitive lookup."""
    return {str(k).strip().lower().replace(" ", "").replace("_", ""): k for k in row.keys()}


def _get_col(key: str, row: Dict, key_map: Dict[str, str]) -> Optional[str]:
    """Get actual column name from row, case-insensitive."""
    if not key:
        return None
    if key in row:
        return key
    k_norm = str(key).strip().lower().replace(" ", "").replace("_", "")
    return key_map.get(k_norm)


def _row_matches_filters(row: Dict, filters: Dict[str, Any], date_column: str, time_range: Optional[Dict] = None) -> bool:
    """Check if row matches all filters."""
    key_map = _normalize_keys(row)
    for col, val in filters.items():
        if val is None:
            continue
        actual_col = _get_col(col, row, key_map)
        if not actual_col:
            return False
        row_val = row.get(actual_col)
        if row_val is None:
            return False
        row_str = str(row_val).strip().lower()
        # List filter (OR match)
        if isinstance(val, list):
            val_strs = [str(v).strip().lower() for v in val if v is not None]
            if not any(row_str == v or v in row_str for v in val_strs):
                return False
        else:
            val_str = str(val).strip().lower()
            # Date comparison
            if date_column and col.lower().replace(" ", "").replace("_", "") == date_column.lower().replace(" ", "").replace("_", ""):
                try:
                    if len(val_str) >= 10:
                        filter_date = datetime.strptime(val_str[:10], "%Y-%m-%d").date()
                        if isinstance(row_val, datetime):
                            row_date = row_val.date()
                        else:
                            parsed = parse_sheet_date(row_val)
                            row_date = parsed.date() if parsed else None
                        if row_date and row_date == filter_date:
                            continue
                        elif row_date:
                            return False
                except ValueError:
                    pass
            # String comparison
            if row_str != val_str and val_str not in row_str:
                return False

    # Time range filter
    if time_range and date_column:
        actual_date_col = _get_col(date_column, row, key_map)
        if actual_date_col:
            date_val = row.get(actual_date_col)
            if date_val is None or (isinstance(date_val, str) and not date_val.strip()):
                return False
            if isinstance(date_val, datetime):
                dt = date_val
            else:
                dt = parse_sheet_date(date_val)
            if not dt:
                return False
            try:
                start = datetime.strptime(time_range["start"][:10], "%Y-%m-%d").date()
                end = datetime.strptime(time_range["end"][:10], "%Y-%m-%d").date()
                if not (start <= dt.date() <= end):
                    return False
            except (ValueError, KeyError):
                return False
    return True


def _numeric_value(val: Any) -> float:
    """Extract numeric value from cell."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace("₹", "").replace(",", "").replace(" ", "").replace("$", "")
    try:
        return float(s) if s and s != "None" else 0.0
    except (ValueError, TypeError):
        return 0.0


def execute_uscf(
    cmd: Dict[str, Any],
    records: List[Dict],
    date_column: str,
    sheets_service: Any = None,
) -> Dict[str, Any]:
    """
    Execute a validated USCF command.
    Returns structured result dict.
    """
    if not records:
        return {"ok": False, "message": "No records available."}

    operation = cmd.get("operation")
    filters = cmd.get("filters") or {}
    key_map = _normalize_keys(records[0])

    # ===== CREATE =====
    if operation == "create":
        data = cmd.get("data", {})
        if not data:
            return {"ok": False, "message": "No data provided for create."}
        if sheets_service:
            ok = sheets_service.append_row_by_columns(data)
            if ok:
                summary = ", ".join(f"{k}: {v}" for k, v in list(data.items())[:5])
                return {"ok": True, "operation": "create", "message": f"Created new record: {summary}"}
            return {"ok": False, "message": "Failed to add record to sheet."}
        return {"ok": False, "message": "Sheet service not available."}

    # ===== DELETE =====
    if operation == "delete":
        if not filters:
            return {"ok": False, "message": "Delete requires filters."}
        # Find matching rows
        time_range = cmd.get("time_range")
        matched = [r for r in records if _row_matches_filters(r, filters, date_column, time_range)]
        if not matched:
            return {"ok": True, "operation": "delete", "message": "No matching records to delete.", "count": 0}
        # Delete rows (from bottom to top to preserve row numbers)
        if sheets_service:
            deleted = 0
            for row in sorted(matched, key=lambda r: r.get("_row", 0), reverse=True):
                row_num = row.get("_row")
                if row_num:
                    # sheets_service would need a delete_row method
                    deleted += 1
            return {"ok": True, "operation": "delete", "message": f"Would delete {len(matched)} record(s).", "count": len(matched)}
        return {"ok": False, "message": "Sheet service not available for delete."}

    # ===== UPDATE =====
    if operation == "update":
        if not filters:
            return {"ok": False, "message": "Update requires filters."}
        updates = cmd.get("updates", [])
        if not updates:
            return {"ok": False, "message": "No updates specified."}
        time_range = cmd.get("time_range")
        matched = [r for r in records if _row_matches_filters(r, filters, date_column, time_range)]
        if not matched:
            return {"ok": True, "operation": "update", "message": "No matching records to update.", "count": 0}
        if sheets_service:
            updated = 0
            for row in matched:
                row_num = row.get("_row")
                if not row_num:
                    continue
                for upd in updates:
                    field = upd.get("field")
                    mode = upd.get("mode", "set")
                    value = upd.get("value")
                    actual_col = _get_col(field, row, key_map)
                    if not actual_col:
                        continue
                    current = row.get(actual_col)
                    new_value = value
                    if mode == "set":
                        new_value = value
                    elif mode == "increase":
                        new_value = _numeric_value(current) + _numeric_value(value)
                    elif mode == "decrease":
                        new_value = _numeric_value(current) - _numeric_value(value)
                    elif mode == "increase_percent":
                        new_value = _numeric_value(current) * (1 + _numeric_value(value) / 100)
                    elif mode == "decrease_percent":
                        new_value = _numeric_value(current) * (1 - _numeric_value(value) / 100)
                    elif mode == "append":
                        current_str = str(current).strip() if current else ""
                        new_value = f"{current_str} {value}".strip() if current_str else str(value)
                    elif mode == "clear":
                        new_value = ""
                    sheets_service.update_cell_by_header(row_num, actual_col, new_value)
                updated += 1
            update_summary = ", ".join(f"{u['field']}={u['value']}" for u in updates[:3])
            return {"ok": True, "operation": "update", "message": f"Updated {updated} record(s): {update_summary}", "count": updated}
        return {"ok": False, "message": "Sheet service not available for update."}

    # ===== QUERY =====
    if operation == "query":
        time_range = cmd.get("time_range")
        filtered = [r for r in records if _row_matches_filters(r, filters, date_column, time_range)]
        if not filtered:
            return {"ok": True, "operation": "query", "metric": cmd.get("metric", "count"), "value": 0, "count": 0, "message": "No matching records."}

        metric = cmd.get("metric", "count")
        column = cmd.get("column")
        group_by = cmd.get("group_by")
        return_fields = cmd.get("return_fields")
        order = cmd.get("order")
        limit = cmd.get("limit")
        offset = cmd.get("offset")

        actual_col = _get_col(column, records[0], key_map) if column else None
        actual_group = _get_col(group_by, records[0], key_map) if group_by else None
        actual_date_col = _get_col(date_column, records[0], key_map)

        # Single value lookup (metric=value)
        if metric == "value":
            if not actual_col:
                return {"ok": False, "message": f"Column '{column}' not found."}
            first = filtered[0]
            val = first.get(actual_col)
            return {"ok": True, "operation": "query", "metric": "value", "value": str(val) if val else "", "column": column, "count": 1}

        # Date max (latest date)
        if metric == "max" and actual_col and actual_date_col and actual_col == actual_date_col:
            dates = []
            for r in filtered:
                dv = r.get(actual_col)
                if isinstance(dv, datetime):
                    dates.append(dv)
                elif dv:
                    dt = parse_sheet_date(dv)
                    if dt:
                        dates.append(dt)
            if not dates:
                return {"ok": True, "operation": "query", "metric": "max", "value": None, "message": "No dates found."}
            latest = max(dates)
            return {"ok": True, "operation": "query", "metric": "max", "value": latest.date().isoformat(), "value_type": "date", "count": len(filtered)}

        # Grouped aggregation
        if actual_group:
            groups: Dict[str, List[float]] = {}
            for r in filtered:
                gv = r.get(actual_group)
                label = str(gv).strip() if gv else ""
                if label:
                    num = _numeric_value(r.get(actual_col)) if actual_col else 1
                    groups.setdefault(label, []).append(num)
            agg = {}
            for label, nums in groups.items():
                if metric == "count":
                    agg[label] = len(nums)
                elif metric == "sum":
                    agg[label] = sum(nums)
                elif metric == "avg":
                    agg[label] = sum(nums) / len(nums) if nums else 0
                elif metric == "min":
                    agg[label] = min(nums) if nums else 0
                elif metric == "max":
                    agg[label] = max(nums) if nums else 0
                else:
                    agg[label] = sum(nums)
            reverse = order != "asc"
            sorted_labels = sorted(agg.keys(), key=lambda k: agg[k], reverse=reverse)
            values = [agg[l] for l in sorted_labels]
            if offset and isinstance(offset, int):
                sorted_labels = sorted_labels[offset:]
                values = values[offset:]
            if limit and isinstance(limit, int):
                sorted_labels = sorted_labels[:limit]
                values = values[:limit]
            return {"ok": True, "operation": "query", "metric": metric, "labels": sorted_labels, "values": values, "count": len(filtered)}

        # Single metric
        if actual_col:
            numbers = [_numeric_value(r.get(actual_col)) for r in filtered]
        else:
            numbers = [1.0] * len(filtered)

        if metric == "count":
            value = len(filtered)
        elif metric == "sum":
            value = sum(numbers)
        elif metric == "avg":
            value = sum(numbers) / len(numbers) if numbers else 0
        elif metric == "min":
            value = min(numbers) if numbers else 0
        elif metric == "max":
            value = max(numbers) if numbers else 0
        else:
            value = sum(numbers)

        return {"ok": True, "operation": "query", "metric": metric, "value": value, "column": column, "count": len(filtered)}

    return {"ok": False, "message": f"Unknown operation: {operation}"}
