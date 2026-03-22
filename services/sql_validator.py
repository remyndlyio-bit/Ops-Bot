"""
Validate generated SQL: allowlist table; allow SELECT, INSERT, and UPDATE only.
"""

import re
from typing import Tuple
from utils.logger import logger

ALLOWED_TABLES = {"job_entries", "public.job_entries"}

FORBIDDEN_KEYWORDS = [
    "drop", "delete", "truncate", "create", "alter",
    "grant", "revoke", "exec", "execute", "prepare", "copy", "vacuum",
]


def _validate_select(raw: str) -> Tuple[bool, str]:
    """Validate SELECT statement. Return (ok, error)."""
    from_match = re.search(r"\bFROM\s+([\w.\"]+)", raw, re.IGNORECASE)
    if not from_match:
        return False, "No FROM clause found."
    table_ref = from_match.group(1).strip().strip('"').lower()
    if "." in table_ref:
        _, table_ref = table_ref.split(".", 1)
    if table_ref not in {"job_entries"}:
        return False, f"Table not allowed: {table_ref}. Use job_entries only."
    return True, ""


def _validate_insert(raw: str) -> Tuple[bool, str]:
    """Validate INSERT statement. Return (ok, error)."""
    into_match = re.search(r"\bINSERT\s+INTO\s+([\w.\"]+)", raw, re.IGNORECASE)
    if not into_match:
        return False, "INSERT must target a table (INSERT INTO ...)."
    table_ref = into_match.group(1).strip().strip('"').lower()
    if "." in table_ref:
        _, table_ref = table_ref.split(".", 1)
    if table_ref not in {"job_entries"}:
        return False, f"Table not allowed: {table_ref}. Use job_entries only."
    return True, ""


def _validate_update(raw: str) -> Tuple[bool, str]:
    """Validate UPDATE statement. Return (ok, error)."""
    table_match = re.search(r"\bUPDATE\s+([\w.\"]+)", raw, re.IGNORECASE)
    if not table_match:
        return False, "UPDATE must target a table."
    table_ref = table_match.group(1).strip().strip('"').lower()
    if "." in table_ref:
        _, table_ref = table_ref.split(".", 1)
    if table_ref not in {"job_entries"}:
        return False, f"Table not allowed: {table_ref}. Use job_entries only."
    # Safety: UPDATE must have a WHERE clause with user_id
    if not re.search(r"\bWHERE\b", raw, re.IGNORECASE):
        return False, "UPDATE must include a WHERE clause."
    if not re.search(r"\buser_id\b", raw, re.IGNORECASE):
        return False, "UPDATE must include user_id in WHERE clause."
    return True, ""


def validate_sql(sql: str) -> Tuple[bool, str, str]:
    """
    Validate SQL for safety and allowlist.
    Allows SELECT, INSERT, and UPDATE on job_entries only.
    Returns (valid, sanitized_sql, error_message).
    """
    if not sql or not sql.strip():
        return False, "", "Empty SQL."

    raw = sql.strip().rstrip(";").strip()
    upper = raw.upper()

    if not upper.startswith("SELECT") and not upper.startswith("INSERT") and not upper.startswith("UPDATE"):
        return False, "", "Only SELECT, INSERT, and UPDATE statements are allowed."

    if ";" in raw:
        return False, "", "Only a single statement is allowed."

    for kw in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{kw}\b", raw, re.IGNORECASE):
            return False, "", f"Keyword not allowed: {kw}."

    if upper.startswith("SELECT"):
        ok, err = _validate_select(raw)
    elif upper.startswith("INSERT"):
        ok, err = _validate_insert(raw)
    else:
        ok, err = _validate_update(raw)
    if not ok:
        return False, "", err

    return True, raw, ""


def extract_limit(sql: str, default: int = 50) -> int:
    """Return LIMIT value from SQL or default."""
    m = re.search(r"\bLIMIT\s+(\d+)\b", sql, re.IGNORECASE)
    if m:
        return min(int(m.group(1)), 200)
    return default
