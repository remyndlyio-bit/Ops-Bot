"""
Validate generated SQL: allowlist table; allow SELECT and INSERT only.
"""

import re
from typing import Tuple
from utils.logger import logger

ALLOWED_TABLES = {"job_entries", "public.job_entries"}

FORBIDDEN_KEYWORDS = [
    "drop", "delete", "update", "truncate", "create", "alter",
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


def validate_sql(sql: str) -> Tuple[bool, str, str]:
    """
    Validate SQL for safety and allowlist.
    Allows SELECT and INSERT on job_entries only.
    Returns (valid, sanitized_sql, error_message).
    """
    if not sql or not sql.strip():
        return False, "", "Empty SQL."

    raw = sql.strip().rstrip(";").strip()
    upper = raw.upper()

    if not upper.startswith("SELECT") and not upper.startswith("INSERT"):
        return False, "", "Only SELECT and INSERT statements are allowed."

    if ";" in raw:
        return False, "", "Only a single statement is allowed."

    for kw in FORBIDDEN_KEYWORDS:
        if re.search(rf"\b{kw}\b", raw, re.IGNORECASE):
            return False, "", f"Keyword not allowed: {kw}."

    if upper.startswith("SELECT"):
        ok, err = _validate_select(raw)
    else:
        ok, err = _validate_insert(raw)
    if not ok:
        return False, "", err

    return True, raw, ""


def extract_limit(sql: str, default: int = 50) -> int:
    """Return LIMIT value from SQL or default."""
    m = re.search(r"\bLIMIT\s+(\d+)\b", sql, re.IGNORECASE)
    if m:
        return min(int(m.group(1)), 200)
    return default
