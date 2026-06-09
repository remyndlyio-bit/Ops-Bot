"""
Date / timestamp columns — invoice_date, job_date, payment_date,
due_date, *_reminder_sent, created_at, updated_at, overdue_audit_sent,
bill_sent_at.

SEMANTIC
--------
Date and timestamptz columns. Postgres rejects ILIKE on these (operator
does not exist: date ~~* unknown). Several past bugs were "planner
emitted a fuzzy filter on a date column, SQL builder ILIKE'd it, 500."

Past bugs covered
-----------------
  * {"invoice_date": null} → invoice_date ILIKE NULL → 500
  * {"invoice_date": "IS NOT NULL"} → invoice_date ILIKE 'IS NOT NULL' → 500
  * Any junk value on a date column → ILIKE → 500
"""

from typing import Any, Optional

from services.columns import ColumnSpec, register


_DATE_COLUMNS = (
    "job_date", "invoice_date", "payment_date", "due_date",
    "first_reminder_sent", "second_reminder_sent", "third_reminder_sent",
    "created_at", "updated_at", "overdue_audit_sent", "bill_sent_at",
)


def _make_handler(col: str):
    def handler(val: Any) -> Optional[str]:
        # Common NULL semantics
        if val is None:
            return f"{col} IS NULL"
        if isinstance(val, str):
            _v = val.strip().upper()
            if _v in ("IS NULL", "NULL"):
                return f"{col} IS NULL"
            if _v in ("IS NOT NULL", "NOT NULL", "ANY", "*"):
                return f"{col} IS NOT NULL"
        # We don't fully take over date filtering — the generic builder
        # already handles operator-prefix strings, dicts, etc. Return None
        # to fall through, BUT only after stripping the ILIKE option upstream.
        # The generic builder consults _DATE_COLUMNS to skip ILIKE.
        return None

    return handler


PROMPT_FRAGMENT = """\
DATE / TIMESTAMP COLUMNS (job_date, invoice_date, payment_date, due_date,
  *_reminder_sent, created_at, updated_at, overdue_audit_sent, bill_sent_at):
  * For a specific date: {"<col>": "YYYY-MM-DD"}
  * Range: {"<col>": {"operator": "<", "value": "YYYY-MM-DD"}} (or > <= >= !=)
  * Range string also OK: {"<col>": "< 2026-03-14"}
  * Existence: {"<col>": null} = IS NULL; {"<col>": "IS NOT NULL"} = IS NOT NULL
  NEVER produce a fuzzy / ILIKE filter on these — Postgres rejects ILIKE on
  date types ("operator does not exist: date ~~* unknown").
"""


def _normalize(val: Any):
    """Path 3 normaliser. Allowed canonical forms for date columns:
    NullCheck, Equality (date literal), Comparison. NEVER TextMatch —
    that would map to ILIKE and Postgres rejects ILIKE on date types.
    Junk values return None so the planner gets a NormalisationError."""
    from services.plan import NullCheck, Equality, Comparison
    import re as _re

    if val is None:
        return NullCheck(is_null=True)
    if isinstance(val, str):
        _v = val.strip().lower().replace(" ", "_")
        if _v in ("is_null", "null", "isnull", ""):
            return NullCheck(is_null=True)
        if _v in ("is_not_null", "not_null", "isnotnull", "any", "*"):
            return NullCheck(is_null=False)
        # Operator-prefixed: "< 2026-03-14"
        m = _re.match(r"^(<=|>=|!=|<|>|=)\s*(.+)$", val.strip())
        if m:
            return Comparison(op=m.group(1), value=m.group(2).strip())
        # Bare ISO date
        if _re.match(r"^\d{4}-\d{2}-\d{2}$", val.strip()):
            return Equality(value=val.strip())
        return None  # junk — let validator surface it
    if isinstance(val, dict) and "operator" in val and "value" in val:
        op = val["operator"]
        if op in ("<", "<=", ">", ">=", "=", "!="):
            return Comparison(op=op, value=val["value"])
        return None
    return None


# Register each date column with the same handler factory
for _col in _DATE_COLUMNS:
    register(ColumnSpec(
        name=_col,
        semantic=__doc__ or "",
        prompt_fragment="",  # one shared fragment exported below
        filter_handler=_make_handler(_col),
        normalize_filter=_normalize,
    ))


# Exported once for the prompt composer (deduplication helper)
SHARED_PROMPT_FRAGMENT = PROMPT_FRAGMENT


def is_date_column(name: str) -> bool:
    """Whether a column is a date/timestamp (used by the generic SQL
    builder to suppress ILIKE)."""
    return name in _DATE_COLUMNS
