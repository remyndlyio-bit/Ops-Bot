"""
Column: paid (text, nullable)

SEMANTIC
--------
Tracks whether the invoice has been paid by the client.

Truthy: 'Yes' / 'true' / 't' / '1' / 'paid'
Falsy / unpaid: NULL, '', 'No', 'false', '0', 'pending', 'outstanding', 'unpaid'

Past bugs covered
-----------------
  * Planner emitted ["no","false","unpaid"] → SQL IN(...) excluded NULL rows.
  * Planner sometimes emitted "false" lowercase from AI canonicalization.
"""

from typing import Any, Optional

from services.columns import ColumnSpec, register


_TRUTHY = {"yes", "true", "t", "1", "paid", "y"}
_FALSY = {"no", "false", "0", "", "n", "unpaid", "pending", "outstanding",
          "is null", "null", "not paid"}

_SQL_PAID = "LOWER(COALESCE(paid, '')) IN ('true', 't', 'yes', '1', 'paid')"
_SQL_UNPAID = (
    "(paid IS NULL OR TRIM(COALESCE(paid, '')) = '' "
    "OR LOWER(paid) NOT IN ('true', 't', 'yes', '1', 'paid'))"
)


def _classify(val: Any) -> Optional[bool]:
    if val is None:
        return False
    if isinstance(val, str):
        _v = val.strip().lower()
        if _v in ("is null", "null"):
            return False
        if _v in ("is not null", "not null", "any", "*"):
            return True
        if _v in _TRUTHY:
            return True
        if _v in _FALSY:
            return False
        return None
    if isinstance(val, list):
        if not val:
            return None
        if any(str(v).lower().strip() in _FALSY for v in val):
            return False
        if all(str(v).lower().strip() in _TRUTHY for v in val):
            return True
        return None
    return None


def filter_handler(val: Any) -> Optional[str]:
    decision = _classify(val)
    if decision is True:
        return _SQL_PAID
    if decision is False:
        return _SQL_UNPAID
    return None


PROMPT_FRAGMENT = """\
COLUMN paid (text, set when invoice is paid; NULL or empty = unpaid):
  {"paid": "yes"} | {"paid": "no"} | {"paid": null}  → SQL normalises.
  "yes" → paid clients only. "no" / null → unpaid (includes NULL rows).
  DO NOT emit IN-clauses or ILIKE on this column.
"""


register(ColumnSpec(
    name="paid",
    semantic=__doc__ or "",
    prompt_fragment=PROMPT_FRAGMENT,
    filter_handler=filter_handler,
))
