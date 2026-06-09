"""
Column: bill_sent (text, nullable)

SEMANTIC
--------
Tracks whether the invoice for a job has actually been EMAILED to the
client. Distinct from invoice_date (which means "PDF exists").

Truthy values: 'Yes' / 'true' / 't' / '1' / 'sent'
Falsy / not sent: NULL, '', 'No', 'false', '0', 'pending', etc.

Set to 'Yes' automatically in intent_service when an invoice email actually
succeeds. The send timestamp lives in a separate column (bill_sent_at).

Invariant: a row counts as "sent" only when (a) bill_sent is truthy AND
(b) poc_email is set. A row with no contact email could never have been
emailed — exclude it from sent results even if bill_sent slipped through
somehow.

Past bugs covered by the test suite
-----------------------------------
  * Planner emitted list ["no","false","0",""] → SQL builder used IN(...),
    excluded NULL rows, returned 0 → user got wrong "no results" reply.
  * Planner emitted "IS NOT NULL" as a string → ILIKE 'IS NOT NULL'.
  * Planner emitted null → ILIKE NULL → 500 from Postgres.
  * Earlier classifier prompt claimed this column DID NOT exist (wrong).
"""

from typing import Any, Optional

from services.columns import ColumnSpec, register


_TRUTHY_TOKENS = {"yes", "true", "t", "1", "sent", "y", "paid"}
# Multi-word tokens MUST appear in BOTH space and underscore form because
# `_classify` normalises the input via `replace(" ", "_")` before lookup.
_FALSY_TOKENS = {
    "no", "false", "0", "", "n", "not",
    "not sent", "not_sent",
    "pending", "unpaid", "outstanding",
    "is null", "is_null", "null",
}

_SQL_TRUTHY = (
    "LOWER(COALESCE(bill_sent, '')) IN ('true', 't', 'yes', '1', 'sent') "
    "AND poc_email IS NOT NULL AND TRIM(poc_email) <> ''"
)
_SQL_NOT_SENT = (
    "(bill_sent IS NULL OR TRIM(COALESCE(bill_sent, '')) = '' "
    "OR LOWER(bill_sent) NOT IN ('true', 't', 'yes', '1', 'sent'))"
)


def _classify(val: Any) -> Optional[bool]:
    """Reduce ANY value the AI might send into True (sent), False (not
    sent), or None (couldn't tell — caller should fall through).
    Normalises 'not_null' / 'is_not_null' (underscore variants) the way
    the planner sometimes emits them."""
    if val is None:
        return False  # treated as "not sent" — what 'bill_sent: null' means semantically
    if isinstance(val, str):
        _v = val.strip().lower().replace(" ", "_")  # normalize spaces & underscores
        if _v in ("is_null", "null"):
            return False
        if _v in ("is_not_null", "not_null", "isnotnull", "any", "*"):
            return True
        # Strip underscores for token comparisons below (tokens are word-form)
        _vt = _v.replace("_", "")
        if _vt in _TRUTHY_TOKENS or _v in _TRUTHY_TOKENS:
            return True
        if _vt in _FALSY_TOKENS or _v in _FALSY_TOKENS:
            return False
        return None
    if isinstance(val, list):
        if not val:
            return None
        # If ANY element is falsy, treat the whole list as 'not sent' —
        # that's the planner's typical "OR" semantics for falsy markers.
        has_falsy = any(str(v).lower().strip() in _FALSY_TOKENS for v in val)
        all_truthy = all(str(v).lower().strip() in _TRUTHY_TOKENS for v in val)
        if has_falsy:
            return False
        if all_truthy:
            return True
        return None
    return None


def filter_handler(val: Any) -> Optional[str]:
    """Returns the SQL predicate for any well-typed filter on bill_sent.
    Returns None when the value is genuinely unrecognised — caller falls
    back to the generic SQL builder (which will at worst produce an
    over-narrow predicate, never a 500)."""
    decision = _classify(val)
    if decision is True:
        return _SQL_TRUTHY
    if decision is False:
        return _SQL_NOT_SENT
    return None  # let the generic builder try


# ── Prompt fragment for the planner / classifier ────────────────────
# Same text injected into BOTH prompts; the registry composes them. Any
# change here propagates to both the planner and the classifier so they
# can't disagree about what bill_sent means.
PROMPT_FRAGMENT = """\
COLUMN bill_sent (text, set on actual invoice-email delivery; NULL = not sent yet):
  Use ONE of these shapes — the SQL layer normalizes all of them.
    {"bill_sent": "yes"} | {"bill_sent": "no"} | {"bill_sent": null}
    {"bill_sent": "IS NOT NULL"} | {"bill_sent": "IS NULL"}
  Semantics:
    "yes" / "sent" / "IS NOT NULL" / truthy list → invoices that were emailed
    "no" / "pending" / null / "IS NULL" / falsy list → invoices NOT yet sent
  When the user asks 'pending invoice / invoice baki / yet to send /
  who haven't I billed', use {"bill_sent": "no"} as a SINGLE filter.
  DO NOT add a poc_email filter unsolicited — the SQL layer already
  requires poc_email for the "sent" case.
"""


def normalize_filter(val: Any):
    """Path 3 normaliser: collapse every shape into a BoolCheck.
    Returns None when the value is genuinely ambiguous — the planner
    boundary will surface this as a NormalisationError so the LLM can
    correct it instead of producing a wrong answer silently."""
    from services.plan import BoolCheck
    decision = _classify(val)
    if decision is True:
        return BoolCheck(truthy=True)
    if decision is False:
        return BoolCheck(truthy=False)
    return None


register(ColumnSpec(
    name="bill_sent",
    semantic=__doc__ or "",
    prompt_fragment=PROMPT_FRAGMENT,
    filter_handler=filter_handler,
    normalize_filter=normalize_filter,
))
