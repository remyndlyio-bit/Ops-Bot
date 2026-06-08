"""
Column: poc_email (text, nullable) — client point-of-contact email.

SEMANTIC
--------
The address the bot emails invoices and reminders to. NULL/empty means
"can't email this client" — and so this column should rarely be a filter
in user-facing queries. The planner has historically added it
unsolicitedly ({"poc_email": null}) when answering "pending invoice" —
which excluded exactly the rows the user wanted.

Past bugs covered
-----------------
  * Planner adding {"poc_email": null} on 'pending invoice' queries,
    silently excluding deliverable rows. (Now caught by tests.)
"""

from typing import Any, Optional

from services.columns import ColumnSpec, register


def filter_handler(val: Any) -> Optional[str]:
    """poc_email is intentionally minimal — most queries don't filter on it,
    and the planner has a history of adding unsolicited NULL filters here
    that exclude deliverable rows. We only handle the explicit forms.
    """
    if val is None:
        return "(poc_email IS NULL OR TRIM(poc_email) = '')"
    if isinstance(val, str):
        _v = val.strip().lower()
        if _v in ("is null", "null", ""):
            return "(poc_email IS NULL OR TRIM(poc_email) = '')"
        if _v in ("is not null", "not null", "any", "*"):
            return "(poc_email IS NOT NULL AND TRIM(poc_email) <> '')"
    return None  # fall through to generic ILIKE for actual address matches


PROMPT_FRAGMENT = """\
COLUMN poc_email (text, client contact email; NULL = no email on file):
  Rarely needed as a filter. DO NOT auto-add a poc_email filter when the
  user asks about 'pending', 'sent', or 'unpaid' — the bill_sent and paid
  handlers already cover deliverability. Only filter on poc_email when
  the user explicitly asks "who has no email" or "find John's email".
"""


register(ColumnSpec(
    name="poc_email",
    semantic=__doc__ or "",
    prompt_fragment=PROMPT_FRAGMENT,
    filter_handler=filter_handler,
))
