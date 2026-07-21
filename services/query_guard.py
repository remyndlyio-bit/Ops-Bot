"""
Query guard — a pure consistency check between a user's message and the SQL we
are about to run for it.

The bug class this prevents: a generator (the deterministic router OR the LLM
planner) emits *well-formed* SQL that silently ignores a qualifier the user
stated — e.g. "how many UNPAID jobs" -> COUNT(*) with no paid filter, or
"show me GARNIER jobs" -> SELECT * with no client filter. The SQL runs fine and
returns a confident WRONG answer.

``sql_reflects_message()`` detects the qualifiers present in the message — drawn
from a CLOSED, schema-bounded vocabulary (status, value/count intent, date
period) plus the one open set (client names, via the known-client list or a
leftover-noun heuristic) — and asserts each is reflected in the SQL. On a
mismatch the caller FAILS CLOSED: the router abstains (-> planner); the planner
asks for clarification instead of executing under-specified SQL.

PURE: no DB / LLM / I/O. Tested in tests/test_query_guard.py.
"""
import re
from typing import Iterable, Tuple

# ── message-side detectors (closed, schema-bounded vocabularies) ─────────────
_UNPAID = r"unpaid|not\s+paid|pending|outstanding|overdue|owe[sd]?|owing|baki|baaki"
_STATUS_RE = re.compile(rf"\b(?:{_UNPAID}|paid|cleared|received|settled)\b")

# bill_sent (invoice dispatch) status — a SEPARATE qualifier from paid/unpaid.
# "who am I yet to invoice" / "which invoices haven't gone out" must be
# reflected by a bill_sent predicate; before this, the guard had NO vocabulary
# for invoice-dispatch language at all, so a planner SQL that silently dropped
# bill_sent sailed through ungated (confirmed live: "Who are you yet to send
# the invoice?" returned all 4 jobs, 3 of which already had an invoice date).
# Requires an invoice/bill NOUN near a dispatch-status VERB (not just either
# alone) — "what's the invoice number for Wilson" must NOT trigger this, since
# it has the noun but no dispatch verb.
_INVOICE_NOUN = r"invoic(?:e|es|ed|ing)|bill(?:s|ed)?"
_DISPATCH_VERB = (
    r"sent|raised|gone\s+out|(?:still\s+)?pending|outstanding|yet\s+to|"
    r"still\s+(?:need|have)\s+to|haven'?t|not\s+(?:yet\s+)?(?:sent|raised|gone|out)"
)
_DISPATCH_RE = re.compile(
    rf"\b(?:{_INVOICE_NOUN})\b.{{0,25}}\b(?:{_DISPATCH_VERB})\b"
    rf"|\b(?:{_DISPATCH_VERB})\b.{{0,25}}\b(?:{_INVOICE_NOUN})\b"
)
_VALUE_RE = re.compile(
    r"\b(?:how\s+much|total|sum|amount|earn(?:ed|ings)?|revenue|kamai|made|worth|owe[sd]?)\b")
_COUNT_RE = re.compile(r"\b(?:how\s+many|number\s+of|count|kitne|kitni)\b")
_DATE_RE = re.compile(
    r"\b(?:today|yesterday|this\s+(?:week|month|quarter|year)|"
    r"last\s+(?:week|month|quarter|year)|past\s+(?:week|month|quarter|year)|"
    r"q[1-4]|20\d{2}|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\b")
# A client typically sits right before a job-noun ("garnier jobs") or after for/from.
_NOUN_RE = re.compile(
    r"\b([a-z][a-z0-9'&.\-]+)\s+(?:jobs?|invoices?|work|records?|entr(?:y|ies)|gigs?|projects?|shoots?)\b")
_FORX_RE = re.compile(r"\b(?:for|from)\s+([a-z0-9][a-z0-9'&.\-]+)")

# Words that are NOT a client name when they appear before "jobs"/"invoices"/etc.
# (function words + the count/value/status/time vocab). Missing one only costs an
# unnecessary deferral to the planner — never a wrong answer — so err broad.
_NOT_CLIENT = {
    "how", "many", "much", "more", "most", "do", "does", "did", "i", "me", "my", "mine",
    "we", "us", "our", "you", "your", "they", "them", "their", "he", "she", "it",
    "is", "are", "am", "was", "were", "be", "been", "being", "have", "has", "had",
    "of", "in", "on", "at", "to", "for", "from", "by", "with", "and", "or", "but",
    "no", "not", "per", "number", "count", "total", "sum", "amount", "money", "fee",
    "fees", "the", "a", "an", "any", "all", "some", "each", "every", "this", "that",
    "these", "those", "here", "there", "show", "list", "give", "get", "fetch", "send",
    "see", "view", "display", "me", "us", "recent", "latest", "last", "first", "new",
    "old", "current", "past", "open", "pending", "unpaid", "paid", "outstanding",
    "overdue", "done", "completed", "today", "yesterday", "week", "month", "quarter",
    "year", "day", "days", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug",
    "sep", "oct", "nov", "dec", "january", "february", "march", "april", "june",
    "july", "august", "september", "october", "november", "december",
    # superlatives / adjectives that describe a job, not a client
    # ("highest paying job", "most expensive job", "biggest job", "single job")
    "paying", "expensive", "cheap", "highest", "lowest", "biggest", "smallest",
    "largest", "top", "bottom", "best", "worst", "single", "typical", "big",
    "small", "high", "low", "least", "profitable", "good", "bad", "next",
    "previous", "upcoming", "one", "average",
}


def _client_in_message(m: str, known_clients: Iterable[str], use_heuristic: bool = True):
    """Return a specific client named in the message, or None. Known clients win;
    otherwise (when ``use_heuristic``) fall back to the leftover-noun / for-X
    heuristic. The heuristic over-fires by design (safe for router abstention);
    callers that FAIL CLOSED pass ``use_heuristic=False`` and rely on the
    known-client list for precision."""
    for kc in (known_clients or ()):
        kc = (kc or "").strip().lower()
        if kc and len(kc) >= 2 and kc in m:
            return kc
    if not use_heuristic:
        return None
    for rx in (_NOUN_RE, _FORX_RE):
        mt = rx.search(m)
        if mt:
            w = mt.group(1).strip()
            if w and not w.isdigit() and w not in _NOT_CLIENT:
                return w
    return None


def _sql_has_client_filter(s: str) -> bool:
    # ILIKE only ever appears in our SQL as a client/brand/production_house filter.
    return ("ilike" in s) or bool(re.search(r"(client_name|brand_name|production_house)\s*=", s))


def sql_reflects_message(message: str, sql: str,
                         known_clients: Iterable[str] = (),
                         use_heuristic_client: bool = True) -> Tuple[bool, str]:
    """True if every qualifier in ``message`` is reflected in ``sql``.

    Returns (ok, reason). On ``ok is False`` the caller should NOT run the SQL —
    abstain to the planner, or ask the user. ``use_heuristic_client=False`` limits
    client detection to ``known_clients`` (use when failing closed, to avoid
    false clarifications)."""
    m = " " + (message or "").lower().strip() + " "
    s = (sql or "").lower()
    is_rows = bool(re.search(r"select\s+\*", s))

    # A value/count question answered with a raw row dump is the wrong shape.
    if is_rows and _VALUE_RE.search(m):
        return False, "value question answered with a row list, not a total"
    if is_rows and _COUNT_RE.search(m):
        return False, "count question answered with a row list, not a count"

    # "paid"/"unpaid" stated but no payment predicate in the SQL.
    if _STATUS_RE.search(m) and "paid" not in s:
        return False, "paid/unpaid qualifier not reflected in SQL"

    # Invoice-dispatch status ("yet to invoice", "invoices sent") stated but no
    # bill_sent predicate in the SQL — a DIFFERENT qualifier from paid/unpaid.
    if _DISPATCH_RE.search(m) and "bill_sent" not in s:
        return False, "invoice-sent qualifier not reflected in SQL"

    # A specific client named but no client filter in the SQL.
    client = _client_in_message(m, known_clients, use_heuristic_client)
    if client and not _sql_has_client_filter(s):
        return False, f"client '{client}' not reflected in SQL"

    # A date/period stated but no date column constrained.
    if _DATE_RE.search(m) and "job_date" not in s and "invoice_date" not in s:
        return False, "date/period not reflected in SQL"

    return True, ""
