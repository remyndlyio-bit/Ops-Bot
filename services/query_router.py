"""
Deterministic-first query router.

Maps the ~20 common natural-language query shapes users send every day DIRECTLY
to SQL, BEFORE the LLM planner runs. The planner remains the fallback for novel
queries it hasn't seen.

Why this exists
---------------
The planner LLM is handed a ~90-rule prompt and asked to map fuzzy language into
a rigid JSON plan, which a deterministic builder then turns into SQL. For common
queries that round-trip is lossy and unreliable — e.g. "highest paying job" came
back sorted by DATE instead of fees because the planner left `column` null. For
unambiguous, high-frequency shapes we don't need an LLM to guess: the mapping is
known, so we encode it as code. Result: fast, free, and reliable for the queries
that actually matter, with the planner reserved for the long tail.

Design
------
- ``route_common_query(message, user_id) -> RoutedQuery | None``  (PURE — no I/O)
- Each route is a small function ``(msg, uid) -> RoutedQuery | None``.
- ``_ROUTES`` lists them most-specific-first; the first match wins.
- ``RoutedQuery.render`` tells the caller how to present the rows. The two
  deterministic renders (client list, payment status) are formatted here so the
  whole path is testable without the DB or the synthesiser.

Adding a route: write a ``_route_*`` function, slot it into ``_ROUTES`` at the
right specificity, and add a test in ``tests/test_query_router.py``.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, List, Optional

# ── Render kinds ────────────────────────────────────────────────────────────
ROWS = "rows"                 # full job rows → job cards / excel / synthesiser
AGGREGATE = "aggregate"       # single numeric result → number-style reply
CLIENT_LIST = "client_list"   # DISTINCT client names → deterministic bullet list
PAYMENT_STATUS = "payment_status"  # has-X-paid → deterministic paid/unpaid summary

# ── SQL building blocks ─────────────────────────────────────────────────────
NOT_DELETED = '("isDeleted" IS NOT TRUE)'
CLIENT_EXPR = "COALESCE(NULLIF(client_name,''),NULLIF(brand_name,''),NULLIF(production_house,''))"
PAID_TRUE = "LOWER(COALESCE(paid,'')) IN ('true','t','yes','1','paid')"
PAID_FALSE = "(paid IS NULL OR TRIM(COALESCE(paid,''))='' OR LOWER(paid) NOT IN ('true','t','yes','1','paid'))"


def _base(uid: str) -> str:
    return f"SELECT * FROM public.job_entries WHERE user_id='{uid}' AND {NOT_DELETED}"


@dataclass(frozen=True)
class RoutedQuery:
    """A deterministic match: the SQL to run plus how to render its rows."""
    name: str
    sql: str
    render: str
    meta: Dict = field(default_factory=dict)


# A "client word" and a "count word" reused across routes.
_CLIENT_WORD = r"(?:clients?|brands?|companies|compan(?:y|ies)|customers?)"
_COUNT_WORD = r"(?:how\s+many|count|kitne|number\s+of)"
_JOB_WORD = r"\b(?:job|project|work|gig|invoice|earner|payday|paycheck)\b"


# ════════════════════════════════════════════════════════════════════════════
# Routes — ordered most-specific first in _ROUTES below.
# Each returns a RoutedQuery on match, or None to let the next route try.
# ════════════════════════════════════════════════════════════════════════════

def _route_payment_status(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Has Nike paid?" / "Did Star Studios pay?" / "Is Acme cleared?" """
    m = re.search(r'\b(?:has|have|did|is|are)\s+(.+?)\s+(?:paid|pay|cleared|settled)\b', msg)
    if not m:
        return None
    client = m.group(1).strip().strip("?.,").strip()
    if not client or client in (
        "i", "me", "you", "they", "we", "it", "everyone", "anyone", "all",
        "the client", "my client", "any client", "the clients", "my clients",
    ):
        return None
    c = client.replace("'", "''")
    sql = (
        f"SELECT {CLIENT_EXPR} AS client_name, job_date, fees, paid, bill_no "
        f"FROM public.job_entries WHERE user_id='{uid}' AND {NOT_DELETED} "
        f"AND (client_name ILIKE '%{c}%' OR brand_name ILIKE '%{c}%' OR production_house ILIKE '%{c}%') "
        f"ORDER BY job_date DESC NULLS LAST"
    )
    return RoutedQuery("payment_status", sql, PAYMENT_STATUS, {"client": client})


def _route_client_owes(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"How much does X owe me?" / "X ka paisa" / "X se paisa aaya kya" — unpaid SUM."""
    m = re.search(
        r'\b(?:how\s+much\s+does\s+(.+?)\s+owe\s+me'
        r'|(.+?)\s+(?:ka\s+paisa|se\s+paisa\s+aaya|ka\s+payment))\b',
        msg,
    )
    if not m:
        return None
    client = (m.group(1) or m.group(2) or "").strip().strip("?").strip()
    if not client or len(client) <= 1:
        return None
    c = client.replace("'", "''")
    sql = (
        f"SELECT SUM(fees) AS result FROM public.job_entries "
        f"WHERE user_id='{uid}' AND {NOT_DELETED} "
        f"AND ({CLIENT_EXPR} ILIKE '%{c}%') AND {PAID_FALSE}"
    )
    return RoutedQuery("client_owes", sql, AGGREGATE, {"client": client})


def _route_clients_paid_list(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Which clients have paid / haven't paid?" — list names, not a count."""
    if not re.search(rf'\b(which|what|list|show|name)\b.{{0,30}}\b{_CLIENT_WORD}\b', msg):
        return None
    if re.search(rf'\b{_COUNT_WORD}\b', msg):
        return None
    wants_unpaid = bool(re.search(
        r"\b(haven'?t\s+paid|hasn'?t\s+paid|not\s+(?:yet\s+)?paid|unpaid|pending|outstanding|owe|baki|baaki)\b", msg))
    wants_paid = bool(re.search(
        r'\b(have\s+paid|has\s+paid|already\s+paid|who\s+paid|paid\s+(?:me|up)?)\b', msg)) and not wants_unpaid
    if not (wants_paid or wants_unpaid):
        return None
    clause = PAID_FALSE if wants_unpaid else PAID_TRUE
    sql = (
        f"SELECT DISTINCT {CLIENT_EXPR} AS client_name "
        f"FROM public.job_entries WHERE user_id='{uid}' AND {NOT_DELETED} "
        f"AND {CLIENT_EXPR} IS NOT NULL AND {clause} ORDER BY 1"
    )
    return RoutedQuery("clients_paid_list", sql, CLIENT_LIST,
                       {"status": "unpaid" if wants_unpaid else "paid"})


def _route_biggest_client(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Biggest / top / largest client" — grouped SUM, top 1."""
    if not re.search(rf'\b(biggest|top|largest|best|highest[- ]paying)\b.{{0,30}}\b{_CLIENT_WORD}\b', msg):
        return None
    sql = (
        f"SELECT {CLIENT_EXPR} AS client_name, SUM(fees) AS result "
        f"FROM public.job_entries WHERE user_id='{uid}' AND {NOT_DELETED} "
        f"GROUP BY 1 HAVING {CLIENT_EXPR} IS NOT NULL ORDER BY result DESC LIMIT 1"
    )
    return RoutedQuery("biggest_client", sql, ROWS)


def _route_earnings_by_client(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Earnings by client" / "fees per brand" / "revenue breakdown by client"."""
    hit = (
        re.search(r'\b(earnings?|fees?|billing|revenue|income)\b.{0,20}\b(by|per|for each|breakdown)\b.{0,20}\b(client|brand|company)\b', msg)
        or re.search(r'\b(by|per|for each)\b.{0,20}\b(client|brand)\b.{0,20}\b(earnings?|fees?|billing)\b', msg)
        or re.search(r'\b(show|list)\b.{0,20}\b(earnings?|income|revenue)\b.{0,20}\b(client|brand)\b', msg)
    )
    if not hit:
        return None
    sql = (
        f"SELECT {CLIENT_EXPR} AS client_name, SUM(fees) AS result "
        f"FROM public.job_entries WHERE user_id='{uid}' AND {NOT_DELETED} "
        f"GROUP BY 1 HAVING {CLIENT_EXPR} IS NOT NULL ORDER BY result DESC"
    )
    return RoutedQuery("earnings_by_client", sql, ROWS)


def _route_top_bottom_job(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Highest / lowest paying job" — single row ordered by FEES (not date)."""
    is_top = bool(
        re.search(rf'\b(highest[- ]?paying|most expensive|biggest|top[- ]?earning|largest|fattest)\b.{{0,20}}{_JOB_WORD}', msg)
        or re.search(rf'{_JOB_WORD}.{{0,20}}\b(highest[- ]?paying|most expensive|biggest|largest|most|paid me the most)\b', msg)
        or re.search(r'\b(highest[- ]?paying|most expensive|biggest|top[- ]?earning|fattest)\b.{0,20}\b(pay(ing|day|check)?|fee|earning)\b', msg)
    )
    is_bottom = bool(
        re.search(rf'\b(lowest[- ]?paying|least expensive|smallest|cheapest|worst[- ]?paying)\b.{{0,20}}{_JOB_WORD}', msg)
        or re.search(rf'{_JOB_WORD}.{{0,20}}\b(lowest[- ]?paying|smallest|cheapest|least)\b', msg)
    )
    if not (is_top or is_bottom):
        return None
    # Must be about a JOB, not a CLIENT (the biggest-client route owns that).
    if re.search(rf'\b{_CLIENT_WORD}\b', msg):
        return None
    direction = "ASC" if (is_bottom and not is_top) else "DESC"
    sql = (
        f"{_base(uid)} AND fees IS NOT NULL ORDER BY fees {direction} NULLS LAST LIMIT 1"
    )
    return RoutedQuery("top_bottom_job", sql, ROWS, {"direction": direction})


def _route_list_clients(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Show all my clients" / "list clients" — DISTINCT names (not a count)."""
    if not re.search(rf'\b(show|list|all|which|my)\b.{{0,20}}\b{_CLIENT_WORD}\b', msg):
        return None
    if re.search(rf'\b{_COUNT_WORD}\b|\b(paid|unpaid|invoice|jobs?)\b', msg):
        return None
    sql = (
        f"SELECT DISTINCT {CLIENT_EXPR} AS client_name "
        f"FROM public.job_entries WHERE user_id='{uid}' AND {NOT_DELETED} "
        f"AND {CLIENT_EXPR} IS NOT NULL ORDER BY 1"
    )
    return RoutedQuery("list_clients", sql, CLIENT_LIST, {"status": "all"})


def _route_average_fees(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Average fees per job" / "average billing" / "औसत"."""
    if not re.search(r'\b(average|avg|औसत)\b.{0,30}\b(fees?|billing|earnings?|amount|income)\b', msg):
        return None
    sql = (
        f"SELECT AVG(fees) AS result FROM public.job_entries "
        f"WHERE user_id='{uid}' AND {NOT_DELETED} AND fees IS NOT NULL"
    )
    return RoutedQuery("average_fees", sql, AGGREGATE)


def _route_count_jobs(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"How many jobs / total number of jobs / kitne jobs"."""
    if not re.search(r'\b(how\s+many|count|total\s+number\s+of|number\s+of|kitne)\b.*\b(jobs?|entr(?:y|ies)?|records?|work|kaam)\b', msg):
        return None
    sql = f"SELECT COUNT(*) AS result FROM public.job_entries WHERE user_id='{uid}' AND {NOT_DELETED}"
    return RoutedQuery("count_jobs", sql, AGGREGATE)


def _route_total_fees(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Total billing / total earnings / overall revenue" (no client, no date)."""
    if not re.search(r'\b(total|sum|overall)\b.*\b(fees?|earnings?|income|revenue|billing)\b', msg):
        return None
    # Defer to the planner when a date phrase is present — date math belongs there.
    if re.search(r'\b(today|yesterday|week|month|quarter|year|q[1-4]|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec|20\d{2}|last|this)\b', msg):
        return None
    sql = f"SELECT SUM(fees) AS result FROM public.job_entries WHERE user_id='{uid}' AND {NOT_DELETED}"
    return RoutedQuery("total_fees", sql, AGGREGATE)


def _route_hinglish_earnings(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Kitna paisa aaya" / "kamai" — total SUM (no client qualifier, no date)."""
    if not re.search(r'\b(paisa\s+aaya|paisa\s+mila|kamai|kamaya|kitna\s+mila)\b', msg):
        return None
    if re.search(r'\b(se\s+paisa\s+aaya|ka\s+paisa|ka\s+payment|se\s+paisa\s+mila)\b', msg):
        return None
    # Date-qualified Hinglish earnings ("pichle mahine ki kamai") → planner.
    if re.search(r'\b(mahine|mahina|saal|hafte|quarter|pichle|pichhle|is\s+mahine|aaj|kal)\b', msg):
        return None
    sql = f"SELECT SUM(fees) AS result FROM public.job_entries WHERE user_id='{uid}' AND {NOT_DELETED}"
    return RoutedQuery("hinglish_earnings", sql, AGGREGATE)


def _route_date_lookup(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"What did I do on 10 April?" — jobs on an exact date."""
    m = re.search(
        r'\b(?:what\s+did\s+i\s+do|what\s+was|show\s+(?:me\s+)?jobs?)\s+on\s+'
        r'(\d{1,2}\s+(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*(?:\s+\d{4})?'
        r'|(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)\w*\s+\d{1,2}(?:\s*,?\s*\d{4})?)',
        msg,
    )
    if not m:
        return None
    raw = m.group(1).strip()
    cur_year = datetime.now().year
    iso = None
    has_year = bool(re.search(r'\b\d{4}\b', raw))
    if has_year:
        candidates = (("%d %B %Y", raw), ("%d %b %Y", raw), ("%B %d %Y", raw), ("%b %d, %Y", raw), ("%b %d %Y", raw))
    else:
        # Append the current year so parsing is unambiguous (avoids the default-1900
        # DeprecationWarning and the Python 3.15 behaviour change for yearless dates).
        candidates = (("%d %B %Y", f"{raw} {cur_year}"), ("%d %b %Y", f"{raw} {cur_year}"),
                      ("%B %d %Y", f"{raw} {cur_year}"), ("%b %d %Y", f"{raw} {cur_year}"))
    for fmt, text in candidates:
        try:
            iso = datetime.strptime(text, fmt).strftime("%Y-%m-%d")
            break
        except ValueError:
            continue
    if not iso:
        return None
    sql = f"{_base(uid)} AND job_date = '{iso}' ORDER BY job_date DESC"
    return RoutedQuery("date_lookup", sql, ROWS, {"date": iso})


def _route_last_job(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Last / latest / most recent job"."""
    if not re.search(r'\b(last|latest|most\s+recent|recent)\b.*\b(jobs?|entr(?:y|ies)?|work|project|gig)\b', msg):
        return None
    sql = f"{_base(uid)} ORDER BY job_date DESC NULLS LAST LIMIT 1"
    return RoutedQuery("last_job", sql, ROWS)


def _route_unpaid_list(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Unpaid / pending / outstanding" — list of unpaid jobs."""
    if not re.search(r'\b(unpaid|pending|not\s+paid|outstanding)\b', msg):
        return None
    # "which clients unpaid" is handled by the client-list route earlier.
    sql = f"{_base(uid)} AND {PAID_FALSE} ORDER BY job_date DESC NULLS LAST LIMIT 25"
    return RoutedQuery("unpaid_list", sql, ROWS)


def _route_list_jobs(msg: str, uid: str) -> Optional[RoutedQuery]:
    """"Show all jobs / list my jobs" — recent jobs list."""
    if not re.search(r'\b(show|list|all|my)\b.*\b(jobs?|entr(?:y|ies)?|records?|work)\b', msg):
        return None
    sql = f"{_base(uid)} ORDER BY job_date DESC NULLS LAST LIMIT 25"
    return RoutedQuery("list_jobs", sql, ROWS)


# Order matters: most-specific first. A query matches at most ONE route.
_ROUTES: List[Callable[[str, str], Optional[RoutedQuery]]] = [
    _route_payment_status,
    _route_client_owes,
    _route_clients_paid_list,
    _route_biggest_client,
    _route_earnings_by_client,
    _route_top_bottom_job,
    _route_list_clients,
    _route_average_fees,
    _route_count_jobs,
    _route_total_fees,
    _route_hinglish_earnings,
    _route_date_lookup,
    _route_last_job,
    _route_unpaid_list,
    _route_list_jobs,
]


def route_common_query(message: str, user_id: str) -> Optional[RoutedQuery]:
    """Return a deterministic RoutedQuery for a common query shape, or None.

    PURE: no DB / LLM calls. The caller executes ``.sql`` and renders by
    ``.render``. None means "not a common shape — hand off to the LLM planner".
    """
    if not message or not message.strip():
        return None
    msg = message.strip().lower()
    uid = (user_id or "").replace("'", "''")
    for route in _ROUTES:
        try:
            result = route(msg, uid)
        except Exception:
            # A misbehaving route must never break the request — skip it.
            result = None
        if result is not None:
            return result
    return None


# ════════════════════════════════════════════════════════════════════════════
# Deterministic renderers for the non-LLM render kinds.
# ════════════════════════════════════════════════════════════════════════════

def format_client_list(rows: List[Dict], status: str = "all") -> str:
    """Render a DISTINCT client-name result as a bullet list."""
    names = [str(r.get("client_name") or "").strip() for r in (rows or [])]
    names = [n for n in names if n]
    if not names:
        if status == "unpaid":
            return "No clients with outstanding payments."
        if status == "paid":
            return "No payments recorded yet."
        return "You don't have any clients on record yet."
    if status == "unpaid":
        header = "Clients who haven't paid yet:"
    elif status == "paid":
        header = "Clients who have paid:"
    else:
        header = "Your clients:"
    return header + "\n" + "\n".join(f"• {n}" for n in names)


def _is_paid(value) -> bool:
    return str(value or "").strip().lower() in ("true", "t", "yes", "1", "paid")


def format_payment_status(rows: List[Dict], meta: Dict) -> str:
    """Render a per-job payment-status result ("Has Nike paid?")."""
    client = (meta or {}).get("client", "that client")
    if not rows:
        return f"I don't have any jobs on record for {client}."
    paid_rows = [r for r in rows if _is_paid(r.get("paid"))]
    unpaid_rows = [r for r in rows if not _is_paid(r.get("paid"))]
    disp = (rows[0].get("client_name") or client).strip()
    paid_total = sum(float(r.get("fees") or 0) for r in paid_rows)
    unpaid_total = sum(float(r.get("fees") or 0) for r in unpaid_rows)

    def _jobs(n: int) -> str:
        return f"{n} job" + ("s" if n != 1 else "")

    if not unpaid_rows:
        return f"Yes — {disp} has paid in full (₹{int(paid_total):,} across {_jobs(len(paid_rows))})."
    if not paid_rows:
        return f"No — {disp} hasn't paid yet. ₹{int(unpaid_total):,} outstanding across {_jobs(len(unpaid_rows))}."
    return (
        f"{disp}: ₹{int(paid_total):,} paid, ₹{int(unpaid_total):,} still outstanding "
        f"({_jobs(len(unpaid_rows))} unpaid)."
    )
