"""
AnswerLedger — WP-1 of ASSISTANT_PLAN.md.

The bot's memory of its OWN claims. Before this, the bot cached the last row
shown (uscf_context.last_row_data) but never the SCOPE of an aggregate answer
— the filters and time_range that produced it. So a question like "Do these
include, paid and unpaid?" had nothing to answer from: the cached context for
an aggregate is just {"result": 75000}, no "paid" key, nothing to read.

The ledger fixes that at the source: every query answer is stored as a typed
claim — {question, kind, scope (filters + time_range, straight from the same
Path-3 plan that produced the SQL — so the spoken scope can never disagree
with what was actually executed), value, row_ids, surface}. A small,
deterministic (zero-LLM) reader answers scope-clarifying follow-ups directly
from ledger[-1], and is fast: no SQL, no LLM call, just a template render.

Storage: piggybacks on MemoryService's existing per-user JSONB blob (no new
table) under the "answer_ledger" key. Schema-versioned so a future shape
change is a read-time upgrade, not a silent wipe.
"""
import re
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

LEDGER_SCHEMA_VERSION = 1
_MAX_ENTRIES = 10


@dataclass
class LedgerEntry:
    # All fields defaulted (even the conceptually "required" ones) so
    # from_dict() can never raise on a partial/empty dict — deserialising a
    # cache blob must degrade gracefully, never crash a turn.
    question: str = ""
    kind: str = "list"                 # "aggregate" | "list" | "field" | "action"
    scope: Dict[str, Any] = field(default_factory=dict)  # {"filters": {...}, "time_range": {...}|None}
    value: Any = None                  # the number / client name / row count given
    row_ids: List[str] = field(default_factory=list)
    surface: str = ""                  # first 200 chars of the reply actually sent
    turn_id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    ts: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "LedgerEntry":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in (d or {}).items() if k in known})


def _empty_state() -> Dict[str, Any]:
    return {"v": LEDGER_SCHEMA_VERSION, "entries": []}


def _load_state(user_mem: Dict[str, Any]) -> Dict[str, Any]:
    raw = user_mem.get("answer_ledger")
    if not isinstance(raw, dict) or raw.get("v") != LEDGER_SCHEMA_VERSION:
        # Missing, malformed, or an older/future schema version — start clean
        # rather than guess at a migration. A ledger is a cache of recent
        # conversational context, not durable data, so losing it costs one
        # turn of "I don't have that in context yet", never correctness.
        return _empty_state()
    return raw


def get_entries(user_mem: Dict[str, Any]) -> List[LedgerEntry]:
    state = _load_state(user_mem)
    return [LedgerEntry.from_dict(e) for e in state.get("entries", [])]


def append_entry(memory, user_id: str, entry: LedgerEntry) -> None:
    """Append one claim to the user's ledger, trimmed to the most recent
    _MAX_ENTRIES. Never raises — a ledger-write failure must not break the
    turn that produced a perfectly good answer."""
    try:
        user_mem = memory.get_user_memory(user_id) or {}
        state = _load_state(user_mem)
        entries = state.get("entries", [])
        entries.append(entry.to_dict())
        state["entries"] = entries[-_MAX_ENTRIES:]
        memory.update_user_memory(user_id, {"answer_ledger": state})
    except Exception:
        pass


def build_entry_from_sql(*, question: str, scope: Dict[str, Any], rows: List[Dict],
                          response: str, render_kind: str) -> LedgerEntry:
    """Alternate constructor for the deterministic router path — same output
    shape as build_entry(), except `scope` comes straight from the
    RoutedQuery that produced the SQL (query_router.py — every route sets
    its own `filters`/`time_range` explicitly) instead of being
    reverse-engineered from the SQL text (P1-1, PLAN_OF_ACTION.md).

    The regex-based scope_from_sql() this replaced had no vocabulary for
    date filters at all (every date-qualified route disclosed "no date
    filter — all time" regardless of what actually ran) and missed a
    client filter wrapped in query_router.CLIENT_EXPR's COALESCE instead
    of a bare `client_name ILIKE`. Passing the route's own scope can't
    drift from what it actually put in the SQL, by construction.
    """
    kind = "aggregate" if render_kind == "AGGREGATE" else "list"
    value = rows[0].get("result") if (kind == "aggregate" and rows) else len(rows)
    row_ids = [str(r["id"]) for r in rows if r.get("id") is not None][:25]
    return LedgerEntry(
        question=question, kind=kind, scope=scope,
        value=value, row_ids=row_ids, surface=(response or "")[:200],
    )


def build_entry(*, question: str, plan: Optional[Dict], rows: List[Dict],
                 response: str) -> LedgerEntry:
    """Construct a LedgerEntry from a completed query answer. `plan` is the
    exact Path-3 plan dict that produced the SQL (metric/filters/time_range),
    so scope is always faithful to what actually ran."""
    plan = plan or {}
    filters = plan.get("filters") or {}
    time_range = plan.get("time_range")
    metric = plan.get("metric")

    if metric in ("sum", "avg", "count"):
        kind = "aggregate"
        value = rows[0].get("result") if rows else None
    else:
        kind = "list"
        value = len(rows)

    row_ids = [str(r["id"]) for r in rows if r.get("id") is not None][:25]

    return LedgerEntry(
        question=question,
        kind=kind,
        scope={"filters": filters, "time_range": time_range},
        value=value,
        row_ids=row_ids,
        surface=(response or "")[:200],
    )


def _format_inr(n: int) -> str:
    """Indian digit grouping (₹11,75,000 — last 3 digits, then pairs), NOT
    Python's default Western grouping (₹1,175,000). The bot's LLM-synthesized
    answers already use Indian grouping per the domain glossary
    (knowledge/rules.py: "currency_is_inr" -> "format with Indian grouping,
    e.g. Rs 1,75,000"); this reader has no LLM in the loop, so it needs its
    own formatter or the same number would read inconsistently between the
    original answer and this follow-up about it."""
    neg = n < 0
    s = str(abs(int(n)))
    if len(s) <= 3:
        out = s
    else:
        last3, rest = s[-3:], s[:-3]
        groups = []
        while len(rest) > 2:
            groups.insert(0, rest[-2:])
            rest = rest[:-2]
        if rest:
            groups.insert(0, rest)
        out = ",".join(groups) + "," + last3
    return f"-{out}" if neg else out


# ── Deterministic scope-question reader (zero LLM) ──────────────────────────

_SCOPE_QUESTION_RE = re.compile(
    r"\b(?:do(?:es)?\s+(?:this|that|these|those|it)\s+includ|"
    r"is\s+(?:this|that)\s+(?:only|just)|"
    r"what\s+does\s+(?:this|that)\s+includ|"
    r"is\s+that\s+(?:all|everyone|everything|only)|"
    r"(?:includes?|include)\s+(?:paid\s+and\s+unpaid|both)\b)"
)


def is_scope_question(message: str) -> bool:
    return bool(_SCOPE_QUESTION_RE.search((message or "").lower()))


def _describe_scope(scope: Dict[str, Any]) -> str:
    filters = (scope or {}).get("filters") or {}
    time_range = (scope or {}).get("time_range")
    parts = []

    paid = filters.get("paid")
    if paid == "yes":
        parts.append("only the PAID ones")
    elif paid == "no":
        parts.append("only the UNPAID ones")
    else:
        parts.append("both paid and unpaid")

    bill_sent = filters.get("bill_sent")
    if bill_sent == "yes":
        parts.append("invoices that have been sent")
    elif bill_sent == "no":
        parts.append("invoices not yet sent")

    client = filters.get("client_name")
    if client:
        parts.append(f"just {client}")
    else:
        parts.append("every client")

    if time_range and time_range.get("value"):
        v = time_range["value"]
        parts.append(f"{v.get('start', '')} to {v.get('end', '')}")
    else:
        parts.append("no date filter — all time")

    return ", ".join(parts)


# Public aliases — ASSISTANT_PLAN.md WP-4's AnswerPayload (services/
# response_synthesis.py) reuses these instead of re-implementing Indian
# currency formatting and English scope description a second time. Kept as
# aliases (not renames) so nothing here or in existing tests has to change.
format_inr = _format_inr
describe_scope = _describe_scope


def answer_scope_question(message: str, user_mem: Dict[str, Any]) -> Optional[str]:
    """Deterministic reader: if `message` is a scope-clarifying question about
    the immediately previous answer, render a reply from ledger[-1] — no SQL,
    no LLM call. Returns None if the message isn't scope-shaped, there's no
    ledger yet, or the last entry was an action (mark-paid etc.) rather than
    a query — those have no "scope" to clarify."""
    if not is_scope_question(message):
        return None
    entries = get_entries(user_mem)
    if not entries:
        return None
    last = entries[-1]
    if last.kind not in ("aggregate", "list"):
        return None

    scope_desc = _describe_scope(last.scope)
    if last.kind == "aggregate" and last.value is not None:
        try:
            value_str = f"₹{_format_inr(int(last.value))}"
        except (TypeError, ValueError):
            value_str = str(last.value)
        return f"Yes — that {value_str} covers {scope_desc}."
    return f"That covers {scope_desc}."
