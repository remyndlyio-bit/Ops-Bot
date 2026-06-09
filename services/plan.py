"""
Typed Plan + canonical filter representation. The "Path 3" architecture.

Why this module exists
----------------------
For months we shipped the same bug class: the planner LLM emitted some
free-form string ("not_null", "IS NOT NULL", ["no","false","0"], null,
"yes", "bheja") and a downstream SQL builder somewhere in the codebase
either ILIKE'd it, missed it, or excluded NULLs. Each fix was local; the
class kept recurring because nothing CENTRALISED the rule "all values
that mean the same thing get normalised to one canonical form before SQL
ever runs."

This module is that central rule. Every raw planner output passes
through `Plan.from_raw()`, which:

  1. Validates the structure (operation/filters/etc. shape).
  2. For every filter value, asks the column registry for the canonical
     `CanonicalFilter` representation.
  3. Returns either a fully-typed `Plan` (every filter is a canonical
     object the SQL emitter can render deterministically) or a list of
     `NormalisationError`s naming exactly which filters it could not
     understand.

The SQL emitter no longer guesses. The synthesizer no longer sees
ambiguous "0 rows" when the issue was actually a malformed predicate.
And tests verify the normaliser, not the SQL string — so as long as the
canonical type is right, every variant of the same semantic meaning
produces the same SQL.

This is wired into `execute_query_plan` in shadow mode first (logs but
does not reject). When STRICT_PLAN_VALIDATION is enabled, validation
failures trigger an LLM retry with the error as feedback — the planner
literally cannot emit an unrecognised shape past the boundary.

Migration path
--------------
  Phase 3a (this commit): shadow mode. Validate, log mismatches as
    [PLAN_VALIDATOR_SHADOW]. Existing SQL builder unchanged.
  Phase 3b (next session): flip strict mode + retry. SQL emitter rewritten
    to consume canonical filters only. Legacy `_build_filter_clause`
    fallback deleted.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union


# ─────────────────────────────────────────────────────────────────────
# Canonical filter types — every filter value the planner can emit
# normalises into exactly ONE of these.
# ─────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class NullCheck:
    """IS NULL or IS NOT NULL on a column."""
    is_null: bool  # True → IS NULL, False → IS NOT NULL


@dataclass(frozen=True)
class BoolCheck:
    """Semantic truthy/falsy check for tri-state text columns like
    bill_sent and paid. The column registry decides the SQL predicate;
    this only carries the intent ('sent' vs 'not sent', 'paid' vs
    'unpaid')."""
    truthy: bool


@dataclass(frozen=True)
class Equality:
    """Exact equality against a literal (numeric, date, or text token)."""
    value: Union[str, int, float]


@dataclass(frozen=True)
class InList:
    """Column IN (v1, v2, ...). Used for explicit multi-value matches."""
    values: tuple  # tuple so the dataclass is hashable


@dataclass(frozen=True)
class Comparison:
    """Open-comparison: column <op> value. op ∈ {<, <=, >, >=, !=, =}."""
    op: str
    value: Union[str, int, float]


@dataclass(frozen=True)
class TextMatch:
    """Case-insensitive substring/text match. Will become ILIKE in SQL."""
    value: str


CanonicalFilter = Union[NullCheck, BoolCheck, Equality, InList, Comparison, TextMatch]


# ─────────────────────────────────────────────────────────────────────
# Normalisation errors
# ─────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class NormalisationError:
    """A filter value the registry could not map to any canonical form.
    Carries enough information to feed back into an LLM retry prompt."""
    column: str
    raw_value: Any
    reason: str

    def as_feedback(self) -> str:
        """Human-readable line for an LLM retry prompt."""
        return (
            f"- column '{self.column}': value {self.raw_value!r} is not a "
            f"recognised shape ({self.reason}). Use one of the documented "
            f"shapes for this column."
        )


# ─────────────────────────────────────────────────────────────────────
# Generic / centralised normalisation helpers
#
# Any column WITHOUT a custom normaliser in services/columns/ falls back
# to these. The point is to handle whitespace/case/underscore variants
# of NULL and boolean values ONCE, here, so every column inherits the
# fix for free.
# ─────────────────────────────────────────────────────────────────────

# Canonical NULL-intent tokens (after lowercase + space→underscore squash).
_NULL_TOKENS = {"is_null", "null", "isnull", ""}
_NOT_NULL_TOKENS = {"is_not_null", "not_null", "isnotnull", "any", "*"}

# Operator-prefix regex for things like "< 100", ">= 2026-03-14".
_OP_PREFIX_RE = re.compile(r"^(<=|>=|!=|<|>|=)\s*(.+)$")

# ISO date regex.
_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _squash(v: str) -> str:
    """Lowercase + collapse whitespace and underscores so 'IS NOT NULL',
    'is_not_null', 'is__not__null', 'Is Not  Null' all become
    'is_not_null'. Single source of truth for textual normalisation."""
    v = v.strip().lower()
    # collapse runs of spaces and underscores to a single underscore
    v = re.sub(r"[\s_]+", "_", v)
    return v.strip("_")


def _is_numeric(val: Any) -> bool:
    try:
        float(str(val).replace(",", ""))
        return True
    except (ValueError, TypeError):
        return False


def _is_date(val: Any) -> bool:
    return bool(_ISO_DATE_RE.match(str(val).strip()))


def generic_normalize_filter(column: str, raw: Any) -> Optional[CanonicalFilter]:
    """Default filter normalisation when the column registry does not
    register a custom normaliser. Returns None when no canonical form
    can be inferred — caller should raise NormalisationError so the
    LLM gets feedback.

    Every shape lives HERE; no per-column code should re-implement
    these. New variants of NULL / booleans / operator prefixes get
    added here once and every column benefits."""

    # None → IS NULL.
    if raw is None:
        return NullCheck(is_null=True)

    # List → either IN or normalised null/bool intent of the list as
    # a whole. We intentionally do NOT try to be clever per-element;
    # if a column wants semantic list handling (e.g. bill_sent with
    # falsy markers), it should register a custom normaliser.
    if isinstance(raw, list):
        if not raw:
            return None  # empty list is ambiguous; let column decide
        # If every element is the SAME null/bool-intent token, collapse.
        squashed = {_squash(str(v)) for v in raw}
        if squashed <= _NULL_TOKENS:
            return NullCheck(is_null=True)
        if squashed <= _NOT_NULL_TOKENS:
            return NullCheck(is_null=False)
        return InList(values=tuple(raw))

    # Dict with operator → Comparison.
    if isinstance(raw, dict):
        if "operator" in raw and "value" in raw:
            op = raw["operator"]
            if op not in ("<", "<=", ">", ">=", "=", "!="):
                return None
            return Comparison(op=op, value=raw["value"])
        return None  # unknown dict shape

    # Strings: try every shape in order.
    if isinstance(raw, str):
        sq = _squash(raw)
        if sq in _NULL_TOKENS:
            return NullCheck(is_null=True)
        if sq in _NOT_NULL_TOKENS:
            return NullCheck(is_null=False)

        # Operator-prefixed: "< 100", ">= 2026-03-14".
        m = _OP_PREFIX_RE.match(raw.strip())
        if m:
            op, v = m.group(1), m.group(2).strip()
            return Comparison(op=op, value=v)

        if _is_numeric(raw):
            try:
                return Equality(value=float(str(raw).replace(",", "")))
            except (ValueError, TypeError):
                pass
        if _is_date(raw):
            return Equality(value=raw.strip())

        # Default: substring/text match.
        return TextMatch(value=raw)

    if isinstance(raw, (int, float)):
        return Equality(value=raw)
    if isinstance(raw, bool):
        return BoolCheck(truthy=raw)

    return None  # truly unknown — caller raises NormalisationError


def normalize_filter(column: str, raw: Any) -> Optional[CanonicalFilter]:
    """Public entry-point: consult the column registry first, then fall
    back to the generic normaliser.

    Returns None ONLY when no canonical form can be inferred. Callers
    should treat None as a normalisation error."""
    try:
        from services.columns import get as _col_get
        spec = _col_get(column)
        if spec is not None:
            custom = getattr(spec, "normalize_filter", None)
            if custom is not None:
                # A registered column normaliser is AUTHORITATIVE.
                # When it returns None it is asserting "this value is not
                # a valid shape for this column" — we surface that as a
                # NormalisationError instead of silently falling through
                # to generic (which would happily ILIKE a junk date).
                return custom(raw)
    except Exception:
        # If the registry import fails (rare), the generic path still works.
        pass
    return generic_normalize_filter(column, raw)


# ─────────────────────────────────────────────────────────────────────
# Plan dataclass — the typed contract
# ─────────────────────────────────────────────────────────────────────

@dataclass
class Plan:
    """A validated, fully-canonical operation plan. Every filter value
    is a CanonicalFilter — the SQL emitter cannot encounter a raw string
    or a list it doesn't recognise.

    This is what `_build_filter_clause` will consume in Phase 3b. For
    now (Phase 3a) the legacy path still runs; this module's main job
    is to surface mismatches via [PLAN_VALIDATOR_SHADOW] logs."""

    operation: str
    sheet: Optional[str] = "sheet1"
    metric: Optional[str] = None
    column: Optional[str] = None
    filters: Dict[str, CanonicalFilter] = field(default_factory=dict)
    updates: Dict[str, Any] = field(default_factory=dict)
    values: Dict[str, Any] = field(default_factory=dict)
    time_range: Optional[Dict[str, Any]] = None
    group_by: Optional[str] = None
    limit: Optional[int] = None
    order: Optional[str] = None
    offset: Optional[int] = None
    confidence: str = "high"
    clarification_question: Optional[str] = None

    @classmethod
    def from_raw(
        cls,
        raw: Dict[str, Any],
        allowed_columns: Optional[List[str]] = None,
    ) -> "PlanResult":
        """Validate and normalise a raw planner dict.

        Returns a PlanResult with .plan set when validation succeeded and
        .errors set when it didn't. Errors and a partial plan can BOTH
        be present — useful for shadow-mode logging where we want to see
        what fell through without blocking the request.

        allowed_columns, when provided, makes unknown column references
        a validation error (we already check this in
        validate_plan_columns, but for the typed Plan we want it inline
        with normalisation feedback so the LLM retry message is complete).
        """
        errors: List[NormalisationError] = []
        allowed_set = set(allowed_columns) if allowed_columns else None

        op = raw.get("operation") or "query"
        if op not in ("query", "update", "create"):
            errors.append(NormalisationError(
                column="<operation>",
                raw_value=op,
                reason="operation must be query|update|create",
            ))
            op = "query"

        filters: Dict[str, CanonicalFilter] = {}
        raw_filters = raw.get("filters") or {}
        if isinstance(raw_filters, dict):
            for col, val in raw_filters.items():
                if not isinstance(col, str):
                    errors.append(NormalisationError(
                        column=str(col),
                        raw_value=val,
                        reason="filter key must be a string column name",
                    ))
                    continue
                if col.startswith("_"):
                    # internal markers like _resolve_latest — leave alone
                    continue
                if allowed_set is not None and col not in allowed_set:
                    errors.append(NormalisationError(
                        column=col,
                        raw_value=val,
                        reason=f"column not in schema (allowed: "
                               f"{sorted(allowed_set)[:8]}...)",
                    ))
                    continue
                canonical = normalize_filter(col, val)
                if canonical is None:
                    errors.append(NormalisationError(
                        column=col,
                        raw_value=val,
                        reason="no canonical form (registry + generic "
                               "normaliser both fell through)",
                    ))
                    continue
                filters[col] = canonical
        elif raw_filters is not None:
            errors.append(NormalisationError(
                column="<filters>",
                raw_value=raw_filters,
                reason="filters must be an object/dict or null",
            ))

        plan = cls(
            operation=op,
            sheet=raw.get("sheet") or "sheet1",
            metric=raw.get("metric"),
            column=raw.get("column"),
            filters=filters,
            updates=raw.get("updates") or {},
            values=raw.get("values") or {},
            time_range=raw.get("time_range"),
            group_by=raw.get("group_by"),
            limit=raw.get("limit"),
            order=raw.get("order"),
            offset=raw.get("offset"),
            confidence=raw.get("confidence") or "high",
            clarification_question=raw.get("clarification_question"),
        )
        return PlanResult(plan=plan, errors=errors)


@dataclass(frozen=True)
class PlanResult:
    """Outcome of Plan.from_raw. `errors` is empty on a fully-valid plan;
    when non-empty, the plan is partial (filters that normalised cleanly
    are still populated, so shadow-mode logging can describe both what
    worked and what didn't)."""
    plan: Plan
    errors: List[NormalisationError]

    @property
    def valid(self) -> bool:
        return not self.errors

    def feedback_for_retry(self) -> str:
        """Compose a feedback string suitable for re-prompting the LLM."""
        if not self.errors:
            return ""
        return (
            "Your previous plan had filter values I could not understand:\n"
            + "\n".join(e.as_feedback() for e in self.errors)
            + "\nReturn a corrected plan using only the documented shapes."
        )
