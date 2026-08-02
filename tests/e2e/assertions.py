"""
TODO.md Phase 4.2 — machine-checkable assertion primitives. "NO eyeball
grading."

A scenario is `(message, [assertion, ...])`. Each assertion is a callable
object that takes a `ScenarioContext` (the process_request result, the
synthetic user id, and an optional DB accessor) and returns an `Outcome`
carrying pass/fail PLUS a human-readable detail string. The detail is what
makes Phase 4.3's `last_run.json` actionable — "failed" alone tells you
nothing at 3am; "expected ₹2,64,000, response said ₹2,46,000" tells you
everything.

Robustness decisions that matter (each one is a false-failure this
harness would otherwise produce on correct bot behaviour):

* **Digit grouping is normalised away.** The bot formats currency with
  INDIAN grouping (`₹11,75,000` — see answer_ledger._format_inr), while an
  LLM-synthesised answer may use Western (`₹1,175,000`). Rather than
  guessing which, `contains_amount` strips commas that sit *between
  digits* from both sides, so every grouping convention compares equal.
  The lookaround is deliberate: a blanket comma strip would fuse
  "3 jobs, 5 clients" into "3 jobs 5 clients" and could create phantom
  matches.

* **Apostrophes are normalised.** "couldn't" and "couldn’t" are the same
  word to a reader; only one of them matches a naive `in` check. Both
  sides get folded to a straight quote.

* **Text matching is case- and whitespace-insensitive by default.** The
  bot's phrasing varies with LLM synthesis; asserting on exact casing
  would make the suite brittle in a way that measures nothing real.

DB-backed assertions (`row_created`, `row_count_is`) require the context to
carry a `db` callable. They fail loudly rather than silently passing when
one isn't wired — a DB assertion that quietly no-ops is worse than no
assertion, because it reads green.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Sequence


# ─────────────────────────────────────────────────────────────────────
# Core types
# ─────────────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class Outcome:
    """Result of one assertion. `detail` explains the check either way —
    on failure it must contain enough to diagnose without a re-run."""
    passed: bool
    detail: str


@dataclass
class ScenarioContext:
    """Everything an assertion may inspect.

    `result` is the dict process_request returns
    ({operation, response, trigger_invoice, invoice_data}).
    `db` is an optional `(sql, params) -> list[dict]` accessor; only the
    DB-backed assertions need it.
    """
    result: Dict[str, Any]
    user_id: str
    db: Optional[Callable[[str, Sequence[Any]], List[Dict[str, Any]]]] = None

    @property
    def response(self) -> str:
        return str((self.result or {}).get("response") or "")

    @property
    def operation(self) -> str:
        return str((self.result or {}).get("operation") or "")


class Assertion:
    """Base class. Subclasses implement `check`; `description` is used in
    test ids and the 4.3 report."""

    description: str = "assertion"

    def check(self, ctx: ScenarioContext) -> Outcome:  # pragma: no cover
        raise NotImplementedError

    def __call__(self, ctx: ScenarioContext) -> Outcome:
        return self.check(ctx)

    def __repr__(self) -> str:
        return f"<{self.description}>"


# ─────────────────────────────────────────────────────────────────────
# Normalisation helpers
# ─────────────────────────────────────────────────────────────────────

_APOSTROPHES = dict.fromkeys(map(ord, "‘’ʼ´`"), "'")

# Commas BETWEEN digits only — see the module docstring on why a blanket
# strip is wrong.
_DIGIT_COMMA = re.compile(r"(?<=\d),(?=\d)")


def normalise_text(s: str) -> str:
    """Fold apostrophe variants, collapse whitespace, lowercase."""
    s = (s or "").translate(_APOSTROPHES)
    return re.sub(r"\s+", " ", s).strip().lower()


def normalise_digits(s: str) -> str:
    """Remove digit-grouping commas so Indian/Western/raw compare equal."""
    return _DIGIT_COMMA.sub("", s or "")


def _snippet(s: str, limit: int = 160) -> str:
    s = re.sub(r"\s+", " ", (s or "").strip())
    return s if len(s) <= limit else s[:limit] + "…"


# ─────────────────────────────────────────────────────────────────────
# Text assertions
# ─────────────────────────────────────────────────────────────────────

class contains(Assertion):
    """Response contains `text` (case/whitespace/apostrophe-insensitive)."""

    def __init__(self, text: str, *, case_sensitive: bool = False):
        self.text = text
        self.case_sensitive = case_sensitive
        self.description = f"contains({text!r})"

    def check(self, ctx: ScenarioContext) -> Outcome:
        hay, needle = ctx.response, self.text
        if not self.case_sensitive:
            hay, needle = normalise_text(hay), normalise_text(needle)
        if needle in hay:
            return Outcome(True, f"found {self.text!r}")
        return Outcome(False, f"expected {self.text!r} in response; got: {_snippet(ctx.response)!r}")


class not_contains(Assertion):
    """Response does NOT contain `text`. The 4.2 spec's canonical use is
    `not contains("couldn't")` — catching the bot's own failure phrasing."""

    def __init__(self, text: str, *, case_sensitive: bool = False):
        self.text = text
        self.case_sensitive = case_sensitive
        self.description = f"not_contains({text!r})"

    def check(self, ctx: ScenarioContext) -> Outcome:
        hay, needle = ctx.response, self.text
        if not self.case_sensitive:
            hay, needle = normalise_text(hay), normalise_text(needle)
        if needle in hay:
            return Outcome(False, f"unexpected {self.text!r} in response: {_snippet(ctx.response)!r}")
        return Outcome(True, f"{self.text!r} absent as expected")


class matches(Assertion):
    """Regex escape hatch for shapes the named primitives don't cover."""

    def __init__(self, pattern: str, *, flags: int = re.IGNORECASE):
        self.pattern = pattern
        self._rx = re.compile(pattern, flags)
        self.description = f"matches({pattern!r})"

    def check(self, ctx: ScenarioContext) -> Outcome:
        if self._rx.search(ctx.response):
            return Outcome(True, f"matched /{self.pattern}/")
        return Outcome(False, f"no match for /{self.pattern}/ in: {_snippet(ctx.response)!r}")


class contains_number(Assertion):
    """Response contains at least one number.

    The 4.2 workhorse: "how many invoices have I sent?" is correct if it
    answers with a figure at all, and pinning the exact value would make
    the assertion depend on fixture arithmetic the scenario may not care
    about. Use `contains_amount` when the value itself matters.
    """

    description = "contains_number"

    def check(self, ctx: ScenarioContext) -> Outcome:
        m = re.search(r"\d", ctx.response)
        if m:
            return Outcome(True, f"found digits in: {_snippet(ctx.response, 80)!r}")
        return Outcome(False, f"expected a number; got: {_snippet(ctx.response)!r}")


class contains_amount(Assertion):
    """Response contains the specific number `value`, in ANY digit-grouping
    convention (₹11,75,000 / ₹1,175,000 / 1175000 all match 1175000).

    Guards against a substring false-positive: 2500 must not be considered
    "found" merely because 25000 appears. The digit run is matched on its
    own boundaries after grouping commas are removed.
    """

    def __init__(self, value: int | float):
        self.value = int(value)
        self.description = f"contains_amount({self.value})"

    def check(self, ctx: ScenarioContext) -> Outcome:
        flat = normalise_digits(ctx.response)
        # (?<!\d) / (?!\d) so 2500 doesn't match inside 25000.
        if re.search(rf"(?<!\d){abs(self.value)}(?!\d)", flat):
            return Outcome(True, f"found amount {self.value}")
        return Outcome(
            False,
            f"expected amount {self.value} (any grouping); got: {_snippet(ctx.response)!r}",
        )


class contains_currency(Assertion):
    """Response renders money — ₹, Rs, or INR."""

    description = "contains_currency"

    def check(self, ctx: ScenarioContext) -> Outcome:
        if re.search(r"₹|\brs\.?\b|\binr\b", ctx.response, re.IGNORECASE):
            return Outcome(True, "currency marker present")
        return Outcome(False, f"expected a currency marker; got: {_snippet(ctx.response)!r}")


# The bot's own failure phrasings, lifted from services/intent_service.py.
# A scenario that trips any of these got an error, not an answer — even
# when the reply superficially looks like prose.
ERROR_PHRASES = (
    "couldn't format",
    "couldn't find",
    "couldn't parse",
    "couldn't save",
    "couldn't detect",
    "couldn't send",
    "couldn't quite work out",
    "something went wrong",
    "i hit an error",
    "please try again",
)


class no_error(Assertion):
    """Response is not one of the bot's known failure messages.

    Worth stating plainly: this is the assertion that catches the failure
    mode the whole harness exists for — a reply that reads fluent but is
    actually the bot giving up.
    """

    description = "no_error"

    def check(self, ctx: ScenarioContext) -> Outcome:
        flat = normalise_text(ctx.response)
        for phrase in ERROR_PHRASES:
            if normalise_text(phrase) in flat:
                return Outcome(False, f"error phrase {phrase!r} in: {_snippet(ctx.response)!r}")
        return Outcome(True, "no known error phrasing")


# ─────────────────────────────────────────────────────────────────────
# Operation assertions
# ─────────────────────────────────────────────────────────────────────

class operation_is(Assertion):
    """process_request's `operation` field equals `expected` — the 4.2
    spec's `operation == "query"`. Checks ROUTING, independent of phrasing:
    a right-sounding answer produced by the wrong flow is still a bug."""

    def __init__(self, expected: str):
        self.expected = expected
        self.description = f"operation_is({expected!r})"

    def check(self, ctx: ScenarioContext) -> Outcome:
        if ctx.operation == self.expected:
            return Outcome(True, f"operation == {self.expected!r}")
        return Outcome(False, f"expected operation {self.expected!r}, got {ctx.operation!r}")


class operation_in(Assertion):
    """`operation` is one of `expected` — for scenarios with more than one
    legitimate route (e.g. a query answerable by the router or the planner)."""

    def __init__(self, expected: Sequence[str]):
        self.expected = tuple(expected)
        self.description = f"operation_in({list(self.expected)!r})"

    def check(self, ctx: ScenarioContext) -> Outcome:
        if ctx.operation in self.expected:
            return Outcome(True, f"operation {ctx.operation!r} in {list(self.expected)!r}")
        return Outcome(False, f"expected operation in {list(self.expected)!r}, got {ctx.operation!r}")


# ─────────────────────────────────────────────────────────────────────
# DB-backed assertions
# ─────────────────────────────────────────────────────────────────────

class _DbAssertion(Assertion):
    """Shared guard: a DB assertion with no accessor wired must FAIL, never
    silently pass. A no-op assertion that reports green is worse than
    having none, because it manufactures false confidence."""

    def _require_db(self, ctx: ScenarioContext) -> Optional[Outcome]:
        if ctx.db is None:
            return Outcome(
                False,
                f"{self.description} needs a DB accessor but ScenarioContext.db is None",
            )
        return None


class row_created(_DbAssertion):
    """A job_entries row matching the given fields exists for this user.

    The 4.2 spec's `row_created(client="...")`. Always scoped to the
    scenario's synthetic user_id, so it can never read another account's
    data. `fees`/`paid` narrow further when the scenario cares.
    """

    def __init__(self, client: Optional[str] = None,
                 fees: Optional[int] = None,
                 paid: Optional[str] = None):
        self.client = client
        self.fees = fees
        self.paid = paid
        crit = ", ".join(
            f"{k}={v!r}" for k, v in
            (("client", client), ("fees", fees), ("paid", paid)) if v is not None
        )
        self.description = f"row_created({crit})"

    def check(self, ctx: ScenarioContext) -> Outcome:
        missing = self._require_db(ctx)
        if missing:
            return missing

        sql = [
            'SELECT client_name, fees, paid FROM public.job_entries '
            'WHERE user_id = %s AND ("isDeleted" IS NOT TRUE)'
        ]
        params: List[Any] = [ctx.user_id]
        if self.client is not None:
            # ILIKE: the bot may normalise or title-case what the user typed.
            sql.append("AND (client_name ILIKE %s OR brand_name ILIKE %s)")
            params += [f"%{self.client}%", f"%{self.client}%"]
        if self.fees is not None:
            sql.append("AND fees = %s")
            params.append(self.fees)
        if self.paid is not None:
            sql.append("AND LOWER(COALESCE(paid,'')) = LOWER(%s)")
            params.append(self.paid)

        rows = ctx.db(" ".join(sql), tuple(params))
        if rows:
            return Outcome(True, f"found {len(rows)} matching row(s)")
        return Outcome(False, f"no job_entries row matched {self.description}")


class row_count_is(_DbAssertion):
    """Exactly `expected` non-deleted rows exist for this user — for
    scenarios asserting a write did (or didn't) happen."""

    def __init__(self, expected: int):
        self.expected = expected
        self.description = f"row_count_is({expected})"

    def check(self, ctx: ScenarioContext) -> Outcome:
        missing = self._require_db(ctx)
        if missing:
            return missing
        rows = ctx.db(
            'SELECT COUNT(*) AS n FROM public.job_entries '
            'WHERE user_id = %s AND ("isDeleted" IS NOT TRUE)',
            (ctx.user_id,),
        )
        actual = int(rows[0]["n"]) if rows else 0
        if actual == self.expected:
            return Outcome(True, f"row count == {self.expected}")
        return Outcome(False, f"expected {self.expected} rows, found {actual}")


# ─────────────────────────────────────────────────────────────────────
# Combinators
# ─────────────────────────────────────────────────────────────────────

class all_of(Assertion):
    """Every sub-assertion passes. Reports the FIRST failure's detail so
    the report names a specific cause rather than a count."""

    def __init__(self, *subs: Assertion):
        self.subs = subs
        self.description = "all_of(" + ", ".join(s.description for s in subs) + ")"

    def check(self, ctx: ScenarioContext) -> Outcome:
        for sub in self.subs:
            out = sub.check(ctx)
            if not out.passed:
                return Outcome(False, f"{sub.description}: {out.detail}")
        return Outcome(True, f"all {len(self.subs)} sub-assertions passed")


class any_of(Assertion):
    """At least one sub-assertion passes — for genuinely multi-valid
    answers (e.g. a total shown as a figure OR as a clarifying question)."""

    def __init__(self, *subs: Assertion):
        self.subs = subs
        self.description = "any_of(" + ", ".join(s.description for s in subs) + ")"

    def check(self, ctx: ScenarioContext) -> Outcome:
        details = []
        for sub in self.subs:
            out = sub.check(ctx)
            if out.passed:
                return Outcome(True, f"{sub.description} passed")
            details.append(f"{sub.description}: {out.detail}")
        return Outcome(False, "none passed — " + " | ".join(details))


def run_assertions(ctx: ScenarioContext,
                   assertions: Sequence[Assertion]) -> List[Dict[str, Any]]:
    """Run every assertion, collecting results. Deliberately does NOT stop
    at the first failure: a scenario report is far more useful showing all
    four checks with two failing than showing one and hiding the rest.

    An assertion that RAISES is recorded as a failure rather than being
    allowed to abort the run — one malformed scenario shouldn't cost you
    the other 133 results.
    """
    out = []
    for a in assertions:
        try:
            res = a.check(ctx)
        except Exception as e:  # noqa: BLE001 — a bad assertion must not kill the run
            res = Outcome(False, f"assertion raised {type(e).__name__}: {e}")
        out.append({"assertion": a.description, "passed": res.passed, "detail": res.detail})
    return out
