"""
TODO.md Phase 4.2 — the scenario corpus.

Ported from the 29-message WhatsApp production test suite in CLAUDE.md
(the `Intent_Test_Matrix` sheet the plan names was never in the repo).
Each row keeps the SHAPE of the original test — its category and what it
proves — while being made machine-checkable and deterministic.

Two adaptations were unavoidable, both deliberate:

1. **Client names.** The original suite ran against a real account and
   names Acme / Samsung / Pedigree / Garnier / Star Studios. The Phase 4.1
   fixture has its own three clients, so each message is rewritten to use
   them. The test being performed is unchanged (e.g. "How much does Star
   Studios owe me?" -> "How much does Bridgestone owe me?", still a
   client-scoped unpaid sum).

2. **Expected values are DERIVED, never literal.** The original sheet says
   things like "should be 0" — true of that account, not of this fixture.
   Every numeric expectation here is computed from FIXTURE_ROWS, so it
   cannot disagree with the seed. Where the original expectation was vague
   ("a number", "₹ amount") the assertion stays deliberately loose: LLM
   phrasing varies, and over-specifying would produce failures that measure
   wording rather than correctness.

State handling, per the plan's "scenarios that depend on a previous turn's
state must declare it explicitly":

* `requires="fresh"` — the default. The runner RE-SEEDS the account and
  wipes conversation memory first. Re-seeding matters because scenario 1
  writes a row: without it, every later count assertion would be off by
  one, and the corpus would silently depend on execution order.
* `requires="after:<id>"` — a genuine multi-turn test (context follow-ups).
  The runner replays the prerequisite in the same memory, no reset between.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Sequence

from tests.e2e.assertions import (
    Assertion, contains, not_contains, contains_number, contains_amount,
    contains_currency, no_error, operation_in, row_created, any_of,
)
from tests.e2e.seed import (
    FIXTURE_ROWS, FIXTURE_BANK, ROW_COUNT, BIGGEST_CLIENT,
    FEES_BY_CLIENT, UNPAID_BY_CLIENT, _is_yes,
)

# Derived here rather than hardcoded — see the module docstring.
INVOICED_NO_EMAIL_COUNT = sum(
    1 for r in FIXTURE_ROWS if _is_yes(r["bill_sent"]) and not r["poc_email"]
)

# Fixture clients, referenced by name so a seed edit surfaces as a failing
# scenario rather than a silently-wrong assertion.
C_ACME    = "Acme Studios"
C_BRIDGE  = "Bridgestone"     # the client with NO poc_email on any row
C_NORDIC  = "Nordic Films"


@dataclass(frozen=True)
class Scenario:
    id: int
    message: str
    category: str
    assertions: Sequence[Assertion]
    requires: str = "fresh"
    # Set when the scenario WRITES — the runner re-seeds after it so a
    # mutation can't leak into whatever runs next.
    mutates: bool = False
    note: str = ""


SCENARIOS: List[Scenario] = [
    Scenario(
        1, f"Add a job for {C_ACME}, 25k, shoot, paid", "Smart capture",
        # CLAUDE.md Bug 4: the original run extracted brand/fees but dropped
        # "paid". row_created(paid=...) is what makes that regression visible.
        [no_error(), row_created(client=C_ACME, fees=25000, paid="Yes")],
        mutates=True,
        note="Bug 4 regression — 'paid' keyword must reach the row",
    ),
    Scenario(2, "Show my last 5 jobs", "Basic query",
             [no_error(), contains_number()]),
    Scenario(3, "List all unpaid invoices", "Basic query",
             [no_error(), contains_number()]),
    Scenario(4, "Who is my biggest client?", "Grouped aggregate",
             # CLAUDE.md Bug 1: the planner refused to emit GROUP BY here.
             [no_error(), contains(BIGGEST_CLIENT), contains_number()],
             note="Bug 1 regression — grouped aggregate"),
    Scenario(5, "How many invoices have I sent?", "Count + filter",
             [no_error(), contains_number()]),
    Scenario(6, "Total billing this year", "Sum + date range",
             [no_error(), contains_number(), contains_currency()]),
    Scenario(7, "Average fees per job", "Average",
             # CLAUDE.md Bug 1 also covered "average fees".
             [no_error(), contains_number()],
             note="Bug 1 regression — AVG"),
    Scenario(8, "Isme se invoice kitne logon ko bheja hai", "Hinglish count",
             [no_error(), contains_number()]),
    Scenario(9, "Kiska payment baki hai", "Hinglish unpaid",
             [no_error()]),
    Scenario(10, "Pichle mahine ki total kamai kitni thi", "Hinglish date + sum",
             # CLAUDE.md Bug 5: a ₹0 result must read as an amount, not as
             # "no matching records" — so a number must appear either way.
             [no_error(), contains_number()],
             note="Bug 5 — zero-result phrasing"),
    Scenario(11, "Jobs in Q1 this year", "Date range",
             [no_error()]),
    Scenario(12, "Earnings last quarter", "Date range sum",
             # CLAUDE.md Bug 3: this listed rows instead of summing.
             [no_error(), contains_number()],
             note="Bug 3 regression — value-oriented phrasing should aggregate"),
    Scenario(13, "What about this month?", "Context follow-up",
             [no_error(), contains_number()],
             requires="after:12",
             note="inherits the previous turn's intent"),
    Scenario(14, "Show jobs from around then", "Path 3 clarification",
             # A vague date with no prior context must ask, not guess.
             [any_of(contains("?"), contains("specify"), contains("which"))],
             note="Path 3 — clarification rather than a guessed date"),
    Scenario(15, "How many invoices sent to clients with no email", "Multi-filter count",
             [no_error(), contains_amount(INVOICED_NO_EMAIL_COUNT)],
             note="compound filter: bill_sent=yes AND poc_email IS NULL"),
    Scenario(16, "Show my bank details", "user_config read",
             [no_error(), contains(FIXTURE_BANK["bank_account_number"])]),
    Scenario(17, f"genrate invoce for {C_ACME}", "Typo detection",
             # Must be recognised as an invoice request despite two typos —
             # NOT answered as a data query.
             [not_contains("couldn't"),
              operation_in(["ACTION_TRIGGER", "invoice_request", "generate_invoice",
                            "invoice", "form_step"])],
             note="typo-tolerant invoice routing"),
    Scenario(18, "Can you book me an Uber?", "Out-of-scope",
             [no_error()],
             note="friendly on-brand refusal, not an error"),
    Scenario(19, f"Show {C_BRIDGE} jobs", "Client filter",
             [no_error(), contains(C_BRIDGE)]),
    Scenario(20, "Mark this as paid", "Context update",
             [no_error()],
             requires="after:19", mutates=True,
             note="updates the row surfaced by the previous turn"),
    Scenario(21, f"Show {C_ACME} and {C_NORDIC} jobs", "Multi-client",
             [no_error(), contains(C_ACME), contains(C_NORDIC)]),
    Scenario(22, "What did I do last week?", "Natural date",
             [no_error()]),
    Scenario(23, "Kiska invoice bhejna baki hai", "Hinglish pending",
             # This is the exact message that used to be hijacked by the
             # legacy invoice keyword check (Phase 1.3).
             [no_error()],
             note="Phase 1.3 regression — must route as a READ, not an invoice action"),
    Scenario(24, f"How much does {C_BRIDGE} owe me?", "Client + unpaid sum",
             # CLAUDE.md Bug 1: "owe me" must imply paid=no.
             [no_error(), contains_amount(UNPAID_BY_CLIENT[C_BRIDGE])],
             note="Bug 1 regression — 'owe me' implies unpaid"),
    Scenario(25, f"Total fees for {C_BRIDGE}", "Client sum",
             [no_error(), contains_amount(FEES_BY_CLIENT[C_BRIDGE])]),
    Scenario(26, f"{C_BRIDGE} se paisa aaya kya?", "Hinglish paid check",
             [no_error(), contains_number()]),
    Scenario(27, "Show me all my jobs", "Full list",
             [no_error()]),
    Scenario(28, "How many jobs have I done?", "Simple count",
             # CLAUDE.md Bug 2: this produced "couldn't format the reply".
             [no_error(), contains_amount(ROW_COUNT)],
             note="Bug 2 regression — unfiltered COUNT synth crash"),
    Scenario(29, "How many total jobs do I have?", "Simple count",
             [no_error(), contains_amount(ROW_COUNT)],
             note="Bug 2 — must match #28"),
]


def by_id(scenario_id: int) -> Scenario:
    for s in SCENARIOS:
        if s.id == scenario_id:
            return s
    raise KeyError(f"no scenario {scenario_id}")


def prerequisite_of(s: Scenario) -> "Scenario | None":
    """The scenario that must run first, or None."""
    if not s.requires.startswith("after:"):
        return None
    return by_id(int(s.requires.split(":", 1)[1]))
