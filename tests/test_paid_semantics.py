"""
Cross-layer consistency of the "is this row paid?" predicate.

The bug this locks down: `paid` is a free-text column and each layer used to
re-implement the semantics. supabase_service's reminder/overdue queries used
    paid IS NULL OR paid::text NOT IN ('true','t','yes','1')
which is wrong twice over — no LOWER(), and 'paid' missing from the truthy
list. Postgres string comparison is case-sensitive, so 'Yes' (the exact
literal EVERY mark-paid path in intent_service writes) failed the NOT IN and
came back as UNPAID. A job the user had just marked paid still surfaced as
"past due" and was queued for a client-facing reminder.

These tests assert the predicates AGREE across layers rather than testing one
in isolation — the divergence is the bug, so the agreement is the contract.
"""
import os
import re
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from services.query_router import PAID_TRUE, PAID_FALSE
from services.supabase_service import SupabaseService
from services.columns.paid import _TRUTHY as PAID_COLUMN_TRUTHY


def _tokens(sql_fragment: str):
    """Pull the quoted truthy tokens out of an IN (...) list."""
    m = re.search(r"IN\s*\(([^)]*)\)", sql_fragment, re.IGNORECASE)
    assert m, f"no IN(...) list found in {sql_fragment!r}"
    return {t.strip().strip("'").lower() for t in m.group(1).split(",")}


class TestTruthyTokenSetsAgree:
    def test_reminder_predicate_matches_query_pipeline(self):
        """supabase_service's unpaid predicate must recognise exactly the same
        truthy tokens as the query pipeline's PAID_FALSE."""
        assert _tokens(SupabaseService._PAID_FALSE_SQL) == _tokens(PAID_FALSE)

    def test_paid_literal_is_in_the_truthy_set(self):
        """'paid' was missing from the reminder predicate's list."""
        assert "paid" in _tokens(SupabaseService._PAID_FALSE_SQL)

    def test_reminder_predicate_is_case_insensitive(self):
        """The original had no LOWER(), so 'Yes' != 'yes' and every bot-written
        paid row read as unpaid."""
        assert "lower(" in SupabaseService._PAID_FALSE_SQL.lower()

    def test_reminder_predicate_handles_blank_string(self):
        assert "trim(" in SupabaseService._PAID_FALSE_SQL.lower()


def _unpaid_by(sql_predicate: str, value):
    """Evaluate an unpaid predicate in Python with Postgres semantics."""
    truthy = _tokens(sql_predicate)
    if value is None:
        return True
    if value.strip() == "":
        return True
    return value.lower() not in truthy


# The literals the bot itself writes, plus realistic hand-entered values.
_VALUES = ["Yes", "yes", "YES", "paid", "Paid", "true", "True", "No", "no", "", None]


class TestCrossLayerAgreement:
    @pytest.mark.parametrize("value", _VALUES)
    def test_reminder_and_query_layers_agree(self, value):
        a = _unpaid_by(PAID_FALSE, value)
        b = _unpaid_by(SupabaseService._PAID_FALSE_SQL, value)
        assert a == b, f"paid={value!r}: query says unpaid={a}, reminder says unpaid={b}"

    @pytest.mark.parametrize("value", ["Yes", "yes", "paid", "Paid", "true", "True"])
    def test_bot_written_paid_values_are_never_chased(self, value):
        """The regression that mattered: anything the bot writes on mark-paid
        must NOT come back from the reminder/overdue queries."""
        assert not _unpaid_by(SupabaseService._PAID_FALSE_SQL, value), (
            f"paid={value!r} would still be chased for payment"
        )

    @pytest.mark.parametrize("value", ["No", "no", "unpaid", "", None])
    def test_genuinely_unpaid_values_still_chased(self, value):
        assert _unpaid_by(SupabaseService._PAID_FALSE_SQL, value)


class TestMarkPaidWritesARecognisedLiteral:
    """Close the loop: the value the mark-paid paths write must be recognised as
    truthy by the predicate above. If someone changes one side, this fails."""

    def test_yes_literal_recognised_by_all_layers(self):
        # intent_service writes exactly 'Yes' (audit reply + _apply_modify_update).
        assert not _unpaid_by(PAID_FALSE, "Yes")
        assert not _unpaid_by(SupabaseService._PAID_FALSE_SQL, "Yes")
        assert "yes" in _tokens(PAID_TRUE)
        assert "yes" in PAID_COLUMN_TRUTHY
