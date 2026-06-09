"""
Path 3 contract — Plan + canonical filter normalisation.

This test file is the architectural guarantee that we will NEVER again
ship "the AI emitted a shape we didn't anticipate, the SQL builder ILIKE'd
it, the user got a wrong answer."

The strategy: for every semantic concept (NULL, NOT NULL, "sent", "not
sent", "paid", "unpaid") we ENUMERATE every plausible variant — case,
whitespace, underscores, hyphens, Hinglish — and assert all of them
normalise to the same CanonicalFilter. New variants the AI invents are
covered as long as they fall within the same lexical class.

If a future planner output produces a variant outside this class, we
get a NormalisationError loud and clear, which (in strict mode) triggers
a retry with feedback. The bug class becomes "couldn't normalise" — a
typed error — instead of "wrong SQL silently shipped."
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Ensure the column registry is imported so per-column normalisers register.
import services.columns  # noqa: F401
from services.plan import (
    BoolCheck,
    Comparison,
    Equality,
    InList,
    NullCheck,
    Plan,
    TextMatch,
    generic_normalize_filter,
    normalize_filter,
)


# ─────────────────────────────────────────────────────────────────────
# NULL / NOT NULL — every column should map every variant to NullCheck
# ─────────────────────────────────────────────────────────────────────

_NULL_VARIANTS = [
    None, "", "null", "NULL", "Null",
    "is null", "IS NULL", "Is Null", "is  null", "is\tnull",
    "is_null", "IS_NULL", "is__null",
    "isnull", "ISNULL",
]

_NOT_NULL_VARIANTS = [
    "is not null", "IS NOT NULL", "Is Not Null",
    "is  not  null", "is\tnot\tnull",
    "is_not_null", "IS_NOT_NULL", "is__not__null",
    "not null", "NOT NULL", "Not Null",
    "not_null", "NOT_NULL",
    "isnotnull", "ISNOTNULL",
    "any", "*",
]


class TestGenericNullNormalisation:
    """The centralised normaliser must collapse every variant of
    'no value' into NullCheck(True), and every variant of 'any value'
    into NullCheck(False). This is the property-based contract."""

    @pytest.mark.parametrize("variant", _NULL_VARIANTS)
    def test_null_variants_collapse(self, variant):
        result = generic_normalize_filter("some_text_col", variant)
        assert result == NullCheck(is_null=True), (
            f"NULL variant {variant!r} did not collapse"
        )

    @pytest.mark.parametrize("variant", _NOT_NULL_VARIANTS)
    def test_not_null_variants_collapse(self, variant):
        result = generic_normalize_filter("some_text_col", variant)
        assert result == NullCheck(is_null=False), (
            f"NOT NULL variant {variant!r} did not collapse"
        )


# ─────────────────────────────────────────────────────────────────────
# bill_sent — every truthy/falsy variant → BoolCheck
# ─────────────────────────────────────────────────────────────────────

_BILL_SENT_TRUTHY = [
    "yes", "Yes", "YES", "true", "True", "TRUE",
    "t", "T", "1", "sent", "Sent", "SENT", "y", "Y",
]

_BILL_SENT_FALSY = [
    None, "no", "No", "NO", "false", "False", "FALSE",
    "0", "n", "N", "not sent", "Not Sent",
    "pending", "Pending", "unpaid",
]


class TestBillSentNormalisation:
    @pytest.mark.parametrize("variant", _BILL_SENT_TRUTHY)
    def test_truthy_variants(self, variant):
        result = normalize_filter("bill_sent", variant)
        assert result == BoolCheck(truthy=True), (
            f"bill_sent={variant!r} should be BoolCheck(truthy=True), "
            f"got {result!r}"
        )

    @pytest.mark.parametrize("variant", _BILL_SENT_FALSY)
    def test_falsy_variants(self, variant):
        result = normalize_filter("bill_sent", variant)
        assert result == BoolCheck(truthy=False), (
            f"bill_sent={variant!r} should be BoolCheck(truthy=False), "
            f"got {result!r}"
        )

    def test_list_with_falsy_marker(self):
        # The "Isme se invoice kitne logon ko bheja hai" bug — list shape.
        result = normalize_filter("bill_sent", ["no", "false", "0", ""])
        assert result == BoolCheck(truthy=False)

    def test_underscore_not_null(self):
        # The bug that triggered this whole refactor:
        # planner emitted {"bill_sent": "not_null"} → silent COUNT=0.
        # Now it canonicalises to BoolCheck(truthy=True) via the
        # column normaliser (bill_sent's _classify treats "not_null"
        # as truthy intent).
        result = normalize_filter("bill_sent", "not_null")
        assert result == BoolCheck(truthy=True)


# ─────────────────────────────────────────────────────────────────────
# paid — mirror of bill_sent
# ─────────────────────────────────────────────────────────────────────

class TestPaidNormalisation:
    @pytest.mark.parametrize("variant",
        ["yes", "true", "1", "paid", "Y", "PAID"])
    def test_truthy(self, variant):
        assert normalize_filter("paid", variant) == BoolCheck(truthy=True)

    @pytest.mark.parametrize("variant",
        [None, "no", "false", "0", "unpaid", "pending", "outstanding"])
    def test_falsy(self, variant):
        assert normalize_filter("paid", variant) == BoolCheck(truthy=False)

    def test_falsy_list(self):
        result = normalize_filter("paid", ["no", "false", "0", ""])
        assert result == BoolCheck(truthy=False)


# ─────────────────────────────────────────────────────────────────────
# poc_email — three legitimate canonical forms
# ─────────────────────────────────────────────────────────────────────

class TestPocEmailNormalisation:
    @pytest.mark.parametrize("variant",
        [None, "null", "is null", "is_null", ""])
    def test_null(self, variant):
        assert normalize_filter("poc_email", variant) == NullCheck(is_null=True)

    @pytest.mark.parametrize("variant",
        ["is not null", "IS NOT NULL", "not_null", "isnotnull", "any", "*"])
    def test_not_null(self, variant):
        assert normalize_filter("poc_email", variant) == NullCheck(is_null=False)

    def test_real_email_is_text_match(self):
        result = normalize_filter("poc_email", "john@example.com")
        assert result == TextMatch(value="john@example.com")


# ─────────────────────────────────────────────────────────────────────
# Date columns — Equality / Comparison / NullCheck only. Never TextMatch.
# ─────────────────────────────────────────────────────────────────────

class TestDateColumnNormalisation:
    @pytest.mark.parametrize("col",
        ["job_date", "invoice_date", "payment_date", "bill_sent_at"])
    def test_null(self, col):
        assert normalize_filter(col, None) == NullCheck(is_null=True)
        assert normalize_filter(col, "IS NULL") == NullCheck(is_null=True)

    @pytest.mark.parametrize("col",
        ["job_date", "invoice_date", "payment_date"])
    def test_iso_date(self, col):
        assert normalize_filter(col, "2026-03-14") == Equality(value="2026-03-14")

    def test_operator_prefix(self):
        result = normalize_filter("invoice_date", "< 2026-03-14")
        assert result == Comparison(op="<", value="2026-03-14")

    def test_operator_dict(self):
        result = normalize_filter("invoice_date",
                                  {"operator": ">=", "value": "2026-01-01"})
        assert result == Comparison(op=">=", value="2026-01-01")

    def test_junk_returns_none(self):
        # The whole point: a junk value on a date column does NOT silently
        # become TextMatch (which would ILIKE-crash Postgres). It returns
        # None so the validator raises a NormalisationError.
        assert normalize_filter("invoice_date", "sometime last week") is None
        assert normalize_filter("job_date", "yesterday") is None


# ─────────────────────────────────────────────────────────────────────
# Generic fallback for unregistered columns
# ─────────────────────────────────────────────────────────────────────

class TestGenericFallback:
    def test_numeric_string(self):
        result = generic_normalize_filter("fees", "25000")
        assert result == Equality(value=25000.0)

    def test_list_becomes_in_clause(self):
        result = generic_normalize_filter("client_name", ["Acme", "Globex"])
        assert result == InList(values=("Acme", "Globex"))

    def test_text_becomes_ilike(self):
        result = generic_normalize_filter("client_name", "Acme")
        assert result == TextMatch(value="Acme")

    def test_operator_dict(self):
        result = generic_normalize_filter(
            "fees", {"operator": ">", "value": 50000})
        assert result == Comparison(op=">", value=50000)


# ─────────────────────────────────────────────────────────────────────
# Plan.from_raw — end-to-end validation contract
# ─────────────────────────────────────────────────────────────────────

class TestPlanFromRaw:
    def test_valid_plan(self):
        raw = {
            "operation": "query",
            "metric": "count",
            "filters": {"bill_sent": "no", "paid": "no"},
        }
        result = Plan.from_raw(raw)
        assert result.valid
        assert result.plan.filters["bill_sent"] == BoolCheck(truthy=False)
        assert result.plan.filters["paid"] == BoolCheck(truthy=False)

    def test_unknown_column_when_allowed_provided(self):
        raw = {
            "operation": "query",
            "filters": {"nonexistent_col": "value"},
        }
        result = Plan.from_raw(raw, allowed_columns=["client_name", "fees"])
        assert not result.valid
        assert any("not in schema" in e.reason for e in result.errors)

    def test_unknown_column_passes_when_no_schema_provided(self):
        """from_raw without allowed_columns is permissive (used in
        shadow mode where we trust the column-validation step)."""
        raw = {
            "operation": "query",
            "filters": {"some_unregistered_col": "value"},
        }
        result = Plan.from_raw(raw)
        # Falls back to generic normaliser → TextMatch — valid
        assert result.valid

    def test_internal_marker_columns_pass(self):
        """_resolve_latest etc. are internal markers — not real columns."""
        raw = {
            "operation": "update",
            "filters": {"_resolve_latest": True, "client_name": "Acme"},
        }
        result = Plan.from_raw(raw)
        assert result.valid
        # Internal markers are not added to filters dict
        assert "_resolve_latest" not in result.plan.filters

    def test_date_column_junk_value_surfaces_error(self):
        """The exact bug class Path 3 is designed to eliminate."""
        raw = {
            "operation": "query",
            "filters": {"invoice_date": "sometime last week"},
        }
        result = Plan.from_raw(raw)
        assert not result.valid
        assert any(e.column == "invoice_date" for e in result.errors)

    def test_feedback_is_human_readable(self):
        raw = {
            "operation": "query",
            "filters": {"invoice_date": "yesterday"},
        }
        result = Plan.from_raw(raw)
        feedback = result.feedback_for_retry()
        assert "invoice_date" in feedback
        assert "yesterday" in feedback
        assert "could not understand" in feedback


# ─────────────────────────────────────────────────────────────────────
# Cross-variant consistency — the most important property test
# ─────────────────────────────────────────────────────────────────────

class TestCrossVariantConsistency:
    """For each semantic concept, EVERY variant must produce the IDENTICAL
    CanonicalFilter. This is the lemma that makes the architecture work:
    once a column normalises, the SQL produced downstream is unique."""

    def test_all_null_variants_identical(self):
        results = {
            generic_normalize_filter("some_col", v) for v in _NULL_VARIANTS
        }
        assert results == {NullCheck(is_null=True)}, (
            f"NULL variants did not all collapse: {results}"
        )

    def test_all_not_null_variants_identical(self):
        results = {
            generic_normalize_filter("some_col", v) for v in _NOT_NULL_VARIANTS
        }
        assert results == {NullCheck(is_null=False)}, (
            f"NOT NULL variants did not all collapse: {results}"
        )

    def test_all_bill_sent_truthy_identical(self):
        results = {
            normalize_filter("bill_sent", v) for v in _BILL_SENT_TRUTHY
        }
        assert results == {BoolCheck(truthy=True)}

    def test_all_bill_sent_falsy_identical(self):
        results = {
            normalize_filter("bill_sent", v) for v in _BILL_SENT_FALSY
        }
        assert results == {BoolCheck(truthy=False)}
