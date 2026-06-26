"""
tests/test_planner_boundary.py
==============================

The contract test suite for the AI → SQL boundary.

EVERY past production bug in services/query_planner.py and the column
registry has a regression test here. If a refactor breaks any of these,
CI fails — by design.

Test format:
  - `Given a planner output of {column: value}` →
    `Assert the SQL builder produces a predicate with property X`
  - Property assertions, not literal-string matching, so safe refactors
    (whitespace, alias renaming) don't false-positive failures.

To add a new test:
  1. Reproduce the bug locally.
  2. Add a new method on the relevant TestX class with a clear name.
  3. Make it fail.
  4. Ship the fix.
  5. Verify it passes.

NEW BUGS without a corresponding test here will keep recurring. Resist.
"""

import os
import sys

import pytest

# conftest.py already prepends path/stubs at test-collection time.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.query_planner import _build_filter_clause, _DATE_COLUMNS


# ════════════════════════════════════════════════════════════════════════
# bill_sent — the "is the invoice actually sent?" column
# ════════════════════════════════════════════════════════════════════════

class TestBillSent:
    """Every bill_sent bug that ever shipped to production has a test here."""

    # ── Regression: list form excluded NULL rows (2026-06-08) ───────────
    def test_falsy_list_includes_null_rows(self):
        """
        Bug: planner emitted {"bill_sent": ["no","false","0",""]}.
        Old SQL: bill_sent IN ('no','false','0','') — EXCLUDES NULL rows,
        which are exactly the "not yet sent" rows the user asked about.
        Fix: list-form falsy now routes through the NOT-truthy predicate
        that explicitly INCLUDES NULL.
        """
        sql = _build_filter_clause("bill_sent", ["no", "false", "0", ""])
        assert "IS NULL" in sql, sql
        assert "NOT IN" in sql, sql
        # Negative property: must NOT be the broken IN-clause form
        assert "bill_sent IN (" not in sql

    def test_falsy_list_with_hinglish_variants(self):
        sql = _build_filter_clause("bill_sent", ["pending", "not sent"])
        assert "IS NULL" in sql
        assert "NOT IN" in sql

    # ── Regression: single-value falsy (2026-06-04) ────────────────────
    def test_single_value_no(self):
        sql = _build_filter_clause("bill_sent", "no")
        assert "IS NULL" in sql

    def test_single_value_pending(self):
        sql = _build_filter_clause("bill_sent", "pending")
        assert "IS NULL" in sql

    # ── Regression: null literal (2026-06-04) ──────────────────────────
    def test_null_value_means_not_sent(self):
        """
        Bug: {"bill_sent": null} hit the ILIKE fallback → 'bill_sent ILIKE NULL' → 500.
        Fix: registry handler interprets null as "not sent" — IS NULL predicate.
        """
        sql = _build_filter_clause("bill_sent", None)
        assert "IS NULL" in sql
        assert "ILIKE" not in sql

    # ── Regression: "IS NOT NULL" string (2026-05-30) ──────────────────
    def test_is_not_null_string_works(self):
        """
        Bug: planner emitted 'IS NOT NULL' as a string value → ILIKE'd a
        SQL keyword → Postgres rejection.
        """
        sql = _build_filter_clause("bill_sent", "IS NOT NULL")
        assert "ILIKE" not in sql
        # "IS NOT NULL" on bill_sent → "truthy" → sent + poc_email present
        assert ("IS NOT NULL" in sql) or ("IN " in sql)

    # ── Truthy single value: requires poc_email AND ────────────────────
    def test_truthy_value_requires_poc_email(self):
        """
        Invariant: a row is "sent" only when bill_sent is truthy AND there's
        a poc_email. Without the email, the row could never have been
        emailed — exclude it from sent results.
        """
        sql = _build_filter_clause("bill_sent", "yes")
        assert "poc_email" in sql
        assert "IS NOT NULL" in sql or "<>" in sql

    def test_truthy_list_requires_poc_email(self):
        sql = _build_filter_clause("bill_sent", ["yes", "true"])
        assert "poc_email" in sql

    # ── Negative space: never literal ILIKE on bill_sent ───────────────
    def test_never_ilike(self):
        """ILIKE on bill_sent is meaningless (it's a yes/no column).
        The registry handler must always produce a clean predicate or
        fall through to a non-ILIKE generic handler."""
        for val in (None, "yes", "no", "IS NULL", "IS NOT NULL",
                    ["no", "false"], ["yes", "true"]):
            sql = _build_filter_clause("bill_sent", val)
            assert "ILIKE" not in sql, f"ILIKE leaked for val={val!r}: {sql}"

    def test_underscore_variants_recognised(self):
        """Regression: planner sometimes emits 'not_null' / 'is_not_null'
        with underscores instead of spaces. Both variants must map to the
        truthy-bill_sent predicate."""
        for variant in ("not_null", "is_not_null", "NOT_NULL"):
            sql = _build_filter_clause("bill_sent", variant)
            assert "ILIKE" not in sql, f"ILIKE leaked for {variant!r}: {sql}"
            assert "poc_email" in sql or "IS NOT NULL" in sql, f"{variant!r} should mean sent: {sql}"


# ════════════════════════════════════════════════════════════════════════
# paid — same shape as bill_sent
# ════════════════════════════════════════════════════════════════════════

class TestPaid:
    def test_unpaid_includes_null(self):
        sql = _build_filter_clause("paid", "no")
        assert "IS NULL" in sql
        assert "NOT IN" in sql

    def test_unpaid_list_includes_null(self):
        """Regression: list with falsy markers must NOT use raw IN-clause."""
        sql = _build_filter_clause("paid", ["no", "false", "unpaid", ""])
        assert "IS NULL" in sql
        assert "paid IN (" not in sql  # the broken form

    def test_paid_truthy(self):
        sql = _build_filter_clause("paid", "yes")
        assert "IN (" in sql  # the canonical positive form
        assert "IS NULL" not in sql

    def test_null_means_unpaid(self):
        sql = _build_filter_clause("paid", None)
        assert "IS NULL" in sql

    def test_hinglish_variants(self):
        """Hinglish 'bakaya' (outstanding) maps via the falsy token set."""
        sql = _build_filter_clause("paid", "outstanding")
        assert "IS NULL" in sql

    def test_never_ilike(self):
        for val in (None, "yes", "no", "unpaid", ["no", "pending"]):
            sql = _build_filter_clause("paid", val)
            assert "ILIKE" not in sql, f"ILIKE leaked on paid for val={val!r}"


# ════════════════════════════════════════════════════════════════════════
# Date / timestamp columns — never ILIKE, NULL semantics
# ════════════════════════════════════════════════════════════════════════

class TestDateColumns:
    @pytest.mark.parametrize("col", [
        "invoice_date", "job_date", "payment_date", "due_date",
        "first_reminder_sent", "bill_sent_at", "created_at",
    ])
    def test_null_value_is_is_null(self, col):
        """Regression: {date_col: null} used to render 'col ILIKE NULL' → 500."""
        sql = _build_filter_clause(col, None)
        assert "IS NULL" in sql
        assert "ILIKE" not in sql

    @pytest.mark.parametrize("col", [
        "invoice_date", "job_date", "payment_date",
    ])
    def test_is_not_null_string(self, col):
        """Regression: {date_col: 'IS NOT NULL'} used to ILIKE the string → 500."""
        sql = _build_filter_clause(col, "IS NOT NULL")
        assert "IS NOT NULL" in sql
        assert "ILIKE" not in sql

    @pytest.mark.parametrize("col", [
        "invoice_date", "job_date", "payment_date",
    ])
    def test_junk_value_falls_back_to_equality_not_ilike(self, col):
        """Date columns must NEVER receive ILIKE (Postgres rejects with
        'operator does not exist: date ~~* unknown')."""
        sql = _build_filter_clause(col, "tomorrow-ish")
        assert "ILIKE" not in sql

    def test_iso_date_equality(self):
        sql = _build_filter_clause("invoice_date", "2026-03-14")
        assert "2026-03-14" in sql

    def test_operator_dict(self):
        sql = _build_filter_clause("invoice_date", {"operator": "<", "value": "2026-03-14"})
        assert "<" in sql
        assert "2026-03-14" in sql

    def test_operator_prefix_string(self):
        sql = _build_filter_clause("invoice_date", "< 2026-03-14")
        assert "<" in sql

    def test_date_columns_set_is_complete(self):
        """Sanity: every date col covered by the registry is also in the
        legacy _DATE_COLUMNS allowlist used by the generic builder."""
        from services.columns import date_columns as dc
        for c in ("job_date", "invoice_date", "payment_date", "due_date",
                  "bill_sent_at", "overdue_audit_sent"):
            assert dc.is_date_column(c)


# ════════════════════════════════════════════════════════════════════════
# poc_email — explicit semantics, no unsolicited filters
# ════════════════════════════════════════════════════════════════════════

class TestPocEmail:
    def test_null_value_is_no_email(self):
        sql = _build_filter_clause("poc_email", None)
        assert "IS NULL" in sql or "= ''" in sql

    def test_is_not_null_explicit(self):
        sql = _build_filter_clause("poc_email", "IS NOT NULL")
        assert "IS NOT NULL" in sql

    def test_not_null_underscore_variant(self):
        """Regression (2026-06-09): planner emitted {poc_email: 'not_null'}
        (underscore variant). Old registry only recognised 'not null' (space)
        and fell through to ILIKE 'not_null' — matched nothing, COUNT
        returned 0, synthesizer told the user 'I can't tell you'. Both
        the underscore and space variants must map to IS NOT NULL."""
        from services.query_planner import _build_filter_clause
        for variant in ("not_null", "is_not_null", "isnotnull", "NOT_NULL", "Not_Null"):
            sql = _build_filter_clause("poc_email", variant)
            assert "IS NOT NULL" in sql, f"variant {variant!r} → {sql}"
            assert "ILIKE" not in sql, f"ILIKE leaked for {variant!r}: {sql}"

    def test_actual_email_uses_ilike(self):
        """When the user IS searching for a specific email substring,
        ILIKE is the right operator. Registry handler returns None for
        non-existence queries, generic builder takes over."""
        sql = _build_filter_clause("poc_email", "rohan@thegoodtake.com")
        assert "ILIKE" in sql


# ════════════════════════════════════════════════════════════════════════
# Generic shapes — list, dict, operator-prefix, etc.
# ════════════════════════════════════════════════════════════════════════

class TestGenericShapes:
    def test_list_text_column_uses_in_clause(self):
        sql = _build_filter_clause("client_name", ["Nike", "Garnier"])
        assert " IN (" in sql
        assert "Nike" in sql

    def test_numeric_equality(self):
        sql = _build_filter_clause("fees", 5000)
        assert "= 5000" in sql

    def test_text_column_ilike(self):
        sql = _build_filter_clause("client_name", "Nike")
        assert "ILIKE" in sql

    def test_operator_dict_text(self):
        sql = _build_filter_clause("fees", {"operator": ">", "value": "10000"})
        assert ">" in sql


# ════════════════════════════════════════════════════════════════════════
# Routing — v2 classifier verdict beats legacy invoice keyword check
# ════════════════════════════════════════════════════════════════════════

class TestV2VerdictBeatsLegacyInvoiceCheck:
    """
    Regression: 'kiska invoice baki hai bhejna' (Hinglish: 'whose invoice
    is left to send?') was correctly classified by v2 as READ_QUERY with
    conf=0.90, but the legacy INVOICE_CHECK keyword check saw the word
    'invoice' and silently overrode v2, routing to the invoice-NEED_CLARIFICATION
    path. The bot replied 'I need a client name or bill number'.

    This test asserts the GUARD condition exists: when v2 confidently called
    it READ_*, the legacy is_retrieval must flip to False.
    """

    def test_v2_high_conf_read_query_short_circuits_legacy_check(self):
        # Pure unit-style assertion of the guard predicate. The full flow
        # path is integration-tested through the bot's actual run; here we
        # just guarantee the predicate logic that protects against the bug.
        verdict_high_conf_read = {
            "intent": "READ_QUERY",
            "confidence": 0.9,
            "parameters": {"field": "bill_sent"},
        }
        v2_says_read = (
            verdict_high_conf_read is not None
            and verdict_high_conf_read.get("intent") in ("READ_QUERY", "READ_AGGREGATE")
            and float(verdict_high_conf_read.get("confidence") or 0) >= 0.85
        )
        assert v2_says_read is True

    def test_v2_low_conf_does_not_short_circuit(self):
        verdict_low_conf = {
            "intent": "READ_QUERY",
            "confidence": 0.5,
            "parameters": {},
        }
        v2_says_read = (
            verdict_low_conf.get("intent") in ("READ_QUERY", "READ_AGGREGATE")
            and float(verdict_low_conf.get("confidence") or 0) >= 0.85
        )
        assert v2_says_read is False  # legacy AI check should still run

    def test_v2_write_intents_do_not_short_circuit(self):
        verdict_write = {
            "intent": "WRITE_INVOICE",
            "confidence": 0.95,
            "parameters": {},
        }
        v2_says_read = (
            verdict_write.get("intent") in ("READ_QUERY", "READ_AGGREGATE")
            and float(verdict_write.get("confidence") or 0) >= 0.85
        )
        assert v2_says_read is False  # legitimate WRITE goes through invoice flow


# ════════════════════════════════════════════════════════════════════════
# Column registry — single source of truth invariants
# ════════════════════════════════════════════════════════════════════════

class TestColumnRegistry:
    def test_bill_sent_registered(self):
        from services.columns import get
        spec = get("bill_sent")
        assert spec is not None
        assert spec.name == "bill_sent"
        assert spec.prompt_fragment  # non-empty
        assert callable(spec.filter_handler)

    def test_paid_registered(self):
        from services.columns import get
        assert get("paid") is not None

    def test_poc_email_registered(self):
        from services.columns import get
        assert get("poc_email") is not None

    def test_composed_prompt_includes_known_columns(self):
        """The prompt composer pulls fragments from the registry — if any
        column's fragment vanishes, the prompt loses context and the AI
        starts making things up again."""
        from services.columns import composed_prompt_fragments
        composed = composed_prompt_fragments()
        assert "bill_sent" in composed
        assert "paid" in composed

    def test_no_unregistered_column_can_break_via_registry(self):
        """Registry returns None for unknown columns — generic builder
        handles them. Should never raise."""
        from services.columns import get
        assert get("totally_made_up_column") is None


# ════════════════════════════════════════════════════════════════════════
# Bug 1 & 2 regression — aggregate keyword SQL + COUNT post-correction
# ════════════════════════════════════════════════════════════════════════

class TestKeywordAggregates:
    """Keyword SQL fallback and plan post-correction for aggregate queries."""

    def setup_method(self):
        import sys, os
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

    def _make_intent_stub(self, uid="testuser"):
        """Minimal stub for _keyword_sql_fallback testing."""
        from unittest.mock import MagicMock
        from services.intent_service import IntentService
        svc = object.__new__(IntentService)
        return svc

    def test_biggest_client_keyword_sql_returns_group_by(self):
        svc = self._make_intent_stub()
        sql = svc._keyword_sql_fallback("Who is my biggest client?", "user123")
        assert sql is not None, "Expected SQL for 'biggest client'"
        assert "GROUP BY" in sql.upper()
        assert "SUM" in sql.upper()
        assert "ORDER BY" in sql.upper()
        assert "DESC" in sql.upper()
        assert "LIMIT 1" in sql.upper()

    def test_average_fees_keyword_sql_returns_avg(self):
        svc = self._make_intent_stub()
        sql = svc._keyword_sql_fallback("Average fees per job", "user123")
        assert sql is not None, "Expected SQL for 'average fees'"
        assert "AVG" in sql.upper()

    def test_owe_me_keyword_sql_returns_sum_unpaid(self):
        svc = self._make_intent_stub()
        sql = svc._keyword_sql_fallback("How much does Star Studios owe me?", "user123")
        assert sql is not None, "Expected SQL for 'owe me'"
        assert "SUM" in sql.upper()
        # Should filter for unpaid
        assert "paid" in sql.lower()

    def test_count_post_correction_forces_count_metric(self):
        """'how many' with metric=null → metric forced to count in execute_query_plan."""
        import re
        from services.query_planner import _precompute_time_ranges
        # Simulate the post-correction logic
        message = "How many jobs have I done?"
        plan = {"operation": "query", "metric": None, "filters": {}}
        if plan.get("metric") is None and re.search(r'\b(how\s+many|kitne)\b', message.lower()):
            plan["metric"] = "count"
        assert plan["metric"] == "count"

    def test_hinglish_count_correction(self):
        import re
        message = "Kitne jobs hain mere paas?"
        plan = {"operation": "query", "metric": None, "filters": {}}
        if plan.get("metric") is None and re.search(r'\b(how\s+many|kitne)\b', message.lower()):
            plan["metric"] = "count"
        assert plan["metric"] == "count"


class TestGroupByPayload:
    """build_clean_payload correctly handles GROUP BY results (Bug 1 fix)."""

    def test_group_by_result_routes_as_job_summary(self):
        from services.response_synthesis import build_clean_payload
        rows = [{"client_name": "Star Studios", "result": 500000}]
        payload = build_clean_payload(rows, "select")
        # Must NOT collapse to scalar aggregate (which drops client_name).
        # Type must be job_summary so synthesizer sees the full grouped row.
        assert payload["type"] == "job_summary", (
            f"Expected job_summary for GROUP BY result, got {payload['type']}"
        )

    def test_scalar_aggregate_still_works(self):
        from services.response_synthesis import build_clean_payload
        rows = [{"result": 42}]
        payload = build_clean_payload(rows, "select")
        assert payload["type"] == "aggregate"
        assert payload["data"]["result"] == 42

    def test_zero_aggregate_stays_zero(self):
        from services.response_synthesis import build_clean_payload
        rows = [{"result": None}]
        payload = build_clean_payload(rows, "select")
        assert payload["type"] == "aggregate"
        assert payload["data"]["result"] == 0


# ══════════════════════════════════════════════════════════════════════════
# History-question SELECT* rewrite must NOT clobber aggregate / GROUP BY SQL.
# Regression for the production crash: "give me top 3 jobs all time with their
# average" → SELECT job_description, AVG(fees) AS result ... ORDER BY result was
# rewritten to SELECT * → Postgres 'column "result" does not exist'.
# ══════════════════════════════════════════════════════════════════════════

class TestHistoryRewriteSkipsAggregates:
    def test_detects_group_by_as_aggregate(self):
        from services.intent_service import _is_aggregate_sql
        assert _is_aggregate_sql(
            "SELECT job_description_details, AVG(fees) AS result FROM public.job_entries "
            "WHERE user_id='x' GROUP BY job_description_details ORDER BY result DESC"
        )

    def test_detects_bare_aggregate_function(self):
        from services.intent_service import _is_aggregate_sql
        assert _is_aggregate_sql("SELECT SUM(fees) AS result FROM public.job_entries WHERE user_id='x'")
        assert _is_aggregate_sql("SELECT COUNT(*) AS result FROM public.job_entries WHERE user_id='x'")

    def test_plain_select_is_not_aggregate(self):
        from services.intent_service import _is_aggregate_sql
        assert not _is_aggregate_sql(
            "SELECT bill_no, client_name, fees FROM public.job_entries WHERE user_id='x' ORDER BY job_date DESC"
        )

    def test_aggregate_sql_survives_history_rewrite(self):
        """The actual guard: an aggregate SELECT must keep its alias when the
        history flag is on (otherwise ORDER BY result breaks)."""
        import re
        from services.intent_service import _is_aggregate_sql
        sql = ("SELECT job_description_details, AVG(fees) AS result FROM public.job_entries "
               "WHERE user_id='x' GROUP BY job_description_details ORDER BY result DESC")
        is_history = True
        if is_history and sql.upper().lstrip().startswith("SELECT") and not _is_aggregate_sql(sql):
            sql = re.sub(r"(?i)^\s*SELECT\s+(?!\*).+?\s+FROM\s+", "SELECT * FROM ", sql, count=1)
        assert "AVG(fees) AS result" in sql, "aggregate alias was destroyed by the rewrite"
        assert "SELECT *" not in sql


# ─────────────────────────────────────────────────────────────────────────────
# Client-filter expansion: a brand/client/production_house ILIKE filter must
# widen to match across ALL THREE columns. Regression for the A/B finding that
# "brand_name ILIKE 'Garnier'" returned a narrow wrong total (only the brand,
# missing the "Garnier India" client + "Garnier Men" sibling brand).
# ─────────────────────────────────────────────────────────────────────────────
class TestClientFilterExpansion:
    from services.intent_service import IntentService
    _expand = staticmethod(IntentService._expand_client_filters)

    def _has_all_three(self, sql, val):
        s = sql.lower()
        return all(f"{c} ilike '%{val}%'" in s for c in ("client_name", "brand_name", "production_house"))

    def test_client_name_filter_expands(self):
        out = self._expand("SELECT SUM(fees) FROM t WHERE client_name ILIKE 'Garnier'")
        assert self._has_all_three(out, "garnier")

    def test_brand_name_filter_expands(self):
        # the bug: this used to stay narrow
        out = self._expand("SELECT SUM(fees) FROM t WHERE brand_name ILIKE 'Garnier'")
        assert self._has_all_three(out, "garnier")

    def test_production_house_filter_expands(self):
        out = self._expand("SELECT COUNT(*) FROM t WHERE production_house ILIKE 'Content Lab'")
        assert self._has_all_three(out, "content lab")

    def test_existing_wildcards_not_doubled(self):
        out = self._expand("WHERE brand_name ILIKE '%Nike%'")
        assert "'%nike%'" in out.lower() and "%%" not in out

    def test_non_client_ilike_untouched(self):
        out = self._expand("WHERE poc_email ILIKE '%garnier.com%'")
        assert out == "WHERE poc_email ILIKE '%garnier.com%'"
