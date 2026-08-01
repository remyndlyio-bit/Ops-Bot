"""
Live LLM integration tests for Bugs 1, 2, 4.

These tests call the actual Gemini/OpenRouter LLM (if AI_KEY is set) to verify
that the prompt fixes for known bugs produce correct planner outputs.

Requires AI_KEY env var. Set to skip if missing.
"""
import os
import pytest
from services.query_planner import build_operation_plan
from services.gemini_service import GeminiService
from unittest.mock import MagicMock


@pytest.fixture
def gemini_service():
    """Real Gemini service (needs a REAL AI_KEY).

    The guard here used to be `if not os.getenv("AI_KEY")`, which never
    fired: conftest.py injects a placeholder AI_KEY so service constructors
    survive import, and CI sets the same value explicitly. AI_KEY is
    therefore ALWAYS truthy, so these 11 tests never skipped — they ran
    against a bogus key and failed on every single run. has_real_ai_key()
    compares the VALUE against that placeholder instead of just checking
    presence, so the skip is finally honest.
    """
    from tests.conftest import has_real_ai_key
    if not has_real_ai_key():
        pytest.skip("No real AI_KEY (unset or placeholder) — skipping live LLM tests")
    return GeminiService()


class TestBug1LiveAggregateQueries:
    """Bug 1: Planner should emit GROUP BY for aggregate queries.

    Tests the actual planner with live LLM calls.
    """

    def test_biggest_client_produces_group_by(self, gemini_service):
        """'Who is my biggest client?' → GROUP BY client_name, SUM, DESC, LIMIT 1"""
        plan = build_operation_plan(
            "Who is my biggest client?",
            gemini_service=gemini_service,
        )

        assert plan is not None, "Should produce a plan"
        assert plan.get("group_by") == "client_name", f"Should group by client_name, got: {plan.get('group_by')}"
        assert plan.get("metric") == "sum", f"Should use SUM metric, got: {plan.get('metric')}"
        assert plan.get("column") == "fees", f"Should sum fees, got: {plan.get('column')}"
        assert plan.get("order") == "desc", f"Should order DESC, got: {plan.get('order')}"
        assert plan.get("limit") == 1, f"Should limit to 1, got: {plan.get('limit')}"

    def test_top_earner_produces_group_by(self, gemini_service):
        """'Top earner' is a client aggregate, not a job aggregate."""
        plan = build_operation_plan(
            "Who's my top earner?",
            gemini_service=gemini_service,
        )

        assert plan is not None
        assert plan.get("group_by") == "client_name", "Top earner should group by client"
        assert plan.get("metric") == "sum", "Should aggregate with SUM"

    def test_average_fees_produces_avg(self, gemini_service):
        """'Average fees per job' → metric:avg, column:fees"""
        plan = build_operation_plan(
            "Average fees per job",
            gemini_service=gemini_service,
        )

        assert plan is not None
        assert plan.get("metric") == "avg", f"Should use AVG metric, got: {plan.get('metric')}"
        assert plan.get("column") == "fees", f"Should average fees, got: {plan.get('column')}"

    def test_client_debt_produces_unpaid_filter_and_sum(self, gemini_service):
        """'How much does Star Studios owe me?' → filters paid:no + client filter + SUM"""
        plan = build_operation_plan(
            "How much does Star Studios owe me?",
            gemini_service=gemini_service,
        )

        assert plan is not None
        assert plan.get("metric") == "sum", f"Should use SUM for 'how much', got: {plan.get('metric')}"
        filters = plan.get("filters", {})
        assert filters.get("paid") == "no", f"Should filter for unpaid, got: {filters.get('paid')}"
        assert "Star Studios" in str(filters.get("client_name", "")), "Should filter by client name"


class TestBug2LiveUnfilteredCount:
    """Bug 2: Planner should emit COUNT for unfiltered 'how many' queries.

    The post-process fix at line 1018-1023 in query_planner.py should catch
    metric=null cases and force metric=count when message says "how many".
    """

    def test_how_many_jobs_unfiltered(self, gemini_service):
        """'How many jobs have I done?' → metric:count"""
        plan = build_operation_plan(
            "How many jobs have I done?",
            gemini_service=gemini_service,
        )

        assert plan is not None, "Should produce a plan"
        assert plan.get("metric") == "count", f"Should use COUNT metric, got: {plan.get('metric')}"
        # Should have no GROUP BY (unfiltered count of all records)
        assert plan.get("group_by") is None, f"Unfiltered count shouldn't group by, got: {plan.get('group_by')}"

    def test_how_many_total_jobs(self, gemini_service):
        """'How many total jobs do I have?' → metric:count"""
        plan = build_operation_plan(
            "How many total jobs do I have?",
            gemini_service=gemini_service,
        )

        assert plan is not None
        assert plan.get("metric") == "count", f"Should count jobs, got metric: {plan.get('metric')}"

    def test_how_many_invoices_sent(self, gemini_service):
        """'How many invoices have I sent?' → metric:count + bill_sent filter"""
        plan = build_operation_plan(
            "How many invoices have I sent?",
            gemini_service=gemini_service,
        )

        assert plan is not None
        assert plan.get("metric") == "count", f"Should count, got metric: {plan.get('metric')}"
        filters = plan.get("filters", {})
        assert filters.get("bill_sent") == "yes", f"Should filter bill_sent=yes, got: {filters.get('bill_sent')}"

    def test_how_many_invoices_without_email(self, gemini_service):
        """'How many invoices to clients with no email?' → metric:count + compound filters"""
        plan = build_operation_plan(
            "How many invoices sent to clients with no email",
            gemini_service=gemini_service,
        )

        assert plan is not None
        assert plan.get("metric") == "count", f"Should count, got metric: {plan.get('metric')}"
        filters = plan.get("filters", {})
        assert filters.get("bill_sent") == "yes", "Should filter bill_sent=yes"
        assert filters.get("poc_email") is None or filters.get("poc_email") == "", "Should filter poc_email empty"


class TestBug4LiveSmartCapture:
    """Bug 4: Smart-capture should extract 'paid' field.

    Tests the extract_job_fields prompt which explicitly mentions paid extraction.
    """

    def test_extraction_recognizes_paid_keyword(self, gemini_service):
        """'Add job for Acme, 25k, shoot, paid' → extracts paid=true"""
        fields = gemini_service.extract_job_fields(
            "Add a job for Acme, 25k, shoot, paid"
        )

        assert fields is not None, "Should extract job fields"
        assert fields.get("paid") is not None, f"Should extract paid field, got: {fields}"
        # "paid" should be recognized as paid=true
        assert fields.get("paid") is True or fields.get("paid") == "true" or "yes" in str(fields.get("paid", "")).lower(), \
            f"Should recognize 'paid' keyword, got: {fields.get('paid')}"

    def test_extraction_recognizes_unpaid_keyword(self, gemini_service):
        """'Add job for Acme, 25k, shoot, unpaid' → extracts paid=false"""
        fields = gemini_service.extract_job_fields(
            "Add a job for Acme, 25k, shoot, unpaid"
        )

        assert fields is not None
        assert fields.get("paid") is not None, "Should extract paid field"
        # "unpaid" should be recognized as paid=false
        assert fields.get("paid") is False or fields.get("paid") == "false" or "no" in str(fields.get("paid", "")).lower(), \
            f"Should recognize 'unpaid' keyword, got: {fields.get('paid')}"

    def test_extraction_with_hindi_paid(self, gemini_service):
        """Hinglish: 'Add job for Nike, 50k, photography, ho gaya' → paid=true"""
        fields = gemini_service.extract_job_fields(
            "Add job for Nike, 50k, photography, ho gaya"
        )

        assert fields is not None
        # "ho gaya" (it's done) should mean paid
        if fields.get("paid") is not None:
            assert fields.get("paid") is True or fields.get("paid") == "true" or "yes" in str(fields.get("paid", "")).lower(), \
                f"Should recognize 'ho gaya' as paid, got: {fields.get('paid')}"
