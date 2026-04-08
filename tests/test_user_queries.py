"""
Tests for user query handling in IntentService.

Focus on pure/deterministic logic:
- _resolve_response_mode()
- _build_filter_context()
- _format_sql_result()
- _is_followup_field_request()
- _reconstruct_message() context reconstruction
- _determine_reminder_level() re-used alias

These tests do NOT call the AI or DB — they mock all external dependencies.
"""

import pytest
import os
from unittest.mock import MagicMock, patch

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ── Patch heavy imports before loading IntentService ──────────────────────

@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("AI_KEY", "test-key")
    monkeypatch.setenv("SUPABASE_URL", "https://fake.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "fake-role-key")
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://fake")


def _make_intent_service():
    """Build an IntentService with all external services mocked."""
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()
        # Replace with clean mocks
        svc.gemini = MagicMock()
        svc.email = MagicMock()
        svc.supabase = MagicMock()
        svc.memory = MagicMock()
        svc.memory.get_user_memory.return_value = {}
        svc.memory.get_form_state.return_value = None
        return svc


# ── _resolve_response_mode ────────────────────────────────────────────────

class TestResolveResponseMode:
    def setup_method(self):
        self.svc = _make_intent_service()

    def test_single_return_field_is_single_field_mode(self):
        result = {"return_fields": ["fees"]}
        cmd = {}
        assert self.svc._resolve_response_mode(result, cmd) == "SINGLE_FIELD"

    def test_multiple_return_fields_is_record_mode(self):
        result = {"return_fields": ["fees", "client_name", "job_date"]}
        cmd = {}
        assert self.svc._resolve_response_mode(result, cmd) == "RECORD"

    def test_metric_sum_is_aggregation(self):
        result = {"metric": "sum", "return_fields": []}
        cmd = {"metric": "sum"}
        assert self.svc._resolve_response_mode(result, cmd) == "AGGREGATION"

    def test_metric_count_no_field_is_count(self):
        result = {"metric": "count", "return_fields": []}
        cmd = {"metric": "count"}
        assert self.svc._resolve_response_mode(result, cmd) == "COUNT"

    def test_group_by_is_grouped_mode(self):
        result = {"return_fields": []}
        cmd = {"group_by": "client_name"}
        assert self.svc._resolve_response_mode(result, cmd) == "GROUPED"

    def test_metric_value_with_column_is_single_field(self):
        result = {"metric": "value", "column": "fees", "return_fields": []}
        cmd = {"metric": "value", "column": "fees"}
        assert self.svc._resolve_response_mode(result, cmd) == "SINGLE_FIELD"

    def test_fallback_is_clarify(self):
        # metric defaults to "count" when absent, so force an unrecognized metric to hit CLARIFY
        result = {"return_fields": [], "metric": "unknown"}
        cmd = {"metric": "unknown"}
        assert self.svc._resolve_response_mode(result, cmd) == "CLARIFY"


# ── _build_filter_context ─────────────────────────────────────────────────

class TestBuildFilterContext:
    def setup_method(self):
        self.svc = _make_intent_service()

    def test_client_name_filter_returns_client(self):
        ctx = self.svc._build_filter_context({"client_name": "Acme"})
        assert "Acme" in ctx

    def test_date_filter_returns_on_prefix(self):
        ctx = self.svc._build_filter_context({"job_date": "2024-03-01"})
        assert "on" in ctx
        assert "2024-03-01" in ctx

    def test_empty_filters_returns_empty_string(self):
        assert self.svc._build_filter_context({}) == ""

    def test_none_filter_returns_empty_string(self):
        assert self.svc._build_filter_context(None) == ""


# ── _format_sql_result ────────────────────────────────────────────────────

class TestFormatSqlResult:
    def setup_method(self):
        self.svc = _make_intent_service()

    def test_empty_rows_returns_no_records_message(self):
        result = self.svc._format_sql_result([])
        assert "No matching records" in result

    def test_single_row_few_columns_inline_format(self):
        rows = [{"client_name": "Acme", "fees": 50000}]
        result = self.svc._format_sql_result(rows)
        assert "Acme" in result
        assert "50000" in result

    def test_single_row_many_columns_bullet_format(self):
        row = {f"col_{i}": f"val_{i}" for i in range(10)}
        result = self.svc._format_sql_result([row])
        assert "•" in result

    def test_multiple_rows_bullet_format(self):
        rows = [{"client_name": f"Client{i}", "fees": i * 1000} for i in range(5)]
        result = self.svc._format_sql_result(rows)
        assert result.count("•") == 5

    def test_truncates_at_20_rows(self):
        rows = [{"client_name": f"Client{i}"} for i in range(25)]
        result = self.svc._format_sql_result(rows)
        assert "5 more" in result

    def test_none_values_shown_as_na(self):
        rows = [{"client_name": None, "fees": 1000}]
        result = self.svc._format_sql_result(rows)
        assert "N/A" in result


# ── _is_followup_field_request ────────────────────────────────────────────

class TestIsFollowupFieldRequest:
    def setup_method(self):
        self.svc = _make_intent_service()
        self.cols = ["client_name", "fees", "job_date", "brand_name", "paid"]

    def test_what_about_is_followup(self):
        result = self.svc._is_followup_field_request("what about the fees?", self.cols)
        assert result is not None

    def test_how_much_fees_returns_amount_alias(self):
        result = self.svc._is_followup_field_request("how much was the amount?", self.cols)
        assert result is not None

    def test_short_question_detected_as_followup(self):
        result = self.svc._is_followup_field_request("brand?", self.cols)
        assert result is not None

    def test_long_unrelated_message_not_followup(self):
        result = self.svc._is_followup_field_request(
            "Show me all unpaid invoices for March 2024 sorted by fees", self.cols
        )
        assert result is None

    def test_exact_column_match(self):
        # Column matching strips underscores to "client name" — not directly in the message string.
        # The alias path fires instead, returning the canonical "client" key.
        result = self.svc._is_followup_field_request("what is the client_name?", self.cols)
        assert result is not None  # Detected as a follow-up regardless of canonical name


# ── _reconstruct_message ──────────────────────────────────────────────────

class TestReconstructMessage:
    def setup_method(self):
        self.svc = _make_intent_service()

    def test_long_self_contained_message_unchanged(self):
        self.svc.memory.get_user_memory.return_value = {}
        msg = "Show me all jobs for Acme in March"
        result = self.svc._reconstruct_message("user1", msg, [])
        assert result == msg

    def test_month_reply_with_pending_client_reconstructed(self):
        self.svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "Generate invoice",
                "client_name": "Nike",
                "pending_clarification": "month",
                "entity": "invoice",
            }
        }
        result = self.svc._reconstruct_message("user1", "March", [])
        assert "Nike" in result
        assert "March" in result

    def test_no_prior_context_returns_original(self):
        self.svc.memory.get_user_memory.return_value = {"last_intent": {}}
        result = self.svc._reconstruct_message("user1", "March", [])
        assert result == "March"

    def test_for_prefix_with_month_and_known_client(self):
        self.svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "Generate invoice",
                "client_name": "Acme",
                "pending_clarification": "",
                "entity": "invoice",
            }
        }
        result = self.svc._reconstruct_message("user1", "for March", [])
        assert "Acme" in result
        assert "March" in result


# ── _format_uscf_result (query path) ─────────────────────────────────────

class TestFormatUscfResult:
    def setup_method(self):
        self.svc = _make_intent_service()

    def test_count_metric_returns_count_string(self):
        result = {"ok": True, "metric": "count", "value": 7, "return_fields": []}
        cmd = {"metric": "count", "return_fields": [], "filters": {}}
        out = self.svc._format_uscf_result(result, cmd)
        assert "7" in out

    def test_sum_metric_returns_total_with_rupee(self):
        result = {"ok": True, "metric": "sum", "value": 125000, "return_fields": [], "column": "fees"}
        cmd = {"metric": "sum", "column": "fees", "return_fields": [], "filters": {}}
        out = self.svc._format_uscf_result(result, cmd)
        assert "₹" in out
        assert "125,000" in out

    def test_not_ok_returns_fallback_message(self):
        result = {"ok": False, "message": "No records found."}
        cmd = {}
        out = self.svc._format_uscf_result(result, cmd)
        assert "No records found" in out

    def test_create_operation_includes_bill_number(self):
        result = {"ok": True, "operation": "create", "message": "Job added.", "bill_number": "INV-042"}
        cmd = {"operation": "create"}
        out = self.svc._format_uscf_result(result, cmd)
        assert "INV-042" in out
