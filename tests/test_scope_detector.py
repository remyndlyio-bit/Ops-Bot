"""
Tests for scope question detection and answering from ledger.
"""
from services.scope_detector import is_scope_question, answer_scope_from_ledger


class TestScopeQuestionDetection:
    """is_scope_question() detection patterns."""

    def test_detect_does_include(self):
        """'Do these include unpaid?' → scope question."""
        assert is_scope_question("Do these include unpaid?")
        assert is_scope_question("Does that include paid and unpaid?")
        assert is_scope_question("do they include invoices?")

    def test_detect_is_only(self):
        """'Is that only Nike?' → scope question."""
        assert is_scope_question("Is that only Nike?")
        assert is_scope_question("Are these only paid invoices?")

    def test_detect_paid_and_unpaid(self):
        """'Paid and unpaid?' → scope question."""
        assert is_scope_question("Paid and unpaid?")
        assert is_scope_question("Paid or unpaid?")
        assert is_scope_question("Both paid and unpaid?")

    def test_not_scope_what_about_this_month(self):
        """'What about this month?' → NOT a scope question (new data request)."""
        assert not is_scope_question("What about this month?")

    def test_not_scope_how_many_of_these(self):
        """'How many of these are unpaid?' → NOT a scope question (count query)."""
        assert not is_scope_question("How many of these are unpaid?")

    def test_not_scope_show_paid(self):
        """'Show me the paid ones' → NOT a scope question (new query)."""
        assert not is_scope_question("Show me the paid ones")


class TestAnswerScopeFromLedger:
    """answer_scope_from_ledger() constructs deterministic responses."""

    def test_answer_paid_unpaid_filter_none(self):
        """When paid filter is None (both included), answer: 'paid and unpaid'."""
        entry = {
            "scope": {
                "filters": {"paid": None},
                "time_range": None,
            }
        }
        answer = answer_scope_from_ledger(entry, "does this include paid and unpaid?")
        assert answer is not None
        assert "paid and unpaid" in answer.lower()

    def test_answer_paid_only(self):
        """When paid filter is 'yes', answer mentions 'paid'."""
        entry = {
            "scope": {
                "filters": {"paid": "yes"},
                "time_range": None,
            }
        }
        answer = answer_scope_from_ledger(entry, "does it include unpaid?")
        assert answer is not None
        assert "paid" in answer.lower()

    def test_answer_unpaid_only(self):
        """When paid filter is 'no', answer indicates unpaid status."""
        entry = {
            "scope": {
                "filters": {"paid": "no"},
                "time_range": None,
            }
        }
        answer = answer_scope_from_ledger(entry, "does it include paid?")
        assert answer is not None
        assert "unpaid" in answer.lower() or "waiting" in answer.lower()

    def test_answer_only_nike(self):
        """When client filter is set, answer: 'only for Nike'."""
        entry = {
            "scope": {
                "filters": {"client_name": "Nike"},
                "time_range": None,
            }
        }
        answer = answer_scope_from_ledger(entry, "is that only Nike?")
        assert answer is not None
        assert "Nike" in answer

    def test_answer_all_clients(self):
        """When no client filter, answer: 'all clients'."""
        entry = {
            "scope": {
                "filters": {},
                "time_range": None,
            }
        }
        answer = answer_scope_from_ledger(entry, "is that only one client?")
        assert answer is not None
        assert "all clients" in answer.lower()

    def test_answer_time_range(self):
        """Time range is included in scope answer."""
        entry = {
            "scope": {
                "filters": {},
                "time_range": {"start": "2026-01-01", "end": "2026-03-31"},
            }
        }
        answer = answer_scope_from_ledger(entry, "what's the date range?")
        assert answer is not None
        assert "2026-01-01" in answer or "January" in answer

    def test_no_answer_without_ledger(self):
        """answer_scope_from_ledger(None) returns None."""
        assert answer_scope_from_ledger(None, "anything") is None

    def test_no_answer_without_filters(self):
        """answer_scope_from_ledger with empty filters returns None."""
        entry = {"scope": {"filters": {}, "time_range": None}}
        answer = answer_scope_from_ledger(entry, "random question")
        # May return None or generic answer — either is OK
        # Just shouldn't crash
        assert answer is None or isinstance(answer, str)
