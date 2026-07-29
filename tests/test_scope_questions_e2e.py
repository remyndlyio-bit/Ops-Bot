"""
End-to-end tests for scope question answering via answer_ledger.

Verifies that:
1. Query results create ledger entries with scope
2. Follow-up scope questions are detected
3. Scope questions are answered from ledger[-1] deterministically (no SQL/LLM)
"""
from services.answer_ledger import (
    LedgerEntry, answer_scope_question, is_scope_question,
    build_entry, get_entries
)


class TestScopeQuestionDetection:
    """Scope question detection against production patterns."""

    def test_detect_scope_questions(self):
        """Common scope question patterns should be detected."""
        scope_Qs = [
            "Do these include paid and unpaid?",
            "Is that only Nike?",
            "Does this include unpaid?",
            "What does this include?",
            "Is that all?",
        ]
        for q in scope_Qs:
            assert is_scope_question(q), f"Should detect scope question: {q}"

    def test_not_scope_new_queries(self):
        """New queries should NOT be detected as scope questions."""
        new_queries = [
            "What about this month?",
            "Show me paid invoices",
            "How many jobs?",
        ]
        for q in new_queries:
            assert not is_scope_question(q), f"Should NOT detect as scope: {q}"


class TestAnswerScopeFromLedger:
    """Answering scope questions from ledger entries (no DB needed)."""

    def test_answer_scope_with_paid_filter_none(self):
        """When paid filter is None, scope answer mentions 'paid and unpaid'."""
        # Build an aggregate entry (scope from a SUM query)
        entry = build_entry(
            question="Total earnings",
            plan={"metric": "sum", "column": "fees", "filters": {"paid": None}, "time_range": None},
            rows=[{"result": 100000}],
            response="₹1,00,000",
        )

        # Simulate ledger state in user_memory
        user_mem = {
            "answer_ledger": {
                "v": 1,
                "entries": [entry.to_dict()]
            }
        }

        # Now answer a scope question
        answer = answer_scope_question("Does this include unpaid?", user_mem)
        assert answer is not None
        assert "paid and unpaid" in answer.lower()

    def test_answer_scope_with_paid_yes(self):
        """When paid filter is 'yes', answer mentions 'paid only'."""
        entry = build_entry(
            question="Paid invoices only",
            plan={"metric": "sum", "column": "fees", "filters": {"paid": "yes"}, "time_range": None},
            rows=[{"result": 75000}],
            response="₹75,000",
        )

        user_mem = {
            "answer_ledger": {
                "v": 1,
                "entries": [entry.to_dict()]
            }
        }

        answer = answer_scope_question("Does this include unpaid?", user_mem)
        assert answer is not None
        assert "paid" in answer.lower()

    def test_answer_scope_with_client_filter(self):
        """When client_name filter is set, answer indicates client-specific."""
        entry = build_entry(
            question="Nike earnings",
            plan={
                "metric": "sum", "column": "fees",
                "filters": {"client_name": "Nike", "paid": None},
                "time_range": None
            },
            rows=[{"result": 50000}],
            response="Nike: ₹50,000",
        )

        user_mem = {
            "answer_ledger": {
                "v": 1,
                "entries": [entry.to_dict()]
            }
        }

        answer = answer_scope_question("Is that only Nike?", user_mem)
        assert answer is not None
        assert "Nike" in answer

    def test_answer_scope_no_ledger(self):
        """If ledger is empty, scope answer returns None."""
        user_mem = {}
        answer = answer_scope_question("Is that all?", user_mem)
        assert answer is None

    def test_answer_scope_list_query(self):
        """Scope questions work on list queries (row count)."""
        entry = build_entry(
            question="All invoices",
            plan={
                "metric": None, "column": None,
                "filters": {},
                "time_range": None
            },
            rows=[
                {"id": "1", "client_name": "Nike", "fees": 25000, "paid": True},
                {"id": "2", "client_name": "Adidas", "fees": 30000, "paid": False},
            ],
            response="2 invoices total",
        )

        user_mem = {
            "answer_ledger": {
                "v": 1,
                "entries": [entry.to_dict()]
            }
        }

        answer = answer_scope_question("Does that include unpaid?", user_mem)
        assert answer is not None
        assert "paid and unpaid" in answer.lower()


class TestLedgerEntryCreation:
    """LedgerEntry creation from query results."""

    def test_build_entry_aggregate(self):
        """build_entry for aggregate query creates correct entry."""
        entry = build_entry(
            question="Total fees",
            plan={"metric": "sum", "column": "fees", "filters": {"paid": "yes"}, "time_range": None},
            rows=[{"result": 100000}],
            response="₹1,00,000 paid",
        )

        assert entry.question == "Total fees"
        assert entry.kind == "aggregate"
        assert entry.value == 100000
        assert entry.scope["filters"]["paid"] == "yes"
        assert entry.row_ids == []

    def test_build_entry_list(self):
        """build_entry for list query creates correct entry."""
        rows = [
            {"id": "123", "client_name": "Nike", "fees": 25000, "paid": True},
            {"id": "456", "client_name": "Adidas", "fees": 30000, "paid": False},
        ]
        entry = build_entry(
            question="Show all jobs",
            plan={"metric": None, "column": None, "filters": {}, "time_range": None},
            rows=rows,
            response="2 jobs found",
        )

        assert entry.kind == "list"
        assert entry.value == 2  # row count
        assert entry.row_ids == ["123", "456"]

    def test_entry_surface_truncated_to_200(self):
        """Entry surface is truncated to 200 chars."""
        long_response = "X" * 300
        entry = build_entry(
            question="Q",
            plan={"metric": "count", "filters": {}, "time_range": None},
            rows=[{"result": 1}],
            response=long_response,
        )
        assert len(entry.surface) == 200
        assert entry.surface == "X" * 200
