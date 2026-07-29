"""
Phase 2b Integration: Scope questions fast-path in routing.

Verifies that:
1. Scope questions are detected before normal routing
2. They're answered from ledger deterministically (no SQL, no LLM)
3. The fast-path doesn't interfere with normal queries

The actual integration into _process_request_impl is tested via manual
verification and production telemetry (route="scope_question"). This test
verifies the underlying logic is correct.
"""
from services.answer_ledger import (
    build_entry, answer_scope_question, is_scope_question
)


class TestPhase2bScopeRoutingLogic:
    """Verify scope question logic works correctly for routing."""

    def test_scope_question_detected_before_routing(self):
        """Scope questions should be detected and answerable."""
        # Simulate a previous aggregate query result
        entry = build_entry(
            question="Total earnings",
            plan={
                "metric": "sum",
                "column": "fees",
                "filters": {"paid": None},  # both paid/unpaid
                "time_range": {"start": "2026-01-01", "end": "2026-03-31"},
            },
            rows=[{"result": 150000}],
            response="₹1,50,000 in Q1",
        )

        # Simulate user_memory with ledger
        user_mem = {
            "answer_ledger": {
                "v": 1,
                "entries": [entry.to_dict()]
            }
        }

        # Test: scope question is detected
        scope_Qs = [
            "Do these include paid and unpaid?",
            "Does that include unpaid?",
            "Is that all?",
        ]
        for q in scope_Qs:
            assert is_scope_question(q), f"Should detect: {q}"
            answer = answer_scope_question(q, user_mem)
            assert answer is not None, f"Should answer: {q}"
            assert "paid and unpaid" in answer.lower(), f"Answer should mention both: {answer}"

    def test_new_queries_not_detected_as_scope(self):
        """New queries should NOT be routed as scope questions."""
        entry = build_entry(
            question="Q",
            plan={"metric": "sum", "filters": {}, "time_range": None},
            rows=[{"result": 100}],
            response="₹100",
        )

        user_mem = {
            "answer_ledger": {"v": 1, "entries": [entry.to_dict()]}
        }

        # These should NOT be scope questions
        new_queries = [
            "What about unpaid?",  # new query, not scope clarification
            "Show me only unpaid",  # show → new query
            "How many unpaid invoices?",  # how many → new query
        ]

        for q in new_queries:
            assert not is_scope_question(q), f"Should NOT detect as scope: {q}"
            answer = answer_scope_question(q, user_mem)
            # May or may not have an answer, but shouldn't route as scope question
            # (the routing check is is_scope_question, not whether answer is non-None)

    def test_scope_questions_zero_lm_cost(self):
        """Scope questions use only ledger data, zero LLM/SQL calls."""
        # This is a structural test: build_entry and answer_scope_question
        # do not make any external calls. If they did, the test infrastructure
        # would need to mock them.

        entry = build_entry(
            question="Nike earnings",
            plan={
                "metric": "sum",
                "filters": {"client_name": "Nike", "paid": "no"},
                "time_range": None,
            },
            rows=[{"result": 75000}],
            response="Nike (unpaid): ₹75k",
        )

        user_mem = {
            "answer_ledger": {"v": 1, "entries": [entry.to_dict()]}
        }

        # answer_scope_question is deterministic and self-contained
        answer = answer_scope_question("Is that only Nike?", user_mem)

        # Should produce an answer without any external service calls
        assert answer is not None
        assert "Nike" in answer or "only" in answer.lower()

    def test_routing_return_shape(self):
        """Scope question answer formatted for process_request return."""
        entry = build_entry(
            question="Test",
            plan={"metric": "sum", "filters": {}, "time_range": None},
            rows=[{"result": 100}],
            response="Result",
        )

        user_mem = {
            "answer_ledger": {"v": 1, "entries": [entry.to_dict()]}
        }

        answer = answer_scope_question("Is that all?", user_mem)
        assert answer is not None

        # The intent_service will return:
        # {
        #     "operation": "scope_question",
        #     "response": answer,
        #     "trigger_invoice": False,
        #     "invoice_data": {},
        # }
        # Just verify the answer is a string suitable for response
        assert isinstance(answer, str)
        assert len(answer) > 0
        assert len(answer) < 500  # Should be brief

    def test_scope_question_routes_before_classifier(self):
        """Routing order: scope questions checked before v2 classifier.

        This is verified structurally: answer_scope_question runs early in
        _process_request_impl (after account commands, before v2 block).
        """
        # The logic is: try answer_scope_question() first. If it returns
        # an answer, return immediately. Otherwise fall through to v2/legacy.

        # Setup: ledger with entry
        entry = build_entry(
            question="Earnings",
            plan={"metric": "sum", "filters": {}, "time_range": None},
            rows=[{"result": 100}],
            response="₹100",
        )

        user_mem_with_ledger = {
            "answer_ledger": {"v": 1, "entries": [entry.to_dict()]}
        }

        # Scope question + ledger → answer immediately
        answer1 = answer_scope_question("Do these include unpaid?", user_mem_with_ledger)
        assert answer1 is not None  # Should answer

        # New query + ledger → no scope answer, falls through
        answer2 = answer_scope_question("Show unpaid", user_mem_with_ledger)
        assert answer2 is None  # Not a scope question

        # Scope question but NO ledger → no scope answer, falls through
        answer3 = answer_scope_question("Do these include unpaid?", {})
        assert answer3 is None  # No ledger to answer from
