"""
Tests for WP-1: AnswerLedger + Focus conversation state.

Verifies:
- Ledger append and trim (cap at 10)
- Focus updates from answers
- Serialization/deserialization
- Schema versioning
"""
import json
from datetime import datetime
from services.conversation_state import (
    ConversationState,
    LedgerEntry,
    Focus,
    load_conversation_state,
    save_conversation_state,
)


class TestLedgerEntry:
    """LedgerEntry factory and persistence."""

    def test_from_answer_factory(self):
        """LedgerEntry.from_answer() creates entry with defaults."""
        entry = LedgerEntry.from_answer(
            question="Total billing this year",
            kind="aggregate",
            value={"count": 5, "sum": 125000},
            reply="₹1,25,000 for 5 jobs",
        )

        assert entry.turn_id is not None
        assert entry.ts is not None
        assert entry.question == "Total billing this year"
        assert entry.kind == "aggregate"
        assert entry.value == {"count": 5, "sum": 125000}
        assert entry.surface == "₹1,25,000 for 5 jobs"
        assert entry.row_ids == []
        assert entry.scope == {}

    def test_from_answer_with_plan_and_scope(self):
        """LedgerEntry includes plan and scope when provided."""
        plan = {"metric": "sum", "column": "fees", "filters": {}}
        scope = {"filters": {"paid": "no"}, "time_range": {"start": "2026-01-01", "end": "2026-03-31"}}
        row_ids = ["row_123", "row_456"]

        entry = LedgerEntry.from_answer(
            question="Unpaid invoices",
            kind="list",
            value=125000,
            reply="2 unpaid invoices",
            plan=plan,
            scope=scope,
            row_ids=row_ids,
        )

        assert entry.plan == plan
        assert entry.scope == scope
        assert entry.row_ids == row_ids


class TestFocus:
    """Focus context tracking."""

    def test_focus_initial_empty(self):
        """Focus initializes empty."""
        focus = Focus()
        assert focus.client is None
        assert focus.time_range is None
        assert focus.row_ids == []

    def test_focus_update(self):
        """Focus.update() changes fields and timestamp."""
        focus = Focus()
        old_ts = focus.updated_at

        focus.update(client="Nike", time_range={"start": "2026-01-01"})

        assert focus.client == "Nike"
        assert focus.time_range == {"start": "2026-01-01"}
        assert focus.updated_at > old_ts

    def test_focus_update_partial(self):
        """Focus.update() preserves unspecified fields."""
        focus = Focus(client="Nike", row_ids=["1", "2"])

        focus.update(time_range={"start": "2026-01-01"})

        assert focus.client == "Nike"  # unchanged
        assert focus.time_range == {"start": "2026-01-01"}  # updated
        assert focus.row_ids == ["1", "2"]  # unchanged


class TestConversationState:
    """ConversationState ledger + focus."""

    def test_state_initial_empty(self):
        """ConversationState initializes with v=1 and empty ledger/focus."""
        state = ConversationState()
        assert state.v == 1
        assert state.ledger == []
        assert state.focus.client is None

    def test_append_answer_adds_to_ledger(self):
        """append_answer() adds entry to ledger."""
        state = ConversationState()

        state.append_answer(
            question="Total fees",
            kind="aggregate",
            value=100000,
            reply="₹1,00,000",
        )

        assert len(state.ledger) == 1
        assert state.ledger[0].value == 100000

    def test_append_answer_updates_focus(self):
        """append_answer() updates focus from scope."""
        state = ConversationState()

        scope = {
            "filters": {"client_name": "Nike", "paid": "no"},
            "time_range": {"start": "2026-01-01"},
        }
        state.append_answer(
            question="Nike unpaid",
            kind="aggregate",
            value=50000,
            reply="Nike owes ₹50k",
            scope=scope,
            row_ids=["row_1"],
        )

        assert state.focus.client == "Nike"
        assert state.focus.time_range == {"start": "2026-01-01"}
        assert state.focus.row_ids == ["row_1"]

    def test_ledger_trim_at_10(self):
        """Ledger trims to ≤10 entries."""
        state = ConversationState()

        for i in range(15):
            state.append_answer(
                question=f"Query {i}",
                kind="aggregate",
                value=i * 1000,
                reply=f"Result {i}",
            )

        assert len(state.ledger) == 10
        # Most recent entries are kept
        assert state.ledger[0].value == 5 * 1000
        assert state.ledger[-1].value == 14 * 1000

    def test_to_dict_serialization(self):
        """to_dict() produces valid dict."""
        state = ConversationState()
        state.append_answer(
            question="Test",
            kind="aggregate",
            value=100,
            reply="Result",
            scope={"filters": {}},
        )

        data = state.to_dict()
        assert isinstance(data, dict)
        assert data["v"] == 1
        assert len(data["ledger"]) == 1
        assert "focus" in data
        assert isinstance(data["ledger"][0], dict)

    def test_from_dict_deserializes(self):
        """from_dict() recreates state from dict."""
        original = ConversationState()
        original.append_answer(
            question="Test query",
            kind="aggregate",
            value=500,
            reply="Got 500",
            scope={"filters": {"client_name": "Acme"}},
        )

        data = original.to_dict()
        restored = ConversationState.from_dict(data)

        assert len(restored.ledger) == 1
        assert restored.ledger[0].question == "Test query"
        assert restored.ledger[0].value == 500
        assert restored.focus.client == "Acme"

    def test_from_dict_handles_empty(self):
        """from_dict(None) or from_dict({}) returns empty state."""
        assert ConversationState.from_dict(None).ledger == []
        assert ConversationState.from_dict({}).ledger == []

    def test_schema_versioning_round_trip(self):
        """State with v=1 survives round-trip."""
        state = ConversationState(v=1)
        state.append_answer("Q", "aggregate", 100, "A", scope={"filters": {}})

        data = state.to_dict()
        assert data["v"] == 1

        restored = ConversationState.from_dict(data)
        assert restored.v == 1
        assert len(restored.ledger) == 1


class TestConversationStateHelpers:
    """Integration: load_conversation_state, save_conversation_state."""

    def test_load_from_user_memory(self):
        """load_conversation_state() reads from user_memory."""
        user_memory = {
            "conversation_state": {
                "v": 1,
                "ledger": [
                    {
                        "turn_id": "uuid1",
                        "ts": "2026-07-29T00:00:00+00:00",
                        "question": "Test",
                        "kind": "aggregate",
                        "plan": None,
                        "scope": {},
                        "value": 100,
                        "row_ids": [],
                        "surface": "Result",
                    }
                ],
                "focus": {"client": "Nike", "time_range": None, "row_ids": [], "updated_at": "2026-07-29T00:00:00+00:00"},
            }
        }

        state = load_conversation_state(user_memory)
        assert len(state.ledger) == 1
        assert state.focus.client == "Nike"

    def test_save_to_user_memory(self):
        """save_conversation_state() updates user_memory."""
        user_memory: dict = {}
        state = ConversationState()
        state.append_answer("Q", "aggregate", 100, "A")

        updated = save_conversation_state(user_memory, state)
        assert "conversation_state" in updated
        assert updated["conversation_state"]["v"] == 1
        assert len(updated["conversation_state"]["ledger"]) == 1

    def test_load_from_empty_memory(self):
        """load_conversation_state() handles missing key gracefully."""
        state = load_conversation_state({})
        assert state.v == 1
        assert state.ledger == []
