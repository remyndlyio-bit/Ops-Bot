"""
WP-1: AnswerLedger + Focus

Conversation state tracking: the bot remembers its own claims (answer ledger)
and the current entity/time context (focus). These enable follow-ups like
"what about this month?" and scope questions like "does this include unpaid?".

Shape persisted in user_memory under "conversation_state" (schema-versioned
JSONB blob for forward migrations).

Ledger: ordered list of ≤10 answers (question → scope → value), each with
the exact Path-3 plan/SQL filters used. Enables:
- "mark this as paid" (row_ids from last shown rows)
- "does this include paid and unpaid?" (answer from ledger[-1].scope)
- "what were they?" (context for follow-up queries)

Focus: the entity/time context in play (client, time_range, row_ids shown).
Updated on every answer, used for inherited context resolution in follow-ups.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

import json


@dataclass
class LedgerEntry:
    """One answer: question, scope, value, rows shown."""

    turn_id: str           # uuid — unique per answer
    ts: str                # ISO8601 — when answer was given
    question: str          # user's message verbatim (first 200 chars)
    kind: str              # "aggregate" | "list" | "field" | "action"
    plan: Optional[Dict]   # exact Path-3 plan/SQL filters used (or None)
    scope: Dict[str, Any]  # {"filters": {...}, "time_range": {...}, "metric": ...}
    value: Any             # the number / count / string given
    row_ids: List[str]     # rows shown (for "mark this as paid", "the first one")
    surface: str           # first 200 chars of reply actually sent

    @staticmethod
    def from_answer(
        question: str,
        kind: str,
        value: Any,
        reply: str,
        plan: Optional[Dict] = None,
        scope: Optional[Dict] = None,
        row_ids: Optional[List[str]] = None,
    ) -> LedgerEntry:
        """Factory: construct from answer components."""
        return LedgerEntry(
            turn_id=str(uuid4()),
            ts=datetime.now(timezone.utc).isoformat(),
            question=question[:200],
            kind=kind,
            plan=plan,
            scope=scope or {},
            value=value,
            row_ids=row_ids or [],
            surface=reply[:200],
        )


@dataclass
class Focus:
    """Current entity/time context."""

    client: Optional[str] = None
    time_range: Optional[Dict[str, Any]] = None
    row_ids: List[str] = field(default_factory=list)
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def update(
        self,
        client: Optional[str] = None,
        time_range: Optional[Dict[str, Any]] = None,
        row_ids: Optional[List[str]] = None,
    ) -> None:
        """Update focus fields."""
        if client is not None:
            self.client = client
        if time_range is not None:
            self.time_range = time_range
        if row_ids is not None:
            self.row_ids = row_ids
        self.updated_at = datetime.now(timezone.utc).isoformat()


@dataclass
class ConversationState:
    """Conversation state: ledger + focus."""

    v: int = 1                                              # schema version
    ledger: List[LedgerEntry] = field(default_factory=list)  # ≤10 entries
    focus: Focus = field(default_factory=Focus)

    def append_answer(
        self,
        question: str,
        kind: str,
        value: Any,
        reply: str,
        plan: Optional[Dict] = None,
        scope: Optional[Dict] = None,
        row_ids: Optional[List[str]] = None,
    ) -> None:
        """Append an answer and update focus. Trims ledger to ≤10."""
        entry = LedgerEntry.from_answer(question, kind, value, reply, plan, scope, row_ids)
        self.ledger.append(entry)

        # Cap at 10
        if len(self.ledger) > 10:
            self.ledger = self.ledger[-10:]

        # Update focus from answer
        if scope:
            self.focus.update(
                client=scope.get("filters", {}).get("client_name"),
                time_range=scope.get("time_range"),
                row_ids=row_ids or [],
            )

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dict (for persistence)."""
        return {
            "v": self.v,
            "ledger": [asdict(e) for e in self.ledger],
            "focus": asdict(self.focus),
        }

    @staticmethod
    def from_dict(data: Optional[Dict[str, Any]]) -> ConversationState:
        """Deserialize from dict. Handles schema upgrades."""
        if not data:
            return ConversationState()

        v = data.get("v", 1)
        if v != 1:
            # Future: add schema upgrade logic here
            pass

        try:
            ledger_data = data.get("ledger", [])
            ledger = []
            for e in ledger_data:
                entry = LedgerEntry(
                    turn_id=e.get("turn_id", str(uuid4())),
                    ts=e.get("ts", datetime.now(timezone.utc).isoformat()),
                    question=e.get("question", ""),
                    kind=e.get("kind", "aggregate"),
                    plan=e.get("plan"),
                    scope=e.get("scope", {}),
                    value=e.get("value"),
                    row_ids=e.get("row_ids", []),
                    surface=e.get("surface", ""),
                )
                ledger.append(entry)

            focus_data = data.get("focus", {})
            focus = Focus(
                client=focus_data.get("client"),
                time_range=focus_data.get("time_range"),
                row_ids=focus_data.get("row_ids", []),
                updated_at=focus_data.get("updated_at", datetime.now(timezone.utc).isoformat()),
            )

            return ConversationState(v=v, ledger=ledger, focus=focus)
        except Exception as e:
            # Deserialization error — return empty state, don't crash
            from utils.logger import logger
            logger.warning(f"[ConversationState] deserialization failed: {e} — resetting to empty")
            return ConversationState()


def load_conversation_state(user_memory: Optional[Dict[str, Any]]) -> ConversationState:
    """Load from user_memory['conversation_state']. Returns empty state if missing."""
    if not user_memory:
        return ConversationState()
    state_blob = user_memory.get("conversation_state")
    return ConversationState.from_dict(state_blob)


def save_conversation_state(user_memory: Dict[str, Any], state: ConversationState) -> Dict[str, Any]:
    """Save conversation state back to user_memory. Returns updated memory."""
    user_memory["conversation_state"] = state.to_dict()
    return user_memory
