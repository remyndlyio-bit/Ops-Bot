"""
Session 2 of the FlowMachine v2 migration.

Owns ONE piece of mutable state per user — `flow_v2` — and is the ONLY module
allowed to write it. Everything else reads.

Persistence: uses MemoryService.update_user_memory to merge a single `flow_v2`
key into the user's memory blob. Survives across messages, per-user.

State shape:
    {
      "flow":       FlowName,       # see Flow enum below; "IDLE" when nothing pending
      "context":    dict,           # whatever the current flow needs (client_name, row_ids, ...)
      "started_at": ISO8601 string, # for 30-minute TTL auto-reset
      "stack":      [               # for push/pop side-question handling
          { "flow": ..., "context": ..., "started_at": ... },
          ...
      ]
    }

Out of scope for session 2:
- Migrating every legacy `awaiting_*` flag. Session 2 only owns
  INVOICE_AWAIT_SEND_CONFIRM. The other flows continue to read from
  user_mem.get("awaiting_*") until session 3 ports them.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from utils.logger import logger

# Flow names owned by FlowMachine v2.
# We add to this list as sessions 2.x → 3 migrate more flows.
FLOW_IDLE = "IDLE"
FLOW_INVOICE_AWAIT_SEND_CONFIRM = "INVOICE_AWAIT_SEND_CONFIRM"

KNOWN_FLOWS = {
    FLOW_IDLE,
    FLOW_INVOICE_AWAIT_SEND_CONFIRM,
}

# Idle TTL: 30 min of silence in a flow → auto-reset to IDLE.
IDLE_TTL_MINUTES = 30

# Stack cap: prevent runaway nesting.
MAX_STACK_DEPTH = 2

# The key under which all v2 state lives inside user_memory.
_MEM_KEY = "flow_v2"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _parse_iso(s: str) -> Optional[datetime]:
    if not s:
        return None
    try:
        # Python's fromisoformat handles the format we emit.
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _empty_state() -> Dict[str, Any]:
    return {
        "flow": FLOW_IDLE,
        "context": {},
        "started_at": None,
        "stack": [],
    }


class FlowMachine:
    """Per-instance facade over MemoryService for v2 flow state.

    All reads/writes are scoped by user_id. Callers pass the user_id every time;
    there is no per-instance user state.
    """

    def __init__(self, memory):
        self._mem = memory  # MemoryService

    # ── Read ──────────────────────────────────────────────────────────

    def get_state(self, user_id: str) -> Dict[str, Any]:
        """Return current v2 state (always a fully-shaped dict, never None)."""
        raw = (self._mem.get_user_memory(user_id) or {}).get(_MEM_KEY)
        if not isinstance(raw, dict):
            return _empty_state()
        # Defensive fill — if a legacy/partial blob was stored we top up missing keys.
        return {
            "flow":       raw.get("flow") or FLOW_IDLE,
            "context":    raw.get("context") if isinstance(raw.get("context"), dict) else {},
            "started_at": raw.get("started_at"),
            "stack":      raw.get("stack") if isinstance(raw.get("stack"), list) else [],
        }

    def is_idle(self, user_id: str) -> bool:
        return self.get_state(user_id).get("flow") == FLOW_IDLE

    def current_flow(self, user_id: str) -> str:
        return self.get_state(user_id).get("flow", FLOW_IDLE)

    # ── TTL ───────────────────────────────────────────────────────────

    def expire_if_stale(self, user_id: str) -> bool:
        """If the user's flow started > IDLE_TTL_MINUTES ago, reset to IDLE.
        Returns True if a reset was applied (caller may want to send a brief
        'I'd dropped that, want to restart?' nudge)."""
        state = self.get_state(user_id)
        if state["flow"] == FLOW_IDLE:
            return False
        started = _parse_iso(state.get("started_at") or "")
        if not started:
            # No timestamp — be conservative and reset.
            logger.info(f"[FLOW_V2] {user_id} flow={state['flow']} has no started_at — resetting")
            self.reset(user_id)
            return True
        age = datetime.now(timezone.utc) - started
        if age >= timedelta(minutes=IDLE_TTL_MINUTES):
            logger.info(
                f"[FLOW_V2] {user_id} flow={state['flow']} stale "
                f"(age={int(age.total_seconds()/60)}m) — auto-resetting"
            )
            self.reset(user_id)
            return True
        return False

    # ── Write ─────────────────────────────────────────────────────────

    def reset(self, user_id: str) -> None:
        """Hard reset: clear flow and any pending stack."""
        self._mem.update_user_memory(user_id, {_MEM_KEY: _empty_state()})

    def set_state(self, user_id: str, flow: str, context: Optional[Dict[str, Any]] = None) -> None:
        """Set the current flow + fresh started_at. Stack unchanged."""
        if flow not in KNOWN_FLOWS:
            logger.warning(f"[FLOW_V2] set_state unknown flow={flow!r} — refusing")
            return
        existing = self.get_state(user_id)
        new = {
            "flow":       flow,
            "context":    dict(context or {}),
            "started_at": _now_iso() if flow != FLOW_IDLE else None,
            "stack":      existing.get("stack") or [],
        }
        self._mem.update_user_memory(user_id, {_MEM_KEY: new})
        logger.info(f"[FLOW_V2] {user_id} set_state → {flow} ctx_keys={list(new['context'].keys())}")

    def update_context(self, user_id: str, patch: Dict[str, Any]) -> None:
        """Merge keys into the current flow's context. No flow/timestamp change."""
        state = self.get_state(user_id)
        state["context"] = {**(state["context"] or {}), **patch}
        self._mem.update_user_memory(user_id, {_MEM_KEY: state})

    def push(self, user_id: str, new_flow: str, new_context: Optional[Dict[str, Any]] = None) -> bool:
        """Push the current flow onto the stack and switch to new_flow.
        Returns False if the stack would exceed MAX_STACK_DEPTH (caller should
        treat this as 'collapse to new flow only' or surface a UX message)."""
        if new_flow not in KNOWN_FLOWS:
            logger.warning(f"[FLOW_V2] push unknown flow={new_flow!r} — refusing")
            return False
        state = self.get_state(user_id)
        if len(state.get("stack") or []) >= MAX_STACK_DEPTH:
            logger.info(f"[FLOW_V2] {user_id} push refused — stack full ({MAX_STACK_DEPTH})")
            return False
        frame = {
            "flow":       state["flow"],
            "context":    state["context"] or {},
            "started_at": state.get("started_at"),
        }
        stacked = (state.get("stack") or []) + [frame]
        new_state = {
            "flow":       new_flow,
            "context":    dict(new_context or {}),
            "started_at": _now_iso(),
            "stack":      stacked,
        }
        self._mem.update_user_memory(user_id, {_MEM_KEY: new_state})
        logger.info(f"[FLOW_V2] {user_id} pushed {frame['flow']} → entering {new_flow}")
        return True

    def pop(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Pop the top frame off the stack and restore it as the active flow.
        Returns the restored frame (with .flow / .context), or None if stack empty."""
        state = self.get_state(user_id)
        stack = state.get("stack") or []
        if not stack:
            return None
        top = stack[-1]
        rest = stack[:-1]
        restored = {
            "flow":       top.get("flow") or FLOW_IDLE,
            "context":    top.get("context") if isinstance(top.get("context"), dict) else {},
            "started_at": top.get("started_at") or _now_iso(),
            "stack":      rest,
        }
        self._mem.update_user_memory(user_id, {_MEM_KEY: restored})
        logger.info(f"[FLOW_V2] {user_id} popped → resumed {restored['flow']}")
        return restored
