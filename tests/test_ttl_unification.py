"""
TODO.md Phase 2.4: "One TTL for everything" — a shared `is_timestamp_stale`
helper in services/flow_machine.py replaces three near-duplicate hand-rolled
`datetime.fromisoformat(...) + timedelta` staleness checks:

  1. FlowMachine's own `expire_if_stale` (the canonical flow TTL) — now
     implemented IN TERMS OF `is_timestamp_stale` instead of its own inline
     comparison.
  2. `_handle_form_step`'s smart-capture form staleness check — now
     delegates to `flow_machine.expire_if_stale()` directly (the form is
     always FlowMachine-tracked via SMART_CAPTURE_CONFIRM_PENDING), with a
     defensive fallback straight to `is_timestamp_stale` against the form's
     own `created_at` for the (should-never-happen) case where FlowMachine
     and form_state have desynced.
  3. The `last_generated_invoice` cache's own 30-minute expiry — now calls
     `is_timestamp_stale` against `cached_at` directly (this one stays a
     standalone cache, not FlowMachine-owned state, since no flow ever
     "arms" it).

Two deliberate behavior changes from unifying onto one conservative policy
(a missing/malformed timestamp is STALE, matching expire_if_stale's
pre-existing "no started_at -> reset" philosophy):
  - The smart-capture form's staleness is now IDLE-based (resets on every
    retry, since FlowMachine's started_at refreshes on every set_state())
    rather than a fixed lifetime cap from the form's original created_at.
  - The invoice cache's malformed/missing cached_at used to be silently
    treated as fresh forever; it's now treated as stale immediately.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from services.flow_machine import (
    FlowMachine, is_timestamp_stale, IDLE_TTL_MINUTES,
    FLOW_IDLE, FLOW_BANK_DETAILS,
)


class TestIsTimestampStale:
    def test_fresh_naive_timestamp_not_stale(self):
        ts = (datetime.now() - timedelta(minutes=5)).isoformat()
        assert is_timestamp_stale(ts) is False

    def test_old_naive_timestamp_is_stale(self):
        ts = (datetime.now() - timedelta(minutes=45)).isoformat()
        assert is_timestamp_stale(ts) is True

    def test_fresh_aware_timestamp_not_stale(self):
        ts = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        assert is_timestamp_stale(ts) is False

    def test_old_aware_timestamp_is_stale(self):
        ts = (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat()
        assert is_timestamp_stale(ts) is True

    def test_exactly_at_threshold_is_stale(self):
        ts = (datetime.now(timezone.utc) - timedelta(minutes=IDLE_TTL_MINUTES)).isoformat()
        assert is_timestamp_stale(ts) is True

    def test_none_is_stale(self):
        assert is_timestamp_stale(None) is True

    def test_empty_string_is_stale(self):
        assert is_timestamp_stale("") is True

    def test_malformed_is_stale(self):
        assert is_timestamp_stale("not-a-date") is True

    def test_custom_minutes(self):
        ts = (datetime.now() - timedelta(minutes=10)).isoformat()
        assert is_timestamp_stale(ts, minutes=5) is True
        assert is_timestamp_stale(ts, minutes=15) is False


class TestExpireIfStaleUsesSharedPolicy:
    def _mem(self, state=None):
        mem = MagicMock()
        mem.get_user_memory.return_value = {"flow_v2": state} if state else {}
        return mem

    def test_idle_flow_never_expires(self):
        fm = FlowMachine(self._mem())
        assert fm.expire_if_stale("u1") is False

    def test_fresh_flow_not_expired(self):
        started = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        mem = self._mem({"flow": FLOW_BANK_DETAILS, "context": {}, "started_at": started, "stack": []})
        fm = FlowMachine(mem)
        assert fm.expire_if_stale("u1") is False
        mem.update_user_memory.assert_not_called()

    def test_stale_flow_resets(self):
        started = (datetime.now(timezone.utc) - timedelta(minutes=45)).isoformat()
        mem = self._mem({"flow": FLOW_BANK_DETAILS, "context": {}, "started_at": started, "stack": []})
        fm = FlowMachine(mem)
        assert fm.expire_if_stale("u1") is True
        written = mem.update_user_memory.call_args.args[1]["flow_v2"]
        assert written["flow"] == FLOW_IDLE

    def test_missing_started_at_resets_conservatively(self):
        mem = self._mem({"flow": FLOW_BANK_DETAILS, "context": {}, "started_at": None, "stack": []})
        fm = FlowMachine(mem)
        assert fm.expire_if_stale("u1") is True


class TestFormStepDelegatesToFlowMachineTTL:
    def _svc(self):
        with patch("services.intent_service.GeminiService"), \
             patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), \
             patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.gemini = MagicMock()
        svc.email = MagicMock()
        svc.supabase = MagicMock()
        svc.memory = MagicMock()
        svc.memory.get_user_memory.return_value = {}
        # Bare MagicMock() is truthy, which would make _handle_form_step's
        # own new-intent escape hatch spuriously cancel the form.
        svc.gemini.is_new_query_not_response.return_value = False
        return svc

    def _form(self, created_at=None):
        return {
            "form_type": "smart_capture_confirm", "values": {},
            "created_at": created_at or datetime.now().isoformat(),
        }

    def test_flow_machine_stale_cancels_form(self):
        svc = self._svc()
        svc.memory.get_form_state.return_value = self._form()
        svc.flow_machine = MagicMock()
        svc.flow_machine.expire_if_stale.return_value = True
        result = svc._handle_form_step("u1", "yes")
        assert result is None
        svc.flow_machine.expire_if_stale.assert_called_once_with("u1")

    def test_flow_machine_fresh_form_proceeds(self):
        svc = self._svc()
        svc.memory.get_form_state.return_value = self._form()
        svc.flow_machine = MagicMock()
        svc.flow_machine.expire_if_stale.return_value = False
        svc.flow_machine.is_idle.return_value = False
        result = svc._handle_form_step("u1", "yes")
        assert result is not None  # falls through to the confirm handler

    def test_desync_fallback_uses_forms_own_timestamp(self):
        """FlowMachine thinks IDLE (expire_if_stale is a no-op) but a form
        is still active -- shouldn't happen in practice (both arm sites
        write FlowMachine directly), but must not leave the form
        unprotected if it ever does."""
        svc = self._svc()
        old = (datetime.now() - timedelta(minutes=45)).isoformat()
        svc.memory.get_form_state.return_value = self._form(created_at=old)
        svc.flow_machine = MagicMock()
        svc.flow_machine.expire_if_stale.return_value = False
        svc.flow_machine.is_idle.return_value = True
        result = svc._handle_form_step("u1", "yes")
        assert result is None
        svc.memory.cancel_form.assert_called_once_with("u1")

    def test_desync_fallback_fresh_form_proceeds(self):
        svc = self._svc()
        svc.memory.get_form_state.return_value = self._form()  # fresh
        svc.flow_machine = MagicMock()
        svc.flow_machine.expire_if_stale.return_value = False
        svc.flow_machine.is_idle.return_value = True
        result = svc._handle_form_step("u1", "yes")
        assert result is not None


class TestInvoiceCacheUsesSharedPolicy:
    def _svc(self):
        with patch("services.intent_service.GeminiService"), \
             patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), \
             patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.gemini = MagicMock()
        svc.email = MagicMock()
        svc.supabase = MagicMock()
        svc.memory = MagicMock()
        return svc

    def test_stale_cache_cleared(self):
        from services.flow_machine import is_timestamp_stale
        old = (datetime.now() - timedelta(minutes=45)).isoformat()
        assert is_timestamp_stale(old) is True

    def test_fresh_cache_kept(self):
        from services.flow_machine import is_timestamp_stale
        fresh = (datetime.now() - timedelta(minutes=5)).isoformat()
        assert is_timestamp_stale(fresh) is False
