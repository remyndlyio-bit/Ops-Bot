"""
Phase 2.2 (TODO.md): "Make FlowMachine the writer, legacy flags the mirror."

Today `_reconcile_legacy_to_flow_machine` was the ONLY thing that ever
populated FlowMachine, and it only ran once, at the top of the NEXT incoming
message — so for the rest of the turn an arm happened in, FlowMachine was
stale by construction. `_arm_awaiting` / `_arm_disambiguation` were the
existing choke points (all ~25 call sites funnel through them) — this made
both call `_sync_flow_machine_now` right after writing the legacy patch, so
FlowMachine reflected the just-armed flow in THE SAME TURN instead of
lagging a full message behind.

Reused `_reconcile_legacy_to_flow_machine`'s exact mapping (no duplicated
per-flow logic) — just ran it immediately instead of waiting.

Phase 2.3 update: all 12 originally-mirrored flows are now migrated off
their legacy mirror entirely -- every arm site (including
_arm_disambiguation, the last one) writes flow_machine.set_state() directly
instead of going through _sync_flow_machine_now/reconcile.
_reconcile_legacy_to_flow_machine itself now has ZERO branches left, so
_sync_flow_machine_now (still called by _arm_awaiting, for any future
boolean flag that might need this pattern again) is a complete no-op today.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import MagicMock, patch

from services.flow_machine import FLOW_IDLE, FLOW_DISAMBIGUATION


def _make_svc():
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
    svc.flow_machine = MagicMock()
    svc.flow_machine.current_flow.return_value = FLOW_IDLE
    svc.memory.get_form_state.return_value = None
    return svc


class TestArmAwaitingEagerSyncIsNowANoOp:
    """Every remaining _AWAITING_FLAGS entry (awaiting_compound_response,
    awaiting_modify_field) has no FlowMachine mapping in
    _reconcile_legacy_to_flow_machine -- the flags that used to (bank
    details, name change, link account, smart-capture description, invoice
    month, etc.) are all FlowMachine-only now and arm via their own
    _arm_*_v2() helpers instead of _arm_awaiting. So _sync_flow_machine_now
    still fires (unconditionally, when v2 is on) but reconcile finds
    nothing to sync to and set_state() is never called."""

    def test_arm_awaiting_resets_then_finds_nothing_to_reconcile(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"awaiting_modify_field": True}

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            svc._arm_awaiting("u1", "awaiting_modify_field")

        svc.flow_machine.reset.assert_called_once_with("u1")
        svc.memory.get_user_memory.assert_called_with("u1")
        svc.flow_machine.set_state.assert_not_called()

    def test_arm_awaiting_skips_flow_machine_when_v2_off(self):
        svc = _make_svc()

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False):
            svc._arm_awaiting("u1", "awaiting_job_input")

        svc.flow_machine.reset.assert_not_called()
        svc.flow_machine.set_state.assert_not_called()
        # No extra memory read beyond the write already performed.
        svc.memory.get_user_memory.assert_not_called()

    def test_legacy_patch_still_written_when_v2_off(self):
        """The core mutual-exclusivity fix (legacy flag patch) is completely
        unaffected by v2 being on or off."""
        svc = _make_svc()

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False):
            svc._arm_awaiting("u1", "awaiting_job_input")

        svc.memory.update_user_memory.assert_called_once()
        patch_arg = svc.memory.update_user_memory.call_args.args[1]
        assert patch_arg["awaiting_job_input"] is True
        assert patch_arg["awaiting_modify_field"] is False

    def test_flow_machine_reset_exception_does_not_propagate(self):
        svc = _make_svc()
        svc.flow_machine.reset.side_effect = Exception("DB write failed")

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            # Must not raise.
            svc._arm_awaiting("u1", "awaiting_job_input")

        # The legacy patch (the actual fix this helper exists for) still landed.
        svc.memory.update_user_memory.assert_called_once()


class TestSyncFlowMachineNowIsNowANoOp:
    """_sync_flow_machine_now in isolation -- with reconcile's last branch
    (disambiguation) migrated away to a direct write, this always finds
    nothing to sync now, regardless of what's in memory."""

    def test_noop_when_v2_disabled(self):
        svc = _make_svc()
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False):
            svc._sync_flow_machine_now("u1")
        svc.flow_machine.reset.assert_not_called()

    def test_finds_nothing_to_reconcile_even_with_legacy_keys_present(self):
        svc = _make_svc()
        pending = {"rows": [{"id": "a"}], "type": "delete"}
        svc.memory.get_user_memory.return_value = {"pending_disambiguation": pending}

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            svc._sync_flow_machine_now("u1")

        svc.memory.get_user_memory.assert_called_once_with("u1")
        svc.flow_machine.set_state.assert_not_called()


class TestArmDisambiguationWritesDirectly:
    """_arm_disambiguation (Phase 2.3) no longer goes through
    _sync_flow_machine_now/reconcile at all -- it writes
    flow_machine.set_state() directly. Full coverage lives in
    tests/test_flow_disambiguation.py::TestArmDisambiguation; this is a
    smoke test confirming it's independent of the (now-inert)
    _sync_flow_machine_now path exercised by the rest of this file."""

    def test_writes_flow_machine_state_directly_no_reset_call(self):
        svc = _make_svc()
        pending = {"rows": [{"id": "a"}, {"id": "b"}], "type": "delete"}

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            svc._arm_disambiguation("u1", pending)

        # Unlike _arm_awaiting's reset-then-reconcile dance, the direct
        # write needs no reset() first -- set_state() unconditionally
        # overwrites whatever flow was previously active.
        svc.flow_machine.reset.assert_not_called()
        svc.flow_machine.set_state.assert_called_once()
        args = svc.flow_machine.set_state.call_args.args
        assert args[1] == FLOW_DISAMBIGUATION
