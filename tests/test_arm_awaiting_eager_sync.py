"""
Phase 2.2 (TODO.md): "Make FlowMachine the writer, legacy flags the mirror."

Today `_reconcile_legacy_to_flow_machine` is the ONLY thing that ever
populates FlowMachine, and it only runs once, at the top of the NEXT
incoming message — so for the rest of the turn an arm happens in, FlowMachine
is stale by construction. `_arm_awaiting` / `_arm_disambiguation` are the
existing choke points (all ~25 call sites funnel through them) — this makes
both call `_sync_flow_machine_now` right after writing the legacy patch, so
FlowMachine reflects the just-armed flow in THE SAME TURN instead of lagging
a full message behind.

Reuses `_reconcile_legacy_to_flow_machine`'s exact mapping (no duplicated
per-flow logic) — just runs it immediately instead of waiting. Gated on
`_flow_machine_v2_enabled_for` so v2-off users see zero behavior or
performance change (no extra memory round-trip, no flow_machine writes).
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import MagicMock, patch

from services.flow_machine import FLOW_INVOICE_NEED_BILLING, FLOW_IDLE, FLOW_DISAMBIGUATION


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
    # _reconcile_legacy_to_flow_machine no-ops unless FlowMachine is IDLE
    # (its own "don't clobber an active flow" guard) — reset() is mocked
    # too, so it won't actually flip current_flow() back to IDLE for us;
    # the test has to say so explicitly, same as a real reset() would.
    svc.flow_machine.current_flow.return_value = FLOW_IDLE
    # reconcile() checks form_state FIRST (highest precedence, "deepest"
    # state) — a bare MagicMock() is truthy, which would silently win over
    # every awaiting_* flag in these tests. None = no active form.
    svc.memory.get_form_state.return_value = None
    return svc


class TestEagerSyncWhenV2Enabled:
    """With FLOW_MACHINE_V2 on, arming a flag syncs FlowMachine immediately."""

    def test_arm_awaiting_resets_then_reconciles(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"awaiting_client_billing": True}

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            svc._arm_awaiting("u1", "awaiting_client_billing")

        svc.flow_machine.reset.assert_called_once_with("u1")
        # Reconciliation reads the FRESH memory (post-patch) and sets state.
        svc.memory.get_user_memory.assert_called_with("u1")
        svc.flow_machine.set_state.assert_called_once()
        args = svc.flow_machine.set_state.call_args.args
        assert args[0] == "u1"
        assert args[1] == FLOW_INVOICE_NEED_BILLING

    def test_arm_disambiguation_resets_then_reconciles(self):
        svc = _make_svc()
        pending = {"rows": [{"id": "a"}, {"id": "b"}], "type": "delete"}
        svc.memory.get_user_memory.return_value = {"pending_disambiguation": pending}

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            svc._arm_disambiguation("u1", pending)

        svc.flow_machine.reset.assert_called_once_with("u1")
        svc.flow_machine.set_state.assert_called_once()
        args = svc.flow_machine.set_state.call_args.args
        assert args[1] == FLOW_DISAMBIGUATION

    def test_reset_happens_before_reconcile_sees_fresh_state(self):
        """Reset must run BEFORE reconcile reads the flag, or reconcile's own
        'no-op unless FlowMachine is IDLE' guard would block the sync when
        FlowMachine was already tracking a DIFFERENT, now-abandoned flow."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"awaiting_poc_name": True}
        call_order = []
        svc.flow_machine.reset.side_effect = lambda uid: call_order.append("reset")
        svc.flow_machine.set_state.side_effect = lambda *a, **k: call_order.append("set_state")

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            svc._arm_awaiting("u1", "awaiting_poc_name")

        assert call_order == ["reset", "set_state"]


class TestNoSyncWhenV2Disabled:
    """With FLOW_MACHINE_V2 off, arming a flag must not touch FlowMachine at all."""

    def test_arm_awaiting_skips_flow_machine_when_v2_off(self):
        svc = _make_svc()

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False):
            svc._arm_awaiting("u1", "awaiting_client_billing")

        svc.flow_machine.reset.assert_not_called()
        svc.flow_machine.set_state.assert_not_called()
        # No extra memory read beyond the write already performed.
        svc.memory.get_user_memory.assert_not_called()

    def test_arm_disambiguation_skips_flow_machine_when_v2_off(self):
        svc = _make_svc()

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False):
            svc._arm_disambiguation("u1", {"rows": [], "type": "delete"})

        svc.flow_machine.reset.assert_not_called()
        svc.memory.get_user_memory.assert_not_called()

    def test_legacy_patch_still_written_when_v2_off(self):
        """The core mutual-exclusivity fix (legacy flag patch) is completely
        unaffected by v2 being on or off."""
        svc = _make_svc()

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False):
            svc._arm_awaiting("u1", "awaiting_client_billing")

        svc.memory.update_user_memory.assert_called_once()
        patch_arg = svc.memory.update_user_memory.call_args.args[1]
        assert patch_arg["awaiting_client_billing"] is True
        assert patch_arg["awaiting_poc_name"] is False


class TestEagerSyncExceptionSafety:
    """A sync failure must never break the arm that triggered it."""

    def test_flow_machine_reset_exception_does_not_propagate(self):
        svc = _make_svc()
        svc.flow_machine.reset.side_effect = Exception("DB write failed")

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            # Must not raise.
            svc._arm_awaiting("u1", "awaiting_client_billing")

        # The legacy patch (the actual fix this helper exists for) still landed.
        svc.memory.update_user_memory.assert_called_once()

    def test_memory_read_exception_does_not_propagate(self):
        svc = _make_svc()
        svc.memory.get_user_memory.side_effect = Exception("connection lost")

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            svc._arm_disambiguation("u1", {"rows": [], "type": "delete"})

        # update_user_memory (the write) was still called before the read failed.
        svc.memory.update_user_memory.assert_called_once()


class TestSyncFlowMachineNowDirectly:
    """_sync_flow_machine_now in isolation."""

    def test_noop_when_v2_disabled(self):
        svc = _make_svc()
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False):
            svc._sync_flow_machine_now("u1")
        svc.flow_machine.reset.assert_not_called()

    def test_reconciles_using_fresh_memory_not_stale_snapshot(self):
        """Must re-fetch memory AFTER the caller's write, not reuse a snapshot
        from before the patch -- otherwise it would reconcile against
        pre-arm state and set the WRONG flow (or none at all)."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"awaiting_client_billing": True}

        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            svc._sync_flow_machine_now("u1")

        svc.memory.get_user_memory.assert_called_once_with("u1")
        svc.flow_machine.set_state.assert_called_once()
        assert svc.flow_machine.set_state.call_args.args[1] == FLOW_INVOICE_NEED_BILLING
