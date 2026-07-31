"""
COMPOUND_RESPONSE — the "You also mentioned: X. Want me to do that now?
(Yes / No)" follow-up after a job save/insert, migrated straight into
FlowMachine v2 (no intermediate legacy-mirror phase). The last of the four
originally `❌ Legacy-only` single-prompt gates named in FLOW_MACHINE_V2.md.

TWO arm sites -- after a smart-capture job save and after a
deterministic-query INSERT -- both delegating to the same
_arm_compound_response_v2 helper. suggested_next_action itself is written
by the CALLER before arming (set earlier during compound-intent detection);
the arm helper just transitions FlowMachine and clears other legacy flags.

_handle_compound_response resets FlowMachine FIRST, not after: every branch
either recurses into process_request (the "yes" case, and the "anything
else" fallthrough case) or is terminal (the "no" case) -- leaving
COMPOUND_RESPONSE active into a recursive process_request call would make
dispatch_in_flow try to route it through this same flow again.

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not mocked),
matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import FlowMachine, FLOW_IDLE, FLOW_COMPOUND_RESPONSE
from services.flow_dispatcher import dispatch_in_flow
from services.flows import CompoundResponse, get_flow


class FakeMemory:
    def __init__(self):
        self._store = {}

    def get_user_memory(self, uid):
        return dict(self._store.get(uid, {}))

    def update_user_memory(self, uid, patch):
        self._store.setdefault(uid, {}).update(patch)

    def get_form_state(self, uid):
        return None

    def get_conversation_history(self, uid):
        return []

    def cancel_form(self, uid):
        pass

    def add_message(self, uid, role, content):
        pass


def _svc():
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()
    svc.memory = FakeMemory()
    svc.flow_machine = FlowMachine(svc.memory)
    svc.gemini = MagicMock()
    svc.supabase = MagicMock()
    svc.email = MagicMock()
    return svc


class TestArmSite:
    def test_writes_flow_machine_state_directly(self):
        svc = _svc()
        svc._arm_compound_response_v2("u1", "send the invoice")
        assert svc.flow_machine.current_flow("u1") == FLOW_COMPOUND_RESPONSE
        assert svc.flow_machine.get_state("u1")["context"]["suggested_next_action"] == "send the invoice"

    def test_no_legacy_flag_written(self):
        svc = _svc()
        svc._arm_compound_response_v2("u1", "send the invoice")
        assert "awaiting_compound_response" not in svc.memory.get_user_memory("u1")

    def test_defensively_clears_other_legacy_flags(self):
        # _AWAITING_FLAGS is empty now -- awaiting_modify_field (the last
        # boolean legacy flag) was migrated to its own FlowMachine flow.
        # Just confirms arming doesn't crash with nothing to iterate.
        svc = _svc()
        svc._arm_compound_response_v2("u1", "send the invoice")
        assert svc.flow_machine.current_flow("u1") == FLOW_COMPOUND_RESPONSE


class TestHandlerDirectly:
    def test_yes_recurses_with_merged_action(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        svc.memory.update_user_memory("u1", {"suggested_next_action": "send the invoice"})
        svc.process_request = MagicMock(return_value={"operation": "resumed"})
        result = svc._handle_compound_response("u1", "yes")
        assert result["operation"] == "resumed"
        svc.process_request.assert_called_once_with(user_id="u1", message="send the invoice")
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_yes_with_qualifier_merges_remainder(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        svc.memory.update_user_memory("u1", {"suggested_next_action": "send the invoice"})
        svc.process_request = MagicMock(return_value={"operation": "resumed"})
        svc._handle_compound_response("u1", "yes, include bill numbers")
        merged = svc.process_request.call_args.kwargs["message"]
        assert merged == "send the invoice include bill numbers"

    def test_no_declines_cleanly(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        svc.memory.update_user_memory("u1", {"suggested_next_action": "send the invoice"})
        result = svc._handle_compound_response("u1", "no")
        assert result["operation"] == "compound_declined"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        assert svc.memory.get_user_memory("u1")["suggested_next_action"] is None

    def test_unrelated_reply_falls_through_as_new_message(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        svc.memory.update_user_memory("u1", {"suggested_next_action": "send the invoice"})
        svc.process_request = MagicMock(return_value={"operation": "query"})
        result = svc._handle_compound_response("u1", "what did I do last week")
        assert result["operation"] == "query"
        svc.process_request.assert_called_once_with(user_id="u1", message="what did I do last week")
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_flow_machine_reset_before_recursing(self):
        """The regression this exists to prevent: if FlowMachine were still
        COMPOUND_RESPONSE when process_request is called recursively,
        dispatch_in_flow would try to route the recursive call through this
        same flow again."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        svc.memory.update_user_memory("u1", {"suggested_next_action": "send the invoice"})

        seen_flow_during_recursion = []
        def _fake_process_request(user_id, message):
            seen_flow_during_recursion.append(svc.flow_machine.current_flow(user_id))
            return {"operation": "resumed"}
        svc.process_request = _fake_process_request

        svc._handle_compound_response("u1", "yes")
        assert seen_flow_during_recursion == [FLOW_IDLE]


class TestFlowClass:
    def test_handle_response_delegates(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        svc.memory.update_user_memory("u1", {"suggested_next_action": "send the invoice"})
        flow = CompoundResponse()
        result = flow.handle_response(svc, "u1", "no", {"suggested_next_action": "send the invoice"})
        assert result["operation"] == "compound_declined"

    def test_on_cancel_declines_without_delegating(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        svc.memory.update_user_memory("u1", {"suggested_next_action": "send the invoice"})
        flow = CompoundResponse()
        result = flow.on_cancel(svc, "u1", "nevermind", {"suggested_next_action": "send the invoice"})
        assert result["operation"] == "compound_declined"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        assert svc.memory.get_user_memory("u1")["suggested_next_action"] is None


class TestDispatchInFlowIntegration:
    def test_flow_response_yes_reaches_handle_response(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        svc.memory.update_user_memory("u1", {"suggested_next_action": "send the invoice"})
        svc.process_request = MagicMock(return_value={"operation": "resumed"})

        verdict = {"intent": "WRITE_INVOICE", "flow_compatible": "FLOW_RESPONSE",
                   "raw_message": "yes", "parameters": {}, "confidence": 0.9,
                   "historical": False, "bulk": False}
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_COMPOUND_RESPONSE, current_context={"suggested_next_action": "send the invoice"},
            conversation_history=[],
        )
        assert result["operation"] == "resumed"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_cancel_verdict_reaches_on_cancel(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        verdict = {"intent": "UNKNOWN", "flow_compatible": "CANCEL",
                   "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
                   "historical": False, "bulk": False}
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_COMPOUND_RESPONSE, current_context={"suggested_next_action": "send the invoice"},
            conversation_history=[],
        )
        assert result["operation"] == "compound_declined"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_side_question_never_reaches_handle_response(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_COMPOUND_RESPONSE, {"suggested_next_action": "send the invoice"})
        with patch.object(CompoundResponse, "handle_response") as mock_handle:
            verdict = {"intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
                       "raw_message": "does that include everything?", "parameters": {},
                       "confidence": 0.9, "historical": False, "bulk": False}
            result = dispatch_in_flow(
                verdict, intent_service=svc, user_id="u1",
                current_flow=FLOW_COMPOUND_RESPONSE, current_context={"suggested_next_action": "send the invoice"},
                conversation_history=[],
            )
        mock_handle.assert_not_called()
        assert result is None


class TestFlowMachineRegistration:
    def test_registered(self):
        from services.flow_machine import KNOWN_FLOWS
        assert FLOW_COMPOUND_RESPONSE in KNOWN_FLOWS
        assert isinstance(get_flow(FLOW_COMPOUND_RESPONSE), CompoundResponse)


class TestResumeNudge:
    def test_mentions_action_when_present(self):
        flow = get_flow(FLOW_COMPOUND_RESPONSE)
        assert "send the invoice" in flow.resume_nudge({"suggested_next_action": "send the invoice"})

    def test_safe_with_no_action(self):
        flow = get_flow(FLOW_COMPOUND_RESPONSE)
        assert flow.resume_nudge({})


class TestClassifierGuidance:
    def test_guidance_present(self):
        from services.classifier import _flow_compat_block
        block = _flow_compat_block("COMPOUND_RESPONSE", {})
        assert "SIDE_QUESTION" not in block or "FLOW_RESPONSE" in block
        assert "FLOW_RESPONSE" in block and "CANCEL" in block


class TestReconciliation:
    def test_no_reconciliation_branch_for_this_flow(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"suggested_next_action": "send the invoice"})
        svc._reconcile_legacy_to_flow_machine("u1", svc.memory.get_user_memory("u1"))
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_no_legacy_flag_anywhere(self):
        from services.intent_service import _ALL_AWAITING_CLEAR_PATCH
        svc = _svc()
        assert "awaiting_compound_response" not in svc._AWAITING_FLAGS
        assert "awaiting_compound_response" not in _ALL_AWAITING_CLEAR_PATCH
