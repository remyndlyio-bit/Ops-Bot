"""
MODIFY_FIELD — the "what would you like to change about X?" prompt mid-modify
(distinct from DISAMBIGUATION's row-pick, which happens BEFORE a row is
pinned), migrated straight into FlowMachine v2. The last of the four
originally `❌ Legacy-only` single-prompt gates named in FLOW_MACHINE_V2.md —
with this, all 12 originally-`✅ Mirrored + owned` flows AND all 6
originally-`❌ Legacy-only` flows are FlowMachine-owned.

Single arm site: _handle_modify_intent's own "no field/value parsed, but a
row is pinned" branch. A genuine 3-way outcome once armed: re-prompt (still
no field/value; re-arms itself via _arm_modify_field_v2), hand off to
DISAMBIGUATION (a client/bill filter supplied alongside the field/value
matched multiple rows -- _arm_disambiguation already transitioned
FlowMachine, so ModifyField.handle_response must NOT clobber it), or apply
the update and finish (success, or a parse/write failure -- both terminal).

The explicit verb-trigger entry point ("modify ...", "update ...") that
starts this flow for a BRAND NEW message stays legacy-only (v2-off
fallback only) -- when v2 is on, a fresh WRITE_UPDATE-shaped message is
shadow-only and the legacy update/query pipeline handles it directly,
never through _handle_modify_intent at all.

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not mocked),
matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import (
    FlowMachine, FLOW_IDLE, FLOW_MODIFY_FIELD, FLOW_DISAMBIGUATION,
)
from services.flow_dispatcher import dispatch_in_flow
from services.flows import ModifyField, get_flow


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
    def test_no_field_value_with_pinned_row_arms_flow_machine(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {
            "uscf_context": {"last_row_data": {"id": "row-1", "client_name": "Nike"}},
        })
        svc.gemini.extract_modify_intent.return_value = None
        result = svc._handle_modify_intent("u1", "I want to change something", svc.memory.get_user_memory("u1"))
        assert result["operation"] == "modify_prompt"
        assert svc.flow_machine.current_flow("u1") == FLOW_MODIFY_FIELD

    def test_no_legacy_flag_written(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {
            "uscf_context": {"last_row_data": {"id": "row-1"}},
        })
        svc.gemini.extract_modify_intent.return_value = None
        svc._handle_modify_intent("u1", "change something", svc.memory.get_user_memory("u1"))
        assert "awaiting_modify_field" not in svc.memory.get_user_memory("u1")

    def test_no_pinned_row_falls_through(self):
        svc = _svc()
        svc.gemini.extract_modify_intent.return_value = None
        result = svc._handle_modify_intent("u1", "change something", {})
        assert result is None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestHandlerOutcomes:
    def test_field_and_value_applies_update(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_MODIFY_FIELD, {"row_id": "row-1"})
        svc.memory.update_user_memory("u1", {"modify_row_id": "row-1"})
        svc.gemini.extract_modify_intent.return_value = {"field": "fee", "value": "30000"}
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"fees": 10000, "notes": ""}]},
            {"ok": True, "rows": [{"id": "row-1", "fees": 30000}]},
        ]
        result = svc._handle_modify_intent("u1", "fee: 30000", svc.memory.get_user_memory("u1"))
        assert result["operation"] == "modify_success"

    def test_multiple_matches_hands_off_to_disambiguation(self):
        svc = _svc()
        svc.gemini.extract_modify_intent.return_value = {
            "field": "fee", "value": "30000", "client_filter": "Nike",
        }
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [
            {"id": "a", "client_name": "Nike", "bill_no": "INV-1", "fees": 10000},
            {"id": "b", "client_name": "Nike", "bill_no": "INV-2", "fees": 20000},
        ]}
        # _arm_disambiguation only writes flow_machine when v2 is enabled
        # for the user -- not mocked by default in this test's env.
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            result = svc._handle_modify_intent("u1", "set Nike's fee to 30000", {})
        assert result["operation"] == "modify_disambiguate"
        assert svc.flow_machine.current_flow("u1") == FLOW_DISAMBIGUATION

    def test_bad_fee_value_returns_error(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_MODIFY_FIELD, {"row_id": "row-1"})
        svc.memory.update_user_memory("u1", {"modify_row_id": "row-1"})
        svc.gemini.extract_modify_intent.return_value = {"field": "fee", "value": "not a number"}
        result = svc._handle_modify_intent("u1", "fee: not a number", svc.memory.get_user_memory("u1"))
        assert result["operation"] == "modify_bad_value"


class TestFlowClassHandleResponse:
    def test_reprompt_stays_in_flow(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_MODIFY_FIELD, {"row_id": "row-1"})
        svc.memory.update_user_memory("u1", {
            "modify_row_id": "row-1",
            "uscf_context": {"last_row_data": {"id": "row-1", "client_name": "Nike"}},
        })
        svc.gemini.extract_modify_intent.return_value = None
        flow = ModifyField()
        result = flow.handle_response(svc, "u1", "hmm not sure", {"row_id": "row-1"})
        assert result["operation"] == "modify_prompt"
        assert svc.flow_machine.current_flow("u1") == FLOW_MODIFY_FIELD

    def test_success_resets_to_idle(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_MODIFY_FIELD, {"row_id": "row-1"})
        svc.memory.update_user_memory("u1", {"modify_row_id": "row-1"})
        svc.gemini.extract_modify_intent.return_value = {"field": "paid", "value": "yes"}
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"paid": "No", "notes": ""}]},
            {"ok": True, "rows": [{"id": "row-1", "paid": "Yes"}]},
        ]
        flow = ModifyField()
        result = flow.handle_response(svc, "u1", "paid: yes", {"row_id": "row-1"})
        assert result["operation"] == "modify_success"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_disambiguate_outcome_does_not_clobber_disambiguation_flow(self):
        """The regression this exists to prevent: _handle_modify_intent's
        multi-match branch already transitioned FlowMachine to
        DISAMBIGUATION via _arm_disambiguation -- ModifyField.handle_response
        must not immediately reset it back to IDLE."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_MODIFY_FIELD, {"row_id": "row-1"})
        svc.memory.update_user_memory("u1", {"modify_row_id": "row-1"})
        svc.gemini.extract_modify_intent.return_value = {
            "field": "fee", "value": "30000", "client_filter": "Nike",
        }
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [
            {"id": "a", "client_name": "Nike", "bill_no": "INV-1", "fees": 10000},
            {"id": "b", "client_name": "Nike", "bill_no": "INV-2", "fees": 20000},
        ]}
        flow = ModifyField()
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True):
            result = flow.handle_response(svc, "u1", "set Nike's fee to 30000", {"row_id": "row-1"})
        assert result["operation"] == "modify_disambiguate"
        assert svc.flow_machine.current_flow("u1") == FLOW_DISAMBIGUATION

    def test_stale_row_handled_gracefully(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_MODIFY_FIELD, {"row_id": "row-1"})
        svc.gemini.extract_modify_intent.return_value = None
        flow = ModifyField()
        result = flow.handle_response(svc, "u1", "something", {"row_id": "row-1"})
        assert result["operation"] == "modify_stale"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_on_cancel_clears_row_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_MODIFY_FIELD, {"row_id": "row-1"})
        svc.memory.update_user_memory("u1", {"modify_row_id": "row-1"})
        flow = ModifyField()
        result = flow.on_cancel(svc, "u1", "cancel", {"row_id": "row-1"})
        assert result["operation"] == "modify_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        assert svc.memory.get_user_memory("u1")["modify_row_id"] is None


class TestDispatchInFlowIntegration:
    def test_cancel_verdict_reaches_on_cancel(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_MODIFY_FIELD, {"row_id": "row-1"})
        verdict = {"intent": "WRITE_UPDATE", "flow_compatible": "CANCEL",
                   "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
                   "historical": False, "bulk": False}
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_MODIFY_FIELD, current_context={"row_id": "row-1"},
            conversation_history=[],
        )
        assert result["operation"] == "modify_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_side_question_never_reaches_handle_response(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_MODIFY_FIELD, {"row_id": "row-1"})
        with patch.object(ModifyField, "handle_response") as mock_handle:
            verdict = {"intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
                       "raw_message": "does that include everything?", "parameters": {},
                       "confidence": 0.9, "historical": False, "bulk": False}
            result = dispatch_in_flow(
                verdict, intent_service=svc, user_id="u1",
                current_flow=FLOW_MODIFY_FIELD, current_context={"row_id": "row-1"},
                conversation_history=[],
            )
        mock_handle.assert_not_called()
        assert result is None


class TestFlowMachineRegistration:
    def test_registered(self):
        from services.flow_machine import KNOWN_FLOWS
        assert FLOW_MODIFY_FIELD in KNOWN_FLOWS
        assert isinstance(get_flow(FLOW_MODIFY_FIELD), ModifyField)


class TestResumeNudge:
    def test_non_empty(self):
        flow = get_flow(FLOW_MODIFY_FIELD)
        assert flow.resume_nudge({})


class TestClassifierGuidance:
    def test_guidance_present(self):
        from services.classifier import _flow_compat_block
        block = _flow_compat_block("MODIFY_FIELD", {})
        assert "SIDE_QUESTION" in block and "FLOW_RESPONSE" in block and "CANCEL" in block


class TestReconciliation:
    def test_no_reconciliation_branch_for_this_flow(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"modify_row_id": "row-1"})
        svc._reconcile_legacy_to_flow_machine("u1", svc.memory.get_user_memory("u1"))
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_no_boolean_legacy_flags_left_at_all(self):
        from services.intent_service import _ALL_AWAITING_CLEAR_PATCH
        svc = _svc()
        assert svc._AWAITING_FLAGS == ()
        assert "awaiting_modify_field" not in _ALL_AWAITING_CLEAR_PATCH
