"""
Phase 2.3 (TODO.md): "Port readers, then delete the mirror."

NAME_CHANGE and LINK_ACCOUNT are the third and fourth flows fully migrated
off their legacy flags — FlowMachine is now their sole source of truth.
Rounds out the WP-3-slice-2 trio (alongside BANK_DETAILS).

NAME_CHANGE was the simplest migration yet: single arm site, no retry loop
(_process_name_change always resolves in one turn — invalid input just
becomes the literal new name, there's no "that's not parseable" branch).

LINK_ACCOUNT looked equally simple from its Flow class's own docstring
("always completes in one turn") but that docstring was WRONG:
_process_link_id DOES have a validation-retry path (an invalid ID re-prompts
via "link_invalid_id"). Pre-migration this "worked" only because the retry
re-armed the legacy awaiting_link_id flag and reconciliation picked it back
up on the NEXT message — FlowMachine bounced IDLE -> LINK_ACCOUNT again via
that side door. With no legacy flag left to reconcile FROM, that trick is
gone, so LinkAccount.handle_response now needs the same check-after pattern
BankDetails already has: skip the reset when the handler returns
"link_invalid_id".

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not mocked),
matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock

from services.flow_machine import FlowMachine, FLOW_IDLE, FLOW_NAME_CHANGE, FLOW_LINK_ACCOUNT
from services.flow_dispatcher import dispatch_in_flow
from services.flows import NameChange, LinkAccount, get_flow


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
    return svc


class TestNameChangeArmSite:
    def test_prompt_writes_flow_machine_state(self):
        svc = _svc()
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"name": "Old Name"},
        }
        result = svc._handle_name_change("u1", "change my name")
        assert result["operation"] == "name_change_prompt"
        assert svc.flow_machine.current_flow("u1") == FLOW_NAME_CHANGE

    def test_inline_name_bypasses_the_flow_entirely(self):
        """'change my name to Akshaj' applies immediately -- no prompt, no
        FlowMachine arm."""
        svc = _svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._handle_name_change("u1", "change my name to Akshaj")
        assert result["operation"] != "name_change_prompt"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_defensively_clears_other_legacy_flags(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"awaiting_invoice_poc_email": True})
        svc._handle_name_change("u1", "change my name")
        assert svc.memory.get_user_memory("u1")["awaiting_invoice_poc_email"] is False

    def test_no_legacy_name_change_flag_written(self):
        svc = _svc()
        svc._handle_name_change("u1", "change my name")
        assert "awaiting_name_change" not in svc.memory.get_user_memory("u1")


class TestNameChangeFlowResponse:
    def test_valid_name_applies_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_NAME_CHANGE, {})
        svc.supabase.upsert_user_profile.return_value = {"ok": True}

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "Akshaj Kumar", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_NAME_CHANGE, current_context={},
            conversation_history=[],
        )

        assert result is not None
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_cancel_word_in_response_cancels_and_resets(self):
        """No retry loop for this flow -- even a 'cancel'-shaped
        FLOW_RESPONSE (not CANCEL flow_compatible) always resets."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_NAME_CHANGE, {})

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "cancel", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_NAME_CHANGE, current_context={},
            conversation_history=[],
        )

        assert result["operation"] == "name_change_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestNameChangeCancel:
    def test_cancel_verdict_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_NAME_CHANGE, {})

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "CANCEL",
            "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_NAME_CHANGE, current_context={},
            conversation_history=[],
        )

        assert result["operation"] == "name_change_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestLinkAccountArmSite:
    def test_prompt_writes_flow_machine_state(self):
        svc = _svc()
        result = svc._handle_link_account("u1", "link my account")
        assert result["operation"] == "link_prompt"
        assert svc.flow_machine.current_flow("u1") == FLOW_LINK_ACCOUNT

    def test_inline_id_bypasses_the_flow_entirely(self):
        svc = _svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._handle_link_account("u1", "link telegram 751256859")
        assert result["operation"] == "account_linked"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_defensively_clears_other_legacy_flags(self):
        svc = _svc()
        svc.memory.update_user_memory("u1", {"awaiting_invoice_poc_email": True})
        svc._handle_link_account("u1", "link my account")
        assert svc.memory.get_user_memory("u1")["awaiting_invoice_poc_email"] is False


class TestLinkAccountFlowResponseValid:
    def test_valid_id_links_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_LINK_ACCOUNT, {})
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "751256859", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_LINK_ACCOUNT, current_context={},
            conversation_history=[],
        )

        assert result["operation"] == "account_linked"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestLinkAccountFlowResponseRetry:
    """The bug this migration had to actually fix, not just port: an
    invalid ID must keep the flow active, not silently drop to IDLE."""

    def test_invalid_id_stays_in_flow(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_LINK_ACCOUNT, {})

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "send all", "parameters": {}, "confidence": 0.7,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_LINK_ACCOUNT, current_context={},
            conversation_history=[],
        )

        assert result["operation"] == "link_invalid_id"
        # The regression this exists to prevent: FlowMachine must NOT drop
        # to IDLE on an invalid reply, or the next message (e.g. a genuine
        # retry ID) would miss dispatch_in_flow entirely.
        assert svc.flow_machine.current_flow("u1") == FLOW_LINK_ACCOUNT

    def test_second_attempt_after_invalid_id_still_works(self):
        """End-to-end: invalid ID, then a valid one, across two separate
        dispatch_in_flow calls -- proves the flow genuinely stayed active,
        not just that the return value looked right once."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_LINK_ACCOUNT, {})
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}

        bad_verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "nonsense", "parameters": {}, "confidence": 0.7,
            "historical": False, "bulk": False,
        }
        dispatch_in_flow(
            bad_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_LINK_ACCOUNT, current_context={},
            conversation_history=[],
        )
        assert svc.flow_machine.current_flow("u1") == FLOW_LINK_ACCOUNT

        good_verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "751256859", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            good_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_LINK_ACCOUNT, current_context={},
            conversation_history=[],
        )
        assert result["operation"] == "account_linked"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestLinkAccountCancel:
    def test_cancel_verdict_resets_without_linking(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_LINK_ACCOUNT, {})

        verdict = {
            "intent": "SETTINGS_COMMAND", "flow_compatible": "CANCEL",
            "raw_message": "nevermind", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_LINK_ACCOUNT, current_context={},
            conversation_history=[],
        )

        assert result["operation"] == "link_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        svc.supabase.upsert_user_profile.assert_not_called()


class TestResumeNudges:
    def test_name_change_nudge_non_empty(self):
        flow = get_flow(FLOW_NAME_CHANGE)
        assert flow.resume_nudge({})

    def test_link_account_nudge_non_empty(self):
        flow = get_flow(FLOW_LINK_ACCOUNT)
        assert flow.resume_nudge({})
