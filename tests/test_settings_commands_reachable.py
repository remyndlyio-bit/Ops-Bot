"""
Regression: explicit settings commands must reach their handlers when
FLOW_MACHINE_V2 is ON (the production default).

The bug this pins down (found auditing TODO.md 1.4 item 3): the whole
settings-command block in `_process_request_impl` was wrapped in
`if not _flow_machine_v2_enabled_for(user_id):` on the stated grounds that
"the classifier handles these as SETTINGS_COMMAND" — but SETTINGS_COMMAND
was never added to the classifier's intent enum or prompt. Only tests
referenced the string.

`_prompt_bank_details_format`, `_show_bank_details`, `_handle_name_change`,
`_handle_address_update` and `_handle_link_account` each had exactly ONE
caller, all inside that gated block. So with v2 on, six commands had no
route at all and fell through to the QUERY PIPELINE — "update my bank
details" was handed to the SQL planner as though it were a data question.

These tests assert reachability, not phrasing: each command must reach its
own handler rather than being swallowed by the query pipeline.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch


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


HANDLERS = (
    "_prompt_bank_details_format",
    "_show_bank_details",
    "_handle_name_change",
    "_handle_address_update",
    "_handle_link_account",
)


def _svc():
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()
    svc.memory = FakeMemory()
    svc.supabase = MagicMock()
    svc.gemini = MagicMock()
    svc.supabase.get_user_profile.return_value = {
        "ok": True, "data": {"onboarded_at": "2024-01-01", "name": "D"},
    }
    svc.gemini.is_invoice_action_request.return_value = False
    svc.gemini.is_new_query_not_response.return_value = False
    svc.supabase.execute_sql.return_value = {"ok": True, "operation": "select", "rows": []}
    # Stub each settings handler so we can see WHICH one the router reached.
    for name in HANDLERS:
        setattr(svc, name, MagicMock(return_value={
            "operation": f"hit:{name}", "response": "ok",
            "trigger_invoice": False, "invoice_data": {},
        }))
    return svc


def _run(svc, message, v2_on=True):
    """Run a message with v2 forced on/off. The classifier is stubbed to
    return None so no live LLM call happens and the legacy cascade is
    exercised deterministically."""
    with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=v2_on), \
         patch("services.classifier.classify", return_value=None):
        return svc.process_request("u1", message)


@pytest.mark.parametrize("message,handler", [
    ("update my bank details",     "_prompt_bank_details_format"),
    ("show my bank details",       "_show_bank_details"),
    ("change my name",             "_handle_name_change"),
    ("update my business address", "_handle_address_update"),
    ("link my account",            "_handle_link_account"),
])
class TestSettingsCommandsReachHandlersWithV2On:

    def test_reaches_its_handler(self, message, handler):
        svc = _svc()
        result = _run(svc, message, v2_on=True)
        assert result["operation"] == f"hit:{handler}", (
            f"{message!r} did not reach {handler} with v2 ON — "
            f"got operation={result['operation']!r}"
        )

    def test_not_swallowed_by_the_query_pipeline(self, message, handler):
        """The specific production symptom: these commands were handed to
        the SQL planner as though they were data questions."""
        svc = _svc()
        result = _run(svc, message, v2_on=True)
        assert result["operation"] != "query", (
            f"{message!r} fell through to the query pipeline with v2 ON"
        )

    def test_still_works_with_v2_off(self, message, handler):
        """The legacy path must keep working — this fix must not trade one
        flag state for the other."""
        svc = _svc()
        result = _run(svc, message, v2_on=False)
        assert result["operation"] == f"hit:{handler}"


class TestUserIdCommand:
    def test_show_user_id_reachable_with_v2_on(self):
        svc = _svc()
        result = _run(svc, "what is my id", v2_on=True)
        assert result["operation"] == "show_user_id", result["operation"]


class TestSettingsTriggersDoNotHijackOrdinaryQueries:
    """Guard against the failure mode Phase 1 exists to prevent — these
    keyword checkpoints run BEFORE the classifier, so an over-broad trigger
    silently steals a real question.

    'my id' is the risky one: it was matched by a bare substring test, so
    'what is my idea for Nike' contains 'my id' and would be hijacked.
    """

    @pytest.mark.parametrize("message", [
        "what is my idea for the Nike shoot",
        "show me my identity documents job",
    ])
    def test_my_id_substring_does_not_hijack(self, message):
        svc = _svc()
        result = _run(svc, message, v2_on=True)
        assert result["operation"] != "show_user_id", (
            f"{message!r} was hijacked by the 'my id' trigger"
        )
