"""
Phase 2.3 (TODO.md): "Port readers, then delete the mirror."

SMART_CAPTURE_CONFIRM_PENDING is the eleventh flow fully migrated -- and the
first whose legacy "mirror" was never a boolean awaiting_* flag but
`memory.get_form_state()` (a dict written by `memory.start_form()`).
FlowMachine's own reconciliation treated any truthy form_state as this flow
(regardless of form_type: `smart_capture_confirm` or `smart_capture_missing`
both map here), which meant FlowMachine always lagged one full turn behind
the form itself.

The fix: both fresh-entry sites now write flow_machine.set_state() directly
at the same moment they call memory.start_form() --
_show_smart_capture_confirmation (form_type=smart_capture_confirm) and
_extract_and_confirm's missing-fields branch (form_type=
smart_capture_missing). Every OTHER call to memory.start_form() in this
module just re-arms the SAME form while already inside this flow (retry
counters, invalid-email re-prompts) so needs no new flow_machine write.
With both fresh-entry sites eager, the reconciliation branch (which used to
be the only thing keeping FlowMachine in sync) is dead and was deleted --
pending_disambiguation is now the ONLY thing left with a reconciliation
branch at all (see test_flow_disambiguation.py /
test_flow_account_settings.py / test_flow_invoice_gates.py for that).

Uses a REAL FlowMachine bound to a real dict-backed FakeMemory (not mocked),
matching production wiring exactly.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime
from unittest.mock import patch, MagicMock

from services.flow_machine import (
    FlowMachine, FLOW_IDLE,
    FLOW_SMART_CAPTURE_NEED_DESCRIPTION, FLOW_SMART_CAPTURE_CONFIRM_PENDING,
)
from services.flow_dispatcher import dispatch_in_flow
from services.flows import get_flow


class FakeMemory:
    def __init__(self):
        self._store = {}
        self._forms = {}

    def get_user_memory(self, uid):
        return dict(self._store.get(uid, {}))

    def update_user_memory(self, uid, patch):
        self._store.setdefault(uid, {}).update(patch)

    def get_form_state(self, uid):
        form = self._forms.get(uid)
        return dict(form) if form else None

    def start_form(self, uid, fields, form_override=None):
        if form_override:
            form_override = dict(form_override)
            form_override["active"] = True
            form_override.setdefault("created_at", datetime.now().isoformat())
            form_override["retry_count"] = 0
            self._forms[uid] = form_override
        else:
            self._forms[uid] = {
                "active": True, "fields": fields, "step": 0, "values": {},
                "created_at": datetime.now().isoformat(),
            }

    def cancel_form(self, uid):
        self._forms.pop(uid, None)

    def get_conversation_history(self, uid):
        return []

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
    svc.gemini.is_new_query_not_response.return_value = False
    return svc


VALUES = {
    "brand_name": "Bridgestone", "job_date": "2026-02-10",
    "job_description_details": "Master film 30 sec", "client_name": "The Good Take",
    "fees": 25000,
}


class TestArmSiteShowConfirmation:
    """_show_smart_capture_confirmation is reached from three different
    callers (_start_smart_capture inline bypass, _extract_and_confirm's
    all-required-fields-present path, and the deterministic-query INSERT
    confirmation route) -- all now sync FlowMachine eagerly."""

    def test_writes_flow_machine_state_directly(self):
        svc = _svc()
        result = svc._show_smart_capture_confirmation("u1", dict(VALUES))
        assert result["operation"] == "smart_capture_confirm"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_CONFIRM_PENDING

    def test_form_state_matches(self):
        svc = _svc()
        svc._show_smart_capture_confirmation("u1", dict(VALUES))
        assert svc.memory.get_form_state("u1")["form_type"] == "smart_capture_confirm"


class TestArmSiteMissingFields:
    """_extract_and_confirm's missing-fields branch is the OTHER
    fresh-entry site -- required fields absent, form_type=
    smart_capture_missing, same target flow."""

    def test_writes_flow_machine_state_directly(self):
        svc = _svc()
        svc.gemini.extract_job_fields.return_value = {"brand_name": "Bridgestone"}
        result = svc._extract_and_confirm("u1", "Bridgestone")
        assert result["operation"] == "smart_capture_missing"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_CONFIRM_PENDING

    def test_form_state_matches(self):
        svc = _svc()
        svc.gemini.extract_job_fields.return_value = {"brand_name": "Bridgestone"}
        svc._extract_and_confirm("u1", "Bridgestone")
        assert svc.memory.get_form_state("u1")["form_type"] == "smart_capture_missing"


class TestReArmSitesStayInFlow:
    """Every other memory.start_form() call in the module (invalid-email
    re-prompt, unrecognised-reply retry counter, still-missing-fields
    re-prompt) re-arms the SAME form while FlowMachine is already tracking
    this flow -- no fresh transition needed, and none was added."""

    def test_invalid_email_reply_during_confirm_stays_in_flow(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_CONFIRM_PENDING, {})
        svc.memory.start_form("u1", [], form_override={
            "form_type": "smart_capture_confirm",
            "values": {**VALUES, "poc_name": None, "poc_email": None},
        })
        svc.gemini.extract_job_fields.return_value = {}
        result = svc._handle_smart_capture_confirm("u1", "notanemail@", svc.memory.get_form_state("u1"))
        assert result["operation"] == "smart_capture_invalid_email"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_CONFIRM_PENDING

    def test_unrecognised_reply_stays_in_flow(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_CONFIRM_PENDING, {})
        svc.memory.start_form("u1", [], form_override={
            "form_type": "smart_capture_confirm", "values": dict(VALUES), "retry_count": 0,
        })
        result = svc._handle_smart_capture_confirm("u1", "huh what", svc.memory.get_form_state("u1"))
        assert result["operation"] == "smart_capture_confirm_retry"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_CONFIRM_PENDING

    def test_still_missing_fields_stays_in_flow(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_CONFIRM_PENDING, {})
        svc.memory.start_form("u1", [], form_override={
            "form_type": "smart_capture_missing",
            "values": {"brand_name": "Bridgestone"},
            "missing_fields": ["job_description_details"],
        })
        svc.gemini.extract_job_fields.return_value = None
        result = svc._handle_smart_capture_missing(
            "u1", "still not sure", svc.memory.get_form_state("u1"),
        )
        assert result["operation"] == "smart_capture_missing_retry"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_CONFIRM_PENDING


class TestDispatchInFlowIntegration:
    def test_yes_saves_job_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_CONFIRM_PENDING, {})
        svc.memory.start_form("u1", [], form_override={
            "form_type": "smart_capture_confirm", "values": dict(VALUES),
        })
        svc.supabase.insert_job_entry.return_value = {"ok": True}

        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "yes", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_CONFIRM_PENDING, current_context={},
            conversation_history=[],
        )
        assert result["operation"] == "form_complete"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_no_cancels_form_and_resets(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_CONFIRM_PENDING, {})
        svc.memory.start_form("u1", [], form_override={
            "form_type": "smart_capture_confirm", "values": dict(VALUES),
        })

        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "no", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_CONFIRM_PENDING, current_context={},
            conversation_history=[],
        )
        assert result["operation"] == "smart_capture_cancelled"
        svc.supabase.insert_job_entry.assert_not_called()
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_side_question_never_reaches_handle_response(self):
        from services.flows import SmartCaptureConfirmPending
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_CONFIRM_PENDING, {})
        with patch.object(SmartCaptureConfirmPending, "handle_response") as mock_handle:
            verdict = {"intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
                       "raw_message": "does that include tax?",
                       "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False}
            result = dispatch_in_flow(
                verdict, intent_service=svc, user_id="u1",
                current_flow=FLOW_SMART_CAPTURE_CONFIRM_PENDING, current_context={},
                conversation_history=[],
            )
        mock_handle.assert_not_called()
        assert result is None


class TestReconciliation:
    def test_no_reconciliation_branch_for_this_flow(self):
        """Phase 2.3: form_state's reconciliation branch is gone -- a stray
        form_state with FlowMachine IDLE is no longer synced by reconcile
        (both fresh-entry arm sites keep them in lockstep already)."""
        svc = _svc()
        svc.memory.start_form("u1", [], form_override={
            "form_type": "smart_capture_confirm", "values": dict(VALUES),
        })
        svc._reconcile_legacy_to_flow_machine("u1", svc.memory.get_user_memory("u1"))
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE


class TestResumeNudge:
    def test_confirm_pending_nudge_non_empty(self):
        flow = get_flow(FLOW_SMART_CAPTURE_CONFIRM_PENDING)
        assert flow.resume_nudge({})


class TestClassifierGuidance:
    def test_guidance_present(self):
        from services.classifier import _flow_compat_block
        block = _flow_compat_block("SMART_CAPTURE_CONFIRM_PENDING", {})
        assert "SIDE_QUESTION" in block and "FLOW_RESPONSE" in block and "CANCEL" in block
