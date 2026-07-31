"""
Phase 2.3 (TODO.md): "Port readers, then delete the mirror."

SMART_CAPTURE_NEED_DESCRIPTION is the tenth flow fully migrated off its
legacy flag (awaiting_job_input) -- FlowMachine is now its sole source of
truth. Meaningfully more complex than the prior nine migrations:

- THREE arm sites instead of one: _start_smart_capture (entry point, "add a
  job" with no inline content), _extract_and_confirm's "nothing extracted"
  retry branch, and _handle_smart_capture_confirm's "Edit" branch
  (transitioning BACK from SMART_CAPTURE_CONFIRM_PENDING).
- A genuine 3-way transition in SmartCaptureNeedDescription.handle_response
  (stay in flow / advance to confirm-pending / reset to idle) instead of a
  simple retry-vs-done binary.
- A real bug, found and fixed as part of this migration, in a DIFFERENT flow
  class: SmartCaptureConfirmPending.handle_response's blanket
  "if not form_state: reset()" would have clobbered the Edit transition back
  to SMART_CAPTURE_NEED_DESCRIPTION (form_state IS gone right after
  cancel_form()), the same class of bug LINK_ACCOUNT had. Pre-migration this
  "worked" only via the legacy-flag-reconciliation-bounce trick, which
  disappears once the legacy flag write is deleted -- see
  test_edit_transitions_back_to_need_description below for the two-call
  end-to-end proof, mirroring LinkAccountFlowResponseRetry's precedent.
- The legacy dispatch block's is_new_query_not_response escape-hatch LLM
  call was deleted outright (not ported): the v2 classifier's per-flow
  guidance for this flow already makes the identical "question-shaped ->
  don't treat as job input" distinction, and dispatch_in_flow's
  SIDE_QUESTION handling (reached earlier in the cascade) routes those
  correctly with a resume_nudge.

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
from services.flows import SmartCaptureNeedDescription, SmartCaptureConfirmPending, get_flow


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
    # Default to "not a new query" so _handle_form_step's escape hatch
    # doesn't spuriously cancel forms in tests not exercising that fallback
    # (a bare MagicMock() is truthy).
    svc.gemini.is_new_query_not_response.return_value = False
    return svc


VALUES = {
    "brand_name": "Bridgestone", "job_date": "2026-02-10",
    "job_description_details": "Master film 30 sec", "client_name": "The Good Take",
    "fees": 25000,
}


class TestArmSiteStartSmartCapture:
    """_start_smart_capture is the flow's entry point -- "add a job" with no
    inline content arms the flow and prompts for a description."""

    def test_no_content_arms_flow_machine(self):
        svc = _svc()
        result = svc._start_smart_capture("u1", "add a job")
        assert result["operation"] == "smart_capture_prompt"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_NEED_DESCRIPTION

    def test_no_legacy_flag_written(self):
        svc = _svc()
        svc._start_smart_capture("u1", "add a job")
        assert "awaiting_job_input" not in svc.memory.get_user_memory("u1")

    def test_inline_content_bypasses_the_flow_entirely(self):
        """_show_smart_capture_confirmation itself never touches FlowMachine
        -- it's only synced to SMART_CAPTURE_CONFIRM_PENDING via
        SmartCaptureNeedDescription.handle_response (the dispatch_in_flow
        path) or via _reconcile_legacy_to_flow_machine's form_state branch
        on the NEXT message. Calling _start_smart_capture directly (as this
        legacy cascade entry point does) leaves FlowMachine untouched --
        the form_state write is what carries the state forward."""
        svc = _svc()
        svc.gemini.extract_job_fields.return_value = dict(VALUES)
        result = svc._start_smart_capture(
            "u1", "add a job: Bridgestone, 10 Feb, master film, The Good Take, 25k",
        )
        assert result["operation"] == "smart_capture_confirm"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        assert svc.memory.get_form_state("u1")["form_type"] == "smart_capture_confirm"


class TestArmSiteExtractAndConfirmRetry:
    """_extract_and_confirm's "nothing extracted" branch re-prompts and
    re-arms -- the user expressed intent but gave no usable data."""

    def test_empty_extraction_arms_flow_machine(self):
        svc = _svc()
        svc.gemini.extract_job_fields.return_value = None
        result = svc._extract_and_confirm("u1", "uh add a job I guess")
        assert result["operation"] == "smart_capture_prompt"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_NEED_DESCRIPTION

    def test_all_null_extraction_treated_as_empty(self):
        svc = _svc()
        svc.gemini.extract_job_fields.return_value = {
            "brand_name": None, "fees": None, "client_name": None,
        }
        result = svc._extract_and_confirm("u1", "something vague")
        assert result["operation"] == "smart_capture_prompt"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_NEED_DESCRIPTION


class TestSmartCaptureNeedDescriptionFlowResponse:
    """The 3-way transition: stay in flow (retry) / advance to confirm
    pending (all fields present) / reset to idle (shouldn't normally
    happen, but the fallback path must not crash)."""

    def test_successful_extraction_advances_to_confirm_pending(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_NEED_DESCRIPTION, {})
        svc.gemini.extract_job_fields.return_value = dict(VALUES)

        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "Bridgestone, 10 Feb, master film, The Good Take, 25k",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_NEED_DESCRIPTION, current_context={},
            conversation_history=[],
        )

        assert result["operation"] == "smart_capture_confirm"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_CONFIRM_PENDING

    def test_empty_extraction_stays_in_flow(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_NEED_DESCRIPTION, {})
        svc.gemini.extract_job_fields.return_value = None

        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "uh not sure", "parameters": {}, "confidence": 0.7,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_NEED_DESCRIPTION, current_context={},
            conversation_history=[],
        )

        assert result["operation"] == "smart_capture_prompt"
        # Must NOT drop to IDLE -- the next message (a genuine retry) would
        # miss dispatch_in_flow entirely otherwise.
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_NEED_DESCRIPTION

    def test_second_attempt_after_empty_extraction_still_works(self):
        """End-to-end: empty extraction, then a real description, across two
        separate dispatch_in_flow calls -- proves the flow genuinely stayed
        active, not just that the return value looked right once."""
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_NEED_DESCRIPTION, {})
        svc.gemini.extract_job_fields.return_value = None

        bad_verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "hmm", "parameters": {}, "confidence": 0.7,
            "historical": False, "bulk": False,
        }
        dispatch_in_flow(
            bad_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_NEED_DESCRIPTION, current_context={},
            conversation_history=[],
        )
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_NEED_DESCRIPTION

        svc.gemini.extract_job_fields.return_value = dict(VALUES)
        good_verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "Bridgestone, 10 Feb, master film, The Good Take, 25k",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            good_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_NEED_DESCRIPTION, current_context={},
            conversation_history=[],
        )
        assert result["operation"] == "smart_capture_confirm"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_CONFIRM_PENDING


class TestSmartCaptureNeedDescriptionCancel:
    def test_cancel_verdict_resets_without_saving(self):
        svc = _svc()
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_NEED_DESCRIPTION, {})

        verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "CANCEL",
            "raw_message": "cancel", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_NEED_DESCRIPTION, current_context={},
            conversation_history=[],
        )

        assert result["operation"] == "smart_capture_cancelled"
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE
        svc.gemini.extract_job_fields.assert_not_called()


class TestEditTransitionBackToNeedDescription:
    """The bug this migration had to actually fix, not just port: clicking
    "Edit" on the confirm card cancels the form AND must transition
    FlowMachine back to SMART_CAPTURE_NEED_DESCRIPTION -- without
    SmartCaptureConfirmPending's own "if not form_state: reset()" guard
    clobbering that transition back to IDLE."""

    def test_edit_reply_transitions_to_need_description_not_idle(self):
        svc = _svc()
        svc.memory.start_form("u1", [], form_override={
            "form_type": "smart_capture_confirm", "values": dict(VALUES),
            "missing_fields": [], "retry_count": 0,
        })
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_CONFIRM_PENDING, {"source": "smart_capture"})

        verdict = {
            "intent": "WRITE_UPDATE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "edit", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_CONFIRM_PENDING, current_context={},
            conversation_history=[],
        )

        assert result["operation"] == "smart_capture_edit"
        svc.supabase.insert_job_entry.assert_not_called()
        # The regression this exists to prevent: without the check-after
        # fix, SmartCaptureConfirmPending's blanket reset would have sent
        # this to IDLE (form_state IS gone right after cancel_form()).
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_NEED_DESCRIPTION

    def test_second_message_after_edit_reaches_need_description_flow(self):
        """End-to-end: Edit, then a real description, across two separate
        dispatch_in_flow calls -- proves the transition genuinely landed in
        SMART_CAPTURE_NEED_DESCRIPTION (not just that the first call's
        return value looked right)."""
        svc = _svc()
        svc.memory.start_form("u1", [], form_override={
            "form_type": "smart_capture_confirm", "values": dict(VALUES),
            "missing_fields": [], "retry_count": 0,
        })
        svc.flow_machine.set_state("u1", FLOW_SMART_CAPTURE_CONFIRM_PENDING, {"source": "smart_capture"})

        edit_verdict = {
            "intent": "WRITE_UPDATE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "edit", "parameters": {}, "confidence": 0.9,
            "historical": False, "bulk": False,
        }
        dispatch_in_flow(
            edit_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_CONFIRM_PENDING, current_context={},
            conversation_history=[],
        )
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_NEED_DESCRIPTION

        svc.gemini.extract_job_fields.return_value = dict(VALUES)
        followup_verdict = {
            "intent": "WRITE_CREATE", "flow_compatible": "FLOW_RESPONSE",
            "raw_message": "Bridgestone, 10 Feb, master film, The Good Take, 25k",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            followup_verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_NEED_DESCRIPTION, current_context={},
            conversation_history=[],
        )
        assert result["operation"] == "smart_capture_confirm"
        assert svc.flow_machine.current_flow("u1") == FLOW_SMART_CAPTURE_CONFIRM_PENDING


class TestReconciliation:
    def test_no_reconciliation_branch_for_this_flow(self):
        """Phase 2.3: SMART_CAPTURE_NEED_DESCRIPTION has no reconciliation
        branch -- all its arm sites write flow_machine.set_state() directly,
        so there's no legacy flag left to reconcile FROM."""
        svc = _svc()
        svc.memory.update_user_memory("u1", {"awaiting_job_input": True})
        svc._reconcile_legacy_to_flow_machine("u1", svc.memory.get_user_memory("u1"))
        assert svc.flow_machine.current_flow("u1") == FLOW_IDLE

    def test_no_legacy_flag_in_awaiting_flags_or_clear_patch(self):
        from services.intent_service import _ALL_AWAITING_CLEAR_PATCH
        svc = _svc()
        assert "awaiting_job_input" not in svc._AWAITING_FLAGS
        assert "awaiting_job_input" not in _ALL_AWAITING_CLEAR_PATCH


class TestResumeNudge:
    def test_need_description_nudge_non_empty(self):
        flow = get_flow(FLOW_SMART_CAPTURE_NEED_DESCRIPTION)
        assert flow.resume_nudge({})

    def test_confirm_pending_nudge_non_empty(self):
        flow = get_flow(FLOW_SMART_CAPTURE_CONFIRM_PENDING)
        assert flow.resume_nudge({})


class TestClassifierGuidance:
    def test_guidance_present(self):
        from services.classifier import _flow_compat_block
        block = _flow_compat_block("SMART_CAPTURE_NEED_DESCRIPTION", {})
        assert "SIDE_QUESTION" in block and "FLOW_RESPONSE" in block and "CANCEL" in block
