"""
Category 2 — Data Capture: the smart-capture form state machine.

Field EXTRACTION itself ("25k" -> 25000, Hinglish parsing) is delegated
entirely to the LLM (gemini_service.extract_job_fields) and is not testable
offline — there's no Python-side parser to unit-test, unlike
invoice_generation_service._parse_fees() which IS deterministic and IS
covered in test_scenarios_from_matrix.py. What's covered here instead is the
DETERMINISTIC state machine wrapped around that extraction: the confirm/
missing-field loop, retry counting, staleness, and the escape hatches that
let a new message interrupt a stuck form — all pure Python, all previously
untested (only 2 live e2e cases existed, both requiring AI_KEY).
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from datetime import datetime, timedelta
import pytest
from unittest.mock import MagicMock, patch


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
    svc.memory.get_user_memory.return_value = {}
    svc.supabase.db_url = "postgresql://fake"
    return svc


def _form(form_type, values=None, missing_fields=None, retry_count=0, created_at=None):
    return {
        "form_type": form_type,
        "values": values or {},
        "missing_fields": missing_fields or [],
        "retry_count": retry_count,
        "created_at": created_at or datetime.now().isoformat(),
    }


class TestFormStepEscapeHatches:
    """A stuck/stale form must never trap the user — these all let a new
    message through instead of forcing it into the old form's flow."""

    def test_no_active_form_returns_none(self):
        svc = _make_svc()
        svc.memory.get_form_state.return_value = None
        assert svc._handle_form_step("u1", "hello") is None

    def test_stale_form_auto_cancelled(self):
        svc = _make_svc()
        old = (datetime.now() - timedelta(minutes=45)).isoformat()
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm", created_at=old)
        result = svc._handle_form_step("u1", "yes")
        assert result is None
        svc.memory.cancel_form.assert_called_once_with("u1")

    def test_malformed_timestamp_treated_as_fresh_not_crash(self):
        svc = _make_svc()
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm", created_at="not-a-date")
        # Must not raise, and must NOT auto-cancel just because parsing failed.
        result = svc._handle_form_step("u1", "yes")
        assert result is not None  # falls through to the confirm handler, not None

    def test_plus_prefixed_message_cancels_old_form(self):
        svc = _make_svc()
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        result = svc._handle_form_step("u1", "+ Nike, 25k, shoot")
        assert result is None
        svc.memory.cancel_form.assert_called_once_with("u1")

    @pytest.mark.parametrize("msg", ["show my jobs", "what is my total", "delete last job", "hi", "hello"])
    def test_new_intent_words_cancel_old_form(self, msg):
        svc = _make_svc()
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        result = svc._handle_form_step("u1", msg)
        assert result is None
        svc.memory.cancel_form.assert_called_once_with("u1")

    @pytest.mark.parametrize("msg", ["cancel", "stop", "nevermind", "abort", "exit"])
    def test_explicit_cancel_words_end_the_form(self, msg):
        svc = _make_svc()
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        result = svc._handle_form_step("u1", msg)
        assert result["operation"] == "form_cancelled"
        svc.memory.cancel_form.assert_called_once_with("u1")

    def test_unknown_form_type_cancelled_silently(self):
        svc = _make_svc()
        svc.memory.get_form_state.return_value = _form("some_future_form_type")
        result = svc._handle_form_step("u1", "whatever")
        assert result is None
        svc.memory.cancel_form.assert_called_once_with("u1")


class TestSmartCaptureConfirm:
    VALUES = {"brand_name": "Nike", "fees": 25000, "paid": "Yes"}

    @pytest.mark.parametrize("msg", ["yes", "y", "save", "confirm", "done", "ok", "okay", "sure"])
    def test_affirmative_replies_save_the_job(self, msg):
        svc = _make_svc()
        form = _form("smart_capture_confirm", values=dict(self.VALUES))
        svc.supabase.insert_job_entry.return_value = {"ok": True, "rows": [{"id": 1}]}
        result = svc._handle_smart_capture_confirm("u1", msg, form)
        assert result["operation"] == "form_complete"
        assert svc.supabase.insert_job_entry.call_count == 1

    @pytest.mark.parametrize("msg", ["no", "nope", "nah", "cancel", "nevermind", "nvm", "abort"])
    def test_negative_replies_cancel_without_saving(self, msg):
        svc = _make_svc()
        form = _form("smart_capture_confirm", values=dict(self.VALUES))
        result = svc._handle_smart_capture_confirm("u1", msg, form)
        assert result["operation"] == "smart_capture_cancelled"
        svc.supabase.insert_job_entry.assert_not_called()

    @pytest.mark.parametrize("msg", ["edit", "change", "modify", "fix"])
    def test_edit_replies_prompt_for_correction_without_saving(self, msg):
        svc = _make_svc()
        form = _form("smart_capture_confirm", values=dict(self.VALUES))
        result = svc._handle_smart_capture_confirm("u1", msg, form)
        assert result["operation"] == "smart_capture_edit"
        svc.supabase.insert_job_entry.assert_not_called()
        awaiting = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                    if "awaiting_job_input" in c.args[1]]
        assert awaiting and awaiting[-1]["awaiting_job_input"] is True

    def test_unrecognised_reply_reprompts_first_time(self):
        svc = _make_svc()
        form = _form("smart_capture_confirm", values=dict(self.VALUES), retry_count=0)
        result = svc._handle_smart_capture_confirm("u1", "maybe later idk", form)
        assert result["operation"] == "smart_capture_confirm_retry"
        svc.supabase.insert_job_entry.assert_not_called()

    def test_unrecognised_reply_auto_cancels_on_second_miss(self):
        svc = _make_svc()
        form = _form("smart_capture_confirm", values=dict(self.VALUES), retry_count=1)
        result = svc._handle_smart_capture_confirm("u1", "still unclear", form)
        assert result["operation"] == "form_cancelled"
        svc.memory.cancel_form.assert_called_once_with("u1")

    def test_reply_with_email_fills_missing_poc_and_reconfirms(self):
        """A reply containing '@' when POC fields are missing must be treated as
        POC info, not as an unrecognised yes/no/edit reply."""
        svc = _make_svc()
        form = _form("smart_capture_confirm", values={"brand_name": "Nike", "fees": 25000})
        svc.gemini.extract_job_fields.return_value = {"poc_email": "karan@starstudios.com"}
        result = svc._handle_smart_capture_confirm("u1", "karan@starstudios.com", form)
        # Must re-show confirmation (not save immediately, not error) with the email merged in.
        assert result["operation"] == "smart_capture_confirm"
        svc.supabase.insert_job_entry.assert_not_called()

    def test_malformed_email_attempt_asks_again_without_saving(self):
        svc = _make_svc()
        form = _form("smart_capture_confirm", values={"brand_name": "Nike", "fees": 25000})
        svc.gemini.extract_job_fields.return_value = {}
        result = svc._handle_smart_capture_confirm("u1", "karan@notreallyanemail", form)
        assert result["operation"] == "smart_capture_invalid_email"
        assert "valid email" in result["response"].lower()
        svc.supabase.insert_job_entry.assert_not_called()


class TestSmartCaptureMissing:
    def test_still_missing_fields_reprompts_with_labels(self):
        svc = _make_svc()
        form = _form("smart_capture_missing", values={"brand_name": "Nike"},
                     missing_fields=["job_date", "poc_email"])
        svc.gemini.extract_job_fields.return_value = {}
        result = svc._handle_smart_capture_missing("u1", "not sure yet", form)
        assert result["operation"] == "smart_capture_missing_retry"
        assert "Date" in result["response"] and "POC email" in result["response"]

    def test_providing_all_missing_fields_shows_confirmation(self):
        svc = _make_svc()
        form = _form("smart_capture_missing", values={"brand_name": "Nike", "fees": 25000},
                     missing_fields=["job_date"])
        svc.gemini.extract_job_fields.return_value = {"job_date": "2026-04-10"}
        result = svc._handle_smart_capture_missing("u1", "10 April", form)
        assert result["operation"] == "smart_capture_confirm"

    def test_invalid_email_in_missing_flow_reprompts(self):
        svc = _make_svc()
        form = _form("smart_capture_missing", values={"brand_name": "Nike"},
                     missing_fields=["poc_email"])
        svc.gemini.extract_job_fields.return_value = {"poc_email": "bad-email"}
        result = svc._handle_smart_capture_missing("u1", "bad-email", form)
        assert result["operation"] == "smart_capture_invalid_email"

    def test_bare_malformed_email_token_caught_even_if_gemini_misses_it(self):
        svc = _make_svc()
        form = _form("smart_capture_missing", values={"brand_name": "Nike"},
                     missing_fields=["poc_email"])
        svc.gemini.extract_job_fields.return_value = None
        result = svc._handle_smart_capture_missing("u1", "karan@notvalid", form)
        assert result["operation"] == "smart_capture_invalid_email"


class TestSaveSmartCaptureJob:
    def test_only_whitelisted_fields_mapped_to_record(self):
        svc = _make_svc()
        svc.supabase.insert_job_entry.return_value = {"ok": True, "rows": [{"id": 1}]}
        extracted = {"brand_name": "Nike", "fees": 25000, "some_unexpected_llm_field": "junk"}
        svc._save_smart_capture_job("u1", extracted)
        record = svc.supabase.insert_job_entry.call_args.args[0]
        assert "some_unexpected_llm_field" not in record
        assert record["brand_name"] == "Nike" and record["fees"] == 25000

    def test_insert_failure_gives_friendly_message_and_clears_form(self):
        svc = _make_svc()
        svc.supabase.insert_job_entry.return_value = {"ok": False, "error": "db down"}
        result = svc._save_smart_capture_job("u1", {"brand_name": "Nike", "fees": 25000})
        assert "couldn't save" in result["response"].lower()
        svc.memory.cancel_form.assert_called_once_with("u1")

    def test_client_name_used_when_brand_missing(self):
        svc = _make_svc()
        svc.supabase.insert_job_entry.return_value = {"ok": True, "rows": [{"id": 1}]}
        result = svc._save_smart_capture_job("u1", {"client_name": "Star Studios", "fees": 25000})
        assert "Star Studios" in result["response"]

    def test_compound_intent_suggestion_surfaced_after_save(self):
        svc = _make_svc()
        svc.supabase.insert_job_entry.return_value = {"ok": True, "rows": [{"id": 1}]}
        svc.memory.get_user_memory.return_value = {"suggested_next_action": "send the invoice"}
        result = svc._save_smart_capture_job("u1", {"brand_name": "Nike", "fees": 25000})
        assert "send the invoice" in result["response"]
        awaiting = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                    if "awaiting_compound_response" in c.args[1]]
        assert awaiting and awaiting[-1]["awaiting_compound_response"] is True
