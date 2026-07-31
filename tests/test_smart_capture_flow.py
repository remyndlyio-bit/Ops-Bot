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
    # Default to "not a new query" so the AI fallback in _handle_form_step's
    # escape hatch doesn't spuriously cancel forms in tests that aren't
    # exercising that fallback (a bare MagicMock() is truthy).
    svc.gemini.is_new_query_not_response.return_value = False
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

    @pytest.mark.parametrize("msg", [
        "Add job for Nike, 20 July, shooting, 25000",
        "add job Nike ke liye 20 July dubbing 15k",
        "Add job for Star Studios, 20 July, dubbing, 15k",
    ])
    def test_add_job_message_cancels_old_form_instead_of_being_treated_as_a_reply(self, msg):
        """Live bug found in a 134-scenario test run: with a smart-capture
        confirmation card still active from a PRIOR job entry, sending a
        fresh "Add job for ..." message didn't match the yes/no/edit
        keywords _handle_smart_capture_confirm checks for, so it fell into
        the "unrecognised reply" branch and got a bare "Please reply Yes to
        save or Edit to make changes" with no field breakdown at all --
        the user had nothing to review before confirming, and their actual
        new job never got parsed. "add " was missing from the new-intent
        escape-hatch word list (generate/send/mark/set/update/delete/show/
        list were all there) even though it's the single most common way to
        start a job-entry message."""
        svc = _make_svc()
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        result = svc._handle_form_step("u1", msg)
        assert result is None, f"{msg!r} was treated as a reply instead of a new job entry: {result}"
        svc.memory.cancel_form.assert_called_once_with("u1")

    @pytest.mark.parametrize("msg", ["cancel", "stop", "nevermind", "abort", "exit"])
    def test_explicit_cancel_words_end_the_form(self, msg):
        svc = _make_svc()
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        result = svc._handle_form_step("u1", msg)
        assert result["operation"] == "form_cancelled"
        svc.memory.cancel_form.assert_called_once_with("u1")

    def test_hindi_new_job_entry_cancels_old_form_even_when_ai_classifier_disagrees(self):
        """Live bug (#19), found TWICE in successive live suite runs:

        Run 1: a Hindi/Hinglish new job entry ("ZZTEST Nike ka kaam kiya 20
        July ko, shooting, 25 hazaar") never matched the English-only
        _new_intent_starts keyword list, so with a stale confirm form still
        active it got swallowed into "Please reply Yes to save or Edit to
        make changes" instead of being recognised as a brand new entry.

        Run 2 (after adding an AI fallback using is_new_query_not_response):
        the SAME message still failed live, because that classifier is
        built to detect new QUERY/command-shaped messages, and a Hindi
        STATEMENT describing a job ("I did work for Nike on 20 July...")
        isn't phrased as a command or question -- the model said "not a new
        query" even though it obviously is a new, distinct job entry.

        The real fix is a deterministic heuristic that doesn't depend on
        the classifier's judgment at all: a message with BOTH a date (day +
        month name) AND a fee/amount token is unmistakably a new job entry.
        This test pins that down by explicitly making the AI mock return
        the WRONG answer (False) to prove the heuristic alone is sufficient."""
        svc = _make_svc()
        svc.gemini.is_new_query_not_response.return_value = False
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        msg = "ZZTEST Nike ka kaam kiya 20 July ko, shooting, 25 hazaar"
        result = svc._handle_form_step("u1", msg)
        assert result is None, f"Hindi new job entry was treated as a reply: {result}"
        svc.memory.cancel_form.assert_called_once_with("u1")
        svc.gemini.is_new_query_not_response.assert_not_called()

    @pytest.mark.parametrize("msg", [
        "ZZTEST Nike ka kaam kiya 20 July ko, shooting, 25 hazaar",
        "kiya Nike ke liye 5 Mar ko, dubbing, 1.5L",
        "Bridgestone 12 Sep ko, shoot, 30k ka kaam",
    ])
    def test_date_plus_amount_heuristic_catches_job_entry_shape(self, msg):
        """Broader coverage for the date+amount heuristic across a few
        Hindi/Hinglish/mixed phrasings, independent of the AI classifier."""
        svc = _make_svc()
        svc.gemini.is_new_query_not_response.return_value = False
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        result = svc._handle_form_step("u1", msg)
        assert result is None, f"{msg!r} was treated as a reply: {result}"

    def test_date_without_amount_still_falls_back_to_ai(self):
        """Over-narrowness guard: a message with a date but no amount isn't
        caught by the deterministic heuristic, so it should still reach the
        AI fallback (which decides normally)."""
        svc = _make_svc()
        svc.gemini.is_new_query_not_response.return_value = False
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        result = svc._handle_form_step("u1", "20 July")
        svc.gemini.is_new_query_not_response.assert_called_once()
        assert result is not None  # AI said "not a new query" -> falls through to confirm handler

    def test_ai_fallback_not_consulted_when_keyword_already_matched(self):
        """Efficiency/safety: don't call the AI classifier when the fast
        keyword path already decided — avoids an extra AI round-trip on the
        common case and avoids a spurious call the assertion below would
        otherwise miss."""
        svc = _make_svc()
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        svc._handle_form_step("u1", "show my jobs")
        svc.gemini.is_new_query_not_response.assert_not_called()

    def test_unknown_form_type_cancelled_silently(self):
        svc = _make_svc()
        svc.memory.get_form_state.return_value = _form("some_future_form_type")
        result = svc._handle_form_step("u1", "whatever")
        assert result is None
        svc.memory.cancel_form.assert_called_once_with("u1")


class TestSmartCaptureConfirmationFeeFormatting:
    """Regression: the smart-capture confirmation card displayed fees using
    Python's default Western digit grouping (₹150,000) while every other
    part of the product (query responses, reminder emails) uses Indian
    grouping (₹1,50,000) via answer_ledger.format_inr — an inconsistency a
    scripted test run flagged directly ("Parses fees as ₹1,50,000" expected,
    got "₹150,000")."""

    def test_confirmation_card_uses_indian_grouping(self):
        svc = _make_svc()
        result = svc._show_smart_capture_confirmation("u1", {"brand_name": "Nike", "fees": 150000})
        assert "₹1,50,000" in result["response"]
        assert "₹150,000" not in result["response"]

    def test_confirmation_after_missing_fields_completed_uses_indian_grouping(self):
        svc = _make_svc()
        svc.gemini.extract_job_fields.return_value = {"job_date": "2026-04-10"}
        result = svc._handle_smart_capture_missing(
            "u1", "10 April",
            _form("smart_capture_missing", values={"brand_name": "Nike", "fees": 150000},
                  missing_fields=["job_date"]),
        )
        assert result["operation"] == "smart_capture_confirm"
        assert "₹1,50,000" in result["response"]


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
        # Phase 2.3: the "Edit" branch no longer writes a legacy
        # awaiting_job_input flag — it transitions FlowMachine directly to
        # SMART_CAPTURE_NEED_DESCRIPTION via _arm_smart_capture_description_v2.
        svc = _make_svc()
        form = _form("smart_capture_confirm", values=dict(self.VALUES))
        result = svc._handle_smart_capture_confirm("u1", msg, form)
        assert result["operation"] == "smart_capture_edit"
        svc.supabase.insert_job_entry.assert_not_called()
        # flow_machine binds to the memory instance present at IntentService
        # construction time (services/intent_service.py's __init__), which
        # is a different mock object than svc.memory (reassigned after
        # construction by _make_svc) -- so writes must be read from
        # svc.flow_machine._mem, not svc.memory.
        from services.flow_machine import FLOW_SMART_CAPTURE_NEED_DESCRIPTION, _MEM_KEY
        fm_writes = [c.args[1][_MEM_KEY] for c in svc.flow_machine._mem.update_user_memory.call_args_list
                     if _MEM_KEY in c.args[1]]
        assert fm_writes and fm_writes[-1]["flow"] == FLOW_SMART_CAPTURE_NEED_DESCRIPTION

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


class TestCompoundIntentEntryPoint:
    """The gap _save_smart_capture_job's own test doesn't cover: does a
    compound "add job ... and send invoice" message flowing through
    process_request() actually reach decompose_compound_intent() and set
    suggested_next_action in the first place? (Previously untested end to
    end — a live scenario-suite run found "Add job for Nike 20 April shoot
    25k and send invoice" never surfaced the "want me to send invoice?"
    follow-up; that turned out to trace back to a DIFFERENT bug -- a stale
    confirmation form from the PRECEDING turn swallowing the message before
    it ever reached this code path (fixed by adding "add " to the
    new-intent escape hatch) -- but the compound-detection entry point
    itself had zero direct test coverage, so that conclusion couldn't be
    verified. These tests close that gap.)"""

    def _make_onboarded_svc(self):
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "Test User"},
        }
        svc.supabase.db_url = "postgresql://fake"
        svc.gemini.is_history_question.return_value = False
        svc.memory.get_form_state.return_value = None
        return svc

    def test_compound_add_job_message_calls_decompose_and_sets_suggestion(self):
        svc = self._make_onboarded_svc()
        svc.gemini.decompose_compound_intent.return_value = [
            "Add job for Nike 20 April shoot 25k",
            "send invoice",
        ]
        svc.gemini.extract_job_fields.return_value = {
            "brand_name": "Nike", "job_date": "2026-04-20",
            "job_description_details": "shoot", "fees": 25000,
        }
        result = svc.process_request("u1", "Add job for Nike 20 April shoot 25k and send invoice")
        svc.gemini.decompose_compound_intent.assert_called_once()
        saved = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                 if "suggested_next_action" in c.args[1]]
        assert saved and saved[-1]["suggested_next_action"] == "send invoice"
        assert result["operation"] == "smart_capture_confirm"

    def test_full_compound_flow_confirm_then_yes_surfaces_next_action(self):
        """End to end: add job (compound) -> confirm save -> the save
        response must surface the "want me to send invoice?" follow-up."""
        svc = self._make_onboarded_svc()
        svc.gemini.decompose_compound_intent.return_value = [
            "Add job for Nike 20 April shoot 25k",
            "send invoice",
        ]
        svc.gemini.extract_job_fields.return_value = {
            "brand_name": "Nike", "job_date": "2026-04-20",
            "job_description_details": "shoot", "fees": 25000,
        }
        svc.supabase.insert_job_entry.return_value = {"ok": True, "rows": [{"id": 1}]}

        first = svc.process_request("u1", "Add job for Nike 20 April shoot 25k and send invoice")
        assert first["operation"] == "smart_capture_confirm"

        # Simulate what got persisted: the confirmation form + the
        # suggested_next_action set by the first turn.
        svc.memory.get_form_state.return_value = _form(
            "smart_capture_confirm",
            values={"brand_name": "Nike", "job_date": "2026-04-20",
                    "job_description_details": "shoot", "fees": 25000},
        )
        svc.memory.get_user_memory.return_value = {"suggested_next_action": "send invoice"}

        second = svc.process_request("u1", "Yes")
        assert "send invoice" in second["response"].lower()

    def test_single_intent_add_job_does_not_call_decompose_for_short_messages(self):
        """decompose_compound_intent is only worth calling on messages long
        enough to plausibly contain two actions -- a short single-intent
        message shouldn't pay for the extra AI round trip."""
        svc = self._make_onboarded_svc()
        svc.gemini.extract_job_fields.return_value = {
            "brand_name": "Nike", "job_date": "2026-04-20",
            "job_description_details": "shoot", "fees": 25000,
        }
        svc.process_request("u1", "add job Nike 25k")
        svc.gemini.decompose_compound_intent.assert_not_called()


class TestFormStepNoneDoesNotCrashTheWholeTurn:
    """The actual root cause of the intermittent-looking None-crash bug
    (process_request() dying with "'NoneType' object has no attribute
    'get'"). _handle_form_step legitimately returns None on several of its
    OWN escape hatches (stale form, a "+..." new job entry, or a message
    matching an obvious new-intent word) -- each cancels the stale form and
    is documented to "fall through to normal processing." But the call site
    in _process_request_impl did `return self._handle_form_step(...)`
    directly, propagating that None as _process_request_impl's own result
    instead of falling through -- skipping the entire rest of the pipeline
    and crashing the wrapper. It reproduced for ANY message that both (a)
    arrives while some form (even a stale/irrelevant one) is active, and
    (b) matches one of those escape hatches -- e.g. "+Nike 20 Jul shoot
    25k" (starts with "+") or "What are my total earnings?" (starts with
    "what ")."""

    def _make_onboarded_svc(self):
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "Test User"},
        }
        svc.supabase.db_url = "postgresql://fake"
        svc.gemini.is_history_question.return_value = False
        return svc

    def test_plus_prefixed_message_with_active_form_does_not_crash(self):
        svc = self._make_onboarded_svc()
        svc.memory.get_form_state.return_value = _form("smart_capture_confirm")
        svc.gemini.extract_job_fields.return_value = {
            "brand_name": "Nike", "job_date": "2026-07-20",
            "job_description_details": "shoot", "fees": 25000,
        }
        result = svc.process_request("u1", "+Nike 20 Jul shoot 25k")
        assert isinstance(result, dict)
        assert result.get("operation") != "error"

    def test_what_question_with_active_form_does_not_crash(self):
        svc = self._make_onboarded_svc()
        svc.memory.get_form_state.return_value = _form("smart_capture_missing")
        result = svc.process_request("u1", "What are my total earnings?")
        assert isinstance(result, dict)
        assert result.get("operation") != "error"

    def test_stale_form_with_any_message_does_not_crash(self):
        svc = self._make_onboarded_svc()
        svc.memory.get_form_state.return_value = _form(
            "smart_capture_confirm", created_at=(datetime.now() - timedelta(minutes=45)).isoformat(),
        )
        result = svc.process_request("u1", "show my jobs")
        assert isinstance(result, dict)
        assert result.get("operation") != "error"
