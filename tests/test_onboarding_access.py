"""
Category 8 — Onboarding & Access Control.

Covers the beta gate (BETA_GATE_ENABLED) and the 3-step onboarding flow
(_continue_onboarding: name -> email -> industry, all skippable). Zero
dedicated coverage existed before this file — the only "onboard" hits in the
suite were incidental (onboarded_at appearing in unrelated mock profiles).

Fee/field EXTRACTION for smart capture ("25k" -> 25000, "1.5L" -> 150000) is
NOT covered here or anywhere offline: unlike invoice_generation_service's
_parse_fees() (a deterministic regex parser, tested in
test_scenarios_from_matrix.py), smart-capture field extraction is delegated
entirely to the LLM prompt in gemini_service.extract_job_fields() -- there is
no Python-side parsing to unit-test. That's covered live (needs AI_KEY) in
test_e2e_live.py's C9-01/C9-02. What IS deterministic and untested before this
file is the FORM STATE MACHINE around it -- confirm/missing-field loop, retry
counting, staleness, escape hatches -- covered in test_smart_capture_flow.py.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
    svc.memory.get_form_state.return_value = None
    svc.memory.get_conversation_history.return_value = []
    svc.memory.get_user_memory.return_value = {}
    svc.supabase.db_url = "postgresql://fake"
    return svc


class TestBetaGate:
    """BETA_GATE_ENABLED blocks first-touch (no profile / not-yet-onboarded)
    users who aren't allowlisted. Onboarded users are ALWAYS exempt, and the
    gate must not create a profile for a blocked user."""

    def test_gate_off_lets_new_user_through_to_onboarding(self, monkeypatch):
        monkeypatch.delenv("BETA_GATE_ENABLED", raising=False)
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": None}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc.process_request("u1", "hi")
        assert result["operation"] == "onboarding_started"
        svc.supabase.is_user_allowed.assert_not_called()

    def test_gate_on_blocks_unallowlisted_new_user(self, monkeypatch):
        monkeypatch.setenv("BETA_GATE_ENABLED", "true")
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": None}
        svc.supabase.is_user_allowed.return_value = False
        result = svc.process_request("u1", "hi")
        assert result["operation"] == "beta_gate_blocked"
        assert "private beta" in result["response"].lower()

    def test_gate_blocked_user_gets_no_profile_created(self, monkeypatch):
        """The whole point of the gate: a blocked user must not be silently
        onboarded — no profile write until an admin allowlists them."""
        monkeypatch.setenv("BETA_GATE_ENABLED", "true")
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": None}
        svc.supabase.is_user_allowed.return_value = False
        svc.process_request("u1", "hi")
        assert svc.supabase.upsert_user_profile.call_count == 0

    def test_gate_on_allows_allowlisted_new_user(self, monkeypatch):
        monkeypatch.setenv("BETA_GATE_ENABLED", "true")
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": None}
        svc.supabase.is_user_allowed.return_value = True
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc.process_request("u1", "hi")
        assert result["operation"] == "onboarding_started"

    def test_gate_on_mid_onboarding_user_still_checked(self, monkeypatch):
        """A profile exists but onboarding isn't finished (no onboarded_at) —
        still subject to the allowlist, not exempt yet."""
        monkeypatch.setenv("BETA_GATE_ENABLED", "true")
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"name": "Akshaj"},  # no onboarded_at
        }
        svc.supabase.is_user_allowed.return_value = False
        result = svc.process_request("u1", "hi")
        assert result["operation"] == "beta_gate_blocked"

    def test_gate_on_onboarded_user_always_exempt(self, monkeypatch):
        """An already-onboarded user must bypass the allowlist check entirely,
        even if they'd fail it."""
        monkeypatch.setenv("BETA_GATE_ENABLED", "true")
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "Akshaj"},
        }
        svc.supabase.is_user_allowed.return_value = False  # would fail if checked
        # Short-circuit right after the gate/onboarding checks so we don't have
        # to mock the entire downstream query pipeline.
        svc.memory.get_form_state.return_value = {"form_type": "x"}
        svc._handle_form_step = MagicMock(return_value={"operation": "stub", "response": "ok"})
        result = svc.process_request("u1", "hi")
        svc.supabase.is_user_allowed.assert_not_called()
        assert result["operation"] == "stub"

    def test_gate_profile_lookup_error_fails_closed_not_open(self, monkeypatch):
        """If the profile lookup itself errors, the gate must still enforce the
        allowlist (treat as not-onboarded) rather than waving everyone through."""
        monkeypatch.setenv("BETA_GATE_ENABLED", "true")
        svc = _make_svc()
        svc.supabase.get_user_profile.side_effect = Exception("db timeout")
        svc.supabase.is_user_allowed.return_value = False
        result = svc.process_request("u1", "hi")
        assert result["operation"] == "beta_gate_blocked"


class TestNewUserRouting:
    """Post-gate: how process_request decides new-user vs continue-onboarding
    vs pass-through, including defensive handling of a broken profile lookup."""

    def test_profile_lookup_failure_treated_as_new_user(self, monkeypatch):
        monkeypatch.delenv("BETA_GATE_ENABLED", raising=False)
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": False, "error": "db down"}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc.process_request("u1", "hi")
        assert result["operation"] == "onboarding_started"

    def test_no_profile_row_starts_onboarding(self, monkeypatch):
        monkeypatch.delenv("BETA_GATE_ENABLED", raising=False)
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": None}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc.process_request("u1", "hello")
        assert result["operation"] == "onboarding_started"

    def test_incomplete_profile_continues_onboarding(self, monkeypatch):
        monkeypatch.delenv("BETA_GATE_ENABLED", raising=False)
        svc = _make_svc()
        svc.gemini.extract_name.return_value = "Akshaj Kasliwal"
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"platform": "whatsapp"},  # no name, no onboarded_at
        }
        result = svc.process_request("u1", "Akshaj Kasliwal")
        assert result["operation"] == "onboarding_name"


class TestOnboardingNameStep:
    def test_skip_uses_generic_name_and_jumps_to_industry(self):
        """Skipping the name step goes straight to the industry prompt (email
        is skipped too when there's no name to personalise it with) — confirmed
        by reading _continue_onboarding directly, not assumed."""
        svc = _make_svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._continue_onboarding("u1", "skip", {"platform": "whatsapp"})
        assert result["operation"] == "onboarding_name"
        assert svc.supabase.upsert_user_profile.call_args.args[2]["name"] == "User"
        assert "industry" in result["response"].lower()

    @pytest.mark.parametrize("msg", ["hi", "hello", "hey", "good morning", "namaste"])
    def test_greeting_only_does_not_get_saved_as_name(self, msg):
        svc = _make_svc()
        result = svc._continue_onboarding("u1", msg, {"platform": "whatsapp"})
        assert result["operation"] == "onboarding_name_retry"
        svc.supabase.upsert_user_profile.assert_not_called()

    def test_ai_extracted_name_saved_title_cased(self):
        svc = _make_svc()
        svc.gemini.extract_name.return_value = "akshaj"
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._continue_onboarding("u1", "my name is akshaj", {"platform": "whatsapp"})
        assert result["operation"] == "onboarding_name"
        assert svc.supabase.upsert_user_profile.call_args.args[2]["name"] == "Akshaj"

    def test_ai_extraction_failure_falls_back_to_pattern_match(self):
        svc = _make_svc()
        svc.gemini.extract_name.return_value = None
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._continue_onboarding("u1", "my name is Akshaj", {"platform": "whatsapp"})
        assert result["operation"] == "onboarding_name"
        saved = svc.supabase.upsert_user_profile.call_args.args[2]["name"]
        assert "akshaj" in saved.lower()

    def test_empty_name_retries(self):
        svc = _make_svc()
        svc.gemini.extract_name.return_value = None
        result = svc._continue_onboarding("u1", "no", {"platform": "whatsapp"})
        assert result["operation"] == "onboarding_name_retry"
        svc.supabase.upsert_user_profile.assert_not_called()


class TestOnboardingEmailStep:
    PROFILE = {"platform": "whatsapp", "name": "Akshaj"}

    def test_skip_completes_onboarding_using_name_as_industry(self):
        svc = _make_svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._continue_onboarding("u1", "skip", self.PROFILE)
        assert result["operation"] == "onboarding_complete"
        call = svc.supabase.upsert_user_profile.call_args.args[2]
        assert call["preferences"]["industry"] == "Akshaj"
        assert "onboarded_at" in call

    def test_invalid_email_retries_without_saving(self):
        svc = _make_svc()
        result = svc._continue_onboarding("u1", "not-an-email", self.PROFILE)
        assert result["operation"] == "onboarding_email_retry"
        svc.supabase.upsert_user_profile.assert_not_called()

    def test_valid_email_extracted_from_sentence(self):
        svc = _make_svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._continue_onboarding("u1", "it's akshaj@studio.com", self.PROFILE)
        assert result["operation"] == "onboarding_email"
        saved_prefs = svc.supabase.upsert_user_profile.call_args.args[2]["preferences"]
        assert saved_prefs["invoice_email"] == "akshaj@studio.com"


class TestOnboardingIndustryStep:
    @staticmethod
    def _profile():
        # A FRESH dict per test — _continue_onboarding mutates prefs["industry"]
        # in place, so a shared class-level dict would leak between tests
        # (test N+1 would see test N's industry already set and fall through to
        # a different branch entirely).
        return {"platform": "whatsapp", "name": "Akshaj",
                "preferences": {"invoice_email": "akshaj@studio.com"}}

    def test_skip_uses_name_as_industry_and_completes(self):
        svc = _make_svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._continue_onboarding("u1", "skip", self._profile())
        assert result["operation"] == "onboarding_complete"
        call = svc.supabase.upsert_user_profile.call_args.args[2]
        assert call["preferences"]["industry"] == "Akshaj"
        assert "onboarded_at" in call

    def test_real_industry_saved_and_completes(self):
        svc = _make_svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._continue_onboarding("u1", "Video Production", self._profile())
        assert result["operation"] == "onboarding_complete"
        call = svc.supabase.upsert_user_profile.call_args.args[2]
        assert call["preferences"]["industry"] == "Video Production"
        assert "onboarded_at" in call

    def test_overlong_industry_truncated(self):
        svc = _make_svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        long_industry = "x" * 200
        svc._continue_onboarding("u1", long_industry, self._profile())
        saved = svc.supabase.upsert_user_profile.call_args.args[2]["preferences"]["industry"]
        assert len(saved) == 80
