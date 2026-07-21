"""
Category 7 — Account, Profile & Bank Details: cross-platform account linking.

`grep -rli "linked_user_id\\|link.*account" tests/*.py` returned zero files
before this one — the whole linking mechanism (link a Telegram/WhatsApp id,
resolve it for every subsequent data query via `_resolve_data_user_id`) had
no coverage at all. `_resolve_data_user_id` sits in front of EVERY query/
mutation for a linked user, so a bug here silently points a user's queries at
the wrong data set (or crashes on malformed preferences JSON).
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import json
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
    svc.supabase.get_user_profile.return_value = {
        "ok": True, "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "Test User"},
    }
    svc.supabase.db_url = "postgresql://fake"
    return svc


class TestResolveDataUserId:
    """The read side: every query/mutation resolves through this first."""

    def test_no_preferences_returns_own_id(self):
        svc = _make_svc()
        assert svc._resolve_data_user_id("u1", {}) == "u1"

    def test_linked_id_present_returns_linked(self):
        svc = _make_svc()
        profile = {"preferences": {"linked_user_id": "751256859"}}
        assert svc._resolve_data_user_id("u1", profile) == "751256859"

    def test_preferences_missing_linked_key_returns_own_id(self):
        svc = _make_svc()
        profile = {"preferences": {"invoice_name": "Studio X"}}
        assert svc._resolve_data_user_id("u1", profile) == "u1"

    def test_preferences_as_json_string_parsed(self):
        svc = _make_svc()
        profile = {"preferences": json.dumps({"linked_user_id": "999"})}
        assert svc._resolve_data_user_id("u1", profile) == "999"

    def test_malformed_json_string_falls_back_to_own_id(self):
        """Preferences corruption must never crash a query — fall back safely."""
        svc = _make_svc()
        profile = {"preferences": "{not valid json"}
        assert svc._resolve_data_user_id("u1", profile) == "u1"

    def test_none_preferences_returns_own_id(self):
        svc = _make_svc()
        assert svc._resolve_data_user_id("u1", {"preferences": None}) == "u1"


class TestHandleLinkAccount:
    """Initiating a link: inline id vs. prompt-then-reply."""

    def test_inline_numeric_id_links_immediately(self):
        svc = _make_svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._handle_link_account("u1", "link telegram 751256859")
        assert result["operation"] == "account_linked"
        assert "751256859" in result["response"]
        svc.memory.update_user_memory.assert_not_called()  # no need to set awaiting_link_id

    def test_inline_whatsapp_id_links_immediately(self):
        svc = _make_svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._handle_link_account("751256859", "link whatsapp:+919876543210")
        assert result["operation"] == "account_linked"

    def test_short_number_not_treated_as_id_prompts_instead(self):
        """A number under 5 digits (e.g. a stray '123' in the sentence) must not
        be mistaken for a real platform id."""
        svc = _make_svc()
        result = svc._handle_link_account("u1", "link my account, it's like #123 I think")
        assert result["operation"] == "link_prompt"
        awaiting = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                    if "awaiting_link_id" in c.args[1]]
        assert awaiting and awaiting[-1]["awaiting_link_id"] is True

    def test_no_inline_id_prompts_and_sets_awaiting_state(self):
        svc = _make_svc()
        result = svc._handle_link_account("u1", "link my account")
        assert result["operation"] == "link_prompt"
        awaiting = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                    if "awaiting_link_id" in c.args[1]]
        assert awaiting and awaiting[-1]["awaiting_link_id"] is True
        assert svc.supabase.upsert_user_profile.call_count == 0, "must not write anything yet"


class TestProcessLinkId:
    """The reply after being prompted."""

    @pytest.mark.parametrize("msg", ["cancel", "nevermind", "never mind", "no"])
    def test_cancel_variants_do_not_link(self, msg):
        svc = _make_svc()
        result = svc._process_link_id("u1", msg)
        assert result["operation"] == "link_cancelled"
        assert svc.supabase.upsert_user_profile.call_count == 0

    def test_valid_id_reply_links(self):
        svc = _make_svc()
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._process_link_id("u1", "751256859")
        assert result["operation"] == "account_linked"
        assert svc.supabase.upsert_user_profile.call_count == 1

    def test_clears_awaiting_state_regardless_of_outcome(self):
        svc = _make_svc()
        svc._process_link_id("u1", "cancel")
        awaiting = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                    if "awaiting_link_id" in c.args[1]]
        assert awaiting and awaiting[0]["awaiting_link_id"] is False


class TestApplyLink:
    """The actual write: must merge into existing preferences, never clobber
    unrelated keys (e.g. invoice_name set by a prior name change)."""

    def test_merges_without_clobbering_existing_preferences(self):
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {
            "ok": True,
            "data": {"preferences": {"invoice_name": "Studio X", "onboarded_at": "2024-01-01"}},
        }
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        svc._apply_link("u1", "link telegram 751256859", "751256859")
        call = svc.supabase.upsert_user_profile.call_args
        written_prefs = json.loads(call.args[2]["preferences"])
        assert written_prefs["linked_user_id"] == "751256859"
        assert written_prefs["invoice_name"] == "Studio X", "must not clobber other preference keys"

    def test_existing_preferences_as_json_string_parsed_before_merge(self):
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {
            "ok": True,
            "data": {"preferences": json.dumps({"invoice_name": "Studio X"})},
        }
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        svc._apply_link("u1", "link telegram 751256859", "751256859")
        written_prefs = json.loads(svc.supabase.upsert_user_profile.call_args.args[2]["preferences"])
        assert written_prefs["invoice_name"] == "Studio X"
        assert written_prefs["linked_user_id"] == "751256859"

    def test_malformed_existing_preferences_does_not_crash(self):
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"preferences": "{not valid json"},
        }
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._apply_link("u1", "link telegram 751256859", "751256859")
        assert result["operation"] == "account_linked"
        written_prefs = json.loads(svc.supabase.upsert_user_profile.call_args.args[2]["preferences"])
        assert written_prefs["linked_user_id"] == "751256859"

    def test_upsert_failure_gives_friendly_message_no_crash(self):
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        svc.supabase.upsert_user_profile.return_value = {"ok": False, "error": "db down"}
        result = svc._apply_link("u1", "link telegram 751256859", "751256859")
        assert result["operation"] == "account_linked"  # op name unchanged; response carries the failure
        assert "couldn't link" in result["response"].lower() or "try again" in result["response"].lower()

    def test_profile_lookup_failure_still_attempts_link_with_empty_prefs(self):
        """If the pre-fetch to read existing preferences fails, still proceed
        with an empty preferences dict rather than aborting the link."""
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": False, "error": "timeout"}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        result = svc._apply_link("u1", "link telegram 751256859", "751256859")
        assert result["operation"] == "account_linked"
        written_prefs = json.loads(svc.supabase.upsert_user_profile.call_args.args[2]["preferences"])
        assert written_prefs["linked_user_id"] == "751256859"


class TestLinkingEndToEndDataResolution:
    """The point of the whole feature: after linking, data queries must resolve
    to the LINKED id, not the messaging platform id."""

    def test_resolved_id_matches_what_was_just_linked(self):
        svc = _make_svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {}}
        svc.supabase.upsert_user_profile.return_value = {"ok": True}
        svc._apply_link("whatsapp_919876543210", "link telegram 751256859", "751256859")
        written_prefs = json.loads(svc.supabase.upsert_user_profile.call_args.args[2]["preferences"])
        # Simulate the next request reading that profile back.
        resolved = svc._resolve_data_user_id("whatsapp_919876543210", {"preferences": written_prefs})
        assert resolved == "751256859"
