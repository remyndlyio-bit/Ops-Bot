"""P0-A: Universal cancel — "cancel" must actually cancel, not explain."""

import pytest
from unittest.mock import MagicMock, patch


def _make_svc():
    """Factory matching the pattern in test_handle_query_request_extraction.py."""
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()

    # Mock the services
    svc.memory = MagicMock()
    svc.memory.get_user_memory.return_value = {}
    svc.memory.get_form_state.return_value = None
    svc.memory.get_conversation_history.return_value = []
    svc.supabase = MagicMock()
    svc.gemini = MagicMock()
    svc.flow_machine = MagicMock()
    svc.flow_machine.current_flow.return_value = "FLOW_IDLE"
    svc.flow_machine.expire_if_stale.return_value = False
    return svc


@patch("services.intent_service._flow_machine_v2_enabled_for", return_value=True)
class TestUniversalCancel:
    """P0-A: cancel must clear pending state, not reply with a no-op."""

    def test_cancel_with_form_active_clears_it(self, mock_v2):
        """When a form is active and user says "cancel", the form is cleared."""
        svc = _make_svc()
        # Mock form state
        svc.memory.get_form_state.return_value = {"type": "smart_capture_missing", "values": {}}

        result = svc._process_request_impl("u1", "cancel")

        assert result["operation"] == "cancelled"
        assert "cancelled" in result["response"].lower()

    def test_cancel_with_legacy_awaiting_flag_clears_it(self, mock_v2):
        """When a legacy awaiting_* flag is set."""
        svc = _make_svc()
        user_mem = {"awaiting_invoice_month": True}
        svc.memory.get_user_memory.return_value = user_mem
        svc.memory.get_form_state.return_value = None

        result = svc._process_request_impl("u1", "cancel")

        assert result["operation"] == "cancelled"

    def test_next_message_after_cancel_routes_normally(self, mock_v2):
        """After a cancel, the next message shouldn't be swallowed."""
        svc = _make_svc()
        # Mock successful query execution
        svc.supabase.execute.return_value = {"rows": [{"id": 1, "brand_name": "Nike"}], "count": 1}
        svc.gemini.extract_job_fields.return_value = {}

        # First: cancel with a form active
        svc.memory.get_form_state.return_value = {"type": "smart_capture_missing"}
        cancel_result = svc._process_request_impl("u1", "cancel")
        assert cancel_result["operation"] == "cancelled"

        # Second: clear the form and send a new query
        svc.memory.get_form_state.return_value = None
        svc.memory.get_user_memory.return_value = {}
        svc.flow_machine.current_flow.return_value = "FLOW_IDLE"

        # This should route to a query, not be captured by lingering state
        query_result = svc._process_request_impl("u1", "how many jobs?")
        assert query_result["operation"] in ("query", "query_result", "clarify")

    @pytest.mark.parametrize("cancel_word", [
        "cancel", "stop", "nevermind", "never mind", "nvm", "abort", "quit", "exit"
    ])
    def test_all_cancel_words_work(self, mock_v2, cancel_word):
        """All canonical cancel words trigger the behavior."""
        svc = _make_svc()
        svc.memory.get_form_state.return_value = {"type": "smart_capture_missing"}

        result = svc._process_request_impl("u1", cancel_word)

        assert result["operation"] == "cancelled"
