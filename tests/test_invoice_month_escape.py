"""P0-B containment: invoice month-await handler escape for email/skip/cancel."""

import pytest
from unittest.mock import MagicMock, patch


def _make_svc():
    """Factory for IntentService with mocked dependencies."""
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()

    svc.memory = MagicMock()
    svc.memory.get_user_memory.return_value = {}
    svc.memory.get_conversation_history.return_value = []
    svc.supabase = MagicMock()
    svc.gemini = MagicMock()
    svc.flow_machine = MagicMock()
    return svc


class TestInvoiceMonthEscape:
    """P0-B containment: prevent month-retry loop when user provides email/cancel/skip."""

    def test_skip_escapes_month_retry(self):
        """When awaiting month and user says 'skip', should cancel, not retry."""
        svc = _make_svc()
        user_mem = {"pending_invoice_client": "Nike", "pending_invoice_send_email": False}

        result = svc._handle_invoice_month_reply("u1", "skip", user_mem, "u1_data", [])

        assert result["operation"] == "cancelled"
        assert "cancelled" in result["response"].lower()

    def test_cancel_escapes_month_retry(self):
        """When awaiting month and user says 'cancel', should cancel, not retry."""
        svc = _make_svc()
        user_mem = {"pending_invoice_client": "Nike", "pending_invoice_send_email": False}

        result = svc._handle_invoice_month_reply("u1", "cancel", user_mem, "u1_data", [])

        assert result["operation"] == "cancelled"

    def test_nevermind_escapes_month_retry(self):
        """When awaiting month and user says 'nevermind', should cancel."""
        svc = _make_svc()
        user_mem = {"pending_invoice_client": "Nike", "pending_invoice_send_email": False}

        result = svc._handle_invoice_month_reply("u1", "nevermind", user_mem, "u1_data", [])

        assert result["operation"] == "cancelled"

    def test_email_address_escapes_month_retry(self):
        """When awaiting month and user provides email (row 90-92 failure pattern),
        should detect it and offer guidance instead of 'couldn't detect month' loop."""
        svc = _make_svc()
        svc._is_valid_email = MagicMock(return_value=True)
        user_mem = {"pending_invoice_client": "Nike", "pending_invoice_send_email": False}

        result = svc._handle_invoice_month_reply("u1", "rahul@starstudios.com", user_mem, "u1_data", [])

        # Should NOT be "invoice_month_retry" — that's the trap rows 90-92 fell into
        assert result["operation"] != "invoice_month_retry"
        assert "email_provided_instead_of_month" in result["operation"]
        # Should NOT show "couldn't detect a month"
        assert "couldn't detect" not in result["response"].lower()

    def test_invalid_email_treated_as_text_not_escaped(self):
        """When user provides text that looks like email but isn't (no @), should not escape."""
        svc = _make_svc()
        user_mem = {"pending_invoice_client": "Nike", "pending_invoice_send_email": False}

        # Something with @ and no space, but not a valid email
        result = svc._handle_invoice_month_reply("u1", "notanemail@", user_mem, "u1_data", [])

        # Should fall through to normal month detection which fails, then retry
        # (this is the OK behavior — we only escape on VALID emails)
        assert result["operation"] == "invoice_month_retry"

    def test_legitimate_month_reply_still_works(self):
        """Normal month replies should still work (regression check)."""
        svc = _make_svc()
        user_mem = {"pending_invoice_client": "Nike", "pending_invoice_send_email": False}

        result = svc._handle_invoice_month_reply("u1", "April", user_mem, "u1_data", [])

        # Should proceed to invoice generation, not retry
        assert result["operation"] != "invoice_month_retry"
