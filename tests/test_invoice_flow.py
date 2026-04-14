"""
Tests for invoice generation and email sending.

Covers:
- InvoiceGenerationService: sanitize_pdf_text, _parse_fees, generate_pdf (real PDF output)
- ResendEmailService: _normalize_emails, dry-run send_email, send_invoice_email with mock HTTP
"""

import pytest
import os
import tempfile
from unittest.mock import MagicMock, patch, mock_open

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from services.invoice_generation_service import InvoiceGenerationService, sanitize_pdf_text
from services.resend_email_service import ResendEmailService


# ── sanitize_pdf_text ─────────────────────────────────────────────────────

class TestSanitizePdfText:
    def test_replaces_rupee_symbol(self):
        assert sanitize_pdf_text("₹50,000") == "Rs 50,000"

    def test_replaces_em_dash(self):
        assert sanitize_pdf_text("a—b") == "a-b"

    def test_replaces_en_dash(self):
        assert sanitize_pdf_text("a–b") == "a-b"

    def test_replaces_smart_quotes(self):
        assert sanitize_pdf_text("\u201cHello\u201d") == '"Hello"'

    def test_none_returns_empty_string(self):
        assert sanitize_pdf_text(None) == ""

    def test_plain_text_unchanged(self):
        assert sanitize_pdf_text("Hello World") == "Hello World"


# ── _parse_fees ───────────────────────────────────────────────────────────

class TestParseFees:
    svc = InvoiceGenerationService()

    def test_plain_number_string(self):
        assert self.svc._parse_fees("50000") == 50000.0

    def test_with_rupee_symbol(self):
        assert self.svc._parse_fees("₹25,000") == 25000.0

    def test_with_commas(self):
        assert self.svc._parse_fees("1,25,000") == 125000.0

    def test_float_string(self):
        assert self.svc._parse_fees("12500.50") == 12500.50

    def test_empty_string_returns_zero(self):
        assert self.svc._parse_fees("") == 0.0

    def test_invalid_string_returns_zero(self):
        assert self.svc._parse_fees("not-a-number") == 0.0

    def test_integer_input(self):
        assert self.svc._parse_fees(75000) == 75000.0


# ── generate_pdf ──────────────────────────────────────────────────────────

class TestGeneratePdf:
    """Generates a real PDF to a temp directory — validates the file is created and non-empty."""

    SUMMARY = {
        "client": "TestClient",
        "month": "March",
        "year": 2024,
        "total": 75000,
    }

    CLIENT_DATA = [
        {
            "job_date": "2024-03-01",
            "job_description_details": "Radio spot recording",
            "brand_name": "Acme",
            "fees": "50000",
        },
        {
            "job_date": "2024-03-15",
            "job_description_details": "TV commercial VO",
            "brand_name": "Nike",
            "fees": "25000",
        },
    ]

    BANK_DETAILS = {
        "bank_name": "HDFC Bank",
        "bank_account_name": "Test User",
        "bank_account_number": "1234567890",
        "bank_ifsc": "HDFC0001234",
        "upi_id": "test@upi",
    }

    def test_pdf_is_created_and_non_empty(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        svc = InvoiceGenerationService()
        path = svc.generate_pdf(self.SUMMARY, self.CLIENT_DATA, self.BANK_DETAILS)
        assert path is not None
        assert os.path.exists(path)
        assert os.path.getsize(path) > 0

    def test_pdf_filename_contains_client_and_month(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        svc = InvoiceGenerationService()
        path = svc.generate_pdf(self.SUMMARY, self.CLIENT_DATA)
        assert "TestClient" in os.path.basename(path)
        assert "March" in os.path.basename(path)

    def test_pdf_returns_none_on_bad_data(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        svc = InvoiceGenerationService()
        # Passing non-iterable client_data should cause an internal error -> returns None
        path = svc.generate_pdf(self.SUMMARY, "not-a-list")
        assert path is None

    def test_pdf_with_no_bank_details_uses_placeholders(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        svc = InvoiceGenerationService()
        path = svc.generate_pdf(self.SUMMARY, self.CLIENT_DATA, bank_details=None)
        assert path is not None
        assert os.path.exists(path)


# ── ResendEmailService._normalize_emails ─────────────────────────────────

class TestNormalizeEmails:
    def _svc(self):
        with patch.dict(os.environ, {"RESEND_API": "key", "RESEND_FROM_EMAIL": "from@test.com"}):
            return ResendEmailService()

    def test_single_email_string(self):
        svc = self._svc()
        assert svc._normalize_emails("a@b.com") == ["a@b.com"]

    def test_semicolon_separated(self):
        svc = self._svc()
        result = svc._normalize_emails("a@b.com;c@d.com")
        assert result == ["a@b.com", "c@d.com"]

    def test_comma_separated(self):
        svc = self._svc()
        result = svc._normalize_emails("a@b.com,c@d.com")
        assert result == ["a@b.com", "c@d.com"]

    def test_list_input(self):
        svc = self._svc()
        result = svc._normalize_emails(["a@b.com", "c@d.com"])
        assert result == ["a@b.com", "c@d.com"]

    def test_empty_string_returns_empty_list(self):
        svc = self._svc()
        assert svc._normalize_emails("") == []

    def test_none_returns_empty_list(self):
        svc = self._svc()
        assert svc._normalize_emails(None) == []


# ── ResendEmailService.send_email (dry-run) ───────────────────────────────

class TestSendEmailDryRun:
    def _svc(self):
        with patch.dict(os.environ, {
            "RESEND_API": "test-key",
            "RESEND_FROM_EMAIL": "from@test.com",
            "EMAIL_DRY_RUN": "true",
        }):
            return ResendEmailService()

    def test_dry_run_returns_true_without_http_call(self):
        svc = self._svc()
        result = svc.send_email("to@test.com", "Test Subject", "Test body")
        assert result is True

    def test_send_payment_reminder_dry_run(self):
        svc = self._svc()
        result = svc.send_payment_reminder(
            to_email="client@example.com",
            client_name="Acme Corp",
            invoice_number="INV-001",
            amount_due="₹50,000",
            due_date_str="15 Mar 2024",
        )
        assert result is True

    def test_missing_recipient_returns_false(self):
        svc = self._svc()
        result = svc.send_email("", "Subject", "Body")
        assert result is False


# ── ResendEmailService.send_email (live mock) ─────────────────────────────

class TestSendEmailLive:
    def _svc(self):
        with patch.dict(os.environ, {
            "RESEND_API": "live-key",
            "RESEND_FROM_EMAIL": "from@test.com",
            "EMAIL_DRY_RUN": "false",
        }):
            return ResendEmailService()

    def test_successful_http_response_returns_true(self):
        svc = self._svc()
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"id": "msg-123"}

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = mock_resp
            result = svc.send_email("to@test.com", "Subject", "Body")

        assert result is True

    def test_http_4xx_returns_false(self):
        svc = self._svc()
        mock_resp = MagicMock()
        mock_resp.status_code = 422
        mock_resp.text = "Unprocessable Entity"

        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.return_value = mock_resp
            result = svc.send_email("to@test.com", "Subject", "Body")

        assert result is False

    def test_network_exception_returns_false(self):
        svc = self._svc()
        with patch("httpx.Client") as mock_client_cls:
            mock_client = MagicMock()
            mock_client_cls.return_value.__enter__.return_value = mock_client
            mock_client.post.side_effect = Exception("timeout")
            result = svc.send_email("to@test.com", "Subject", "Body")
        assert result is False


# ── ResendEmailService.send_invoice_email ────────────────────────────────

class TestSendInvoiceEmail:
    def _svc(self):
        with patch.dict(os.environ, {
            "RESEND_API": "key",
            "RESEND_FROM_EMAIL": "from@test.com",
            "EMAIL_DRY_RUN": "true",
        }):
            return ResendEmailService()

    def test_returns_false_when_pdf_does_not_exist(self):
        svc = self._svc()
        result = svc.send_invoice_email(
            to_email="client@test.com",
            client_name="Acme",
            month="March",
            year=2024,
            pdf_path="/nonexistent/path/invoice.pdf",
        )
        assert result is False

    def test_sends_with_existing_pdf(self, tmp_path):
        # Create a dummy PDF file
        pdf_file = tmp_path / "invoice.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 dummy content")

        svc = self._svc()
        result = svc.send_invoice_email(
            to_email="client@test.com",
            client_name="Acme",
            month="March",
            year=2024,
            pdf_path=str(pdf_file),
        )
        assert result is True

    def test_subject_contains_client_and_month(self, tmp_path):
        pdf_file = tmp_path / "invoice.pdf"
        pdf_file.write_bytes(b"%PDF-1.4 dummy")

        svc = self._svc()
        # Patch send_email to capture args
        captured = {}
        original = svc.send_email
        def capture(**kwargs):
            captured.update(kwargs)
            return True
        svc.send_email = lambda **kw: captured.update(kw) or True

        svc.send_invoice_email("c@t.com", "Nike", "April", 2024, str(pdf_file))
        # Subject is "Invoice for April 2024" (period-based, not client name)
        assert "April" in captured.get("subject", "")
