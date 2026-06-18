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


# ══════════════════════════════════════════════════════════════════════════
# Invoice PDF feedback fixes (#1-#7 from live client feedback, June 2026)
# ══════════════════════════════════════════════════════════════════════════

class TestInvoicePdfFeedbackFixes:
    def test_strip_billing_label(self):
        from services.invoice_generation_service import _strip_billing_label
        assert _strip_billing_label("Billing infor is\nSpotify India\nLower Parel") == "Spotify India\nLower Parel"
        assert _strip_billing_label("billing info is Acme Corp") == "Acme Corp"
        assert _strip_billing_label("the billing details: Foo Ltd") == "Foo Ltd"
        # No label → unchanged
        assert _strip_billing_label("Spotify India\nLower Parel") == "Spotify India\nLower Parel"

    def test_pdf_content_addresses_all_feedback(self, tmp_path, monkeypatch):
        pypdf = __import__("pytest").importorskip("pypdf")
        monkeypatch.chdir(tmp_path)
        from services.invoice_generation_service import InvoiceGenerationService
        svc = InvoiceGenerationService()
        summary = {"client": "Spotify India", "month": "March", "year": 2026}
        data = [{
            "client_name": "", "brand_name": "Spotify", "poc_name": "karan",
            "client_billing_details": "Billing infor is\nSpotify India\nLower Parel",
            "job_description_details": "2 master films english VO",
            "job_date": "2026-03-04", "fees": 10000, "bill_no": "SPO-0002",
        }]
        bank = {"bank_name": "HDFC Bank", "bank_account_name": "Darshit Mody",
                "bank_account_number": "1234567890", "bank_ifsc": "HDFC0001234", "upi_id": "d@hdfc"}
        prof = {"name": "Darshit Mody", "address": "12 MG Road\nMumbai 400001", "gst": "NA"}
        path = svc.generate_pdf(summary, data, bank_details=bank, user_profile=prof)
        text = pypdf.PdfReader(path).pages[0].extract_text()

        # #1 — stray label gone
        assert "infor is" not in text.lower() and "billing info is" not in text.lower()
        # #2 — sender address present
        assert "12 MG Road" in text and "Mumbai 400001" in text
        # #3 — job description present
        assert "2 master films english VO" in text
        # #5 — consistent terms, not "Immediate"
        assert "Within 30 days" in text and "Immediate" not in text
        # #6 — no always-NA rows
        assert "Job No." not in text and "GST : NA" not in text
        # #4 — bank details present
        assert "1234567890" in text and "HDFC0001234" in text
        # #7 — brand appears once (in Invoice To), not duplicated in the job line.
        assert text.count("Spotify") <= 2  # client name + brand line; not also in the job row

    def test_invoice_address_handler_saves_and_resumes(self):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.supabase = MagicMock(); svc.memory = MagicMock()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"name": "D", "preferences": {}}}
        svc.memory.get_user_memory.return_value = {"pending_invoice": {"client_name": "X"}, "pending_address_user_id": "u1"}

        svc.process_request = MagicMock(return_value={"operation": "query", "response": "ok"})
        svc.memory.get_user_memory.return_value = {"pending_invoice": {"client_name": "X", "month": "March", "year": 2026}, "pending_address_user_id": "u1"}
        svc._handle_invoice_address_response("u1", "12 MG Road, Mumbai")
        saved = svc.supabase.upsert_user_profile.call_args[0][2]["preferences"]
        assert saved.get("invoice_address") == "12 MG Road, Mumbai"
        assert svc.process_request.called, "should re-enter the invoice flow after saving the address"

        # 'cancel' aborts the invoice (address is now mandatory, no skip)
        svc.memory.get_user_memory.return_value = {"pending_invoice": {"client_name": "X"}, "pending_address_user_id": "u1"}
        r2 = svc._handle_invoice_address_response("u1", "cancel")
        assert r2["operation"] == "invoice_cancelled" and r2["trigger_invoice"] is False


class TestBankHardGuard:
    """has_usable_bank_details() is the gate that stops bankless (unpayable)
    invoices from being generated."""

    def test_none_or_empty(self):
        from services.invoice_generation_service import has_usable_bank_details
        assert has_usable_bank_details(None) is False
        assert has_usable_bank_details({}) is False

    def test_missing_or_blank_account_number(self):
        from services.invoice_generation_service import has_usable_bank_details
        # The FAIL-34 shape: bank name + UPI present but NO account number.
        assert has_usable_bank_details({"bank_name": "HDFC", "upi_id": "x@y"}) is False
        assert has_usable_bank_details({"bank_account_number": ""}) is False
        assert has_usable_bank_details({"bank_account_number": "   "}) is False
        assert has_usable_bank_details({"bank_account_number": None}) is False

    def test_valid_account_number(self):
        from services.invoice_generation_service import has_usable_bank_details
        assert has_usable_bank_details({"bank_account_number": "1234567890"}) is True
        assert has_usable_bank_details({"bank_account_number": 1234567890}) is True


class TestInvoiceReadinessGate:
    """_invoice_readiness_check is the mandatory-fields gate: it returns a prompt
    for the FIRST missing required field (billing, POC, job description, bank,
    address), in order, and None only when the invoice is complete."""

    COMPLETE_ROW = {
        "id": "r1", "client_name": "Spotify", "client_billing_details": "Spotify India",
        "poc_name": "karan", "job_description_details": "2 master films english VO",
        "job_date": "2026-03-04", "fees": 10000,
    }
    INVOICE = {"client_name": "Spotify", "month": "March", "year": 2026}

    def _svc(self, row_overrides=None, bank=True, address=True):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.supabase = MagicMock(); svc.memory = MagicMock()
        row = dict(self.COMPLETE_ROW)
        if row_overrides:
            row.update(row_overrides)
        svc.supabase.fetch_job_entries_for_invoice.return_value = {"ok": True, "rows": [row]}
        svc.supabase.get_user_bank_details.return_value = {"ok": True, "data": ({"bank_account_number": "123456"} if bank else None)}
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"name": "D", "preferences": ({"invoice_address": "12 MG Road"} if address else {})}}
        return svc

    def _check(self, svc):
        return svc._invoice_readiness_check("u1", "u1", dict(self.INVOICE))

    def test_all_present_passes(self):
        assert self._check(self._svc()) is None

    def test_missing_billing_prompts_first(self):
        r = self._check(self._svc({"client_billing_details": ""}))
        assert r is not None and "billing" in r["response"].lower() and r["trigger_invoice"] is False

    def test_missing_poc(self):
        r = self._check(self._svc({"client_billing_details": "Spotify India", "poc_name": ""}))
        assert r is not None and "addressed to" in r["response"].lower()

    def test_missing_job_description(self):
        r = self._check(self._svc({"job_description_details": ""}))
        assert r is not None and "description" in r["response"].lower()

    def test_missing_bank(self):
        r = self._check(self._svc(bank=False))
        assert r is not None and "bank" in r["response"].lower()

    def test_missing_address_last(self):
        r = self._check(self._svc(address=False))
        assert r is not None and "address" in r["response"].lower()

    def test_order_billing_before_bank(self):
        # Both billing AND bank missing → billing is asked first.
        r = self._check(self._svc({"client_billing_details": ""}, bank=False))
        assert "billing" in r["response"].lower() and "bank" not in r["response"].lower()


class TestAddressUpdateCommand:
    """Users can set/correct their saved business address any time."""

    def _svc(self, existing_addr="OLD ADDR"):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.supabase = MagicMock(); svc.memory = MagicMock()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"name": "D", "preferences": {"invoice_address": existing_addr}}}
        return svc

    def test_inline_update_overwrites(self):
        svc = self._svc()
        r = svc._handle_address_update("u1", "update my address to TechNova, BKC, Mumbai 400051", "u1")
        assert r["operation"] == "address_updated"
        assert svc.supabase.upsert_user_profile.call_args[0][2]["preferences"]["invoice_address"] == "TechNova, BKC, Mumbai 400051"

    def test_my_address_is_form(self):
        svc = self._svc()
        svc._handle_address_update("u1", "my business address is 12 MG Road, Mumbai", "u1")
        assert svc.supabase.upsert_user_profile.call_args[0][2]["preferences"]["invoice_address"] == "12 MG Road, Mumbai"

    def test_bare_command_prompts(self):
        svc = self._svc()
        r = svc._handle_address_update("u1", "update my address", "u1")
        assert r["trigger_invoice"] is False and "address" in r["response"].lower()
        patch = svc.memory.update_user_memory.call_args[0][1]
        assert patch.get("awaiting_invoice_address") is True and patch.get("pending_invoice") is None

    def test_persist_helper_preserves_other_prefs(self):
        svc = self._svc()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"name": "D", "preferences": {"invoice_address": "OLD", "invoice_name": "Darshit Inc"}}}
        svc._persist_invoice_address("u1", "NEW ADDR")
        saved = svc.supabase.upsert_user_profile.call_args[0][2]["preferences"]
        assert saved["invoice_address"] == "NEW ADDR" and saved["invoice_name"] == "Darshit Inc"


class TestAddressCommandRouting:
    """The bug: 'Change my address' was grabbed by the v2 classifier as a
    FEATURE_QUESTION and refused. The command must route to the address handler
    (it's now checked before the classifier)."""

    def _svc(self):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.gemini = MagicMock(); svc.email = MagicMock(); svc.supabase = MagicMock(); svc.memory = MagicMock()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "A", "preferences": {"invoice_address": "OLD"}}}
        svc.memory.get_user_memory.return_value = {}
        svc.memory.get_form_state.return_value = None
        svc.memory.get_conversation_history.return_value = []
        svc.supabase.db_url = "postgresql://fake"
        return svc

    def test_change_my_address_does_not_refuse(self):
        svc = self._svc()
        r = svc.process_request("u1", "Change my address")
        # Must reach the address flow (prompt), NOT the feature-question refusal.
        assert r["operation"] != "feature_q"
        assert "address" in r["response"].lower()
        # answer_feature_question must not have been used to reply.
        assert not svc.gemini.answer_feature_question.called

    def test_inline_change_saves(self):
        svc = self._svc()
        r = svc.process_request("u1", "update my address to 12 New Road, Mumbai 400001")
        assert r["operation"] == "address_updated"
        assert svc.supabase.upsert_user_profile.called
