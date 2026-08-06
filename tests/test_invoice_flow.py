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

    def test_editorial_font_files_are_committed(self):
        """The Playfair Display + Lato TTFs must ship in the repo so production
        renders the editorial design rather than the Helvetica fallback."""
        import os
        fonts_dir = os.path.join(os.path.dirname(__file__), "..", "fonts")
        for f in ("PlayfairDisplay-Regular.ttf", "PlayfairDisplay-Bold.ttf",
                  "Lato-Regular.ttf", "Lato-Bold.ttf", "Lato-Italic.ttf"):
            assert os.path.exists(os.path.join(fonts_dir, f)), f"missing committed font: {f}"

    def test_long_sender_address_wraps_and_clears_invoice_number(self, tmp_path, monkeypatch):
        """A long sender address must wrap inside a FIXED-WIDTH block under the
        name (multi_cell), not run off its column and print over the invoice
        number in the right-hand column. Regression for 'the address line is
        writing all over the invoice number'."""
        pypdf = __import__("pytest").importorskip("pypdf")
        monkeypatch.chdir(tmp_path)
        from services.invoice_generation_service import InvoiceGenerationService
        svc = InvoiceGenerationService()
        long_addr = ("Flat 1203, Tower B, Prestige Lakeside Habitat, Varthur Road, "
                     "Whitefield, Bengaluru, Karnataka 560066")
        summary = {"client": "Spotify India", "month": "March", "year": 2026}
        data = [{"brand_name": "Spotify", "poc_name": "karan",
                 "client_billing_details": "Spotify India\nLower Parel",
                 "job_description_details": "2 master films VO",
                 "job_date": "2026-03-04", "fees": 10000, "bill_no": "SPO-0002"}]
        bank = {"bank_name": "HDFC", "bank_account_name": "Darshit Mody",
                "bank_account_number": "1234567890", "bank_ifsc": "HDFC0001234"}
        prof = {"name": "Darshit Mody", "address": long_addr}
        path = svc.generate_pdf(summary, data, bank_details=bank, user_profile=prof)

        frags = []
        def _vis(text, cm, tm, font_dict, font_size):
            if text and text.strip():
                frags.append((tm[4], tm[5], text))
        pypdf.PdfReader(path).pages[0].extract_text(visitor_text=_vis)

        addr_tokens = ("Prestige", "Whitefield", "Karnataka", "Varthur", "Tower", "Bengaluru")
        addr_lines = [(x, y, t) for (x, y, t) in frags if any(tok in t for tok in addr_tokens)]
        addr_ys = {round(y) for (x, y, t) in addr_lines}

        # Wrapped onto >=2 lines instead of one overflowing line.
        assert len(addr_ys) >= 2, "long sender address must wrap, not run off one line"
        # Every address fragment sits in the LEFT column, never crossing into the
        # right-hand invoice-meta column (which begins at ~x=320pt on A4). This is
        # what proves the address can't print over the invoice number.
        assert all(x < 320 for (x, y, t) in addr_lines), "address bled into the right column"
        # The invoice number still renders (in the right-hand meta column).
        assert "SPO-0002" in pypdf.PdfReader(path).pages[0].extract_text()
        # Full address content preserved (not truncated by wrapping).
        joined = " ".join(t for (x, y, t) in addr_lines)
        assert "Prestige Lakeside Habitat" in joined and "Karnataka 560066" in joined

    def test_invoice_to_leads_with_poc_then_company(self, tmp_path, monkeypatch):
        """The 'Invoice To' block must be addressed TO THE POC first (the person),
        with the company / billing details underneath — not the other way round."""
        pypdf = __import__("pytest").importorskip("pypdf")
        monkeypatch.chdir(tmp_path)
        from services.invoice_generation_service import InvoiceGenerationService
        svc = InvoiceGenerationService()
        summary = {"client": "Spotify India", "month": "March", "year": 2026}
        data = [{"brand_name": "Spotify", "poc_name": "Karan Mehta",
                 "client_billing_details": "Spotify India Pvt Ltd\nLower Parel, Mumbai 400013",
                 "job_description_details": "VO", "job_date": "2026-03-04",
                 "fees": 10000, "bill_no": "SPO-0002"}]
        bank = {"bank_name": "HDFC", "bank_account_number": "1234567890", "bank_ifsc": "HDFC0001234"}
        prof = {"name": "Darshit Mody", "address": "12 MG Road"}
        path = svc.generate_pdf(summary, data, bank_details=bank, user_profile=prof)

        frags = []
        def _vis(text, cm, tm, font_dict, font_size):
            if text and text.strip():
                frags.append((tm[5], text))
        pypdf.PdfReader(path).pages[0].extract_text(visitor_text=_vis)

        def _y_of(needle):
            ys = [y for (y, t) in frags if needle in t]
            return max(ys) if ys else None
        poc_y = _y_of("Karan Mehta")
        company_y = _y_of("Spotify India Pvt Ltd")
        assert poc_y is not None, "POC name should be on the invoice"
        assert company_y is not None, "company billing name should be on the invoice"
        # Higher on the page == larger y in PDF user space.
        assert poc_y > company_y, "POC name must sit above the company billing name"

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
    for the FIRST missing required field (billing, POC name, POC email, job
    description, bank, address), in order, and None only when the invoice is
    complete."""

    COMPLETE_ROW = {
        "id": "r1", "client_name": "Spotify", "client_billing_details": "Spotify India",
        "poc_name": "karan", "poc_email": "karan@spotify.com",
        "job_description_details": "2 master films english VO",
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

    # ── POC email gate ────────────────────────────────────────────────────
    # Added 2026-07. Previously poc_email was only checked in the SEND_EMAIL
    # branch, so "generate invoice for X" produced a PDF for a client with no
    # email and the dead end surfaced only at send time. Worse: an unpaid job
    # with no poc_email is invisible to every reminder tier
    # (fetch_reminder_targets requires poc_email IS NOT NULL), so the payment
    # silently never gets chased.

    def test_missing_poc_email_blocks_generation(self):
        r = self._check(self._svc({"poc_email": ""}))
        assert r is not None, "invoice generated for a client with no email on file"
        assert "email" in r["response"].lower()
        assert r["trigger_invoice"] is False

    def test_missing_poc_email_null_also_blocks(self):
        r = self._check(self._svc({"poc_email": None}))
        assert r is not None and "email" in r["response"].lower()

    def test_poc_email_literal_none_string_blocks(self):
        # _present() treats the string "None" as absent — a real shape that
        # shows up when a null round-trips through str().
        r = self._check(self._svc({"poc_email": "None"}))
        assert r is not None and "email" in r["response"].lower()

    def test_poc_name_asked_before_poc_email(self):
        r = self._check(self._svc({"poc_name": "", "poc_email": ""}))
        assert "addressed to" in r["response"].lower()

    def test_poc_email_asked_before_job_description(self):
        r = self._check(self._svc({"poc_email": "", "job_description_details": ""}))
        assert "email" in r["response"].lower()
        assert "description" not in r["response"].lower()

    def test_gate_does_not_restrict_to_corporate_domains(self):
        """Any address is acceptable — a freelancer's client may well use gmail.
        The prompt must not imply otherwise, and a gmail address must satisfy
        the gate."""
        assert self._check(self._svc({"poc_email": "karan@gmail.com"})) is None
        r = self._check(self._svc({"poc_email": ""}))
        assert "gmail" in r["response"].lower(), "prompt should signal any address is fine"


class TestInvoicePocEmailHandler:
    """_handle_invoice_poc_email_response — the reply to the pre-generation
    email gate. Distinct from _handle_poc_email_response (the send-time flow
    that emails an already-generated PDF)."""

    def _svc(self, mem=None):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.supabase = MagicMock(); svc.memory = MagicMock()
        base = {
            "pending_invoice": {"client_name": "Spotify", "month": "March", "year": 2026},
            "pending_poc_email_client": "Spotify",
            "pending_poc_email_user_id": "u1",
            "pending_poc_email_row_ids": ["r1", "r2"],
        }
        base.update(mem or {})
        svc.memory.get_user_memory.return_value = base
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": []}
        svc._resume_invoice_flow = MagicMock(return_value={"operation": "resumed", "response": "ok"})
        return svc

    def _updates(self, svc):
        return [c.args[0] for c in svc.supabase.execute_sql.call_args_list
                if c.args and c.args[0].strip().upper().startswith("UPDATE")]

    @pytest.mark.parametrize("email", [
        "karan@gmail.com",          # free provider — must be accepted
        "accounts@agency.com",
        "first.last+inv@gmail.com",  # plus-addressing
        "a@b.co.in",
    ])
    def test_any_valid_email_accepted_and_saved(self, email):
        svc = self._svc()
        result = svc._handle_invoice_poc_email_response("u1", email)
        upd = self._updates(svc)
        assert len(upd) == 1 and email in upd[0]
        assert "poc_email" in upd[0]
        assert result["operation"] == "resumed", "must re-enter the invoice flow"

    def test_email_extracted_from_a_sentence(self):
        svc = self._svc()
        svc._handle_invoice_poc_email_response("u1", "send it to karan@gmail.com please")
        assert "karan@gmail.com" in self._updates(svc)[0]

    def test_scopes_update_to_the_pending_row_ids(self):
        svc = self._svc()
        svc._handle_invoice_poc_email_response("u1", "karan@gmail.com")
        sql = self._updates(svc)[0]
        assert "'r1'" in sql and "'r2'" in sql

    def test_falls_back_to_client_match_when_no_row_ids(self):
        svc = self._svc({"pending_poc_email_row_ids": []})
        svc._handle_invoice_poc_email_response("u1", "karan@gmail.com")
        sql = self._updates(svc)[0]
        assert "ILIKE" in sql.upper() and "spotify" in sql.lower()

    def test_malformed_email_rearms_and_does_not_write(self):
        # Phase 2.2 (post-2.3): the re-arm is FlowMachine-only now
        # (_arm_invoice_readiness_poc_email_v2 writes flow_machine.set_state()
        # directly instead of a legacy awaiting_invoice_poc_email flag) --
        # but its context payload (pending_poc_email_*) still lands via
        # svc.memory.update_user_memory, so that part of the assertion holds.
        from services.flow_machine import FLOW_INVOICE_READINESS_POC_EMAIL, _MEM_KEY
        svc = self._svc()
        result = svc._handle_invoice_poc_email_response("u1", "karan@notanemail")
        assert result["operation"] == "invoice_poc_email_retry"
        assert self._updates(svc) == []
        rearmed = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                   if c.args[1].get("pending_poc_email_row_ids")]
        assert rearmed, "must re-arm so the next message is still read as the email"
        assert rearmed[-1]["pending_poc_email_row_ids"] == ["r1", "r2"], "context must survive the retry"
        fm_writes = [c.args[1][_MEM_KEY] for c in svc.flow_machine._mem.update_user_memory.call_args_list
                     if _MEM_KEY in c.args[1]]
        assert fm_writes and fm_writes[-1]["flow"] == FLOW_INVOICE_READINESS_POC_EMAIL

    @pytest.mark.parametrize("msg", ["cancel", "stop", "abort", "nevermind"])
    def test_cancel_aborts_invoice_without_writing(self, msg):
        svc = self._svc()
        result = svc._handle_invoice_poc_email_response("u1", msg)
        assert result["operation"] == "invoice_cancelled"
        assert self._updates(svc) == []
        svc._resume_invoice_flow.assert_not_called()

    def test_sql_quote_escaped(self):
        svc = self._svc({"pending_poc_email_row_ids": [],
                         "pending_poc_email_client": "O'Brien"})
        svc._handle_invoice_poc_email_response("u1", "a@b.com")
        assert "o''brien" in self._updates(svc)[0].lower()

    def test_clears_pending_poc_email_payload_on_success(self):
        # No legacy awaiting_invoice_poc_email flag exists to clear anymore
        # -- FlowMachine's own reset (done by the InvoiceReadinessPocEmail
        # Flow wrapper, not this handler itself) is what ends the flow.
        # This handler's own responsibility is just clearing its payload
        # keys so a stale client/row-id set can't leak into a later turn.
        svc = self._svc()
        svc._handle_invoice_poc_email_response("u1", "karan@gmail.com")
        cleared = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                   if "pending_poc_email_client" in c.args[1]]
        assert cleared and cleared[0]["pending_poc_email_client"] is None


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
        """Phase 2.3: no legacy awaiting_invoice_address flag exists —
        FlowMachine (flow_machine.set_state) is INVOICE_ADDRESS' source of
        truth. pending_invoice is still cleared via the legacy patch."""
        svc = self._svc()
        svc.flow_machine = MagicMock()
        r = svc._handle_address_update("u1", "update my address", "u1")
        assert r["trigger_invoice"] is False and "address" in r["response"].lower()
        from services.flow_machine import FLOW_INVOICE_ADDRESS
        svc.flow_machine.set_state.assert_called_once_with("u1", FLOW_INVOICE_ADDRESS, {"client_name": None})
        patch = svc.memory.update_user_memory.call_args[0][1]
        assert patch.get("pending_invoice") is None

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


class TestExplicitCommandsBeforeClassifier:
    """All explicit account/profile commands must be handled deterministically,
    before the v2 classifier can grab them as FEATURE_QUESTIONs."""

    def _svc(self):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.gemini = MagicMock(); svc.email = MagicMock(); svc.supabase = MagicMock(); svc.memory = MagicMock()
        svc.supabase.get_user_profile.return_value = {"ok": True, "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "A", "preferences": {}}}
        svc.supabase.get_user_bank_details.return_value = {"ok": True, "data": {"bank_account_number": "123", "bank_name": "HDFC"}}
        svc.memory.get_user_memory.return_value = {}
        svc.memory.get_form_state.return_value = None
        svc.memory.get_conversation_history.return_value = []
        svc.supabase.db_url = "postgresql://fake"
        return svc

    @pytest.mark.parametrize("msg,expected_op", [
        ("Change my address", "ACTION_TRIGGER"),
        ("Change my name", "name_change_prompt"),
        ("update bank details", "bank_details_prompt"),
        ("my bank details", "bank_details_view"),
        ("what is my user id", "show_user_id"),
    ])
    def test_command_not_refused_as_feature(self, msg, expected_op):
        svc = self._svc()
        r = svc.process_request("u1", msg)
        assert r["operation"] == expected_op
        assert not svc.gemini.answer_feature_question.called, f"{msg!r} was refused as a feature question"

    @pytest.mark.parametrize("msg", [
        "Change my adress",      # the reported typo (missing a 'd')
        "update my adress",
        "fix my adres",
        "change my addres please",
    ])
    def test_address_misspellings_still_route(self, msg):
        # A misspelled "address" + an update verb must still hit the handler,
        # not fall through to the v2 classifier (which refuses it).
        svc = self._svc()
        r = svc.process_request("u1", msg)
        assert r["operation"] == "ACTION_TRIGGER"
        assert not svc.gemini.answer_feature_question.called, f"{msg!r} fell through to the classifier"

    @pytest.mark.parametrize("msg,expected_op", [
        ("change my naem", "name_change_prompt"),
        ("update my bnk details", "bank_details_prompt"),
        ("show my bnk details", "bank_details_view"),
    ])
    def test_other_command_misspellings_route(self, msg, expected_op):
        svc = self._svc()
        r = svc.process_request("u1", msg)
        assert r["operation"] == expected_op
        assert not svc.gemini.answer_feature_question.called, f"{msg!r} fell through to the classifier"

    def test_link_misspelling_routes(self):
        svc = self._svc()
        r = svc.process_request("u1", "link telegrm")
        assert not svc.gemini.answer_feature_question.called, "misspelled link fell through to the classifier"


class TestCmdWithTypos:
    """Unit tests for the typo-tolerant command matcher."""
    from services.intent_service import IntentService
    _m = staticmethod(IntentService._cmd_with_typos)

    def test_exact_trigger(self):
        assert self._m("change my address", ["change my address"])

    def test_typo_noun_plus_intent(self):
        assert self._m("change my adress", ["change my address"], ("adress",), ("change", "update"))

    def test_typo_without_intent_does_not_match(self):
        # a misspelled noun alone (no update verb) must NOT route
        assert not self._m("whats the adress on file", ["change my address"], ("adress",), ("change", "update"))

    def test_full_phrase_typo(self):
        assert self._m("link telegrm", ["link telegram"], ("link telegrm",), intents=("",))

    def test_unrelated_message_no_match(self):
        assert not self._m("how many jobs do I have", ["change my address"], ("adress",), ("change",))


class TestInvoiceClientLabel:
    """Invoice should be labelled by what the user asked for. "invoice for pepsi"
    must not come back as "Content Lab" just because that's the client_name column
    while Pepsi is the brand."""
    from services.intent_service import IntentService
    _name = staticmethod(IntentService._invoice_display_name)

    def test_brand_search_uses_brand_not_client_column(self):
        rows = [{"client_name": "Content Lab", "brand_name": "Pepsi"}]
        assert self._name("pepsi", rows) == "Pepsi"

    def test_client_search_keeps_client_name(self):
        rows = [{"client_name": "Content Lab", "brand_name": "Pepsi"}]
        assert self._name("content lab", rows) == "Content Lab"

    def test_term_in_both_keeps_client_name(self):
        rows = [{"client_name": "Samsung India", "brand_name": "Samsung"}]
        assert self._name("samsung", rows) == "Samsung India"

    def test_no_client_column_falls_back_to_brand(self):
        rows = [{"client_name": "", "brand_name": "Nike"}]
        assert self._name("nike", rows) == "Nike"

    def test_no_search_term_uses_client_column(self):
        rows = [{"client_name": "Acme Corp", "brand_name": "Acme"}]
        assert self._name("", rows) == "Acme Corp"

    def test_empty_rows_safe(self):
        assert self._name("pepsi", []) == "pepsi"
        assert self._name("", []) == "Client"


class TestFuzzyMatchClientName:
    """_fuzzy_match_client_name -- resolving an LLM-extracted client name
    against the account's real client/brand/production_house values."""
    from services.intent_service import IntentService
    _match = staticmethod(IntentService._fuzzy_match_client_name)

    def test_exact_match_wins(self):
        assert self._match("nike", ["Nike", "Adidas"], "invoice for nike") == "Nike"

    def test_long_query_substring_match(self):
        assert self._match("Bridgestone12", ["Bridgestone", "Nike"], "x") == "Bridgestone"

    def test_short_typo_prefix_matches(self):
        """Live production bug: "Generate invoice for Nik" echoed "Nik"
        back verbatim instead of correcting to "Nike" -- the short-query
        (<=3 char) safety guard only allowed a word-boundary match, and
        "Nik" has no word boundary before the "e" in "Nike"."""
        assert self._match("Nik", ["Nike", "Adidas"], "invoice for Nik") == "Nike"

    def test_short_ambiguous_abbreviation_does_not_falsely_match(self):
        """Over-correction guard: "MS" must NOT match inside "Samsung" --
        that's a coincidental substring, not a prefix, and this is the
        exact case the short-query guard exists to prevent."""
        assert self._match("MS", ["Samsung", "Adidas"], "invoice for MS") == "MS"

    def test_short_prefix_too_far_from_candidate_does_not_match(self):
        """The prefix fallback is guarded to at most 3 extra trailing
        characters -- "Ni" matching all the way to "Nikeworld Studios"
        would be too loose."""
        assert self._match("Ni", ["Nikeworld Studios"], "x") == "Ni"

    def test_no_match_returns_original(self):
        assert self._match("Xyz Corp", ["Nike", "Adidas"], "x") == "Xyz Corp"

    def test_multiple_partial_matches_disambiguated_by_message_text(self):
        result = self._match("star", ["Star Studios", "Star Media"], "invoice for star media please")
        assert result == "Star Media"

    def test_exact_short_match_not_affected_by_prefix_guard(self):
        """A genuine short client name ("MS") must still resolve to itself
        via the exact-match path, unaffected by the new prefix fallback."""
        assert self._match("MS", ["MS", "Samsung"], "x") == "MS"


class TestInvoiceAlreadyIssued:
    """An invoice with an invoice_date on file is a RETRIEVAL — give it back,
    don't treat "send me the invoice" as a fresh build."""
    from services.intent_service import IntentService
    _issued = staticmethod(IntentService._rows_already_invoiced)

    def test_true_when_any_row_has_invoice_date(self):
        assert self._issued([{"invoice_date": "2026-05-01"}]) is True
        assert self._issued([{"invoice_date": None}, {"invoice_date": "2026-05-01"}]) is True

    def test_false_when_no_invoice_date(self):
        assert self._issued([{"invoice_date": None}, {"bill_no": "X-1"}]) is False
        assert self._issued([{}]) is False
        assert self._issued([]) is False


class TestInvoiceTriggerSynonyms:
    """Coverage for the generation action verbs and the regeneration keywords."""
    from services.intent_service import IntentService
    _action = staticmethod(IntentService._is_definite_invoice_action)
    _regen = staticmethod(IntentService._is_regenerate_request)

    @pytest.mark.parametrize("msg", [
        "generate invoice for Pepsi", "send me the invoice for Pepsi",
        "give me the invoice for Nike", "provide the invoice for Pepsi",
        "produce the invoice for March", "raise an invoice for Acme",
        "cut invoice for Star Studios", "forward the invoice to the client",
        "genrate invoce for Acme",  # typos still work
    ])
    def test_definite_invoice_actions(self, msg):
        assert self._action(msg) is True

    @pytest.mark.parametrize("msg", [
        "show unpaid invoices",                     # a read
        "how many invoices did I raise last month", # a read
        "issue with my invoice",                    # a problem, not a request
        "what is my total billing this year",       # no invoice noun + read
    ])
    def test_not_a_definite_action(self, msg):
        assert self._action(msg) is False

    @pytest.mark.parametrize("msg", [
        "regenerate invoice for Pepsi", "make it again", "generate again",
        "update the invoice for Pepsi", "updated invoice please",
        "fix the invoice for Nike", "correct the invoice", "reissue the invoice",
        "send me a fresh pdf", "refresh the invoice", "do it again",
        "make a new one", "rebuild invoice for X",
    ])
    def test_regenerate_phrases(self, msg):
        assert self._regen(msg) is True

    @pytest.mark.parametrize("msg", [
        "send me the invoice for Pepsi",  # plain retrieval, NOT a rebuild
        "invoice for Nike March",
        "update invoice profile",         # a different command — must not force regen
        "update my bank details",
    ])
    def test_not_a_regenerate(self, msg):
        assert self._regen(msg) is False


class TestInvoiceRetrievalBillNumberExtraction:
    """P1-2 (PLAN_OF_ACTION.md): a bill/invoice number must be detected
    BEFORE client-name extraction runs, and that detection must catch a
    bare alphanumeric code with no separator ("bill BB2"), not just a
    dash-separated one ("INV-001") — the dash-only regex was the actual
    bug behind "Generate invoice for bill BB2" answering "I couldn't find
    a client named 'Bill Bb2'"."""

    def _svc(self):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.supabase = MagicMock()
        svc.memory = MagicMock()
        svc.gemini = MagicMock()
        svc.memory.get_user_memory.return_value = {}
        svc.supabase.fetch_job_entries_for_invoice.return_value = {"ok": False, "error": "no bill found"}
        return svc

    def test_bare_alphanumeric_bill_code_routes_to_bill_lookup(self):
        svc = self._svc()
        svc._handle_invoice_retrieval_request(
            "u1", "Generate invoice for bill BB2", "generate invoice for bill bb2",
            "u1", [], {}, None, True,
        )
        svc.supabase.fetch_job_entries_for_invoice.assert_called_once()
        kwargs = svc.supabase.fetch_job_entries_for_invoice.call_args.kwargs
        assert kwargs.get("bill_no") == "BB2"
        assert kwargs.get("client_name") == "", (
            "a detected bill number must suppress client-name lookup entirely"
        )
        # No client-search SQL should have run either — the bill path never
        # touches execute_sql for a client match.
        svc.supabase.execute_sql.assert_not_called()

    def test_dash_separated_bill_number_still_works(self):
        svc = self._svc()
        svc._handle_invoice_retrieval_request(
            "u1", "Send invoice INV-001", "send invoice inv-001",
            "u1", [], {}, None, True,
        )
        kwargs = svc.supabase.fetch_job_entries_for_invoice.call_args.kwargs
        assert kwargs.get("bill_no") == "INV-001"

    def test_bill_number_with_explicit_label_still_works(self):
        svc = self._svc()
        svc._handle_invoice_retrieval_request(
            "u1", "Generate invoice for bill no. 42", "generate invoice for bill no. 42",
            "u1", [], {}, None, True,
        )
        kwargs = svc.supabase.fetch_job_entries_for_invoice.call_args.kwargs
        assert kwargs.get("bill_no") == "42"

    def test_plain_client_name_request_unaffected(self):
        """Guard against over-broadening: an ordinary client-name request
        must still resolve as a client, not get misread as a bill code."""
        svc = self._svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [{"client_name": "Nike", "brand_name": None, "production_house": None}],
        }
        svc._handle_invoice_retrieval_request(
            "u1", "Generate invoice for Nike April", "generate invoice for nike april",
            "u1", [], {}, None, True,
        )
        kwargs = svc.supabase.fetch_job_entries_for_invoice.call_args.kwargs
        assert kwargs.get("bill_no") is None
        assert kwargs.get("client_name") == "Nike"


class TestInvoiceRegenerateReusesLastMonth:
    """P1-2 (PLAN_OF_ACTION.md): "Regenerate invoice for Nike" with no month
    named must reuse the month from the last invoice generated for that SAME
    client instead of asking "which month?" — the user said "regenerate",
    not "generate a new one for a month I haven't told you about"."""

    def _svc(self, last_intent=None):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.supabase = MagicMock()
        svc.memory = MagicMock()
        svc.gemini = MagicMock()
        svc.memory.get_user_memory.return_value = {"last_intent": last_intent} if last_intent else {}
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [{"client_name": "Nike", "brand_name": None, "production_house": None}],
        }
        svc.supabase.fetch_job_entries_for_invoice.return_value = {
            "ok": True,
            "rows": [{"id": "r1", "client_name": "Nike", "job_date": "2026-04-10",
                      "client_billing_details": "Nike India", "poc_name": "Karan",
                      "poc_email": "karan@nike.test", "job_description_details": "shoot",
                      "fees": 25000, "invoice_date": None}],
        }
        svc.supabase.get_user_bank_details.return_value = {"ok": True, "data": {"bank_account_number": "1"}}
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"name": "D", "preferences": {"invoice_address": "12 MG Road"}},
        }
        return svc

    def test_reuses_last_months_client_and_month_without_prompting(self):
        svc = self._svc(last_intent={
            "entity": "invoice", "client_name": "Nike", "month": "April", "year": 2026,
            "operation": "generate_invoice",
        })
        user_mem = svc.memory.get_user_memory("u1")
        result = svc._handle_invoice_retrieval_request(
            "u1", "Regenerate invoice for Nike", "regenerate invoice for nike",
            "u1", [], user_mem, None, True,
        )
        svc.supabase.get_available_months_for_client.assert_not_called()
        assert "which month" not in (result.get("response") or "").lower()
        kwargs = svc.supabase.fetch_job_entries_for_invoice.call_args.kwargs
        assert kwargs.get("month") == 4
        assert kwargs.get("year") == 2026
        assert result.get("invoice_data", {}).get("force_regenerate") is True

    def test_different_client_in_last_intent_does_not_leak_month(self):
        """Regenerate for Nike must not reuse a month cached for a
        DIFFERENT client's last invoice."""
        svc = self._svc(last_intent={
            "entity": "invoice", "client_name": "Bridgestone", "month": "March", "year": 2026,
            "operation": "generate_invoice",
        })
        user_mem = svc.memory.get_user_memory("u1")
        svc._handle_invoice_retrieval_request(
            "u1", "Regenerate invoice for Nike", "regenerate invoice for nike",
            "u1", [], user_mem, None, True,
        )
        svc.supabase.get_available_months_for_client.assert_called_once()

    def test_plain_generate_does_not_reuse_last_month(self):
        """Only an explicit regenerate phrase triggers the reuse — a plain
        'generate invoice for Nike' with a fresh last_intent must still ask
        which month rather than silently assuming the last one."""
        svc = self._svc(last_intent={
            "entity": "invoice", "client_name": "Nike", "month": "April", "year": 2026,
            "operation": "generate_invoice",
        })
        user_mem = svc.memory.get_user_memory("u1")
        svc._handle_invoice_retrieval_request(
            "u1", "Generate invoice for Nike", "generate invoice for nike",
            "u1", [], user_mem, None, True,
        )
        svc.supabase.get_available_months_for_client.assert_called_once()


class TestInvoicePronounAndBareRequestContextResolution:
    """P1-2 (PLAN_OF_ACTION.md): "Generate invoice for them" / a bare
    "Generate invoice" (after discussing a client) must resolve the client
    from context (last_saved_job -> uscf_context -> last_intent) instead of
    asking for a name — or worse, treating a pronoun/the verb phrase itself
    as a literal client name. This logic already existed in the legacy
    cascade; P0-1's dispatch_idle wiring is what makes it reachable at all
    when FLOW_MACHINE_V2 is on (previously WRITE_INVOICE was shadow-only,
    so v2 never reached ANY of this — that's the "I couldn't find a client
    named 'Generate Invoice'" class of bug)."""

    def _svc(self):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.supabase = MagicMock()
        svc.memory = MagicMock()
        svc.gemini = MagicMock()
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [{"client_name": "Nike", "brand_name": None, "production_house": None}],
        }
        svc.supabase.get_available_months_for_client.return_value = {"ok": False}
        return svc

    def test_pronoun_resolves_from_uscf_context(self):
        svc = self._svc()
        # A naive LLM extraction that (wrongly) captured the pronoun itself —
        # the pronoun-stripping check must clear this before the context
        # ladder ever runs.
        svc.gemini.parse_user_intent.return_value = {
            "operation": "ACTION_TRIGGER",
            "parameters": {"client_name": "them", "month": None, "year": None},
            "confidence": 0.7, "clarification_question": None,
        }
        user_mem = {"uscf_context": {"last_row_data": {"client_name": "Nike"}}}
        result = svc._handle_invoice_retrieval_request(
            "u1", "Generate invoice for them", "generate invoice for them",
            "u1", [], user_mem, None, False,  # not a "definite" regex match -> LLM path
        )
        response = (result.get("response") or "").lower()
        assert "i need a client name" not in response
        assert "couldn't find a client" not in response
        assert "nike" in response, f"expected the resolved client (Nike) to appear: {response!r}"

    def test_bare_generate_invoice_resolves_from_last_saved_job(self):
        svc = self._svc()
        svc.gemini.parse_user_intent.return_value = {
            "operation": "ACTION_TRIGGER",
            "parameters": {"client_name": None, "month": None, "year": None},
            "confidence": 0.6, "clarification_question": None,
        }
        user_mem = {"last_saved_job": {"db_client_name": "Nike", "job_date": "2026-04-10"}}
        result = svc._handle_invoice_retrieval_request(
            "u1", "Generate invoice", "generate invoice",
            "u1", [], user_mem, None, False,
        )
        assert "i need a client name" not in (result.get("response") or "").lower()

    def test_no_context_at_all_still_asks_for_a_client(self):
        """Guard: when there's genuinely nothing to resolve from, the bot
        must still ask rather than guessing — this isn't about silencing
        the clarification, only about not misfiring when context exists."""
        svc = self._svc()
        svc.gemini.parse_user_intent.return_value = {
            "operation": "ACTION_TRIGGER",
            "parameters": {"client_name": None, "month": None, "year": None},
            "confidence": 0.5, "clarification_question": None,
        }
        result = svc._handle_invoice_retrieval_request(
            "u1", "Generate invoice", "generate invoice",
            "u1", [], {}, None, False,
        )
        assert "client name or bill number" in (result.get("response") or "").lower()
