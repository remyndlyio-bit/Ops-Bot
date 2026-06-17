from fpdf import FPDF
import os
from datetime import datetime
from num2words import num2words
from typing import List, Dict
from utils.logger import logger

import re as _re


def _strip_billing_label(text: str) -> str:
    """Remove a stray leading label the user often types into billing details,
    e.g. "Billing info is Spotify India..." → "Spotify India...". Defensive
    cleanup so a captured label doesn't print on the invoice (#1)."""
    if not text:
        return text
    cleaned = _re.sub(
        r'^\s*(?:the\s+)?billing\s*(?:info(?:rmation|r)?|details?|address)?\s*'
        r'(?:is|are|:|-)?\s*',
        '',
        text,
        count=1,
        flags=_re.IGNORECASE,
    )
    return cleaned.strip() or text


def sanitize_pdf_text(text):
    """Return PDF-safe text.

    1. Replace common Unicode punctuation with ASCII equivalents.
    2. If the result still contains characters outside the Latin-1 range
       (e.g. Devanagari, Arabic, CJK), transliterate to ASCII via unidecode
       so the text remains readable in the PDF rather than showing blank boxes.
    """
    if text is None:
        return ""
    text = str(text)
    text = text.replace("—", "-")
    text = text.replace("–", "-")
    text = text.replace("₹", "Rs ")
    text = text.replace("“", '"')
    text = text.replace("”", '"')
    try:
        text.encode("latin-1")
    except (UnicodeEncodeError, UnicodeDecodeError):
        try:
            from unidecode import unidecode
            text = unidecode(text)
        except ImportError:
            text = text.encode("latin-1", errors="replace").decode("latin-1")
    return text


# Brand colors
_DARK = (30, 30, 30)
_GRAY_BAR = (220, 220, 220)
_WHITE = (255, 255, 255)
_ACCENT = (50, 50, 50)
_LIGHT_ROW = (248, 248, 248)


class InvoiceGenerationService:
    def __init__(self):
        pass

    def generate_pdf(self, summary: Dict, client_data: List[Dict], bank_details: Dict = None, user_profile: Dict = None) -> str:
        """
        Generates a PDF using fpdf2.

        user_profile keys: name, title, address, email, mobile, pan, gst
        bank_details keys: bank_name, bank_account_name, bank_account_number,
                           bank_ifsc, upi_id, mobile_number, pan_number, gst_number
        """
        try:
            pdf = FPDF()
            pdf.add_page()
            pdf.set_auto_page_break(auto=True, margin=15)
            pdf.set_margins(15, 15, 15)

            font_family = "Helvetica"
            try:
                fonts_dir = os.path.join(os.path.dirname(__file__), "..", "fonts")
                font_path      = os.path.join(fonts_dir, "DejaVuSans.ttf")
                font_path_bold = os.path.join(fonts_dir, "DejaVuSans-Bold.ttf")
                font_path_italic = os.path.join(fonts_dir, "DejaVuSans-Oblique.ttf")
                if not os.path.exists(font_path):
                    raise FileNotFoundError(f"TTF Font file not found: {font_path}")
                pdf.add_font("DejaVu", "",  font_path)
                pdf.add_font("DejaVu", "B", font_path_bold   if os.path.exists(font_path_bold)   else font_path)
                pdf.add_font("DejaVu", "I", font_path_italic if os.path.exists(font_path_italic) else font_path)
                font_family = "DejaVu"
                logger.info("[INVOICE] Loaded DejaVu Unicode font")
            except Exception as e:
                logger.warning(f"Could not load DejaVu font, falling back to Helvetica: {e}")

            up = user_profile or {}
            bd = bank_details or {}

            invoicer_name    = up.get("name") or bd.get("bank_account_name") or ""
            invoicer_address = up.get("address") or ""
            invoicer_email   = up.get("email") or ""
            invoicer_mobile  = up.get("mobile") or bd.get("mobile_number") or ""
            invoicer_pan     = up.get("pan") or bd.get("pan_number") or ""
            invoicer_gst     = up.get("gst") or bd.get("gst_number") or "NA"

            # ── Invoice number ────────────────────────────────────────────
            _db_bill_no = ""
            for _r in (client_data or []):
                _bn = str(_r.get("bill_no") or "").strip()
                if _bn:
                    _db_bill_no = _bn
                    break
            if not _db_bill_no:
                client_prefix = sanitize_pdf_text(summary.get("client", "")[:3].upper())
                _db_bill_no = f"{datetime.now().strftime('%y%m%d')}-{client_prefix}"

            invoice_date = datetime.now().strftime("%d/%m/%y")

            # ── PAGE WIDTH helpers ────────────────────────────────────────
            page_w = pdf.w - pdf.l_margin - pdf.r_margin  # ~180mm
            left_w = page_w * 0.55
            right_w = page_w * 0.45

            # ══════════════════════════════════════════════════════════════
            # SECTION 1 — Two-column header
            # ══════════════════════════════════════════════════════════════
            top_y = pdf.get_y()

            # Left: user details
            pdf.set_font(font_family, "B", 20)
            pdf.set_text_color(*_DARK)
            pdf.cell(left_w, 10, sanitize_pdf_text(invoicer_name), ln=0)
            pdf.ln(10)

            pdf.set_font(font_family, "", 9)
            pdf.set_text_color(80, 80, 80)
            if invoicer_address:
                for _line in sanitize_pdf_text(invoicer_address).split("\n"):
                    _line = _line.strip()
                    if _line:
                        pdf.cell(left_w, 5, _line, ln=1)
            if invoicer_email:
                pdf.cell(left_w, 5, sanitize_pdf_text(f"Email ID: {invoicer_email}"), ln=1)
            if invoicer_mobile:
                pdf.cell(left_w, 5, sanitize_pdf_text(f"Mobile Number: {invoicer_mobile}"), ln=1)
            if invoicer_pan:
                pdf.cell(left_w, 5, sanitize_pdf_text(f"PAN: {invoicer_pan}"), ln=1)

            after_left_y = pdf.get_y()

            # Right: invoice metadata (go back to top_y)
            pdf.set_xy(pdf.l_margin + left_w, top_y)
            pdf.set_font(font_family, "", 9)
            pdf.set_text_color(*_DARK)

            def _right_row(label, value):
                pdf.set_x(pdf.l_margin + left_w)
                pdf.set_font(font_family, "", 9)
                pdf.cell(right_w * 0.48, 6, sanitize_pdf_text(label), ln=0, align="L")
                pdf.set_font(font_family, "B", 9)
                pdf.cell(right_w * 0.52, 6, sanitize_pdf_text(value), ln=1, align="R")

            _right_row("Invoice Number :", _db_bill_no)
            _right_row("Invoice Date :", invoice_date)
            # #5 — keep terms consistent with the T&C ("full payment within 30 days"),
            # not the contradictory "Immediate".
            _right_row("Payment Terms :", "Within 30 days")
            # #6 — only show GST when it's a real value; drop the always-"NA" Job No. row.
            if invoicer_gst and str(invoicer_gst).strip().upper() not in ("NA", "N/A", ""):
                _right_row("GST :", invoicer_gst)

            pdf.set_y(max(after_left_y, pdf.get_y()) + 4)

            # Thin separator line
            pdf.set_draw_color(180, 180, 180)
            pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
            pdf.ln(5)

            # ══════════════════════════════════════════════════════════════
            # SECTION 2 — Invoice To
            # ══════════════════════════════════════════════════════════════
            # Gray header bar
            pdf.set_fill_color(*_GRAY_BAR)
            pdf.set_text_color(*_DARK)
            pdf.set_font(font_family, "B", 10)
            pdf.cell(0, 7, "  Invoice To:", ln=1, fill=True)
            pdf.ln(2)

            # Collect client billing info
            client_billing = ""
            production_house = ""
            poc_name = ""
            brand_name_invoice = ""
            for row in (client_data or []):
                if not client_billing:
                    client_billing = _strip_billing_label(
                        (str(row.get("client_billing_details") or "")).strip()
                    )
                if not production_house:
                    production_house = (str(row.get("production_house") or "")).strip()
                if not poc_name:
                    _pn = (str(row.get("poc_name") or "")).strip()
                    if _pn and _pn.lower() not in ("none", ""):
                        poc_name = _pn
                if not brand_name_invoice:
                    _bn = (str(row.get("brand_name") or "")).strip()
                    if _bn and _bn.lower() not in ("none", ""):
                        brand_name_invoice = _bn

            client_display = sanitize_pdf_text(summary.get("client", ""))
            pdf.set_font(font_family, "B", 11)
            pdf.set_text_color(*_DARK)
            # Lead with client / billing name
            if client_billing:
                first_line = client_billing.split("\n")[0].strip()
                pdf.cell(0, 6, sanitize_pdf_text(first_line), ln=1)
                pdf.set_font(font_family, "", 9)
                pdf.set_text_color(80, 80, 80)
                for _bl in client_billing.split("\n")[1:]:
                    _bl = _bl.strip()
                    if _bl:
                        pdf.cell(0, 5, sanitize_pdf_text(_bl), ln=1)
            elif production_house and production_house.lower() not in ("none", ""):
                pdf.cell(0, 6, sanitize_pdf_text(production_house), ln=1)
            elif client_display:
                pdf.cell(0, 6, client_display, ln=1)

            if poc_name:
                pdf.set_font(font_family, "", 9)
                pdf.set_text_color(80, 80, 80)
                pdf.cell(0, 5, sanitize_pdf_text(poc_name), ln=1)

            # Brand line — right-aligned
            if brand_name_invoice:
                _by = pdf.get_y()
                pdf.set_xy(pdf.l_margin, _by)
                pdf.set_font(font_family, "B", 9)
                pdf.set_text_color(*_DARK)
                pdf.cell(0, 6, sanitize_pdf_text(f"Brand : {brand_name_invoice}"), ln=1, align="R")

            pdf.ln(4)

            # ══════════════════════════════════════════════════════════════
            # SECTION 3 — Jobs table: Description | Date | Amount
            # #3/#7 — show the JOB DESCRIPTION (the relevant line item) and drop
            # the redundant client / brand / POC / bill-no dump (all already shown
            # above or in the invoice number).
            # ══════════════════════════════════════════════════════════════
            pdf.set_fill_color(*_GRAY_BAR)
            pdf.set_text_color(*_DARK)
            pdf.set_font(font_family, "B", 9)
            pdf.cell(0, 7, "  Jobs", ln=1, fill=True)
            pdf.ln(2)

            # Column widths for the line-item table.
            _date_w = page_w * 0.20
            _amt_w  = page_w * 0.20
            _desc_w = page_w - _date_w - _amt_w

            # Column header row
            pdf.set_fill_color(*_LIGHT_ROW)
            pdf.set_font(font_family, "B", 8)
            pdf.set_text_color(80, 80, 80)
            pdf.cell(_desc_w, 6, "  Description", border=0, ln=0, fill=True, align="L")
            pdf.cell(_date_w, 6, "Date", border=0, ln=0, fill=True, align="C")
            pdf.cell(_amt_w, 6, "Amount", border=0, ln=1, fill=True, align="R")

            pdf.set_font(font_family, "", 9)
            total = 0
            for idx, row in enumerate(client_data or [], 1):
                bg = _LIGHT_ROW if idx % 2 == 0 else _WHITE
                pdf.set_fill_color(*bg)
                pdf.set_text_color(*_DARK)

                date_val = sanitize_pdf_text(str(row.get("job_date", "")).strip())
                try:
                    _d = datetime.strptime(date_val[:10], "%Y-%m-%d")
                    date_val = _d.strftime("%d %b %y")
                except Exception:
                    pass

                desc_val = sanitize_pdf_text(str(row.get("job_description_details", "") or "").strip())
                if not desc_val or desc_val.lower() == "none":
                    # Fall back to the brand/client only when there's genuinely no
                    # description, so the line is never empty.
                    desc_val = sanitize_pdf_text(
                        str(row.get("brand_name") or row.get("client_name") or "Job").strip()
                    )
                # Keep the description on one line (truncate very long text).
                if len(desc_val) > 70:
                    desc_val = desc_val[:67] + "..."
                fees_val = self._parse_fees(row.get("fees", "0"))
                total += fees_val

                pdf.cell(_desc_w, 7, f"  {idx}. {desc_val}", border=0, ln=0, align="L", fill=True)
                pdf.cell(_date_w, 7, date_val or "-", border=0, ln=0, align="C", fill=True)
                pdf.cell(_amt_w, 7, sanitize_pdf_text(f"Rs {fees_val:,.0f}"), border=0, ln=1, align="R", fill=True)

            pdf.ln(2)

            # Total row
            _fee_w = 40
            pdf.set_fill_color(*_GRAY_BAR)
            pdf.set_font(font_family, "B", 10)
            pdf.cell(page_w - _fee_w, 8, "TOTAL", 1, 0, "R", True)
            pdf.cell(_fee_w, 8, sanitize_pdf_text(f"Rs {total:,.0f}"), 1, 1, "R", True)

            pdf.ln(3)

            # In Words
            try:
                total_int = int(round(total))
                total_words = num2words(total_int, lang="en_IN").capitalize() + " Only"
            except Exception:
                total_words = ""
            pdf.set_font(font_family, "B", 9)
            pdf.set_text_color(*_DARK)
            pdf.cell(20, 6, "In words:", ln=0)
            pdf.set_font(font_family, "I", 9)
            pdf.cell(0, 6, sanitize_pdf_text(total_words), ln=1)

            pdf.ln(5)

            # ══════════════════════════════════════════════════════════════
            # SECTION 4 — Terms & Bank (gray header bar + two columns)
            # ══════════════════════════════════════════════════════════════
            pdf.set_fill_color(*_GRAY_BAR)
            pdf.set_text_color(*_DARK)
            pdf.set_font(font_family, "B", 9)
            pdf.cell(0, 7, "  Terms and Condition", ln=1, fill=True)
            pdf.ln(2)

            footer_y = pdf.get_y()
            left_col_w  = page_w * 0.50
            right_col_w = page_w * 0.50

            # Left: T&C text
            pdf.set_font(font_family, "", 8)
            pdf.set_text_color(60, 60, 60)
            payee_name = invoicer_name or bd.get("bank_account_name") or "the account holder"
            tc_lines = [
                "Advance payments should be made within 2 working",
                "days from invoice date.",
                "Full payment should be made within 30 days of",
                "invoice date.",
                f"Payment in favour of '{sanitize_pdf_text(payee_name)}'.",
            ]
            for _tl in tc_lines:
                pdf.set_x(pdf.l_margin)
                pdf.cell(left_col_w, 5, sanitize_pdf_text(_tl), ln=1)

            # Right: Bank details (go back to footer_y)
            right_x = pdf.l_margin + left_col_w
            pdf.set_xy(right_x, footer_y)
            pdf.set_font(font_family, "B", 9)
            pdf.set_text_color(*_DARK)
            pdf.cell(right_col_w, 5, "BANK ACCOUNT DETAILS:", ln=1)
            pdf.set_font(font_family, "", 8)
            pdf.set_text_color(60, 60, 60)

            def _bank_row(label, value):
                if not value:
                    return
                pdf.set_x(right_x)
                pdf.set_font(font_family, "B", 8)
                pdf.cell(right_col_w * 0.38, 5, sanitize_pdf_text(label), ln=0)
                pdf.set_font(font_family, "", 8)
                pdf.cell(right_col_w * 0.62, 5, sanitize_pdf_text(str(value)), ln=1)

            _bank_row("Bank:",          bd.get("bank_name") or "")
            _bank_row("A/C Name:",      bd.get("bank_account_name") or "")
            _bank_row("A/C No.:",       bd.get("bank_account_number") or "")
            _bank_row("IFSC Code:",     bd.get("bank_ifsc") or "")
            if bd.get("upi_id"):
                _bank_row("UPI:",       bd.get("upi_id"))
            if bd.get("swift_code"):
                _bank_row("Swift Code:", bd.get("swift_code"))

            # ══════════════════════════════════════════════════════════════
            # FOOTER — Powered by Remyndly
            # ══════════════════════════════════════════════════════════════
            pdf.ln(6)
            pdf.set_draw_color(180, 180, 180)
            pdf.line(pdf.l_margin, pdf.get_y(), pdf.w - pdf.r_margin, pdf.get_y())
            pdf.ln(3)
            pdf.set_font(font_family, "I", 8)
            pdf.set_text_color(140, 140, 140)
            pdf.cell(0, 5, "Powered by Remyndly", ln=1, align="C")

            # ── Save ──────────────────────────────────────────────────────
            os.makedirs("output", exist_ok=True)
            safe_client = sanitize_pdf_text(summary.get("client", "Client")).replace(" ", "_")
            safe_month  = sanitize_pdf_text(summary.get("month", "Period")).replace(" ", "_")
            output_path = os.path.join("output", f"Invoice_{safe_client}_{safe_month}.pdf")
            pdf.output(output_path)
            logger.info(f"PDF generated: {output_path}")
            return output_path

        except Exception:
            logger.error("Failed to generate invoice PDF", exc_info=True)
            return None

    # Multiplier table for fee shorthand parsing.
    # English: k = thousand, L / lakh / lac = 100,000, cr / crore = 10,000,000.
    # Hindi / Hinglish: hazaar / hazar / hajaar / hajar = thousand.
    # Match order matters — longer suffixes first so "lakh" beats "l".
    _FEE_SUFFIX_MULTIPLIERS = (
        ("crore",   10_000_000),
        ("cr",      10_000_000),
        ("lakhs",   100_000),
        ("lakh",    100_000),
        ("lacs",    100_000),
        ("lac",     100_000),
        ("hazaar",  1_000),
        ("hajaar",  1_000),
        ("hazar",   1_000),
        ("hajar",   1_000),
        ("thousand",1_000),
        # Single letters last — they should only match standalone.
        ("l",       100_000),
        ("k",       1_000),
    )

    def _parse_fees(self, fees_str: str) -> float:
        """Parse a fee string into a numeric value.

        Handles:
          plain numbers:    "25000", "25,000", "25000.50"
          currency prefix:  "₹25,000", "Rs 25,000", "Rs.25000"
          k notation:       "25k", "25K", "2.5k"
          lakh notation:    "1.5L", "1.5 lakh", "2 lakhs", "1.5 lac"
          hazaar notation:  "25 hazaar", "25 hazar", "25 hajaar"
          crore notation:   "1cr", "1.5 crore"

        Returns 0.0 on any failure — callers downstream handle the
        zero case gracefully (the row still saves, the fee just reads
        as ₹0 until the user fixes it).

        Tested by tests/test_scenarios_from_matrix.py::TestFeeParsing.
        Adding a new shorthand = add a row to _FEE_SUFFIX_MULTIPLIERS +
        add a test case. CI rejects regressions.
        """
        import re

        if fees_str is None:
            return 0.0
        if isinstance(fees_str, (int, float)):
            return float(fees_str)

        # Strip currency markers / commas / whitespace, lower-case for matching.
        clean = (
            str(fees_str)
            .replace("₹", "")
            .replace("Rs.", "")
            .replace("Rs", "")
            .replace("INR", "")
            .replace(",", "")
            .strip()
            .lower()
        )
        if not clean:
            return 0.0

        # Fast path: pure numeric (with optional decimals).
        try:
            return float(clean)
        except ValueError:
            pass

        # Shorthand path: <number> [optional space] <suffix>
        # Try each suffix longest-first so 'lakh' beats 'l', 'hazaar' beats 'k'.
        for suffix, multiplier in self._FEE_SUFFIX_MULTIPLIERS:
            # \b around the suffix avoids matching "lakshmi" (contains 'l').
            # For multi-letter suffixes, the \b handles word boundaries; for
            # single-letter suffixes we require the suffix is at end-of-string.
            if len(suffix) == 1:
                pattern = rf"^\s*([\d]+(?:\.\d+)?)\s*{re.escape(suffix)}\s*$"
            else:
                pattern = rf"^\s*([\d]+(?:\.\d+)?)\s*{re.escape(suffix)}\s*$"
            m = re.match(pattern, clean)
            if m:
                try:
                    return float(m.group(1)) * multiplier
                except ValueError:
                    return 0.0

        # Truly unparseable — return 0 and let the row save anyway.
        return 0.0
