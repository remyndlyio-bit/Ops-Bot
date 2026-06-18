from fpdf import FPDF
from fpdf.enums import XPos, YPos
import os
from datetime import datetime
from num2words import num2words
from typing import List, Dict
from utils.logger import logger

import re as _re


def has_usable_bank_details(bank_details) -> bool:
    """True only when bank details carry a non-empty account number — the one
    field a payer actually needs. Used as the HARD GUARD against emitting an
    unpayable (bankless) invoice. Shared so the pre-check and the generation-time
    guard agree on what 'has bank details' means."""
    if not bank_details:
        return False
    return bool(str(bank_details.get("bank_account_number") or "").strip())


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


# Brand colors (legacy — kept for any external reference)
_DARK = (30, 30, 30)
_GRAY_BAR = (220, 220, 220)
_WHITE = (255, 255, 255)
_ACCENT = (50, 50, 50)
_LIGHT_ROW = (248, 248, 248)

# Editorial / luxury palette (Playfair Display + Lato redesign)
_INK   = (26, 26, 28)      # near-black headlines
_BODYC = (104, 104, 110)   # muted body text
_MUTE  = (150, 150, 156)   # light labels / secondary
_GOLD  = (166, 134, 80)    # antique-gold accent
_HAIR  = (210, 204, 192)   # warm hairline rule
_CREAM = (247, 245, 240)   # soft panel / row fill


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

            # Editorial type system: Playfair Display (serif) for display/headers,
            # Lato (sans) for body. Falls back to core Helvetica if the TTFs are
            # missing. All rendered text is run through sanitize_pdf_text(), so any
            # non-Latin characters are transliterated to ASCII before they reach
            # these (Latin) fonts.
            HEAD = "Helvetica"   # display / headers → Playfair Display
            BODY = "Helvetica"   # body text         → Lato
            try:
                fonts_dir = os.path.join(os.path.dirname(__file__), "..", "fonts")
                _font_files = [
                    ("Playfair", "",  "PlayfairDisplay-Regular.ttf"),
                    ("Playfair", "B", "PlayfairDisplay-Bold.ttf"),
                    ("Lato",     "",  "Lato-Regular.ttf"),
                    ("Lato",     "B", "Lato-Bold.ttf"),
                    ("Lato",     "I", "Lato-Italic.ttf"),
                ]
                for _fam, _style, _fname in _font_files:
                    _fpath = os.path.join(fonts_dir, _fname)
                    if not os.path.exists(_fpath):
                        raise FileNotFoundError(f"Font file not found: {_fpath}")
                    pdf.add_font(_fam, _style, _fpath)
                HEAD, BODY = "Playfair", "Lato"
                logger.info("[INVOICE] Loaded Playfair Display + Lato fonts")
            except Exception as e:
                logger.warning(f"Could not load Playfair/Lato, falling back to Helvetica: {e}")

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

            invoice_date = datetime.now().strftime("%d %b %Y")

            # ── layout helpers ────────────────────────────────────────────
            page_w = pdf.w - pdf.l_margin - pdf.r_margin  # ~180mm
            S = sanitize_pdf_text

            def _rule(color, width=0.2, gap_after=0.0):
                pdf.set_draw_color(*color)
                pdf.set_line_width(width)
                _y = pdf.get_y()
                pdf.line(pdf.l_margin, _y, pdf.w - pdf.r_margin, _y)
                pdf.set_line_width(0.2)
                if gap_after:
                    pdf.ln(gap_after)

            def _kicker(text, x, w, color=_GOLD, size=7.5, align="L", spacing=1.6, h=4.6):
                """Tracked, uppercase section label — the editorial 'kicker'."""
                pdf.set_x(x)
                pdf.set_font(BODY, "B", size)
                pdf.set_text_color(*color)
                pdf.set_char_spacing(spacing)
                pdf.cell(w, h, S(text).upper(), ln=1, align=align)
                pdf.set_char_spacing(0)

            # ══════════════════════════════════════════════════════════════
            # MASTHEAD — name (Playfair) + INVOICE wordmark
            # ══════════════════════════════════════════════════════════════
            top_y = pdf.get_y()
            pdf.set_xy(pdf.l_margin, top_y)
            pdf.set_font(HEAD, "B", 24)
            pdf.set_text_color(*_INK)
            pdf.cell(page_w * 0.63, 12, S(invoicer_name), ln=0)

            pdf.set_xy(pdf.l_margin + page_w * 0.63, top_y + 1.5)
            pdf.set_font(HEAD, "", 27)
            pdf.set_text_color(*_GOLD)
            pdf.set_char_spacing(3.0)
            pdf.cell(page_w * 0.37, 11, "INVOICE", ln=1, align="R")
            pdf.set_char_spacing(0)

            pdf.set_y(top_y + 15)
            _rule(_GOLD, width=0.5, gap_after=5)

            # ══════════════════════════════════════════════════════════════
            # FROM (sender)  |  INVOICE META — two columns
            # ══════════════════════════════════════════════════════════════
            block_y = pdf.get_y()
            left_w  = page_w * 0.56
            right_x = pdf.l_margin + left_w + 6
            right_w = page_w - left_w - 6

            # Left — sender details under a 'FROM' kicker. The address wraps inside
            # a FIXED-WIDTH block (multi_cell) so a long address can never run off
            # its column and print over the invoice meta on the right.
            _kicker("From", pdf.l_margin, left_w, color=_MUTE)
            pdf.ln(0.5)
            detail_w = left_w - 4
            pdf.set_font(BODY, "", 9)
            pdf.set_text_color(*_BODYC)

            def _left_detail(text):
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(detail_w, 4.8, S(text), align="L",
                               new_x=XPos.LMARGIN, new_y=YPos.NEXT)

            if invoicer_address:
                for _line in S(invoicer_address).split("\n"):
                    _line = _line.strip()
                    if _line:
                        _left_detail(_line)
            if invoicer_email:
                _left_detail(f"Email   {invoicer_email}")
            if invoicer_mobile:
                _left_detail(f"Mobile   {invoicer_mobile}")
            if invoicer_pan:
                _left_detail(f"PAN   {invoicer_pan}")
            after_left_y = pdf.get_y()

            # Right — invoice metadata: tracked label + value, aligned to the edge.
            pdf.set_xy(right_x, block_y)

            def _meta(label, value):
                pdf.set_x(right_x)
                pdf.set_font(BODY, "B", 6.5)
                pdf.set_text_color(*_MUTE)
                pdf.set_char_spacing(1.1)
                pdf.cell(right_w * 0.42, 5.6, S(label).upper(), ln=0, align="L")
                pdf.set_char_spacing(0)
                pdf.set_font(BODY, "", 9.5)
                pdf.set_text_color(*_INK)
                pdf.cell(right_w * 0.58, 5.6, S(value), ln=1, align="R")

            _meta("Invoice No.", _db_bill_no)
            _meta("Date", invoice_date)
            # #5 — terms consistent with the T&C ("within 30 days"), not "Immediate".
            _meta("Terms", "Within 30 days")
            # #6 — only show GST when it's real; no always-"NA" rows.
            if invoicer_gst and str(invoicer_gst).strip().upper() not in ("NA", "N/A", ""):
                _meta("GST", invoicer_gst)

            pdf.set_y(max(after_left_y, pdf.get_y()) + 6)
            _rule(_HAIR, width=0.2, gap_after=5)

            # ══════════════════════════════════════════════════════════════
            # INVOICE TO — POC first, then company / billing address
            # ══════════════════════════════════════════════════════════════
            _kicker("Invoice To", pdf.l_margin, page_w, color=_GOLD)
            pdf.ln(1.5)

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

            client_display = S(summary.get("client", ""))

            # Build the company / billing block (name + any address lines).
            billing_lines = []
            if client_billing:
                billing_lines = [l.strip() for l in client_billing.split("\n") if l.strip()]
            elif production_house and production_house.lower() not in ("none", ""):
                billing_lines = [production_house]
            elif client_display:
                billing_lines = [client_display]

            # Addressed TO THE POC first (Playfair, prominent), company + billing
            # address underneath (Lato). No POC on file → lead with the company.
            if poc_name:
                pdf.set_font(HEAD, "B", 13)
                pdf.set_text_color(*_INK)
                pdf.cell(0, 6.5, S(poc_name), ln=1)
                pdf.set_font(BODY, "", 9)
                pdf.set_text_color(*_BODYC)
                for _bl in billing_lines:
                    pdf.cell(0, 4.8, S(_bl), ln=1)
            elif billing_lines:
                pdf.set_font(HEAD, "B", 13)
                pdf.set_text_color(*_INK)
                pdf.cell(0, 6.5, S(billing_lines[0]), ln=1)
                pdf.set_font(BODY, "", 9)
                pdf.set_text_color(*_BODYC)
                for _bl in billing_lines[1:]:
                    pdf.cell(0, 4.8, S(_bl), ln=1)

            # Brand — small tracked label inline beneath the billing block
            # (left-aligned, part of the Invoice To group, not floating right).
            if brand_name_invoice:
                pdf.ln(1)
                pdf.set_font(BODY, "B", 7)
                pdf.set_text_color(*_MUTE)
                pdf.set_char_spacing(1.0)
                pdf.cell(0, 4.5, S(f"Brand   {brand_name_invoice}").upper(), ln=1, align="L")
                pdf.set_char_spacing(0)

            pdf.ln(4)

            # ══════════════════════════════════════════════════════════════
            # LINE ITEMS — Description | Date | Amount
            # ══════════════════════════════════════════════════════════════
            _kicker("Description of Work", pdf.l_margin, page_w, color=_GOLD)
            pdf.ln(1.5)

            _date_w = page_w * 0.18
            _amt_w  = page_w * 0.22
            _desc_w = page_w - _date_w - _amt_w

            # Column header — tracked labels above a solid ink rule.
            pdf.set_font(BODY, "B", 7)
            pdf.set_text_color(*_MUTE)
            pdf.set_char_spacing(1.0)
            pdf.cell(_desc_w, 6, "DESCRIPTION", ln=0, align="L")
            pdf.cell(_date_w, 6, "DATE", ln=0, align="C")
            pdf.cell(_amt_w, 6, "AMOUNT", ln=1, align="R")
            pdf.set_char_spacing(0)
            _rule(_INK, width=0.3)

            total = 0
            pdf.set_font(BODY, "", 9.5)
            for row in (client_data or []):
                date_val = S(str(row.get("job_date", "")).strip())
                try:
                    _d = datetime.strptime(date_val[:10], "%Y-%m-%d")
                    date_val = _d.strftime("%d %b %y")
                except Exception:
                    pass

                desc_val = S(str(row.get("job_description_details", "") or "").strip())
                if not desc_val or desc_val.lower() == "none":
                    # Fall back to the brand/client only when there's genuinely no
                    # description, so the line is never empty.
                    desc_val = S(str(row.get("brand_name") or row.get("client_name") or "Job").strip())
                # Keep the description on one line (truncate very long text).
                if len(desc_val) > 64:
                    desc_val = desc_val[:61] + "..."
                fees_val = self._parse_fees(row.get("fees", "0"))
                total += fees_val

                pdf.set_y(pdf.get_y() + 1.6)
                pdf.set_font(BODY, "", 9.5)
                pdf.set_text_color(*_INK)
                pdf.cell(_desc_w, 6.4, S(desc_val), ln=0, align="L")
                pdf.set_text_color(*_BODYC)
                pdf.cell(_date_w, 6.4, date_val or "-", ln=0, align="C")
                # Amount set in Playfair — its figures are oldstyle by default,
                # giving the line items that refined editorial feel.
                pdf.set_font(HEAD, "", 10.5)
                pdf.set_text_color(*_INK)
                pdf.cell(_amt_w, 6.4, S(f"Rs {fees_val:,.0f}"), ln=1, align="R")
                _rule(_HAIR, width=0.15)

            pdf.ln(3)

            # Total — cream panel on the right with a large Playfair amount.
            total_y = pdf.get_y()
            panel_x = pdf.l_margin + page_w * 0.52
            panel_w = page_w - page_w * 0.52
            pdf.set_fill_color(*_CREAM)
            pdf.rect(panel_x, total_y, panel_w, 14, style="F")
            pdf.set_xy(panel_x + 5, total_y)
            pdf.set_font(BODY, "B", 7.5)
            pdf.set_text_color(*_GOLD)
            pdf.set_char_spacing(1.6)
            pdf.cell(panel_w * 0.4, 14, "TOTAL", ln=0, align="L")
            pdf.set_char_spacing(0)
            pdf.set_xy(panel_x, total_y)
            pdf.set_font(HEAD, "B", 16)
            pdf.set_text_color(*_INK)
            pdf.cell(panel_w - 5, 14, S(f"Rs {total:,.0f}"), ln=1, align="R")
            pdf.set_y(total_y + 14)

            # In words — tracked label + the amount spelled out in Playfair.
            try:
                total_int = int(round(total))
                total_words = num2words(total_int, lang="en_IN").capitalize() + " Only"
            except Exception:
                total_words = ""
            if total_words:
                pdf.ln(2)
                pdf.set_font(BODY, "B", 6.5)
                pdf.set_text_color(*_MUTE)
                pdf.set_char_spacing(1.1)
                pdf.cell(26, 5.5, "IN WORDS", ln=0)
                pdf.set_char_spacing(0)
                pdf.set_font(HEAD, "", 10)
                pdf.set_text_color(*_INK)
                pdf.cell(0, 5.5, S(total_words), ln=1)

            pdf.ln(5)

            # ══════════════════════════════════════════════════════════════
            # TERMS  |  PAYMENT DETAILS — two columns
            # ══════════════════════════════════════════════════════════════
            block_y2 = pdf.get_y()
            col_w = page_w * 0.5

            # Left — Terms
            _kicker("Terms", pdf.l_margin, col_w - 4, color=_GOLD)
            pdf.ln(1)
            payee_name = invoicer_name or bd.get("bank_account_name") or "the account holder"
            tc_lines = [
                "Advance payment due within 2 working days of the invoice date.",
                "Full payment due within 30 days of the invoice date.",
                f"Please make payment in favour of {S(payee_name)}.",
            ]
            pdf.set_font(BODY, "", 8)
            pdf.set_text_color(*_BODYC)
            for _tl in tc_lines:
                pdf.set_x(pdf.l_margin)
                pdf.multi_cell(col_w - 6, 4.8, S(_tl), align="L",
                               new_x=XPos.LMARGIN, new_y=YPos.NEXT)
            after_terms_y = pdf.get_y()

            # Right — Payment / bank details
            rx = pdf.l_margin + col_w
            pdf.set_xy(rx, block_y2)
            _kicker("Payment Details", rx, col_w, color=_GOLD)
            pdf.ln(1)

            def _bank_row(label, value):
                if not value:
                    return
                pdf.set_x(rx)
                pdf.set_font(BODY, "B", 7.5)
                pdf.set_text_color(*_MUTE)
                pdf.set_char_spacing(0.6)
                pdf.cell(col_w * 0.32, 5.2, S(label).upper(), ln=0)
                pdf.set_char_spacing(0)
                pdf.set_font(BODY, "", 8.5)
                pdf.set_text_color(*_INK)
                pdf.cell(col_w * 0.68, 5.2, S(str(value)), ln=1)

            _bank_row("Bank",     bd.get("bank_name") or "")
            _bank_row("A/C Name", bd.get("bank_account_name") or "")
            _bank_row("A/C No.",  bd.get("bank_account_number") or "")
            _bank_row("IFSC",     bd.get("bank_ifsc") or "")
            if bd.get("upi_id"):
                _bank_row("UPI",   bd.get("upi_id"))
            if bd.get("swift_code"):
                _bank_row("Swift", bd.get("swift_code"))

            pdf.set_y(max(after_terms_y, pdf.get_y()))

            # ══════════════════════════════════════════════════════════════
            # FOOTER
            # ══════════════════════════════════════════════════════════════
            pdf.ln(7)
            _rule(_GOLD, width=0.4, gap_after=3)
            pdf.set_font(HEAD, "", 10)
            pdf.set_text_color(*_INK)
            pdf.set_char_spacing(1.2)
            pdf.cell(0, 6, "Thank you", ln=1, align="C")
            pdf.set_char_spacing(0)
            pdf.set_font(BODY, "I", 7.5)
            pdf.set_text_color(*_MUTE)
            pdf.cell(0, 4.5, "Powered by Remyndly", ln=1, align="C")

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
