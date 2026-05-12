from fpdf import FPDF
import os
from datetime import datetime
from num2words import num2words
from typing import List, Dict
from utils.logger import logger


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
    text = text.replace("\u201c", '"')
    text = text.replace("\u201d", '"')
    # Final guard: transliterate any remaining non-Latin-1 characters
    try:
        text.encode("latin-1")          # fast check — raises if outside range
    except (UnicodeEncodeError, UnicodeDecodeError):
        try:
            from unidecode import unidecode
            text = unidecode(text)
        except ImportError:
            text = text.encode("latin-1", errors="replace").decode("latin-1")
    return text


class InvoiceGenerationService:
    def __init__(self):
        pass

    def generate_pdf(self, summary: Dict, client_data: List[Dict], bank_details: Dict = None, user_profile: Dict = None) -> str:
        """
        Generates a PDF using fpdf2.

        user_profile (optional) keys used for the header:
          name, title, address, email  (from user_profiles.preferences or defaults)
        bank_details keys used for bank section:
          bank_name, bank_account_name, bank_account_number, bank_ifsc, upi_id
        client_data rows may contain:
          client_billing_details, production_house  (used in "Invoice To" section)
        """
        try:
            pdf = FPDF()
            pdf.add_page()
            pdf.set_auto_page_break(auto=True, margin=15)

            # Unicode font (DejaVu) — required for non-Latin names (Hindi, etc.).
            # fpdf2 >= 2.5 dropped the uni=True kwarg; bold variant registered separately.
            font_family = "Helvetica"
            try:
                fonts_dir = os.path.join(os.path.dirname(__file__), "..", "fonts")
                font_path      = os.path.join(fonts_dir, "DejaVuSans.ttf")
                font_path_bold = os.path.join(fonts_dir, "DejaVuSans-Bold.ttf")
                if not os.path.exists(font_path):
                    raise FileNotFoundError(f"TTF Font file not found: {font_path}")
                font_path_italic = os.path.join(fonts_dir, "DejaVuSans-Oblique.ttf")
                pdf.add_font("DejaVu", "",  font_path)
                pdf.add_font("DejaVu", "B", font_path_bold   if os.path.exists(font_path_bold)   else font_path)
                pdf.add_font("DejaVu", "I", font_path_italic if os.path.exists(font_path_italic) else font_path)
                font_family = "DejaVu"
                logger.info("[INVOICE] Loaded DejaVu Unicode font")
            except Exception as e:
                logger.warning(f"Could not load DejaVu font, falling back to Helvetica: {e}")

            # Resolve user profile fields
            up = user_profile or {}
            invoicer_name = up.get("name") or "Your Name"
            invoicer_title = up.get("title") or ""
            invoicer_address = up.get("address") or ""
            invoicer_email = up.get("email") or ""

            # Colors
            primary_color = (0, 0, 0)
            gray_color = (128, 128, 128)

            # Header - Name & Details
            pdf.set_font(font_family, "B", 20)
            pdf.cell(100, 10, sanitize_pdf_text(invoicer_name), ln=0)

            pdf.set_font(font_family, "B", 12)
            pdf.set_text_color(*gray_color)
            pdf.cell(90, 10, sanitize_pdf_text("INVOICE"), ln=1, align="R")
            pdf.set_text_color(*primary_color)

            pdf.set_font(font_family, "", 10)
            if invoicer_title:
                pdf.cell(100, 5, sanitize_pdf_text(invoicer_title), ln=0)
            else:
                pdf.cell(100, 5, "", ln=0)
            # Use the DB bill_no if present (assigned by trigger on insert);
            # fall back to date+client prefix only for legacy rows missing bill_no.
            _db_bill_no = ""
            for _r in data:
                _bn = str(_r.get("bill_no") or "").strip()
                if _bn:
                    _db_bill_no = _bn
                    break
            if _db_bill_no:
                invoice_no = sanitize_pdf_text(f"Invoice #: {_db_bill_no}")
            else:
                client_prefix = sanitize_pdf_text(summary.get("client", "")[:3].upper())
                invoice_no = sanitize_pdf_text(f"Invoice #: {datetime.now().strftime('%y%m%d')}-{client_prefix}")
            pdf.cell(90, 5, invoice_no, ln=1, align="R")

            if invoicer_address:
                pdf.cell(100, 5, sanitize_pdf_text(invoicer_address), ln=0)
            else:
                pdf.cell(100, 5, "", ln=0)
            pdf.cell(90, 5, sanitize_pdf_text(f"Date: {datetime.now().strftime('%d-%m-%Y')}"), ln=1, align="R")

            if invoicer_email:
                pdf.cell(100, 5, sanitize_pdf_text(f"Email: {invoicer_email}"), ln=0)
            else:
                pdf.cell(100, 5, "", ln=0)
            pdf.cell(90, 5, sanitize_pdf_text("Terms: Immediate"), ln=1, align="R")

            pdf.ln(10)

            # Client Info — use client_billing_details, poc_name, production_house from data rows.
            # Walk the FULL list so poc_name is found even if billing details came first.
            client_billing = ""
            production_house = ""
            poc_name = ""
            for row in client_data:
                if not client_billing:
                    client_billing = (str(row.get("client_billing_details") or "")).strip()
                if not production_house:
                    production_house = (str(row.get("production_house") or "")).strip()
                if not poc_name:
                    _pn = (str(row.get("poc_name") or "")).strip()
                    if _pn and _pn.lower() != "none":
                        poc_name = _pn

            pdf.set_font(font_family, "B", 12)
            pdf.cell(0, 7, sanitize_pdf_text("Invoice To:"), ln=1)
            pdf.set_font(font_family, "", 11)
            client_display = sanitize_pdf_text(summary.get("client", "Client Name"))
            # Always lead with POC name when available (the actual person to invoice)
            if poc_name:
                pdf.cell(0, 6, sanitize_pdf_text(poc_name), ln=1)
            if client_billing:
                # client_billing_details may contain multi-line info (name, address, GST, etc.)
                for line in client_billing.split("\n"):
                    line = line.strip()
                    if line:
                        pdf.cell(0, 6, sanitize_pdf_text(line), ln=1)
            else:
                if production_house and production_house.lower() not in ("none", ""):
                    pdf.cell(0, 6, sanitize_pdf_text(production_house), ln=1)
                elif not poc_name:
                    # Fallback to client/brand name only if we have nothing else
                    pdf.cell(0, 6, client_display, ln=1)

            pdf.ln(10)

            # Table Header
            pdf.set_font(font_family, "B", 10)
            pdf.set_fill_color(240, 240, 240)
            pdf.cell(12, 10, sanitize_pdf_text("Sr."), 1, 0, "C", True)
            pdf.cell(25, 10, sanitize_pdf_text("Date"), 1, 0, "C", True)
            pdf.cell(113, 10, sanitize_pdf_text("Particulars / Job Description"), 1, 0, "L", True)
            pdf.cell(40, 10, sanitize_pdf_text("Fees (INR)"), 1, 1, "R", True)

            # Table Rows
            pdf.set_font(font_family, "", 10)
            for idx, row in enumerate(client_data, 1):
                date_val = sanitize_pdf_text(str(row.get("job_date", row.get("Date", ""))).strip())
                job_val = sanitize_pdf_text(str(row.get("job_description_details", row.get("Job", ""))).strip())
                brand_val_raw = row.get("brand_name", "")
                brand_val = sanitize_pdf_text(brand_val_raw).strip()
                if brand_val and brand_val.lower() != "none":
                    job_val = f"{brand_val} - {job_val}" if job_val else brand_val
                fees_val = self._parse_fees(row.get("fees", row.get("Fees", "0")))

                # Dynamic height based on job description length
                pdf.cell(12, 10, sanitize_pdf_text(str(idx)), 1, 0, "C")
                pdf.cell(25, 10, date_val, 1, 0, "C")
                pdf.cell(113, 10, job_val, 1, 0, "L")
                pdf.cell(40, 10, sanitize_pdf_text(f"{fees_val:,.2f}"), 1, 1, "R")

            # Totals
            pdf.set_font(font_family, "B", 11)
            pdf.cell(150, 10, sanitize_pdf_text("TOTAL"), 1, 0, "R")
            pdf.cell(40, 10, sanitize_pdf_text(f"{summary.get('total', 0):,.2f}"), 1, 1, "R")

            pdf.ln(5)
            # In Words
            total = summary.get("total", 0)
            total_words = sanitize_pdf_text(num2words(total, lang="en_IN").capitalize())
            pdf.set_font(font_family, "I", 10)
            pdf.cell(0, 10, sanitize_pdf_text(f"Amount in Words: {total_words} Only"), ln=1)

            pdf.ln(10)

            # Footer / Bank Details
            bd = bank_details or {}
            pdf.set_font(font_family, "B", 10)
            pdf.cell(0, 6, sanitize_pdf_text("BANK ACCOUNT DETAILS:"), ln=1)
            pdf.set_font(font_family, "", 10)
            pdf.cell(0, 5, sanitize_pdf_text(f"Bank Name: {bd.get('bank_name') or '[Your Bank Name]'}"), ln=1)
            pdf.cell(0, 5, sanitize_pdf_text(f"Account Holder: {bd.get('bank_account_name') or '[Account Holder]'}"), ln=1)
            pdf.cell(0, 5, sanitize_pdf_text(f"Account Number: {bd.get('bank_account_number') or '[Your Account Number]'}"), ln=1)
            pdf.cell(0, 5, sanitize_pdf_text(f"IFSC Code: {bd.get('bank_ifsc') or '[Your IFSC Code]'}"), ln=1)
            if bd.get("upi_id"):
                pdf.cell(0, 5, sanitize_pdf_text(f"UPI ID: {bd['upi_id']}"), ln=1)

            pdf.ln(5)
            pdf.set_font(font_family, "B", 10)
            pdf.cell(0, 6, sanitize_pdf_text("Terms and Conditions:"), ln=1)
            pdf.set_font(font_family, "", 9)
            pdf.cell(
                0,
                5,
                sanitize_pdf_text("- Advance payments should be made within 2 working days from invoice date."),
                ln=1,
            )
            payee_name = (up.get("name") or bd.get("bank_account_name") or "the account holder")
            pdf.cell(
                0,
                5,
                sanitize_pdf_text(f"- Payment should be made in favor of '{payee_name}'."),
                ln=1,
            )

            # Save
            os.makedirs("output", exist_ok=True)
            safe_client = sanitize_pdf_text(summary.get("client", "Client")).replace(" ", "_")
            safe_month = sanitize_pdf_text(summary.get("month", "Period")).replace(" ", "_")
            output_filename = f"Invoice_{safe_client}_{safe_month}.pdf"
            output_path = os.path.join("output", output_filename)
            pdf.output(output_path)

            logger.info(f"PDF generated successfully manually with fpdf2: {output_path}")
            return output_path

        except Exception:
            logger.error("Failed to generate invoice PDF", exc_info=True)
            return None

    def _parse_fees(self, fees_str: str) -> float:
        try:
            clean = str(fees_str).replace("₹", "").replace(",", "").strip()
            return float(clean) if clean else 0.0
        except ValueError:
            return 0.0
