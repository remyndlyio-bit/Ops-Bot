from fpdf import FPDF
import os
from datetime import datetime
from num2words import num2words
from typing import List, Dict
from utils.logger import logger

class InvoiceGenerationService:
    def __init__(self):
        pass

    def generate_pdf(self, summary: Dict, client_data: List[Dict]) -> str:
        """
        Generates a PDF using fpdf2.
        """
        try:
            pdf = FPDF()
            pdf.add_page()
            pdf.set_auto_page_break(auto=True, margin=15)
            
            # Colors
            primary_color = (0, 0, 0)
            gray_color = (128, 128, 128)
            
            # Header - Name & Details
            pdf.set_font("Helvetica", "B", 20)
            pdf.cell(100, 10, "Darshit Mody", ln=0)
            
            pdf.set_font("Helvetica", "B", 12)
            pdf.set_text_color(*gray_color)
            pdf.cell(90, 10, "INVOICE", ln=1, align="R")
            pdf.set_text_color(*primary_color)
            
            pdf.set_font("Helvetica", "", 10)
            pdf.cell(100, 5, "Voice Over Artist", ln=0)
            pdf.cell(90, 5, f"Invoice #: {datetime.now().strftime('%y%m%d')}-{summary['client'][:3].upper()}", ln=1, align="R")
            
            pdf.cell(100, 5, "Residence Address: [Your Address]", ln=0)
            pdf.cell(90, 5, f"Date: {datetime.now().strftime('%d-%m-%Y')}", ln=1, align="R")
            
            pdf.cell(100, 5, "Email: [Your Email]", ln=0)
            pdf.cell(90, 5, "Terms: Immediate", ln=1, align="R")
            
            pdf.ln(10)
            
            # Client Info
            pdf.set_font("Helvetica", "B", 12)
            pdf.cell(0, 7, "Invoice To:", ln=1)
            pdf.set_font("Helvetica", "", 11)
            pdf.cell(0, 6, summary.get("client", "Client Name"), ln=1)
            pdf.cell(0, 6, "Production House", ln=1)
            
            pdf.ln(10)
            
            # Table Header
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_fill_color(240, 240, 240)
            pdf.cell(12, 10, "Sr.", 1, 0, "C", True)
            pdf.cell(25, 10, "Date", 1, 0, "C", True)
            pdf.cell(113, 10, "Particulars / Job Description", 1, 0, "L", True)
            pdf.cell(40, 10, "Fees (INR)", 1, 1, "R", True)
            
            # Table Rows
            pdf.set_font("Helvetica", "", 10)
            for idx, row in enumerate(client_data, 1):
                date_val = str(row.get("Date", "")).strip()
                job_val = str(row.get("Job", "")).strip()
                fees_val = self._parse_fees(row.get("Fees", "0"))
                
                # Dynamic height based on job description length
                pdf.cell(12, 10, str(idx), 1, 0, "C")
                pdf.cell(25, 10, date_val, 1, 0, "C")
                pdf.cell(113, 10, job_val, 1, 0, "L")
                pdf.cell(40, 10, f"{fees_val:,.2f}", 1, 1, "R")
            
            # Totals
            pdf.set_font("Helvetica", "B", 11)
            pdf.cell(150, 10, "TOTAL", 1, 0, "R")
            pdf.cell(40, 10, f"{summary.get('total', 0):,.2f}", 1, 1, "R")
            
            pdf.ln(5)
            # In Words
            total = summary.get("total", 0)
            total_words = num2words(total, lang='en_IN').capitalize()
            pdf.set_font("Helvetica", "I", 10)
            pdf.cell(0, 10, f"Amount in Words: {total_words} Only", ln=1)
            
            pdf.ln(10)
            
            # Footer / Bank Details
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 6, "BANK ACCOUNT DETAILS:", ln=1)
            pdf.set_font("Helvetica", "", 10)
            pdf.cell(0, 5, "Bank Name: [Your Bank Name]", ln=1)
            pdf.cell(0, 5, "Account Holder: Darshit Mody", ln=1)
            pdf.cell(0, 5, "Account Number: [Your Account Number]", ln=1)
            pdf.cell(0, 5, "IFSC Code: [Your IFSC Code]", ln=1)
            
            pdf.ln(5)
            pdf.set_font("Helvetica", "B", 10)
            pdf.cell(0, 6, "Terms and Conditions:", ln=1)
            pdf.set_font("Helvetica", "", 9)
            pdf.cell(0, 5, "- Advance payments should be made within 2 working days from invoice date.", ln=1)
            pdf.cell(0, 5, "- Payment should be made in favor of 'Darshit Mody'.", ln=1)
            
            # Save
            os.makedirs("output", exist_ok=True)
            output_filename = f"Invoice_{summary['client']}_{summary['month']}.pdf".replace(" ", "_")
            output_path = os.path.join("output", output_filename)
            pdf.output(output_path)
            
            logger.info(f"PDF generated successfully manually with fpdf2: {output_path}")
            return output_path

        except Exception as e:
            logger.error(f"Failed to generate invoice PDF with fpdf2: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _parse_fees(self, fees_str: str) -> float:
        try:
            clean = str(fees_str).replace('₹', '').replace(',', '').strip()
            return float(clean) if clean else 0.0
        except ValueError:
            return 0.0
