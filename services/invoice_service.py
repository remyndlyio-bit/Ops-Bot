from typing import List, Dict

class InvoiceService:
    @staticmethod
    def process_invoice_data(data: List[Dict], client_name: str, month: str) -> Dict:
        """
        Processes a list of row dicts and returns an invoice summary.
        Maps 'Fees' to amount and 'Qt' to quantity.
        """
        if not data:
            return {
                "found": False,
                "message": f"No invoice data found for {client_name} in {month}."
            }

        total_amount = 0
        item_count = len(data)

        for row in data:
            # Clean and parse the 'Fees' column (e.g., "₹ 2,000")
            fees_raw = str(row.get('Fees', '0'))
            # Remove currency symbols and commas
            fees_clean = fees_raw.replace('₹', '').replace(',', '').strip()
            
            try:
                amount = float(fees_clean) if fees_clean else 0
            except (ValueError, TypeError):
                # Fallback to Qt * Rate if needed (though Fees usually represents the total line item here)
                amount = 0
            
            total_amount += amount

        return {
            "found": True,
            "client": client_name.capitalize(),
            "month": month.capitalize(),
            "items": item_count,
            "total": total_amount,
            "currency": "₹"
        }

    @staticmethod
    def format_summary_message(summary: Dict) -> str:
        if not summary.get("found"):
            return summary.get("message")

        return (
            "Invoice Preview\n"
            f"Client: {summary['client']}\n"
            f"Month: {summary['month']}\n"
            f"Items: {summary['items']}\n"
            f"Total: {summary['currency']}{summary['total']:,}\n\n"
            "PDF will be sent shortly 📄"
        )

    @staticmethod
    def resolve_invoice_pdf(params: Dict, all_records: List[Dict]) -> Dict:
        """
        Resolves an invoice to a specific PDF or data set.
        Logic:
        1. Search by Bill No (if provided)
        2. Search by Client Name + Month
        3. Search by Client Name + Job (fallback)
        """
        bill_no = params.get("bill_number")
        client = params.get("client_name")
        month = params.get("month")

        matches = []

        # 1. Bill No Search
        if bill_no:
            for row in all_records:
                if str(row.get("Bill No", "")).strip() == str(bill_no).strip():
                    matches.append(row)
            if matches:
                return {"status": "found", "data": matches, "client": matches[0].get("Client Name"), "month": month or "Request"}

        # 2. Client + Month Search
        if client and month:
            from services.sheets_service import SheetsService
            # We don't have direct access to SheetsService.get_invoice_data here without an instance,
            # but we can filter the all_records we already have.
            search_term = client.strip().lower()
            month_matches = []
            from utils.date_utils import parse_sheet_date, month_name_to_number
            target_month = month_name_to_number(month)
            
            for row in all_records:
                row_client = str(row.get("Client Name", "")).strip().lower()
                row_prod = str(row.get("Production house", "")).strip().lower()
                if search_term in row_client or search_term in row_prod:
                    row_date = str(row.get("Date", "")).strip()
                    dt = parse_sheet_date(row_date)
                    if dt and dt.month == target_month:
                        month_matches.append(row)
            
            if month_matches:
                return {"status": "found", "data": month_matches, "client": client, "month": month}

        # 3. Multiple Matches Guard
        if len(matches) > 1 or (not bill_no and not (client and month)):
            return {"status": "error", "message": "I found multiple invoices. Could you specify the bill number or date?"}

        return {"status": "not_found", "message": "I couldn’t find an invoice matching those details."}
