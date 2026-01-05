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
