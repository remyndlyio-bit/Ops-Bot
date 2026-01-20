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

        from utils.logger import logger
        logger.info(f"[QUERY] Invoice Resolution Query - Bill={bill_no} | Client={client} | Month={month} | Year={params.get('year')}")
        logger.info(f"[QUERY] Searching through {len(all_records)} total records")

        matches = []

        # 0. Pre-clean all records to have stripped keys
        clean_records = []
        for row in all_records:
            clean_records.append({str(k).strip(): v for k, v in row.items()})

        # 1. Bill No Search
        if bill_no:
            bill_no_str = str(bill_no).strip()
            logger.info(f"[QUERY] Filtering by Bill No: '{bill_no_str}'")
            logger.info(f"[QUERY] Checking column: 'Bill No'")
            for row in clean_records:
                bill_value = str(row.get("Bill No", "")).strip()
                if bill_value == bill_no_str:
                    matches.append(row)
                    logger.info(f"[QUERY] Match found - Bill No: {bill_value}, Client: {row.get('Client Name', 'N/A')}")
            
            logger.info(f"[QUERY] Bill No search result: {len(matches)} match(es)")
            if matches:
                 # Resolve client name correctly for summary
                res_client = str(matches[0].get("Client Name") or matches[0].get("Production house") or client or "Client")
                logger.info(f"[QUERY] Invoice resolved successfully - Client: {res_client}, Matches: {len(matches)}")
                return {"status": "found", "data": matches, "client": res_client, "month": month or "Request"}

        # 2. Client + Month Search
        client_exists = False
        if client and month:
            search_term = client.strip().lower()
            month_matches = []
            from utils.date_utils import parse_sheet_date, month_name_to_number
            target_month = month_name_to_number(month)
            from datetime import datetime
            
            # Use provided year or current year (normalized to 2 digits for sheet comparison)
            year_val = params.get("year")
            target_year = year_val if (year_val and year_val != 0) else datetime.now().year
            if target_year > 2000: target_year -= 2000
            
            logger.info(f"[QUERY] Filtering by Client + Month - Client='{search_term}' | Month={target_month} | Year={target_year}")
            logger.info(f"[QUERY] Checking columns: 'Client Name', 'Production house' for client match")
            logger.info(f"[QUERY] Checking column: 'Date' for month/year match")
            
            client_matches_count = 0
            for row in clean_records:
                row_client = str(row.get("Client Name", "")).strip().lower()
                row_prod = str(row.get("Production house", "")).strip().lower()
                
                if search_term and (search_term in row_client or search_term in row_prod):
                    client_exists = True # We found at least one client record
                    client_matches_count += 1
                    
                    # Handle datetime objects that Google Sheets might return
                    row_date_value = row.get("Date")
                    if isinstance(row_date_value, datetime):
                        dt = row_date_value
                        row_date = dt.strftime('%Y-%m-%d')
                    else:
                        row_date = str(row_date_value).strip() if row_date_value else ""
                        if not row_date or row_date == 'None':
                            continue
                        dt = parse_sheet_date(row_date)
                    
                    if dt:
                        row_year = dt.year % 100 if dt.year >= 2000 else dt.year
                        if dt.month == target_month and row_year == target_year:
                            month_matches.append(row)
                            logger.info(f"[QUERY] Match found - Client: {row_client or row_prod}, Date: {row_date}, Month: {dt.month}, Year: {row_year}")
            
            logger.info(f"[QUERY] Client matches: {client_matches_count} | Month+Year matches: {len(month_matches)}")
            if month_matches:
                logger.info(f"[QUERY] Invoice resolved successfully - Client: {client}, Month: {month}, Matches: {len(month_matches)}")
                return {"status": "found", "data": month_matches, "client": client, "month": month}

        # 3. Decision Logic for Not Found
        if not client_exists and client:
            return {"status": "client_not_found", "message": f"I don't see any records for {client} in my current sheet."}
        
        if client_exists and client and month:
             return {"status": "invoice_not_found", "message": f"I found records for {client}, but no invoice matching {month} {target_year + 2000 if target_year < 100 else target_year}."}

        # 4. Multiple Matches Guard
        if len(matches) > 1:
            return {"status": "error", "message": "I found multiple invoices for these details. Could you specify the bill number or exact date?"}

        logger.warning(f"No invoice resolved for {client} in {month}")
        return {"status": "not_found", "message": "I couldn’t find an invoice matching those details."}
