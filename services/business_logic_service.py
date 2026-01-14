from datetime import datetime
from typing import List, Dict, Optional
from utils.logger import logger
from utils.date_utils import parse_sheet_date
import os

class BusinessLogicService:
    @staticmethod
    def _get_column_names(all_available_columns: List[str] = None) -> Dict[str, List[str]]:
        """
        Gets column name mappings from COLUMN_NAMES env variable and intelligently categorizes them.
        If all_available_columns is provided, uses those; otherwise uses env variable.
        Always includes Notes and AdditionalNotes as fallback.
        """
        # Get comma-separated column names from env variable
        column_names_str = os.getenv("COLUMN_NAMES", "")
        
        if all_available_columns:
            # Use actual columns from the sheet
            columns = [col.strip() for col in all_available_columns if col.strip()]
        elif column_names_str:
            # Use columns from env variable
            columns = [col.strip() for col in column_names_str.split(",") if col.strip()]
        else:
            # Fallback to common defaults if nothing is set
            columns = ["Date", "Client Name", "Job", "Notes", "Language", "Production-House", 
                      "Studio", "Qt", "Length", "Fees", "Advance", "added 3rd Party cut", 
                      "Bill-No", "Bill-Sent", "Paid", "Payment-Date", "Payment-Followup", 
                      "Expense", "Payment-Details", "AdditionalNotes"]
        
        # Categorize columns intelligently based on their names
        due_date_cols = []
        status_cols = []
        client_cols = []
        notes_cols = []
        
        for col in columns:
            col_lower = col.lower().strip()
            
            # Check for due date columns (contains both "date" and ("due" or "payment"))
            if "date" in col_lower and ("due" in col_lower or "payment" in col_lower):
                due_date_cols.append(col)
            # Check for status/paid columns (exact match for "paid" or contains "status")
            elif col_lower == "paid" or "status" in col_lower:
                status_cols.append(col)
            # Check for client columns (contains "client" or "production")
            elif "client" in col_lower or "production" in col_lower:
                client_cols.append(col)
            # Check for notes columns (contains "notes" or "additional")
            elif "notes" in col_lower or "additional" in col_lower:
                notes_cols.append(col)
        
        # Always ensure Notes and AdditionalNotes are in notes columns (case-insensitive check)
        for col in columns:
            col_lower = col.lower().strip()
            if col_lower in ["notes", "additionalnotes", "additional notes"] and col not in notes_cols:
                notes_cols.append(col)
        
        # Fallback: If no due date columns found, try to find any date-related columns with payment context
        if not due_date_cols:
            for col in columns:
                col_lower = col.lower().strip()
                if "date" in col_lower and ("payment" in col_lower or "due" in col_lower):
                    due_date_cols.append(col)
        
        # Fallback: If no status columns found, look for "Paid" specifically (case-insensitive)
        if not status_cols:
            for col in columns:
                if col.lower().strip() == "paid":
                    status_cols.append(col)
        
        # Fallback: If no client columns found, use common patterns
        if not client_cols:
            for col in columns:
                col_lower = col.lower().strip()
                if "client" in col_lower or ("production" in col_lower and "house" in col_lower):
                    client_cols.append(col)
        
        logger.info(f"Column categorization - Due Date: {due_date_cols}, Status: {status_cols}, Client: {client_cols}, Notes: {notes_cols}")
        
        return {
            "due_date": due_date_cols,
            "status": status_cols,
            "client": client_cols,
            "notes": notes_cols
        }

    @staticmethod
    def _find_column_value(row: Dict, possible_names: List[str]) -> Optional[str]:
        """Finds the first matching column value from a list of possible column names."""
        # First try exact match (case-sensitive)
        for col_name in possible_names:
            if col_name in row:
                val = str(row[col_name]).strip()
                if val:
                    return val
        
        # Then try case-insensitive match
        row_keys_lower = {k.lower(): k for k in row.keys()}
        for col_name in possible_names:
            col_lower = col_name.lower()
            if col_lower in row_keys_lower:
                val = str(row[row_keys_lower[col_lower]]).strip()
                if val:
                    return val
        
        return None

    @staticmethod
    def is_paid(val: any) -> bool:
        """Determines if a status value represents 'Paid'."""
        s = str(val).strip().lower()
        return s in ['paid', 'yes', 'y', 'true', '1']

    @staticmethod
    def get_overdue_invoices(all_records: List[Dict]) -> List[Dict]:
        """
        Returns invoices that are unpaid and past their due date.
        Uses configurable column names from COLUMN_NAMES env variable or auto-detects from sheet.
        """
        if not all_records:
            logger.info("[QUERY] Overdue invoice query - No records to search")
            return []
        
        # Extract column names from the first record
        available_columns = list(all_records[0].keys())
        overdue = []
        now = datetime.now()
        column_map = BusinessLogicService._get_column_names(all_available_columns=available_columns)
        
        logger.info(f"[QUERY] Overdue Invoice Query - Searching {len(all_records)} records")
        logger.info(f"[QUERY] Using columns - Status: {column_map['status']}, Due Date: {column_map['due_date']}")
        logger.info(f"[QUERY] Filter criteria - Status: NOT paid, Due Date: < {now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        checked_count = 0
        skipped_no_status = 0
        skipped_paid = 0
        skipped_no_due_date = 0
        
        for row in all_records:
            checked_count += 1
            # Find status column
            status_val = BusinessLogicService._find_column_value(row, column_map["status"])
            if not status_val:
                skipped_no_status += 1
                continue
            
            # Check if paid
            if BusinessLogicService.is_paid(status_val):
                skipped_paid += 1
                continue
            
            # Find due date column
            due_date_str = BusinessLogicService._find_column_value(row, column_map["due_date"])
            if not due_date_str:
                skipped_no_due_date += 1
                continue
            
            # Parse and check if overdue
            due_date = parse_sheet_date(due_date_str)
            if due_date and due_date < now:
                client = BusinessLogicService._find_column_value(row, column_map["client"])
                overdue.append(row)
                logger.info(f"[QUERY] Overdue invoice found - Client: {client or 'Unknown'}, Due Date: {due_date_str}, Parsed: {due_date.strftime('%Y-%m-%d')}")
        
        logger.info(f"[QUERY] Overdue query results - Total checked: {checked_count}, Overdue: {len(overdue)}, Skipped (no status): {skipped_no_status}, Skipped (paid): {skipped_paid}, Skipped (no due date): {skipped_no_due_date}")
        return overdue

    @staticmethod
    def calculate_total_billing(all_records: List[Dict], period: str = "month") -> float:
        """
        Calculates total billing for a period (day/month/year).
        Uses 'Fees' column and 'Date' column (%d/%m/%y).
        """
        logger.info(f"[QUERY] Billing Calculation Query - Period: {period}, Records: {len(all_records)}")
        logger.info(f"[QUERY] Filtering by Date column, Aggregating Fees column")
        
        total = 0
        now = datetime.now()
        match_count = 0
        skipped_no_date = 0
        skipped_date_parse_fail = 0
        skipped_period_mismatch = 0
        
        for row in all_records:
            date_str = str(row.get('Date', '')).strip()
            if not date_str:
                skipped_no_date += 1
                continue
                
            dt = parse_sheet_date(date_str)
            if not dt:
                skipped_date_parse_fail += 1
                continue

            match = False
            if period == "day" and dt.date() == now.date(): 
                match = True
            elif period == "month" and dt.month == now.month and dt.year == now.year: 
                match = True
            elif period == "year" and dt.year == now.year: 
                match = True
            
            if match:
                # Aggregate using Fees column ONLY
                fees_raw = str(row.get('Fees', '0')).replace('₹', '').replace(',', '').strip()
                try:
                    val = float(fees_raw) if fees_raw else 0
                    total += val
                    match_count += 1
                except: 
                    continue
            else:
                skipped_period_mismatch += 1
        
        logger.info(f"[QUERY] Billing calculation results - Period: {period}, Matches: {match_count}, Total: {total}, Skipped (no date): {skipped_no_date}, Skipped (parse fail): {skipped_date_parse_fail}, Skipped (period mismatch): {skipped_period_mismatch}")
        return total

    @staticmethod
    def format_overdue_invoices_response(overdue_invoices: List[Dict]) -> str:
        """
        Formats overdue invoices into a readable response.
        Includes client names, due dates, and notes if available.
        """
        if not overdue_invoices:
            return "Great news! I don't see any invoices that have passed their due date."
        
        # Extract column names from the first invoice
        available_columns = list(overdue_invoices[0].keys())
        column_map = BusinessLogicService._get_column_names(all_available_columns=available_columns)
        response_parts = [f"I found {len(overdue_invoices)} invoice(s) that have passed their due date:\n"]
        
        for idx, invoice in enumerate(overdue_invoices, 1):
            # Get client name
            client = BusinessLogicService._find_column_value(invoice, column_map["client"])
            client_display = client if client else "Unknown Client"
            
            # Get due date
            due_date_str = BusinessLogicService._find_column_value(invoice, column_map["due_date"])
            due_date_display = due_date_str if due_date_str else "Unknown Date"
            
            # Get bill number if available
            bill_no = invoice.get("Bill-No") or invoice.get("Bill No") or invoice.get("BillNo")
            bill_display = f" (Bill #{bill_no})" if bill_no else ""
            
            # Get notes (check Notes and AdditionalNotes as fallback)
            notes = BusinessLogicService._find_column_value(invoice, column_map["notes"])
            notes_display = f"\n   Notes: {notes}" if notes else ""
            
            response_parts.append(f"{idx}. {client_display}{bill_display}")
            response_parts.append(f"   Due Date: {due_date_display}{notes_display}")
        
        return "\n".join(response_parts)

    @staticmethod
    def get_blacklisted_clients(all_records: List[Dict]) -> List[str]:
        """Clients with unpaid bills > 3 months old."""
        if not all_records:
            logger.info("[QUERY] Blacklist query - No records to search")
            return []
        
        # Extract column names from the first record
        available_columns = list(all_records[0].keys())
        blacklist = set()
        now = datetime.now()
        column_map = BusinessLogicService._get_column_names(all_available_columns=available_columns)
        
        logger.info(f"[QUERY] Blacklist Query - Searching {len(all_records)} records")
        logger.info(f"[QUERY] Using columns - Status: {column_map['status']}, Date: Date, Client: {column_map['client']}")
        logger.info(f"[QUERY] Filter criteria - Status: NOT paid, Date: > 3 months old")
        
        checked_count = 0
        skipped_paid = 0
        skipped_no_date = 0
        skipped_date_parse_fail = 0
        skipped_too_recent = 0
        
        for row in all_records:
            checked_count += 1
            status_val = BusinessLogicService._find_column_value(row, column_map["status"])
            if not status_val or BusinessLogicService.is_paid(status_val):
                skipped_paid += 1
                continue
            
            date_str = str(row.get('Date', '')).strip()
            if not date_str:
                skipped_no_date += 1
                continue
                
            dt = parse_sheet_date(date_str)
            if dt:
                months_diff = (now.year - dt.year) * 12 + now.month - dt.month
                if months_diff >= 3:
                    client = BusinessLogicService._find_column_value(row, column_map["client"])
                    if client:
                        blacklist.add(str(client))
                        logger.info(f"[QUERY] Blacklisted client found - Client: {client}, Date: {date_str}, Age: {months_diff} months")
                else:
                    skipped_too_recent += 1
            else:
                skipped_date_parse_fail += 1
        
        logger.info(f"[QUERY] Blacklist query results - Total checked: {checked_count}, Blacklisted clients: {len(blacklist)}, Skipped (paid): {skipped_paid}, Skipped (no date): {skipped_no_date}, Skipped (parse fail): {skipped_date_parse_fail}, Skipped (too recent): {skipped_too_recent}")
        return list(blacklist)
