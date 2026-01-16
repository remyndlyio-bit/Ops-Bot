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
        invoice_date_cols = []
        email_cols = []
        first_reminder_cols = []
        
        for col in columns:
            col_lower = col.lower().strip()
            # normalize to help detect headers like "Paid ?", "Payment-Date", "Date\t"
            col_norm = (
                col_lower.replace("\t", " ")
                .replace("-", " ")
                .replace("_", " ")
                .replace("?", " ")
            )
            col_norm = " ".join(col_norm.split())
            
            # Check for status/paid columns FIRST.
            # Accept variants like "Paid ?", "Paid?", "Payment status", etc.
            if (
                col_norm == "paid"
                or col_norm.startswith("paid ")
                or " paid" in f" {col_norm} "
                or "status" in col_norm
            ):
                status_cols.append(col)
            # Check for due date columns (contains "due" AND "date")
            elif "due" in col_norm and "date" in col_norm:
                due_date_cols.append(col)
            # Check for invoice date column (contains "date" but NOT due date and NOT payment date)
            elif "date" in col_norm and "payment" not in col_norm and "due" not in col_norm:
                invoice_date_cols.append(col)
            # Check for client columns (contains "client" or "production")
            elif "client" in col_norm or "production" in col_norm:
                client_cols.append(col)
            # Check for notes columns (contains "notes" or "additional")
            elif "notes" in col_norm or "additional" in col_norm:
                notes_cols.append(col)
            elif "email" in col_norm:
                email_cols.append(col)
            elif "firstremindersent" in col_norm.replace(" ", "") or "first reminder sent" in col_norm:
                first_reminder_cols.append(col)
        
        # Always ensure Notes and AdditionalNotes are in notes columns (case-insensitive check)
        for col in columns:
            col_lower = col.lower().strip()
            if col_lower in ["notes", "additionalnotes", "additional notes"] and col not in notes_cols:
                notes_cols.append(col)
        
        # Fallback: If no status columns found, look for "Paid" specifically (case-insensitive)
        if not status_cols:
            for col in columns:
                col_lower = col.lower().strip()
                if col_lower == "paid" or col_lower == "payment status":
                    status_cols.append(col)
        
        # Note: Payment-Date is NOT a due date - it's when payment was received
        # If no explicit due date column exists, we'll use Date + payment terms (default 30 days)
        # Don't add Payment-Date to due_date_cols
        
        # Fallback: If no invoice date column found, use a literal "Date" if present (any case)
        if not invoice_date_cols:
            for col in columns:
                if col.lower().strip().replace("\t", " ") == "date":
                    invoice_date_cols.append(col)
                    break

        # Fallback: If no client columns found, use common patterns
        if not client_cols:
            for col in columns:
                col_lower = col.lower().strip()
                if "client" in col_lower or ("production" in col_lower and "house" in col_lower):
                    client_cols.append(col)
        
        logger.info(
            "Column categorization - "
            f"Invoice Date: {invoice_date_cols}, Due Date: {due_date_cols}, "
            f"Status: {status_cols}, Client: {client_cols}, Notes: {notes_cols}, "
            f"Email: {email_cols}, FirstReminderSent: {first_reminder_cols}"
        )
        
        return {
            "invoice_date": invoice_date_cols,
            "due_date": due_date_cols,
            "status": status_cols,
            "client": client_cols,
            "notes": notes_cols,
            "email": email_cols,
            "first_reminder_sent": first_reminder_cols,
        }

    @staticmethod
    def _is_truthy(val: any) -> bool:
        s = str(val).strip().lower()
        return s in {"1", "true", "yes", "y", "sent"}

    @staticmethod
    def get_approaching_due_reminder_targets(
        all_records: List[Dict],
        approaching_days: int = 7,
        payment_terms_days: int = 0
    ) -> List[Dict]:
        """
        Returns rows that should receive a first payment reminder:
        - unpaid (Paid?/Paid/Status not truthy)
        - has Email
        - FirstReminderSent not truthy
        - due date is within [now, now + approaching_days]
        If no explicit due date column exists, uses Date column as the due date
        (optionally shifted by payment_terms_days if configured, default 0).

        Expected each record to include '_row' so we can update the sheet.
        """
        if not all_records:
            return []

        from datetime import timedelta
        now = datetime.now()
        window_end = now + timedelta(days=approaching_days)

        available_columns = list(all_records[0].keys())
        column_map = BusinessLogicService._get_column_names(all_available_columns=available_columns)
        has_due_date_column = len(column_map["due_date"]) > 0

        targets: List[Dict] = []

        for row in all_records:
            row_num = row.get("_row")
            if not row_num:
                continue

            # Skip if reminder already sent
            reminder_val = BusinessLogicService._find_column_value(row, column_map.get("first_reminder_sent", []))
            if reminder_val and BusinessLogicService._is_truthy(reminder_val):
                continue

            # Determine paid/unpaid
            status_val = BusinessLogicService._find_column_value(row, column_map["status"])
            # Treat missing/empty as unpaid; treat 1/true/yes as paid
            if status_val and BusinessLogicService.is_paid(status_val):
                continue

            # Email required
            email_val = BusinessLogicService._find_column_value(row, column_map.get("email", [])) or str(row.get("Email", "")).strip()
            if not email_val:
                continue

            client = BusinessLogicService._find_column_value(row, column_map["client"]) or "Unknown Client"

            # Due date
            due_date = None
            if has_due_date_column:
                due_raw = BusinessLogicService._find_column_value(row, column_map["due_date"])
                if due_raw:
                    due_date = parse_sheet_date(due_raw)
            else:
                invoice_date_raw = BusinessLogicService._find_column_value(row, column_map.get("invoice_date", []))
                if invoice_date_raw:
                    inv_dt = parse_sheet_date(invoice_date_raw)
                    if inv_dt:
                        due_date = inv_dt + timedelta(days=payment_terms_days)

            if not due_date:
                continue

            if now <= due_date <= window_end:
                # Get invoice number (Bill-No, Bill No, or BillNo)
                invoice_number = (
                    str(row.get("Bill-No", "")).strip() or 
                    str(row.get("Bill No", "")).strip() or 
                    str(row.get("BillNo", "")).strip() or 
                    "N/A"
                )
                
                # Get amount due (Fees column)
                fees_raw = str(row.get("Fees", "0")).strip()
                # Remove currency symbols and commas
                fees_clean = fees_raw.replace("₹", "").replace(",", "").replace("$", "").strip()
                try:
                    amount = float(fees_clean) if fees_clean else 0.0
                    amount_due = f"₹{amount:,.2f}" if amount > 0 else "₹0.00"
                except (ValueError, TypeError):
                    amount_due = fees_raw if fees_raw else "₹0.00"
                
                targets.append(
                    {
                        "_row": int(row_num),
                        "client": client,
                        "email": email_val,
                        "due_date": due_date,
                        "invoice_number": invoice_number,
                        "amount_due": amount_due,
                    }
                )

        return targets

    @staticmethod
    def _find_column_value(row: Dict, possible_names: List[str]) -> Optional[str]:
        """Finds the first matching column value from a list of possible column names."""
        # First try exact match (case-sensitive)
        for col_name in possible_names:
            if col_name in row:
                raw_val = row[col_name]
                # Handle None, empty string, but allow 0 as valid value
                if raw_val is None:
                    continue
                val = str(raw_val).strip()
                # Return even if "0" (unpaid status) - only skip if truly empty
                if val or val == "0":
                    return val
        
        # Then try case-insensitive match
        row_keys_lower = {k.lower().strip(): k for k in row.keys()}
        for col_name in possible_names:
            col_lower = col_name.lower().strip()
            # Also try matching with normalized punctuation (e.g., "paid?" matches "paid ?")
            col_normalized = col_lower.replace("?", "").replace(" ", "").strip()
            for row_key_lower, row_key_actual in row_keys_lower.items():
                row_key_normalized = row_key_lower.replace("?", "").replace(" ", "").strip()
                if col_lower == row_key_lower or col_normalized == row_key_normalized:
                    raw_val = row[row_key_actual]
                    if raw_val is None:
                        continue
                    val = str(raw_val).strip()
                    if val or val == "0":
                        return val
        
        return None

    @staticmethod
    def is_paid(val: any) -> bool:
        """Determines if a status value represents 'Paid'."""
        s = str(val).strip().lower()
        return s in ['paid', 'yes', 'y', 'true', '1']

    @staticmethod
    def get_overdue_invoices(all_records: List[Dict], payment_terms_days: int = 30) -> List[Dict]:
        """
        Returns invoices that are unpaid and past their due date.
        Uses configurable column names from COLUMN_NAMES env variable or auto-detects from sheet.
        If no explicit due date column exists, calculates due date as invoice Date + payment_terms_days.
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
        logger.info(
            f"[QUERY] Using columns - Status: {column_map['status']}, "
            f"Due Date: {column_map['due_date']}, Invoice Date: {column_map.get('invoice_date', [])}"
        )
        logger.info(f"[QUERY] Payment terms: {payment_terms_days} days (used if no due date column)")
        
        # Check if we have a due date column or need to calculate from Date
        has_due_date_column = len(column_map["due_date"]) > 0
        if not has_due_date_column:
            logger.info(f"[QUERY] No explicit due date column found - will calculate due date as Date + {payment_terms_days} days")
        
        checked_count = 0
        skipped_no_status = 0
        skipped_paid = 0
        skipped_no_date = 0
        skipped_date_parse_fail = 0
        
        for row_idx, row in enumerate(all_records, 1):
            checked_count += 1
            
            # Find status column - if empty/missing, treat as unpaid
            status_val = BusinessLogicService._find_column_value(row, column_map["status"])
            
            # If no status column was detected, try direct lookup of common status column names
            if not status_val and len(column_map["status"]) == 0:
                # Try common status column names directly
                for status_col_name in ["Paid", "paid", "PAID", "Status", "status", "STATUS", "Payment Status", "Paid?", "Paid ?"]:
                    if status_col_name in row:
                        raw_val = row[status_col_name]
                        status_val = str(raw_val).strip()
                        logger.info(f"[QUERY] Row {row_idx}: Found status column '{status_col_name}' with raw value: {repr(raw_val)}, string value: '{status_val}'")
                        if status_val:
                            break
                
                # If still no status found, treat as unpaid (empty Paid = unpaid)
                if not status_val:
                    logger.info(f"[QUERY] Row {row_idx}: No status value found - treating as unpaid")
            elif status_val:
                # Log what we found
                logger.info(f"[QUERY] Row {row_idx}: Status value found: '{status_val}'")
            
            # Check if paid - also handle numeric 0/1 values
            is_paid_status = False
            if status_val:
                # Handle numeric values: 0 = unpaid, 1 = paid
                if status_val in ['0', 0]:
                    is_paid_status = False
                    logger.info(f"[QUERY] Row {row_idx}: Status is '0' (numeric) - UNPAID")
                elif status_val in ['1', 1]:
                    is_paid_status = True
                    logger.info(f"[QUERY] Row {row_idx}: Status is '1' (numeric) - PAID")
                else:
                    is_paid_status = BusinessLogicService.is_paid(status_val)
                    logger.info(f"[QUERY] Row {row_idx}: Status '{status_val}' -> is_paid check: {is_paid_status}")
            
            if is_paid_status:
                skipped_paid += 1
                logger.info(f"[QUERY] Row {row_idx}: Skipping as PAID")
                continue
            else:
                logger.info(f"[QUERY] Row {row_idx}: Status indicates UNPAID - checking date")
            
            # Determine due date
            due_date = None
            due_date_str = None
            
            if has_due_date_column:
                # Use explicit due date column
                due_date_str = BusinessLogicService._find_column_value(row, column_map["due_date"])
                if due_date_str:
                    due_date = parse_sheet_date(due_date_str)
                    logger.info(f"[QUERY] Row {row_idx}: Using explicit due date: '{due_date_str}' -> {due_date}")
            else:
                # Calculate due date from invoice date + payment terms
                invoice_date_str = BusinessLogicService._find_column_value(row, column_map.get("invoice_date", []))
                logger.info(f"[QUERY] Row {row_idx}: Looking for invoice date in columns {column_map.get('invoice_date', [])}, found: '{invoice_date_str}'")
                
                if invoice_date_str:
                    invoice_date = parse_sheet_date(invoice_date_str)
                    if invoice_date:
                        from datetime import timedelta
                        due_date = invoice_date + timedelta(days=payment_terms_days)
                        due_date_str = f"{invoice_date_str} + {payment_terms_days} days"
                        logger.info(f"[QUERY] Row {row_idx}: Calculated due date: {invoice_date.strftime('%Y-%m-%d')} + {payment_terms_days} days = {due_date.strftime('%Y-%m-%d')}")
                    else:
                        logger.info(f"[QUERY] Row {row_idx}: Failed to parse invoice date '{invoice_date_str}'")
                        skipped_date_parse_fail += 1
                        continue
                else:
                    logger.info(f"[QUERY] Row {row_idx}: No invoice date found - checking available columns: {list(row.keys())}")
                    skipped_no_date += 1
                    continue
            
            # Check if overdue
            if due_date and due_date < now:
                client = BusinessLogicService._find_column_value(row, column_map["client"])
                overdue.append(row)
                logger.info(f"[QUERY] Row {row_idx}: ✅ OVERDUE invoice found - Client: {client or 'Unknown'}, Due Date: {due_date_str}, Calculated: {due_date.strftime('%Y-%m-%d')}, Today: {now.strftime('%Y-%m-%d')}")
            elif due_date:
                logger.info(f"[QUERY] Row {row_idx}: Not overdue - Due Date: {due_date.strftime('%Y-%m-%d')}, Today: {now.strftime('%Y-%m-%d')}")
            else:
                logger.info(f"[QUERY] Row {row_idx}: Could not determine due date")
                skipped_date_parse_fail += 1
        
        logger.info(f"[QUERY] Overdue query results - Total checked: {checked_count}, Overdue: {len(overdue)}, Skipped (paid): {skipped_paid}, Skipped (no date): {skipped_no_date}, Skipped (date parse fail): {skipped_date_parse_fail}")
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
    def format_overdue_invoices_response(overdue_invoices: List[Dict], payment_terms_days: int = 30) -> str:
        """
        Formats overdue invoices into a readable response.
        Includes client names, due dates, and notes if available.
        """
        if not overdue_invoices:
            return "Great news! I don't see any invoices that have passed their due date."
        
        # Extract column names from the first invoice
        available_columns = list(overdue_invoices[0].keys())
        column_map = BusinessLogicService._get_column_names(all_available_columns=available_columns)
        
        # Determine if we need to calculate due dates
        has_due_date_column = len(column_map["due_date"]) > 0
        
        if len(overdue_invoices) == 1:
            response_parts = [f"One invoice is past due:\n"]
        else:
            response_parts = [f"I found {len(overdue_invoices)} invoices that have passed their due date:\n"]
        
        for idx, invoice in enumerate(overdue_invoices, 1):
            # Get client name - try multiple column variations
            client = BusinessLogicService._find_column_value(invoice, column_map["client"])
            if not client:
                # Fallback: try direct column access with various name formats
                for col_name in ["Client Name", "Production-House", "Production house", "Client", "client"]:
                    if col_name in invoice:
                        client = str(invoice[col_name]).strip()
                        if client:
                            break
            
            client_display = client if client else "Unknown Client"
            
            # Get due date - calculate if needed
            due_date_display = None
            invoice_date_str = None
            
            if has_due_date_column:
                # Use explicit due date column
                due_date_str = BusinessLogicService._find_column_value(invoice, column_map["due_date"])
                if due_date_str:
                    from utils.date_utils import parse_sheet_date
                    due_date = parse_sheet_date(due_date_str)
                    if due_date:
                        due_date_display = due_date.strftime('%Y-%m-%d')
            else:
                # Calculate due date from invoice date + payment terms
                invoice_date_str = BusinessLogicService._find_column_value(invoice, column_map.get("invoice_date", []))
                if not invoice_date_str:
                    # Fallback: try direct "Date" column
                    invoice_date_str = invoice.get("Date") or invoice.get("date")
                
                if invoice_date_str:
                    from utils.date_utils import parse_sheet_date
                    from datetime import timedelta
                    invoice_date = parse_sheet_date(str(invoice_date_str))
                    if invoice_date:
                        due_date = invoice_date + timedelta(days=payment_terms_days)
                        due_date_display = due_date.strftime('%Y-%m-%d')
            
            if not due_date_display:
                due_date_display = "Unknown Date"
            
            # Get bill number if available
            bill_no = invoice.get("Bill-No") or invoice.get("Bill No") or invoice.get("BillNo")
            bill_display = f" (Bill #{bill_no})" if bill_no else ""
            
            # Get notes (check Notes and AdditionalNotes as fallback)
            notes = BusinessLogicService._find_column_value(invoice, column_map["notes"])
            notes_display = f"\n   Notes: {notes}" if notes else ""
            
            # Format response with client name prominently displayed
            if len(overdue_invoices) == 1:
                # Single invoice - show client name in the main message
                response_parts[0] = f"One invoice is past due for {client_display}:\n"
                response_parts.append(f"   Due Date: {due_date_display}{bill_display}{notes_display}")
            else:
                # Multiple invoices - list format
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
