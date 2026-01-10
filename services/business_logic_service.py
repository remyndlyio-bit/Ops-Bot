from datetime import datetime
from typing import List, Dict
from utils.logger import logger
from utils.date_utils import parse_sheet_date

class BusinessLogicService:
    @staticmethod
    def is_paid(val: any) -> bool:
        """Determines if a status value represents 'Paid'."""
        s = str(val).strip().lower()
        return s in ['paid', 'yes', 'y', 'true']

    @staticmethod
    def get_overdue_invoices(all_records: List[Dict]) -> List[Dict]:
        """Returns invoices that are unpaid and past their due date."""
        overdue = []
        now = datetime.now()
        for row in all_records:
            status = row.get('Status', '')
            due_date_str = str(row.get('Due Date', '')).strip()
            
            if not BusinessLogicService.is_paid(status) and due_date_str:
                due_date = parse_sheet_date(due_date_str)
                if due_date and due_date < now:
                    overdue.append(row)
        return overdue

    @staticmethod
    def calculate_total_billing(all_records: List[Dict], period: str = "month") -> float:
        """
        Calculates total billing for a period (day/month/year).
        Uses 'Fees' column and 'Date' column (%d/%m/%y).
        """
        total = 0
        now = datetime.now()
        match_count = 0
        
        for row in all_records:
            date_str = str(row.get('Date', '')).strip()
            dt = parse_sheet_date(date_str)
            if not dt: continue

            match = False
            if period == "day" and dt.date() == now.date(): match = True
            elif period == "month" and dt.month == now.month and dt.year == now.year: match = True
            elif period == "year" and dt.year == now.year: match = True
            
            if match:
                # Aggregate using Fees column ONLY
                fees_raw = str(row.get('Fees', '0')).replace('₹', '').replace(',', '').strip()
                try:
                    val = float(fees_raw) if fees_raw else 0
                    total += val
                    match_count += 1
                except: continue
        
        logger.info(f"Billing Calc: Period={period} | Matches={match_count} | Total={total}")
        return total

    @staticmethod
    def get_blacklisted_clients(all_records: List[Dict]) -> List[str]:
        """Clients with unpaid bills > 3 months old."""
        blacklist = set()
        now = datetime.now()
        for row in all_records:
            status = row.get('Status', '')
            date_str = str(row.get('Date', '')).strip()
            if not BusinessLogicService.is_paid(status) and date_str:
                dt = parse_sheet_date(date_str)
                if dt:
                    months_diff = (now.year - dt.year) * 12 + now.month - dt.month
                    if months_diff >= 3:
                        # Use Client Name column for consistency
                        client = row.get('Client Name') or row.get('Production house')
                        if client: blacklist.add(str(client))
        return list(blacklist)
