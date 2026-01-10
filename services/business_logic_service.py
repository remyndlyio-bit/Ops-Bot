from datetime import datetime
from typing import List, Dict
from utils.logger import logger
from utils.date_utils import parse_sheet_date

class BusinessLogicService:
    @staticmethod
    def get_overdue_invoices(all_records: List[Dict]) -> List[Dict]:
        """Returns invoices that are unpaid and past their due date."""
        overdue = []
        now = datetime.now()
        for row in all_records:
            status = str(row.get('Status', '')).lower()
            due_date_str = str(row.get('Due Date', ''))
            
            if status != 'paid' and due_date_str:
                due_date = parse_sheet_date(due_date_str)
                if due_date and due_date < now:
                    overdue.append(row)
        return overdue

    @staticmethod
    def calculate_total_billing(all_records: List[Dict], period: str = "month") -> float:
        """Calculates total billing for a period (day/month/year)."""
        total = 0
        now = datetime.now()
        match_count = 0
        
        for row in all_records:
            date_str = str(row.get('Date', ''))
            dt = parse_sheet_date(date_str)
            if not dt: continue

            match = False
            if period == "day" and dt.date() == now.date(): match = True
            elif period == "month" and dt.month == now.month and dt.year == now.year: match = True
            elif period == "year" and dt.year == now.year: match = True
            
            if match:
                fees_raw = str(row.get('Fees', '0')).replace('₹', '').replace(',', '').strip()
                try:
                    total += float(fees_raw) if fees_raw else 0
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
            status = str(row.get('Status', '')).lower()
            date_str = str(row.get('Date', ''))
            if status != 'paid' and date_str:
                dt = parse_sheet_date(date_str)
                if dt:
                    months_diff = (now.year - dt.year) * 12 + now.month - dt.month
                    if months_diff >= 3:
                        client = row.get('Production house') or row.get('Client name')
                        if client: blacklist.add(str(client))
        return list(blacklist)
