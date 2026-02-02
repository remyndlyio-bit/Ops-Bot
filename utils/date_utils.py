from datetime import datetime
from utils.logger import logger
from typing import Optional, List, Tuple

def parse_sheet_date(date_str: str) -> Optional[datetime]:
    """
    Centrally parses Google Sheet dates.
    Primary format: YYYY-MM-DD
    Fallback formats: DD/MM/YY, DD/MM/YYYY (for backward compatibility)
    Returns a datetime object or None if parsing fails.
    """
    if not date_str:
        return None
    
    date_str = str(date_str).strip()
    
    # Try YYYY-MM-DD format first (new format)
    try:
        parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
        return parsed_date
    except ValueError:
        pass
    
    # Fallback to old formats for backward compatibility
    try:
        # Try DD/MM/YY format
        parsed_date = datetime.strptime(date_str, "%d/%m/%y")
        return parsed_date
    except ValueError:
        try:
            # Try DD/MM/YYYY format
            return datetime.strptime(date_str, "%d/%m/%Y")
        except ValueError:
            logger.warning(f"Failed to parse date string: {date_str}")
            return None

def month_name_to_number(month_name: str) -> Optional[int]:
    """Converts a month name (e.g. 'April') to its numeric value (1-12)."""
    if not month_name:
        return None
    
    try:
        # Try full name
        return datetime.strptime(month_name.strip().capitalize(), "%B").month
    except ValueError:
        try:
            # Try abbreviated name
            return datetime.strptime(month_name.strip().capitalize(), "%b").month
        except ValueError:
            return None


def number_to_month_name(month_num: int) -> str:
    """Converts month number (1-12) to full month name."""
    if not 1 <= month_num <= 12:
        return ""
    return datetime(2000, month_num, 1).strftime("%B")


def get_last_quarter_months() -> List[Tuple[int, int]]:
    """
    Returns list of (month_num, year) for the previous calendar quarter.
    E.g. if today is Feb 2026 -> [(10, 2025), (11, 2025), (12, 2025)].
    """
    now = datetime.now()
    current_month = now.month
    current_year = now.year
    # Q1=1-3, Q2=4-6, Q3=7-9, Q4=10-12
    if current_month <= 3:
        # Last quarter was Q4 of previous year
        return [(10, current_year - 1), (11, current_year - 1), (12, current_year - 1)]
    if current_month <= 6:
        return [(1, current_year), (2, current_year), (3, current_year)]
    if current_month <= 9:
        return [(4, current_year), (5, current_year), (6, current_year)]
    return [(7, current_year), (8, current_year), (9, current_year)]


def infer_year_for_month(month_name: str) -> int:
    """
    When user gives only a month (e.g. December), infer year.
    If we're in Jan and they said December, assume previous year; else current year.
    """
    now = datetime.now()
    month_num = month_name_to_number(month_name)
    if not month_num:
        return now.year
    if now.month == 1 and month_num == 12:
        return now.year - 1
    return now.year
