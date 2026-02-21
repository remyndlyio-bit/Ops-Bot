from datetime import datetime, timedelta
from utils.logger import logger
from typing import Optional, List, Tuple, Any


def normalize_date_for_write(value: Any, column_name: str = "") -> Tuple[Optional[str], Optional[str]]:
    """
    Normalize a date value to ISO format (YYYY-MM-DD) for writing to sheets.
    Handles natural language: today, yesterday, tomorrow, 15 Feb, Feb 15 2026, etc.
    
    Returns: (normalized_date, error_message)
    - If successful: ("2026-02-15", None)
    - If failed: (None, "Invalid date format...")
    """
    if value is None:
        return None, None
    
    original = str(value).strip()
    if not original:
        return None, None
    
    logger.info(f"[DATE_NORM] Input: '{original}' for column '{column_name}'")
    
    today = datetime.now().date()
    lower = original.lower().strip()
    
    # Handle relative dates
    if lower == "today":
        result = today.isoformat()
        logger.info(f"[DATE_NORM] 'today' -> {result}")
        return result, None
    if lower == "yesterday":
        result = (today - timedelta(days=1)).isoformat()
        logger.info(f"[DATE_NORM] 'yesterday' -> {result}")
        return result, None
    if lower == "tomorrow":
        result = (today + timedelta(days=1)).isoformat()
        logger.info(f"[DATE_NORM] 'tomorrow' -> {result}")
        return result, None
    
    # Already ISO format
    try:
        dt = datetime.strptime(original[:10], "%Y-%m-%d")
        result = dt.date().isoformat()
        logger.info(f"[DATE_NORM] ISO format -> {result}")
        return result, None
    except (ValueError, IndexError):
        pass
    
    # Common date formats to try
    formats = [
        "%d %b %Y",      # 15 Feb 2026
        "%d %B %Y",      # 15 February 2026
        "%b %d %Y",      # Feb 15 2026
        "%B %d %Y",      # February 15 2026
        "%d %b",         # 15 Feb (assume current year)
        "%d %B",         # 15 February
        "%b %d",         # Feb 15
        "%B %d",         # February 15
        "%d/%m/%Y",      # 15/02/2026
        "%d/%m/%y",      # 15/02/26
        "%m/%d/%Y",      # 02/15/2026
        "%d-%m-%Y",      # 15-02-2026
        "%d-%m-%y",      # 15-02-26
        "%Y/%m/%d",      # 2026/02/15
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(original, fmt)
            # If year not in format, use current year
            if "%Y" not in fmt and "%y" not in fmt:
                dt = dt.replace(year=today.year)
                # If the date is in the past by more than 6 months, assume next year
                if (today - dt.date()).days > 180:
                    dt = dt.replace(year=today.year + 1)
            result = dt.date().isoformat()
            logger.info(f"[DATE_NORM] Parsed with '{fmt}' -> {result}")
            return result, None
        except ValueError:
            continue
    
    # Try parsing with dateutil if available (more flexible)
    try:
        from dateutil import parser as dateutil_parser
        dt = dateutil_parser.parse(original, dayfirst=True, fuzzy=True)
        result = dt.date().isoformat()
        logger.info(f"[DATE_NORM] dateutil parsed -> {result}")
        return result, None
    except Exception:
        pass
    
    # Failed to parse
    logger.warning(f"[DATE_NORM] Failed to parse: '{original}'")
    return None, f"Invalid date format for '{column_name}': '{original}'. Please use a valid date (e.g., 'today', '15 Feb 2026', '2026-02-15')."


def is_date_column(column_name: str) -> bool:
    """Check if a column name indicates a date field."""
    if not column_name:
        return False
    lower = column_name.lower().replace(" ", "").replace("_", "")
    date_indicators = ["date", "day", "when", "created", "updated", "due", "start", "end", "time"]
    return any(ind in lower for ind in date_indicators)


def parse_sheet_date(date_str: str) -> Optional[datetime]:
    """
    Centrally parses Google Sheet dates.
    Handles: datetime objects, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS (from Sheets),
    DD/MM/YY, DD/MM/YYYY.
    Returns a datetime object or None if parsing fails.
    """
    if date_str is None:
        return None
    if isinstance(date_str, datetime):
        return date_str
    date_str = str(date_str).strip()
    if not date_str or date_str == "None":
        return None

    # Try YYYY-MM-DD format first (and first 10 chars when Sheets returns "YYYY-MM-DD HH:MM:SS")
    try:
        return datetime.strptime(date_str[:10], "%Y-%m-%d")
    except (ValueError, IndexError):
        pass
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        pass

    # Fallback to common formats for backward compatibility
    for fmt in ["%d/%m/%y", "%d/%m/%Y", "%d-%m-%Y", "%d-%m-%y"]:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
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
