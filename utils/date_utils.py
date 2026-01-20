from datetime import datetime
from utils.logger import logger
from typing import Optional

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
