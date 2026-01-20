import gspread
from google.oauth2.service_account import Credentials
import os
from typing import List, Dict, Optional, Any
from datetime import datetime
from utils.logger import logger

class SheetsService:
    def __init__(self):
        self.scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        self.sheet_url = os.getenv("SHEET_URL")
        self.client = self._authenticate()

    def _authenticate(self):
        try:
            # Flexible detection: Check if the variable is a path or the actual JSON content
            creds_raw = os.getenv("GOOGLE_CREDS_JSON")
            
            if not creds_raw:
                logger.error("Google credentials not found in GOOGLE_CREDS_JSON.")
                return None

            import json
            # If it looks like JSON, parse it directly from the string
            if creds_raw.strip().startswith("{"):
                creds_dict = json.loads(creds_raw)
                credentials = Credentials.from_service_account_info(creds_dict, scopes=self.scope)
                logger.info("Authenticated using JSON content from environment.")
            else:
                # Otherwise, treat it as a file path
                if os.path.exists(creds_raw):
                    credentials = Credentials.from_service_account_file(creds_raw, scopes=self.scope)
                    logger.info(f"Authenticated using file: {creds_raw}")
                else:
                    logger.error(f"Google credentials path does not exist: {creds_raw}")
                    return None
            
            return gspread.authorize(credentials)
        except Exception as e:
            logger.error(f"Failed to authenticate with Google Sheets: {e}")
            return None

    def get_invoice_data(self, client_name: str, month_name: str, year: int = None) -> List[Dict]:
        """
        Fetches rows from the sheet filtering by client, month and year.
        If client_name is None, returns all rows that match month/year.
        Primary match: 'Client Name' or 'Production house' columns.
        """
        from utils.date_utils import parse_sheet_date, month_name_to_number
        if not self.client:
            self.client = self._authenticate()
            if not self.client: return []

        try:
            sheet = self.client.open_by_url(self.sheet_url).sheet1
            # Normalize column names to handle trailing spaces (e.g., "Date ")
            all_records_raw = sheet.get_all_records()
            all_records = [{str(k).strip(): v for k, v in row.items()} for row in all_records_raw]
            
            target_month = month_name_to_number(month_name)
            from datetime import datetime
            target_year = year if (year and year != 0) else datetime.now().year
            # Support 2-digit year format common in sheets
            if target_year > 2000: target_year -= 2000
            
            search_term = client_name.strip().lower() if client_name else None
            
            logger.info(f"[QUERY] Invoice Data Query - Client: '{client_name}', Month: {month_name}, Year: {target_year + 2000 if target_year < 100 else target_year}")
            logger.info(f"[QUERY] Searching through {len(all_records)} total records (normalized column names)")
            logger.info(f"[QUERY] Filter Step 1: Client name match (columns: 'Client Name', 'Production house') - skipped if client is None")
            
            # Step 1: Filter by Client Name (Substring match) if provided
            if search_term:
                client_matches = []
                for row in all_records:
                    row_client_name = str(row.get('Client Name', '')).strip().lower()
                    row_prod_house = str(row.get('Production house', '')).strip().lower()
                    if search_term in row_client_name or search_term in row_prod_house:
                        client_matches.append(row)
                logger.info(f"[QUERY] Client name filter results: {len(client_matches)} matches")
            else:
                client_matches = all_records
                logger.info(f"[QUERY] Client name filter skipped (no client specified). Using all records: {len(client_matches)}")
            # Determine which date column to use from available columns
            date_column_name = None
            if client_matches:
                sample_row = client_matches[0]
                date_columns = [k for k in sample_row.keys() if 'date' in k.lower()]
                logger.info(f"[QUERY] Available date-related columns: {date_columns}")
                logger.info(f"[QUERY] Sample row keys: {list(sample_row.keys())[:10]}")
                
                # Prioritize date columns: job_date > Date > payment_date > any other date column
                for preferred_col in ['job_date', 'Date', 'Date ', 'date', 'payment_date']:
                    if preferred_col in sample_row:
                        date_column_name = preferred_col
                        break
                
                # If no preferred column found, use the first date column
                if not date_column_name and date_columns:
                    date_column_name = date_columns[0]
            
            if not date_column_name:
                logger.warning("[QUERY] No date column found in sheet records")
                return []
            
            logger.info(f"[QUERY] Filter Step 2: Date/Month match (column: '{date_column_name}', target: Month={target_month}, Year={target_year})")

            # Step 2: Filter by Date/Month
            filtered_data = []
            skipped_no_date = 0
            skipped_date_parse_fail = 0
            skipped_month_mismatch = 0
            
            for row in client_matches:
                # Use the discovered date column name
                # Also handle datetime objects that Google Sheets might return
                row_date_value = row.get(date_column_name)
                
                # If it's already a datetime object, use it directly
                if isinstance(row_date_value, datetime):
                    dt = row_date_value
                    row_date_str = dt.strftime('%Y-%m-%d')
                else:
                    # Convert to string and parse
                    row_date_str = str(row_date_value).strip() if row_date_value else ""
                    if not row_date_str or row_date_str == 'None':
                        skipped_no_date += 1
                        continue
                    
                    dt = parse_sheet_date(row_date_str)
                    if not dt:
                        skipped_date_parse_fail += 1
                        logger.debug(f"[QUERY] Failed to parse date: '{row_date_str}' (type: {type(row_date_value)})")
                        continue
                
                # Compare month and year (handle both 2-digit and 4-digit year formats)
                row_year = dt.year % 100 if dt.year >= 2000 else dt.year
                if dt.month == target_month and row_year == target_year:
                    filtered_data.append(row)
                    logger.info(f"[QUERY] Match found - Client: '{row.get('Client Name')}', Date: {row_date_str}, Parsed: {dt.strftime('%Y-%m-%d')}")
                else:
                    skipped_month_mismatch += 1
                    logger.debug(f"[QUERY] Month/Year mismatch - Date: {row_date_str}, Parsed: {dt.strftime('%Y-%m-%d')}, Expected: Month={target_month}, Year={target_year}, Got: Month={dt.month}, Year={row_year}")
            
            logger.info(f"[QUERY] Invoice data query results - Total records: {len(all_records)}, Client matches: {len(client_matches)}, Final matches: {len(filtered_data)}, Skipped (no date): {skipped_no_date}, Skipped (parse fail): {skipped_date_parse_fail}, Skipped (month mismatch): {skipped_month_mismatch}")
            return filtered_data
        except Exception as e:
            logger.error(f"Error fetching data from sheet: {e}")
            return []

    def get_all_records_with_row_numbers(self) -> List[Dict[str, Any]]:
        """
        Returns all records from sheet1 with normalized (stripped) keys and attaches the sheet row number
        as '_row' (2-based because row 1 is headers).
        """
        if not self.client:
            self.client = self._authenticate()
            if not self.client:
                return []

        try:
            sheet = self.client.open_by_url(self.sheet_url).sheet1
            all_records_raw = sheet.get_all_records()
            records: List[Dict[str, Any]] = []
            for idx, row in enumerate(all_records_raw, start=2):
                normalized = {str(k).strip(): v for k, v in row.items()}
                normalized["_row"] = idx
                records.append(normalized)
            return records
        except Exception as e:
            logger.error(f"Error fetching records with row numbers: {e}")
            return []

    def update_cell_by_header(self, row_number: int, header_name: str, value: Any) -> bool:
        """
        Updates a single cell in sheet1 by matching header_name (case/space-insensitive) in row 1.
        """
        if not self.client:
            self.client = self._authenticate()
            if not self.client:
                return False

        try:
            ws = self.client.open_by_url(self.sheet_url).sheet1
            headers = ws.row_values(1)
            # Map normalized header -> column index (1-based)
            header_map = {}
            for i, h in enumerate(headers, start=1):
                norm = str(h).strip().lower().replace(" ", "").replace("\t", "")
                header_map[norm] = i

            target_norm = str(header_name).strip().lower().replace(" ", "").replace("\t", "")
            col_idx: Optional[int] = header_map.get(target_norm)
            if not col_idx:
                logger.error(f"[SHEETS] Header not found: {header_name} (available={headers})")
                return False

            ws.update_cell(row_number, col_idx, value)
            logger.info(f"[SHEETS] Updated row={row_number} col={col_idx} ({header_name}) -> {value}")
            return True
        except Exception as e:
            logger.error(f"[SHEETS] Failed to update cell row={row_number} header={header_name}: {e}")
            return False

    def add_row(self, sheet_name: str, data: list):
        """Adds a new row to the specified sheet."""
        try:
            sheet = self.client.open_by_url(self.sheet_url).worksheet(sheet_name)
            sheet.append_row(data)
            return f"Successfully added row to {sheet_name}."
        except Exception as e:
            logger.error(f"Error adding row: {e}")
            return f"Error adding row: {str(e)}"

    def find_row(self, sheet_name: str, query: str):
        """Finds rows matching a query string in any column."""
        try:
            sheet = self.client.open_by_url(self.sheet_url).worksheet(sheet_name)
            all_records = sheet.get_all_records()
            results = [row for row in all_records if any(query.lower() in str(val).lower() for val in row.values())]
            return f"Found {len(results)} rows in {sheet_name}." if results else "No matching rows found."
        except Exception as e:
            logger.error(f"Error finding row: {e}")
            return f"Error finding row: {str(e)}"

    def update_row(self, sheet_name: str, query: str, data: dict):
        """Updates the first row matching query with new data."""
        try:
            sheet = self.client.open_by_url(self.sheet_url).worksheet(sheet_name)
            cell = sheet.find(query)
            if cell:
                # This is a simplified update logic for demonstration
                # Real implementation would need to match keys to columns
                headers = sheet.row_values(1)
                for key, value in data.items():
                    if key in headers:
                        col_idx = headers.index(key) + 1
                        sheet.update_cell(cell.row, col_idx, value)
                return f"Updated row {cell.row} in {sheet_name}."
            return "Row not found."
        except Exception as e:
            logger.error(f"Error updating row: {e}")
            return f"Error updating row: {str(e)}"

    def delete_row(self, sheet_name: str, query: str):
        """Deletes the first row matching query."""
        try:
            sheet = self.client.open_by_url(self.sheet_url).worksheet(sheet_name)
            cell = sheet.find(query)
            if cell:
                sheet.delete_rows(cell.row)
                return f"Deleted row {cell.row} from {sheet_name}."
            return "Row not found."
        except Exception as e:
            logger.error(f"Error deleting row: {e}")
            return f"Error deleting row: {str(e)}"

    def get_sheet_summary(self, sheet_name: str):
        """Returns a high-level summary of the sheet for metadata queries."""
        try:
            if not self.client: return "No sheet connection."
            spr = self.client.open_by_url(self.sheet_url)
            
            # Try to find the worksheet, fallback to first one if not found
            try:
                sheet = spr.worksheet(sheet_name)
            except:
                logger.info(f"Worksheet '{sheet_name}' not found, falling back to sheet1")
                sheet = spr.get_worksheet(0)
                sheet_name = sheet.title

            all_records = sheet.get_all_records()
            count = len(all_records)
            headers = [h.strip() for h in sheet.row_values(1)]
            
            # Extract unique client names if possible
            client_col = next((h for h in headers if h.lower() in ["client name", "production house", "client", "customer"]), None)
            clients = []
            if client_col:
                clients = sorted(list(set([str(r.get(client_col)).strip() for r in all_records if r.get(client_col)])))

            return {
                "active_sheet": sheet_name,
                "total_rows": count,
                "headers": headers,
                "unique_clients_count": len(clients),
                "unique_clients_sample": clients[:20]
            }
        except Exception as e:
            logger.error(f"Error getting sheet summary: {e}")
            return f"Error accessing sheet '{sheet_name}'. Please ensure it exists."

