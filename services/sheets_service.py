import gspread
from google.oauth2.service_account import Credentials
import os
from typing import List, Dict
from utils.logger import logger

class SheetsService:
    def __init__(self):
        self.scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        self.credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        self.sheet_url = os.getenv("SHEET_URL")
        self.client = self._authenticate()

    def _authenticate(self):
        try:
            # Flexible detection: Check if the variable is a path or the actual JSON content
            creds_raw = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.getenv("GOOGLE_CREDS_JSON")
            
            if not creds_raw:
                logger.error("Google credentials not found in environment variables.")
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

    def get_invoice_data(self, client_name: str, month_name: str) -> List[Dict]:
        """
        Fetches rows from the sheet filtering by client and month.
        Priority: 1. Normalize Client Name, 2. Filter by Date/Month.
        """
        from utils.date_utils import parse_sheet_date, month_name_to_number
        if not self.client:
            self.client = self._authenticate()
            if not self.client: return []

        try:
            sheet = self.client.open_by_url(self.sheet_url).sheet1
            all_records = sheet.get_all_records()
            
            target_month = month_name_to_number(month_name)
            search_term = client_name.strip().lower()
            
            # Step 1: Normalize and Filter by Client Name
            client_matches = []
            for row in all_records:
                clean_row = {str(k).strip(): v for k, v in row.items()}
                col_client_name = next((v for k, v in clean_row.items() if "client" in k.lower()), "")
                col_prod_house = next((v for k, v in clean_row.items() if "production" in k.lower()), "")
                
                row_client_name = str(col_client_name).strip().lower()
                row_prod_house = str(col_prod_house).strip().lower()

                if search_term in row_client_name or search_term in row_prod_house:
                    client_matches.append(clean_row)

            # Step 2: Filter by Date/Month
            filtered_data = []
            for row in client_matches:
                row_date_str = str(row.get('Date', '')).strip()
                dt = parse_sheet_date(row_date_str)
                
                if dt:
                    row_month = dt.month
                    # Match across all years if only month is provided
                    if row_month == target_month:
                        filtered_data.append(row)
                        # Debug Log
                        logger.info(f"MATCH: Raw='{row_date_str}' | Parsed={dt} | Month={row_month}")
                else:
                    logger.warning(f"SKIP: Invalid date format in sheet: '{row_date_str}'")

            logger.info(f"Result: {len(all_records)} total rows -> {len(client_matches)} client matches -> {len(filtered_data)} date matches")
            return filtered_data
        except Exception as e:
            logger.error(f"Error fetching data from sheet: {e}")
            return []

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

