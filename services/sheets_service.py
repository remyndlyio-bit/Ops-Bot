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
        Fetches rows from the sheet filtering by 'Production house' and deriving month from 'Date'.
        """
        if not self.client:
            self.client = self._authenticate()
            if not self.client:
                return []

        try:
            from datetime import datetime
            sheet = self.client.open_by_url(self.sheet_url).sheet1
            all_records = sheet.get_all_records()
            if all_records:
                logger.info(f"Sheet Headers found: {list(all_records[0].keys())}")
            
            filtered_data = []
            for row in all_records:
                # Create a normalized version of the row with stripped keys
                clean_row = {str(k).strip(): v for k, v in row.items()}
                
                # 1. Dynamic Header Discovery
                # We search for keys that contain certain words to be extra safe
                col_client_name = next((v for k, v in clean_row.items() if "client" in k.lower()), None)
                col_prod_house = next((v for k, v in clean_row.items() if "production" in k.lower()), None)
                
                row_client_name = str(col_client_name).strip().lower() if col_client_name else ""
                row_prod_house = str(col_prod_house).strip().lower() if col_prod_house else ""
                
                # 2. Check Month (Derived from Date column DD/MM/YY)
                row_date_str = str(clean_row.get('Date', '')).strip()
                row_month = ""
                try:
                    if "/" in row_date_str:
                        parts = row_date_str.split('/')
                        if len(parts[2]) == 2:
                            dt = datetime.strptime(row_date_str, "%d/%m/%y")
                        else:
                            dt = datetime.strptime(row_date_str, "%d/%m/%Y")
                        row_month = dt.strftime("%B").lower()
                except Exception as e:
                    continue

                # Case-insensitive comparison
                search_term = client_name.strip().lower()
                month_term = month_name.strip().lower()

                if (row_client_name == search_term or row_prod_house == search_term) and row_month == month_term:
                    filtered_data.append(clean_row)
            
            if not filtered_data and all_records:
                # Log first row sample to see why it didn't match
                sample = {str(k).strip(): v for k, v in all_records[0].items()}
                logger.info(f"No match found. Sample row keys: {list(sample.keys())}")
                logger.info(f"Sample row values: {list(sample.values())}")

            logger.info(f"Fetched {len(filtered_data)} rows for {client_name} in {month_name}")
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

