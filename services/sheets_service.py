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
            if not self.credentials_path or not os.path.exists(self.credentials_path):
                # Fallback for Railway where we might use a JSON string in env
                import json
                creds_json = os.getenv("GOOGLE_CREDS_JSON")
                if creds_json:
                    creds_dict = json.loads(creds_json)
                    credentials = Credentials.from_service_account_info(creds_dict, scopes=self.scope)
                else:
                    logger.error("Google credentials not found.")
                    return None
            else:
                credentials = Credentials.from_service_account_file(self.credentials_path, scopes=self.scope)
            
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
