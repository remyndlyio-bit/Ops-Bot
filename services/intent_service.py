from services.gemini_service import GeminiService
from services.sheets_service import SheetsService
from utils.memory_service import MemoryService
from utils.logger import logger
from typing import Dict
import json

class IntentService:
    def __init__(self):
        self.gemini = GeminiService()
        self.sheets = SheetsService()
        self.memory = MemoryService()

    def process_request(self, user_id: str, message: str) -> Dict:
        """
        Coordinates the Operations Architecture using single-call Gemini parsing.
        """
        from services.business_logic_service import BusinessLogicService
        logic = BusinessLogicService()

        # 1. Single-call Intent & Parameter Parsing
        result = self.gemini.parse_user_intent(message)
        
        # Validation Layer
        operation = result.get("operation")
        params = result.get("parameters", {})
        entity = result.get("entity")

        # Log the parsed intent for debugging
        logger.info(f"Parsed intent - Operation: {operation}, Entity: {entity}, Params: {params}")

        # Handle explicit Gemini API errors
        if operation == "GEMINI_ERROR":
            error_msg = result.get("error_message", "Unknown Gemini API error")
            return {
                "operation": "GEMINI_ERROR",
                "response": error_msg,
                "trigger_invoice": False
            }

        # 2. Execution
        action_result = "I don't see this information in my records yet."
        trigger_invoice = False
        invoice_data = {}

        try:
            # Check for invoice retrieval FIRST (before operation-based logic)
            # This ensures invoice requests are handled regardless of how Gemini categorizes them
            is_retrieval_query = any(word in message.lower() for word in ["get", "download", "send", "give", "show", "retrieve", "fetch", "can you get"])
            has_invoice_keyword = entity == "invoice" or "invoice" in message.lower()
            is_action_trigger_invoice = operation == "ACTION_TRIGGER" and entity == "invoice"
            
            # High-Priority Retrieval Path (Handle "Get me X Invoice")
            # Also handle ACTION_TRIGGER with invoice entity as retrieval
            if (is_retrieval_query and has_invoice_keyword) or is_action_trigger_invoice:
                logger.info(f"Detected invoice retrieval query - fetching records")
                # Fetch records for invoice retrieval
                all_records = []
                try:
                    sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                    all_records = sheet.get_all_records()
                    logger.info(f"Fetched {len(all_records)} records from sheet for invoice retrieval.")
                except Exception as se:
                    logger.error(f"Sheet access failed: {se}")
                    return {
                        "operation": operation,
                        "response": "I encountered an error accessing the data records.",
                        "trigger_invoice": False
                    }
                
                from services.invoice_service import InvoiceService
                resolved = InvoiceService.resolve_invoice_pdf(params, all_records)
                
                if resolved["status"] == "found":
                    trigger_invoice = True
                    invoice_data = {
                        "client_name": resolved["client"],
                        "month": resolved["month"],
                        "bill_number": params.get("bill_number"),
                        "year": params.get("year")
                    }
                    action_result = f"Confirmed! I've found the record for {resolved['client']}. I'm generating the invoice now... 📄"
                    logger.info(f"Invoice retrieval successful - Client: {resolved['client']}, Month: {resolved['month']}")
                else:
                    action_result = resolved["message"]
                    logger.info(f"Invoice retrieval failed - {resolved.get('status', 'unknown')}: {action_result}")
                    # If it's a retrieval query and we didn't find it, we stop here.
                    return {
                        "operation": operation,
                        "response": action_result,
                        "trigger_invoice": False
                    }
            
            # Handle SMALL_TALK only if not an invoice retrieval
            elif operation == "SMALL_TALK":
                return {
                    "operation": "SMALL_TALK",
                    "response": "Hello! I'm your Operations Bot. How can I help you today?",
                    "trigger_invoice": False
                }

            # Sheets data often needed for other operations
            all_records = []
            if operation in ["AGGREGATE_ENTITY", "READ_ENTITY", "ACTION_TRIGGER"]:
                try:
                    sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                    all_records = sheet.get_all_records()
                    logger.info(f"Fetched {len(all_records)} records from sheet.")
                except Exception as se:
                    logger.error(f"Sheet access failed: {se}")

            # 2. Standard Operation Path (only if not already handled by retrieval)
            if action_result == "I don't see this information in my records yet.":
                from services.invoice_service import InvoiceService
                resolved = InvoiceService.resolve_invoice_pdf(params, all_records)
                
                if resolved["status"] == "found":
                    trigger_invoice = True
                    invoice_data = {
                        "client_name": resolved["client"],
                        "month": resolved["month"],
                        "bill_number": params.get("bill_number"),
                        "year": params.get("year")
                    }
                    action_result = f"Confirmed! I've found the record for {resolved['client']}. I'm generating the invoice now... 📄"
                else:
                    action_result = resolved["message"]
                    # If it's a retrieval query and we didn't find it, we stop here.
                    return {
                        "operation": operation,
                        "response": action_result,
                        "trigger_invoice": False
                    }

            # 2. Standard Operation Path
            if action_result == "I don't see this information in my records yet.":
                if operation == "READ_ENTITY":
                    name = params.get("client_name")
                    if entity == "bank_details":
                        action_result = "Our bank details: HDFC Bank, Acct: 12345678, IFSC: HDFC0001234."
                    elif name:
                        results = [r for r in all_records if any(name.lower() in str(v).lower() for v in r.values())]
                        if results:
                            action_result = f"Found {len(results)} records matching '{name}'."
                        else:
                            action_result = f"I don't see any records for {name} in my current sheet."
                
                elif operation == "AGGREGATE_ENTITY":
                    client = params.get("client_name")
                    month = params.get("month")
                    if client and month:
                        from services.invoice_service import InvoiceService
                        data = self.sheets.get_invoice_data(client, month, year=params.get("year"))
                        if not data:
                            action_result = f"I don't see any billing records for {client} in {month} yet."
                        else:
                            summary = InvoiceService.process_invoice_data(data, client, month)
                            action_result = f"Total billing for {client} in {month} is {summary['currency']}{summary['total']:,}."
                    else:
                         action_result = "I couldn't calculate the aggregate. Please specify a client and month."

        except Exception as e:
            logger.error(f"Execution failure: {e}")
            action_result = "I encountered an error accessing the data records."

        # 3. Final Response Phrasing
        if trigger_invoice:
            response = action_result # Use the "Confirmed!" message directly
        elif action_result.startswith("I don't see") or "error" in action_result.lower():
            response = action_result
        else:
            response = self.gemini.generate_response(message, action_result)

        return {
            "operation": operation,
            "parameters": params,
            "response": response,
            "trigger_invoice": trigger_invoice,
            "invoice_data": invoice_data
        }

    @staticmethod
    def get_help_text() -> str:
        return (
            "I'm your conversational assistant! You can naturally ask me to:\n"
            "- 'Add a lead for John Doe'\n"
            "- 'Get me Garnier invoice for April'\n"
            "How can I help you today?"
        )
