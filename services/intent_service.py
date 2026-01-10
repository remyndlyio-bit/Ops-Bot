from services.gemini_service import GeminiService
from services.sheets_service import SheetsService
from utils.memory_service import MemoryService
from utils.logger import logger
from typing import Dict

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
        params = result.get("parameters")
        entity = result.get("entity")

        # Defensive Validation
        is_valid = True
        fail_reason = ""
        
        if not operation:
            is_valid = False
            fail_reason = "Missing 'operation' field"
        elif operation == "UNKNOWN":
            is_valid = False
            fail_reason = "Operation is UNKNOWN"
        elif params is None:
            is_valid = False
            fail_reason = "Missing 'parameters' object"

        if not is_valid:
            logger.warning(f"Validation failed: {fail_reason} | Raw response: {result}")
            # Only ask for clarification if the query was truly un-parseable
            return {
                "operation": "UNKNOWN",
                "response": "I'm not quite sure what you'd like me to do. Could you please rephrase that?",
                "trigger_invoice": False
            }

        logger.info(f"Valid Operation: {operation} | Entity: {entity}")
        logger.info(f"Parameters: {params}")

        # 2. Execution
        action_result = "I don’t see this information in my records yet."
        trigger_invoice = False
        invoice_data = {}

        try:
            # Sheets data often needed
            all_records = []
            if operation in ["AGGREGATE_ENTITY", "READ_ENTITY"]:
                sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                all_records = sheet.get_all_records()

            if operation == "SMALL_TALK":
                action_result = "Hello! I'm your Operations Bot. How can I help you today?"
            
            elif operation == "READ_ENTITY":
                name = params.get("client_name")
                if entity == "bank_details":
                    action_result = "Our bank details: HDFC Bank, Acct: 12345678, IFSC: HDFC0001234." # Mock
                elif entity == "gst_details":
                    action_result = "GST Details: 27AAAAA0000A1Z5." # Mock
                elif "overdue" in message.lower():
                    overdue = logic.get_overdue_invoices(all_records)
                    action_result = f"Found {len(overdue)} overdue items."
                elif name:
                    results = [r for r in all_records if any(name.lower() in str(v).lower() for v in r.values())]
                    action_result = f"Found {len(results)} records matching '{name}'."
                else:
                    action_result = "I couldn’t find the specific record you’re looking for. Who is the client?"

            elif operation == "AGGREGATE_ENTITY":
                client = params.get("client_name")
                month = params.get("month")
                period = params.get("period") or "month"
                
                if entity in ["payment", "invoice", "client"] or "billing" in message.lower() or "amount" in message.lower():
                    if client and month:
                        from services.invoice_service import InvoiceService
                        data = self.sheets.get_invoice_data(client, month)
                        if not data:
                            action_result = f"I don’t see any billing records for {client} in {month} yet."
                        else:
                            summary = InvoiceService.process_invoice_data(data, client, month)
                            action_result = f"Total billing for {client} in {month} is {summary['currency']}{summary['total']:,}."
                    else:
                        total_sum = logic.calculate_total_billing(all_records, period)
                        action_result = f"Total billing for this {period} is ₹{total_sum:,.2f}."
                elif "outstanding" in message.lower():
                    total_out = sum([float(str(r.get('Fees', '0')).replace('₹', '').replace(',', '').strip() or 0) 
                                   for r in all_records if str(r.get('Status', '')).lower() != 'paid'])
                    action_result = f"Total outstanding balance is ₹{total_out:,.2f}."

            elif operation == "ACTION_TRIGGER":
                client = params.get("client_name")
                month = params.get("month")
                if client and month:
                    from services.invoice_service import InvoiceService
                    data = self.sheets.get_invoice_data(client, month)
                    summary = InvoiceService.process_invoice_data(data, client, month)
                    if summary.get("found"):
                        trigger_invoice = True
                        invoice_data = {"client_name": client, "month": month}
                        action_result = f"Generating invoice for {client} ({month})."
                    else:
                        action_result = f"No records found for {client} in {month}."

            elif operation == "SCHEDULE_REMINDER":
                name = params.get("client_name") or "the user"
                month = params.get("month") or "as requested"
                action_result = f"Reminder set for {name} regarding {month}."

        except Exception as e:
            logger.error(f"Execution failure: {e}")
            action_result = "I encountered an error accessing the data records."

        # 3. Final Response Phrasing
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
            "- 'Add a lead for John Doe at 555-0199 from NYC'\n"
            "- 'Find rows containing Nikkunj'\n"
            "- 'Delete the row for Xiaomi'\n"
            "How can I help you today?"
        )
