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

    def _get_rule_based_intent(self, message: str) -> str:
        """Fast rule-based detection for common intents."""
        msg = message.lower().strip()
        if any(word in msg for word in ["hi", "hello", "thanks", "thank you"]):
            return "SMALL_TALK"
        if any(word in msg for word in ["total", "sum", "how many", "billing", "outstanding"]):
            return "AGGREGATE_ENTITY"
        if "invoice" in msg and any(word in msg for word in ["make", "generate", "create"]):
            return "ACTION_TRIGGER"
        if any(word in msg for word in ["remind", "schedule", "call him", "follow up"]):
            return "SCHEDULE_REMINDER"
        return None

    def process_request(self, user_id: str, message: str) -> Dict:
        """
        Coordinates the Operations Architecture.
        """
        from services.business_logic_service import BusinessLogicService
        logic = BusinessLogicService()

        # 1. Intent Detection
        operation = self._get_rule_based_intent(message)
        if not operation:
            operation = self.gemini.classify_intent(message)
        
        logger.info(f"Operation identified: {operation}")

        # 2. Parameter Extraction
        params = self.gemini.extract_parameters(message)
        logger.info(f"Parameters extracted: {params}")

        # 3. Execution
        action_result = "I don’t see this information in my records yet."
        trigger_invoice = False
        invoice_data = {}

        try:
            # Sheets data often needed for aggregations
            all_records = []
            if operation in ["AGGREGATE_ENTITY", "READ_ENTITY"]:
                # Try primary sheet first
                sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                all_records = sheet.get_all_records()

            if operation == "SMALL_TALK":
                action_result = "Hello! I'm your Operations Bot. I can help you with billing, invoices, and tracking your sheet data."
            
            elif operation == "READ_ENTITY":
                # Implementation for lookup
                name = params.get("names", [None])[0]
                entity = params.get("entities", [""])[0]
                
                if "overdue" in message.lower():
                    overdue = logic.get_overdue_invoices(all_records)
                    action_result = f"Found {len(overdue)} overdue items."
                elif "blacklist" in message.lower():
                    blacklist = logic.get_blacklisted_clients(all_records)
                    action_result = f"Blacklisted clients (>3m unpaid): {', '.join(blacklist) or 'None'}"
                elif name:
                    # Specific client lookup
                    results = [r for r in all_records if any(name.lower() in str(v).lower() for v in r.values())]
                    action_result = f"Found {len(results)} records for {name}."
                else:
                    action_result = "I don’t see specific lookup details in your request. Could you provide a name?"

            elif operation == "AGGREGATE_ENTITY":
                client = params.get("names", [None])[0]
                month = params.get("month") or (params.get("time_ranges", [])[0] if params.get("time_ranges") else None)
                
                period = "month"
                if "day" in message.lower(): period = "day"
                elif "year" in message.lower(): period = "year"
                
                if ("billing" in message.lower() or "sum" in message.lower() or "amount" in message.lower()):
                    if client and month:
                        from services.invoice_service import InvoiceService
                        data = self.sheets.get_invoice_data(client, month)
                        # We return "Not in records" only if No rows matched the filters
                        if not data:
                            action_result = f"I don’t see any billing records for {client} in {month} yet."
                        else:
                            summary = InvoiceService.process_invoice_data(data, client, month)
                            # If rows exist, return the total even if it is 0
                            action_result = f"Total billing for {client} in {month} is {summary['currency']}{summary['total']:,}."
                    else:
                        total_sum = logic.calculate_total_billing(all_records, period)
                        action_result = f"Total billing for this {period} is ₹{total_sum:,.2f}."
                elif "outstanding" in message.lower():
                    # Simplified outstanding calculation
                    total_out = sum([float(str(r.get('Fees', '0')).replace('₹', '').replace(',', '').strip() or 0) 
                                   for r in all_records if str(r.get('Status', '')).lower() != 'paid'])
                    action_result = f"Total outstanding balance is ₹{total_out:,.2f}."

            elif operation == "ACTION_TRIGGER":
                client = params.get("names", [None])[0]
                month = params.get("month") or params.get("time_ranges", [None])[0]
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
                date = params.get("dates", [None])[0] or "tomorrow"
                name = params.get("names", [None])[0] or "this task"
                action_result = f"Reminder set for {name} regarding {date}."

        except Exception as e:
            logger.error(f"Execution failure: {e}")
            action_result = "I encountered an error accessing the data records."

        # 4. Final Response Phrasing
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
