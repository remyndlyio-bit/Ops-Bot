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
        Coordinates the Three-Stage Architecture:
        1. Router: Classify message (action, chat, mixed)
        2. Action Parser: Extract intent JSON
        3. Backend Execution: Execute Sheets/Invoices
        4. Conversational Responder: Friendly reply
        """
        memory_context = self.memory.get_memory_context(user_id)
        
        # Stage 1: Router
        category = self.gemini.route_message(message)
        logger.info(f"Router category: {category}")

        action_result = "No specific action was required."
        trigger_invoice = False
        invoice_data = {}
        
        # Stage 2: Action Parser
        if category in ["action", "mixed"]:
            intent_data = self.gemini.parse_action(message, memory_context)
            action = intent_data.get("action")
            sheet = intent_data.get("sheet", "Leads")
            data = intent_data.get("data")

            # Stage 3: Backend Execution
            if action == "add_row" and data:
                action_result = self.sheets.add_row(sheet, data)
            elif action == "find_row" and data:
                query = data[0] if isinstance(data, list) else str(data)
                action_result = self.sheets.find_row(sheet, query)
            elif action == "update_row" and data:
                query = intent_data.get("query", "")
                action_result = self.sheets.update_row(sheet, query, data)
            elif action == "delete_row" and data:
                query = data[0] if isinstance(data, list) else str(data)
                action_result = self.sheets.delete_row(sheet, query)
            elif action == "summarize":
                # Handle general questions like "total number of clients"
                summary = self.sheets.get_sheet_summary(sheet)
                action_result = f"Data Summary: {json.dumps(summary)}"
            elif action in ["generate_invoice", "get_summary"]:
                client_name = intent_data.get("client_name")
                month = intent_data.get("month")
                if client_name and month:
                    from services.invoice_service import InvoiceService
                    sheet_data = self.sheets.get_invoice_data(client_name, month)
                    summary = InvoiceService.process_invoice_data(sheet_data, client_name, month)
                    action_result = InvoiceService.format_summary_message(summary)
                    if action == "generate_invoice" and summary.get("found"):
                        trigger_invoice = True
                        invoice_data = {"client_name": client_name, "month": month}
                else:
                    action_result = "I couldn't identify the client or month for the invoice."

            # Update last used sheet in memory
            self.memory.update_user_memory(user_id, {"last_sheet": sheet})

        # Stage 4: Conversational Response
        response = self.gemini.generate_response(message, action_result, memory_context)
        
        return {
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
