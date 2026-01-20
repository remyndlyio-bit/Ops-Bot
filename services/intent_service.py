from services.gemini_service import GeminiService
from services.sheets_service import SheetsService
from services.gmail_service import GmailService
from utils.memory_service import MemoryService
from utils.logger import logger
from typing import Dict
import json

class IntentService:
    def __init__(self):
        self.gemini = GeminiService()
        self.sheets = SheetsService()
        self.email = GmailService()
        self.memory = MemoryService()

    def _store_conversation(self, user_id: str, user_message: str, bot_response: str):
        """Helper method to store user message and bot response in conversation history."""
        self.memory.add_message(user_id, "user", user_message)
        self.memory.add_message(user_id, "assistant", bot_response)

    def process_request(self, user_id: str, message: str) -> Dict:
        """
        Coordinates the Operations Architecture using single-call Gemini parsing.
        """
        from services.business_logic_service import BusinessLogicService
        logic = BusinessLogicService()

        # Get conversation history for context-aware parsing (before storing current message)
        conversation_history = self.memory.get_conversation_history(user_id)

        # 1. Single-call Intent & Parameter Parsing with context
        result = self.gemini.parse_user_intent(message, conversation_history=conversation_history)
        
        # Validation Layer
        operation = result.get("operation")
        params = result.get("parameters", {})
        entity = result.get("entity")

        # Log the parsed intent for debugging
        logger.info(f"Parsed intent - Operation: {operation}, Entity: {entity}, Params: {params}")

        # Handle explicit Gemini API errors
        if operation == "GEMINI_ERROR":
            error_msg = result.get("error_message", "Unknown Gemini API error")
            response = error_msg
            self._store_conversation(user_id, message, response)
            return {
                "operation": "GEMINI_ERROR",
                "response": response,
                "trigger_invoice": False
            }

        # 2. Execution
        action_result = "I don't see this information in my records yet."
        trigger_invoice = False
        invoice_data = {}

        try:
            # Payment reminder intent (keyword-based, runs before normal ops)
            reminder_keywords = [
                "payment reminder",
                "payment reminders",
                "send reminder",
                "send reminders",
                "remind clients",
                "approaching due",
                "upcoming due",
                "due soon",
            ]
            is_reminder_query = any(k in message.lower() for k in reminder_keywords)
            if is_reminder_query:
                logger.info("[REMINDER] Detected payment reminder query")
                records = self.sheets.get_all_records_with_row_numbers()
                logger.info(f"[REMINDER] Loaded {len(records)} records for reminder scan")

                # Use 'days' from intent parameters if provided, else default to 7
                days_param = params.get("days")
                approaching_days = int(days_param) if isinstance(days_param, int) and days_param > 0 else 7
                # For now we treat sheet 'Date' as the due date (no extra payment terms shift)
                payment_terms_days = 0

                targets = logic.get_approaching_due_reminder_targets(
                    records,
                    approaching_days=approaching_days,
                    payment_terms_days=payment_terms_days,
                )

                sent = 0
                failed = 0
                sent_details = []
                
                for t in targets:
                    to_email = t["email"]
                    client = t["client"]
                    invoice_number = t.get("invoice_number", "N/A")
                    amount_due = t.get("amount_due", "₹0.00")
                    due_date_str = t["due_date"].strftime("%Y-%m-%d")

                    ok = self.email.send_payment_reminder(
                        to_email=to_email,
                        client_name=client,
                        invoice_number=invoice_number,
                        amount_due=amount_due,
                        due_date_str=due_date_str,
                    )
                    if ok:
                        # Mark FirstReminderSent as True
                        upd_ok = self.sheets.update_cell_by_header(t["_row"], "FirstReminderSent", "True")
                        if not upd_ok:
                            logger.error(f"[REMINDER] Email sent but failed to mark FirstReminderSent for row {t['_row']}")
                        sent += 1
                        sent_details.append(f"{client} ({invoice_number}) - {to_email}")
                    else:
                        failed += 1

                if not targets:
                    response = f"I don't see any clients with payments due in the next {approaching_days} days that need a first reminder."
                    self._store_conversation(user_id, message, response)
                    return {
                        "operation": "ACTION_TRIGGER",
                        "response": response,
                        "trigger_invoice": False,
                    }

                response_parts = [f"Sent {sent} payment reminder(s)."]
                if sent_details:
                    response_parts.append("\n\nClients notified:")
                    for detail in sent_details:
                        response_parts.append(f"• {detail}")
                if failed > 0:
                    response_parts.append(f"\nFailed: {failed}.")
                
                response = "\n".join(response_parts)
                self._store_conversation(user_id, message, response)
                return {
                    "operation": "ACTION_TRIGGER",
                    "response": response,
                    "trigger_invoice": False,
                }

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
                    logger.info(f"[QUERY] Fetching dataset for invoice retrieval")
                    sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                    all_records = sheet.get_all_records()
                    logger.info(f"[QUERY] Dataset loaded - Total records: {len(all_records)}")
                    if all_records:
                        logger.info(f"[QUERY] Dataset columns: {list(all_records[0].keys())}")
                except Exception as se:
                    logger.error(f"Sheet access failed: {se}")
                    response = "I encountered an error accessing the data records."
                    self._store_conversation(user_id, message, response)
                    return {
                        "operation": operation,
                        "response": response,
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
                    self._store_conversation(user_id, message, action_result)
                    return {
                        "operation": operation,
                        "response": action_result,
                        "trigger_invoice": False
                    }
            
            # Check for overdue invoice queries (after invoice retrieval, before other operations)
            if action_result == "I don't see this information in my records yet.":
                overdue_keywords = ["overdue", "due date", "passed due", "past due", "past the due", "exceeded due", "late payment", "which.*passed", "have passed"]
                is_overdue_query = any(keyword in message.lower() for keyword in overdue_keywords)
                if is_overdue_query and (entity == "invoice" or entity == "client" or "invoice" in message.lower() or "client" in message.lower()):
                    logger.info("Detected overdue invoice query")
                    # Fetch records for overdue query
                    all_records = []
                    try:
                        logger.info(f"[QUERY] Fetching dataset for overdue invoice query")
                        sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                        all_records = sheet.get_all_records()
                        logger.info(f"[QUERY] Dataset loaded - Total records: {len(all_records)}")
                        if all_records:
                            logger.info(f"[QUERY] Dataset columns: {list(all_records[0].keys())}")
                    except Exception as se:
                        logger.error(f"Sheet access failed: {se}")
                        response = "I encountered an error accessing the data records."
                        self._store_conversation(user_id, message, response)
                        return {
                            "operation": operation,
                            "response": response,
                            "trigger_invoice": False
                        }
                    
                    overdue_invoices = logic.get_overdue_invoices(all_records)
                    action_result = logic.format_overdue_invoices_response(overdue_invoices, payment_terms_days=30)
                    logger.info(f"Found {len(overdue_invoices)} overdue invoices")
            
            # Handle SMALL_TALK only if not an invoice retrieval or overdue query
            if operation == "SMALL_TALK" and action_result == "I don't see this information in my records yet.":
                response = "Hello! I'm your Operations Bot. How can I help you today?"
                self._store_conversation(user_id, message, response)
                return {
                    "operation": "SMALL_TALK",
                    "response": response,
                    "trigger_invoice": False
                }

            # Sheets data often needed for other operations
            if action_result == "I don't see this information in my records yet.":
                all_records = []
                if operation in ["AGGREGATE_ENTITY", "READ_ENTITY", "ACTION_TRIGGER"]:
                    try:
                        logger.info(f"[QUERY] Fetching dataset for operation: {operation}")
                        sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                        all_records = sheet.get_all_records()
                        logger.info(f"[QUERY] Dataset loaded - Total records: {len(all_records)}")
                        if all_records:
                            logger.info(f"[QUERY] Dataset columns: {list(all_records[0].keys())}")
                    except Exception as se:
                        logger.error(f"Sheet access failed: {se}")

            # 2. Standard Operation Path (only if not already handled by retrieval or overdue)
            if action_result == "I don't see this information in my records yet.":
                if operation == "READ_ENTITY":
                    name = params.get("client_name")
                    if entity == "bank_details":
                        action_result = "Our bank details: HDFC Bank, Acct: 12345678, IFSC: HDFC0001234."
                    elif entity == "client":
                        # Handle client list requests
                        if not name:
                            # User wants a list of clients
                            logger.info(f"[QUERY] Client List Query - Searching {len(all_records)} records")
                            logger.info(f"[QUERY] Checking columns: 'Client Name', 'Production house'")
                            client_names = set()
                            for row in all_records:
                                client = row.get("Client Name") or row.get("Production house")
                                if client and str(client).strip():
                                    client_names.add(str(client).strip())
                            
                            logger.info(f"[QUERY] Client list query results - Found {len(client_names)} unique clients")
                            if client_names:
                                client_list = sorted(list(client_names))
                                if len(client_list) <= 10:
                                    action_result = f"Here are the client names in my records:\n" + "\n".join(f"• {c}" for c in client_list)
                                else:
                                    action_result = f"I found {len(client_list)} clients. Here are some:\n" + "\n".join(f"• {c}" for c in client_list[:10]) + f"\n... and {len(client_list) - 10} more."
                            else:
                                action_result = "I don't see any client names in my current sheet."
                        else:
                            # User is searching for a specific client
                            logger.info(f"[QUERY] Client Search Query - Searching for: '{name}' in {len(all_records)} records")
                            logger.info(f"[QUERY] Searching across all columns for partial match")
                            results = []
                            for r in all_records:
                                for v in r.values():
                                    if name.lower() in str(v).lower():
                                        results.append(r)
                                        break
                            
                            logger.info(f"[QUERY] Client search results - Found {len(results)} records matching '{name}'")
                            if results:
                                action_result = f"Found {len(results)} records matching '{name}'."
                            else:
                                action_result = f"I don't see any records for {name} in my current sheet."
                
                elif operation == "AGGREGATE_ENTITY":
                    client = params.get("client_name")
                    month = params.get("month")
                    year = params.get("year")
                    logger.info(f"[QUERY] Aggregate Query - Client: {client}, Month: {month}, Year: {year}")
                    if not month:
                        action_result = "Please specify a month to calculate the total billing."
                    else:
                        from services.invoice_service import InvoiceService
                        logger.info(f"[QUERY] Fetching invoice data for aggregation")
                        data = self.sheets.get_invoice_data(client, month, year=year)
                        logger.info(f"[QUERY] Aggregate query results - Found {len(data) if data else 0} records")
                        if not data:
                            if client:
                                action_result = f"I don't see any billing records for {client} in {month} yet."
                            else:
                                action_result = f"I don't see any billing records for {month} yet."
                        else:
                            summary_client = client if client else "All Clients"
                            summary = InvoiceService.process_invoice_data(data, summary_client, month)
                            logger.info(f"[QUERY] Aggregation complete - Total: {summary['currency']}{summary['total']:,}, Items: {summary['items']}, Client: {summary_client}")
                            action_result = f"Total billing for {summary_client} in {month} is {summary['currency']}{summary['total']:,}."

        except Exception as e:
            logger.error(f"Execution failure: {e}")
            action_result = "I encountered an error accessing the data records."

        # 3. Final Response Phrasing
        if trigger_invoice:
            response = action_result # Use the "Confirmed!" message directly
        elif action_result.startswith("I don't see") or "error" in action_result.lower():
            response = action_result
        elif action_result.startswith("Total billing") or action_result.startswith("The total billing"):
            # Skip Gemini phrasing for billing responses - they're already well-formatted
            response = action_result
        else:
            response = self.gemini.generate_response(message, action_result)

        # Store both user message and bot response in conversation history after processing
        self._store_conversation(user_id, message, response)

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
