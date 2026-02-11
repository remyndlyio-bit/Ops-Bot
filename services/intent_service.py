from services.gemini_service import GeminiService
from services.sheets_service import SheetsService
from services.gmail_service import GmailService
from services.query_planner import get_query_plan
from services.query_validator import validate_plan
from services.query_executor import execute_plan
from utils.memory_service import MemoryService
from utils.logger import logger
from typing import Dict, List
import json

class IntentService:
    # Cache AI-generated schema by column names so we don't call the AI on every message
    _schema_cache: Dict[tuple, str] = {}

    def __init__(self):
        self.gemini = GeminiService()
        self.sheets = SheetsService()
        self.email = GmailService()
        self.memory = MemoryService()

    def _store_conversation(self, user_id: str, user_message: str, bot_response: str):
        """Store user message and bot response in conversation history."""
        self.memory.add_message(user_id, "user", user_message)
        self.memory.add_message(user_id, "assistant", bot_response)

    def _get_schema_and_columns(self, records: List[Dict]) -> tuple:
        """Return (schema_description, allowed_columns, date_column). Prefer AI-generated schema; fallback to rule-based."""
        from services.business_logic_service import BusinessLogicService
        logic = BusinessLogicService()
        cols = list(records[0].keys()) if records else []
        if not cols:
            column_map = logic._get_column_names(None)
            cols = list({c for v in column_map.values() for c in v})
        # Prefer AI-generated schema; cache by column names so we don't call the AI every message
        cache_key = tuple(sorted(cols))
        schema_description = IntentService._schema_cache.get(cache_key)
        if not schema_description:
            schema_description = self.gemini.generate_schema_from_columns(
                cols,
                sample_row=records[0] if records else None,
            )
            if schema_description:
                IntentService._schema_cache[cache_key] = schema_description
        if not schema_description:
            schema_description = logic.get_schema_for_intent(cols)
        column_map = logic._get_column_names(cols)
        date_cols = column_map.get("invoice_date", []) or ([c for c in cols if "date" in c.lower()] if cols else [])
        date_column = date_cols[0] if date_cols else (cols[0] if cols else "Date")
        return schema_description, cols, date_column

    def _format_query_result(self, result: Dict, plan: Dict) -> str:
        """Format executor result for the user. Uses metric type for correct formatting."""
        if not result.get("ok"):
            return result.get("message", "I don't see this information in my records yet.")
        metric = result.get("metric") or plan.get("metric", "sum")
        column = plan.get("column", "")

        if result.get("value_type") == "date":
            val = result.get("value")
            if val is None or result.get("message"):
                return result.get("message", "I don't have any gigs on record with a date.")
            try:
                from datetime import datetime
                dt = datetime.strptime(str(val)[:10], "%Y-%m-%d")
                return f"date: {dt.strftime('%d %b %Y')}"
            except ValueError:
                return f"date: {val}"
        if result.get("value_type") == "text":
            val = result.get("value")
            if result.get("message"):
                return result.get("message")
            if val is None or not str(val).strip():
                return "I don't have that detail on record."
            return f"{column}: {val}"

        # Grouped results (labels + values)
        if "labels" in result and result["labels"]:
            labels = result["labels"]
            values = result.get("values") or []
            lines = []
            for idx, label in enumerate(labels[:30]):
                prefix = f"• {label}"
                if idx < len(values):
                    v = values[idx]
                    if isinstance(v, (int, float)):
                        if metric == "count":
                            prefix += f": {int(v)}"
                        else:
                            prefix += f" – ₹{v:,.2f}"
                    else:
                        prefix += f": {v}"
                lines.append(prefix)
            if len(labels) > 30:
                lines.append(f"... and {len(labels) - 30} more.")
            return "\n".join(lines)

        # Single value result
        value = result.get("value", 0)
        row_count = result.get("count", 0)
        if not isinstance(value, (int, float)):
            return str(value)

        # Format based on metric type
        if metric == "count":
            return f"count: {int(value)}"
        elif metric == "avg":
            return f"average {column}: ₹{value:,.2f} (across {row_count} records)"
        elif metric == "min":
            return f"minimum {column}: ₹{value:,.2f}"
        elif metric == "max":
            return f"maximum {column}: ₹{value:,.2f}"
        else:  # sum or default
            return f"total {column}: ₹{value:,.2f}"

    def _handle_form_step(self, user_id: str, message: str) -> Dict:
        """Handle an active form: store value, advance, ask next or complete."""
        form = self.memory.get_form_state(user_id)
        if not form:
            return None
        fields = form.get("fields", [])
        step = form.get("step", 0)

        # Cancel form if user says cancel/stop/nevermind
        if message.strip().lower() in ("cancel", "stop", "nevermind", "abort", "exit"):
            self.memory.cancel_form(user_id)
            response = "No problem, I've cancelled the form. Let me know if you need anything else."
            self._store_conversation(user_id, message, response)
            return {"operation": "form_cancelled", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # Store the current answer
        current_field = fields[step]
        self.memory.set_form_value(user_id, current_field, message.strip())
        self.memory.advance_form_step(user_id)

        # Check if there's a next field
        next_step = step + 1
        if next_step < len(fields):
            next_field = fields[next_step]
            response = f"Got it! Now, what's the {next_field}?"
            self._store_conversation(user_id, message, response)
            return {"operation": "form_in_progress", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # All fields collected - save to sheet
        values = self.memory.complete_form(user_id)
        if values:
            ok = self.sheets.append_row_by_columns(values)
            if ok:
                summary = ", ".join(f"{k}: {v}" for k, v in values.items())
                response = f"Done! I've added the new job: {summary}"
            else:
                response = "I collected all the info but couldn't save it to the sheet. Please try again later."
        else:
            response = "Something went wrong completing the form."
        self._store_conversation(user_id, message, response)
        return {"operation": "form_complete", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _start_add_job_form(self, user_id: str, message: str) -> Dict:
        """Start the 'add new job' form by asking for the first field."""
        fields = self.sheets.get_first_n_columns(5)
        if not fields:
            response = "I couldn't get the column headers from your sheet. Please check the sheet connection."
            self._store_conversation(user_id, message, response)
            return {"operation": "form_error", "response": response, "trigger_invoice": False, "invoice_data": {}}
        self.memory.start_form(user_id, fields)
        first_field = fields[0]
        response = f"Let's add a new job! I'll ask you for a few details.\n\nFirst, what's the {first_field}?\n\n(Type 'cancel' anytime to stop.)"
        self._store_conversation(user_id, message, response)
        return {"operation": "form_started", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def process_request(self, user_id: str, message: str) -> Dict:
        """
        Main handler: keyword-based branches for reminder/invoice/overdue;
        then LLM query plan → validate → resolve time → execute → format.
        """
        from services.business_logic_service import BusinessLogicService
        logic = BusinessLogicService()
        conversation_history = self.memory.get_conversation_history(user_id)
        trigger_invoice = False
        invoice_data = {}

        try:
            # 0. Check for active form (multi-step data entry)
            form_state = self.memory.get_form_state(user_id)
            if form_state:
                return self._handle_form_step(user_id, message)

            # 0b. Check for "add job" / "add new job" trigger to start form
            add_job_triggers = ["add job", "add a job", "add new job", "new job", "log a job", "log job", "record job", "record a job"]
            if any(t in message.lower() for t in add_job_triggers):
                return self._start_add_job_form(user_id, message)

            # 1. Payment reminder (keyword-based)
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
                approaching_days = 7
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

            # 2. Invoice retrieval (keyword-based; use LLM only to extract params)
            is_retrieval = any(w in message.lower() for w in ["get", "download", "send", "give", "show", "retrieve", "fetch"]) and "invoice" in message.lower()
            if is_retrieval:
                schema_info = logic.get_schema_for_intent()
                intent_result = self.gemini.parse_user_intent(message, conversation_history=conversation_history, schema_info=schema_info)
                params = intent_result.get("parameters", {})
                if intent_result.get("operation") != "GEMINI_ERROR":
                    all_records = []
                    try:
                        sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                        all_records = sheet.get_all_records()
                    except Exception as se:
                        logger.error(f"Sheet access failed: {se}")
                    if all_records:
                        from services.invoice_service import InvoiceService
                        resolved = InvoiceService.resolve_invoice_pdf(params, all_records)
                        if resolved["status"] == "found":
                            trigger_invoice = True
                            invoice_data = {"client_name": resolved["client"], "month": resolved["month"], "bill_number": params.get("bill_number"), "year": resolved.get("year")}
                            response = f"Confirmed! I've found the record for {resolved['client']}. I'm generating the invoice now... 📄"
                        else:
                            response = resolved.get("message", "I don't see that invoice in my records.")
                        self._store_conversation(user_id, message, response)
                        return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": trigger_invoice, "invoice_data": invoice_data}

            # 3. Overdue / payment followup (keyword-based)
            overdue_keywords = ["overdue", "due date", "passed due", "past due", "late payment", "follow up", "followup", "payment followup", "payment status"]
            is_overdue = any(k in message.lower() for k in overdue_keywords) and ("invoice" in message.lower() or "client" in message.lower() or "payment" in message.lower())
            if is_overdue:
                try:
                    sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                    all_records = sheet.get_all_records()
                    overdue_invoices = logic.get_overdue_invoices(all_records)
                    response = logic.format_overdue_invoices_response(overdue_invoices, payment_terms_days=30)
                    self._store_conversation(user_id, message, response)
                    return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}
                except Exception as se:
                    logger.error(f"Overdue query failed: {se}")

            # 4. Query-plan path: LLM returns structured JSON → validate → resolve time → execute
            try:
                sheet = self.sheets.client.open_by_url(self.sheets.sheet_url).sheet1
                records = sheet.get_all_records()
            except Exception as se:
                logger.error(f"Sheet access failed: {se}")
                records = []
            if not records:
                response = "I don't have any records to query yet."
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            schema_description, allowed_columns, date_column = self._get_schema_and_columns(records)
            plan = get_query_plan(message, self.gemini, schema_description, allowed_columns, conversation_history, date_column=date_column)

            if plan.get("_error"):
                response = "I couldn't process that. Please try rephrasing (e.g. specify a time period like 'last quarter' or 'this month')."
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            if plan.get("confidence") == "low" and plan.get("clarification_question"):
                response = plan["clarification_question"]
                self._store_conversation(user_id, message, response)
                return {"operation": "NEED_CLARIFICATION", "response": response, "trigger_invoice": False, "invoice_data": {}}

            valid, sanitized, err = validate_plan(plan, allowed_columns)
            if not valid:
                # Ask for more context instead of showing technical validation errors
                response = (
                    "I'm not quite sure what you're looking for. Could you give a bit more detail? "
                    "For example, are you asking for a total amount, a date, a list of clients or jobs, "
                    "or details about a specific gig? I'll use that to answer you."
                )
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            exec_result = execute_plan(sanitized, records, date_column)
            factual_output = self._format_query_result(exec_result, sanitized)
            # Response maker: smarter reply using only RAG facts, matching tone, concise
            polished = self.gemini.make_response(message, conversation_history, factual_output)
            response = polished if (polished and polished.strip()) else factual_output

        except Exception as e:
            logger.error(f"Execution failure: {e}")
            response = "I encountered an error accessing the data records."

        self._store_conversation(user_id, message, response)
        return {
            "operation": "query",
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
