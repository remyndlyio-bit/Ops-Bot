from services.gemini_service import GeminiService
from services.sheets_service import SheetsService
from services.gmail_service import GmailService
from services.uscf_parser import parse_uscf_command
from services.uscf_validator import validate_uscf
from services.uscf_executor import execute_uscf
from utils.memory_service import MemoryService
from utils.logger import logger
from typing import Dict, List, Optional
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

    def _format_uscf_result(self, result: Dict, cmd: Dict) -> str:
        """Format USCF executor result as factual output for response maker."""
        if not result.get("ok"):
            return result.get("message", "I don't see this information in my records.")

        operation = result.get("operation") or cmd.get("operation")

        # CREATE result
        if operation == "create":
            return result.get("message", "Record created.")

        # UPDATE result
        if operation == "update":
            return result.get("message", f"Updated {result.get('count', 0)} record(s).")

        # DELETE result
        if operation == "delete":
            return result.get("message", f"Deleted {result.get('count', 0)} record(s).")

        # QUERY result
        metric = result.get("metric", "count")
        column = result.get("column") or cmd.get("column", "")
        count = result.get("count", 0)

        # Date result
        if result.get("value_type") == "date":
            val = result.get("value")
            if val is None:
                return result.get("message", "No date found.")
            try:
                from datetime import datetime
                dt = datetime.strptime(str(val)[:10], "%Y-%m-%d")
                return f"date: {dt.strftime('%d %b %Y')}"
            except ValueError:
                return f"date: {val}"

        # Value lookup (single cell)
        if metric == "value":
            val = result.get("value", "")
            if not val:
                return "No value found."
            return f"{column}: {val}"

        # Grouped results
        if "labels" in result and result["labels"]:
            labels = result["labels"]
            values = result.get("values", [])
            lines = []
            for idx, label in enumerate(labels[:30]):
                line = f"• {label}"
                if idx < len(values):
                    v = values[idx]
                    if isinstance(v, (int, float)):
                        if metric == "count":
                            line += f": {int(v)}"
                        else:
                            line += f": ₹{v:,.2f}"
                    else:
                        line += f": {v}"
                lines.append(line)
            if len(labels) > 30:
                lines.append(f"... and {len(labels) - 30} more.")
            return "\n".join(lines)

        # Single value
        value = result.get("value", 0)
        if not isinstance(value, (int, float)):
            return str(value) if value else "No result."

        if metric == "count":
            return f"count: {int(value)}"
        elif metric == "avg":
            return f"average {column}: ₹{value:,.2f} (across {count} records)"
        elif metric == "min":
            return f"minimum {column}: ₹{value:,.2f}"
        elif metric == "max":
            return f"maximum {column}: ₹{value:,.2f}"
        else:  # sum
            return f"total {column}: ₹{value:,.2f}"

    def _build_uscf_context(self, user_id: str, conversation_history: List[Dict]) -> Optional[Dict]:
        """Build context for USCF parser (helps resolve 'it', 'that', 'update it')."""
        ctx = self.memory.get_user_memory(user_id).get("uscf_context", {})
        # Extract info from recent assistant messages (dates, clients mentioned)
        if conversation_history:
            for msg in reversed(conversation_history[-4:]):
                if msg.get("role") == "assistant":
                    content = msg.get("content", "")
                    # Look for dates like "04 Apr 2025" or "2025-04-04"
                    import re
                    date_match = re.search(r"(\d{1,2}\s+\w+\s+\d{4}|\d{4}-\d{2}-\d{2})", content)
                    if date_match and not ctx.get("last_result_date"):
                        ctx["last_result_date"] = date_match.group(1)
                    break
        return ctx if ctx else None

    def _update_uscf_context(self, user_id: str, cmd: Dict, result: Dict):
        """Update context after command execution for future reference resolution."""
        ctx = self.memory.get_user_memory(user_id).get("uscf_context", {})
        filters = cmd.get("filters", {})
        # Store filters for "update it" type references
        if filters:
            ctx["current_filters"] = filters
        # Store date from result
        if result.get("value_type") == "date" and result.get("value"):
            ctx["last_result_date"] = result["value"]
        # Store operation type
        ctx["last_operation"] = cmd.get("operation")
        self.memory.update_user_memory(user_id, {"uscf_context": ctx})

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

            # 4. USCF path: AI → JSON command → validate → execute → format → response maker
            try:
                records = self.sheets.get_all_records_with_row_numbers()
            except Exception as se:
                logger.error(f"Sheet access failed: {se}")
                records = []
            if not records:
                response = "I don't have any records to work with yet."
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # Get columns and date column
            columns = list(records[0].keys()) if records else []
            columns = [c for c in columns if c and not c.startswith("_")]  # Exclude internal keys
            column_map = logic._get_column_names(columns)
            date_cols = column_map.get("invoice_date", []) or [c for c in columns if "date" in c.lower()]
            date_column = date_cols[0] if date_cols else "Date"

            # Build context for AI (helps with "it", "that", "update it" references)
            uscf_context = self._build_uscf_context(user_id, conversation_history)

            # Parse intent to USCF command
            cmd = parse_uscf_command(message, self.gemini, columns, conversation_history, uscf_context)

            if cmd.get("_error"):
                response = (
                    "I'm not quite sure what you're asking. Could you give a bit more detail? "
                    "For example: a total amount, a date, a list of jobs, or update a specific record?"
                )
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # Validate command
            valid, sanitized, err = validate_uscf(cmd, columns)
            if not valid:
                response = (
                    "I couldn't quite understand that. Could you rephrase? "
                    "For example: 'How many jobs did I do?', 'Total billing last month', or 'Add a new job'."
                )
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # Execute command
            exec_result = execute_uscf(sanitized, records, date_column, self.sheets)
            factual_output = self._format_uscf_result(exec_result, sanitized)

            # Update context for future reference resolution
            self._update_uscf_context(user_id, sanitized, exec_result)

            # Response maker: natural reply using only facts
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
