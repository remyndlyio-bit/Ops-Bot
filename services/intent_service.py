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

    def _resolve_response_mode(self, result: Dict, cmd: Dict) -> str:
        """
        Determine ResponseMode based on priority:
        1. SINGLE_FIELD: return_fields has 1 field OR metric=value with column
        2. RECORD: multiple return_fields
        3. COUNT: metric=count with no specific field requested
        4. AGGREGATION: sum/avg/min/max
        5. GROUPED: group_by present
        6. CLARIFY: otherwise
        """
        metric = result.get("metric") or cmd.get("metric", "count")
        return_fields = result.get("return_fields") or cmd.get("return_fields") or []
        column = result.get("column") or cmd.get("column")
        group_by = cmd.get("group_by")

        # Priority 1: Single field requested
        if len(return_fields) == 1:
            return "SINGLE_FIELD"
        if metric == "value" and column:
            return "SINGLE_FIELD"

        # Priority 2: Multiple return fields
        if len(return_fields) > 1:
            return "RECORD"

        # Priority 5: Grouped results
        if group_by or ("labels" in result and result["labels"]):
            return "GROUPED"

        # Priority 3/4: Aggregation metrics
        if metric in ("sum", "avg", "min", "max"):
            return "AGGREGATION"

        if metric == "count":
            return "COUNT"

        return "CLARIFY"

    def _build_filter_context(self, filters: Dict) -> str:
        """Build human-readable context from filters (e.g., 'the Apple job')."""
        if not filters:
            return ""
        parts = []
        for k, v in filters.items():
            if v and not str(k).startswith("_"):
                k_lower = str(k).lower()
                if "client" in k_lower or "name" in k_lower:
                    parts.append(str(v))
                elif "date" in k_lower:
                    parts.append(f"on {v}")
        return " ".join(parts) if parts else ""

    def _format_uscf_result(self, result: Dict, cmd: Dict) -> str:
        """Format USCF executor result as factual output for response maker."""
        if not result.get("ok"):
            return result.get("message", "I don't see this information in my records.")

        operation = result.get("operation") or cmd.get("operation")

        # CREATE result
        if operation == "create":
            msg = result.get("message", "Record created.")
            bill_no = result.get("bill_number")
            if bill_no:
                msg += f" Invoice/Bill #: {bill_no}."
            return msg

        # UPDATE result
        if operation == "update":
            return result.get("message", f"Updated {result.get('count', 0)} record(s).")

        # DELETE result
        if operation == "delete":
            return result.get("message", f"Deleted {result.get('count', 0)} record(s).")

        # QUERY result - use ResponseMode resolver
        mode = self._resolve_response_mode(result, cmd)
        filters = result.get("filters") or cmd.get("filters") or {}
        context = self._build_filter_context(filters)
        column = result.get("column") or cmd.get("column", "")
        count = result.get("count", 0)
        metric = result.get("metric") or cmd.get("metric", "count")

        logger.info(f"[RESPONSE] mode={mode}, column={column}, metric={metric}, filters={filters}")

        # Date result (special case for max on date column)
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

        # SINGLE_FIELD mode: return specific field value
        if mode == "SINGLE_FIELD":
            val = result.get("value")
            rows = result.get("rows", [])
            return_fields = result.get("return_fields") or [column]
            target_field = return_fields[0] if return_fields else column

            # Try to get value from result or first row
            if val is None and rows:
                val = rows[0].get(target_field)

            if val is None or (isinstance(val, str) and not val.strip()):
                return f"No {target_field} found{' for ' + context if context else ''}."

            # Format based on value type
            if isinstance(val, (int, float)):
                return f"{target_field}{' for ' + context if context else ''}: ₹{val:,.2f}"
            else:
                return f"{target_field}{' for ' + context if context else ''}: {val}"

        # RECORD mode: return multiple fields
        if mode == "RECORD":
            rows = result.get("rows", [])
            return_fields = result.get("return_fields", [])
            if not rows:
                return "No matching records found."
            lines = []
            for row in rows[:10]:
                parts = [f"{f}: {row.get(f, 'N/A')}" for f in return_fields]
                lines.append("• " + ", ".join(parts))
            return "\n".join(lines)

        # GROUPED mode: labels + values
        if mode == "GROUPED":
            labels = result.get("labels", [])
            values = result.get("values", [])
            if not labels:
                return "No grouped results."
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

        # AGGREGATION mode: sum/avg/min/max
        if mode == "AGGREGATION":
            value = result.get("value", 0)
            if not isinstance(value, (int, float)):
                return str(value) if value else "No result."
            prefix = context + " " if context else ""
            if metric == "sum":
                return f"total {column}{' for ' + context if context else ''}: ₹{value:,.2f}"
            elif metric == "avg":
                return f"average {column}{' for ' + context if context else ''}: ₹{value:,.2f} (across {count} records)"
            elif metric == "min":
                return f"minimum {column}{' for ' + context if context else ''}: ₹{value:,.2f}"
            elif metric == "max":
                return f"maximum {column}{' for ' + context if context else ''}: ₹{value:,.2f}"

        # COUNT mode (only when no specific field requested)
        if mode == "COUNT":
            value = result.get("value", result.get("count", 0))
            return f"count{' for ' + context if context else ''}: {int(value)}"

        # CLARIFY fallback
        return "Could you clarify what specific information you're looking for?"

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

    def _is_followup_field_request(self, message: str, columns: List[str]) -> Optional[str]:
        """
        Check if message is a follow-up request for a specific field from last row.
        Returns the requested field keyword if detected, None otherwise.
        """
        msg_lower = message.lower().strip()
        
        # Common follow-up patterns that indicate user wants info from previous result
        followup_patterns = [
            "and the", "what about", "what's the", "what is the", "how about",
            "the ", "show me the", "tell me the", "give me the", "what was the",
            "what's", "whats",
        ]
        
        # Check if message matches follow-up pattern or is a short question
        is_followup = (
            any(msg_lower.startswith(p) for p in followup_patterns) or 
            msg_lower.endswith("?") or
            len(msg_lower.split()) <= 4  # Short messages like "language?" or "brand name"
        )
        
        if not is_followup:
            return None
        
        # Comprehensive field aliases mapping
        field_aliases = {
            "brand": ["brand", "brand_name", "brand name", "brandname"],
            "client": ["client", "client_name", "client name", "clientname", "company"],
            "amount": ["amount", "fees", "fee", "billing", "payment", "cost", "price", "total", "value"],
            "paid": ["paid", "payment_status", "status", "payment status", "ispaid"],
            "date": ["date", "job_date", "job date", "jobdate", "when", "day"],
            "job": ["job", "job_name", "job name", "jobname", "work", "gig", "project", "task"],
            "notes": ["notes", "note", "description", "details", "info", "about"],
            "language": ["language", "lang", "languages"],
            "location": ["location", "place", "city", "venue", "where"],
            "contact": ["contact", "phone", "email", "poc", "person"],
            "production": ["production", "production_house", "production house", "productionhouse", "house"],
            "invoice": ["invoice", "invoice_number", "invoice number", "invoicenumber", "bill"],
            "due": ["due", "due_date", "due date", "duedate", "deadline"],
        }
        
        # First check for exact column match
        for col in columns:
            col_lower = col.lower().replace("_", " ").replace("-", " ")
            col_variants = [col_lower, col_lower.replace(" ", "")]
            for variant in col_variants:
                if variant in msg_lower:
                    return col
        
        # Then check aliases
        for canonical, aliases in field_aliases.items():
            for alias in aliases:
                if alias in msg_lower:
                    return canonical  # Return the canonical name, we'll match it to columns later
        
        return None

    def _try_answer_from_context(self, user_id: str, message: str, columns: List[str]) -> Optional[str]:
        """
        Try to answer follow-up question directly from stored last_row_data.
        Returns factual answer string if possible.
        
        IMPORTANT: If we have last_row_data but can't find the field, we return
        a "not found" message instead of None, to prevent re-querying with stale filters.
        """
        ctx = self.memory.get_user_memory(user_id).get("uscf_context", {})
        last_row_data = ctx.get("last_row_data")
        
        if not last_row_data:
            logger.info("[FOLLOWUP] No last_row_data in context - allowing new query")
            return None
        
        # Check if this is a follow-up field request
        requested_field = self._is_followup_field_request(message, columns)
        if not requested_field:
            logger.info("[FOLLOWUP] Not a follow-up field request - allowing new query")
            return None
        
        logger.info(f"[FOLLOWUP] Looking for field '{requested_field}' in stored row with keys: {list(last_row_data.keys())}")
        
        # Comprehensive alias mapping for field lookup
        field_aliases = {
            "brand": ["brand", "brand_name", "brandname"],
            "client": ["client", "client_name", "clientname", "company"],
            "amount": ["amount", "fees", "fee", "billing", "total", "cost", "price"],
            "paid": ["paid", "payment_status", "status", "ispaid"],
            "date": ["date", "job_date", "jobdate"],
            "job": ["job", "job_name", "jobname", "work", "project", "task"],
            "notes": ["notes", "note", "description", "details", "about"],
            "language": ["language", "lang", "languages"],
            "location": ["location", "place", "city", "venue"],
            "contact": ["contact", "phone", "email", "poc"],
            "production": ["production", "production_house", "productionhouse", "house"],
            "invoice": ["invoice", "invoice_number", "invoicenumber", "bill"],
            "due": ["due", "due_date", "duedate", "deadline"],
        }
        
        # Get all aliases for the requested field
        search_terms = [requested_field.lower()]
        for canonical, aliases in field_aliases.items():
            if requested_field.lower() == canonical or requested_field.lower() in aliases:
                search_terms = aliases + [canonical]
                break
        
        # Try to find the field value in last_row_data
        value = None
        matched_col = None
        
        for col, val in last_row_data.items():
            col_lower = col.lower().replace("_", "").replace(" ", "")
            col_lower_spaced = col.lower().replace("_", " ")
            
            for term in search_terms:
                term_clean = term.replace("_", "").replace(" ", "")
                if (col_lower == term_clean or 
                    term_clean in col_lower or 
                    col_lower in term_clean or
                    term in col_lower_spaced):
                    value = val
                    matched_col = col
                    break
            if value is not None:
                break
        
        # If we have context but can't find the field, return a "not found" message
        # DO NOT return None here - that would trigger a re-query with stale filters
        if value is None or (isinstance(value, str) and not value.strip()):
            available_fields = ", ".join(list(last_row_data.keys())[:8])
            logger.info(f"[FOLLOWUP] Field '{requested_field}' not found in stored row. Available: {available_fields}")
            return f"I don't have '{requested_field}' information for this record. Available fields: {available_fields}"
        
        logger.info(f"[FOLLOWUP] Serving field from stored row without DB call: {matched_col} = {value}")
        
        # Format the value
        if isinstance(value, (int, float)):
            # Check if it looks like a currency field
            col_lower = matched_col.lower() if matched_col else ""
            if any(term in col_lower for term in ["amount", "fee", "billing", "cost", "price", "total", "payment"]):
                return f"{matched_col}: ₹{value:,.2f}"
            return f"{matched_col}: {value}"
        return f"{matched_col}: {value}"

    def _update_uscf_context(self, user_id: str, cmd: Dict, result: Dict):
        """Update context after command execution for future reference resolution."""
        ctx = self.memory.get_user_memory(user_id).get("uscf_context", {})
        
        # Only update context if we got successful results with matched rows
        matched_rows = result.get("count", 0)
        rows = result.get("rows", [])
        full_rows = result.get("_full_rows", [])  # Full rows for context (not filtered by return_fields)
        
        if matched_rows == 0 and not rows and not full_rows:
            # Don't store context for empty results
            logger.info("[CONTEXT] No matched rows - not updating context")
            return
        
        filters = cmd.get("filters", {})
        # Store filters for "update it" type references
        if filters:
            ctx["current_filters"] = filters
        
        # Store date from result
        if result.get("value_type") == "date" and result.get("value"):
            ctx["last_result_date"] = result["value"]
        
        # Store operation type
        ctx["last_operation"] = cmd.get("operation")
        
        # Store FULL row data for follow-up questions (prefer _full_rows over rows)
        # This ensures we have ALL columns, not just return_fields
        source_rows = full_rows if full_rows else rows
        if source_rows and len(source_rows) > 0:
            last_row = source_rows[0]
            # Store the ENTIRE row, excluding only internal keys
            ctx["last_row_data"] = {k: v for k, v in last_row.items() if not str(k).startswith("_")}
            ctx["last_row_id"] = last_row.get("_row")
            all_keys = list(ctx["last_row_data"].keys())
            logger.info(f"[CONTEXT] Stored full row with keys: {all_keys}")
            logger.info(f"[CONTEXT] last_row_id={ctx.get('last_row_id')}, total_fields={len(all_keys)}")
        
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
            ok, _ = self.sheets.append_row_by_columns(values)
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

            # 4a. Follow-up resolver: try to answer from last successful row context
            followup_answer = self._try_answer_from_context(user_id, message, columns)
            if followup_answer:
                logger.info(f"[FOLLOWUP] Answered from context: {followup_answer}")
                # Use response maker for natural reply
                polished = self.gemini.make_response(message, conversation_history, followup_answer)
                response = polished if (polished and polished.strip()) else followup_answer
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

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
            is_invoice_create = "invoice" in message.lower() and sanitized.get("operation") == "create"
            exec_result = execute_uscf(
                sanitized, records, date_column, self.sheets, is_invoice_create=is_invoice_create
            )

            # Handle empty results - don't store context, return safe message
            matched_count = exec_result.get("count", 0)
            matched_rows = exec_result.get("rows", [])
            if not exec_result.get("ok") or (matched_count == 0 and not matched_rows):
                if exec_result.get("operation") == "query":
                    response = "I couldn't find any matching records. Could you try with different criteria?"
                    self._store_conversation(user_id, message, response)
                    return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # Invoice CREATE: trigger PDF generation and include bill number in invoice_data
            if is_invoice_create and exec_result.get("ok") and exec_result.get("operation") == "create":
                create_data = sanitized.get("data", {})
                client_name = (
                    create_data.get("Client Name")
                    or create_data.get("Production house")
                    or create_data.get("client_name")
                )
                if client_name:
                    from datetime import datetime
                    now = datetime.now()
                    trigger_invoice = True
                    invoice_data = {
                        "client_name": str(client_name).strip(),
                        "month": now.strftime("%B"),
                        "year": now.year,
                        "bill_number": exec_result.get("bill_number"),
                    }

            factual_output = self._format_uscf_result(exec_result, sanitized)

            # Update context for future reference resolution (only if we have results)
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
