from services.gemini_service import GeminiService
from services.sheets_service import SheetsService
from services.gmail_service import GmailService
from services.supabase_service import SupabaseService, JOB_ENTRIES_COLUMNS
from services.sql_generator import generate_sql
from services.sql_validator import validate_sql
from services.response_formatter import (
    format_response,
    format_as_job_summary_block,
    payment_status_note,
    STRICT_DATA_MODE,
    ASSISTANT_MODE,
    REMINDER_MODE,
    ERROR_MODE,
    clarify_phrase,
    no_result_phrase,
    error_calm_phrase,
    query_invalid_phrase,
)
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
        self.supabase = SupabaseService()
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

    def _format_sql_result(self, rows: List[Dict]) -> str:
        """Format SQL result rows into a short factual reply. Never returns empty when rows exist."""
        if not rows:
            return "No matching records found."
        def _fmt_val(v):
            return v if v is not None else "N/A"
        if len(rows) == 1 and len(rows[0]) <= 3:
            parts = [f"{k}: {_fmt_val(v)}" for k, v in rows[0].items()]
            out = ", ".join(parts)
            return out if out.strip() else "1 row (no values)"
        if len(rows) == 1:
            lines = [f"• {k}: {_fmt_val(v)}" for k, v in list(rows[0].items())[:15]]
            out = "\n".join(lines)
            return out if out.strip() else "1 row (no values)"
        keys = list(rows[0].keys())[:6]
        lines = []
        for r in rows[:20]:
            parts = [f"{k}: {_fmt_val(r.get(k))}" for k in keys]
            lines.append("• " + ", ".join(parts))
        if len(rows) > 20:
            lines.append(f"... and {len(rows) - 20} more.")
        return "\n".join(lines)

    def _update_sql_context(self, user_id: str, rows: List[Dict]):
        """Store first result row for follow-up questions (same shape as USCF context)."""
        if not rows:
            return
        ctx = self.memory.get_user_memory(user_id).get("uscf_context", {})
        ctx["last_row_data"] = dict(rows[0])
        ctx["last_operation"] = "query"
        self.memory.update_user_memory(user_id, {"uscf_context": ctx})

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
        If we don't find the requested field in context, we now allow a fresh query.
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
        
        if value is None or (isinstance(value, str) and not value.strip()):
            available_fields = ", ".join(list(last_row_data.keys())[:8])
            logger.info(f"[FOLLOWUP] Field '{requested_field}' not found in stored row. Available: {available_fields}")
            logger.info("[FOLLOWUP] Falling back to a new query for this follow-up.")
            # Allow the main flow to run a new query instead of forcing a 'not found' reply
            return None
        
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
                    response = format_response(
                        REMINDER_MODE,
                        clarification_hint="Would you like me to check a different window or list overdue items?",
                        reminder_sent_count=0,
                    )
                    self._store_conversation(user_id, message, response)
                    return {
                        "operation": "ACTION_TRIGGER",
                        "response": response,
                        "trigger_invoice": False,
                    }

                response = format_response(
                    REMINDER_MODE,
                    reminder_sent_count=sent,
                    reminder_details=sent_details,
                )
                if failed > 0:
                    response = response.rstrip() + f"\n\nFailed to send: {failed}."
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
                            response = f"Confirmed. I've found the record for {resolved['client']}. Generating the invoice now."
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

            # 4. SQL path: intent → generate SQL → validate → execute on Supabase → format → response
            columns = [c for c in JOB_ENTRIES_COLUMNS if not c.startswith("_")]

            if not self.supabase.db_url:
                response = format_response(
                    ERROR_MODE,
                    error_detail="Query service isn't configured right now. I can still help with payment reminders and invoice retrieval.",
                )
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # 4a. Follow-up: answer from last result row if applicable
            followup_answer = self._try_answer_from_context(user_id, message, columns)
            if followup_answer:
                logger.info(f"[FOLLOWUP] Answered from context: {followup_answer}")
                response = format_response(ASSISTANT_MODE, factual=followup_answer)
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # Generate SQL from natural language
            sql_result = generate_sql(message, self.gemini, self.supabase, conversation_history)
            if sql_result.get("_error"):
                response = clarify_phrase(["How many jobs?", "Total fees for Garnier", "Last payment date"])
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            sql = sql_result.get("sql")
            valid, sanitized_sql, err = validate_sql(sql)
            if not valid:
                response = query_invalid_phrase()
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            exec_result = self.supabase.execute_sql(sanitized_sql)
            if not exec_result.get("ok"):
                response = format_response(
                    ERROR_MODE,
                    error_detail=exec_result.get("error") or error_calm_phrase(),
                )
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            rows = exec_result.get("rows", [])
            op = exec_result.get("operation", "select")

            if op == "insert":
                if rows:
                    self._update_sql_context(user_id, rows)
                    response = format_response(ASSISTANT_MODE, insert_confirmation=True)
                else:
                    response = format_response(ASSISTANT_MODE, insert_confirmation=True)
            else:
                if not rows:
                    response = format_response(ERROR_MODE)  # no_result_phrase inside
                    self._store_conversation(user_id, message, response)
                    return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}
                self._update_sql_context(user_id, rows)
                response = format_response(
                    ASSISTANT_MODE,
                    rows=rows,
                    add_payment_note=True,
                )

        except Exception as e:
            logger.error(f"Execution failure: {e}")
            response = format_response(ERROR_MODE, error_detail=error_calm_phrase())

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
