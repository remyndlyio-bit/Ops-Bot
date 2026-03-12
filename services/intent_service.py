from services.gemini_service import GeminiService
from services.resend_email_service import ResendEmailService
from services.supabase_service import SupabaseService, JOB_ENTRIES_COLUMNS, _COLUMN_SCHEMA_FROM_ENV
from utils.date_utils import month_name_to_number, number_to_month_name
from services.sql_generator import generate_sql
from services.sql_validator import validate_sql
from services.response_formatter import (
    format_response,
    ASSISTANT_MODE,
    REMINDER_MODE,
    ERROR_MODE,
    clarify_phrase,
    error_calm_phrase,
    query_invalid_phrase,
)
from services.response_synthesis import build_clean_payload, build_field_answer_payload
from utils.memory_service import MemoryService
from utils.logger import logger
from typing import Dict, List, Optional
import json

class IntentService:
    # Cache AI-generated schema by column names so we don't call the AI on every message
    _schema_cache: Dict[tuple, str] = {}

    # Ordered fields for the "add new job" conversational form.
    _FORM_JOB_FIELDS: List[str] = [
        "job_date",
        "client_name",
        "brand_name",
        "job_description_details",
        "job_notes",
        "language",
        "production_house",
        "studio",
        "qt",
        "length",
        "fees",
    ]

    # Small-talk trigger words / phrases (case-insensitive, matched as whole tokens)
    _SMALL_TALK_TRIGGERS = {
        "hi", "hey", "hello", "hiya", "howdy", "yo", "sup", "heya",
        "how are you", "how r u", "how are u", "how are you doing",
        "how\'s it going", "hows it going", "how do you do",
        "what\'s up", "whats up", "wassup",
        "thanks", "thank you", "thx", "ty", "cheers",
        "bye", "goodbye", "good bye", "see you", "see ya", "cya", "ttyl",
        "ok", "okay", "cool", "got it", "great", "nice", "awesome",
        "good morning", "good afternoon", "good evening", "good night",
        "morning", "afternoon", "evening",
    }

    _SMALL_TALK_RESPONSES = {
        "greeting": [
            "Hey! What can I help you with today?",
            "Hi there! Need an invoice, a query, or something else?",
            "Hello! Ready when you are — just tell me what you need.",
        ],
        "how_are_you": [
            "Doing great, thanks for asking! What can I pull up for you?",
            "All good on my end! What do you need today?",
            "Running smoothly! What can I help with?",
        ],
        "thanks": [
            "Happy to help! Anything else?",
            "Anytime! Let me know if you need more.",
            "Of course! Just ask if there\'s anything else.",
        ],
        "bye": [
            "Take care! Come back anytime.",
            "Goodbye! Have a great day.",
            "See you! I\'ll be here whenever you need me.",
        ],
        "affirmation": [
            "Got it! Let me know if there\'s anything else.",
            "Sure thing! Anything else I can help with?",
        ],
        "time_of_day": [
            "Good to hear from you! What do you need?",
            "Hope your day\'s going well! What can I help with?",
        ],
    }

    def __init__(self):
        self.gemini = GeminiService()
        self.email = ResendEmailService()
        self.supabase = SupabaseService()
        self.memory = MemoryService()
        # Column schema (if provided via COLUMN_SCHEMA env) for AI validation.
        self.column_schema = _COLUMN_SCHEMA_FROM_ENV or {}

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
        payload = build_field_answer_payload(matched_col, value, last_row_data)
        response = self.gemini.synthesize_response(payload, message)
        if response and response.strip():
            return response
        # Fallback if synthesis fails: minimal natural phrasing (no raw field:value)
        if isinstance(value, (int, float)):
            col_lower = matched_col.lower() if matched_col else ""
            if any(term in col_lower for term in ["amount", "fee", "billing", "cost", "price", "total"]):
                return f"The amount was ₹{value:,.0f}."
            return f"The value is {value}."
        return f"That was {value}."

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

        # Validate and store the current answer using AI + COLUMN_SCHEMA (if available)
        current_field = fields[step]
        schema_entry = self.column_schema.get(current_field) if isinstance(self.column_schema, dict) else None
        try:
            validation = self.gemini.validate_field_value(
                column_name=current_field,
                user_input=message,
                column_schema_entry=schema_entry,
            )
        except Exception as e:
            logger.warning(f"Field validation fallback for {current_field}: {e}")
            validation = {
                "is_valid": True,
                "normalized_value": message.strip(),
                "error_message": None,
                "clarification_question": None,
            }

        if not validation.get("is_valid", True):
            # Stay on the same step; ask user to correct the value.
            error_msg = validation.get("error_message") or "That doesn't look right for this field."
            clarification = validation.get("clarification_question")
            details = error_msg
            if clarification:
                details = f"{error_msg} {clarification}"
            response = f"{details}\n\nWhat's the {current_field}?"
            self._store_conversation(user_id, message, response)
            return {"operation": "form_in_progress", "response": response, "trigger_invoice": False, "invoice_data": {}}

        normalized_value = validation.get("normalized_value", message.strip())
        self.memory.set_form_value(user_id, current_field, normalized_value)
        self.memory.advance_form_step(user_id)

        # Check if there's a next field
        next_step = step + 1
        if next_step < len(fields):
            next_field = fields[next_step]
            response = f"Got it! Now, what's the {next_field}?"
            self._store_conversation(user_id, message, response)
            return {"operation": "form_in_progress", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # All fields collected - save to Supabase job_entries
        values = self.memory.complete_form(user_id)
        if values:
            insert_result = self.supabase.insert_job_entry(values)
            if insert_result.get("ok"):
                summary = ", ".join(f"{k}: {v}" for k, v in values.items())
                response = f"Done! I've added the new job: {summary}"
            else:
                response = "I collected all the info but couldn't save it. Please try again later."
        else:
            response = "Something went wrong completing the form."
        self._store_conversation(user_id, message, response)
        return {"operation": "form_complete", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _start_add_job_form(self, user_id: str, message: str) -> Dict:
        """Start the 'add new job' form by asking for the first field."""
        fields = list(self._FORM_JOB_FIELDS) if self._FORM_JOB_FIELDS else []
        if not fields:
            response = "I couldn't determine which columns to use for a new job."
            self._store_conversation(user_id, message, response)
            return {"operation": "form_error", "response": response, "trigger_invoice": False, "invoice_data": {}}
        self.memory.start_form(user_id, fields)
        first_field = fields[0]
        response = f"Let's add a new job! I'll ask you for a few details.\n\nFirst, what's the {first_field}?\n\n(Type 'cancel' anytime to stop.)"
        self._store_conversation(user_id, message, response)
        return {"operation": "form_started", "response": response, "trigger_invoice": False, "invoice_data": {}}


    def _detect_small_talk(self, message: str) -> Optional[str]:
        """
        Returns a canned response if the message is pure small talk, else None.
        Short messages with no data keywords are matched against _SMALL_TALK_TRIGGERS.
        """
        import hashlib
        msg = message.strip().lower().rstrip("!?.,:;")

        data_keywords = {
            "invoice", "bill", "payment", "fees", "client", "job",
            "remind", "overdue", "due", "total", "billing", "record",
            "add", "show", "get", "send", "fetch", "how much", "how many",
            "query", "list", "find", "search", "last", "latest",
        }

        is_exact = msg in self._SMALL_TALK_TRIGGERS
        is_short = len(msg.split()) <= 6
        has_data = any(kw in msg for kw in data_keywords)

        if has_data:
            return None
        if not is_exact:
            if not is_short:
                return None
            multi_match = any(trigger in msg for trigger in self._SMALL_TALK_TRIGGERS if " " in trigger)
            if not multi_match:
                return None

        def _pick(options):
            idx = int(hashlib.md5(message.encode()).hexdigest(), 16) % len(options)
            return options[idx]

        bye_words = {"bye", "goodbye", "good bye", "see you", "see ya", "cya", "ttyl"}
        thanks_words = {"thanks", "thank you", "thx", "ty", "cheers"}
        how_words = {"how are you", "how r u", "how are u", "how are you doing",
                     "how\'s it going", "hows it going", "what\'s up", "whats up", "wassup"}
        time_words = {"good morning", "good afternoon", "good evening", "good night",
                      "morning", "afternoon", "evening"}
        affirmation_words = {"ok", "okay", "cool", "got it", "great", "nice", "awesome"}

        if msg in bye_words:
            return _pick(self._SMALL_TALK_RESPONSES["bye"])
        if msg in thanks_words:
            return _pick(self._SMALL_TALK_RESPONSES["thanks"])
        if any(hw in msg for hw in how_words):
            return _pick(self._SMALL_TALK_RESPONSES["how_are_you"])
        if msg in time_words:
            return _pick(self._SMALL_TALK_RESPONSES["time_of_day"])
        if msg in affirmation_words:
            return _pick(self._SMALL_TALK_RESPONSES["affirmation"])
        return _pick(self._SMALL_TALK_RESPONSES["greeting"])

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


            # 0c. Small talk — respond directly, skip all data paths
            small_talk_response = self._detect_small_talk(message)
            if small_talk_response:
                self._store_conversation(user_id, message, small_talk_response)
                return {
                    "operation": "small_talk",
                    "response": small_talk_response,
                    "trigger_invoice": False,
                    "invoice_data": {},
                }

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
                approaching_days = 7
                payment_terms_days = 30
                targets = self.supabase.fetch_reminder_targets(
                    approaching_days=approaching_days,
                    payment_terms_days=payment_terms_days,
                )
                logger.info(f"[REMINDER] Loaded {len(targets)} reminder targets from Supabase")

                sent = 0
                failed = 0
                sent_details = []
                from datetime import datetime as dt_now
                for t in targets:
                    to_email = (t.get("poc_email") or "").strip()
                    client = (t.get("client_name") or "Client").strip()
                    invoice_number = (t.get("bill_no") or "N/A")
                    if isinstance(invoice_number, (int, float)):
                        invoice_number = str(invoice_number)
                    fees_val = t.get("fees") or 0
                    try:
                        amount_due = f"₹{float(fees_val):,.2f}"
                    except (TypeError, ValueError):
                        amount_due = "₹0.00"
                    due_date_str = (t.get("due_date") or "").strip()[:10] or "N/A"

                    if not to_email:
                        continue
                    ok = self.email.send_payment_reminder(
                        to_email=to_email,
                        client_name=client,
                        invoice_number=invoice_number,
                        amount_due=amount_due,
                        due_date_str=due_date_str,
                    )
                    if ok:
                        row_id = t.get("id")
                        if row_id:
                            self.supabase.update_job_entry_field(row_id, "first_reminder_sent", dt_now.utcnow().isoformat())
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

            # 2. Invoice retrieval (keyword-based; use LLM to extract params, fetch from Supabase)
            is_retrieval = any(w in message.lower() for w in ["get", "download", "send", "give", "show", "retrieve", "fetch"]) and "invoice" in message.lower()
            if is_retrieval:
                schema_info = logic.get_schema_for_intent() if hasattr(logic, "get_schema_for_intent") else None
                intent_result = self.gemini.parse_user_intent(message, conversation_history=conversation_history, schema_info=schema_info)
                params = intent_result.get("parameters", {})
                if intent_result.get("operation") != "GEMINI_ERROR":
                    client_name = (params.get("client_name") or "").strip()
                    month_name = (params.get("month") or "").strip()
                    year_val = params.get("year")
                    bill_number = (params.get("bill_number") or "").strip() or None
                    month_num = month_name_to_number(month_name) if month_name else None
                    if not year_val:
                        from datetime import datetime
                        year_val = datetime.now().year

                    if not client_name and not bill_number:
                        response = "I need a client name or bill number to find an invoice. For example: 'Send invoice for Garnier for March'."
                        self._store_conversation(user_id, message, response)
                        return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}
                    if client_name and not month_num and not bill_number:
                        response = f"I see you want an invoice for {client_name}. Which month? For example: 'Send invoice for {client_name} for March'."
                        self._store_conversation(user_id, message, response)
                        return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}

                    if bill_number:
                        result = self.supabase.fetch_job_entries_for_invoice(client_name="", bill_no=bill_number)
                    else:
                        result = self.supabase.fetch_job_entries_for_invoice(client_name=client_name, month=month_num, year=year_val)
                    if not result.get("ok"):
                        response = result.get("error", "I couldn't fetch invoice data. Please try again.")
                        self._store_conversation(user_id, message, response)
                        return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}
                    rows = result.get("rows") or []
                    if not rows:
                        if client_name and month_num:
                            response = f"I found no invoice for {client_name} for {month_name or month_num} {year_val}."
                        else:
                            response = f"I don't see any records for {client_name or 'that bill'} in my records."
                        self._store_conversation(user_id, message, response)
                        return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}
                    trigger_invoice = True
                    display_client = (rows[0].get("client_name") or client_name or "Client").strip()
                    month_display = month_name
                    if not month_display and rows and rows[0].get("job_date"):
                        jd = str(rows[0]["job_date"])[:10]
                        if len(jd) >= 7:
                            try:
                                month_display = number_to_month_name(int(jd[5:7]))
                            except (ValueError, TypeError):
                                pass
                    if not month_display:
                        month_display = "Request"
                    invoice_data = {"client_name": display_client, "month": month_display, "bill_number": bill_number, "year": year_val}
                    response = f"Confirmed. I've found the record for {display_client}. Generating the invoice now."
                    self._store_conversation(user_id, message, response)
                    return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": trigger_invoice, "invoice_data": invoice_data}

            # 3. Overdue / payment followup (keyword-based; data from Supabase)
            overdue_keywords = ["overdue", "due date", "passed due", "past due", "late payment", "follow up", "followup", "payment followup", "payment status"]
            is_overdue = any(k in message.lower() for k in overdue_keywords) and ("invoice" in message.lower() or "client" in message.lower() or "payment" in message.lower())
            if is_overdue:
                overdue_jobs = self.supabase.fetch_overdue_jobs(payment_terms_days=30)
                if not overdue_jobs:
                    response = "Great news! I don't see any invoices that have passed their due date."
                else:
                    lines = [f"I found {len(overdue_jobs)} invoice(s) past due:\n"]
                    for j in overdue_jobs[:20]:
                        client = (j.get("client_name") or "Unknown").strip()
                        due = (j.get("due_date") or "")[:10]
                        bill = j.get("bill_no") or ""
                        lines.append(f"• {client}" + (f" (Due: {due})" if due else "") + (f" — Bill #{bill}" if bill else ""))
                    response = "\n".join(lines)
                self._store_conversation(user_id, message, response)
                return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # 4. SQL path: intent → generate SQL → validate → execute on Supabase → format → response
            columns = [c for c in JOB_ENTRIES_COLUMNS if not c.startswith("_")]

            if not self.supabase.db_url:
                response = format_response(
                    ERROR_MODE,
                    error_detail="Query service isn't configured right now. I can still help with payment reminders and invoice retrieval.",
                )
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # 4a. Follow-up: answer from last result row via AI synthesis (no raw field:value)
            followup_answer = self._try_answer_from_context(user_id, message, columns)
            if followup_answer:
                logger.info(f"[FOLLOWUP] Answered from context (synthesized)")
                response = followup_answer
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
                    response = format_response(ERROR_MODE)
                    self._store_conversation(user_id, message, response)
                    return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}
                self._update_sql_context(user_id, rows)
                payload = build_clean_payload(rows, "select")
                response = self.gemini.synthesize_response(payload, message)
                if not response or not response.strip():
                    response = "I found matching records but couldn't format the reply. Try asking again?"

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
