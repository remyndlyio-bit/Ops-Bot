from services.gemini_service import GeminiService
from services.resend_email_service import ResendEmailService
from services.supabase_service import SupabaseService, JOB_ENTRIES_COLUMNS, _COLUMN_SCHEMA_FROM_ENV
from utils.date_utils import month_name_to_number, number_to_month_name
from services.sql_generator import generate_sql
from services.sql_validator import validate_sql
from services.query_planner import execute_query_plan
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
from utils.pending_reminders import get_pending, clear_pending, remove_single
from utils.logger import logger
from typing import Dict, List, Optional
import json
import re

class IntentService:
    # Cache AI-generated schema by column names so we don't call the AI on every message
    _schema_cache: Dict[tuple, str] = {}

    # Required fields for smart capture job creation
    _SMART_CAPTURE_REQUIRED = ["brand_name", "job_date", "job_description_details"]

    # Trigger phrases for bank detail commands
    _UPDATE_BANK_TRIGGERS = [
        "update bank details", "update bank detail", "change bank details",
        "set bank details", "edit bank details", "add bank details",
        "update my bank", "change my bank", "set my bank",
        "save bank details", "new bank details",
    ]
    _VIEW_BANK_TRIGGERS = [
        "my bank details", "show bank details", "view bank details",
        "what are my bank details", "bank details", "show my bank",
        "get bank details", "see bank details", "check bank details",
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
        """Handle smart capture confirmation, missing fields, or edit flow."""
        form = self.memory.get_form_state(user_id)
        if not form:
            return None

        # Cancel if user says cancel/stop
        if message.strip().lower() in ("cancel", "stop", "nevermind", "abort", "exit"):
            self.memory.cancel_form(user_id)
            response = "No problem, cancelled. Let me know if you need anything else."
            self._store_conversation(user_id, message, response)
            return {"operation": "form_cancelled", "response": response, "trigger_invoice": False, "invoice_data": {}}

        form_type = form.get("form_type", "smart_capture")

        # --- Smart Capture: awaiting confirmation ---
        if form_type == "smart_capture_confirm":
            return self._handle_smart_capture_confirm(user_id, message, form)

        # --- Smart Capture: awaiting missing fields ---
        if form_type == "smart_capture_missing":
            return self._handle_smart_capture_missing(user_id, message, form)

        # Fallback: cancel unknown form
        self.memory.cancel_form(user_id)
        return None

    def _handle_smart_capture_confirm(self, user_id: str, message: str, form: Dict) -> Dict:
        """Handle Yes/Edit response to smart capture confirmation."""
        msg = message.strip().lower()
        extracted = form.get("values", {})

        if msg in ("yes", "y", "save", "confirm", "done", "ok", "okay", "sure"):
            # Save to database
            return self._save_smart_capture_job(user_id, extracted)

        elif msg in ("edit", "change", "modify", "fix", "no"):
            response = (
                "No problem! Send the corrected job info in one message.\n\n"
                "Example:\n"
                "Bridgestone\n"
                "10 Feb\n"
                "Master film 30 sec\n"
                "Client: The Good Take\n"
                "Fees: 25k"
            )
            self.memory.cancel_form(user_id)
            self.memory.update_user_memory(user_id, {"awaiting_job_input": True})
            self._store_conversation(user_id, message, response)
            return {"operation": "smart_capture_edit", "response": response, "trigger_invoice": False, "invoice_data": {}}

        else:
            response = "Please reply 'Yes' to save or 'Edit' to make changes."
            self._store_conversation(user_id, message, response)
            return {"operation": "smart_capture_confirm_retry", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _handle_smart_capture_missing(self, user_id: str, message: str, form: Dict) -> Dict:
        """Handle response with missing required fields."""
        extracted = form.get("values", {})
        missing = form.get("missing_fields", [])

        # Try to extract fields from the user's response
        new_data = self.gemini.extract_job_fields(message)
        if new_data:
            for k, v in new_data.items():
                if v is not None:
                    extracted[k] = v

        # Check if still missing required fields
        still_missing = [f for f in missing if not extracted.get(f)]
        if still_missing:
            field_labels = {"brand_name": "Brand", "job_date": "Date", "job_description_details": "Job details"}
            missing_str = ", ".join(field_labels.get(f, f) for f in still_missing)
            response = f"I still need: {missing_str}. Please provide them."
            # Update form with new values
            form["values"] = extracted
            form["missing_fields"] = still_missing
            self.memory.start_form(user_id, [], form_override=form)
            self._store_conversation(user_id, message, response)
            return {"operation": "smart_capture_missing_retry", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # All required fields present - show confirmation
        return self._show_smart_capture_confirmation(user_id, extracted)

    def _save_smart_capture_job(self, user_id: str, extracted: Dict) -> Dict:
        """Save the extracted job to database."""
        self.memory.cancel_form(user_id)

        # Map extracted fields to job_entries columns
        record = {"user_id": user_id}
        field_map = {
            "job_date": "job_date",
            "brand_name": "client_name",  # brand_name maps to client_name column
            "client_name": "production_house",  # client/agency maps to production_house
            "job_description_details": "job_description_details",
            "fees": "fees",
            "notes": "notes",
        }
        for src, dst in field_map.items():
            val = extracted.get(src)
            if val is not None:
                record[dst] = val

        insert_result = self.supabase.insert_job_entry(record)
        if insert_result.get("ok"):
            brand = extracted.get("brand_name", "")
            client = extracted.get("client_name", "")
            response = f"Job saved! ✅ {brand} has been added to your records."
            # Store last job context so user can reference "this job" in follow-up
            self.memory.update_user_memory(user_id, {
                "last_saved_job": {
                    "brand_name": brand,
                    "client_name": client,
                    "job_date": extracted.get("job_date"),
                    "job_description_details": extracted.get("job_description_details"),
                    "fees": extracted.get("fees"),
                    "db_client_name": record.get("client_name"),  # what's actually in client_name col
                }
            })
        else:
            logger.error(f"[SMART_CAPTURE] Insert failed: {insert_result.get('error')}")
            response = "I couldn't save the job. Please try again."
        # Build a summary of what was saved for conversation context
        summary = ", ".join(f"{k}: {v}" for k, v in extracted.items() if v is not None)
        self._store_conversation(user_id, f"Save job: {summary}", response)
        return {"operation": "form_complete", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _show_smart_capture_confirmation(self, user_id: str, extracted: Dict) -> Dict:
        """Show confirmation message and wait for Yes/Edit."""
        lines = ["Got it 👍\n"]
        field_labels = [
            ("brand_name", "Brand"),
            ("client_name", "Client"),
            ("job_date", "Date"),
            ("job_description_details", "Details"),
            ("fees", "Fees"),
            ("notes", "Notes"),
        ]
        for key, label in field_labels:
            val = extracted.get(key)
            if val is not None:
                if key == "fees":
                    val = f"₹{val:,}" if isinstance(val, (int, float)) else val
                lines.append(f"{label}: {val}")

        lines.append("\nSave this job? (Yes / Edit)")
        response = "\n".join(lines)

        # Store in form state for confirmation
        form_data = {
            "form_type": "smart_capture_confirm",
            "values": extracted,
            "fields": [],
            "step": 0,
        }
        self.memory.start_form(user_id, [], form_override=form_data)
        # Store the extracted details as user message for context
        summary = ", ".join(f"{k}: {v}" for k, v in extracted.items() if v is not None)
        self._store_conversation(user_id, f"Job details: {summary}", response)
        return {"operation": "smart_capture_confirm", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _start_smart_capture(self, user_id: str, message: str) -> Dict:
        """
        AI Smart Capture: extract job fields from natural language.
        If message only contains trigger words, prompt for details.
        If message contains job data, extract and confirm.
        """
        import re
        # Strip all job-intent phrases to isolate actual job content
        content = message.strip()
        # Remove leading "+" 
        if content.startswith("+"):
            content = content[1:].strip()
        # Remove common intent phrases (anywhere in the message)
        intent_phrases = [
            r"i\s+want\s+to\s+", r"i\'?d\s+like\s+to\s+", r"can\s+you\s+",
            r"please\s+", r"let\s*'?s\s+",
            r"add\s+(?:a\s+)?(?:new\s+)?job\s*", r"new\s+job\s*",
            r"log\s+(?:a\s+)?job\s*", r"record\s+(?:a\s+)?job\s*",
            r"create\s+(?:a\s+)?(?:new\s+)?(?:job|entry)\s*",
        ]
        content_clean = content
        for pat in intent_phrases:
            content_clean = re.sub(pat, "", content_clean, flags=re.IGNORECASE).strip()

        # If no meaningful content remains, prompt for details
        if not content_clean or len(content_clean) < 3:
            self.memory.update_user_memory(user_id, {"awaiting_job_input": True})
            response = (
                "Describe the job in one message.\n\n"
                "Example:\n"
                "Bridgestone\n"
                "10 Feb\n"
                "Master film 30 sec + 4 cutdowns\n"
                "Client: The Good Take\n"
                "Fees: 25k"
            )
            self._store_conversation(user_id, message, response)
            return {"operation": "smart_capture_prompt", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # Content available - extract fields
        return self._extract_and_confirm(user_id, content_clean)

    def _extract_and_confirm(self, user_id: str, content: str) -> Dict:
        """Extract fields from content and show confirmation or ask for missing."""
        self.memory.update_user_memory(user_id, {"awaiting_job_input": False})
        extracted = self.gemini.extract_job_fields(content)

        # Treat all-null extraction as failure
        if extracted and all(v is None for v in extracted.values()):
            extracted = None

        if not extracted:
            response = (
                "I couldn't understand the job details. Please try again.\n\n"
                "Example:\n"
                "Bridgestone\n"
                "10 Feb\n"
                "Master film 30 sec\n"
                "Fees: 25k"
            )
            self.memory.update_user_memory(user_id, {"awaiting_job_input": True})
            self._store_conversation(user_id, content, response)
            return {"operation": "smart_capture_failed", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # Check required fields
        required = ["brand_name", "job_date", "job_description_details"]
        missing = [f for f in required if not extracted.get(f)]

        if missing:
            field_labels = {"brand_name": "Brand", "job_date": "Date", "job_description_details": "Job details"}
            missing_str = ", ".join(field_labels.get(f, f) for f in missing)

            # Show what we got so far + ask for missing
            lines = ["I got some of the details:\n"]
            field_display = [
                ("brand_name", "Brand"), ("client_name", "Client"), ("job_date", "Date"),
                ("job_description_details", "Details"), ("fees", "Fees"), ("notes", "Notes"),
            ]
            for key, label in field_display:
                val = extracted.get(key)
                if val is not None:
                    if key == "fees":
                        val = f"₹{val:,}" if isinstance(val, (int, float)) else val
                    lines.append(f"{label}: {val}")

            lines.append(f"\nI still need: {missing_str}")
            lines.append("Please send the missing info.")
            response = "\n".join(lines)

            form_data = {
                "form_type": "smart_capture_missing",
                "values": extracted,
                "missing_fields": missing,
                "fields": [],
                "step": 0,
            }
            self.memory.start_form(user_id, [], form_override=form_data)
            self._store_conversation(user_id, content, response)
            return {"operation": "smart_capture_missing", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # All required fields present - show confirmation
        return self._show_smart_capture_confirmation(user_id, extracted)

    def _handle_poc_email_response(self, user_id: str, message: str) -> Dict:
        """Handle user providing a client POC email after invoice generation."""
        import re
        user_mem = self.memory.get_user_memory(user_id)

        # Clear awaiting state
        self.memory.update_user_memory(user_id, {"awaiting_poc_email": False})

        # Allow cancel
        if message.strip().lower() in ("cancel", "skip", "no", "nevermind"):
            response = "No problem, skipped. You can add the client email later."
            self._store_conversation(user_id, message, response)
            return {"operation": "poc_email_cancelled", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # Validate email format
        email = message.strip()
        if not re.match(r'^[^@\s]+@[^@\s]+\.[^@\s]+$', email):
            # Not a valid email - re-prompt
            self.memory.update_user_memory(user_id, {"awaiting_poc_email": True})
            response = "That doesn't look like a valid email. Please send the client's email address (e.g. client@agency.com) or type 'skip'."
            self._store_conversation(user_id, message, response)
            return {"operation": "poc_email_retry", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # Save POC email to job entries for this client
        client_name = user_mem.get("poc_email_client", "")
        result = self.supabase.update_poc_email_for_client(user_id, client_name, email)

        if result.get("ok"):
            updated = result.get("updated", 0)
            pdf_path = user_mem.get("poc_email_pdf_path")
            month = user_mem.get("poc_email_month", "")
            year = user_mem.get("poc_email_year")

            # Try to send the invoice email now
            email_sent = False
            if pdf_path:
                try:
                    from services.resend_email_service import ResendEmailService
                    email_svc = ResendEmailService()
                    ok = email_svc.send_invoice_email(
                        to_email=email,
                        client_name=client_name,
                        month=month,
                        year=year or 2026,
                        pdf_path=pdf_path,
                    )
                    email_sent = ok
                except Exception as e:
                    logger.error(f"[POC] Failed to send invoice email after saving POC: {e}")

            if email_sent:
                response = f"Saved! Email {email} has been added for {client_name} and the invoice has been sent. ✅"
                # Update invoice_date for matching rows
                try:
                    self.supabase.execute_sql(
                        f"UPDATE public.job_entries SET invoice_date = CURRENT_DATE "
                        f"WHERE user_id = '{user_id}' AND client_name ILIKE '%{client_name}%' "
                        f"AND invoice_date IS NULL"
                    )
                    logger.info(f"[INVOICE] Updated invoice_date for {client_name} after POC email save")
                except Exception as e:
                    logger.warning(f"[INVOICE] Failed to update invoice_date after POC save: {e}")
            else:
                response = f"Saved! Email {email} has been added for {client_name} ({updated} job{'s' if updated != 1 else ''} updated)."
        else:
            response = f"I couldn't save the email: {result.get('error', 'Unknown error')}. Please try again."

        # Clean up memory
        self.memory.update_user_memory(user_id, {
            "poc_email_client": None,
            "poc_email_pdf_path": None,
            "poc_email_month": None,
            "poc_email_year": None,
        })
        self._store_conversation(user_id, message, response)
        return {"operation": "poc_email_saved", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _prompt_bank_details_format(self, user_id: str, message: str) -> Dict:
        """Ask the user to send all bank details in a single structured message."""
        self.memory.update_user_memory(user_id, {"awaiting_bank_details": True})
        response = (
            "Sure! Please send your bank details in this format:\n\n"
            "Account Name: Darshit Mody\n"
            "Bank Name: HDFC Bank\n"
            "Account Number: 1234567890\n"
            "IFSC: HDFC0001234\n"
            "UPI: darshit@upi\n\n"
            "UPI is optional — skip it if you don't have one.\n"
            "Type 'cancel' to skip."
        )
        self._store_conversation(user_id, message, response)
        return {"operation": "bank_details_prompt", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _handle_bank_details_response(self, user_id: str, message: str) -> Dict:
        """Parse a single structured message containing bank details and upsert."""
        # Clear the awaiting flag first
        self.memory.update_user_memory(user_id, {"awaiting_bank_details": False})

        if message.strip().lower() in ("cancel", "stop", "nevermind", "skip"):
            response = "No problem, bank details update cancelled."
            self._store_conversation(user_id, message, response)
            return {"operation": "bank_details_cancelled", "response": response, "trigger_invoice": False, "invoice_data": {}}

        parsed = self._parse_bank_details_message(message)
        if not parsed:
            response = (
                "I couldn't find the bank details in your message. "
                "Please send them in this format:\n\n"
                "Account Name: Your Name\n"
                "Bank Name: HDFC Bank\n"
                "Account Number: 1234567890\n"
                "IFSC: HDFC0001234\n"
                "UPI: you@upi\n\n"
                "Or type 'cancel' to skip."
            )
            # Re-enable the awaiting flag so user can try again
            self.memory.update_user_memory(user_id, {"awaiting_bank_details": True})
            self._store_conversation(user_id, message, response)
            return {"operation": "bank_details_retry", "response": response, "trigger_invoice": False, "invoice_data": {}}

        result = self.supabase.upsert_user_config(user_id, parsed)
        if result.get("ok"):
            # Check if there's a pending invoice to generate
            user_mem = self.memory.get_user_memory(user_id)
            pending_invoice = user_mem.get("pending_invoice")
            if pending_invoice:
                # Clear pending invoice flag
                self.memory.update_user_memory(user_id, {"pending_invoice": None})
                client_name = pending_invoice.get("client_name", "Client")
                response = (
                    "Your bank details have been saved! ✅\n\n"
                    f"Now generating the invoice for {client_name}..."
                )
                self._store_conversation(user_id, message, response)
                return {
                    "operation": "bank_config_complete",
                    "response": response,
                    "trigger_invoice": True,
                    "invoice_data": pending_invoice
                }
            else:
                response = "Your bank details have been saved successfully! Say 'my bank details' to view them."
        else:
            response = f"I couldn't save your bank details: {result.get('error', 'Unknown error')}. Please try again."
        self._store_conversation(user_id, message, response)
        return {"operation": "bank_config_complete", "response": response, "trigger_invoice": False, "invoice_data": {}}

    @staticmethod
    def _parse_bank_details_message(message: str) -> Optional[Dict[str, str]]:
        """
        Parse a structured message like:
          Account Name: Darshit Mody
          Bank Name: HDFC Bank
          Account Number: 1234567890
          IFSC: HDFC0001234
          UPI: darshit@upi
        Returns dict of bank fields or None if nothing was parseable.
        """
        import re
        text = message.strip()
        result = {}

        # Map of possible labels → db field name
        label_map = {
            "bank_account_name": [r"account\s*(?:holder\s*)?name", r"holder\s*name", r"name\s*on\s*account"],
            "bank_name": [r"bank\s*name", r"bank"],
            "bank_account_number": [r"account\s*(?:no|number|num|#)", r"a/?c\s*(?:no|number|num|#)?"],
            "bank_ifsc": [r"ifsc\s*(?:code)?"],
            "upi_id": [r"upi\s*(?:id)?"],
        }

        for field, patterns in label_map.items():
            for pat in patterns:
                match = re.search(rf"(?:^|\n)\s*{pat}\s*[:=\-]\s*(.+)", text, re.IGNORECASE)
                if match:
                    val = match.group(1).strip().rstrip(",;")
                    if val.lower() not in ("", "none", "na", "n/a", "-", "skip"):
                        result[field] = val
                    break

        # Need at least account name + account number to be useful
        if not result.get("bank_account_name") and not result.get("bank_account_number"):
            return None
        return result if result else None

    def _show_bank_details(self, user_id: str, message: str) -> Dict:
        """Show stored bank details for the user with masked account number."""
        result = self.supabase.get_user_bank_details(user_id)
        if not result.get("ok"):
            response = f"I couldn't retrieve your bank details: {result.get('error', 'Unknown error')}."
        elif not result.get("data"):
            response = "You haven't set up bank details yet. Say 'update bank details' to add them."
        else:
            bd = result["data"]
            acct = bd.get("bank_account_number") or ""
            masked_acct = f"****{acct[-4:]}" if len(acct) >= 4 else acct or "Not set"
            lines = [
                "Your stored bank details:\n",
                f"Account Holder: {bd.get('bank_account_name') or 'Not set'}",
                f"Bank Name: {bd.get('bank_name') or 'Not set'}",
                f"Account Number: {masked_acct}",
                f"IFSC Code: {bd.get('bank_ifsc') or 'Not set'}",
                f"UPI ID: {bd.get('upi_id') or 'Not set'}",
                "\nSay 'update bank details' to change these.",
            ]
            response = "\n".join(lines)
        self._store_conversation(user_id, message, response)
        return {"operation": "bank_details_view", "response": response, "trigger_invoice": False, "invoice_data": {}}

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
            "bank", "update",
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

        msg = message.strip().lower()
        user_name = self._get_user_name(user_id)
        
        bye_words = {"bye", "goodbye", "good bye", "see you", "see ya", "cya", "ttyl"}
        thanks_words = {"thanks", "thank you", "thx", "ty", "cheers"}
        how_words = {"how are you", "how r u", "how are u", "how are you doing",
                     "how\'s it going", "hows it going", "what\'s up", "whats up", "wassup"}
        time_words = {"good morning", "good afternoon", "good evening", "good night",
                      "morning", "afternoon", "evening"}
        affirmation_words = {"ok", "okay", "cool", "got it", "great", "nice", "awesome"}

        # Get base response
        if msg in bye_words:
            response = _pick(self._SMALL_TALK_RESPONSES["bye"])
        elif msg in thanks_words:
            response = _pick(self._SMALL_TALK_RESPONSES["thanks"])
        elif any(hw in msg for hw in how_words):
            response = _pick(self._SMALL_TALK_RESPONSES["how_are_you"])
        elif msg in time_words:
            response = _pick(self._SMALL_TALK_RESPONSES["time_of_day"])
        elif msg in affirmation_words:
            response = _pick(self._SMALL_TALK_RESPONSES["affirmation"])
        else:
            response = _pick(self._SMALL_TALK_RESPONSES["greeting"])
        
        # Personalize if we know the user's name
        if user_name and "Hi there" not in response:  # Avoid double personalization
            response = response.replace("Hey!", f"Hey {user_name}!")
            response = response.replace("Hi there!", f"Hi {user_name}!")
            response = response.replace("Hello!", f"Hello {user_name}!")
        
        return response

    def _handle_pending_reminder(self, user_id: str, message: str) -> Optional[Dict]:
        """
        Check if a WhatsApp user has pending reminders and is replying with
        a number (e.g. '1', '2') to send, or 'skip' to dismiss.
        Returns a response dict if handled, None otherwise.
        """
        pending = get_pending(user_id)
        if not pending:
            return None

        msg = message.strip().lower()

        # "skip" / "skip all" → clear pending
        if msg in ("skip", "skip all", "no", "cancel"):
            clear_pending(user_id)
            response = "⏭ Reminders skipped. You can always send them manually later."
            self._store_conversation(user_id, message, response)
            return {"operation": "reminder", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # Check for a number reply like "1", "2", "send 1", "#1"
        num_match = re.search(r"(\d+)", msg)
        if not num_match:
            return None  # Not a reminder reply, let normal flow handle it

        idx = int(num_match.group(1))
        if idx < 1 or idx > len(pending):
            response = f"Please reply with a number between 1 and {len(pending)}, or 'skip' to skip all."
            self._store_conversation(user_id, message, response)
            return {"operation": "reminder", "response": response, "trigger_invoice": False, "invoice_data": {}}

        reminder = pending[idx - 1]
        job_id = reminder.get("id")
        level = reminder.get("_reminder_level", "first")
        poc_email = reminder.get("poc_email")
        bill_no = reminder.get("bill_no") or "N/A"
        client_name = reminder.get("client_name") or "Client"
        poc_name = reminder.get("poc_name") or client_name
        fees = reminder.get("fees")

        if not poc_email:
            response = f"❌ No email on file for {client_name}. Please add a POC email first."
            self._store_conversation(user_id, message, response)
            return {"operation": "reminder", "response": response, "trigger_invoice": False, "invoice_data": {}}

        try:
            amount_str = f"₹{int(float(fees)):,}"
        except (ValueError, TypeError):
            amount_str = str(fees) if fees else "N/A"

        subject_map = {
            "first": f"First Payment Reminder – Invoice #{bill_no}",
            "second": f"Second Payment Reminder – Invoice #{bill_no}",
            "third": f"Final Payment Reminder – Invoice #{bill_no}",
        }
        subject = subject_map.get(level, f"Payment Reminder – Invoice #{bill_no}")

        # Get sender name
        profile = self.supabase.get_user_profile(user_id)
        sender_name = "Team"
        if profile.get("ok") and profile.get("data"):
            sender_name = profile["data"].get("name") or sender_name

        body = (
            f"Hi {poc_name},\n\n"
            f"This is a friendly reminder regarding invoice #{bill_no}.\n\n"
            f"Amount Due: {amount_str}\n\n"
            f"Please let us know if payment has already been processed.\n\n"
            f"Best regards,\n{sender_name}\n"
        )

        ok = self.email.send_email(to_email=poc_email, subject=subject, body=body)

        if not ok:
            response = f"❌ Failed to send reminder email to {poc_email}. Please try again later."
            self._store_conversation(user_id, message, response)
            return {"operation": "reminder", "response": response, "trigger_invoice": False, "invoice_data": {}}

        # Update DB flag
        flag_map = {
            "first": "first_reminder_sent",
            "second": "second_reminder_sent",
            "third": "third_reminder_sent",
        }
        flag_col = flag_map.get(level)
        if flag_col and job_id:
            update_sql = f"UPDATE public.job_entries SET {flag_col} = NOW() WHERE id = '{job_id}'"
            self.supabase.execute_sql(update_sql)

        # Remove this reminder from pending list
        remove_single(user_id, job_id)

        label_map = {"first": "First", "second": "Second", "third": "Final"}
        label = label_map.get(level, level.title())
        response = f"✅ {label} reminder sent to {poc_email} for invoice #{bill_no}."

        # If more pending, remind user
        remaining = get_pending(user_id)
        if remaining:
            response += f"\n\n{len(remaining)} reminder(s) still pending. Reply with a number or 'skip'."

        self._store_conversation(user_id, message, response)
        return {"operation": "reminder", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def process_request(self, user_id: str, message: str) -> Dict:
        """
        Main handler: keyword-based branches for reminder/invoice/overdue;
        then LLM query plan → validate → resolve time → execute → format.
        """
        # Check if user is new and needs onboarding
        profile = self.supabase.get_user_profile(user_id)
        if not profile.get("ok"):
            logger.error(f"Failed to check user profile for {user_id}: {profile.get('error')}")
        elif not profile.get("data"):
            # New user - start onboarding
            return self._start_onboarding(user_id, message)
        elif not profile.get("data", {}).get("onboarded_at"):
            # User exists but not onboarded - continue onboarding
            return self._continue_onboarding(user_id, message, profile["data"])

        from services.business_logic_service import BusinessLogicService
        logic = BusinessLogicService()
        conversation_history = self.memory.get_conversation_history(user_id)
        trigger_invoice = False
        invoice_data = {}

        try:
            # 0. Check for active form (smart capture confirmation / missing fields)
            form_state = self.memory.get_form_state(user_id)
            if form_state:
                return self._handle_form_step(user_id, message)

            # 0+. Check for pending payment reminders (WhatsApp reply flow)
            reminder_result = self._handle_pending_reminder(user_id, message)
            if reminder_result:
                return reminder_result

            # 0a. Check if user is responding with job data (awaiting smart capture input)
            user_mem = self.memory.get_user_memory(user_id)
            if user_mem.get("awaiting_job_input"):
                return self._extract_and_confirm(user_id, message)

            # 0b. Check for "add job" / "+" trigger → AI Smart Capture
            msg_stripped = message.strip()
            add_job_triggers = ["add job", "add a job", "add new job", "new job",
                               "log a job", "log job", "record job", "record a job"]
            is_add_job = any(t in msg_stripped.lower() for t in add_job_triggers)
            is_plus = msg_stripped.startswith("+") and len(msg_stripped) > 1
            if is_add_job or is_plus:
                return self._start_smart_capture(user_id, message)

            # 0b1.5. Check if user is providing a client POC email
            if user_mem.get("awaiting_poc_email"):
                return self._handle_poc_email_response(user_id, message)

            # 0b2. Check if user is responding with bank details (awaiting state)
            if user_mem.get("awaiting_bank_details"):
                return self._handle_bank_details_response(user_id, message)

            # 0b3. "update bank details" — ask user for details in a specific format
            msg_lower = message.strip().lower()
            if any(t in msg_lower for t in self._UPDATE_BANK_TRIGGERS):
                return self._prompt_bank_details_format(user_id, message)

            # 0b4. "my bank details" / "show bank details" — show stored (masked)
            if any(t in msg_lower for t in self._VIEW_BANK_TRIGGERS):
                return self._show_bank_details(user_id, message)

            # 0b5. Negative intent — user declining a follow-up question
            _NEGATIVE_RESPONSES = {
                "no", "nope", "nah", "not required", "not needed", "no thanks",
                "no thank you", "skip", "don't need", "dont need", "i'm good",
                "im good", "pass", "no need", "that's fine", "thats fine",
                "all good", "not now", "maybe later", "no its fine",
                "no it's fine", "not right now", "i'm fine", "im fine",
            }
            _FOLLOWUP_MARKERS = [
                "would you like", "do you want", "shall i", "want me to",
                "should i", "need a breakdown", "like a breakdown",
                "want a breakdown", "like to see", "want to see",
                "interested in", "like more detail", "want more detail",
            ]
            if msg_lower in _NEGATIVE_RESPONSES:
                # Check if last assistant message was a follow-up question
                if conversation_history:
                    last_msgs = [m for m in conversation_history if m.get("role") == "assistant"]
                    if last_msgs:
                        last_assistant = last_msgs[-1].get("content", "").lower()
                        is_followup = any(marker in last_assistant for marker in _FOLLOWUP_MARKERS) or last_assistant.rstrip().endswith("?")
                        if is_followup:
                            response = "👍 Got it. Let me know if you need anything else."
                            self._store_conversation(user_id, message, response)
                            return {"operation": "decline_followup", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # 0c. Payment reminder queries
            reminder_keywords = [
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
                    user_id=user_id,
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
            msg_lower = message.lower()
            is_retrieval = any(w in msg_lower for w in ["get", "download", "send", "give", "show", "retrieve", "fetch"]) and "invoice" in msg_lower
            if is_retrieval:
                schema_info = logic.get_schema_for_intent() if hasattr(logic, "get_schema_for_intent") else None
                intent_result = self.gemini.parse_user_intent(message, conversation_history=conversation_history, schema_info=schema_info)
                params = intent_result.get("parameters", {})
                if intent_result.get("operation") != "GEMINI_ERROR":
                    # Email-specific override: if user explicitly mentions sending over email,
                    # treat this as SEND_EMAIL instead of a generic ACTION_TRIGGER.
                    email_keywords = [
                        "email invoice",
                        "send invoice over email",
                        "send over email",
                        "mail the invoice",
                        "mail invoice",
                        "share invoice via email",
                        "forward invoice",
                    ]
                    if "email" in msg_lower or "e-mail" in msg_lower or any(k in msg_lower for k in email_keywords):
                        intent_result["operation"] = "SEND_EMAIL"

                    client_name = (params.get("client_name") or "").strip()
                    month_name = (params.get("month") or "").strip()
                    year_val = params.get("year")
                    bill_number = (params.get("bill_number") or "").strip() or None
                    month_num = month_name_to_number(month_name) if month_name else None
                    if not year_val:
                        from datetime import datetime
                        year_val = datetime.now().year

                    # Resolve "this job" / missing client from last saved job context
                    if not client_name and not bill_number:
                        last_job = user_mem.get("last_saved_job")
                        if last_job:
                            # Use the DB column value (brand stored as client_name)
                            client_name = last_job.get("db_client_name") or last_job.get("brand_name", "")
                            if not month_name and last_job.get("job_date"):
                                try:
                                    job_month = int(last_job["job_date"][5:7])
                                    month_name = number_to_month_name(job_month)
                                    month_num = job_month
                                    year_val = int(last_job["job_date"][:4])
                                except (ValueError, IndexError):
                                    pass
                            logger.info(f"[INVOICE] Resolved from last_saved_job: client={client_name}, month={month_name}")

                    if not client_name and not bill_number:
                        response = "I need a client name or bill number to find an invoice. For example: 'Send invoice for Garnier for March'."
                        self._store_conversation(user_id, message, response)
                        return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}
                    if client_name and not month_num and not bill_number:
                        response = f"I see you want an invoice for {client_name}. Which month? For example: 'Send invoice for {client_name} for March'."
                        self._store_conversation(user_id, message, response)
                        return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}

                    if bill_number:
                        result = self.supabase.fetch_job_entries_for_invoice(client_name="", bill_no=bill_number, user_id=user_id)
                    else:
                        result = self.supabase.fetch_job_entries_for_invoice(client_name=client_name, month=month_num, year=year_val, user_id=user_id)
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

                    # Decide between generating/sending invoice via WhatsApp/Telegram vs email
                    if intent_result.get("operation") == "SEND_EMAIL":
                        poc_email = (rows[0].get("poc_email") or "").strip()
                        if not poc_email:
                            response = f"I found the invoice for {display_client} but there's no contact email (poc_email) stored."
                            self._store_conversation(user_id, message, response)
                            return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}

                        # Reuse existing PDF if present; otherwise generate it
                        from services.invoice_service import InvoiceService
                        from services.invoice_generation_service import InvoiceGenerationService

                        safe_client = display_client.replace(" ", "_")
                        safe_month = month_display.replace(" ", "_")
                        pdf_candidate = os.path.join("output", f"Invoice_{safe_client}_{safe_month}.pdf")

                        pdf_path = pdf_candidate if os.path.exists(pdf_candidate) else None
                        if not pdf_path:
                            summary = InvoiceService.process_invoice_data(rows, display_client, month_display)
                            bank_result = self.supabase.get_user_bank_details(user_id)
                            bank_details = bank_result.get("data") if bank_result.get("ok") else None
                            pdf_path = InvoiceGenerationService().generate_pdf(summary, rows, bank_details=bank_details)

                        if not pdf_path:
                            response = "I tried to generate the invoice PDF but something went wrong. Please try again."
                            self._store_conversation(user_id, message, response)
                            return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}

                        # Send via Resend with attachment
                        ok = self.email.send_invoice_email(
                            to_email=poc_email,
                            client_name=display_client,
                            month=month_display,
                            year=year_val,
                            pdf_path=pdf_path,
                        )
                        if ok:
                            response = f"The invoice has been sent to {poc_email}."
                            # Update invoice_date for all affected rows
                            row_ids = [r["id"] for r in rows if r.get("id")]
                            if row_ids:
                                ids_str = ",".join(f"'{rid}'" for rid in row_ids)
                                self.supabase.execute_sql(
                                    f"UPDATE public.job_entries SET invoice_date = CURRENT_DATE WHERE id IN ({ids_str})"
                                )
                                logger.info(f"[INVOICE] Updated invoice_date for {len(row_ids)} row(s)")
                        else:
                            response = "I couldn't send the invoice email. Please check the email configuration and try again."
                        self._store_conversation(user_id, message, response)
                        return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}

                    # Check if bank details exist before generating invoice
                    bank_result = self.supabase.get_user_bank_details(user_id)
                    bank_details = bank_result.get("data") if bank_result.get("ok") else None
                    if not bank_details or not bank_details.get("bank_account_number"):
                        # No bank details - prompt user to add them
                        self.memory.update_user_memory(user_id, {
                            "pending_invoice": invoice_data
                        })
                        response = (
                            f"I found the records for {display_client}, but you haven't added your bank details yet.\n\n"
                            "Please send your bank details in this format:\n\n"
                            "Account Name: Your Name\n"
                            "Bank Name: HDFC Bank\n"
                            "Account Number: 1234567890\n"
                            "IFSC: HDFC0001234\n"
                            "UPI: you@upi (optional)\n\n"
                            "Once saved, I'll generate the invoice automatically."
                        )
                        self.memory.update_user_memory(user_id, {"awaiting_bank_details": True})
                        self._store_conversation(user_id, message, response)
                        return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": False, "invoice_data": {}}

                    # Default path: generate PDF and send via WhatsApp/Telegram (existing behavior)
                    trigger_invoice = True
                    response = f"Confirmed. I've found the record for {display_client}. Generating the invoice now."
                    self._store_conversation(user_id, message, response)
                    return {"operation": "ACTION_TRIGGER", "response": response, "trigger_invoice": trigger_invoice, "invoice_data": invoice_data}

            # 3. Overdue / payment followup (keyword-based; data from Supabase)
            overdue_keywords = ["overdue", "due date", "passed due", "past due", "late payment", "follow up", "followup", "payment followup", "payment status"]
            is_overdue = any(k in message.lower() for k in overdue_keywords) and ("invoice" in message.lower() or "client" in message.lower() or "payment" in message.lower())
            if is_overdue:
                overdue_jobs = self.supabase.fetch_overdue_jobs(payment_terms_days=30, user_id=user_id)
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

            # 4a. Check for invoice confirmation (Yes/No after being asked)
            msg_lower = message.strip().lower()
            if msg_lower in ("yes", "y", "sure", "ok", "okay", "please do", "generate", "create"):
                # Check if we recently asked about invoice generation
                conv = self.memory.get_conversation_history(user_id)
                if conv and len(conv) >= 2:
                    last_assistant = conv[-1].get("content", "").lower() if conv[-1].get("role") == "assistant" else ""
                    if "generate an invoice" in last_assistant and "would you like" in last_assistant:
                        # Generate invoice for the last job we found
                        ctx = self.memory.get_user_memory(user_id).get("uscf_context", {})
                        last_row = ctx.get("last_row_data") if ctx else None
                        if last_row:
                            client_name = last_row.get("client_name", "Client")
                            job_date = last_row.get("job_date")
                            month = None
                            year = None
                            if job_date:
                                try:
                                    from datetime import datetime
                                    if isinstance(job_date, str):
                                        job_dt = datetime.fromisoformat(job_date[:10])
                                    else:
                                        job_dt = job_date
                                    month = job_dt.strftime("%B")
                                    year = job_dt.year
                                except:
                                    pass
                            
                            invoice_data = {
                                "client_name": client_name,
                                "month": month or "Period",
                                "bill_number": None,
                                "year": year
                            }
                            response = f"Generating invoice for {client_name}..."
                            self._store_conversation(user_id, message, response)
                            return {
                                "operation": "ACTION_TRIGGER",
                                "response": response,
                                "trigger_invoice": True,
                                "invoice_data": invoice_data
                            }

            # 4b. Follow-up: answer from last result row via AI synthesis (no raw field:value)
            followup_answer = self._try_answer_from_context(user_id, message, columns)
            if followup_answer:
                logger.info(f"[FOLLOWUP] Answered from context (synthesized)")
                response = followup_answer
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            # Generate SQL via query planner pipeline (Classify → Plan → Resolve → Validate → SQL)
            conv_ctx = user_mem.get("uscf_context") or {}
            conv_ctx["last_saved_job"] = user_mem.get("last_saved_job")
            plan_result = execute_query_plan(
                message, self.gemini, self.supabase,
                conversation_history, user_id=user_id,
                conversation_context=conv_ctx,
            )

            # Handle clarification from planner
            if plan_result.get("clarification"):
                response = plan_result["clarification"]
                self._store_conversation(user_id, message, response)
                return {"operation": "query", "response": response, "trigger_invoice": False, "invoice_data": {}}

            sql = plan_result.get("sql")
            planner_failed = plan_result.get("_error") or not sql

            # Fallback to direct SQL generation if planner fails
            if planner_failed:
                logger.info(f"[PIPELINE] Planner failed ({plan_result.get('_error')}), falling back to direct SQL generation")
                sql_result = generate_sql(message, self.gemini, self.supabase, conversation_history, user_id=user_id)
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

            if op == "update":
                rowcount = exec_result.get("rowcount", 0)
                if rows:
                    self._update_sql_context(user_id, rows)
                    payload = build_clean_payload(rows, "select")
                    response = self.gemini.synthesize_response(payload, message)
                    if not response or not response.strip():
                        response = f"Done! Updated {rowcount} record{'s' if rowcount != 1 else ''}."
                else:
                    response = f"Done! Updated {rowcount} record{'s' if rowcount != 1 else ''}."
            elif op == "insert":
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
            user_name = self._get_user_name(user_id)
            if user_name:
                response = format_response(ERROR_MODE, error_detail=f"Sorry {user_name}, {error_calm_phrase().lower()}")
            else:
                response = format_response(ERROR_MODE, error_detail=error_calm_phrase())

        self._store_conversation(user_id, message, response)
        return {
            "operation": "query",
            "response": response,
            "trigger_invoice": trigger_invoice,
            "invoice_data": invoice_data
        }

    def _get_user_name(self, user_id: str) -> str:
        """Get user's name from profile, return None if not found."""
        profile = self.supabase.get_user_profile(user_id)
        if profile.get("ok") and profile.get("data"):
            return profile["data"].get("name")
        return None

    def _start_onboarding(self, user_id: str, message: str) -> Dict:
        """Start onboarding for a new user."""
        # Determine platform from user_id format
        platform = "telegram" if user_id.isdigit() else "whatsapp"
        
        # Create initial profile
        self.supabase.upsert_user_profile(user_id, platform, {"platform": platform})
        
        response = self._get_welcome_message(platform)
        self._store_conversation(user_id, message, response)
        return {"operation": "onboarding_started", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _continue_onboarding(self, user_id: str, message: str, profile: Dict) -> Dict:
        """Continue onboarding based on what info we already have."""
        platform = profile.get("platform", "telegram")
        
        # Check what step we're on
        if not profile.get("name"):
            # Step 1: Get name
            raw_name = message.strip()
            if raw_name.lower() in ("skip", "no", "n/a"):
                name = "there"  # Generic greeting instead of user_id
            else:
                # Extract just the name part from common patterns
                name_patterns = [
                    "my name is ",
                    "i'm ",
                    "i am ",
                    "call me ",
                    "this is ",
                    "it's ",
                    "its ",
                ]
                name = raw_name
                for pattern in name_patterns:
                    if pattern.lower() in raw_name.lower():
                        # Extract text after the pattern
                        idx = raw_name.lower().find(pattern.lower())
                        name = raw_name[idx + len(pattern):].strip()
                        break
                # If name is too long, it might still include extra words
                if len(name.split()) > 3:
                    # Take first 2-3 words as name
                    name = " ".join(name.split()[:2])
                # Capitalize properly
                if name:
                    name = name.title()
            
            result = self.supabase.upsert_user_profile(user_id, platform, {"name": name})
            if not result.get("ok"):
                logger.error(f"[ONBOARDING] Failed to save name for {user_id}: {result.get('error')}")
                # Still respond but log the error
            
            response = (
                f"Nice to meet you, {name}! 🎉\n\n"
                "What's your company or business name?\n"
                "(Type 'skip' to use your name)"
            )
            self._store_conversation(user_id, message, response)
            return {"operation": "onboarding_name", "response": response, "trigger_invoice": False, "invoice_data": {}}
        
        elif not profile.get("company_name"):
            # Step 2: Get company name, then complete onboarding
            company = message.strip()
            if company.lower() in ("skip", "no", "n/a"):
                company = profile.get("name", "Your Business")
            
            # Save company name AND mark as onboarded in one call
            from datetime import datetime
            self.supabase.upsert_user_profile(user_id, platform, {
                "company_name": company,
                "onboarded_at": datetime.now().isoformat()
            })
            
            user_name = profile.get("name", "there")
            response = (
                f"Great, {user_name}! You're all set! ✅\n\n"
                "Here's how to use me:\n\n"
                "📊 View data:\n"
                "• 'How many jobs this month?'\n"
                "• 'Total fees for Client X'\n\n"
                "📄 Generate invoices:\n"
                "• 'Send invoice to Client for March'\n\n"
                "✏️ Add jobs:\n"
                "• 'Add a job for Client X'\n\n"
                "💳 Bank details:\n"
                "• 'Update bank details'\n\n"
                "Try it now! Say 'Add a job' to get started."
            )
            self._store_conversation(user_id, message, response)
            return {"operation": "onboarding_complete", "response": response, "trigger_invoice": False, "invoice_data": {}}
        
        else:
            # Shouldn't reach here, but complete onboarding if somehow stuck
            return self._complete_onboarding(user_id, message)

    def _get_welcome_message(self, platform: str) -> str:
        """Get platform-specific welcome message."""
        if platform == "telegram":
            return (
                "👋 Welcome! I'm your personal invoice assistant.\n\n"
                "I help you:\n"
                "• Track jobs and payments\n"
                "• Generate professional invoices\n"
                "• Send payment reminders\n\n"
                "Let's get started! What's your name?"
            )
        else:  # WhatsApp
            return (
                "Welcome to Ops Bot! 🤖\n\n"
                "I help manage your invoices and payments.\n"
                "What's your business name to get started?"
            )

    def _handle_excel_import(self, user_id: str, message: str) -> Dict:
        """Handle Excel file import choice."""
        response = (
            "📎 To import from Excel:\n\n"
            "1. Download the template from: [Your template URL]\n"
            "2. Fill it with your job data\n"
            "3. Send the file here\n\n"
            "Or reply 'back' to choose another option."
        )
        self._store_conversation(user_id, message, response)
        return {"operation": "onboarding_excel", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _handle_csv_import(self, user_id: str, message: str) -> Dict:
        """Handle CSV import choice."""
        response = (
            "📋 Paste your CSV data in this format:\n\n"
            "Client Name,Job Description,Date,Fees,Email\n"
            "Garnier,Short animation,2026-02-20,2000,email@example.com\n\n"
            "Send your data or reply 'back' to choose another option."
        )
        self._store_conversation(user_id, message, response)
        return {"operation": "onboarding_csv", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _handle_manual_entry(self, user_id: str, message: str) -> Dict:
        """Handle manual entry choice."""
        response = (
            "✏️ I'll help you add jobs manually!\n\n"
            "Let's add your first job. What's the client name?\n\n"
            "(Type 'cancel' anytime to stop)"
        )
        self._store_conversation(user_id, message, response)
        return {"operation": "onboarding_manual", "response": response, "trigger_invoice": False, "invoice_data": {}}

    def _complete_onboarding(self, user_id: str, message: str) -> Dict:
        """Complete the onboarding process."""
        # Mark as onboarded
        from datetime import datetime
        self.supabase.upsert_user_profile(user_id, "", {"onboarded_at": datetime.now().isoformat()})
        
        response = (
            "✅ You're all set! Here's how to use me:\n\n"
            "📊 View data:\n"
            "• 'How many jobs for Client X?'\n"
            "• 'Total fees this month'\n"
            "• 'Last payment date'\n\n"
            "📄 Generate invoices:\n"
            "• 'Send invoice to Client for March'\n"
            "• 'Generate invoice for last job'\n\n"
            "💳 Manage bank details:\n"
            "• 'Update bank details'\n"
            "• 'My bank details'\n\n"
            "Try: 'Show my jobs from last week'"
        )
        self._store_conversation(user_id, message, response)
        return {"operation": "onboarding_complete", "response": response, "trigger_invoice": False, "invoice_data": {}}

    @staticmethod
    def get_help_text() -> str:
        return (
            "I'm your conversational assistant! Here's what I can do:\n\n"
            "✏️ Add a job (one message!):\n"
            "Add job\n"
            "Bridgestone\n"
            "10 Feb\n"
            "Master film 30 sec + 4 cutdowns\n"
            "Client: The Good Take\n"
            "Fees: 25k\n\n"
            "Or ultra-fast: + Bridgestone 10 Feb 25k master film\n\n"
            "📄 Invoices: 'Send invoice to Garnier for April'\n"
            "📊 Queries: 'Total fees this month' / 'Jobs for Client X'\n"
            "💳 Bank: 'Update bank details' / 'My bank details'\n\n"
            "How can I help you today?"
        )
