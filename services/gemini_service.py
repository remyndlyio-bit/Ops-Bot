import os
import json
import httpx
from typing import List, Dict, Optional
from utils.logger import logger

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "google/gemini-2.5-flash"


class GeminiService:
    """
    AI backend via OpenRouter. Set one env variable: AI_KEY (your OpenRouter API key).
    """
    def __init__(self):
        self.api_key = None
        self.model_name = DEFAULT_MODEL
        self._initialized = False
        self._ensure_initialized()

    def _ensure_initialized(self) -> bool:
        """Initialize from AI_KEY. Called at startup and lazily on first use."""
        if self._initialized:
            return True
        raw = os.getenv("AI_KEY")
        api_key = (raw or "").strip()
        if not api_key:
            logger.warning("AI_KEY not set. Will retry on first request.")
            return False
        logger.info(f"AI_KEY loaded (length={len(api_key)}). Verifying OpenRouter...")
        self.api_key = api_key
        ok = self._verify()
        self._initialized = ok
        if not ok:
            logger.error("OpenRouter verification failed. Check key at https://openrouter.ai/keys")
        return ok

    def _verify(self) -> bool:
        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.post(
                    OPENROUTER_URL,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self.model_name,
                        "messages": [{"role": "user", "content": "hi"}],
                        "max_tokens": 1,
                    },
                )
                if response.status_code == 200:
                    logger.info(f"Verified OpenRouter model: {self.model_name}")
                    return True
                logger.error(f"OpenRouter verification failed: {response.status_code} - {response.text[:200]}")
        except Exception as e:
            logger.error(f"OpenRouter verification error: {e}")
        return False

    def _call_api(
        self,
        prompt: str,
        generation_config: Optional[Dict] = None,
    ) -> Optional[str]:
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            raise Exception("AI not initialized (set AI_KEY)")
        payload = {
            "model": self.model_name,
            "messages": [{"role": "user", "content": prompt}],
        }
        if generation_config:
            if "maxOutputTokens" in generation_config:
                payload["max_tokens"] = generation_config["maxOutputTokens"]
            if "temperature" in generation_config:
                payload["temperature"] = generation_config["temperature"]
            if generation_config.get("responseMimeType") == "application/json":
                payload["response_format"] = {"type": "json_object"}
        try:
            with httpx.Client(timeout=30.0) as client:
                response = client.post(
                    OPENROUTER_URL,
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json=payload,
                )
                response.raise_for_status()
                result = response.json()
                if "choices" in result and len(result["choices"]) > 0:
                    choice = result["choices"][0]
                    if "message" in choice and "content" in choice["message"]:
                        return choice["message"]["content"]
                logger.error(f"OpenRouter unexpected response: {json.dumps(result)[:500]}")
                return None
        except httpx.HTTPStatusError as e:
            logger.error(f"OpenRouter HTTP error: {e.response.status_code} - {e.response.text[:500]}")
            raise
        except Exception as e:
            logger.error(f"OpenRouter error: {str(e)}")
            raise

    def generate_schema_from_columns(
        self,
        column_names: List[str],
        sample_row: Optional[Dict] = None,
    ) -> Optional[str]:
        """
        Ask the AI to generate a short schema description from the actual sheet columns.
        Used so the query planner always gets an accurate, semantic schema (which column is date, client, fees, job, etc.).
        Returns None on failure; caller should fall back to rule-based schema.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key or not column_names:
            return None
        columns_str = ", ".join(f'"{c}"' for c in column_names[:80])
        sample_section = ""
        if sample_row and isinstance(sample_row, dict):
            sample_pairs = [f"{k}: {str(v)[:50]}" for k, v in list(sample_row.items())[:20]]
            sample_section = "\nExample row (column: value): " + " | ".join(sample_pairs)

        prompt = (
            "Given these column headers from a spreadsheet, write a SHORT schema description for a query planner.\n\n"
            f"Columns: {columns_str}\n"
            f"{sample_section}\n\n"
            "In 5–10 lines, list:\n"
            "- Which column(s) are the DATE column (invoice/job date – use for 'when', 'last gig', time filters). Use the exact column name.\n"
            "- Which column(s) are CLIENT (client name, production house). Use exact name.\n"
            "- Which column is the FEES/BILLING/AMOUNT column (numeric – use for sum, total billing). Use exact name.\n"
            "- Which column(s) are JOB/PROJECT (job name, role, task). Use exact name.\n"
            "- Any other columns that are useful for filtering (e.g. status, paid, notes). Use exact names.\n\n"
            "Write ONLY the schema description, no preamble. Use the exact column names as they appear. Keep it concise."
        )
        try:
            out = self._call_api(prompt, generation_config={"temperature": 0, "maxOutputTokens": 512})
            if out and isinstance(out, str) and len(out.strip()) > 10:
                logger.info(f"AI-generated schema length: {len(out)} chars")
                return out.strip()
        except Exception as e:
            logger.warning(f"AI schema generation failed: {e}")
        return None

    def synthesize_response(self, structured_payload: dict, user_message: str) -> Optional[str]:
        """
        AI synthesis layer: convert clean structured JSON into natural response.
        Uses ONLY provided data; no hallucination; omits nulls gracefully; concise.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            return None

        system = (
            "You are a smart, professional personal operations assistant chatting over WhatsApp.\n"
            "You should sound concise, confident, and a bit more human and interesting than a basic status bot.\n\n"
            "DATA RULES:\n"
            "- Use ONLY the provided structured data. Do not invent or assume values. Do not expose technical fields.\n"
            "- If a field is null, omit it naturally.\n"
            "- Convert dates to readable format (e.g. 20 Feb 2026). Mention fees naturally (e.g. ₹2,000).\n"
            "- For multiple records: summarize intelligently, do not dump rows.\n"
            "- For field_answer type: answer the user's question naturally (e.g. 'Your most recent client was Xiaomi.', 'That project was valued at ₹4,000.'). Never output raw field:value or one-word answers.\n"
            "- Output plain text only, no bullet lists or key:value format. 2–4 sentences max.\n\n"
            "TONE RULES:\n"
            "- Default: composed and minimal, but you can use light transitions like 'Here’s the snapshot', 'Quick read:', or 'In short,'.\n"
            "- When context justifies it, add subtle momentum (never hype or over-celebrate):\n"
            "  * High-value project or record/highest metric: 'solid project', 'strong number', 'tops the list'.\n"
            "  * Payment completed: 'Payment has been received and recorded. Good progress.'\n"
            "  * Growth implied: 'nice milestone', 'that’s a good one'.\n"
            "- Use these sparingly; only when the data genuinely warrants it.\n"
            "- Never: emojis, exclamation marks, or over-celebrate. Sound confident, warm, and slightly energized, not chatty."
        )

        try:
            payload_str = json.dumps(structured_payload, default=str)
        except (TypeError, ValueError):
            payload_str = str(structured_payload)

        full_prompt = (
            f"{system}\n\n"
            f"DATA:\n{payload_str}\n\n"
            f"USER ASKED: {user_message}\n\n"
            "Your response:"
        )

        try:
            out = self._call_api(
                full_prompt,
                generation_config={"temperature": 0.2, "maxOutputTokens": 300},
            )
            if out and isinstance(out, str) and out.strip():
                return out.strip()
        except Exception as e:
            logger.warning(f"Response synthesis failed: {e}")
        return None

    def make_response(
        self,
        user_message: str,
        conversation_history: Optional[List[Dict[str, str]]] = None,
        factual_output: str = "",
    ) -> Optional[str]:
        """
        Response maker: produce final user-facing reply from RAG output.
        Uses only provided facts; natural, conversational; matches tone; plain text only.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            return None
        context_lines = []
        if conversation_history:
            for msg in conversation_history:
                role = "User" if msg.get("role") == "user" else "Assistant"
                context_lines.append(f"{role}: {msg.get('content', '')}")
        context_block = "\n".join(context_lines) if context_lines else "(no prior messages)"

        prompt = (
            "You are a response maker. You get the user's message, recent conversation, and factual output from a data system. "
            "Produce the final reply to the user.\n\n"
            "RULES:\n"
            "- Use ONLY the provided facts. Do not invent or add facts.\n"
            "- Respond naturally and conversationally (not robotic).\n"
            "- Avoid repeating the user's question verbatim.\n"
            "- Combine related facts when appropriate.\n"
            "- Match the user's tone (casual vs direct).\n"
            "- If the factual output is empty or says nothing matched, ask one brief clarification (e.g. narrow by client or time?).\n\n"
            "STYLE:\n"
            "- Friendly, concise, human.\n"
            "- No templates like \"The X was Y\" or \"Here is the information\".\n"
            "- Plain text only. No explanations, no metadata. Output ONLY the reply.\n\n"
            f"Recent conversation:\n{context_block}\n\n"
            f"User message:\n{user_message}\n\n"
            f"Factual output from system:\n{factual_output or '(none)'}\n\n"
            "Your reply:"
        )
        try:
            out = self._call_api(prompt, generation_config={"temperature": 0.3, "maxOutputTokens": 512})
            if out and isinstance(out, str):
                return out.strip()
        except Exception as e:
            logger.warning(f"Response maker failed: {e}")
        return None

    def parse_user_intent(
        self,
        message: str,
        conversation_history: List[Dict[str, str]] = None,
        schema_info: Optional[str] = None,
    ) -> dict:
        """
        Schema-aware intent parsing with confidence, semantic column mapping, and ambiguity handling.
        When confidence is low, returns NEED_CLARIFICATION with a concise clarification question.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            logger.error("AI not initialized.")
            return {
                "operation": "GEMINI_ERROR",
                "entity": None,
                "parameters": {},
                "confidence": 0.0,
                "clarification_question": None,
                "resolved_columns": {},
                "error_message": "AI not initialized. Set AI_KEY in Railway and redeploy.",
            }
        context_section = ""
        if conversation_history and len(conversation_history) > 0:
            context_lines = ["Recent conversation history:"]
            for msg in conversation_history:
                role_label = "User" if msg.get("role") == "user" else "Assistant"
                context_lines.append(f"{role_label}: {msg.get('content', '')}")
            context_section = "\n".join(context_lines) + "\n\n"
        schema_section = ""
        if schema_info:
            schema_section = (
                "DATA SCHEMA (from COLUMN_NAMES / COLUMN_NAME env):\n"
                f"{schema_info}\n\n"
                "Use this schema to map user intent to the correct columns. Prefer time-aware columns "
                "(invoice_date, order_by) when the query implies recency (latest, last, previous, first).\n\n"
            )
        system_prompt = (
            "You are a schema-aware Intent and Parameter Parser for an Operations Bot. "
            "Infer the user's underlying intent, not just literal phrasing. Be conversational and context-aware like a smart assistant.\n\n"
            f"{schema_section}"
            "INTENT UNDERSTANDING:\n"
            "- Identify: timeline-based (latest, last, previous, first), role/task/job/client/invoice/payment, summary vs comparison vs specific record.\n"
            "- Use SEMANTIC COLUMN MAPPING: map queries to the correct data fields from the schema above.\n"
            "- Prefer time-aware columns when the query implies recency.\n\n"
            "CONTEXT & REFERENCE RESOLUTION (CRITICAL):\n"
            "- Use conversation history to resolve pronouns and references. If the Assistant just said something (e.g. 'Your last gig was on 2026-01-18'), and the user asks 'What was it about?' or 'Client?' or 'What job did I do on this date?', resolve 'it' / 'this date' to that specific date and set specific_date to that date (YYYY-MM-DD).\n"
            "- If the user just listed or asked about 'client names in my records' and then asks 'What are the dates on these jobs?' or 'What is the billing amount?', resolve 'these jobs' / 'these' to all jobs (set scope to 'all').\n"
            "- When the user replies with only 'Jobs', 'Invoices', or 'Clients' after a clarification, use the PREVIOUS user message or assistant context for the time period (e.g. 'December', 'last quarter'). So 'Jobs' after 'December' means: show billing or job info for December.\n"
            "- When the user says 'All' or 'All jobs' for billing, set scope to 'all' and do not require month/client.\n"
            "- Temporal expressions: 'last quarter' -> set period to 'quarter' (backend will compute Oct–Dec or Jan–Mar etc.). 'December' with no year -> set month to 'December', year can be null (backend will infer current or previous year).\n\n"
            "AMBIGUITY HANDLING:\n"
            "- If multiple interpretations are equally valid OR required column cannot be confidently inferred, set operation to NEED_CLARIFICATION and confidence < 0.7 with a concise clarification_question.\n"
            "- When context clearly disambiguates (e.g. assistant just said a date), prefer resolving from context over asking.\n\n"
            "CONFIDENCE: Only set a non-NEED_CLARIFICATION operation if confidence >= 0.7.\n\n"
            "STRICT SCHEMA (MUST RETURN ALL KEYS):\n"
            "{\n"
            '  "operation": "READ_ENTITY | AGGREGATE_ENTITY | CREATE_ENTITY | UPDATE_ENTITY | ACTION_TRIGGER | SCHEDULE_REMINDER | SMALL_TALK | NEED_CLARIFICATION | UNKNOWN",\n'
            '  "entity": "client | invoice | job | payment | project | bank_details | gst_details | reminder | communication_log | null",\n'
            '  "confidence": number (0.0 to 1.0),\n'
            '  "clarification_question": string | null (concise question when ambiguous),\n'
            '  "resolved_columns": {"order_by": string | null, "filter_by": string | null, "display": string | null},\n'
            '  "timeline_hint": "latest | last | previous | first | none | null",\n'
            "  \"parameters\": {\n"
            "    \"client_name\": string | null,\n"
            "    \"bill_number\": string | null,\n"
            "    \"month\": string | null,\n"
            "    \"year\": number | null,\n"
            "    \"period\": \"day | month | quarter | year | null\",\n"
            "    \"days\": number | null,\n"
            "    \"specific_date\": string | null (YYYY-MM-DD when user refers to a date from context, e.g. last gig date),\n"
            "    \"scope\": \"all | null\" (set to \"all\" when user says all jobs, all clients, or refers to \"these\" jobs from just-listed context)\n"
            "  }\n"
            "}\n\n"
            f"{context_section}"
            "EXAMPLES:\n"
            "1. 'What is the total billing for April for Garnier?'\n"
            "   -> {\"operation\": \"AGGREGATE_ENTITY\", \"entity\": \"invoice\", \"confidence\": 0.95, \"clarification_question\": null, \"resolved_columns\": {\"order_by\": null, \"filter_by\": \"invoice_date\", \"display\": null}, \"timeline_hint\": null, \"parameters\": {\"client_name\": \"Garnier\", \"bill_number\": null, \"month\": \"April\", \"year\": null, \"period\": \"month\", \"days\": null}}\n"
            "2. 'What was my last job?' (ambiguous: could mean job record, job title, task)\n"
            "   -> If schema has Job column: {\"operation\": \"READ_ENTITY\", \"entity\": \"job\", \"confidence\": 0.85, \"clarification_question\": null, \"resolved_columns\": {\"order_by\": \"invoice_date\", \"filter_by\": null, \"display\": \"job\"}, \"timeline_hint\": \"latest\", \"parameters\": {\"client_name\": null, \"bill_number\": null, \"month\": null, \"year\": null, \"period\": null, \"days\": null}}\n"
            "   -> If ambiguous: {\"operation\": \"NEED_CLARIFICATION\", \"entity\": \"job\", \"confidence\": 0.5, \"clarification_question\": \"Do you mean your most recent client project, your last job title, or the last task you completed?\", \"resolved_columns\": {}, \"timeline_hint\": \"latest\", \"parameters\": {}}\n"
            "3. 'Send me invoice #101'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"confidence\": 0.98, \"clarification_question\": null, \"resolved_columns\": {}, \"timeline_hint\": null, \"parameters\": {\"client_name\": null, \"bill_number\": \"101\", \"month\": null, \"year\": null, \"period\": null, \"days\": null}}\n"
            "4. 'Can I follow up for a payment'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"payment\", \"confidence\": 0.9, \"clarification_question\": null, \"resolved_columns\": {}, \"timeline_hint\": null, \"parameters\": {\"client_name\": null, \"bill_number\": null, \"month\": null, \"year\": null, \"period\": null, \"days\": null}}\n"
            "5. 'Get me Garnier invoice for April'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"confidence\": 0.95, \"clarification_question\": null, \"resolved_columns\": {}, \"timeline_hint\": null, \"parameters\": {\"client_name\": \"Garnier\", \"bill_number\": null, \"month\": \"April\", \"year\": null, \"period\": \"month\", \"days\": null}}\n"
            "6. 'Display clients for April' or 'Show clients for April' or 'List clients in April'\n"
            "   -> {\"operation\": \"READ_ENTITY\", \"entity\": \"client\", \"confidence\": 0.95, \"clarification_question\": null, \"resolved_columns\": {\"filter_by\": \"invoice_date\", \"order_by\": null, \"display\": null}, \"timeline_hint\": null, \"parameters\": {\"client_name\": null, \"bill_number\": null, \"month\": \"April\", \"year\": null, \"period\": \"month\", \"days\": null}}\n"
            "7. Context: 'Get me Garnier invoice for April', current 'Send it again'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"confidence\": 0.95, \"clarification_question\": null, \"resolved_columns\": {}, \"timeline_hint\": null, \"parameters\": {\"client_name\": \"Garnier\", \"bill_number\": null, \"month\": \"April\", \"year\": null, \"period\": \"month\", \"days\": null, \"specific_date\": null, \"scope\": null}}\n"
            "8. 'What is my total billing for the last quarter?'\n"
            "   -> {\"operation\": \"AGGREGATE_ENTITY\", \"entity\": \"invoice\", \"confidence\": 0.95, \"clarification_question\": null, \"resolved_columns\": {\"filter_by\": \"invoice_date\", \"order_by\": null, \"display\": null}, \"timeline_hint\": null, \"parameters\": {\"client_name\": null, \"bill_number\": null, \"month\": null, \"year\": null, \"period\": \"quarter\", \"days\": null, \"specific_date\": null, \"scope\": null}}\n"
            "9. Context: Assistant asked 'invoices, jobs, or clients for December?'; user replies 'Jobs'\n"
            "   -> {\"operation\": \"AGGREGATE_ENTITY\", \"entity\": \"invoice\", \"confidence\": 0.9, \"clarification_question\": null, \"resolved_columns\": {}, \"timeline_hint\": null, \"parameters\": {\"client_name\": null, \"bill_number\": null, \"month\": \"December\", \"year\": null, \"period\": \"month\", \"days\": null, \"specific_date\": null, \"scope\": null}}\n"
            "10. Context: Assistant said 'Your last gig was on 2026-01-18.'; user asks 'What was it about?' or 'Client?'\n"
            "   -> {\"operation\": \"READ_ENTITY\", \"entity\": \"job\", \"confidence\": 0.9, \"clarification_question\": null, \"resolved_columns\": {\"order_by\": null, \"filter_by\": null, \"display\": \"client,notes\"}, \"timeline_hint\": \"last\", \"parameters\": {\"client_name\": null, \"bill_number\": null, \"month\": null, \"year\": null, \"period\": null, \"days\": null, \"specific_date\": \"2026-01-18\", \"scope\": null}}\n"
            "11. Context: Assistant said 'Here are the client names in my records: 7up, Duracell...'; user asks 'What are the dates on these jobs?' or 'What is the billing amount?' then 'All' / 'All jobs'\n"
            "   -> {\"operation\": \"READ_ENTITY\" or \"AGGREGATE_ENTITY\", \"entity\": \"job\" or \"invoice\", \"confidence\": 0.9, \"clarification_question\": null, \"resolved_columns\": {}, \"timeline_hint\": null, \"parameters\": {\"client_name\": null, \"bill_number\": null, \"month\": null, \"year\": null, \"period\": null, \"days\": null, \"specific_date\": null, \"scope\": \"all\"}}\n\n"
            "RULES: 1) Handle typos. 2) NEVER omit keys. 3) Use null for unknown values. 4) Resolve 'it'/'these'/'all' from conversation when clear. 5) confidence < 0.7 with ambiguity -> NEED_CLARIFICATION. 6) Return ONLY valid JSON."
        )
        try:
            full_prompt = f"{system_prompt}\n\nCurrent user message:\n{message}"
            generation_config = {"responseMimeType": "application/json", "temperature": 0, "maxOutputTokens": 1536}
            raw_text = self._call_api(full_prompt, generation_config=generation_config)
            if not raw_text:
                raise Exception("Empty response from AI API")
            raw_text = raw_text.strip()
            if raw_text.startswith("```"):
                lines = raw_text.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                raw_text = "\n".join(lines)
            logger.info(f"Raw AI Intent Response: {raw_text[:400]}...")
            parsed = json.loads(raw_text)
            # Ensure new fields exist for backward compatibility
            parsed.setdefault("confidence", 1.0)
            parsed.setdefault("clarification_question", None)
            parsed.setdefault("resolved_columns", {})
            parsed.setdefault("timeline_hint", None)
            params = parsed.get("parameters") or {}
            params.setdefault("specific_date", None)
            params.setdefault("scope", None)
            parsed["parameters"] = params
            return parsed
        except Exception as e:
            error_msg = str(e)
            logger.error(f"AI Runtime Error: {error_msg}")
            friendly_error = error_msg
            if "quota" in error_msg.lower() or "429" in error_msg or "resource_exhausted" in error_msg.lower():
                friendly_error = "AI API quota exceeded. Please try again later."
            return {
                "operation": "GEMINI_ERROR",
                "entity": None,
                "parameters": {},
                "error_message": friendly_error,
            }

    def validate_field_value(
        self,
        column_name: str,
        user_input: str,
        column_schema_entry: Optional[Dict] = None,
    ) -> Dict:
        """
        Use the AI model to validate and normalize a single form field value.

        Returns a dict:
        {
            "is_valid": bool,
            "normalized_value": Any,
            "error_message": str | None,
            "clarification_question": str | None,
        }

        On AI errors or if AI is not initialized, it falls back to accepting the
        raw user_input as-is (is_valid=True).
        """
        self._ensure_initialized()
        # Fallback: if AI isn't ready, accept raw value
        if not self._initialized or not self.api_key:
            return {
                "is_valid": True,
                "normalized_value": user_input.strip(),
                "error_message": None,
                "clarification_question": None,
            }

        entry = column_schema_entry or {}
        col_type = str(entry.get("type", "string")).strip().lower() or "string"
        description = str(entry.get("description", "")).strip()

        try:
            schema_json = json.dumps(entry, ensure_ascii=False)
        except (TypeError, ValueError):
            schema_json = "{}"

        prompt = (
            "You are a STRICT field validator for a multi-step form in an operations bot.\n\n"
            "You receive a single column definition and one user-provided value.\n"
            "Your job is to decide if the value is valid for that column, optionally normalize it,\n"
            "and, when invalid, provide a short error message and/or a simple clarification question.\n\n"
            "COLUMN DEFINITION:\n"
            f"- name: {column_name}\n"
            f"- type: {col_type}\n"
            f"- description: {description or '(none)'}\n"
            f"- raw_schema_entry: {schema_json}\n\n"
            f"USER_INPUT: {user_input!r}\n\n"
            "TYPE RULES:\n"
            "- type 'date': accept natural language dates like '5 March 2026', '05/03/26', 'yesterday';\n"
            "  normalize to ISO 'YYYY-MM-DD'. Reject impossible dates.\n"
            "- type 'number': accept integers or decimals; strip currency symbols and commas when obvious\n"
            "  (e.g. '₹2,500' -> 2500). Reject values that clearly are not numeric.\n"
            "- type 'boolean': map 'yes/no', 'y/n', 'true/false', '1/0', 'paid/unpaid', etc. to true/false.\n"
            "- type 'string': accept any non-empty text; trim whitespace; you may lightly normalize spacing/casing.\n\n"
            "OUTPUT REQUIREMENTS:\n"
            "- You MUST return ONLY a single JSON object with EXACTLY these keys:\n"
            "  {\"is_valid\": bool,\n"
            "   \"normalized_value\": any or null,\n"
            "   \"error_message\": string or null,\n"
            "   \"clarification_question\": string or null}\n"
            "- When is_valid is true, error_message and clarification_question should be null.\n"
            "- When is_valid is false, set error_message to a SHORT, user-facing explanation (1 sentence),\n"
            "  and optionally a concise clarification_question (e.g. 'Can you give the date as YYYY-MM-DD?').\n"
            "- Do NOT include any extra keys or text outside the JSON object."
        )

        try:
            generation_config = {
                "responseMimeType": "application/json",
                "temperature": 0,
                "maxOutputTokens": 512,
            }
            raw_text = self._call_api(prompt, generation_config=generation_config)
            if not raw_text:
                raise Exception("Empty response from AI API")
            raw_text = raw_text.strip()
            if raw_text.startswith("```"):
                lines = raw_text.split("\n")
                if lines[0].startswith("```"):
                    lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
                raw_text = "\n".join(lines)
            parsed = json.loads(raw_text)
            # Ensure all keys exist with sane defaults
            parsed.setdefault("is_valid", True)
            parsed.setdefault("normalized_value", user_input.strip())
            parsed.setdefault("error_message", None)
            parsed.setdefault("clarification_question", None)
            return parsed
        except Exception as e:
            logger.warning(f"AI field validation failed for {column_name}: {e}")
            return {
                "is_valid": True,
                "normalized_value": user_input.strip(),
                "error_message": None,
                "clarification_question": None,
            }

    def generate_response(self, user_message: str, backend_result: str) -> str:
        fallback = "I don't see this information in my records yet."
        self._ensure_initialized()
        if not self._initialized or not self.api_key or not backend_result or backend_result == fallback:
            return backend_result or fallback
        prompt = (
            "You are a context-aware business assistant. Phrase a response based ONLY on this result.\n"
            f"Result: {backend_result}\n"
            f"User asked: {user_message}\n"
            "Rules: Be precise and contextual. Align your phrasing with how the user asked. "
            "Avoid vague or template-like responses. NO technical jargon. "
            "If information is missing/error, say: 'I don't see this information in my records yet.'"
        )
        try:
            text = self._call_api(prompt, generation_config={"maxOutputTokens": 500, "temperature": 0.2})
            if not text:
                return backend_result or fallback
            text = text.strip()
            if len(text) < 15:
                return backend_result
            return text
        except Exception as e:
            logger.error(f"Response Generation failed: {e}")
            return backend_result or fallback
