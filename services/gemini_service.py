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

    def synthesize_response(
        self,
        structured_payload: dict,
        user_message: str,
        history_question: bool = False,
        conversation_history: Optional[List[Dict[str, str]]] = None,
    ) -> Optional[str]:
        """
        AI synthesis layer: convert clean structured JSON into natural response.
        Uses ONLY provided data; no hallucination; omits nulls gracefully.
        Pass conversation_history (last few turns) to maintain context across messages.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            return None

        system = (
            "You're a sharp, helpful operations assistant chatting over WhatsApp with someone running their own business.\n"
            "Sound like a smart colleague — warm, concise, natural. Vary your sentence shape. Don't be robotic.\n\n"
            "GROUND RULES (these never bend):\n"
            "- Use ONLY the data provided. Never invent values. If something isn't there, don't mention it.\n"
            "- Plain text only — no markdown (**bold**, _italic_, ## headers), no HTML. If you need bullets, use the • character.\n"
            "- Format dates like '20 Feb 2026' and money like '₹2,000'.\n"
            "- Reply in English even if the user wrote in Hindi / Hinglish / Roman script.\n\n"
            "HOW TO RESPOND:\n"
            "- Match the shape of the answer to the shape of the question. A one-line question gets a one-line answer; a 'show me everything' gets a short list. Don't force a 4-paragraph template on a simple ask.\n"
            "- For 3+ records use bullets; for 1–2 records prose reads better.\n"
            "- A short, specific follow-up question at the end is welcome — only if it genuinely helps the user move forward. Skip if it doesn't.\n"
            "- Never use filler like 'Anything else I can help with?', 'Hope this helps.', 'Here you go!' — get straight to the substance.\n"
            "- A light, well-placed emoji is fine (✅ for payment received, 📌 for a flag) — but sparingly, and never as decoration.\n"
            "- If the data shows something notable (high-value project, overdue invoice, recent record), call it out briefly. Don't celebrate; just observe.\n"
            "- If notes contain change history in the format '[DATE] field: old → new', and the user asks about a previous value, the answer is the OLD value (before the arrow).\n"
        )

        # Optional conversation context — last few turns help maintain thread of conversation
        context_block = ""
        if conversation_history:
            recent_lines = []
            for m in conversation_history[-6:]:
                role = "User" if m.get("role") == "user" else "You"
                content = (m.get("content") or "").strip()
                if content:
                    recent_lines.append(f"{role}: {content[:200]}")
            if recent_lines:
                context_block = (
                    "\n\nRECENT CHAT (for context only — don't repeat yourself):\n"
                    + "\n".join(recent_lines)
                )

        try:
            payload_str = json.dumps(structured_payload, default=str)
        except (TypeError, ValueError):
            payload_str = str(structured_payload)

        history_note = (
            "\n\nNOTE: The user is asking about a PREVIOUS / OLD value. Check the 'notes' field for "
            "change history entries '[DATE] field: old → new'. Answer with the OLD value (before the arrow)."
        ) if history_question else ""

        full_prompt = (
            f"{system}{history_note}{context_block}\n\n"
            f"DATA:\n{payload_str}\n\n"
            f"USER ASKED: {user_message}\n\n"
            "Your reply:"
        )

        try:
            out = self._call_api(
                full_prompt,
                generation_config={"temperature": 0.4, "maxOutputTokens": 350},
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
            "- Plain text only. No markdown (**bold**, *italic*), no HTML. Use '•' for bullet points if needed.\n"
            "- No explanations, no metadata. Output ONLY the reply.\n\n"
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
            '  "operation": "READ_ENTITY | AGGREGATE_ENTITY | CREATE_ENTITY | UPDATE_ENTITY | ACTION_TRIGGER | SCHEDULE_REMINDER | SEND_EMAIL | SMALL_TALK | NEED_CLARIFICATION | UNKNOWN",\n'
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
            "LANGUAGE: Users may write in English, Hindi (Devanagari), Roman Hindi (Hindi in English script), or Hinglish (mixed Hindi-English). "
            "Understand ALL of these. Examples: 'mera last job kya tha' = 'what was my last job', 'kitna paisa aaya' = 'how much payment received', "
            "'invoice bhejo Nike ka' = 'send invoice for Nike', 'पिछले महीने की कमाई' = 'last month earnings'. "
            "Always respond in English but understand input in any of these languages.\n\n"
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

    def decompose_compound_intent(self, message: str) -> Optional[List[str]]:
        """
        Check if a message contains multiple distinct intents.
        Returns a list of individual intent strings if compound, or None if single intent.
        Only called when the message is long enough to plausibly contain multiple intents.
        """
        self._ensure_initialized()
        prompt = f"""Analyze this user message and determine if it contains multiple distinct action requests.

Message: "{message}"

Rules:
- Only split if there are genuinely SEPARATE actions (e.g. "add a job AND send invoice")
- Do NOT split a single action with details (e.g. "add a job for Garnier on 10 Feb" is ONE intent)
- Do NOT split if the second part is just context for the first
- Return the intents in the logical execution order

Return JSON:
- If SINGLE intent: {{"compound": false, "intents": ["{message}"]}}
- If MULTIPLE intents: {{"compound": true, "intents": ["first action", "second action"]}}

Return ONLY valid JSON, nothing else."""

        try:
            raw = self._call_api(prompt, generation_config={
                "temperature": 0.0,
                "maxOutputTokens": 200,
                "responseMimeType": "application/json",
            })
            if not raw:
                return None
            result = json.loads(raw)
            if result.get("compound") and len(result.get("intents", [])) > 1:
                logger.info(f"[AI_COMPOUND] Decomposed into {len(result['intents'])} intents: {result['intents']}")
                return result["intents"]
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"[AI_COMPOUND] Failed to decompose: {e}")
        return None

    def is_send_to_client_intent(self, message: str, last_bot_message: str = "", cached_client: str = "") -> bool:
        """
        Use AI to determine if the user is asking to send/email a previously
        generated invoice to a client. Works with any phrasing — no pattern list.
        """
        self._ensure_initialized()
        client_ctx = f'\nThe cached invoice is for client: "{cached_client}".' if cached_client else ""
        prompt = f"""The bot just generated an invoice and sent it to the user.

Last bot message: "{last_bot_message[:300]}"{client_ctx}
User's reply: "{message}"

Is the user asking to SEND or EMAIL this specific invoice to the client/recipient?
Examples of YES: "send it to the client", "email this to them", "forward to poc", "send to client", "mail it", "share with client"
Examples of NO: "thanks", "show me jobs", "generate another invoice", "what's the total"
Also NO if the user mentions a DIFFERENT client name than the cached one.

Return ONLY JSON: {{"send_to_client": true}} or {{"send_to_client": false}}"""

        try:
            raw = self._call_api(prompt, generation_config={
                "temperature": 0.0,
                "maxOutputTokens": 50,
                "responseMimeType": "application/json",
            })
            if raw:
                result = json.loads(raw)
                return result.get("send_to_client", False)
        except (json.JSONDecodeError, Exception) as e:
            logger.warning(f"[AI_SEND_CHECK] Failed: {e}")
        return False

    def extract_job_fields(self, message: str, today: str = None) -> Optional[Dict]:
        """
        Extract structured job fields from a natural language message.
        Returns a dict with keys: job_date, brand_name, client_name,
        job_description_details, fees, notes. Missing fields are null.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            return None

        if not today:
            from datetime import date
            today = date.today().isoformat()

        prompt = f"""Extract job/project information from the user's message into structured JSON.

Today's date is {today}. Use this to resolve relative dates (e.g. "10 Feb" = {today[:4]}-02-10, "yesterday" = the day before today).

Return ONLY valid JSON with these keys:
- "job_date": ISO date string "YYYY-MM-DD" or null
- "brand_name": The brand or product name (e.g. "Bridgestone", "Xiaomi") or null
- "client_name": The production house, agency, or client entity (e.g. "The Good Take", "Leo Burnett") or null
- "job_description_details": What was done — film type, deliverables, role, duration (e.g. "Master film 30 sec + 4 cutdowns") or null
- "fees": Numeric amount in integer (e.g. "25k" = 25000, "1.5L" = 150000, "2000" = 2000) or null
- "poc_name": The point-of-contact person's name at the client (e.g. "Rohan Mehta") or null
- "poc_email": The point-of-contact email address (e.g. "rohan@studio.com") or null
- "notes": Any additional info that doesn't fit above, or null

Rules:
- If the brand and client are the same entity, put it in brand_name and set client_name to null.
- "k" means thousands (25k = 25000), "L" or "lac" or "lakh" means 100000 (1.5L = 150000). "hazaar" = thousands, "lakh" = 100000.
- If a field is not mentioned, set it to null.
- Do NOT hallucinate or invent data. Only extract what's explicitly stated.
- The user may write in English, Hindi (Devanagari), Roman Hindi, or Hinglish. Understand all. Examples: "Nike ka kaam kiya 10 April ko, 25 hazaar" → brand_name: "Nike", job_date: "2026-04-10", fees: 25000.

User message:
{message}

JSON:"""

        try:
            raw = self._call_api(prompt, generation_config={
                "responseMimeType": "application/json",
                "temperature": 0,
                "maxOutputTokens": 400,
            })
            if not raw:
                return None
            data = json.loads(raw.strip())
            logger.info(f"[SMART_CAPTURE] Extracted fields: {data}")
            return data
        except (json.JSONDecodeError, Exception) as e:
            logger.error(f"[SMART_CAPTURE] Extraction failed: {e}")
            return None

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

    def extract_modify_intent(self, message: str, last_row: Optional[Dict] = None) -> Optional[Dict]:
        """
        Extract update intent from a free-form message like "change Spotify fee to 25000"
        or "mark Garnier as paid". Returns dict with keys:
          - field:  one of [fees, paid, client_name, brand_name, job_description_details,
                            invoice_date, poc_email, poc_name, deadline_date, job_date,
                            production_house]  (or null)
          - value:  the new value (string or number) or null
          - client_filter:  client/brand identifier to locate the row, or null
          - bill_no_filter: bill_no to locate the row, or null
        Returns None on failure.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            return None

        ctx_hint = ""
        if last_row:
            _client = last_row.get("client_name") or last_row.get("brand_name") or ""
            _bill = last_row.get("bill_no") or ""
            ctx_hint = f"\nContext: the user was just shown a job for client='{_client}' bill_no='{_bill}'. If they don't specify a different row, assume they mean this one."

        prompt = f"""Extract an update/modify intent from the user's message into JSON.

Return ONLY valid JSON with these keys:
- "field": one of ["fees","paid","client_name","brand_name","job_description_details","invoice_date","poc_email","poc_name","deadline_date","job_date","production_house"] or null
- "value": the new value (number for fees; "Yes"/"No" for paid; ISO date "YYYY-MM-DD" for date fields; string otherwise) or null
- "client_filter": client/brand name to find the row, or null
- "bill_no_filter": bill number, or null

Rules:
- "fee", "amount", "price" → field "fees"; convert "25k"=25000, "1.5L"=150000
- "paid", "payment status", "mark as paid/unpaid" → field "paid"; value "Yes" or "No"
- "description", "details", "what was done" → field "job_description_details"
- "invoice date", "billing date" → field "invoice_date"
- "email", "contact email" → field "poc_email"
- "contact", "poc", "person" → field "poc_name"
- If the user does NOT name a different client/brand/bill, leave client_filter and bill_no_filter null (use context row).
- If you cannot identify both a field and a value, return all nulls.
{ctx_hint}

User message: "{message}"

Output:"""
        try:
            raw = self._call_api(prompt, generation_config={"maxOutputTokens": 200, "temperature": 0.0})
            if not raw:
                return None
            import re as _re, json as _json
            m = _re.search(r"\{[\s\S]*\}", raw)
            if not m:
                return None
            parsed = _json.loads(m.group(0))
            return parsed if isinstance(parsed, dict) else None
        except Exception as e:
            logger.error(f"extract_modify_intent failed: {e}")
            return None

    def extract_name(self, raw_message: str) -> Optional[str]:
        """
        Extract a person's first name (or full name) from a raw onboarding message.
        Works across languages — English, Hindi, Hinglish, etc.
        Returns the extracted name string, or None if extraction fails.
        """
        prompt = (
            "Extract the person's name from the following message. "
            "The message may be in any language (English, Hindi, Hinglish, etc.). "
            "Return ONLY the name — nothing else, no punctuation, no explanation. "
            "If you cannot identify a name, return the single word: UNKNOWN\n\n"
            f"Message: {raw_message}"
        )
        try:
            result = self._call_api(prompt, generation_config={"maxOutputTokens": 20, "temperature": 0.0})
            if not result:
                return None
            name = result.strip().strip('"').strip("'").strip()
            if not name or name.upper() == "UNKNOWN" or len(name) > 50:
                return None
            return name
        except Exception as e:
            logger.error(f"[GEMINI] extract_name failed: {e}")
            return None

    def is_new_query_not_response(self, message: str, awaiting_context: str) -> bool:
        """
        Detects whether the user is starting a new task vs answering a pending prompt.
        awaiting_context describes what the bot is waiting for (e.g. "yes/no to send invoice",
        "POC email address", "bank details", "month name", "client billing details").
        Returns True if the message looks like a NEW query/command (not a response).
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            return False
        prompt = (
            "The bot is currently waiting for the user to answer this:\n"
            f"  EXPECTED_ANSWER_TYPE: {awaiting_context}\n\n"
            f"USER MESSAGE: {message}\n\n"
            "Is the user's message a NEW question or new command (NOT an answer to the expected prompt)?\n"
            "Reply only YES if it is a new query/command, or NO if it is an answer to the prompt."
        )
        try:
            result = self._call_api(prompt, generation_config={"maxOutputTokens": 5, "temperature": 0.0})
            return bool(result and result.strip().upper().startswith("YES"))
        except Exception as e:
            logger.error(f"[GEMINI] is_new_query_not_response failed: {e}")
            return False

    def is_invoice_action_request(self, message: str) -> bool:
        """
        Classifies whether the user wants to GENERATE/SEND an invoice (an action),
        vs just QUERY data that happens to mention 'invoice' (e.g. 'jobs with invoice_date older than 60 days').
        Returns True only when the user is requesting invoice generation/sending.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            return False
        prompt = (
            "Decide if the user wants to GENERATE or SEND an invoice PDF (an ACTION).\n"
            "Examples that ARE invoice actions:\n"
            "  'send invoice for Nike for March', 'generate invoice', 'create bill for Samsung'\n"
            "Examples that are NOT invoice actions (just queries about invoice data):\n"
            "  'what jobs have invoice_date older than 60 days', 'show invoices that are unpaid',\n"
            "  'list jobs without invoice_date', 'how many invoices last month', 'invoice status for Nike'\n\n"
            f"USER MESSAGE: {message}\n\n"
            "Reply only YES if the user is asking to generate/send/email an invoice, or NO otherwise."
        )
        try:
            result = self._call_api(prompt, generation_config={"maxOutputTokens": 5, "temperature": 0.0})
            return bool(result and result.strip().upper().startswith("YES"))
        except Exception as e:
            logger.error(f"[GEMINI] is_invoice_action_request failed: {e}")
            return False

    def suggest_for_empty_result(self, user_message: str, recent_columns: List[str] = None) -> str:
        """
        Returns a short helpful suggestion when a query returned 0 rows.
        Should briefly say nothing matched and offer 1–2 concrete alternative queries.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            return ""
        cols_hint = ""
        if recent_columns:
            cols_hint = f"\nAvailable filterable fields: {', '.join(recent_columns[:15])}"
        prompt = (
            "The user asked a question against their job/invoice database but the query returned no rows.\n"
            "Write a short, professional reply (max 3 sentences, plain text, no markdown):\n"
            "1. Tell them nothing matched their specific filter.\n"
            "2. Suggest 1 or 2 concrete looser queries they could try (e.g. broaden a date range, drop a filter, check spelling of a client name).\n"
            "Use '•' for bullets if needed. Be confident and helpful, not apologetic.\n"
            f"{cols_hint}\n\n"
            f"USER ASKED: {user_message}\n\n"
            "Your reply:"
        )
        try:
            out = self._call_api(prompt, generation_config={"maxOutputTokens": 120, "temperature": 0.3})
            return (out or "").strip()
        except Exception as e:
            logger.error(f"[GEMINI] suggest_for_empty_result failed: {e}")
            return ""

    def is_history_question(self, message: str) -> bool:
        """
        Returns True if the message is asking about a past/historical value
        (e.g. previous amount, what it was before an update).
        Uses a tiny AI call instead of keyword heuristics.
        """
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            return False
        prompt = (
            "Does the following message ask about a previous, earlier, or historical value "
            "(e.g. what was the old amount, what was it before the update, last fee)? "
            "Answer only YES or NO.\n\n"
            f"Message: {message}"
        )
        try:
            result = self._call_api(prompt, generation_config={"maxOutputTokens": 5, "temperature": 0.0})
            return bool(result and result.strip().upper().startswith("YES"))
        except Exception as e:
            logger.error(f"[GEMINI] is_history_question failed: {e}")
            return False
