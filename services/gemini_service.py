import os
import json
import httpx
from typing import List, Dict, Optional
from utils.logger import logger

OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
DEFAULT_MODEL = "google/gemini-2.0-flash-exp"


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

    def parse_user_intent(self, message: str, conversation_history: List[Dict[str, str]] = None) -> dict:
        self._ensure_initialized()
        if not self._initialized or not self.api_key:
            logger.error("AI not initialized.")
            return {
                "operation": "GEMINI_ERROR",
                "entity": None,
                "parameters": {},
                "error_message": "AI not initialized. Set AI_KEY in Railway and redeploy.",
            }
        context_section = ""
        if conversation_history and len(conversation_history) > 0:
            context_lines = ["Recent conversation history:"]
            for msg in conversation_history:
                role_label = "User" if msg.get("role") == "user" else "Assistant"
                context_lines.append(f"{role_label}: {msg.get('content', '')}")
            context_section = "\n".join(context_lines) + "\n\n"
        system_prompt = (
            "You are a specialized Intent and Parameter Parser for an Operations Bot. Return ONLY valid JSON.\n"
            "STRICT SCHEMA (MUST RETURN ALL KEYS, NO OMISSIONS):\n"
            "{\n"
            '  "operation": "READ_ENTITY | AGGREGATE_ENTITY | CREATE_ENTITY | UPDATE_ENTITY | ACTION_TRIGGER | SCHEDULE_REMINDER | SMALL_TALK | UNKNOWN",\n'
            '  "entity": "client | invoice | job | payment | project | bank_details | gst_details | reminder | communication_log | null",\n'
            "  \"parameters\": {\n"
            "    \"client_name\": string | null,\n"
            "    \"bill_number\": string | null,\n"
            "    \"month\": string | null,\n"
            "    \"year\": number | null,\n"
            "    \"period\": \"day | month | quarter | year | null\",\n"
            "    \"days\": number | null\n"
            "  }\n"
            "}\n\n"
            "CONTEXT AWARENESS:\n"
            "• Use the conversation history below to resolve references like 'it', 'that', 'this', 'the same one', 'do it again', etc.\n"
            "• If the current message references something from recent conversation, extract parameters from that context.\n"
            "• If multiple possible references exist, choose the most recent and relevant one.\n"
            "• If the current message is self-contained and doesn't reference prior conversation, process it independently.\n"
            "• DO NOT invent or assume entities, actions, or parameters that aren't in the conversation history or current message.\n"
            "• If context is insufficient to safely resolve references, use null for ambiguous parameters.\n\n"
            f"{context_section}"
            "EXAMPLES:\n"
            "1. 'What is the total billing for April for Garnier?'\n"
            "   -> {\"operation\": \"AGGREGATE_ENTITY\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": \"Garnier\", \"bill_number\": null, \"month\": \"April\", \"year\": null, \"period\": \"month\", \"days\": null}}\n"
            "2. 'Send me invoice #101' or 'Get me invoice #101'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": null, \"bill_number\": \"101\", \"month\": null, \"year\": null, \"period\": null, \"days\": null}}\n"
            "3. 'Get me Garnier invoice for April for 2025'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": \"Garnier\", \"bill_number\": null, \"month\": \"April\", \"year\": 2025, \"period\": \"month\", \"days\": null}}\n"
            "4. 'Download invoice for ClientX in March'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": \"ClientX\", \"bill_number\": null, \"month\": \"March\", \"year\": null, \"period\": \"month\", \"days\": null}}\n"
            "5. 'Can I follow up for a payment' or 'Follow up on payment'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"payment\", \"parameters\": {\"client_name\": null, \"bill_number\": null, \"month\": null, \"year\": null, \"period\": null, \"days\": null}}\n"
            "6. Context: previous 'Get me Garnier invoice for April', current 'Send it again'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": \"Garnier\", \"bill_number\": null, \"month\": \"April\", \"year\": null, \"period\": \"month\", \"days\": null}}\n\n"
            "IMPORTANT: GET/DOWNLOAD/SEND invoice -> operation=ACTION_TRIGGER, entity=invoice.\n"
            "IMPORTANT: FOLLOW UP / payment status -> operation=ACTION_TRIGGER, entity=payment.\n\n"
            "RULES:\n"
            "1. Handle common typos.\n"
            "2. NEVER omit any keys listed in the schema.\n"
            "3. Use null for any values you cannot extract.\n"
            "4. Return ONLY valid JSON."
        )
        try:
            full_prompt = f"{system_prompt}\n\nCurrent user message:\n{message}"
            generation_config = {"responseMimeType": "application/json", "temperature": 0, "maxOutputTokens": 1024}
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
            logger.info(f"Raw AI Intent Response: {raw_text[:300]}...")
            parsed = json.loads(raw_text)
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

    def generate_response(self, user_message: str, backend_result: str) -> str:
        fallback = "I don't see this information in my records yet."
        self._ensure_initialized()
        if not self._initialized or not self.api_key or not backend_result or backend_result == fallback:
            return backend_result or fallback
        prompt = (
            "You are a professional business assistant. Phrase a response based ONLY on this result.\n"
            f"Result: {backend_result}\n"
            f"User asked: {user_message}\n"
            "Rules: Concise, professional, human-like. NO technical jargon. If information is missing/error, say: 'I don't see this information in my records yet.'"
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
