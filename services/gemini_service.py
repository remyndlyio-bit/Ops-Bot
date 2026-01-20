import google.generativeai as genai
import os
import json
from typing import List, Dict
from utils.logger import logger

class GeminiService:
    def __init__(self):
        api_key = os.getenv("GEMINI_KEY")
        if not api_key:
            # Try a common alternative name
            api_key = os.getenv("GOOGLE_API_KEY")
        
        if api_key:
            # Clean possible whitespace
            api_key = api_key.strip()
            genai.configure(api_key=api_key)
            self.model = self._initialize_model()
            if not self.model:
                logger.error("Gemini model initialization failed after trying multiple models.")
        else:
            logger.error("No Gemini API key found. Checked GEMINI_KEY and GOOGLE_API_KEY.")
            self.model = None

    def _initialize_model(self):
        # Prefer flash for cost and speed
        models_to_try = [
            'gemini-2.0-flash', 
            'gemini-1.5-flash', 
            'gemini-flash-latest',
            'gemini-pro'
        ]
        errors = []
        for model_name in models_to_try:
            try:
                model = genai.GenerativeModel(model_name)
                # Verify accessibility
                model.generate_content("hi", generation_config={"max_output_tokens": 1})
                logger.info(f"Verified Gemini model: {model_name}")
                return model
            except Exception as e:
                errors.append(f"{model_name}: {str(e)}")
                continue
        
        try:
            available = [m.name for m in genai.list_models()]
            logger.error(f"Models failed. Available for this key: {available}")
        except Exception as e:
            logger.error(f"Could not list models: {e}")

        logger.error(f"Failed all models. Errors: {errors}")
        return None

    def parse_user_intent(self, message: str, conversation_history: List[Dict[str, str]] = None) -> dict:
        """
        Single constrained call for Intent and Parameter parsing.
        conversation_history: List of previous messages with 'role' (user/assistant) and 'content'.
        """
        if not self.model: 
            logger.error("Gemini model not initialized.")
            return {
                "operation": "GEMINI_ERROR",
                "entity": None,
                "parameters": {},
                "error_message": "Gemini model not initialized (check API key or quota)"
            }
        
        # Build context from conversation history if available
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
            "  \"operation\": \"READ_ENTITY | AGGREGATE_ENTITY | CREATE_ENTITY | UPDATE_ENTITY | ACTION_TRIGGER | SCHEDULE_REMINDER | SMALL_TALK | UNKNOWN\",\n"
            "  \"entity\": \"client | invoice | job | payment | project | bank_details | gst_details | reminder | communication_log | null\",\n"
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
            "3. 'Get me Garnier invoice for April for 2025' or 'Can you get me Garnier invoice for April for 2025'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": \"Garnier\", \"bill_number\": null, \"month\": \"April\", \"year\": 2025, \"period\": \"month\", \"days\": null}}\n"
            "4. 'Download invoice for ClientX in March'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": \"ClientX\", \"bill_number\": null, \"month\": \"March\", \"year\": null, \"period\": \"month\", \"days\": null}}\n"
            "5. Context example: If previous message was 'Get me Garnier invoice for April' and current message is 'Send it again'\n"
            "   -> {\"operation\": \"ACTION_TRIGGER\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": \"Garnier\", \"bill_number\": null, \"month\": \"April\", \"year\": null, \"period\": \"month\", \"days\": null}}\n\n"
            "IMPORTANT: Any request to GET, DOWNLOAD, SEND, RETRIEVE, or FETCH an invoice should use operation=\"ACTION_TRIGGER\" and entity=\"invoice\".\n\n"
            "RULES:\n"
            "1. Handle common typos.\n"
            "2. NEVER omit any keys listed in the schema.\n"
            "3. Use null for any values you cannot extract.\n"
            "4. Return ONLY valid JSON."
        )
        
        try:
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
            ]
            
            response = self.model.generate_content(
                contents=[
                    {
                        "role": "user",
                        "parts": [
                            f"{system_prompt}\n\nCurrent user message:\n{message}"
                        ]
                    }
                ],
                generation_config={
                    "response_mime_type": "application/json",
                    "temperature": 0
                },
                safety_settings=safety_settings
            )
            
            raw_text = response.text.strip()
            logger.info(f"Raw Gemini Intent Response: {raw_text}")

            parsed = json.loads(raw_text)
            return parsed

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Gemini Runtime Error: {error_msg}")
            
            friendly_error = error_msg
            if "quota" in error_msg.lower() or "429" in error_msg or "resource_exhausted" in error_msg.lower():
                friendly_error = "Gemini API quota exceeded for the day. Please try again later or check your billing."

            return {
                "operation": "GEMINI_ERROR",
                "entity": None,
                "parameters": {},
                "error_message": friendly_error
            }

    def generate_response(self, user_message: str, backend_result: str) -> str:
        """Stage 3: Professional Phrasing with safety guards."""
        fallback = "I don't see this information in my records yet."
        
        if not self.model or not backend_result or backend_result == fallback:
            return backend_result or fallback
        
        prompt = (
            "You are a professional business assistant. Phrase a response based ONLY on this result.\n"
            f"Result: {backend_result}\n"
            f"User asked: {user_message}\n"
            "Rules: Concise, professional, human-like. NO technical jargon. If information is missing/error, say: 'I don't see this information in my records yet.'"
        )

        try:
            response = self.model.generate_content(
                prompt,
                generation_config={"max_output_tokens": 500, "temperature": 0.2}
            )
            
            text = response.text.strip()
            if len(text) < 15:
                return backend_result
            return text
        except Exception as e:
            logger.error(f"Response Generation failed (Stage 3): {e}")
            return backend_result or fallback
