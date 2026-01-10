import google.generativeai as genai
import os
import json
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

    def parse_user_intent(self, message: str) -> dict:
        """Single constrained call for Intent and Parameter parsing."""
        if not self.model: 
            logger.error("Gemini model not initialized.")
            return {
                "operation": "GEMINI_ERROR",
                "entity": None,
                "parameters": {},
                "error_message": "Gemini model not initialized (check API key or quota)"
            }
        
        system_prompt = (
            "You are a specialized Intent and Parameter Parser. Return ONLY valid JSON.\n"
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
            "EXAMPLES:\n"
            "1. 'What is the total biling for April for Garnier?'\n"
            "   -> {\"operation\": \"AGGREGATE_ENTITY\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": \"Garnier\", \"bill_number\": null, \"month\": \"April\", \"year\": null, \"period\": \"month\", \"days\": null}}\n"
            "2. 'Send me invoice #101'\n"
            "   -> {\"operation\": \"READ_ENTITY\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": null, \"bill_number\": \"101\", \"month\": null, \"year\": null, \"period\": null, \"days\": null}}\n\n"
            "RULES:\n"
            "1. Handle common typos (e.g., 'biling' -> billing).\n"
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
            
            # Combine instructions and user input into a single 'user' block for API compatibility
            response = self.model.generate_content(
                contents=[
                    {
                        "role": "user",
                        "parts": [
                            f"{system_prompt}\n\nUser message:\n{message}"
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

            # Clean possible markdown code blocks
            if raw_text.startswith("```"):
                lines = raw_text.splitlines()
                if lines[0].startswith("```"): lines = lines[1:]
                if lines and lines[-1].startswith("```"): lines = lines[:-1]
                raw_text = "\n".join(lines).strip()

            try:
                parsed = json.loads(raw_text)
                return parsed
            except json.JSONDecodeError as je:
                logger.error(f"JSON Parsing failed: {je} | Raw: {raw_text}")
                return {
                    "operation": "GEMINI_ERROR",
                    "entity": None,
                    "parameters": {},
                    "error_message": f"Failed to parse Gemini JSON response: {str(je)}"
                }

        except Exception as e:
            error_msg = str(e)
            logger.error(f"Gemini Runtime Error: {error_msg}")
            return {
                "operation": "GEMINI_ERROR",
                "entity": None,
                "parameters": {},
                "error_message": error_msg
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
            # Generate content synchronously (waits for full completion)
            response = self.model.generate_content(
                prompt,
                generation_config={"max_output_tokens": 150, "temperature": 0.2}
            )
            
            # Ensure generate_content fully completed by checking response.text
            text = response.text.strip()
            length = len(text)
            
            logger.info(f"LLM Response (len={length}): {text}")

            # Minimum length guard (20 chars)
            if length < 20:
                logger.warning(f"LLM response discarded (too short: {length} chars). Using fallback.")
                return backend_result

            return text
        except Exception as e:
            logger.error(f"Response Generation failed (Stage 3): {e}")
            return backend_result or fallback

    # Keep compatibility or legacy methods if needed, but the user wants a refactor.
    # The analyze_data and parse_user_message are replaced by this new flow.

