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
            return {"operation": "UNKNOWN", "entity": None, "parameters": {}}
        
        system_prompt = (
            "You are a specialized Intent and Parameter Parser. Return ONLY valid JSON.\n"
            "STRICT SCHEMA (MUST RETURN ALL KEYS, NO OMISSIONS):\n"
            "{\n"
            "  \"operation\": \"READ_ENTITY | AGGREGATE_ENTITY | CREATE_ENTITY | UPDATE_ENTITY | ACTION_TRIGGER | SCHEDULE_REMINDER | SMALL_TALK | UNKNOWN\",\n"
            "  \"entity\": \"client | invoice | job | payment | project | bank_details | gst_details | reminder | communication_log | null\",\n"
            "  \"parameters\": {\n"
            "    \"client_name\": string | null,\n"
            "    \"month\": string | null,\n"
            "    \"year\": number | null,\n"
            "    \"period\": \"day | month | quarter | year | null\",\n"
            "    \"days\": number | null\n"
            "  }\n"
            "}\n\n"
            "EXAMPLES:\n"
            "1. 'What is the total biling for April for Garnier?'\n"
            "   -> {\"operation\": \"AGGREGATE_ENTITY\", \"entity\": \"invoice\", \"parameters\": {\"client_name\": \"Garnier\", \"month\": \"April\", \"year\": null, \"period\": \"month\", \"days\": null}}\n"
            "2. 'Remind me to call John tomorrow'\n"
            "   -> {\"operation\": \"SCHEDULE_REMINDER\", \"entity\": \"reminder\", \"parameters\": {\"client_name\": \"John\", \"month\": null, \"year\": null, \"period\": null, \"days\": null}}\n\n"
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
            
            # Use structured messages with roles to separate instructions from user input
            response = self.model.generate_content(
                contents=[
                    {"role": "system", "parts": [system_prompt]},
                    {"role": "user", "parts": [message]}
                ],
                generation_config={
                    "response_mime_type": "application/json",
                    "temperature": 0
                },
                safety_settings=safety_settings
            )
            
            raw_text = response.text.strip()
            # Clean possible markdown code blocks
            if raw_text.startswith("```"):
                raw_text = raw_text.splitlines()
                if raw_text[0].startswith("```"): raw_text = raw_text[1:]
                if raw_text[-1].startswith("```"): raw_text = raw_text[:-1]
                raw_text = "\n".join(raw_text).strip()

            logger.info(f"Raw Gemini Intent Response: {raw_text}")
            return json.loads(raw_text)
        except Exception as e:
            logger.error(f"Intent parsing failed: {e}")
            return {"operation": "UNKNOWN", "entity": None, "parameters": {}}

    def generate_response(self, user_message: str, backend_result: str) -> str:
        """Stage 3: Professional Phrasing."""
        if not self.model: return str(backend_result)
        
        prompt = (
            "You are a professional business assistant. Phrase a response based ONLY on this result.\n"
            f"Result: {backend_result}\n"
            f"User asked: {user_message}\n"
            "Rules: Concise, professional, human-like. NO technical jargon. If information is missing/error, say: 'I don’t see this information in my records yet.'"
        )

        try:
            response = self.model.generate_content(
                prompt,
                generation_config={"max_output_tokens": 150, "temperature": 0.2}
            )
            return response.text.strip()
        except Exception:
            return "I don't see this information in my records yet."

    # Keep compatibility or legacy methods if needed, but the user wants a refactor.
    # The analyze_data and parse_user_message are replaced by this new flow.

