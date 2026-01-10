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

    def route_message(self, message: str) -> str:
        """Stage 1: Router - Classify message into action, chat, or mixed."""
        if not self.model: 
            logger.error("Router failed: No Gemini model initialized.")
            return "chat"
        prompt = f"Classify this WhatsApp message as 'action', 'chat', or 'mixed'. Return ONLY one word.\n\nMessage: {message}"
        try:
            response = self.model.generate_content(prompt, generation_config={"max_output_tokens": 5})
            category = response.text.strip().lower()
            return category if category in ["action", "chat", "mixed"] else "chat"
        except Exception:
            return "chat"

    def parse_action(self, message: str, memory_context: str) -> dict:
        """Stage 2: Action Parser - Extract structured intent."""
        if not self.model: return {}
        system_prompt = (
            "You are an Action Parser. Return ONLY JSON. "
            "Allowed actions: add_row, find_row, update_row, delete_row, generate_invoice, get_summary, summarize. "
            "Extract: {action, sheet, data, client_name, month}. "
            f"Context: {memory_context}"
        )
        try:
            response = self.model.generate_content(
                f"{system_prompt}\n\nMessage: {message}",
                generation_config={"response_mime_type": "application/json"}
            )
            return json.loads(response.text.strip())
        except Exception as e:
            logger.error(f"Action parsing failed: {e}")
            return {}


    def generate_response(self, user_message: str, action_result: str, memory_context: str) -> str:
        """Stage 3: Conversational Responder - Human-like reply."""
        if not self.model: return "Sorry, I'm having trouble connecting."
        prompt = (
            "You are a helpful, senior backend-assistant for a WhatsApp bot. "
            f"User Context: {memory_context}\n"
            f"Fact-based Backend Result: {action_result}\n"
            f"User Message: {user_message}\n"
            "INSTRUCTIONS:\n"
            "1. Answer the user based ONLY on the Backend Result.\n"
            "2. Be concise, friendly, and professional.\n"
            "3. If the Backend Result says rows were found, mention it naturally.\n"
            "4. NEVER mention APIs, models, or internal logic."
        )
        try:
            # Lower temperature for deterministic helpfulness
            response = self.model.generate_content(
                prompt, 
                generation_config={"max_output_tokens": 200, "temperature": 0.2}
            )
            text = response.text.strip()
            return text if text else "I've handled that for you. Is there anything else?"
        except Exception as e:
            logger.error(f"Responder failed: {e}")
            return "I've processed your request. How else can I help?"

    # Keep compatibility or legacy methods if needed, but the user wants a refactor.
    # The analyze_data and parse_user_message are replaced by this new flow.

