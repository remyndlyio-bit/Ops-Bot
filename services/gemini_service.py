import google.generativeai as genai
import os
import json
from utils.logger import logger

class GeminiService:
    def __init__(self):
        api_key = os.getenv("GEMINI_KEY")
        if api_key:
            genai.configure(api_key=api_key)
            self.model = self._initialize_model()
        else:
            logger.error("GEMINI_KEY not found in environment variables.")
            self.model = None

    def _initialize_model(self):
        # Prefer flash for cost and speed
        models_to_try = ['gemini-2.0-flash', 'gemini-1.5-flash', 'gemini-pro']
        for model_name in models_to_try:
            try:
                model = genai.GenerativeModel(model_name)
                # Verify accessibility
                model.generate_content("hi", generation_config={"max_output_tokens": 1})
                logger.info(f"Using Gemini model: {model_name}")
                return model
            except Exception:
                continue
        return None

    def route_message(self, message: str) -> str:
        """Stage 1: Router - Classify message into action, chat, or mixed."""
        if not self.model: return "chat"
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
            "Allowed actions: add_row, find_row, update_row, delete_row, generate_invoice, get_summary. "
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
            "You are a friendly WhatsApp assistant. "
            f"User Context: {memory_context}\n"
            f"Action Result: {action_result}\n"
            f"User: {user_message}\n"
            "Respond concisely and naturally. Never mention APIs or internal logic."
        )
        try:
            response = self.model.generate_content(prompt, generation_config={"max_output_tokens": 150})
            return response.text.strip()
        except Exception:
            return "I've processed that for you."

    # Keep compatibility or legacy methods if needed, but the user wants a refactor.
    # The analyze_data and parse_user_message are replaced by this new flow.

