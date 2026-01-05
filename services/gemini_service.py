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
        # List of models to try in order of preference
        models_to_try = [
            'gemini-2.0-flash',
            'gemini-1.5-flash',
            'gemini-flash-latest',
            'gemini-2.0-flash-lite',
            'gemini-pro'
        ]
        
        for model_name in models_to_try:
            try:
                model = genai.GenerativeModel(model_name)
                # Test the model with a tiny prompt to verify it exists and is accessible
                model.generate_content("test", generation_config={"max_output_tokens": 1})
                logger.info(f"Successfully initialized Gemini with model: {model_name}")
                return model
            except Exception as e:
                logger.warning(f"Failed to initialize model '{model_name}': {e}")
                continue
        
        # If all fail, try to list models for debugging
        try:
            available_models = [m.name for m in genai.list_models()]
            logger.error(f"All preferred models failed. Available models for this key: {available_models}")
        except Exception as list_err:
            logger.error(f"Could not list models: {list_err}")
            
        return None

    def parse_user_message(self, message: str) -> dict:
        """
        Uses Gemini to understand user intent and extract entities.
        Returns a dictionary with 'intent', 'client_name', and 'month'.
        """
        if not self.model:
            return {"intent": "unknown"}

        system_prompt = """
        You are an AI assistant for a WhatsApp Invoice Bot. Your job is to extract the user's intent from their message.
        Possible intents:
        1. 'generate_invoice': User wants to create/get a PDF invoice.
        2. 'get_summary': User wants a text summary of a client's status or month.
        3. 'help': User is asking for help or what the bot can do.
        4. 'status': User is checking system health.

        Extract 'client_name' and 'month' if applicable.
        Return ONLY a JSON object in this format:
        {"intent": "intent_name", "client_name": "extracted_name", "month": "extracted_month"}
        If a field is not found, use null.
        Example: "can you send me the invoice for Xiaomi for May?"
        Output: {"intent": "generate_invoice", "client_name": "Xiaomi", "month": "May"}
        """

        try:
            response = self.model.generate_content(f"{system_prompt}\nUser Message: {message}")
            # Clean response text in case Gemini adds markdown blocks
            text = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(text)
        except Exception as e:
            logger.error(f"Gemini parsing failed: {e}")
            return {"intent": "unknown"}
