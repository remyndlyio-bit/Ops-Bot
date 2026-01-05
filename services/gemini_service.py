import google.generativeai as genai
import os
import json
from utils.logger import logger

class GeminiService:
    def __init__(self):
        api_key = os.getenv("GEMINI_KEY")
        if api_key:
            genai.configure(api_key=api_key)
            # Use 'gemini-1.5-flash-latest' for better compatibility
            self.model_name = 'gemini-1.5-flash-latest'
            self.model = genai.GenerativeModel(self.model_name)
        else:
            logger.error("GEMINI_KEY not found in environment variables.")
            self.model = None

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
