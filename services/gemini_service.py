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
        """
        if not self.model:
            return {"intent": "unknown"}

        system_prompt = """
        You are an AI assistant for a WhatsApp/Telegram Invoice Bot.
        Extract the user's intent and entities.
        
        Intents:
        1. 'generate_invoice': User wants a PDF invoice (needs client_name and month).
        2. 'get_summary': User wants a quick text summary (needs client_name and month).
        3. 'general_query': User is asking a question about the data (e.g., totals, statistics, counts).
        4. 'help': Asking for help.
        5. 'status': Checking system health.

        Return ONLY a JSON object:
        {"intent": "intent_name", "client_name": "extracted_name or null", "month": "extracted_month or null"}
        """

        try:
            response = self.model.generate_content(f"{system_prompt}\nUser Message: {message}")
            text = response.text.replace('```json', '').replace('```', '').strip()
            return json.loads(text)
        except Exception as e:
            logger.error(f"Gemini parsing failed: {e}")
            return {"intent": "unknown"}

    def analyze_data(self, question: str, data: list) -> str:
        """
        Uses Gemini to answer questions about the provided sheet data in a token-efficient way.
        """
        if not self.model:
            return "AI service is currently unavailable."

        if not data:
            return "No data found in the spreadsheet to analyze."

        # Token-saving strategy:
        # 1. Only pick essential columns
        # 2. Use a compact header-less CSV-like format or simplified list
        # 3. Limit to the first 500 rows to prevent extreme token usage (adjustable)
        
        essential_data = []
        # Header for the AI's reference
        header = "Client | Fees | Bill Sent | Paid | Date"
        
        for row in data[:500]: 
            client = row.get("Client Name", row.get("Production house", "N/A"))
            fees = row.get(" Fees ", row.get("Fees", 0))
            bill_sent = row.get("Bill sent", "No")
            paid = row.get("Paid", "No")
            date = row.get("Date ", row.get("Date", ""))
            
            essential_data.append(f"{client} | {fees} | {bill_sent} | {paid} | {date}")

        data_context = "\n".join(essential_data)
        
        prompt = f"""
        You are an expert data analyst for an invoice tracking system.
        Answer the question based on the spreadsheet data provided below.
        
        Data Format: {header}
        
        Data:
        {data_context}
        
        Question: {question}
        
        Instructions:
        - Be precise with numbers.
        - If the user asks for a 'total', sum the Fees.
        - If they ask about 'unbilled' or 'not sent', look at 'Bill Sent'.
        - If they ask about 'unpaid', look at 'Paid'.
        - Provide a concise summary or clear answer.
        """

        try:
            response = self.model.generate_content(prompt)
            return response.text.strip()
        except Exception as e:
            logger.error(f"Gemini analysis failed: {e}")
            return "Sorry, I couldn't analyze the data at this moment."
