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
        
        system_prompt = (
            "You are a message router for a business bot that manages Google Sheets and Invoices.\n"
            "Classify the message into one of these categories:\n"
            "1. 'action': If the user wants to add, find, update, delete data, or get statistics/summaries from sheets.\n"
            "2. 'chat': General conversation, greetings, or questions not requiring sheet data.\n"
            "3. 'mixed': Both an action and casual chat.\n"
            "Return ONLY the category name (one word)."
        )
        
        try:
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
            ]
            
            response = self.model.generate_content(
                f"{system_prompt}\n\nMessage: {message}",
                generation_config={"max_output_tokens": 10, "temperature": 0},
                safety_settings=safety_settings
            )
            
            try:
                category = response.text.strip().lower()
            except (ValueError, IndexError, AttributeError):
                logger.warning(f"Router blocked or failed for message: {message}")
                return "chat"

            return category if category in ["action", "chat", "mixed"] else "chat"
        except Exception as e:
            logger.error(f"Router exception: {e}")
            return "chat"

    def parse_action(self, message: str, memory_context: str) -> dict:
        """Stage 2: Action Parser - Extract structured intent."""
        if not self.model: return {}
        system_prompt = (
            "You are an Action Parser for a business bot. Return ONLY JSON.\n"
            "Allowed actions: add_row, find_row, update_row, delete_row, generate_invoice, get_summary, summarize.\n"
            "Rules:\n"
            "- 'get_summary' is for questions about a SPECIFIC client's totals, billing, or records for a specific month (e.g., 'What is the billing for Garnier for April?').\n"
            "- 'summarize' is for general statistics about the sheet (e.g., 'How many clients total?', 'How many rows?').\n"
            "- 'generate_invoice' is for creating PDF invoices.\n"
            "Extract: {action, sheet, data, client_name, month}.\n"
            f"Context: {memory_context}"
        )
        try:
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
            ]
            
            response = self.model.generate_content(
                f"{system_prompt}\n\nMessage: {message}",
                generation_config={"response_mime_type": "application/json", "temperature": 0},
                safety_settings=safety_settings
            )
            
            try:
                return json.loads(response.text.strip())
            except (ValueError, IndexError, AttributeError, json.JSONDecodeError):
                logger.warning(f"Action parser blocked or failed for message: {message}")
                return {}
        except Exception as e:
            logger.error(f"Action parsing failed: {e}")
            return {}


    def generate_response(self, user_message: str, action_result: str, memory_context: str) -> str:
        """Stage 3: Conversational Responder - Human-like reply."""
        if not self.model: return "Sorry, I'm having trouble connecting."
        
        prompt = (
            "You are a helpful business assistant for an Operations Bot. "
            "Your job is to provide a natural, friendly response based on data from a backend system.\n\n"
            f"SYSTEM DATA: {action_result}\n"
            f"USER ASKED: \"{user_message}\"\n\n"
            "INSTRUCTIONS:\n"
            "1. Answer based ONLY on the SYSTEM DATA provided.\n"
            "2. If the data is empty or indicates an error, politely inform the user and suggest what they might try instead.\n"
            "3. Be concise and professional. Do not use technical jargon.\n"
            "4. Never reveal that you are an AI or mention internal identifiers."
        )

        try:
            # Relax safety settings for business bot context
            safety_settings = [
                {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
                {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
            ]

            # Lower temperature for deterministic helpfulness
            response = self.model.generate_content(
                prompt, 
                generation_config={"max_output_tokens": 300, "temperature": 0.2},
                safety_settings=safety_settings
            )
            
            # Safely get text, handling block cases
            try:
                # Some versions of the SDK use 'response.candidates[0].content.parts[0].text'
                # but response.text is usually a safe alias if not blocked.
                text = response.text.strip()
            except (ValueError, IndexError, AttributeError):
                # This happens if Gemini blocks the response (finish_reason=2)
                logger.warning(f"Gemini blocked response for message: {user_message}")
                # Analyze why it might have blocked OR provide a very safe fallback
                if "error" in str(action_result).lower():
                    return "I encountered an issue accessing the sheet. Could you please double-check the sheet name or your request?"
                return "I've processed your request, but I'm having trouble phrasing it. Everything looks okay on my end, though!"
                
            return text if text else "I've handled that for you. Is there anything else?"
        except Exception as e:
            logger.error(f"Responder failed: {e}")
            return "I've processed your request. How else can I help?"

    # Keep compatibility or legacy methods if needed, but the user wants a refactor.
    # The analyze_data and parse_user_message are replaced by this new flow.

