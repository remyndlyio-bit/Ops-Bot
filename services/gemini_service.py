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

    def classify_intent(self, message: str) -> str:
        """Stage 1: Strict Intent Classification. Returns exactly one operation label."""
        if not self.model: 
            return "UNKNOWN"
        
        system_prompt = (
            "Classify the message into EXACTLY ONE of these labels:\n"
            "READ_ENTITY: Request to find or look up specific records/details.\n"
            "AGGREGATE_ENTITY: Questions about totals, counts, billing, or outstanding sums.\n"
            "CREATE_ENTITY: Adding new rows or jobs.\n"
            "UPDATE_ENTITY: Updating existing jobs, payments, or records.\n"
            "ACTION_TRIGGER: Generating invoices or specific system actions.\n"
            "SCHEDULE_REMINDER: Setting dates/times for calls or follow-ups.\n"
            "SMALL_TALK: Greetings, thanks, or general pleasantries.\n"
            "UNKNOWN: If the intent is unclear.\n"
            "Return ONLY the label."
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
            
            label = response.text.strip().upper()
            valid_labels = ["READ_ENTITY", "AGGREGATE_ENTITY", "CREATE_ENTITY", "UPDATE_ENTITY", "ACTION_TRIGGER", "SCHEDULE_REMINDER", "SMALL_TALK", "UNKNOWN"]
            return label if label in valid_labels else "UNKNOWN"
        except Exception as e:
            logger.error(f"Classification failed: {e}")
            return "UNKNOWN"

    def extract_parameters(self, message: str) -> dict:
        """Stage 2: Parameter Extraction. Returns clean JSON."""
        if not self.model: return {}
        
        system_prompt = (
            "Extract parameters from the message as JSON. "
            "Fields: names (list), dates (ISO format if possible), time_ranges, amounts (numbers), invoice_numbers, entities (e.g., 'billing', 'invoice', 'gst').\n"
            "Normalize time phrases:\n"
            "- EOD -> 'end_of_day'\n"
            "- tomorrow -> 'tomorrow'\n"
            "- one week -> '7_days'\n"
            "Return ONLY valid JSON."
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
                generation_config={
                    "response_mime_type": "application/json",
                    "temperature": 0
                },
                safety_settings=safety_settings
            )
            
            return json.loads(response.text.strip())
        except Exception as e:
            logger.error(f"Extraction failed: {e}")
            return {}

    def generate_response(self, user_message: str, backend_result: str) -> str:
        """Stage 3: Professional Phrasing."""
        if not self.model: return "Result: " + str(backend_result)
        
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

