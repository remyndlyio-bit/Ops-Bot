from services.gemini_service import GeminiService
from typing import Dict

class IntentService:
    def __init__(self):
        self.gemini = GeminiService()

    def parse_intent(self, message_body: str) -> Dict:
        """
        Parses intent using Gemini for natural language understanding.
        """
        # Try Gemini first
        ai_result = self.gemini.parse_user_message(message_body)
        if ai_result.get("intent") != "unknown":
            return ai_result

        # Fallback to simple regex/string matches for robustness
        body = message_body.strip().lower()
        
        if "help" in body:
            return {"intent": "help"}
        
        if "status" in body:
            return {"intent": "status"}
        
        return {"intent": "unknown"}

    @staticmethod
    def get_help_text() -> str:
        return (
            "You can talk to me naturally! Try saying:\n"
            "- 'Send me the invoice for Nikkunj for July'\n"
            "- 'Summarize Xiaomi records for April'\n"
            "- 'status' or 'help'"
        )
