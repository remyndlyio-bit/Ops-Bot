import re
from typing import Optional, Tuple, Dict

class IntentService:
    @staticmethod
    def parse_intent(message_body: str) -> Dict:
        """
        Parses the incoming WhatsApp message body to determine user intent.
        
        Supported formats:
        - help
        - status
        - invoice for <client> <month>
        """
        body = message_body.strip().lower()
        
        if body == "help":
            return {"intent": "help"}
        
        if body == "status":
            return {"intent": "status"}
        
        # Regex for 'invoice for <client> <month>'
        # Matches: invoice for nikkunj july
        invoice_match = re.match(r"^invoice for\s+([\w\s]+)\s+(\w+)$", body)
        if invoice_match:
            client_name = invoice_match.group(1).strip()
            month = invoice_match.group(2).strip()
            return {
                "intent": "generate_invoice",
                "client_name": client_name,
                "month": month
            }
        
        return {"intent": "unknown"}

    @staticmethod
    def get_help_text() -> str:
        return (
            "Available Commands:\n"
            "1. *help* - Show this message\n"
            "2. *status* - Check system status\n"
            "3. *invoice for <client> <month>* - Get invoice summary\n"
            "\nExample: *invoice for nikkunj july*"
        )
