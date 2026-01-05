from twilio.rest import Client
import os
from utils.logger import logger

class WhatsAppService:
    def __init__(self):
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID")
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN")
        self.from_number = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886")
        
        if self.account_sid and self.auth_token:
            self.client = Client(self.account_sid, self.auth_token)
        else:
            logger.error("Twilio credentials missing.")
            self.client = None

    def send_text_message(self, to_number: str, body: str):
        """
        Sends a plain text message via Twilio WhatsApp.
        """
        if not self.client:
            logger.error("Twilio client not initialized.")
            return None

        try:
            # Ensure the 'to' number has the whatsapp: prefix
            if not to_number.startswith("whatsapp:"):
                to_number = f"whatsapp:{to_number}"

            message = self.client.messages.create(
                from_=self.from_number,
                body=body,
                to=to_number
            )
            logger.info(f"Message sent successfully. SID: {message.sid}")
            return message.sid
        except Exception as e:
            logger.error(f"Failed to send WhatsApp message: {e}")
            return None

    def send_media_message(self, to_number: str, body: str, media_url: str):
        """
        Phase 2: Sends a message with a PDF attachment.
        """
        if not self.client:
            return None

        try:
            if not to_number.startswith("whatsapp:"):
                to_number = f"whatsapp:{to_number}"

            message = self.client.messages.create(
                from_=self.from_number,
                body=body,
                media_url=[media_url],
                to=to_number
            )
            return message.sid
        except Exception as e:
            logger.error(f"Failed to send PDF message: {e}")
            return None
