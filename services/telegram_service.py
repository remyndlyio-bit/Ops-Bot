import os
import httpx
from utils.logger import logger

class TelegramService:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.base_url = f"https://api.telegram.org/bot{self.token}"

    async def send_text_message(self, chat_id: int, text: str):
        url = f"{self.base_url}/sendMessage"
        # Sanitize basic HTML tags if user-generated content is passed
        safe_text = str(text).replace("<", "&lt;").replace(">", "&gt;") if text else ""
        payload = {
            "chat_id": chat_id,
            "text": safe_text,
            "parse_mode": "HTML"
        }
        async with httpx.AsyncClient() as client:
            try:
                logger.info(f"[TELEGRAM] Sending text -> ChatID={chat_id}, Text={safe_text}")
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Failed to send Telegram text: {e}")
                return None

    async def send_document(self, chat_id: int, file_path: str, caption: str = ""):
        url = f"{self.base_url}/sendDocument"
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None

        async with httpx.AsyncClient() as client:
            try:
                with open(file_path, "rb") as f:
                    files = {"document": f}
                    data = {"chat_id": chat_id, "caption": caption}
                    logger.info(f"[TELEGRAM] Sending document -> ChatID={chat_id}, FilePath={file_path}, Caption={caption}")
                    response = await client.post(url, data=data, files=files)
                    response.raise_for_status()
                    return response.json()
            except Exception as e:
                logger.error(f"Failed to send Telegram document: {e}")
                return None
