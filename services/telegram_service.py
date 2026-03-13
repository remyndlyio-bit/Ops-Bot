import os
import json
import httpx
from typing import List, Dict, Optional
from utils.logger import logger

class TelegramService:
    def __init__(self):
        self.token = os.getenv("TELEGRAM_BOT_TOKEN")
        self.base_url = f"https://api.telegram.org/bot{self.token}"

    async def send_chat_action(self, chat_id: int, action: str = "typing"):
        """Send a chat action (e.g. typing). Action lasts ~5 seconds."""
        url = f"{self.base_url}/sendChatAction"
        async with httpx.AsyncClient() as client:
            try:
                await client.post(url, json={"chat_id": chat_id, "action": action})
            except Exception as e:
                logger.debug(f"send_chat_action failed: {e}")

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

    async def send_message_with_buttons(
        self, chat_id: int, text: str, buttons: List[List[Dict[str, str]]]
    ):
        """
        Send a message with an inline keyboard.
        buttons: list of rows, each row is a list of {"text": "...", "callback_data": "..."}.
        """
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "reply_markup": json.dumps({"inline_keyboard": buttons}),
        }
        async with httpx.AsyncClient() as client:
            try:
                response = await client.post(url, json=payload)
                response.raise_for_status()
                return response.json()
            except Exception as e:
                logger.error(f"Failed to send Telegram message with buttons: {e}")
                return None

    async def answer_callback_query(self, callback_query_id: str, text: str = ""):
        """Acknowledge an inline button press."""
        url = f"{self.base_url}/answerCallbackQuery"
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        async with httpx.AsyncClient() as client:
            try:
                await client.post(url, json=payload)
            except Exception as e:
                logger.debug(f"answer_callback_query failed: {e}")

    async def edit_message_text(self, chat_id: int, message_id: int, text: str):
        """Edit an existing message (e.g. to remove buttons after action)."""
        url = f"{self.base_url}/editMessageText"
        payload = {
            "chat_id": chat_id,
            "message_id": message_id,
            "text": text,
            "parse_mode": "Markdown",
        }
        async with httpx.AsyncClient() as client:
            try:
                await client.post(url, json=payload)
            except Exception as e:
                logger.debug(f"edit_message_text failed: {e}")

    # ── Sync helpers (for standalone worker scripts) ──────────────────────

    def send_message_with_buttons_sync(
        self, chat_id: int, text: str, buttons: List[List[Dict[str, str]]]
    ):
        """Synchronous version of send_message_with_buttons for worker scripts."""
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "Markdown",
            "reply_markup": json.dumps({"inline_keyboard": buttons}),
        }
        try:
            with httpx.Client(timeout=15.0) as client:
                response = client.post(url, json=payload)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"[TELEGRAM_SYNC] Failed to send message with buttons: {e}")
            return None

    def send_text_message_sync(self, chat_id: int, text: str):
        """Synchronous version of send_text_message for worker scripts."""
        url = f"{self.base_url}/sendMessage"
        safe_text = str(text).replace("<", "&lt;").replace(">", "&gt;") if text else ""
        payload = {"chat_id": chat_id, "text": safe_text, "parse_mode": "HTML"}
        try:
            with httpx.Client(timeout=15.0) as client:
                response = client.post(url, json=payload)
                response.raise_for_status()
                return response.json()
        except Exception as e:
            logger.error(f"[TELEGRAM_SYNC] Failed to send text: {e}")
            return None
