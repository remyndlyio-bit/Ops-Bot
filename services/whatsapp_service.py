from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
import os
import requests
from requests.auth import HTTPBasicAuth
from utils.logger import logger

import time

# Twilio error codes that mean "outside the 24h customer-service session
# window — free-form not permitted; only pre-approved templates allowed".
# Surfaced separately from generic errors so cron telemetry is clean.
_OUT_OF_WINDOW_CODES = {63016, 63018}

# Twilio terminal statuses for WhatsApp messages. Once a message hits one of
# these, the status won't change.
_TERMINAL_SUCCESS = {"sent", "delivered", "read"}
_TERMINAL_FAILURE = {"failed", "undelivered"}

# How long we wait for Twilio to update the status from 'queued' to a terminal
# state before giving up. WhatsApp sandbox / Business API typically resolves
# within ~1.5s for sync rejections; we cap at 4s and back off.
_STATUS_POLL_INTERVAL_S = 0.5
_STATUS_POLL_MAX_S = 4.0


def _verify_delivery_status(client, message_sid: str, to_number: str, kind: str) -> bool:
    """Poll Twilio briefly for the message's terminal status.

    Returns True if status is sent/delivered/read (success), False on
    failed/undelivered (async failure — e.g. 63015 session expired, even
    though messages.create() returned 200). When status is still 'queued'
    after the poll budget runs out, treat as success (Twilio will deliver
    later); this trades a small false-positive risk for not blocking the
    worker indefinitely.
    """
    if not message_sid:
        return False
    deadline = time.monotonic() + _STATUS_POLL_MAX_S
    last_status = None
    last_code = None
    while time.monotonic() < deadline:
        try:
            msg = client.messages(message_sid).fetch()
            last_status = (msg.status or "").lower()
            last_code = getattr(msg, "error_code", None)
            if last_status in _TERMINAL_SUCCESS:
                return True
            if last_status in _TERMINAL_FAILURE:
                # Distinguish async window-closed from generic async failure.
                if last_code in _OUT_OF_WINDOW_CODES:
                    logger.warning(
                        f"[WHATSAPP_WINDOW_CLOSED] (async) {kind} To={to_number} "
                        f"SID={message_sid} status={last_status} code={last_code}"
                    )
                else:
                    logger.warning(
                        f"[WHATSAPP_ASYNC_FAILURE] {kind} To={to_number} "
                        f"SID={message_sid} status={last_status} code={last_code} "
                        f"— message rejected after API acceptance."
                    )
                return False
        except Exception as e:
            logger.warning(f"[WHATSAPP] status poll for {message_sid} threw: {e}")
            # Don't block the worker on Twilio API hiccups.
            return True
        time.sleep(_STATUS_POLL_INTERVAL_S)
    # Still queued/accepted after our budget — assume Twilio will deliver.
    logger.info(
        f"[WHATSAPP] status for {message_sid} still {last_status!r} after "
        f"{_STATUS_POLL_MAX_S}s — treating as success."
    )
    return True

class WhatsAppService:
    def __init__(self):
        self.account_sid = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
        self.auth_token = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
        self.from_number = os.getenv("TWILIO_WHATSAPP_NUMBER", "whatsapp:+14155238886").strip()
        
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

            logger.info(f"[WHATSAPP] Sending text -> To={to_number}, Body={body}")
            message = self.client.messages.create(
                from_=self.from_number,
                body=body,
                to=to_number
            )
            logger.info(f"[WHATSAPP] Message accepted by Twilio. SID: {message.sid}")
            # Twilio's WhatsApp delivery can fail asynchronously even after a
            # 200 + SID response (e.g. 63015 'session expired', 63016 caught
            # too late). Poll for terminal status so the worker can react.
            ok = _verify_delivery_status(self.client, message.sid, to_number, "text")
            if not ok:
                return None
            return message.sid
        except TwilioRestException as te:
            if getattr(te, "code", None) in _OUT_OF_WINDOW_CODES:
                # 24h customer-service window closed — only templates allowed
                # outside it. Use a distinct log tag so cron/worker logs are
                # easy to filter and we can measure how often this hits.
                logger.warning(
                    f"[WHATSAPP_WINDOW_CLOSED] To={to_number} code={te.code} "
                    f"msg={te.msg!r} — free-form blocked; user needs to message "
                    f"the bot first to reopen the 24h session."
                )
            else:
                logger.error(
                    f"Failed to send WhatsApp message To={to_number} "
                    f"twilio_code={getattr(te, 'code', None)}: {te.msg if hasattr(te, 'msg') else te}"
                )
            return None
        except Exception as e:
            logger.error(f"Failed to send WhatsApp message: {e}")
            return None

    def send_typing_indicator(self, inbound_message_sid: str):
        """
        Trigger a 'typing…' indicator in the user's WhatsApp client.
        Twilio also marks the inbound message as read. Indicator clears on next
        outbound message or after 25s. Beta endpoint, not in Twilio Python SDK.
        """
        if not (self.account_sid and self.auth_token and inbound_message_sid):
            return
        try:
            requests.post(
                "https://messaging.twilio.com/v2/Indicators/Typing.json",
                auth=HTTPBasicAuth(self.account_sid, self.auth_token),
                data={"messageId": inbound_message_sid, "channel": "whatsapp"},
                timeout=3,
            )
        except Exception as e:
            logger.warning(f"[WHATSAPP] typing indicator failed: {e}")

    def send_media_message(self, to_number: str, body: str, media_url: str):
        """
        Phase 2: Sends a message with a PDF attachment.
        """
        if not self.client:
            return None

        try:
            if not to_number.startswith("whatsapp:"):
                to_number = f"whatsapp:{to_number}"

            logger.info(f"[WHATSAPP] Sending media -> To={to_number}, MediaURL={media_url}, Body={body}")
            message = self.client.messages.create(
                from_=self.from_number,
                body=body,
                media_url=[media_url],
                to=to_number
            )
            logger.info(f"[WHATSAPP] Media message accepted by Twilio. SID: {message.sid}")
            ok = _verify_delivery_status(self.client, message.sid, to_number, "media")
            if not ok:
                return None
            return message.sid
        except TwilioRestException as te:
            if getattr(te, "code", None) in _OUT_OF_WINDOW_CODES:
                logger.warning(
                    f"[WHATSAPP_WINDOW_CLOSED] media To={to_number} code={te.code} "
                    f"msg={te.msg!r} — free-form blocked."
                )
            else:
                logger.error(
                    f"Failed to send PDF message To={to_number} "
                    f"twilio_code={getattr(te, 'code', None)}: {te.msg if hasattr(te, 'msg') else te}"
                )
            return None
        except Exception as e:
            logger.error(f"Failed to send PDF message: {e}")
            return None
