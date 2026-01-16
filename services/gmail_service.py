import os
import json
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from googleapiclient.discovery import build

from utils.logger import logger


class GmailService:
    """
    Gmail API sender using OAuth2 user credentials (Installed App flow).

    Env (required):
    - GOOGLE_OAUTH_CLIENT_JSON  (stringified OAuth client JSON; must include client_id, client_secret, auth_uri, token_uri)
    - GOOGLE_TOKEN  (stringified OAuth token JSON from gmail_token.json)

    Optional:
    - GMAIL_FROM_EMAIL (defaults to the Gmail account associated with the token)
    - GMAIL_FROM_NAME (defaults to "FIRST_USER")
    - REMINDER_BCC (semicolon-separated email addresses to BCC on all reminder emails)
    - EMAIL_DRY_RUN ("true"/"1" to log instead of sending)
    """

    GMAIL_SCOPE = ["https://www.googleapis.com/auth/gmail.send"]

    def __init__(self):
        self.from_email = (os.getenv("GMAIL_FROM_EMAIL") or "").strip()
        self.from_name = (os.getenv("GMAIL_FROM_NAME") or "FIRST_USER").strip()
        self.reminder_bcc = (os.getenv("REMINDER_BCC") or "").strip()
        self.dry_run = (os.getenv("EMAIL_DRY_RUN") or "").strip().lower() in {"1", "true", "yes", "y"}

        self.client_config = self._load_client_config()
        self._service = None

    def _load_client_config(self):
        raw = os.getenv("GOOGLE_OAUTH_CLIENT_JSON")
        if not raw:
            logger.error("[GMAIL] Missing env GOOGLE_OAUTH_CLIENT_JSON")
            return None
        try:
            cfg = json.loads(raw)
        except Exception as e:
            logger.error(f"[GMAIL] Failed to parse GOOGLE_OAUTH_CLIENT_JSON: {e}")
            return None

        # The client config can be under "installed" or "web"
        client = cfg.get("installed") or cfg.get("web")
        required = {"client_id", "client_secret", "auth_uri", "token_uri"}
        if not client or not required.issubset(client.keys()):
            logger.error("[GMAIL] GOOGLE_OAUTH_CLIENT_JSON missing required fields (client_id, client_secret, auth_uri, token_uri)")
            return None

        return {"installed": client} if "installed" in cfg else {"web": client}

    def _get_creds(self) -> Credentials:
        """
        Load creds from GOOGLE_TOKEN env variable and refresh if needed.
        """
        if not self.client_config:
            return None

        token_json = os.getenv("GOOGLE_TOKEN")
        if not token_json:
            logger.error("[GMAIL] Missing env GOOGLE_TOKEN")
            return None

        try:
            token_data = json.loads(token_json)
            creds = Credentials.from_authorized_user_info(token_data, scopes=self.GMAIL_SCOPE)
        except Exception as e:
            logger.error(f"[GMAIL] Failed to parse GOOGLE_TOKEN: {e}")
            return None

        if creds and creds.valid:
            return creds

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                logger.info("[GMAIL] Token refreshed successfully")
                return creds
            except Exception as e:
                logger.error(f"[GMAIL] Token refresh failed: {e}")
                return None

        logger.error("[GMAIL] Token is invalid and cannot be refreshed")
        return None

    def _get_service(self):
        if self._service:
            return self._service

        creds = self._get_creds()
        if not creds:
            return None

        try:
            self._service = build("gmail", "v1", credentials=creds, cache_discovery=False)
            return self._service
        except Exception as e:
            logger.error(f"[GMAIL] Failed to initialize Gmail service: {e}")
            return None

    @staticmethod
    def _encode_message(msg) -> str:
        raw_bytes = msg.as_bytes()
        return base64.urlsafe_b64encode(raw_bytes).decode("utf-8")

    def send_payment_reminder(
        self, 
        to_email: str, 
        client_name: str, 
        invoice_number: str,
        amount_due: str,
        due_date_str: str
    ) -> bool:
        subject = f"Payment Reminder – {invoice_number}"
        body = (
            f"Hi {client_name},\n\n"
            f"This is a friendly reminder that payment for {invoice_number} in the amount of {amount_due} is due on {due_date_str}.\n\n"
            f"Should you have any questions or require additional information, feel free to reach out.\n\n"
            f"Thank you for your cooperation.\n\n"
            f"Best regards,\n{self.from_name}\n"
        )
        return self.send_email(to_email=to_email, subject=subject, body=body, bcc=self.reminder_bcc)

    def send_email(self, to_email: str, subject: str, body: str, bcc: str = None) -> bool:
        if not to_email:
            logger.error("[GMAIL] Missing recipient email")
            return False

        svc = self._get_service()
        if not svc:
            return False

        bcc_list = [email.strip() for email in bcc.split(";")] if bcc else []
        bcc_str = ", ".join(bcc_list) if bcc_list else None

        logger.info(f"[GMAIL] Preparing email -> To={to_email} | BCC={bcc_str} | Subject={subject} | DryRun={self.dry_run}")
        if self.dry_run:
            logger.info(f"[GMAIL] DRY RUN BODY:\n{body}")
            return True

        msg = MIMEMultipart()
        msg["To"] = to_email
        if bcc_str:
            msg["Bcc"] = bcc_str
        from_addr = self.from_email or "me"
        msg["From"] = f"{self.from_name} <{from_addr}>"
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        try:
            raw = self._encode_message(msg)
            svc.users().messages().send(userId="me", body={"raw": raw}).execute()
            logger.info(f"[GMAIL] Sent email -> To={to_email} | BCC={bcc_str}")
            return True
        except Exception as e:
            logger.error(f"[GMAIL] Failed to send email -> To={to_email} | Error={e}")
            return False

