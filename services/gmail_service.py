import os
import json
import base64
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.oauth2.credentials import Credentials
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from utils.logger import logger


class GmailService:
    """
    Gmail API sender using OAuth2 user credentials (Installed App flow).

    Env (required):
    - GOOGLE_OAUTH_CLIENT_JSON  (stringified OAuth client JSON; must include client_id, client_secret, auth_uri, token_uri)

    Optional:
    - GMAIL_FROM_EMAIL (defaults to the Gmail account associated with the token)
    - GMAIL_FROM_NAME
    - EMAIL_DRY_RUN ("true"/"1" to log instead of sending)

    Tokens are stored in a local file: gmail_token.json
    """

    GMAIL_SCOPE = ["https://www.googleapis.com/auth/gmail.send"]
    TOKEN_PATH = Path("gmail_token.json")

    def __init__(self):
        self.from_email = (os.getenv("GMAIL_FROM_EMAIL") or "").strip()
        self.from_name = (os.getenv("GMAIL_FROM_NAME") or "Ops Bot").strip()
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
        Load creds from token file if valid; otherwise run local OAuth flow and save.
        """
        if not self.client_config:
            return None

        creds = None
        if self.TOKEN_PATH.exists():
            try:
                creds = Credentials.from_authorized_user_file(str(self.TOKEN_PATH), scopes=self.GMAIL_SCOPE)
            except Exception as e:
                logger.warning(f"[GMAIL] Failed to load existing token file: {e}")
                creds = None

        if creds and creds.valid:
            return creds

        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                self._save_token(creds)
                return creds
            except Exception as e:
                logger.warning(f"[GMAIL] Token refresh failed: {e}")

        # Run OAuth flow
        try:
            flow = InstalledAppFlow.from_client_config(self.client_config, self.GMAIL_SCOPE)
            creds = flow.run_local_server(port=0)
            self._save_token(creds)
            return creds
        except Exception as e:
            logger.error(f"[GMAIL] OAuth flow failed: {e}")
            return None

    def _save_token(self, creds: Credentials):
        try:
            self.TOKEN_PATH.write_text(creds.to_json())
            logger.info(f"[GMAIL] Saved token to {self.TOKEN_PATH}")
        except Exception as e:
            logger.error(f"[GMAIL] Failed to save token: {e}")

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

    def send_payment_reminder(self, to_email: str, client_name: str, due_date_str: str, details: str = "") -> bool:
        subject = f"Payment Reminder - {client_name}"
        body = (
            f"Hi {client_name},\n\n"
            f"This is a friendly reminder that your payment is due on {due_date_str}.\n"
        )
        if details:
            body += f"\nDetails:\n{details}\n"
        body += "\nIf you’ve already paid, please ignore this message.\n\nThanks,\nOperations\n"
        return self.send_email(to_email=to_email, subject=subject, body=body)

    def send_email(self, to_email: str, subject: str, body: str) -> bool:
        if not to_email:
            logger.error("[GMAIL] Missing recipient email")
            return False

        svc = self._get_service()
        if not svc:
            return False

        logger.info(f"[GMAIL] Preparing email -> To={to_email} | Subject={subject} | DryRun={self.dry_run}")
        if self.dry_run:
            logger.info(f"[GMAIL] DRY RUN BODY:\n{body}")
            return True

        msg = MIMEMultipart()
        msg["To"] = to_email
        from_addr = self.from_email or "me"
        msg["From"] = f"{self.from_name} <{from_addr}>"
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        try:
            raw = self._encode_message(msg)
            svc.users().messages().send(userId="me", body={"raw": raw}).execute()
            logger.info(f"[GMAIL] Sent email -> To={to_email}")
            return True
        except Exception as e:
            logger.error(f"[GMAIL] Failed to send email -> To={to_email} | Error={e}")
            return False

