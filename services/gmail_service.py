import os
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from utils.logger import logger


class GmailService:
    """
    Gmail API sender.

    Auth (recommended): Service Account + Domain-Wide Delegation (Google Workspace).
    Env:
    - GOOGLE_CREDS_JSON (same JSON used by SheetsService)
    - GMAIL_DELEGATED_USER: the mailbox to impersonate (e.g. ops@yourdomain.com)

    Message:
    - GMAIL_FROM_NAME (optional)
    - EMAIL_DRY_RUN ("true"/"1" to log instead of sending)

    Note: Regular @gmail.com accounts generally require OAuth user consent flow, not service accounts.
    """

    GMAIL_SCOPE = ["https://www.googleapis.com/auth/gmail.send"]

    def __init__(self):
        self.delegated_user = (os.getenv("GMAIL_DELEGATED_USER") or "").strip()
        self.from_name = (os.getenv("GMAIL_FROM_NAME") or "Ops Bot").strip()
        self.dry_run = (os.getenv("EMAIL_DRY_RUN") or "").strip().lower() in {"1", "true", "yes", "y"}

        self._service = None

    def _get_service(self):
        if self._service:
            return self._service

        creds_raw = os.getenv("GOOGLE_CREDS_JSON")
        if not creds_raw:
            logger.error("[GMAIL] Missing credentials env GOOGLE_CREDS_JSON")
            return None

        try:
            if creds_raw.strip().startswith("{"):
                import json
                creds_dict = json.loads(creds_raw)
                creds = Credentials.from_service_account_info(creds_dict, scopes=self.GMAIL_SCOPE)
            else:
                creds = Credentials.from_service_account_file(creds_raw, scopes=self.GMAIL_SCOPE)

            if self.delegated_user:
                creds = creds.with_subject(self.delegated_user)
            else:
                logger.error("[GMAIL] GMAIL_DELEGATED_USER not set (required for service-account send)")
                return None

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
        msg["From"] = f"{self.from_name} <{self.delegated_user}>"
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

