import os
import httpx
from typing import Optional, List, Dict, Union
from utils.logger import logger


class ResendEmailService:
    """
    Email sender using Resend API.

    Required env vars:
    - RESEND_API: Resend API key (Bearer token)
    - RESEND_FROM_EMAIL: Verified sender email in Resend

    Optional:
    - RESEND_FROM_NAME: Display name for the sender (default: "Ops Bot")
    - EMAIL_DRY_RUN: "true"/"1" to log instead of sending
    - REMINDER_BCC: semicolon-separated BCCs for payment reminders
    """

    RESEND_URL = "https://api.resend.com/emails"

    def __init__(self):
        self.api_key = (os.getenv("RESEND_API") or "").strip()
        self.from_email = (os.getenv("RESEND_FROM_EMAIL") or "").strip()
        self.from_name = (os.getenv("RESEND_FROM_NAME") or "Ops Bot").strip()
        self.reminder_bcc = (os.getenv("REMINDER_BCC") or "").strip()
        self.dry_run = (os.getenv("EMAIL_DRY_RUN") or "").strip().lower() in {"1", "true", "yes", "y"}

        if not self.api_key:
            logger.error("[RESEND] Missing RESEND_API key – emails will not be sent.")
        if not self.from_email:
            logger.error("[RESEND] Missing RESEND_FROM_EMAIL – emails will not be sent.")

    def _normalize_emails(self, email_input: Union[str, List[str]]) -> List[str]:
        """
        Normalize email addresses into a clean list.
        Supports semicolon and comma separators, and handles list inputs.
        """
        if not email_input:
            return []

        if isinstance(email_input, list):
            return [e.strip() for e in email_input if e.strip()]

        if isinstance(email_input, str):
            # Replace semicolons with commas, then split
            emails = email_input.replace(";", ",").split(",")
            return [e.strip() for e in emails if e.strip()]

        return []

    def _build_from_header(self) -> Optional[str]:
        if not self.from_email:
            return None
        return f"{self.from_name} <{self.from_email}>"

    def send_payment_reminder(
        self,
        to_email: str,
        client_name: str,
        invoice_number: str,
        amount_due: str,
        due_date_str: str,
    ) -> bool:
        subject = f"Payment Reminder – {invoice_number}"
        body = (
            f"Hi {client_name},\n\n"
            f"This is a friendly reminder that payment for {invoice_number} in the amount of {amount_due} "
            f"is due on {due_date_str}.\n\n"
            f"If you've already made the payment, you can ignore this message.\n\n"
            f"Thanks,\n{self.from_name}\n"
        )
        return self.send_email(to_email=to_email, subject=subject, body=body, bcc=self.reminder_bcc)

    def send_email(
        self,
        to_email: str,
        subject: str,
        body: str,
        bcc: str = None,
        attachments: Optional[List[Dict[str, str]]] = None,
    ) -> bool:
        if not to_email:
            logger.error("[RESEND] Missing recipient email")
            return False
        if not self.api_key or not self.from_email:
            logger.error("[RESEND] Cannot send email – RESEND_API or RESEND_FROM_EMAIL not configured.")
            return False

        from_header = self._build_from_header()
        if not from_header:
            logger.error("[RESEND] Invalid from header")
            return False

        # Normalize recipient emails
        to_emails = self._normalize_emails(to_email)
        bcc_list = self._normalize_emails(bcc) if bcc else []

        logger.info(
            f"[RESEND] Preparing email -> To={to_emails} | BCC={bcc_list or None} | "
            f"Subject={subject} | Attachments={bool(attachments)} | DryRun={self.dry_run}"
        )

        if not to_emails:
            logger.error("[RESEND] No valid recipient emails after normalization")
            return False

        if self.dry_run:
            logger.info(f"[RESEND] DRY RUN BODY:\n{body}")
            return True

        payload: Dict[str, object] = {
            "from": from_header,
            "to": to_emails,
            "subject": subject,
            "text": body,
        }
        if bcc_list:
            payload["bcc"] = bcc_list
        if attachments:
            payload["attachments"] = attachments

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        try:
            with httpx.Client(timeout=15.0) as client:
                resp = client.post(self.RESEND_URL, json=payload, headers=headers)
            if resp.status_code >= 200 and resp.status_code < 300:
                try:
                    data = resp.json()
                except Exception:
                    data = {}
                message_id = data.get("id") or data.get("message") or "<no-id>"
                logger.info(f"[RESEND] Email sent -> To={to_emails} | BCC={bcc_list or None} | ID={message_id}")
                return True
            else:
                logger.error(
                    f"[RESEND] Failed to send email -> To={to_emails} | "
                    f"Status={resp.status_code} | Body={resp.text[:500]}"
                )
                return False
        except Exception as e:
            logger.error(f"[RESEND] Exception while sending email -> To={to_emails} | Error={e}")
            return False

    def send_invoice_email(
        self,
        to_email: str,
        client_name: str,
        month: str,
        year: Optional[int],
        pdf_path: str,
    ) -> bool:
        """
        Send an invoice email with the PDF attached.
        """
        if not os.path.exists(pdf_path):
            logger.error(f"[RESEND] Invoice PDF not found at path: {pdf_path}")
            return False

        subject = f"Invoice – {client_name} – {month} {year}" if year else f"Invoice – {client_name} – {month}"
        body = (
            f"Hi {client_name},\n\n"
            f"Please find your invoice for {month} {year or ''} attached.\n\n"
            f"If you have any questions about the details or payment, just reply to this email.\n\n"
            f"Thanks,\n{self.from_name}\n"
        )

        try:
            import base64

            with open(pdf_path, "rb") as f:
                content_b64 = base64.b64encode(f.read()).decode("ascii")
        except Exception as e:
            logger.error(f"[RESEND] Failed to read/encode invoice PDF at {pdf_path}: {e}")
            return False

        attachments = [
            {
                "filename": os.path.basename(pdf_path),
                "content": content_b64,
            }
        ]

        return self.send_email(to_email=to_email, subject=subject, body=body, attachments=attachments)

