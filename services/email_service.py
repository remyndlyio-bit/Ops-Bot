import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from utils.logger import logger


class EmailService:
    """
    Simple SMTP email sender.

    Required env vars:
    - SMTP_HOST
    - SMTP_PORT (default 587)
    - SMTP_USER (optional if server allows unauthenticated)
    - SMTP_PASS (optional if server allows unauthenticated)
    - SMTP_FROM_EMAIL (fallbacks to SMTP_USER)
    - SMTP_FROM_NAME (optional)
    - EMAIL_DRY_RUN ("true"/"1" to log instead of sending)
    """

    def __init__(self):
        self.host = (os.getenv("SMTP_HOST") or "").strip()
        self.port = int((os.getenv("SMTP_PORT") or "587").strip())
        self.user = (os.getenv("SMTP_USER") or "").strip()
        self.password = (os.getenv("SMTP_PASS") or "").strip()
        self.from_email = (os.getenv("SMTP_FROM_EMAIL") or self.user).strip()
        self.from_name = (os.getenv("SMTP_FROM_NAME") or "Ops Bot").strip()
        self.dry_run = (os.getenv("EMAIL_DRY_RUN") or "").strip().lower() in {"1", "true", "yes", "y"}

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
            logger.error("[EMAIL] Missing recipient email")
            return False
        if not self.host:
            logger.error("[EMAIL] SMTP_HOST not set")
            return False
        if not self.from_email:
            logger.error("[EMAIL] SMTP_FROM_EMAIL/SMTP_USER not set")
            return False

        logger.info(f"[EMAIL] Preparing email -> To={to_email} | Subject={subject} | DryRun={self.dry_run}")

        if self.dry_run:
            logger.info(f"[EMAIL] DRY RUN BODY:\n{body}")
            return True

        msg = MIMEMultipart()
        msg["From"] = f"{self.from_name} <{self.from_email}>"
        msg["To"] = to_email
        msg["Subject"] = subject
        msg.attach(MIMEText(body, "plain", "utf-8"))

        try:
            with smtplib.SMTP(self.host, self.port, timeout=20) as server:
                server.ehlo()
                # STARTTLS if supported/standard port
                try:
                    server.starttls()
                    server.ehlo()
                except Exception as e:
                    logger.warning(f"[EMAIL] STARTTLS not used/failed: {e}")

                if self.user and self.password:
                    server.login(self.user, self.password)

                server.sendmail(self.from_email, [to_email], msg.as_string())
            logger.info(f"[EMAIL] Sent email -> To={to_email}")
            return True
        except Exception as e:
            logger.error(f"[EMAIL] Failed to send email -> To={to_email} | Error={e}")
            return False

