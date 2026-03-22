"""
Payment Reminder Worker.

Standalone cron script that:
  1. Scans job_entries for invoices needing reminders (15/30/45 day rules).
  2. Groups by user_id.
  3. Sends notification per user:
     - Telegram: inline buttons
     - WhatsApp: numbered text list + stored pending state for reply handling

Executed via: python workers/reminder_worker.py
Scheduling is handled externally (Railway cron).
"""

import os
import sys
from collections import defaultdict
from datetime import date

# Ensure project root is on path so services/ can be imported
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from dotenv import load_dotenv
load_dotenv()

from utils.logger import logger
from services.supabase_service import SupabaseService
from services.telegram_service import TelegramService
from services.whatsapp_service import WhatsAppService
from utils.pending_reminders import save_pending

# ── Constants ─────────────────────────────────────────────────────────────

FIRST_REMINDER_DAYS = 15
SECOND_REMINDER_DAYS = 30
THIRD_REMINDER_DAYS = 45

REMINDER_QUERY = """
SELECT
    id,
    user_id,
    client_name,
    poc_email,
    poc_name,
    fees,
    bill_no,
    invoice_date,
    first_reminder_sent,
    second_reminder_sent,
    third_reminder_sent
FROM public.job_entries
WHERE
    (paid IS NULL OR TRIM(paid) = '' OR LOWER(paid) IN ('false', 'no', 'unpaid'))
    AND invoice_date IS NOT NULL
    AND (
        -- First reminder: 15+ days, not yet sent
        (
            first_reminder_sent IS NULL
            AND invoice_date <= CURRENT_DATE - INTERVAL '{first_days} days'
        )
        OR
        -- Second reminder: 30+ days, first sent, second not
        (
            first_reminder_sent IS NOT NULL
            AND second_reminder_sent IS NULL
            AND invoice_date <= CURRENT_DATE - INTERVAL '{second_days} days'
        )
        OR
        -- Third reminder: 45+ days, second sent, third not
        (
            second_reminder_sent IS NOT NULL
            AND third_reminder_sent IS NULL
            AND invoice_date <= CURRENT_DATE - INTERVAL '{third_days} days'
        )
    )
ORDER BY user_id, invoice_date ASC
""".format(
    first_days=FIRST_REMINDER_DAYS,
    second_days=SECOND_REMINDER_DAYS,
    third_days=THIRD_REMINDER_DAYS,
)


# ── Helpers ───────────────────────────────────────────────────────────────

def _determine_reminder_level(row: dict) -> str:
    """Return 'first', 'second', or 'third' based on current flag state (timestamptz: NULL = not sent)."""
    if row.get("first_reminder_sent") is None:
        return "first"
    if row.get("second_reminder_sent") is None:
        return "second"
    return "third"


REMINDER_LABELS = {
    "first": "First",
    "second": "Second",
    "third": "Final",
}


def _format_amount(fees) -> str:
    """Format fees as ₹XX,XXX."""
    try:
        return f"₹{int(float(fees)):,}"
    except (ValueError, TypeError):
        return str(fees) if fees else "N/A"


# ── Core Functions ────────────────────────────────────────────────────────

def scan_reminders() -> list:
    """Query Supabase for all invoices that need a reminder today."""
    db = SupabaseService()
    logger.info("[REMINDER_WORKER] Scanning for due reminders...")
    result = db.execute_sql(REMINDER_QUERY)

    if not result.get("ok"):
        logger.error(f"[REMINDER_WORKER] DB query failed: {result.get('error')}")
        return []

    rows = result.get("rows", [])
    logger.info(f"[REMINDER_WORKER] Found {len(rows)} invoice(s) needing reminders.")
    return rows


def group_by_user(rows: list) -> dict:
    """Group reminder rows by user_id."""
    grouped = defaultdict(list)
    for row in rows:
        level = _determine_reminder_level(row)
        row["_reminder_level"] = level
        grouped[row["user_id"]].append(row)
    return dict(grouped)


def _is_telegram_user(user_id: str) -> bool:
    """Telegram user_ids are numeric; WhatsApp ones contain 'whatsapp:' or '+'."""
    try:
        int(user_id)
        return True
    except (ValueError, TypeError):
        return False


def _build_reminder_text(reminders: list) -> str:
    """Build the numbered reminder list text (shared by both platforms)."""
    lines = ["⚠️ Payment Reminders Due Today\n"]
    for idx, row in enumerate(reminders, start=1):
        level = row["_reminder_level"]
        label = REMINDER_LABELS.get(level, level.title())
        client = row.get("client_name") or "Unknown"
        bill = row.get("bill_no") or "N/A"
        amount = _format_amount(row.get("fees"))
        lines.append(
            f"{idx}. Client: {client}\n"
            f"   Invoice: {bill}\n"
            f"   Amount: {amount}\n"
            f"   Reminder: {label}\n"
        )
    return "\n".join(lines)


def notify_user_telegram(user_id: str, reminders: list, telegram: TelegramService):
    """Send Telegram message with inline buttons."""
    chat_id = int(user_id)
    message_text = _build_reminder_text(reminders)

    buttons = []
    for idx, row in enumerate(reminders, start=1):
        job_id = row.get("id")
        level = row["_reminder_level"]
        buttons.append([{
            "text": f"📧 Send Reminder #{idx}",
            "callback_data": f"remind:{job_id}:{level}",
        }])
    buttons.append([{"text": "📧 Send All Reminders", "callback_data": "remind:send:all"}])
    buttons.append([{"text": "⏭ Skip All", "callback_data": "remind:skip:all"}])

    logger.info(f"[REMINDER_WORKER] Notifying Telegram user {user_id} ({len(reminders)} reminder(s))")
    telegram.send_message_with_buttons_sync(chat_id, message_text, buttons)


def notify_user_whatsapp(user_id: str, reminders: list, whatsapp: WhatsAppService):
    """Send WhatsApp text with numbered list and store pending state for reply handling."""
    message_text = _build_reminder_text(reminders)
    message_text += (
        "\nReply with a number (e.g. 1) to send that reminder, "
        "all to send all, or skip to skip all."
    )

    # Store pending reminders so the app can handle the reply
    pending = []
    for row in reminders:
        pending.append({
            "id": row.get("id"),
            "client_name": row.get("client_name"),
            "bill_no": row.get("bill_no"),
            "fees": row.get("fees"),
            "poc_email": row.get("poc_email"),
            "poc_name": row.get("poc_name"),
            "_reminder_level": row["_reminder_level"],
        })
    save_pending(user_id, pending)

    # WhatsApp message text uses plain text (no Markdown bold)
    plain_text = message_text.replace("*", "")
    logger.info(f"[REMINDER_WORKER] Notifying WhatsApp user {user_id} ({len(reminders)} reminder(s))")
    whatsapp.send_text_message(user_id, plain_text)


# ── DB Flag Update ────────────────────────────────────────────────────────

LEVEL_TO_FLAG = {
    "first": "first_reminder_sent",
    "second": "second_reminder_sent",
    "third": "third_reminder_sent",
}


def mark_reminders_sent(db: SupabaseService, reminders: list):
    """Update the DB flag for each reminder that was sent.

    Called immediately after successful notification to ensure idempotency
    (no duplicate sends on retry).  Uses independent if-blocks so all
    pending levels for old jobs are handled in a single pass.
    """
    for row in reminders:
        job_id = row.get("id")
        level = row.get("_reminder_level")
        flag_col = LEVEL_TO_FLAG.get(level)
        if not job_id or not flag_col:
            continue
        try:
            db.execute_sql(
                f"UPDATE public.job_entries SET {flag_col} = NOW() WHERE id = '{job_id}'"
            )
            logger.info(f"[REMINDER_WORKER] Marked {flag_col} for job {job_id}")
        except Exception as e:
            logger.error(f"[REMINDER_WORKER] Failed to mark {flag_col} for job {job_id}: {e}")


# ── Entry Point ───────────────────────────────────────────────────────────

def run():
    """Main entry point for the reminder worker."""
    logger.info("[REMINDER_WORKER] === Starting reminder scan ===")

    db = SupabaseService()

    rows = scan_reminders()
    if not rows:
        logger.info("[REMINDER_WORKER] No reminders due. Exiting.")
        return

    grouped = group_by_user(rows)
    logger.info(f"[REMINDER_WORKER] Reminders grouped for {len(grouped)} user(s).")

    telegram = TelegramService()
    whatsapp = WhatsAppService()

    total_sent = 0
    total_failed = 0

    for user_id, reminders in grouped.items():
        try:
            if _is_telegram_user(user_id):
                notify_user_telegram(user_id, reminders, telegram)
            else:
                notify_user_whatsapp(user_id, reminders, whatsapp)

            # Mark DB flags immediately after successful notification
            mark_reminders_sent(db, reminders)
            total_sent += len(reminders)
            logger.info(f"[REMINDER_WORKER] Sent {len(reminders)} reminder(s) for user {user_id}")
        except Exception as e:
            total_failed += len(reminders)
            logger.error(f"[REMINDER_WORKER] Failed to notify user {user_id}: {e}")

    logger.info(f"[REMINDER_WORKER] === Scan complete: {total_sent} sent, {total_failed} failed ===")


if __name__ == "__main__":
    run()
