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

# Daily overdue-audit: ping the OWNER (not the client) when an invoice has
# been outstanding 60+ days, in case it was paid offline and they forgot to
# update the bot. Re-ping every OVERDUE_AUDIT_RENAG_DAYS until resolved.
OVERDUE_AUDIT_THRESHOLD_DAYS = 60
OVERDUE_AUDIT_RENAG_DAYS = 7

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


# ── Overdue audit (>60 day owner-side check) ──────────────────────────────

OVERDUE_AUDIT_QUERY = """
SELECT
    id, user_id, client_name, poc_name, fees, bill_no, invoice_date,
    overdue_audit_sent
FROM public.job_entries
WHERE
    (paid IS NULL OR TRIM(paid) = '' OR LOWER(paid) IN ('false', 'no', 'unpaid'))
    AND ("isDeleted" IS NOT TRUE)
    AND invoice_date IS NOT NULL
    AND invoice_date <= CURRENT_DATE - INTERVAL '{threshold_days} days'
    AND (
        overdue_audit_sent IS NULL
        OR overdue_audit_sent <= NOW() - INTERVAL '{renag_days} days'
    )
ORDER BY user_id, invoice_date ASC
""".format(threshold_days=OVERDUE_AUDIT_THRESHOLD_DAYS, renag_days=OVERDUE_AUDIT_RENAG_DAYS)


def _days_since(d) -> int:
    """How many days ago was invoice_date (date or ISO string)?"""
    try:
        if isinstance(d, str):
            from datetime import datetime
            d = datetime.fromisoformat(d[:10]).date()
        return (date.today() - d).days
    except Exception:
        return 0


def scan_overdue_audits() -> list:
    """Query Supabase for unpaid invoices that are 60+ days old and need an owner audit ping."""
    db = SupabaseService()
    logger.info(f"[AUDIT_WORKER] Scanning for unpaid invoices >{OVERDUE_AUDIT_THRESHOLD_DAYS} days old…")
    result = db.execute_sql(OVERDUE_AUDIT_QUERY)
    if not result.get("ok"):
        logger.error(f"[AUDIT_WORKER] DB query failed: {result.get('error')}")
        return []
    rows = result.get("rows", [])
    logger.info(f"[AUDIT_WORKER] Found {len(rows)} invoice(s) needing audit.")
    return rows


def _build_audit_text(audits: list) -> str:
    """Build the owner-facing audit text."""
    lines = ["📌 Overdue Invoice Check\n"]
    lines.append(
        f"These invoices are {OVERDUE_AUDIT_THRESHOLD_DAYS}+ days old "
        "and still marked unpaid. Any of them actually paid?\n"
    )
    for idx, row in enumerate(audits, start=1):
        client = row.get("client_name") or "Unknown"
        bill = row.get("bill_no") or "N/A"
        amount = _format_amount(row.get("fees"))
        days_old = _days_since(row.get("invoice_date"))
        lines.append(
            f"{idx}. {client} — {bill}\n"
            f"   {amount} — invoiced {days_old} days ago"
        )
    return "\n".join(lines)


def notify_audit_telegram(user_id: str, audits: list, telegram: TelegramService):
    """Telegram message with per-job 'Mark Paid' / 'Remind Later' buttons."""
    chat_id = int(user_id)
    message_text = _build_audit_text(audits)

    buttons = []
    for idx, row in enumerate(audits, start=1):
        job_id = row.get("id")
        # Compact label so two buttons fit on one Telegram row
        label = f"#{idx} {(row.get('client_name') or '')[:14]}".strip()
        buttons.append([
            {"text": f"✅ Paid · {label}", "callback_data": f"audit:paid:{job_id}"},
            {"text": f"⏸ Later · {label}", "callback_data": f"audit:later:{job_id}"},
        ])
    buttons.append([{"text": "✅ Mark All Paid",     "callback_data": "audit:paid:all"}])
    buttons.append([{"text": "⏸ Remind Me Next Week", "callback_data": "audit:later:all"}])

    logger.info(f"[AUDIT_WORKER] Notifying Telegram user {user_id} ({len(audits)} audit row(s))")
    telegram.send_message_with_buttons_sync(chat_id, message_text, buttons)


def notify_audit_whatsapp(user_id: str, audits: list, whatsapp: WhatsAppService):
    """WhatsApp text + pending state. User replies e.g. 'paid 1' or 'all paid'."""
    message_text = _build_audit_text(audits)
    message_text += (
        "\n\nReply:\n"
        "• paid <number>  — mark one as paid (e.g. 'paid 1')\n"
        "• all paid       — mark all of these as paid\n"
        "• later          — remind me next week"
    )
    pending = []
    for row in audits:
        pending.append({
            "id": row.get("id"),
            "client_name": row.get("client_name"),
            "bill_no": row.get("bill_no"),
            "fees": row.get("fees"),
            "_audit_row": True,
        })
    # Reuse the same pending_reminders store; reply handler will branch on _audit_row.
    save_pending(user_id, pending)
    logger.info(f"[AUDIT_WORKER] Notifying WhatsApp user {user_id} ({len(audits)} audit row(s))")
    whatsapp.send_text_message(user_id, message_text)


def mark_audits_pinged(db: SupabaseService, audits: list):
    """Stamp overdue_audit_sent = NOW() so we don't re-nag for RENAG_DAYS."""
    for row in audits:
        job_id = row.get("id")
        if not job_id:
            continue
        try:
            db.execute_sql(
                f"UPDATE public.job_entries SET overdue_audit_sent = NOW() WHERE id = '{job_id}'"
            )
        except Exception as e:
            logger.error(f"[AUDIT_WORKER] Failed to stamp overdue_audit_sent for job {job_id}: {e}")


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
    telegram = TelegramService()
    whatsapp = WhatsAppService()

    # ── Phase 1: client-facing payment reminders (15/30/45-day cadence) ──
    rows = scan_reminders()
    if rows:
        grouped = group_by_user(rows)
        logger.info(f"[REMINDER_WORKER] Reminders grouped for {len(grouped)} user(s).")
        total_sent = 0
        total_failed = 0
        for user_id, reminders in grouped.items():
            try:
                if _is_telegram_user(user_id):
                    notify_user_telegram(user_id, reminders, telegram)
                else:
                    notify_user_whatsapp(user_id, reminders, whatsapp)
                mark_reminders_sent(db, reminders)
                total_sent += len(reminders)
                logger.info(f"[REMINDER_WORKER] Sent {len(reminders)} reminder(s) for user {user_id}")
            except Exception as e:
                total_failed += len(reminders)
                logger.error(f"[REMINDER_WORKER] Failed to notify user {user_id}: {e}")
        logger.info(f"[REMINDER_WORKER] Phase 1 complete: {total_sent} sent, {total_failed} failed")
    else:
        logger.info("[REMINDER_WORKER] No client-facing reminders due.")

    # ── Phase 2: owner-side overdue audit (>60 days, weekly re-nag) ──────
    logger.info("[AUDIT_WORKER] === Starting overdue audit ===")
    audits = scan_overdue_audits()
    if audits:
        audit_grouped = defaultdict(list)
        for row in audits:
            audit_grouped[row["user_id"]].append(row)
        logger.info(f"[AUDIT_WORKER] Audit groups: {len(audit_grouped)} user(s).")
        audit_sent = 0
        audit_failed = 0
        for user_id, user_audits in audit_grouped.items():
            try:
                if _is_telegram_user(user_id):
                    notify_audit_telegram(user_id, user_audits, telegram)
                else:
                    notify_audit_whatsapp(user_id, user_audits, whatsapp)
                mark_audits_pinged(db, user_audits)
                audit_sent += len(user_audits)
                logger.info(f"[AUDIT_WORKER] Sent audit ping for {len(user_audits)} job(s) to user {user_id}")
            except Exception as e:
                audit_failed += len(user_audits)
                logger.error(f"[AUDIT_WORKER] Failed audit ping for user {user_id}: {e}")
        logger.info(f"[AUDIT_WORKER] Phase 2 complete: {audit_sent} pinged, {audit_failed} failed")
    else:
        logger.info("[AUDIT_WORKER] No overdue audits due.")

    logger.info("[REMINDER_WORKER] === All phases complete ===")


if __name__ == "__main__":
    run()
