from fastapi import FastAPI, Form, BackgroundTasks, Request
from fastapi.responses import PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import os
import asyncio
import httpx
from services.intent_service import IntentService
from services.invoice_service import InvoiceService
from services.whatsapp_service import WhatsAppService
from services.telegram_service import TelegramService  # Added TelegramService import
from services.invoice_generation_service import InvoiceGenerationService
from services.resend_email_service import ResendEmailService
from utils.logger import logger

# Load environment variables
load_dotenv()

app = FastAPI(title="Ops Bot - WhatsApp & Telegram") # Updated FastAPI title

# Mount static files to serve generated PDFs
os.makedirs("output", exist_ok=True)
app.mount("/static", StaticFiles(directory="output"), name="static")

@app.get("/favicon.ico")
async def favicon():
    """Return empty response to remove favicon from browser tab."""
    return Response(status_code=204)

# Initialize Services
from services.supabase_service import SupabaseService
from utils.date_utils import month_name_to_number

supabase_service = SupabaseService()
whatsapp_service = WhatsAppService()
telegram_service = TelegramService()  # Initialized TelegramService
invoice_gen_service = InvoiceGenerationService()
email_service = ResendEmailService()
intent_service = IntentService()

UPDATE_MESSAGE = (
    "Hi! I've just been updated with new features and I'm smarter than ever. Have a great day 😊"
)


@app.on_event("startup")
async def startup_event():
    """Set the Telegram webhook and send update message on startup (e.g. after deploy)."""
    base_url = os.getenv("BASE_URL")
    token = os.getenv("TELEGRAM_BOT_TOKEN")

    if base_url and token:
        webhook_url = f"{base_url.rstrip('/')}/webhooks/telegram"
        async with httpx.AsyncClient() as client:
            resp = await client.post(f"https://api.telegram.org/bot{token}/setWebhook", data={"url": webhook_url})
            logger.info(f"Telegram webhook set to {webhook_url}: {resp.json()}")

    # Log the expected WhatsApp webhook URL (must be configured manually in Twilio console)
    if base_url:
        wa_webhook = f"{base_url.rstrip('/')}/webhooks/whatsapp"
        logger.info(f"WhatsApp webhook URL (set this in Twilio console): {wa_webhook}")
    else:
        logger.warning("BASE_URL not set — WhatsApp webhook URL unknown. Set BASE_URL env var.")

    # Send "I've been updated" message to all known Telegram chats (user_id = chat_id for Telegram)
    if token:
        try:
            memory = getattr(intent_service, "memory", None)
            if memory and getattr(memory, "memory", None):
                for user_id in memory.memory:
                    try:
                        chat_id = int(user_id)
                        await telegram_service.send_text_message(chat_id, UPDATE_MESSAGE)
                        logger.info(f"Sent update message to Telegram chat_id={chat_id}")
                    except (ValueError, TypeError):
                        pass  # skip non-numeric IDs (e.g. WhatsApp numbers)
        except Exception as e:
            logger.warning(f"Could not send Telegram update message: {e}")


@app.get("/health")
def health_check():
    return {"status": "healthy", "version": "1.0.0"}

@app.get("/webhooks/whatsapp")
def whatsapp_health():
    """GET handler so you can verify the WhatsApp webhook URL is reachable."""
    return {"status": "ok", "endpoint": "whatsapp_webhook"}


def _notify_user(platform: str, chat_id, user_id_str: str, msg: str):
    """Send a notification to the user on either platform."""
    try:
        if platform == "telegram" and chat_id:
            import asyncio as _asyncio
            loop = _asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(telegram_service.send_text_message(chat_id, msg))
            else:
                loop.run_until_complete(telegram_service.send_text_message(chat_id, msg))
        elif platform == "whatsapp" and user_id_str:
            whatsapp_service.send_text_message(user_id_str, msg)
    except Exception as e:
        logger.warning(f"Failed to notify {platform} user: {e}")


def send_invoice_email(
    client_name: str,
    month: str,
    year: int,
    file_path: str,
    rows: list,
    platform: str = "telegram",
    chat_id: int | None = None,
    user_id: str | None = None,
) -> None:
    """
    Send the generated invoice PDF via email using poc_email from job_entries.
    Notifies the user on BOTH platforms if poc_email is missing or email fails.
    """
    # Resolve user_id for notifications (works for both platforms)
    user_id_str = user_id or (str(chat_id) if chat_id else None)

    # 1. Look up poc_email
    poc_email = None
    for row in rows or []:
        val = (row.get("poc_email") or "").strip()
        if val:
            poc_email = val
            break

    if not poc_email:
        logger.warning("Invoice generated but client email (poc_email) is missing.")
        # Store state so user can provide POC email
        try:
            if user_id_str and hasattr(intent_service, 'memory'):
                intent_service.memory.update_user_memory(user_id_str, {
                    "awaiting_poc_email": True,
                    "poc_email_client": client_name,
                    "poc_email_pdf_path": file_path,
                    "poc_email_month": month,
                    "poc_email_year": year,
                })
        except Exception as mem_err:
            logger.warning(f"Failed to store POC email state: {mem_err}")

        _notify_user(platform, chat_id, user_id_str, (
            f"Invoice generated but I don't have a contact email for {client_name}.\n\n"
            f"Please provide the client's email so I can send it:\n"
            f"Example: client@agency.com"
        ))
        return

    # 2. Send email with PDF attached
    try:
        ok = email_service.send_invoice_email(
            to_email=poc_email,
            client_name=client_name,
            month=month,
            year=year,
            pdf_path=file_path,
        )
    except Exception as e:
        ok = False
        logger.error(f"Invoice generated but email sending failed (exception): {e}")

    if not ok:
        logger.error("Invoice generated but email sending failed.")
        _notify_user(platform, chat_id, user_id_str, "Invoice generated but email sending failed.")
        return

    # 3. On success, confirm
    _notify_user(platform, chat_id, user_id_str, f"Invoice has been emailed to {poc_email}.")


async def process_and_send_invoice(
    to_number: str,
    client_name: str,
    month: str,
    platform: str = "whatsapp",
    chat_id: int = None,
    bill_number: str = None,
    year: int = None,
    user_id: str = None,
):
    """
    Background task to generate PDF and send it via WhatsApp or Telegram.
    Data is fetched from Supabase job_entries.
    """
    try:
        month_num = month_name_to_number(month) if month else None
        if not year:
            from datetime import datetime
            year = datetime.now().year
        if bill_number:
            result = supabase_service.fetch_job_entries_for_invoice(client_name="", bill_no=bill_number, user_id=user_id)
        else:
            result = supabase_service.fetch_job_entries_for_invoice(client_name=client_name, month=month_num, year=year, user_id=user_id)
        if not result.get("ok") or not result.get("rows"):
            logger.warning(f"No data found for invoice generation: {client_name} - {month} (Year: {year})")
            return
        data = result["rows"]

        # 2. Process Summary
        summary = InvoiceService.process_invoice_data(data, client_name, month or "Request")

        # 3. Generate or reuse PDF
        safe_client = (summary.get("client") or client_name or "Client").replace(" ", "_")
        safe_month = (summary.get("month") or month or "Period").replace(" ", "_")
        os.makedirs("output", exist_ok=True)
        candidate_path = os.path.join("output", f"Invoice_{safe_client}_{safe_month}.pdf")

        if os.path.exists(candidate_path):
            pdf_path = candidate_path
        else:
            bank_details = None
            if user_id:
                bank_result = supabase_service.get_user_bank_details(user_id)
                if bank_result.get("ok") and bank_result.get("data"):
                    bank_details = bank_result["data"]
                    logger.info(f"[INVOICE] Loaded bank details for user_id={user_id}")
                else:
                    logger.info(f"[INVOICE] No bank details found for user_id={user_id}, using defaults")
            pdf_path = invoice_gen_service.generate_pdf(summary, data, bank_details=bank_details)
        if not pdf_path:
            logger.error("Failed to generate PDF")
            return

        confirmation_text = f"Here’s the invoice for {summary['client']} {summary['month']}."

        # 4. Send PDF + confirmation — platform-specific transport only
        if platform == "whatsapp" and to_number:
            base_url = os.getenv("BASE_URL", "").strip()
            if base_url and not base_url.startswith("http"):
                base_url = f"https://{base_url}"
            if not base_url:
                base_url = "http://localhost:8080"
            filename = os.path.basename(pdf_path)
            media_url = f"{base_url}/static/{filename}"
            whatsapp_service.send_media_message(to_number=to_number, body="", media_url=media_url)
            whatsapp_service.send_text_message(to_number, confirmation_text)

        elif platform == "telegram" and chat_id:
            await telegram_service.send_document(chat_id=chat_id, file_path=pdf_path, caption="")
            await telegram_service.send_text_message(chat_id, confirmation_text)

        # 5. Update invoice_date for all affected rows — SAME for both platforms
        row_ids = [r["id"] for r in data if r.get("id")]
        if row_ids:
            ids_str = ",".join(f"'{rid}'" for rid in row_ids)
            supabase_service.execute_sql(
                f"UPDATE public.job_entries SET invoice_date = CURRENT_DATE WHERE id IN ({ids_str})"
            )
            logger.info(f"[INVOICE] Updated invoice_date for {len(row_ids)} row(s)")

        # 6. Cache last generated invoice in user memory for follow-up commands
        try:
            if user_id and hasattr(intent_service, 'memory'):
                poc_email = None
                for row in data:
                    val = (row.get("poc_email") or "").strip()
                    if val:
                        poc_email = val
                        break
                intent_service.memory.update_user_memory(user_id, {
                    "last_generated_invoice": {
                        "client_name": summary.get("client", client_name),
                        "month": summary.get("month", month or "Request"),
                        "year": year,
                        "pdf_path": pdf_path,
                        "poc_email": poc_email,
                        "row_ids": [r["id"] for r in data if r.get("id")],
                    }
                })
                logger.info(f"[INVOICE] Cached last_generated_invoice for user {user_id}")
        except Exception as cache_err:
            logger.warning(f"Failed to cache invoice context: {cache_err}")

    except Exception as e:
        logger.error(f"Error in process_and_send_invoice task: {e}")

async def _keep_typing(chat_id: int, stop_event: asyncio.Event):
    """Send typing action every 4 seconds until stop_event is set."""
    while not stop_event.is_set():
        await telegram_service.send_chat_action(chat_id, "typing")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=4.0)
        except asyncio.TimeoutError:
            pass


async def _handle_bot_message(
    user_id: str,
    message: str,
    platform: str,
    background_tasks: BackgroundTasks,
    chat_id: int = None,
):
    """
    Unified message handler for both Telegram and WhatsApp.
    Ensures IDENTICAL processing flow regardless of platform.
    """
    tag = platform.upper()
    result = {"operation": "error", "response": None, "trigger_invoice": False, "invoice_data": {}}

    # 1. Typing indicator (Telegram only — WhatsApp/Twilio has no native equivalent)
    stop_typing = None
    typing_task = None
    if platform == "telegram" and chat_id:
        await telegram_service.send_chat_action(chat_id, "typing")
        stop_typing = asyncio.Event()
        typing_task = asyncio.create_task(_keep_typing(chat_id, stop_typing))

    try:
        # 2. Process the message — SAME for both platforms
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: intent_service.process_request(user_id=user_id, message=message)
        )
        logger.info(f"[{tag}] Result operation={result.get('operation')} for {user_id}")
    except Exception as proc_err:
        logger.error(f"[{tag}] process_request failed for {user_id}: {proc_err}")
        result["response"] = "Something went wrong processing your message. Please try again."
    finally:
        # 3. Stop typing indicator
        if stop_typing:
            stop_typing.set()
        if typing_task:
            typing_task.cancel()
            try:
                await typing_task
            except asyncio.CancelledError:
                pass

    # 4. Send response — platform-specific transport only
    if result.get("response"):
        logger.info(f"[{tag}] Sending text -> To={user_id}, Body={result['response'][:120]}")
        if platform == "telegram" and chat_id:
            await telegram_service.send_text_message(chat_id, result["response"])
        elif platform == "whatsapp":
            whatsapp_service.send_text_message(user_id, result["response"])

    # 5. Handle invoice generation — SAME for both platforms
    if result.get("trigger_invoice"):
        inv = result["invoice_data"]
        background_tasks.add_task(
            process_and_send_invoice,
            user_id if platform == "whatsapp" else None,  # to_number (WhatsApp only)
            inv["client_name"],
            inv["month"],
            platform=platform,
            chat_id=chat_id,
            bill_number=inv.get("bill_number"),
            year=inv.get("year"),
            user_id=user_id,
        )

    return result


@app.post("/webhooks/whatsapp")
async def whatsapp_webhook(
    background_tasks: BackgroundTasks,
    Body: str = Form(...),
    From: str = Form(...)
):
    """Twilio WhatsApp Webhook — delegates to unified handler."""
    try:
        logger.info(f"Received WhatsApp message from {From}: {Body}")
        await _handle_bot_message(
            user_id=From, message=Body, platform="whatsapp",
            background_tasks=background_tasks,
        )
    except Exception as e:
        logger.error(f"WhatsApp webhook error: {e}")
    return Response(status_code=204)

async def _handle_send_all_reminders(callback_query: dict):
    """Handle 'Send All' button — send reminder emails for every job in the inline keyboard."""
    chat_id = callback_query["message"]["chat"]["id"]
    message_id = callback_query["message"]["message_id"]
    user_id = str(chat_id)

    # Extract job_id:level pairs from the other inline buttons' callback_data
    reply_markup = callback_query["message"].get("reply_markup", {})
    job_pairs = []
    for row in reply_markup.get("inline_keyboard", []):
        for btn in row:
            cb = btn.get("callback_data", "")
            parts = cb.split(":")
            if len(parts) == 3 and parts[0] == "remind" and parts[1] not in ("skip", "send"):
                job_pairs.append((parts[1], parts[2]))  # (job_id, level)

    if not job_pairs:
        await telegram_service.edit_message_text(chat_id, message_id, "No reminders found to send.")
        return

    # Get sender name once
    profile = supabase_service.get_user_profile(user_id)
    sender_name = "Team"
    if profile.get("ok") and profile.get("data"):
        sender_name = profile["data"].get("name") or sender_name

    sent = []
    failed = []
    flag_map = {
        "first": "first_reminder_sent",
        "second": "second_reminder_sent",
        "third": "third_reminder_sent",
    }
    subject_map_tpl = {
        "first": "First Payment Reminder – Invoice #{bill_no}",
        "second": "Second Payment Reminder – Invoice #{bill_no}",
        "third": "Final Payment Reminder – Invoice #{bill_no}",
    }

    await telegram_service.edit_message_text(
        chat_id, message_id, f"⏳ Sending {len(job_pairs)} reminder(s)..."
    )

    for job_id, level in job_pairs:
        fetch_sql = f"SELECT * FROM public.job_entries WHERE id = '{job_id}'"
        result = supabase_service.execute_sql(fetch_sql)
        if not result.get("ok") or not result.get("rows"):
            failed.append(job_id)
            continue

        row = result["rows"][0]
        poc_email = row.get("poc_email")
        if not poc_email:
            failed.append(row.get("client_name") or job_id)
            continue

        bill_no = row.get("bill_no") or "N/A"
        client_name = row.get("client_name") or "Client"
        poc_name = row.get("poc_name") or client_name
        fees = row.get("fees")
        try:
            amount_str = f"₹{int(float(fees)):,}"
        except (ValueError, TypeError):
            amount_str = str(fees) if fees else "N/A"

        subject = subject_map_tpl.get(level, "Payment Reminder – Invoice #{bill_no}").replace("{bill_no}", str(bill_no))
        body = (
            f"Hi {poc_name},\n\n"
            f"This is a friendly reminder regarding invoice #{bill_no}.\n\n"
            f"Amount Due: {amount_str}\n\n"
            f"Please let us know if payment has already been processed.\n\n"
            f"Best regards,\n{sender_name}\n"
        )

        ok = email_service.send_email(to_email=poc_email, subject=subject, body=body)
        if ok:
            sent.append(f"{client_name} → {poc_email}")
            flag_col = flag_map.get(level)
            if flag_col:
                supabase_service.execute_sql(
                    f"UPDATE public.job_entries SET {flag_col} = NOW() WHERE id = '{job_id}'"
                )
        else:
            failed.append(f"{client_name} ({poc_email})")

    # Build summary
    lines = [f"✅ Sent {len(sent)} reminder(s)."]
    for s in sent:
        lines.append(f"  • {s}")
    if failed:
        lines.append(f"\n❌ Failed for {len(failed)}:")
        for f_item in failed:
            lines.append(f"  • {f_item}")
    await telegram_service.edit_message_text(chat_id, message_id, "\n".join(lines))


async def _handle_reminder_callback(callback_query: dict):
    """Handle inline button presses from the reminder worker notifications."""
    cb_id = callback_query.get("id")
    cb_data = callback_query.get("data", "")
    chat_id = callback_query["message"]["chat"]["id"]
    message_id = callback_query["message"]["message_id"]

    # Acknowledge the button press immediately
    await telegram_service.answer_callback_query(cb_id)

    if cb_data == "remind:skip:all":
        await telegram_service.edit_message_text(
            chat_id, message_id, "⏭ Reminders skipped. You can always send them manually later."
        )
        return

    if cb_data == "remind:send:all":
        await _handle_send_all_reminders(callback_query)
        return

    # Parse callback_data: remind:<job_id>:<level>
    parts = cb_data.split(":")
    if len(parts) != 3 or parts[0] != "remind":
        return
    job_id, level = parts[1], parts[2]

    # Fetch the job row to get email details
    fetch_sql = f"SELECT * FROM public.job_entries WHERE id = '{job_id}'"
    result = supabase_service.execute_sql(fetch_sql)
    if not result.get("ok") or not result.get("rows"):
        await telegram_service.edit_message_text(
            chat_id, message_id, "❌ Could not find that invoice. It may have been deleted."
        )
        return

    row = result["rows"][0]
    poc_email = row.get("poc_email")
    if not poc_email:
        await telegram_service.edit_message_text(
            chat_id, message_id,
            f"❌ No email on file for {row.get('client_name', 'this client')}. "
            f"Please add a POC email first."
        )
        return

    # Build email
    bill_no = row.get("bill_no") or "N/A"
    client_name = row.get("client_name") or "Client"
    poc_name = row.get("poc_name") or client_name
    fees = row.get("fees")
    try:
        amount_str = f"₹{int(float(fees)):,}"
    except (ValueError, TypeError):
        amount_str = str(fees) if fees else "N/A"

    subject_map = {
        "first": f"First Payment Reminder – Invoice #{bill_no}",
        "second": f"Second Payment Reminder – Invoice #{bill_no}",
        "third": f"Final Payment Reminder – Invoice #{bill_no}",
    }
    subject = subject_map.get(level, f"Payment Reminder – Invoice #{bill_no}")

    # Get sender name from user profile
    user_id = str(chat_id)
    profile = supabase_service.get_user_profile(user_id)
    sender_name = "Team"
    if profile.get("ok") and profile.get("data"):
        sender_name = profile["data"].get("name") or sender_name

    body = (
        f"Hi {poc_name},\n\n"
        f"This is a friendly reminder regarding invoice #{bill_no}.\n\n"
        f"Amount Due: {amount_str}\n\n"
        f"Please let us know if payment has already been processed.\n\n"
        f"Best regards,\n{sender_name}\n"
    )

    # Send email
    ok = email_service.send_email(to_email=poc_email, subject=subject, body=body)

    if not ok:
        await telegram_service.edit_message_text(
            chat_id, message_id,
            f"❌ Failed to send reminder email to {poc_email}. Please try again later."
        )
        return

    # Update DB flag
    flag_map = {
        "first": "first_reminder_sent",
        "second": "second_reminder_sent",
        "third": "third_reminder_sent",
    }
    flag_col = flag_map.get(level)
    if flag_col:
        update_sql = f"UPDATE public.job_entries SET {flag_col} = NOW() WHERE id = '{job_id}'"
        supabase_service.execute_sql(update_sql)

    label_map = {"first": "First", "second": "Second", "third": "Final"}
    label = label_map.get(level, level.title())
    await telegram_service.edit_message_text(
        chat_id, message_id,
        f"✅ {label} reminder sent to {poc_email} for invoice #{bill_no}."
    )


@app.post("/webhooks/telegram")
async def telegram_webhook(background_tasks: BackgroundTasks, request: Request):
    """Telegram Webhook — delegates to unified handler."""
    try:
        data = await request.json()

        # Handle inline button callbacks (e.g. reminder confirmations)
        if "callback_query" in data:
            await _handle_reminder_callback(data["callback_query"])
            return {"status": "ok"}

        if "message" not in data:
            return {"status": "ok"}

        chat_id = data["message"]["chat"]["id"]
        text = data["message"].get("text", "")
        logger.info(f"Received Telegram message from {chat_id}: {text}")

        await _handle_bot_message(
            user_id=str(chat_id), message=text, platform="telegram",
            background_tasks=background_tasks, chat_id=chat_id,
        )
        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        return {"status": "error", "message": str(e)}



if __name__ == "__main__":
    import uvicorn
    # Use environment port for Railway
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
