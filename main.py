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


def send_invoice_email(
    client_name: str,
    month: str,
    year: int,
    file_path: str,
    rows: list,
    platform: str = "telegram",
    chat_id: int | None = None,
) -> None:
    """
    Send the generated invoice PDF via email using poc_email from job_entries.
    - If poc_email is missing, log and optionally notify the Telegram user.
    - On success, confirm in Telegram; on failure, log and notify.
    """
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
            user_id_str = str(chat_id) if chat_id else None
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

        prompt_msg = (
            f"Invoice generated but I don't have a contact email for {client_name}.\n\n"
            f"Please provide the client's email so I can send it:\n"
            f"Example: client@agency.com"
        )
        if platform == "telegram" and chat_id:
            try:
                import asyncio as _asyncio
                loop = _asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(
                        telegram_service.send_text_message(chat_id, prompt_msg)
                    )
                else:
                    loop.run_until_complete(
                        telegram_service.send_text_message(chat_id, prompt_msg)
                    )
            except Exception as notify_err:
                logger.warning(f"Failed to notify Telegram about missing email: {notify_err}")
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
        if platform == "telegram" and chat_id:
            try:
                import asyncio as _asyncio

                loop = _asyncio.get_event_loop()
                if loop.is_running():
                    loop.create_task(
                        telegram_service.send_text_message(
                            chat_id,
                            "Invoice generated but email sending failed.",
                        )
                    )
                else:
                    loop.run_until_complete(
                        telegram_service.send_text_message(
                            chat_id,
                            "Invoice generated but email sending failed.",
                        )
                    )
            except Exception as notify_err:
                logger.warning(f"Failed to notify Telegram about email failure: {notify_err}")
        return

    # 3. On success, confirm in Telegram
    if platform == "telegram" and chat_id:
        try:
            import asyncio as _asyncio

            loop = _asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(
                    telegram_service.send_text_message(
                        chat_id,
                        f"Invoice has been emailed to {poc_email}.",
                    )
                )
            else:
                loop.run_until_complete(
                    telegram_service.send_text_message(
                        chat_id,
                        f"Invoice has been emailed to {poc_email}.",
                    )
                )
        except Exception as notify_err:
            logger.warning(f"Failed to send Telegram confirmation for emailed invoice: {notify_err}")


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

        if platform == "whatsapp":
            # 4. Construct Public URL
            base_url = os.getenv("BASE_URL", "").strip()
            if base_url and not base_url.startswith("http"):
                base_url = f"https://{base_url}"
            
            if not base_url:
                base_url = "http://localhost:8080" # Fallback

            filename = os.path.basename(pdf_path)
            media_url = f"{base_url}/static/{filename}"

            # 5. Send PDF first (no caption to ensure it's first)
            whatsapp_service.send_media_message(
                to_number=to_number,
                body="",
                media_url=media_url
            )
            # 6. Then send confirmation
            whatsapp_service.send_text_message(to_number, confirmation_text)

        # Update invoice_date for all affected rows
        row_ids = [r["id"] for r in data if r.get("id")]
        if row_ids:
            ids_str = ",".join(f"'{rid}'" for rid in row_ids)
            supabase_service.execute_sql(
                f"UPDATE public.job_entries SET invoice_date = CURRENT_DATE WHERE id IN ({ids_str})"
            )
            logger.info(f"[INVOICE] Updated invoice_date for {len(row_ids)} row(s)")

        if platform == "telegram" and chat_id:
            # 5. Send PDF first
            await telegram_service.send_document(
                chat_id=chat_id,
                file_path=pdf_path,
                caption=""
            )
            # 6. Then send confirmation
            await telegram_service.send_text_message(chat_id, confirmation_text)
            # 7. Then send the same PDF over email (if possible)
            send_invoice_email(
                client_name=summary.get("client", client_name),
                month=summary.get("month", month or "Request"),
                year=year,
                file_path=pdf_path,
                rows=data,
                platform="telegram",
                chat_id=chat_id,
            )

    except Exception as e:
        logger.error(f"Error in process_and_send_invoice task: {e}")

@app.post("/webhooks/whatsapp")
async def whatsapp_webhook(
    background_tasks: BackgroundTasks,
    Body: str = Form(...),
    From: str = Form(...)
):
    """Twilio WhatsApp Webhook"""
    logger.info(f"Received message from {From}: {Body}")

    # Use the new Three-Stage architecture
    result = intent_service.process_request(user_id=From, message=Body)
    
    # Only send immediate response if it's not a suppressed retrieval response
    if result.get("response"):
        whatsapp_service.send_text_message(From, result["response"])

    if result.get("trigger_invoice"):
        data = result["invoice_data"]
        background_tasks.add_task(
            process_and_send_invoice, 
            From, data["client_name"], data["month"], 
            platform="whatsapp",
            bill_number=data.get("bill_number"),
            year=data.get("year"),
            user_id=From,
        )

    # Return an empty 204 so Twilio does not send an extra 'OK' message.
    return Response(status_code=204)

async def _keep_typing(chat_id: int, stop_event: asyncio.Event):
    """Send typing action every 4 seconds until stop_event is set."""
    while not stop_event.is_set():
        await telegram_service.send_chat_action(chat_id, "typing")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=4.0)
        except asyncio.TimeoutError:
            pass

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

    # Parse callback_data: remind:<job_id>:<level>
    parts = cb_data.split(":")
    if len(parts) != 3 or parts[0] != "remind":
        return
    job_id, level = parts[1], parts[2]

    # Fetch the job row to get email details
    fetch_sql = f"SELECT * FROM public.job_entries WHERE id = {int(job_id)}"
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
        update_sql = f"UPDATE public.job_entries SET {flag_col} = NOW() WHERE id = {int(job_id)}"
        supabase_service.execute_sql(update_sql)

    label_map = {"first": "First", "second": "Second", "third": "Final"}
    label = label_map.get(level, level.title())
    await telegram_service.edit_message_text(
        chat_id, message_id,
        f"✅ {label} reminder sent to {poc_email} for invoice #{bill_no}."
    )


@app.post("/webhooks/telegram")
async def telegram_webhook(background_tasks: BackgroundTasks, request: Request):
    """Telegram Webhook"""
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

        await telegram_service.send_chat_action(chat_id, "typing")
        stop_typing = asyncio.Event()
        typing_task = asyncio.create_task(_keep_typing(chat_id, stop_typing))

        user_id = str(chat_id)
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            None, lambda: intent_service.process_request(user_id=user_id, message=text)
        )

        stop_typing.set()
        typing_task.cancel()
        try:
            await typing_task
        except asyncio.CancelledError:
            pass

        if result.get("response"):
            await telegram_service.send_text_message(chat_id, result["response"])

        if result.get("trigger_invoice"):
            data_inv = result["invoice_data"]
            background_tasks.add_task(
                process_and_send_invoice, 
                None, data_inv["client_name"], data_inv["month"], 
                platform="telegram", chat_id=chat_id,
                bill_number=data_inv.get("bill_number"),
                year=data_inv.get("year"),
                user_id=user_id,
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
