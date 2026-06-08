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

# Register MIME types BEFORE mounting StaticFiles. Some container base
# images don't have these in /etc/mime.types, so Starlette would otherwise
# serve them as application/octet-stream — which Twilio (and downstream
# WhatsApp / Meta) sometimes rejects for media messages.
import mimetypes
mimetypes.add_type(
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    ".xlsx",
)
mimetypes.add_type("text/csv", ".csv")
mimetypes.add_type("application/pdf", ".pdf")

# Mount static files to serve generated PDFs / Excel / CSV
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
    """Set the Telegram webhook, run migrations, and send update message on startup."""
    # Run lightweight migrations — add columns that may not exist in older deployments.
    # Must use raw psycopg2 since execute_sql only allows SELECT/INSERT/UPDATE.
    try:
        import psycopg2
        db_url = os.getenv("SUPABASE_DB_URL")
        if db_url:
            conn = psycopg2.connect(db_url)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("ALTER TABLE public.job_entries ADD COLUMN IF NOT EXISTS production_house text")
                cur.execute("ALTER TABLE public.job_entries ADD COLUMN IF NOT EXISTS client_billing_details text")
            conn.close()
            logger.info("[STARTUP] Schema migrations applied successfully")
        else:
            logger.warning("[STARTUP] SUPABASE_DB_URL not set, skipping migrations")
    except Exception as e:
        logger.warning(f"[STARTUP] Schema migration failed (non-fatal): {e}")

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


from utils.whatsapp_export import pick_whatsapp_export_path as _pick_whatsapp_export_path


def _build_static_url(file_path: str) -> str:
    """Build a full publicly-fetchable URL for a file under output/ that Twilio
    can reach. BASE_URL may be set without a scheme (e.g.
    'web-production-02c14.up.railway.app') — Twilio rejects schemeless URLs
    with error 21620 'Invalid media URL(s)', so we normalize here. Single
    helper used by every send_media_message call so the bug can't recur
    by drift."""
    base_url = os.getenv("BASE_URL", "").strip()
    if base_url and not base_url.startswith("http"):
        base_url = f"https://{base_url}"
    if not base_url:
        base_url = "http://localhost:8080"
    filename = os.path.basename(file_path)
    return f"{base_url}/static/{filename}"


def _get_user_email(user_id: str | None) -> str:
    """Fetch the user's own invoice email from preferences for CCing on outbound mail."""
    if not user_id:
        return ""
    try:
        prof = supabase_service.get_user_profile(user_id)
        if not (prof.get("ok") and prof.get("data")):
            return ""
        prefs = prof["data"].get("preferences") or {}
        if isinstance(prefs, str):
            import json as _json
            try:
                prefs = _json.loads(prefs)
            except Exception:
                prefs = {}
        return (prefs.get("invoice_email") or "").strip()
    except Exception as e:
        logger.warning(f"[CC] Failed to fetch user email for {user_id}: {e}")
        return ""


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

    # Extract poc_name from rows for email greeting
    poc_name = None
    for row in rows or []:
        val = (row.get("poc_name") or "").strip()
        if val and val.lower() != "none":
            poc_name = val
            break

    # 2. Send email with PDF attached
    try:
        ok = email_service.send_invoice_email(
            to_email=poc_email,
            client_name=client_name,
            month=month,
            year=year,
            pdf_path=file_path,
            poc_name=poc_name,
            cc=_get_user_email(user_id_str),
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
    force_regenerate: bool = False,
):
    """
    Background task to generate PDF and send it via WhatsApp or Telegram.
    Data is fetched from Supabase job_entries.
    Reuses a cached PDF from generated_invoices unless force_regenerate=True.
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

        # ── Try cache first (unless user asked to regenerate) ─────────────
        cached_pdf_path = None
        if user_id and not force_regenerate:
            try:
                _cache_lookup = supabase_service.get_cached_invoice(
                    user_id=user_id,
                    client_name=summary.get("client") or client_name,
                    month=summary.get("month") or month or "",
                    year=int(year),
                )
                _cached = _cache_lookup.get("data") if _cache_lookup.get("ok") else None
                if _cached and _cached.get("pdf_bytes"):
                    cached_filename = _cached.get("pdf_filename") or f"Invoice_{safe_client}_{safe_month}.pdf"
                    cached_pdf_path = os.path.join("output", cached_filename)
                    _bytes = _cached["pdf_bytes"]
                    if isinstance(_bytes, memoryview):
                        _bytes = bytes(_bytes)
                    with open(cached_pdf_path, "wb") as _f:
                        _f.write(_bytes)
                    logger.info(
                        f"[INVOICE_CACHE] Reusing stored PDF for {user_id} | "
                        f"{summary.get('client')} {summary.get('month')} {year} "
                        f"(regenerated_count={_cached.get('regenerated_count')})"
                    )
            except Exception as _ce:
                logger.warning(f"[INVOICE_CACHE] lookup failed (will regenerate): {_ce}")
                cached_pdf_path = None

        if cached_pdf_path:
            pdf_path = cached_pdf_path
            # Skip the full bank/profile load — they're baked into the cached PDF
            bank_details = None
            user_profile = None
        else:
            # Fresh generation — load bank + profile, then build the PDF.
            bank_details = None
            user_profile = None
            if user_id:
                bank_result = supabase_service.get_user_bank_details(user_id)
                if bank_result.get("ok") and bank_result.get("data"):
                    bank_details = bank_result["data"]
                    logger.info(f"[INVOICE] Loaded bank details for user_id={user_id}")
                else:
                    logger.info(f"[INVOICE] No bank details found for user_id={user_id}, using defaults")
                # Fetch user profile for invoice header (name, title, address, email)
                prof_result = supabase_service.get_user_profile(user_id)
                if prof_result.get("ok") and prof_result.get("data"):
                    prof_data = prof_result["data"]
                    prefs = prof_data.get("preferences") or {}
                    if isinstance(prefs, str):
                        import json as _json
                        try:
                            prefs = _json.loads(prefs)
                        except Exception:
                            prefs = {}
                    profile_name = prefs.get("invoice_name") or prof_data.get("name")
                    if not profile_name and bank_details:
                        profile_name = bank_details.get("bank_account_name")
                    user_profile = {
                        "name": profile_name or "",
                        "title": prefs.get("invoice_title", ""),
                        "address": prefs.get("invoice_address", ""),
                        "email": prefs.get("invoice_email", ""),
                        "mobile": bank_details.get("mobile_number", "") if bank_details else "",
                        "pan": bank_details.get("pan_number", "") if bank_details else "",
                        "gst": bank_details.get("gst_number", "") if bank_details else "",
                    }
                    logger.info(f"[INVOICE] Loaded user profile for invoice header: name={user_profile.get('name')}")
            pdf_path = invoice_gen_service.generate_pdf(summary, data, bank_details=bank_details, user_profile=user_profile)
            if not pdf_path:
                logger.error("Failed to generate PDF")
                return

            # Store the freshly-generated PDF in Supabase so future asks can reuse it
            if user_id:
                try:
                    with open(pdf_path, "rb") as _pf:
                        _pdf_bytes = _pf.read()
                    _poc_email_for_cache = ""
                    _poc_name_for_cache = ""
                    for row in data:
                        if not _poc_email_for_cache:
                            _ev = (row.get("poc_email") or "").strip()
                            if _ev:
                                _poc_email_for_cache = _ev
                        if not _poc_name_for_cache:
                            _nv = (row.get("poc_name") or "").strip()
                            if _nv and _nv.lower() != "none":
                                _poc_name_for_cache = _nv
                        if _poc_email_for_cache and _poc_name_for_cache:
                            break
                    supabase_service.upsert_cached_invoice(
                        user_id=user_id,
                        client_name=summary.get("client") or client_name,
                        month=summary.get("month") or month or "",
                        year=int(year),
                        pdf_filename=os.path.basename(pdf_path),
                        pdf_bytes=_pdf_bytes,
                        poc_email=_poc_email_for_cache or None,
                        poc_name=_poc_name_for_cache or None,
                        invoicer_name=(user_profile or {}).get("name") or None,
                        row_ids=[r["id"] for r in data if r.get("id")],
                        invoice_total=summary.get("total"),
                        bill_no=summary.get("bill_no"),
                        is_regeneration=force_regenerate,
                    )
                    logger.info(
                        f"[INVOICE_CACHE] Stored PDF for {user_id} | "
                        f"{summary.get('client')} {summary.get('month')} {year} "
                        f"(force_regenerate={force_regenerate})"
                    )
                except Exception as _se:
                    logger.warning(f"[INVOICE_CACHE] Failed to store PDF: {_se}")

        # Look up poc_email up-front so we can combine the delivery message
        # with the "Should I email this?" prompt into a single message.
        poc_email = ""
        for row in data:
            val = (row.get("poc_email") or "").strip()
            if val:
                poc_email = val
                break

        if poc_email:
            confirmation_text = (
                f"Here's your invoice for {summary['client']} ({summary['month']}).\n\n"
                f"Should I also email it to {poc_email}?\n"
                f"Reply Yes to send or No to skip."
            )
        else:
            confirmation_text = f"Here's your invoice for {summary['client']} ({summary['month']})."

        # 4. Send PDF + single combined confirmation — platform-specific transport
        if platform == "whatsapp" and to_number:
            media_url = _build_static_url(pdf_path)
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

        # 6. Cache last generated invoice + arm Yes/No state for follow-up.
        # The "Should I email?" prompt is already part of confirmation_text above,
        # so we don't send a separate prompt here — just stage the state.
        try:
            if user_id and hasattr(intent_service, 'memory'):
                from datetime import datetime
                cached_client_name = summary.get("client", client_name)
                cached_month_name = summary.get("month", month or "Request")
                cached_row_ids = [r["id"] for r in data if r.get("id")]
                _cached_poc_name = ""
                for row in data:
                    val = (row.get("poc_name") or "").strip()
                    if val and val.lower() != "none":
                        _cached_poc_name = val
                        break
                _cached_invoicer_name = (user_profile or {}).get("name", "")

                _patch = {
                    "last_generated_invoice": {
                        "client_name": cached_client_name,
                        "month": cached_month_name,
                        "year": year,
                        "pdf_path": pdf_path,
                        "poc_email": poc_email or None,
                        "row_ids": cached_row_ids,
                        "cached_at": datetime.now().isoformat(),
                    }
                }
                if poc_email:
                    _patch["awaiting_send_confirmation"] = True
                    _patch["pending_send_invoice"] = {
                        "client_name": cached_client_name,
                        "month": cached_month_name,
                        "year": year,
                        "poc_email": poc_email,
                        "row_ids": cached_row_ids,
                        "poc_name": _cached_poc_name,
                        "invoicer_name": _cached_invoicer_name,
                    }
                intent_service.memory.update_user_memory(user_id, _patch)
                logger.info(f"[INVOICE] Cached invoice + armed email confirm state for user {user_id}")
                # Mirror into FlowMachine v2 so dispatch_in_flow can recognise
                # this state when FLOW_MACHINE_V2 is on. Legacy flag still
                # drives behaviour today; FlowMachine is a parallel writer.
                if poc_email:
                    try:
                        from services.flow_machine import FLOW_INVOICE_AWAIT_SEND_CONFIRM
                        intent_service.flow_machine.set_state(
                            user_id,
                            FLOW_INVOICE_AWAIT_SEND_CONFIRM,
                            {
                                "client_name": cached_client_name,
                                "month": cached_month_name,
                                "year": year,
                                "poc_email": poc_email,
                            },
                        )
                    except Exception as fm_err:
                        logger.warning(f"[FLOW_V2] mirror set_state failed (non-fatal): {fm_err}")
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

    _uid_short = str(user_id)[-6:]  # Last 6 chars for compact log prefix
    logger.info(f"┌─ [{tag}] IN  uid=…{_uid_short} │ {message[:120]}")

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
        logger.info(f"│  [{tag}] op={result.get('operation')} uid=…{_uid_short}")
    except Exception as proc_err:
        logger.error(f"│  [{tag}] ERROR uid=…{_uid_short}: {proc_err}")
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
        logger.info(f"└─ [{tag}] OUT uid=…{_uid_short} │ {result['response'][:120]}")
        if platform == "telegram" and chat_id:
            await telegram_service.send_text_message(chat_id, result["response"])
        elif platform == "whatsapp":
            whatsapp_service.send_text_message(user_id, result["response"])

    # 5. Send Excel attachment if query returned >4 job rows
    if result.get("excel_path"):
        excel_path = result["excel_path"]
        if platform == "telegram" and chat_id:
            await telegram_service.send_document(chat_id, excel_path, caption="")
        elif platform == "whatsapp":
            send_path = _pick_whatsapp_export_path(excel_path)
            media_url = _build_static_url(send_path)
            logger.info(f"[WHATSAPP] Sending Excel export as {os.path.basename(send_path)}")
            whatsapp_service.send_media_message(to_number=user_id, body="", media_url=media_url)

    # 6. Handle invoice generation — SAME for both platforms
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
            force_regenerate=bool(inv.get("force_regenerate")),
        )

    return result


@app.post("/webhooks/whatsapp")
async def whatsapp_webhook(
    background_tasks: BackgroundTasks,
    Body: str = Form(...),
    From: str = Form(...),
    MessageSid: str = Form(None),
):
    """Twilio WhatsApp Webhook — delegates to unified handler."""
    try:
        logger.info(f"[WHATSAPP] Received from {From}: {Body[:120]}")
        # Fire typing indicator IMMEDIATELY (don't wait for processing to finish).
        # BackgroundTasks runs AFTER the response is returned — too late, the reply
        # is already out by then. asyncio.to_thread runs the sync requests.post
        # call in a worker so it doesn't block the event loop.
        if MessageSid:
            asyncio.create_task(
                asyncio.to_thread(whatsapp_service.send_typing_indicator, MessageSid)
            )
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
    user_cc = _get_user_email(user_id)

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
            f"Best regards,\n{sender_name}"
            f"{email_service.REMINDER_DISCLAIMER}"
        )

        ok = email_service.send_email(to_email=poc_email, subject=subject, body=body, cc=user_cc)
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


async def _handle_overdue_audit_callback(callback_query: dict, action: str, target: str):
    """Handle overdue-invoice audit buttons: 'audit:paid:<id|all>' or 'audit:later:<id|all>'."""
    cb_id = callback_query.get("id")
    chat_id = callback_query["message"]["chat"]["id"]
    message_id = callback_query["message"]["message_id"]
    await telegram_service.answer_callback_query(cb_id)

    # Resolve the set of job_ids this action applies to
    if target == "all":
        # Pull every job_id from the reply markup's other buttons
        reply_markup = callback_query["message"].get("reply_markup", {})
        ids = set()
        for row in reply_markup.get("inline_keyboard", []):
            for btn in row:
                cb = btn.get("callback_data", "")
                parts = cb.split(":")
                if len(parts) == 3 and parts[0] == "audit" and parts[2] not in ("all",):
                    ids.add(parts[2])
        job_ids = list(ids)
    else:
        job_ids = [target]

    if not job_ids:
        await telegram_service.edit_message_text(chat_id, message_id, "Nothing to update.")
        return

    ids_sql = ",".join(f"'{jid}'" for jid in job_ids)
    if action == "paid":
        update_sql = (
            f"UPDATE public.job_entries SET paid = 'Yes', payment_date = CURRENT_DATE "
            f"WHERE id IN ({ids_sql}) RETURNING id, client_name, fees"
        )
        result = supabase_service.execute_sql(update_sql)
        if not result.get("ok"):
            await telegram_service.edit_message_text(chat_id, message_id,
                f"❌ Couldn't update: {result.get('error', 'unknown')[:120]}")
            return
        rows = result.get("rows", []) or []
        if len(rows) == 1:
            r = rows[0]
            client = (r.get("client_name") or "the invoice").strip()
            try:
                amt = f"₹{int(float(r.get('fees') or 0)):,}"
            except Exception:
                amt = ""
            msg = f"✅ Marked paid: {client}" + (f" — {amt}" if amt else "")
        else:
            msg = f"✅ Marked {len(rows)} invoice(s) as paid."
        await telegram_service.edit_message_text(chat_id, message_id, msg)
        return

    if action == "later":
        # Push the next nag out by RENAG_DAYS — stamp overdue_audit_sent = NOW()
        bump_sql = (
            f"UPDATE public.job_entries SET overdue_audit_sent = NOW() "
            f"WHERE id IN ({ids_sql}) RETURNING id"
        )
        supabase_service.execute_sql(bump_sql)
        await telegram_service.edit_message_text(
            chat_id, message_id,
            "⏸ Got it — I'll check back next week."
        )
        return


async def _handle_reminder_callback(callback_query: dict):
    """Handle inline button presses from the reminder worker notifications."""
    cb_id = callback_query.get("id")
    cb_data = callback_query.get("data", "")
    chat_id = callback_query["message"]["chat"]["id"]
    message_id = callback_query["message"]["message_id"]

    # Overdue-audit buttons (separate flow from client-reminder buttons)
    if cb_data.startswith("audit:"):
        parts = cb_data.split(":")
        if len(parts) == 3 and parts[1] in ("paid", "later"):
            await _handle_overdue_audit_callback(callback_query, parts[1], parts[2])
        else:
            await telegram_service.answer_callback_query(cb_id)
        return

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
        f"Best regards,\n{sender_name}"
        f"{email_service.REMINDER_DISCLAIMER}"
    )

    # Send email — CC the user themselves if we know their email
    ok = email_service.send_email(
        to_email=poc_email,
        subject=subject,
        body=body,
        cc=_get_user_email(user_id),
    )

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
        logger.info(f"[TELEGRAM] Received from {chat_id}: {text[:120]}")

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
