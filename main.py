from fastapi import FastAPI, Form, BackgroundTasks, Request
from fastapi.responses import PlainTextResponse, Response
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import os
import asyncio
import httpx
from services.intent_service import IntentService
from services.sheets_service import SheetsService
from services.invoice_service import InvoiceService
from services.whatsapp_service import WhatsAppService
from services.telegram_service import TelegramService # Added TelegramService import
from services.invoice_generation_service import InvoiceGenerationService
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
sheets_service = SheetsService()
whatsapp_service = WhatsAppService()
telegram_service = TelegramService() # Initialized TelegramService
invoice_gen_service = InvoiceGenerationService()
intent_service = IntentService()

@app.on_event("startup")
async def startup_event():
    """Set the Telegram webhook on startup."""
    base_url = os.getenv("BASE_URL")
    if base_url:
        webhook_url = f"{base_url.rstrip('/')}/webhooks/telegram"
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        if token:
            async with httpx.AsyncClient() as client:
                resp = await client.post(f"https://api.telegram.org/bot{token}/setWebhook", data={"url": webhook_url})
                logger.info(f"Telegram webhook set to {webhook_url}: {resp.json()}")

@app.get("/health")
def health_check():
    return {"status": "healthy", "version": "1.0.0"}

async def process_and_send_invoice(to_number: str, client_name: str, month: str, platform: str = "whatsapp", chat_id: int = None, bill_number: str = None, year: int = None):
    """
    Background task to generate PDF and send it via WhatsApp or Telegram.
    Follows: Send PDF first, then confirmation message.
    """
    try:
        # 1. Fetch Data
        data = sheets_service.get_invoice_data(client_name, month, year=year)
        if not data:
            logger.warning(f"No data found for invoice generation: {client_name} - {month} (Year: {year})")
            return
        
        # 2. Process Summary
        summary = InvoiceService.process_invoice_data(data, client_name, month)
        
        # 3. Generate PDF
        pdf_path = invoice_gen_service.generate_pdf(summary, data)
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

        elif platform == "telegram" and chat_id:
            # 5. Send PDF first
            await telegram_service.send_document(
                chat_id=chat_id,
                file_path=pdf_path,
                caption=""
            )
            # 6. Then send confirmation
            await telegram_service.send_text_message(chat_id, confirmation_text)

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
            year=data.get("year")
        )

    return PlainTextResponse("OK")

async def _keep_typing(chat_id: int, stop_event: asyncio.Event):
    """Send typing action every 4 seconds until stop_event is set."""
    while not stop_event.is_set():
        await telegram_service.send_chat_action(chat_id, "typing")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=4.0)
        except asyncio.TimeoutError:
            pass

@app.post("/webhooks/telegram")
async def telegram_webhook(background_tasks: BackgroundTasks, request: Request):
    """Telegram Webhook"""
    try:
        data = await request.json()
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
                year=data_inv.get("year")
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
