from fastapi import FastAPI, Form, BackgroundTasks, Request # Added Request for Telegram webhook
from fastapi.responses import PlainTextResponse
from fastapi.staticfiles import StaticFiles
from dotenv import load_dotenv
import os
import httpx # Added for Telegram webhook setup
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

async def process_and_send_invoice(to_number: str, client_name: str, month: str, platform: str = "whatsapp", chat_id: int = None): # Modified signature
    """
    Background task to generate PDF and send it via WhatsApp or Telegram.
    """
    try:
        # 1. Fetch Data
        data = sheets_service.get_invoice_data(client_name, month)
        if not data:
            return
        
        # 2. Process Summary
        summary = InvoiceService.process_invoice_data(data, client_name, month)
        
        # 3. Generate PDF
        pdf_path = invoice_gen_service.generate_pdf(summary, data)
        if not pdf_path:
            logger.error("Failed to generate PDF")
            return

        if platform == "whatsapp":
            # 4. Construct Public URL
            base_url = os.getenv("BASE_URL", "").strip()
            if base_url and not base_url.startswith("http"):
                base_url = f"https://{base_url}"
            
            if not base_url:
                base_url = "http://localhost:8080" # Fallback

            filename = os.path.basename(pdf_path)
            media_url = f"{base_url}/static/{filename}"

            # 5. Send Media Message
            whatsapp_service.send_media_message(
                to_number=to_number,
                body=f"Here is the PDF invoice for {summary['client']} - {summary['month']} 📄",
                media_url=media_url
            )
        elif platform == "telegram" and chat_id:
            await telegram_service.send_document(
                chat_id=chat_id,
                file_path=pdf_path,
                caption=f"Here is the PDF invoice for {summary['client']} - {summary['month']} 📄"
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
    whatsapp_service.send_text_message(From, result["response"])

    if result.get("trigger_invoice"):
        data = result["invoice_data"]
        background_tasks.add_task(
            process_and_send_invoice, 
            From, data["client_name"], data["month"], 
            platform="whatsapp"
        )

    return PlainTextResponse("OK")

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

        user_id = str(chat_id)
        result = intent_service.process_request(user_id=user_id, message=text)
        await telegram_service.send_text_message(chat_id, result["response"])

        if result.get("trigger_invoice"):
            data_inv = result["invoice_data"]
            background_tasks.add_task(
                process_and_send_invoice, 
                None, data_inv["client_name"], data_inv["month"], 
                platform="telegram", chat_id=chat_id
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
