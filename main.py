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
    """Twilio WhatsApp Webhook""" # Simplified docstring
    logger.info(f"Received message from {From}: {Body}")

    intent_data = intent_service.parse_intent(Body)
    intent = intent_data.get("intent")

    if intent == "help":
        whatsapp_service.send_text_message(From, intent_service.get_help_text())
    
    elif intent == "status":
        whatsapp_service.send_text_message(From, "✅ System is online and ready.")

    elif intent in ["generate_invoice", "get_summary"]:
        client_name = intent_data.get("client_name")
        month = intent_data.get("month")
        
        if not client_name or not month:
            whatsapp_service.send_text_message(From, "I understood you want an invoice, but I couldn't catch the client name or month. Could you specify?") # Updated message
            return PlainTextResponse("OK")

        try:
            # Fetch data
            data = sheets_service.get_invoice_data(client_name, month)
            summary = InvoiceService.process_invoice_data(data, client_name, month)
            response_text = InvoiceService.format_summary_message(summary)
            
            # Send text summary/preview
            whatsapp_service.send_text_message(From, response_text)

            # If they specifically asked for an invoice PDF, queue it
            if intent == "generate_invoice" and summary.get("found"):
                background_tasks.add_task(process_and_send_invoice, From, client_name, month, platform="whatsapp") # Added platform argument

        except Exception as e:
            logger.error(f"Error processing invoice/summary request: {e}") # Simplified error message
            whatsapp_service.send_text_message(From, "Error processing your request.")

    else:
        whatsapp_service.send_text_message(From, "I'm not sure how to help. Try asking for an invoice!") # Updated message

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

        intent_data = intent_service.parse_intent(text)
        intent = intent_data.get("intent")

        if intent == "help":
            await telegram_service.send_text_message(chat_id, intent_service.get_help_text())
        elif intent == "status":
            await telegram_service.send_text_message(chat_id, "✅ System is online and ready.")
        elif intent in ["generate_invoice", "get_summary"]:
            client_name = intent_data.get("client_name")
            month = intent_data.get("month")
            if not client_name or not month:
                await telegram_service.send_text_message(chat_id, "I couldn't catch the client name or month. Could you specified?")
                return {"status": "ok"}
            
            try:
                sheet_data = sheets_service.get_invoice_data(client_name, month)
                summary = InvoiceService.process_invoice_data(sheet_data, client_name, month)
                await telegram_service.send_text_message(chat_id, InvoiceService.format_summary_message(summary))
                if intent == "generate_invoice" and summary.get("found"):
                    background_tasks.add_task(process_and_send_invoice, None, client_name, month, platform="telegram", chat_id=chat_id)
            except Exception as e:
                logger.error(f"Error: {e}")
                await telegram_service.send_text_message(chat_id, "Error processing your request.")
        else:
            await telegram_service.send_text_message(chat_id, "I'm not sure how to help. Try asking for an invoice!")

        return {"status": "ok"}
    except Exception as e:
        logger.error(f"Telegram webhook error: {e}")
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    # Use environment port for Railway
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
