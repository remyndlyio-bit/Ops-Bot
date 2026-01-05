from fastapi import FastAPI, Request, Form, BackgroundTasks
from fastapi.responses import PlainTextResponse
from dotenv import load_dotenv
import os
from services.intent_service import IntentService
from services.sheets_service import SheetsService
from services.invoice_service import InvoiceService
from services.whatsapp_service import WhatsAppService
from utils.logger import logger

# Load environment variables
load_dotenv()

app = FastAPI(title="WhatsApp Invoice Automation Bot")

# Initialize Services
sheets_service = SheetsService()
whatsapp_service = WhatsAppService()

@app.get("/health")
def health_check():
    return {"status": "healthy", "version": "1.0.0"}

@app.post("/webhooks/whatsapp")
async def whatsapp_webhook(
    background_tasks: BackgroundTasks,
    Body: str = Form(...),
    From: str = Form(...)
):
    """
    Twilio WhatsApp Webhook
    Body: The message text
    From: The sender WhatsApp number (format: whatsapp:+<number>)
    """
    logger.info(f"Received message from {From}: {Body}")

    # 1. Parse Intent
    intent_data = IntentService.parse_intent(Body)
    intent = intent_data.get("intent")
    logger.info(f"Parsed intent: {intent}")

    # 2. Handle Intent
    response_text = ""

    if intent == "help":
        response_text = IntentService.get_help_text()
    
    elif intent == "status":
        response_text = "✅ System is online and ready to process invoices."

    elif intent == "generate_invoice":
        client_name = intent_data.get("client_name")
        month = intent_data.get("month")
        
        # Immediate processing to keep response < 5s
        # Note: If sheet is large, we might want to move this to a background task
        # But per requirements, we return the summary text.
        try:
            data = sheets_service.get_invoice_data(client_name, month)
            summary = InvoiceService.process_invoice_data(data, client_name, month)
            response_text = InvoiceService.format_summary_message(summary)
        except Exception as e:
            logger.error(f"Error processing invoice request: {e}")
            response_text = "Sorry, I encountered an error while fetching invoice data. Please try again later."

    else:
        response_text = "I didn't quite get 그. " + IntentService.get_help_text()

    # 3. Send Response via Twilio
    # We use BackgroundTasks to ensure the webhook returns quickly (< 5s)
    background_tasks.add_task(whatsapp_service.send_text_message, From, response_text)

    # Twilio expects a 200 OK. We can return empty TwiML or plain text.
    # Returning plain text is fine if we are sending the reply via Messaging API separately.
    return PlainTextResponse("OK")

if __name__ == "__main__":
    import uvicorn
    # Use environment port for Railway
    port = int(os.getenv("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)
