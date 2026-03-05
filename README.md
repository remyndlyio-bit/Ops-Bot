# WhatsApp Invoice Automation Backend

Production-ready FastAPI backend for Twilio WhatsApp invoice automation.

## Features
- **Twilio WhatsApp Integration**: Handles incoming webhooks and sends automated replies.
- **Google Sheets Source of Truth**: Fetches invoice data directly from Google Sheets.
- **Intent Parsing**: Supports `help`, `status`, and `invoice for <client> <month>` commands.
- **Railway Ready**: Optimized for deployment on Railway.app.

## Commands
- `help`: Show available commands.
- `status`: Check if the bot is healthy.
- `invoice for nikkunj july`: Get a summary of the invoice for a specific client and month.

## Setup Instructions

### 1. Environment Variables
Create a `.env` file based on `.env.example`:
- `TWILIO_ACCOUNT_SID`: From Twilio Console.
- `TWILIO_AUTH_TOKEN`: From Twilio Console.
- `TWILIO_WHATSAPP_NUMBER`: Your Twilio WhatsApp Sandbox/Production number (e.g. `whatsapp:+14155238886`).
- `BASE_URL`: Public URL where this FastAPI app is deployed (e.g. `https://your-app.up.railway.app`).
- `GOOGLE_CREDS_JSON`: Service Account JSON (either the full JSON string or a path to the JSON file).
- `SHEET_URL`: The URL of your Google Sheet.

### 2. Google Sheets Configuration
1. Create a Google Service Account in Google Cloud Console.
2. Download the JSON key and save it as `google-credentials.json`.
3. Share your Google Sheet with the service account email (with Editor access).
4. Ensure your sheet has the following headers:
   - `client_name`
   - `invoice_month`
   - `description`
   - `quantity`
   - `rate`
   - `amount`

### 3. Local Development
```bash
pip install -r requirements.txt
python main.py
```

### 4. Deployment to Railway
1. Connect your GitHub repository to Railway.
2. Add the environment variables in the Railway dashboard.
3. Railway will automatically pick up the `Procfile` and deploy.

### 5. Twilio WhatsApp Webhook Configuration
To activate WhatsApp:
1. Ensure `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_NUMBER`, and `BASE_URL` are set in Railway (or your `.env`).
2. In the Twilio Console, go to your WhatsApp Sandbox or WhatsApp-enabled number.
3. Set the **“When a message comes in”** webhook URL to:
   - `https://your-app.up.railway.app/webhooks/whatsapp` (replace with your actual `BASE_URL`).
4. Save the configuration, then send a WhatsApp message to your Twilio number to start chatting with the bot.

## Project Structure
- `main.py`: FastAPI application and webhook route.
- `services/`:
  - `intent_service.py`: Logic for parsing WhatsApp commands.
  - `sheets_service.py`: Google Sheets integration.
  - `invoice_service.py`: Calculation and summary formatting.
  - `whatsapp_service.py`: Twilio API integration.
- `utils/`: Common utilities like logging.
