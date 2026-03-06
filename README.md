# Ops Bot – WhatsApp & Telegram

Production-ready FastAPI backend for conversational ops: job entries, invoices, payment reminders, and queries. Data lives in **Supabase** (`public.job_entries`).

## Features
- **Twilio WhatsApp & Telegram**: Incoming webhooks, text and media (e.g. invoice PDFs).
- **Supabase as source of truth**: All job entries, invoice data, and reminder state in Postgres.
- **Natural language**: Queries (“total billing for Garnier”), add job, send invoice, payment reminders, overdue list.
- **Invoice PDFs**: Generated from Supabase rows and sent via WhatsApp or Telegram.
- **Railway ready**: Procfile and env-based config.

## Commands (examples)
- “Add a job” / “New job” → multi-step form (job_date, client_name, brand_name, fees, …).
- “Send invoice for Garnier for March” → PDF generated from Supabase and sent.
- “Payment reminders” / “Send reminders” → approaching-due jobs (by `job_date` + terms), email sent, `first_reminder_sent` updated.
- “Overdue invoices” → list of unpaid jobs past due.
- “Total billing this year” / “Last client” / “How many jobs for X?” → SQL over `job_entries`.

## Setup

### 1. Environment variables
Create a `.env` from `.env.example`:

- **Twilio**: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_NUMBER` (e.g. `whatsapp:+14155238886`).
- **App URL**: `BASE_URL` (e.g. `https://your-app.up.railway.app`).
- **Telegram**: `TELEGRAM_BOT_TOKEN`.
- **AI**: `AI_KEY` (OpenRouter API key for intent/SQL generation).
- **Supabase**: `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_DB_URL` (use the **pooler** connection string, port 6543).

Optional: `COLUMN_SCHEMA` – JSON mapping column names to `{description, type}` for job_entries (used for prompts and validation).

### 2. Supabase
- Create a project and the `public.job_entries` table (see `scripts/load_excel_to_supabase.py` for `CREATE TABLE` and column list, or run the script once to create the table).
- Set `SUPABASE_DB_URL` to the **Transaction (pooler)** connection string from Dashboard → Project Settings → Database.

### 3. Local development
```bash
pip install -r requirements.txt
python main.py
```

### 4. Deploy to Railway
1. Connect the repo to Railway.
2. Set all environment variables in the dashboard.
3. Deploy; the `Procfile` runs the web process.

### 5. WhatsApp webhook
In Twilio Console → WhatsApp Sandbox (or your number) → “When a message comes in”:
- URL: `https://your-app.up.railway.app/webhooks/whatsapp` (your `BASE_URL` + `/webhooks/whatsapp`).

## Project structure
- `main.py`: FastAPI app, webhooks (WhatsApp, Telegram), background invoice send.
- `services/`:
  - `intent_service.py`: Routing, add-job form, invoice/reminder/overdue, NL → SQL.
  - `supabase_service.py`: Supabase client, SQL run, `job_entries` fetch/insert/update (incl. invoice and reminders).
  - `invoice_service.py`, `invoice_generation_service.py`: Invoice summary and PDF.
  - `gemini_service.py`: Intent parsing, SQL generation, response synthesis, field validation.
  - `whatsapp_service.py`, `telegram_service.py`: Messaging.
- `utils/`: Logging, date helpers, memory.
