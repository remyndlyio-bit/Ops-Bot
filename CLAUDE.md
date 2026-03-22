# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally (http://localhost:8080)
python main.py

# Health check
curl http://localhost:8080/health

# Validate AI key setup
python test_ai_key.py

# Run reminder worker standalone (normally scheduled via Railway cron)
python workers/reminder_worker.py

# Load Excel data to Supabase (creates job_entries table)
python scripts/load_excel_to_supabase.py path/to/data.xlsx
```

No test suite, linter, or formatter is configured.

## Architecture

**Ops Bot** is a FastAPI backend for a multi-platform conversational bot (Telegram + WhatsApp via Twilio). Users interact via chat to manage job entries, generate invoices, and track payments. Data lives in Supabase Postgres.

### Request Flow

```
POST /webhooks/{whatsapp,telegram}
  → _handle_bot_message(user_id, message, platform)   [main.py]
    → IntentService.process_request()                  [services/intent_service.py]
      → GeminiService (OpenRouter/Gemini 2.5 Flash)   [services/gemini_service.py]
      → SupabaseService (DB read/write)               [services/supabase_service.py]
      → InvoiceGenerationService (PDF via fpdf2)      [services/invoice_generation_service.py]
      → ResendEmailService / WhatsAppService / TelegramService
```

The `_handle_bot_message` function in `main.py` is the unified handler for both platforms. It delegates everything to `IntentService`, which does routing, multi-step form state management, and NL→SQL translation.

### Intent Routing (intent_service.py)

The main `process_request()` method classifies user input into these flows:
- **form_step**: Multi-step job entry (state stored in MemoryService per user)
- **smart_capture**: NL field extraction to add a job in one message
- **invoice_request**: Generate PDF invoice, email it, update `invoice_date` in DB
- **query**: NL→SQL via GeminiService, execute against `job_entries`
- **reminder_handling**: Payment escalation prompts (15/30/45-day tiers)
- **bank_details**: CRUD on `user_config` table
- **account_linking**: Cross-platform user linking via `user_profiles.preferences.linked_user_id`

### Database (Supabase Postgres, `supabase/schema.sql`)

Three tables:
- **`job_entries`** – core data: jobs, fees, invoice state, reminder timestamps (`first_reminder_sent`, `second_reminder_sent`, `third_reminder_sent`), `user_id`
- **`user_config`** – bank details per user (account, IFSC, UPI)
- **`user_profiles`** – onboarding state, platform, cross-platform linking via `preferences` JSONB

`user_id` is either a Telegram `chat_id` or WhatsApp phone number. Cross-platform linking stores `linked_user_id` in `preferences` so both platforms can query the same `job_entries` rows.

### AI Layer (gemini_service.py)

All AI calls go through OpenRouter to Gemini 2.5 Flash (`AI_KEY` env var). Used for:
- Intent classification
- NL→SQL generation (with schema context from `COLUMN_SCHEMA` env var)
- Field extraction from free-form messages
- Response synthesis

### Background Workers

`workers/reminder_worker.py` scans `job_entries` for unpaid invoices past 15/30/45-day thresholds and sends Telegram inline-button messages or WhatsApp text. It runs as a Railway cron job.

### Startup Side Effects (main.py)

On startup, the app:
1. Sets the Telegram webhook URL (`BASE_URL/webhooks/telegram`)
2. Broadcasts "bot updated" to known Telegram chats

### Platform Differences

Telegram supports inline buttons (payment reminder actions). WhatsApp (Twilio) uses text only with `pending_reminders` state in `utils/pending_reminders.py` to track which action the next message should trigger.

### Environment Variables

See `.env.example`. Required: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `TWILIO_WHATSAPP_NUMBER`, `BASE_URL`, `TELEGRAM_BOT_TOKEN`, `AI_KEY`, `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`, `SUPABASE_DB_URL` (use pooler connection, port 6543). Optional: `RESEND_API`, `RESEND_FROM_EMAIL`, `COLUMN_SCHEMA`.

### Deployment

Railway with `Procfile`: `web: uvicorn main:app --host 0.0.0.0 --port $PORT`. Python 3.12.9 pinned in `.python-version`.
