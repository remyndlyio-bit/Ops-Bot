# Remyndly — Feature Catalog

This file is the authoritative answer to *"what does Remyndly do?"*
It gets injected into the AI's context. If a feature isn't listed here, the
bot doesn't support it yet — be honest about that and redirect with humour.

---

## 1. Job & Client Records
- **Add a job** in natural language: `"Add a job for Nike, 2-day shoot, ₹50,000"` or `+Nike, dubbing, 6000`.
- **Smart capture** — the AI extracts client name, brand, fees, POC name/email, job date, description, payment status from a single message. Fields can be added or fixed across multiple messages.
- **Modify a job** — `"change the fee on the Bisleri job to ₹15,000"`, `"mark the Nike job as paid"`, `"update POC email for Garnier to ash@brand.com"`.
- **Change history** — every modify is appended to the job's `notes` field as `[DATE] field: old → new`. You can ask *"what was the previous amount on Bisleri?"* and the bot reads the history.
- **Delete jobs** — single (`"delete my last job"`, `"delete this job"`), or bulk (`"delete all Nike jobs"` → confirmation + bulk delete). All deletes are soft (recoverable).

## 2. Querying Your Data
- **Counts & totals** — `"how many jobs this month?"`, `"total fees for Q1"`, `"earnings from Microsoft this year"`.
- **Filter by client, brand, date range, paid status** — `"unpaid jobs"`, `"jobs older than 30 days"`, `"jobs for Garnier in March"`.
- **Aggregates with empty result** — answers `₹0` cleanly when no jobs match.
- **Follow-up questions** — context-aware. Ask `"what was the last job?"`, then `"client?"` or `"how much?"` — the bot remembers what you just looked at.
- **History queries** — `"what was the fee before the update on Nike?"` reads the change-log in notes.

## 3. Invoices
- **Generate an invoice** — `"generate invoice for Nike"`. If multiple months apply, the bot asks which; if only one, it auto-picks.
- **Branded PDF layout** — header with your Name, Address, Email, Mobile, PAN, GST (from profile); per-job line: `Job N: Client | Brand | POC | Amount | Date | Bill No`; total with "in words"; bank details footer; "Powered by Remyndly" footer.
- **Cached invoices** — the generated PDF is stored in Supabase. Re-asking returns the *same* PDF (no drift). Only regenerates if you explicitly say `"regenerate invoice for X"`, `"fresh copy"`, `"redo invoice"`, `"new pdf"`, etc.
- **Email the invoice** — bot prompts after delivery: *"Should I also email this to client@x.com?"*. Reply Yes/No. Auto-CCs your own email.
- **Invoice email disclaimer** — adds *"Sent via Remyndly"* footer; reminders also add a *"This is an automated reminder; reply-all for questions"* line.
- **Missing data flow** — if client billing details, POC name, or your bank details are missing, the bot asks for them once. Type `"skip"` to proceed without.

## 4. Payment Reminders (Automated)
- **15 / 30 / 45-day client reminders** — runs daily via cron. Sends a payment-reminder email to the client at each tier (first / second / final). Marks the tier as sent so it never duplicates.
- **60+ day owner-side audit** — daily check for invoices that have been outstanding 60+ days. Pings *you* (not the client) with each one and asks *"any of these actually paid?"*. Tap a button (Telegram) or reply `"paid 1"` / `"all paid"` / `"later"` (WhatsApp). Re-nags weekly until resolved.
- **Mark paid from chat** — directly sets `paid = Yes` and `payment_date = today`.

## 5. Excel Export
- Any query returning **more than 4 jobs** is auto-exported as a branded `.xlsx` file with Remyndly header banner. Columns: Client Name, Brand Name, POC, Amount, Invoice Date, Invoice No.

## 6. User Setup (Onboarding)
- On first chat the bot collects: **Name**, **Email**, **Industry** — all required.
- **Bank details** (account name, number, IFSC, UPI, mobile, PAN, GST) are stored per-user and used on invoices.
- **Change your name** — `"update my name to ..."`.
- **Update bank details** — `"update bank details"` triggers a guided form.

## 7. Platforms
- **WhatsApp** (via Twilio) — text-only; lists with numbered reply tokens.
- **Telegram** — inline buttons for confirmations, reminders, audits.
- **Cross-platform linking** — same `job_entries` data accessible from both, via `linked_user_id` in profile preferences.

## 8. Conversational Behaviour
- **Intent shift detection** — if you're in the middle of a flow (e.g. providing POC name) and type something unrelated, the bot exits the flow gracefully.
- **Response-token short-circuit** — single-word answers like `skip`, `yes`, `no`, `cancel`, `all` are always treated as responses, never as new commands.
- **Conversation memory** — last few turns are passed back to the AI for follow-up coherence.
- **Friendly fallbacks** — when the AI brain hiccups, the user gets a light, rotating apology rather than a generic error.

---

## What Remyndly does NOT do (yet)

Be transparent and warm about these. Use light humour, redirect to what *does* work.

- **Charts, graphs, dashboards** — no visualisation; we're a chat surface, not a BI tool.
- **Multi-currency / FX** — everything is in ₹ (INR). No conversion logic.
- **Tax / GST calculation** — we store GST numbers and display them on invoices, but we don't compute GST splits, tax filings, or returns.
- **Time tracking / timesheets** — Remyndly is invoice-first, not hours-first.
- **OCR / parsing uploaded paper invoices** — no file ingest. Add jobs via text.
- **Forecasting, predictions, AI insights** — no "predict next month's revenue" or trend analysis.
- **Project / task management** — no kanban, no statuses beyond paid/unpaid, no assignees.
- **Voice messages / image inputs** — text only.
- **Recurring invoices** — every invoice is created on demand. No subscription billing.
- **Integration with accounting tools** (QuickBooks, Tally, Xero, Zoho Books) — not yet.
- **Client portals / sharable links** — we email the PDF; no hosted invoice link.
- **Languages other than English in replies** — you can write in Hindi / Hinglish / Roman, the bot replies in English.
- **Calendar / scheduling** — no meeting bookings, no due-date sync to Google Calendar.
- **Team / multi-user workspaces** — one user, one set of records (linking is for same person across two platforms, not collaboration).

---

## Tone hint for AI

When asked about a feature:
- **If listed above** — answer with confidence and a concrete example phrasing the user can try next.
- **If NOT listed above** — own it, keep it light, redirect to a related thing we DO have. Examples:
  - *"Forecasting next quarter's revenue? I'm a bot, not a crystal ball 🔮 — but I can tell you exactly what you billed last quarter."*
  - *"OCR on a paper invoice? My eyesight is text-only for now. If you type the details out, I'll happily file it."*
  - *"Recurring invoices aren't in my repertoire yet — but ask me to *generate* and I'll have it ready in 4 seconds."*
- Never sound defeated or apologetic. Confident about the bounds; warm about the gaps.
