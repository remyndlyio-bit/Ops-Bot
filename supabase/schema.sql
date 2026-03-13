-- Job entries table (matches "Job Entry - Template for Nikkunj.xlsx" Main Sheet)
-- Run this in Supabase Dashboard → SQL Editor

create table if not exists public.job_entries (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz default now(),
  user_id text not null,

  job_date date,
  client_name text,
  job_description_details text,
  job_notes text,
  client_billing_details text,
  language text,
  production_house text,
  studio text,
  qt integer,
  length text,
  fees integer,
  advance numeric,
  added_3rd_party_cut numeric,
  bill_no text,
  bill_sent text,
  paid text,
  payment_date date,
  poc_email text,
  poc_name text,
  first_reminder_sent timestamptz,
  second_reminder_sent timestamptz,
  third_reminder_sent timestamptz,
  payment_details text,
  notes text
);

-- Index on user_id for fast per-user queries
create index if not exists idx_job_entries_user_id on public.job_entries(user_id);

comment on table public.job_entries is 'Job/invoice entries loaded from Excel template';

-- ============================================================
-- MIGRATION: If job_entries already exists with user_id as uuid,
-- run this to convert it to text (supports Telegram/WhatsApp IDs):
-- ============================================================
-- ALTER TABLE public.job_entries DROP CONSTRAINT IF EXISTS fk_job_entries_user;
-- ALTER TABLE public.job_entries ALTER COLUMN user_id TYPE text USING user_id::text;
-- ============================================================
-- If job_entries exists WITHOUT user_id at all:
-- ============================================================
-- ALTER TABLE public.job_entries ADD COLUMN user_id text;
-- CREATE INDEX IF NOT EXISTS idx_job_entries_user_id ON public.job_entries(user_id);
-- UPDATE public.job_entries SET user_id = 'YOUR_DEFAULT_USER_ID' WHERE user_id IS NULL;
-- ALTER TABLE public.job_entries ALTER COLUMN user_id SET NOT NULL;
-- ============================================================

-- ============================================================
-- User configuration table (bank details, one row per user)
-- ============================================================
create table if not exists public.user_config (
  id uuid primary key default gen_random_uuid(),
  user_id text unique not null,
  bank_account_name text,
  bank_account_number text,
  bank_ifsc text,
  bank_name text,
  upi_id text,
  created_at timestamptz default now(),
  updated_at timestamptz default now()
);

comment on table public.user_config is 'Per-user configuration: bank details for invoice generation';
alter table public.user_config disable row level security;

-- If SELECT returns 0 rows even though data exists, RLS is likely blocking the direct DB connection.
-- Run ONE of the following in Supabase SQL Editor:

-- Option A: Disable RLS (simplest if only your backend accesses this table)
alter table public.job_entries disable row level security;

-- Option B: Keep RLS but allow the database role used by SUPABASE_DB_URL (e.g. postgres)
-- alter table public.job_entries enable row level security;
-- create policy "Allow backend full access" on public.job_entries for all to postgres using (true) with check (true);
