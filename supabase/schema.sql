-- Job entries table (matches "Job Entry - Template for Nikkunj.xlsx" Main Sheet)
-- Run this in Supabase Dashboard → SQL Editor

create table if not exists public.job_entries (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz default now(),

  job_date date,
  client_name text,
  job_description_details text,
  job_notes text,
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
  payment_followup text,
  payment_details text,
  notes text
);

comment on table public.job_entries is 'Job/invoice entries loaded from Excel template';

-- If SELECT returns 0 rows even though data exists, RLS is likely blocking the direct DB connection.
-- Run ONE of the following in Supabase SQL Editor:

-- Option A: Disable RLS (simplest if only your backend accesses this table)
alter table public.job_entries disable row level security;

-- Option B: Keep RLS but allow the database role used by SUPABASE_DB_URL (e.g. postgres)
-- alter table public.job_entries enable row level security;
-- create policy "Allow backend full access" on public.job_entries for all to postgres using (true) with check (true);
