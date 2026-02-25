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

-- Optional: enable RLS and add policies (adjust as needed)
-- alter table public.job_entries enable row level security;
-- create policy "Allow service role full access" on public.job_entries for all using (true) with check (true);

comment on table public.job_entries is 'Job/invoice entries loaded from Excel template';
