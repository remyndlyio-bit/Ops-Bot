#!/usr/bin/env python3
"""
Load job entries from the Excel template into Supabase.
Usage:
  python scripts/load_excel_to_supabase.py /path/to/Job Entry.xlsx

Requires .env: SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY
Optional: SUPABASE_DB_URL (Postgres connection URI) to create the table if missing.
  Get it from Supabase Dashboard → Project Settings → Database → Connection string (URI).
"""

import os
import sys
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from postgrest.exceptions import APIError

CREATE_TABLE_SQL = """
create table if not exists public.job_entries (
  id uuid primary key default gen_random_uuid(),
  created_at timestamptz default now(),
  job_date date,
  client_name text,
  brand_name text,
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
"""

# Optional: put Excel in project and set default, e.g.:
# DEFAULT_EXCEL = Path(__file__).resolve().parent.parent / "data" / "Job Entry - Template for Nikkunj.xlsx"
DEFAULT_EXCEL = None

# Column names in Excel (Main Sheet) → same as DB columns
COLUMNS = [
    "job_date", "client_name", "brand_name", "job_description_details",
    "job_notes", "language", "production_house", "studio", "qt", "length",
    "fees", "advance", "added_3rd_party_cut", "bill_no", "bill_sent", "paid",
    "payment_date", "poc_email", "poc_name", "first_reminder_sent",
    "second_reminder_sent", "third_reminder_sent", "payment_followup",
    "payment_details", "notes",
]
INTEGER_COLUMNS = {"qt", "fees"}
DATE_COLUMNS = {
    "job_date", "payment_date", "first_reminder_sent",
    "second_reminder_sent", "third_reminder_sent",
}


def _to_iso_date(val):
    """Normalize to ISO date/datetime string for Postgres."""
    if val is None or (isinstance(val, float) and pd.isna(val)) or pd.isna(val):
        return None
    if hasattr(val, "isoformat"):
        s = val.isoformat()
        if s == "NaT" or not s:
            return None
        return s[:10] if len(s) >= 10 else s  # date only or full ISO
    if isinstance(val, str) and val.strip():
        # e.g. "21-02-2026" (DD-MM-YYYY) -> YYYY-MM-DD
        from datetime import datetime
        for fmt in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
            try:
                dt = datetime.strptime(val.strip()[:10], fmt)
                return dt.strftime("%Y-%m-%d")
            except ValueError:
                continue
    return val


def _serialize_value(val, column: str):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    if pd.isna(val):  # pandas NaT, etc.
        return None
    if column in INTEGER_COLUMNS and isinstance(val, (int, float)):
        return int(val)
    if column in DATE_COLUMNS or hasattr(val, "isoformat"):
        out = _to_iso_date(val)
        return None if out == "NaT" or out is None else out
    return val


def row_to_record(row: pd.Series) -> dict:
    return {c: _serialize_value(row.get(c), c) for c in COLUMNS}


def load_excel(path: Path) -> pd.DataFrame:
    df = pd.read_excel(path, sheet_name="Main Sheet")
    for c in COLUMNS:
        if c not in df.columns:
            df[c] = None
    return df[COLUMNS]


def ensure_table_exists():
    """Create job_entries table if SUPABASE_DB_URL is set."""
    db_url = os.getenv("SUPABASE_DB_URL")
    if not db_url:
        return
    try:
        import psycopg2
        conn = psycopg2.connect(db_url)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(CREATE_TABLE_SQL)
        conn.close()
        print("Table public.job_entries ready (created or already existed).")
    except Exception as e:
        print(f"Could not create table via SUPABASE_DB_URL: {e}")
        print("Create the table manually in Supabase Dashboard → SQL Editor (see supabase/schema.sql).")


def main():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
    if not url or not key:
        print("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")
        sys.exit(1)

    excel_path = Path(sys.argv[1]).expanduser() if len(sys.argv) > 1 else DEFAULT_EXCEL
    if not excel_path:
        print("Usage: python scripts/load_excel_to_supabase.py <path/to/Job Entry.xlsx>")
        print("Example: python scripts/load_excel_to_supabase.py ~/Downloads/Job\\ Entry\\ -\\ Template\\ for\\ Nikkunj.xlsx")
        sys.exit(1)
    if not excel_path.exists():
        print(f"File not found: {excel_path}")
        sys.exit(1)

    ensure_table_exists()

    df = load_excel(excel_path)
    records = [row_to_record(df.iloc[i]) for i in range(len(df))]

    client: Client = create_client(url, key)
    # Insert in batches of 100
    batch_size = 100
    inserted = 0
    try:
        for i in range(0, len(records), batch_size):
            batch = records[i : i + batch_size]
            client.table("job_entries").insert(batch).execute()
            inserted += len(batch)
            print(f"Inserted {inserted}/{len(records)} rows")
    except APIError as e:
        if "PGRST205" in str(e) or "schema cache" in str(e).lower():
            print("\nThe table 'job_entries' does not exist yet.")
            print("Create it in Supabase Dashboard → SQL Editor → New query.")
            print("Paste and run the SQL from: supabase/schema.sql")
            print("Or add SUPABASE_DB_URL to .env (Database connection URI) to create it automatically.")
        raise

    print(f"Done. Loaded {len(records)} job entries into Supabase job_entries.")


if __name__ == "__main__":
    main()
