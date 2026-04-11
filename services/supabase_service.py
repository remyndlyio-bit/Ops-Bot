"""
Supabase data access: schema for job_entries and execution of validated SQL (SELECT + INSERT).
Uses SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY for REST; SUPABASE_DB_URL for raw SQL.

IMPORTANT: On Railway/serverless use the CONNECTION POOLER URL (port 6543), not direct (5432).
Direct (db.xxx.supabase.co:5432) often fails with "Network is unreachable".
Dashboard → Project Settings → Database → Connection string → "Transaction" / pooler (port 6543).
"""

import os
import json
from datetime import date, datetime
from typing import List, Dict, Any, Optional
from utils.logger import logger


def _build_schema_description(column_schema: Dict[str, Dict[str, Any]]) -> str:
    """
    Build a human-readable schema description from a COLUMN_SCHEMA-style mapping.

    Expected shape (from env JSON or code):
    {
        "job_date": {
            "description": "The date on which the job was executed",
            "type": "date"
        },
        ...
    }
    """
    lines: List[str] = ["Table: public.job_entries"]
    for name, meta in column_schema.items():
        if not isinstance(meta, dict):
            # Fallback if user only provided a description string
            desc = str(meta).strip()
            if desc:
                lines.append(f"- {name}: {desc}")
            else:
                lines.append(f"- {name}")
            continue

        desc = str(meta.get("description", "")).strip()
        col_type = str(meta.get("type", "")).strip() or "text"

        if desc:
            lines.append(f"- {name} ({col_type}): {desc}")
        else:
            lines.append(f"- {name} ({col_type})")

    lines.append("Use exact column names. For dates use ISO YYYY-MM-DD.")
    return "\n".join(lines)


def _load_column_schema_from_env() -> Optional[Dict[str, Dict[str, Any]]]:
    """
    Optionally load a structured column schema from the COLUMN_SCHEMA env variable.

    COLUMN_SCHEMA should be JSON, for example:

    {
        "job_date": {
            "description": "The date on which the job was executed",
            "type": "date"
        },
        "client_name": {
            "description": "Entity from where the job was procured",
            "type": "string"
        }
    }
    """
    raw = (os.getenv("COLUMN_SCHEMA") or "").strip()
    if not raw:
        return None

    try:
        data = json.loads(raw)
    except Exception as e:
        logger.warning(f"Failed to parse COLUMN_SCHEMA env as JSON: {e}")
        return None

    if not isinstance(data, dict):
        logger.warning("COLUMN_SCHEMA env must be a JSON object mapping column_name -> {description, type}.")
        return None

    # Normalize keys to strings and values to dicts
    normalized: Dict[str, Dict[str, Any]] = {}
    for key, value in data.items():
        col_name = str(key).strip()
        if not col_name:
            continue
        if isinstance(value, dict):
            normalized[col_name] = value
        else:
            # Allow simple "column": "description" format as a convenience
            normalized[col_name] = {"description": str(value), "type": "string"}

    if not normalized:
        return None

    logger.info(f"Loaded COLUMN_SCHEMA from env with {len(normalized)} columns.")
    return normalized


_COLUMN_SCHEMA_FROM_ENV = _load_column_schema_from_env()

# Schema for job_entries (single table); used for SQL generation and validation.
# If COLUMN_SCHEMA env is provided, derive columns and description from it.
if _COLUMN_SCHEMA_FROM_ENV:
    JOB_ENTRIES_COLUMNS = ["id", "created_at"] + list(_COLUMN_SCHEMA_FROM_ENV.keys())
    SCHEMA_DESCRIPTION = _build_schema_description(_COLUMN_SCHEMA_FROM_ENV)
else:
    JOB_ENTRIES_COLUMNS = [
        "id",
        "created_at",
        "user_id",
        "job_date",
        "client_name",
        "brand_name",
        "job_description_details",
        "job_notes",
        "client_billing_details",
        "fees",
        "advance",
        "added_3rd_party_cut",
        "bill_no",
        "bill_sent",
        "invoice_date",
        "paid",
        "payment_date",
        "poc_email",
        "poc_name",
        "first_reminder_sent",
        "second_reminder_sent",
        "third_reminder_sent",
        "payment_details",
        "notes",
        "isDeleted",
    ]

    SCHEMA_DESCRIPTION = """
Table: public.job_entries
- user_id (uuid): owner of the row; every SELECT must filter by user_id, every INSERT must include user_id.
- job_date (date): when the job was done; use for "when", "last gig", time filters.
- client_name (text): client name or organization.
- brand_name (text): brand or product (e.g. Titan, Tanishq, Surf Excel).
- job_description_details (text): job/project description.
- job_notes (text): notes.
- client_billing_details (text): billing instructions or special terms for this client.
- fees (integer): amount in rupees.
- advance (numeric), added_3rd_party_cut (numeric).
- bill_no (text), bill_sent (text), invoice_date (date): when the invoice was sent to the client, paid (text): billing status.
- payment_date (date): when payment was received.
- poc_email (text), poc_name (text): contact.
- first_reminder_sent, second_reminder_sent, third_reminder_sent (timestamptz).
- payment_details (text), notes (text).
- "isDeleted" (boolean): soft-delete flag; rows with "isDeleted" = true are treated as deleted and must be excluded from all queries.
Use exact column names. For dates use ISO YYYY-MM-DD. TODAY for relative ranges.
"""


class SupabaseService:
    def __init__(self):
        self.url = (os.getenv("SUPABASE_URL") or "").strip()
        self.key = (os.getenv("SUPABASE_SERVICE_ROLE_KEY") or "").strip()
        self.db_url = (os.getenv("SUPABASE_DB_URL") or "").strip()
        self._client = None

    @property
    def client(self):
        if self._client is None and self.url and self.key:
            from supabase import create_client
            self._client = create_client(self.url, self.key)
        return self._client

    def get_schema(self) -> Dict[str, Any]:
        """Return table name, column list, and description for SQL generation."""
        return {
            "table": "job_entries",
            "schema_name": "public",
            "columns": JOB_ENTRIES_COLUMNS,
            "description": SCHEMA_DESCRIPTION.strip(),
        }

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """
        Execute validated SQL (SELECT, INSERT, or UPDATE) via direct Postgres connection.
        SELECT: returns {"ok": True, "rows": [...], "operation": "select"}.
        INSERT/UPDATE: returns {"ok": True, "rows": [...]} if RETURNING used, else {"ok": True, "rowcount": N, "operation": "insert"|"update"}.
        On error: {"ok": False, "error": "..."}.
        """
        if not self.db_url:
            logger.warning("SUPABASE_DB_URL not set; cannot run raw SQL")
            return {"ok": False, "error": "Database URL not configured (SUPABASE_DB_URL)."}

        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
        except ImportError:
            return {"ok": False, "error": "psycopg2 not installed (required for raw SQL)."}

        sql = sql.strip().rstrip(";")
        upper = sql.upper()
        if not upper.startswith("SELECT") and not upper.startswith("INSERT") and not upper.startswith("UPDATE"):
            return {"ok": False, "error": "Only SELECT, INSERT, and UPDATE are allowed."}

        try:
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql)
                if upper.startswith("SELECT"):
                    rows = cur.fetchall()
                    out = []
                    for row in rows:
                        d = dict(row)
                        for k, v in d.items():
                            if hasattr(v, "isoformat") and v is not None:
                                d[k] = v.isoformat()
                        out.append(d)
                    conn.close()
                    return {"ok": True, "rows": out, "operation": "select"}
                else:
                    rowcount = cur.rowcount
                    rows = []
                    if "RETURNING" in upper:
                        rows = cur.fetchall()
                        out = []
                        for row in rows:
                            d = dict(row)
                            for k, v in d.items():
                                if hasattr(v, "isoformat") and v is not None:
                                    d[k] = v.isoformat()
                            out.append(d)
                        rows = out
                    conn.close()
                    op = "update" if upper.startswith("UPDATE") else "insert"
                    return {"ok": True, "rows": rows, "rowcount": rowcount, "operation": op}
        except Exception as e:
            logger.error(f"Supabase SQL execution error: {e}")
            err_msg = str(e)
            # Don't send raw connection errors to Telegram
            if "network" in err_msg.lower() or "unreachable" in err_msg.lower() or "connection" in err_msg.lower():
                if "db." in err_msg and "supabase.co" in err_msg:
                    logger.info(
                        "Tip: SUPABASE_DB_URL must use the POOLER HOST from Supabase Dashboard → Database → Connection string → "
                        "Transaction mode (e.g. aws-0-REGION.pooler.supabase.com:6543), NOT db.PROJECT_REF.supabase.co"
                    )
                return {"ok": False, "error": "I couldn't reach the database right now. Please try again in a moment."}
            return {"ok": False, "error": "Something went wrong with that query. Please try again."}

    def execute_read_only_sql(self, sql: str) -> Dict[str, Any]:
        """Alias for execute_sql (kept for backward compatibility)."""
        return self.execute_sql(sql)

    def insert_job_entry(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Safely insert a job_entries record using parameterized SQL.
        Returns {"ok": True, "row": {...}} or {"ok": False, "error": "..."}.
        """
        if not self.db_url:
            return {"ok": False, "error": "Database URL not configured (SUPABASE_DB_URL)."}

        if not record or not isinstance(record, dict):
            return {"ok": False, "error": "No data provided to insert."}

        # Require user_id for multi-tenant isolation
        if "user_id" not in record or not record["user_id"]:
            return {"ok": False, "error": "user_id is required for every insert."}

        # Remove disallowed keys
        cleaned = {k: v for k, v in record.items() if k and k not in {"id", "created_at"}}
        if not cleaned:
            return {"ok": False, "error": "No insertable fields provided."}

        allowed = set(JOB_ENTRIES_COLUMNS)
        cleaned = {k: v for k, v in cleaned.items() if k in allowed}
        if not cleaned:
            return {"ok": False, "error": "No valid fields to insert (check column names)." }

        # Normalize some common types to strings for Postgres
        for k, v in list(cleaned.items()):
            if isinstance(v, (date, datetime)):
                cleaned[k] = v.isoformat()[:10]

        cols = list(cleaned.keys())
        placeholders = ", ".join(["%s"] * len(cols))
        col_list = ", ".join([f'"{c}"' for c in cols])
        sql = f'INSERT INTO public.job_entries ({col_list}) VALUES ({placeholders}) RETURNING *'
        values = [cleaned[c] for c in cols]

        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
        except ImportError:
            return {"ok": False, "error": "psycopg2 not installed (required for DB inserts)."}

        try:
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, values)
                row = cur.fetchone()
            conn.close()
            out = dict(row) if row else {}
            for k, v in list(out.items()):
                if hasattr(v, "isoformat") and v is not None:
                    out[k] = v.isoformat()
            return {"ok": True, "row": out}
        except Exception as e:
            logger.error(f"Supabase insert error: {e}")
            return {"ok": False, "error": "Failed to insert record."}

    def fetch_job_entries_for_invoice(
        self,
        client_name: str,
        month: Optional[int] = None,
        year: Optional[int] = None,
        bill_no: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Fetch job_entries rows for invoice generation.
        - If bill_no provided: match by bill_no exactly.
        - Else match by client_name (ILIKE) and optional month/year on job_date.
        - If user_id provided, results are scoped to that user.
        Returns {"ok": True, "rows": [...]} or {"ok": False, "error": "..."}.
        """
        if not self.db_url:
            return {"ok": False, "error": "Database URL not configured (SUPABASE_DB_URL)."}

        if not client_name and not bill_no:
            return {"ok": False, "error": "client_name or bill_no is required."}

        where = ["(\"isDeleted\" IS NOT TRUE)"]
        params: List[Any] = []

        if user_id:
            where.append("user_id = %s")
            params.append(str(user_id))

        if bill_no:
            where.append("bill_no = %s")
            params.append(str(bill_no).strip())
        else:
            where.append("(client_name ILIKE %s OR production_house ILIKE %s)")
            params.append(f"%{client_name.strip()}%")
            params.append(f"%{client_name.strip()}%")

        if month:
            where.append("EXTRACT(MONTH FROM job_date) = %s")
            params.append(int(month))
        if year:
            where.append("EXTRACT(YEAR FROM job_date) = %s")
            params.append(int(year))

        sql = (
            "SELECT * FROM public.job_entries "
            f"WHERE {' AND '.join(where)} "
            "ORDER BY job_date ASC "
            "LIMIT 500"
        )
        logger.info(f"[INVOICE DEBUG] SQL: {sql}")
        logger.info(f"[INVOICE DEBUG] Params: {params}")

        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
        except ImportError:
            return {"ok": False, "error": "psycopg2 not installed (required for DB queries)."}

        try:
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    cur.execute(sql, params)
                except Exception as col_err:
                    if "production_house" in str(col_err):
                        logger.warning("[INVOICE] production_house column missing, retrying without it")
                        # Rebuild query using client_name only
                        w2 = ['("isDeleted" IS NOT TRUE)']
                        p2 = []
                        if user_id:
                            w2.append("user_id = %s")
                            p2.append(str(user_id))
                        if bill_no:
                            w2.append("bill_no = %s")
                            p2.append(str(bill_no).strip())
                        else:
                            w2.append("client_name ILIKE %s")
                            p2.append(f"%{client_name.strip()}%")
                        if month:
                            w2.append("EXTRACT(MONTH FROM job_date) = %s")
                            p2.append(int(month))
                        if year:
                            w2.append("EXTRACT(YEAR FROM job_date) = %s")
                            p2.append(int(year))
                        sql2 = f"SELECT * FROM public.job_entries WHERE {' AND '.join(w2)} ORDER BY job_date ASC LIMIT 500"
                        cur.execute(sql2, p2)
                    else:
                        raise
                rows = cur.fetchall()
            conn.close()
            out = []
            for row in rows:
                d = dict(row)
                for k, v in d.items():
                    if hasattr(v, "isoformat") and v is not None:
                        d[k] = v.isoformat()
                out.append(d)
            return {"ok": True, "rows": out}
        except Exception as e:
            logger.error(f"Supabase fetch invoice rows error: {e}")
            return {"ok": False, "error": "Failed to fetch invoice rows."}

    def get_available_months_for_client(self, client_name: str, user_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Return distinct (year, month) pairs for a client with non-deleted job entries.
        Returns {"ok": True, "months": [{"year": 2025, "month": 3, "label": "March 2025"}, ...]}
        sorted newest first.
        """
        if not self.db_url:
            return {"ok": False, "error": "Database URL not configured."}

        client_pattern = f"%{client_name.strip()}%"
        params: List[Any] = [client_pattern, client_pattern]
        user_filter = ""
        if user_id:
            user_filter = "AND user_id = %s "
            params.append(str(user_id))

        sql = (
            "SELECT EXTRACT(YEAR FROM job_date)::int AS yr, EXTRACT(MONTH FROM job_date)::int AS mo "
            "FROM public.job_entries "
            f"WHERE (client_name ILIKE %s OR production_house ILIKE %s) {user_filter}"
            "AND job_date IS NOT NULL "
            "AND (\"isDeleted\" IS NOT TRUE) "
            "GROUP BY yr, mo "
            "ORDER BY yr DESC, mo DESC "
            "LIMIT 36"
        )

        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
        except ImportError:
            return {"ok": False, "error": "psycopg2 not installed."}

        try:
            import calendar
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                try:
                    cur.execute(sql, params)
                except Exception as col_err:
                    if "production_house" in str(col_err):
                        logger.warning("[MONTHS] production_house column missing, retrying without it")
                        sql_fallback = (
                            "SELECT EXTRACT(YEAR FROM job_date)::int AS yr, EXTRACT(MONTH FROM job_date)::int AS mo "
                            "FROM public.job_entries "
                            f"WHERE client_name ILIKE %s {user_filter}"
                            "AND job_date IS NOT NULL "
                            "AND (\"isDeleted\" IS NOT TRUE) "
                            "GROUP BY yr, mo "
                            "ORDER BY yr DESC, mo DESC "
                            "LIMIT 36"
                        )
                        fb_params = [client_pattern]
                        if user_id:
                            fb_params.append(str(user_id))
                        cur.execute(sql_fallback, fb_params)
                    else:
                        raise
                rows = cur.fetchall()
            conn.close()
            months = []
            for row in rows:
                yr, mo = int(row["yr"]), int(row["mo"])
                months.append({"year": yr, "month": mo, "label": f"{calendar.month_name[mo]} {yr}"})
            return {"ok": True, "months": months}
        except Exception as e:
            logger.error(f"get_available_months_for_client error: {e}")
            return {"ok": False, "error": "Failed to fetch months."}

    def update_job_entry_field(self, row_id: str, field: str, value: Any) -> Dict[str, Any]:
        """
        Update a single field of a job_entries row by id (UUID).
        Used e.g. to set first_reminder_sent = now() after sending a reminder.
        Returns {"ok": True} or {"ok": False, "error": "..."}.
        """
        if not self.db_url:
            return {"ok": False, "error": "Database URL not configured (SUPABASE_DB_URL)."}
        if field not in JOB_ENTRIES_COLUMNS or field in ("id", "created_at"):
            return {"ok": False, "error": f"Invalid or read-only field: {field}."}
        if hasattr(value, "isoformat"):
            value = value.isoformat()
        try:
            import psycopg2
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(f'UPDATE public.job_entries SET "{field}" = %s WHERE id = %s', (value, row_id))
                conn.close()
            return {"ok": True}
        except Exception as e:
            logger.error(f"Supabase update_job_entry_field error: {e}")
            return {"ok": False, "error": str(e)}

    def update_poc_email_for_client(self, user_id: str, client_name: str, poc_email: str) -> Dict[str, Any]:
        """
        Update poc_email for all job_entries matching a client_name (ILIKE) for a user.
        Returns {"ok": True, "updated": N} or {"ok": False, "error": "..."}.
        """
        if not self.db_url:
            return {"ok": False, "error": "Database URL not configured."}
        try:
            import psycopg2
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    'UPDATE public.job_entries SET poc_email = %s '
                    'WHERE user_id = %s AND client_name ILIKE %s AND (poc_email IS NULL OR TRIM(poc_email) = %s)',
                    (poc_email, str(user_id), f'%{client_name}%', '')
                )
                updated = cur.rowcount
            conn.close()
            logger.info(f"[POC] Updated poc_email for {updated} rows (client={client_name}, user={user_id})")
            return {"ok": True, "updated": updated}
        except Exception as e:
            logger.error(f"Supabase update_poc_email_for_client error: {e}")
            return {"ok": False, "error": str(e)}

    def fetch_reminder_targets(
        self,
        approaching_days: int = 7,
        payment_terms_days: int = 30,
        user_id: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Fetch job_entries that are unpaid, have poc_email, first_reminder_sent is null,
        and (job_date + payment_terms_days) is within [today, today + approaching_days].
        If user_id provided, results are scoped to that user.
        Returns list of dicts with id, client_name, poc_email, job_date, fees, bill_no, etc.
        """
        if not self.db_url:
            return []
        user_clause = "AND user_id = %s" if user_id else ""
        sql = f"""
        SELECT id, client_name, poc_email, job_date, fees, bill_no,
               (job_date + (%s::int || ' days')::interval)::date AS due_date
        FROM public.job_entries
        WHERE ("isDeleted" IS NOT TRUE)
          AND (paid IS NULL OR paid::text NOT IN ('true','t','yes','1'))
          AND poc_email IS NOT NULL AND TRIM(poc_email::text) != ''
          AND first_reminder_sent IS NULL
          AND job_date IS NOT NULL
          AND (job_date + (%s::int || ' days')::interval)::date >= CURRENT_DATE
          AND (job_date + (%s::int || ' days')::interval)::date <= CURRENT_DATE + (%s::int || ' days')::interval
          {user_clause}
        ORDER BY job_date ASC
        LIMIT 100
        """
        params = [payment_terms_days, payment_terms_days, payment_terms_days, approaching_days]
        if user_id:
            params.append(str(user_id))
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
            conn.close()
            out = []
            for r in rows:
                d = dict(r)
                for k, v in d.items():
                    if hasattr(v, "isoformat") and v is not None:
                        d[k] = v.isoformat()
                out.append(d)
            return out
        except Exception as e:
            logger.error(f"Supabase fetch_reminder_targets error: {e}")
            return []

    def fetch_overdue_jobs(self, payment_terms_days: int = 30, user_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Fetch job_entries that are unpaid and (job_date + payment_terms_days) < today.
        If user_id provided, results are scoped to that user.
        """
        if not self.db_url:
            return []
        user_clause = "AND user_id = %s" if user_id else ""
        sql = f"""
        SELECT id, client_name, job_date, fees, bill_no, poc_email,
               (job_date + (%s::int || ' days')::interval)::date AS due_date
        FROM public.job_entries
        WHERE ("isDeleted" IS NOT TRUE)
          AND (paid IS NULL OR paid::text NOT IN ('true','t','yes','1'))
          AND job_date IS NOT NULL
          AND (job_date + (%s::int || ' days')::interval)::date < CURRENT_DATE
          {user_clause}
        ORDER BY (job_date + (%s::int || ' days')::interval)::date ASC
        LIMIT 100
        """
        params = [payment_terms_days, payment_terms_days]
        if user_id:
            params.append(str(user_id))
        params.append(payment_terms_days)
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
            conn.close()
            out = []
            for r in rows:
                d = dict(r)
                for k, v in d.items():
                    if hasattr(v, "isoformat") and v is not None:
                        d[k] = v.isoformat()
                out.append(d)
            return out
        except Exception as e:
            logger.error(f"Supabase fetch_overdue_jobs error: {e}")
            return []

    # --- User Config (bank details) ---

    _BANK_DETAIL_FIELDS = [
        "bank_account_name", "bank_account_number", "bank_ifsc",
        "bank_name", "upi_id",
    ]

    def get_user_bank_details(self, user_id: str) -> Dict[str, Any]:
        """
        Fetch bank details from user_config for a given user_id.
        Returns {"ok": True, "data": {...}} or {"ok": False, "error": "..."}.
        """
        if not self.db_url:
            return {"ok": False, "error": "Database URL not configured."}
        if not user_id:
            return {"ok": False, "error": "user_id is required."}

        sql = "SELECT * FROM public.user_config WHERE user_id = %s LIMIT 1"
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, (str(user_id),))
                row = cur.fetchone()
            conn.close()
            if not row:
                logger.info(f"[BANK] No bank details found for user_id={user_id}")
                return {"ok": True, "data": None}
            out = dict(row)
            for k, v in list(out.items()):
                if hasattr(v, "isoformat") and v is not None:
                    out[k] = v.isoformat()
            logger.info(f"[BANK] Retrieved bank details for user_id={user_id}")
            return {"ok": True, "data": out}
        except Exception as e:
            logger.error(f"Supabase get_user_bank_details error: {e}")
            return {"ok": False, "error": "Failed to retrieve bank details."}

    def upsert_user_config(self, user_id: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert or update user_config row for a given user_id.
        Only bank-detail fields are written; unknown keys are ignored.
        Returns {"ok": True, "data": {...}} or {"ok": False, "error": "..."}.
        """
        if not self.db_url:
            return {"ok": False, "error": "Database URL not configured."}
        if not user_id:
            return {"ok": False, "error": "user_id is required."}

        cleaned = {k: v for k, v in config.items() if k in self._BANK_DETAIL_FIELDS and v}
        if not cleaned:
            return {"ok": False, "error": "No valid bank detail fields provided."}

        cols = list(cleaned.keys())
        values = [cleaned[c] for c in cols]

        col_list = ", ".join([f'"{c}"' for c in cols])
        placeholders = ", ".join(["%s"] * len(cols))
        update_set = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols])

        sql = (
            f'INSERT INTO public.user_config (user_id, {col_list}) '
            f"VALUES (%s, {placeholders}) "
            f"ON CONFLICT (user_id) DO UPDATE SET {update_set}, "
            f"updated_at = now() "
            f"RETURNING *"
        )
        params = [str(user_id)] + values

        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                row = cur.fetchone()
            conn.close()
            out = dict(row) if row else {}
            for k, v in list(out.items()):
                if hasattr(v, "isoformat") and v is not None:
                    out[k] = v.isoformat()
            logger.info(f"[BANK] Upserted bank details for user_id={user_id}: fields={cols}")
            return {"ok": True, "data": out}
        except Exception as e:
            logger.error(f"Supabase upsert_user_config error: {e}")
            return {"ok": False, "error": "Failed to save bank details."}

    # --- User Profiles (onboarding) ---

    def get_user_profile(self, user_id: str) -> Dict[str, Any]:
        """
        Fetch user profile for a given user_id.
        Returns {"ok": True, "data": {...}} or {"ok": False, "error": "..."}.
        """
        if not self.db_url:
            return {"ok": False, "error": "Database URL not configured."}
        if not user_id:
            return {"ok": False, "error": "user_id is required."}

        sql = "SELECT * FROM public.user_profiles WHERE user_id = %s LIMIT 1"
        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, (str(user_id),))
                row = cur.fetchone()
            conn.close()
            if not row:
                logger.info(f"[PROFILE] No profile found for user_id={user_id}")
                return {"ok": True, "data": None}
            out = dict(row)
            for k, v in list(out.items()):
                if hasattr(v, "isoformat") and v is not None:
                    out[k] = v.isoformat()
            logger.info(f"[PROFILE] Retrieved profile for user_id={user_id}")
            return {"ok": True, "data": out}
        except Exception as e:
            logger.error(f"Supabase get_user_profile error: {e}")
            return {"ok": False, "error": "Failed to retrieve user profile."}

    def upsert_user_profile(self, user_id: str, platform: str, profile: Dict[str, Any]) -> Dict[str, Any]:
        """
        Insert or update user profile for a given user_id.
        Returns {"ok": True, "data": {...}} or {"ok": False, "error": "..."}.
        """
        if not self.db_url:
            return {"ok": False, "error": "Database URL not configured."}
        if not user_id:
            return {"ok": False, "error": "user_id is required."}

        # Merge with existing profile if updating
        existing = self.get_user_profile(user_id)
        if existing.get("ok") and existing.get("data"):
            # Keep existing fields that aren't being updated
            current = existing["data"]
            for k, v in profile.items():
                current[k] = v
            profile = current

        # Add/update platform and timestamps
        if platform:  # Only set platform if provided
            profile["platform"] = platform
        if not profile.get("platform"):
            profile["platform"] = "telegram" if user_id.isdigit() else "whatsapp"

        # Convert JSONB field to string for PostgreSQL
        if "preferences" in profile:
            if isinstance(profile["preferences"], dict):
                import json
                profile["preferences"] = json.dumps(profile["preferences"])
            # If it's already a string (from DB), keep it as is

        # Build dynamic upsert query
        # Remove system columns and None values to avoid issues
        skip_cols = {'user_id', 'updated_at', 'created_at', 'id'}
        profile_for_update = {k: v for k, v in profile.items() 
                             if k not in skip_cols and v is not None}
        logger.info(f"[PROFILE] Upsert cols for {user_id}: {list(profile_for_update.keys())}")
        cols = list(profile_for_update.keys())
        values = [profile_for_update[c] for c in cols]
        
        if cols:  # Only if there are columns to update
            col_list = ", ".join([f'"{c}"' for c in cols])
            placeholders = ", ".join(["%s"] * len(cols))
            update_set = ", ".join([f'"{c}" = EXCLUDED."{c}"' for c in cols])
            
            sql = (
                f'INSERT INTO public.user_profiles (user_id, {col_list}) '
                f"VALUES (%s, {placeholders}) "
                f"ON CONFLICT (user_id) DO UPDATE SET {update_set}, "
                f"updated_at = now() "
                f"RETURNING *"
            )
            params = [str(user_id)] + values
        else:  # Just insert with user_id only
            sql = (
                'INSERT INTO public.user_profiles (user_id) '
                "VALUES (%s) "
                "ON CONFLICT (user_id) DO UPDATE SET "
                "updated_at = now() "
                "RETURNING *"
            )
            params = [str(user_id)]

        try:
            import psycopg2
            from psycopg2.extras import RealDictCursor
            conn = psycopg2.connect(self.db_url)
            conn.autocommit = True
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                logger.info(f"[PROFILE] SQL: {sql}")
                logger.info(f"[PROFILE] Params: {params}")
                cur.execute(sql, params)
                row = cur.fetchone()
            conn.close()
            out = dict(row) if row else {}
            for k, v in list(out.items()):
                if hasattr(v, "isoformat") and v is not None:
                    out[k] = v.isoformat()
            logger.info(f"[PROFILE] Upserted profile for user_id={user_id}")
            return {"ok": True, "data": out}
        except Exception as e:
            logger.error(f"Supabase upsert_user_profile error: {e}")
            return {"ok": False, "error": "Failed to save user profile."}
