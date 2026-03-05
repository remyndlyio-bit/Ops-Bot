"""
Supabase data access: schema for job_entries and execution of validated SQL (SELECT + INSERT).
Uses SUPABASE_URL + SUPABASE_SERVICE_ROLE_KEY for REST; SUPABASE_DB_URL for raw SQL.

IMPORTANT: On Railway/serverless use the CONNECTION POOLER URL (port 6543), not direct (5432).
Direct (db.xxx.supabase.co:5432) often fails with "Network is unreachable".
Dashboard → Project Settings → Database → Connection string → "Transaction" / pooler (port 6543).
"""

import os
import json
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
        "job_date",
        "client_name",
        "brand_name",
        "job_description_details",
        "job_notes",
        "language",
        "production_house",
        "studio",
        "qt",
        "length",
        "fees",
        "advance",
        "added_3rd_party_cut",
        "bill_no",
        "bill_sent",
        "paid",
        "payment_date",
        "poc_email",
        "poc_name",
        "first_reminder_sent",
        "second_reminder_sent",
        "third_reminder_sent",
        "payment_followup",
        "payment_details",
        "notes",
    ]

    SCHEMA_DESCRIPTION = """
Table: public.job_entries
- job_date (date): when the job was done; use for "when", "last gig", time filters.
- client_name (text): client name or organization.
- brand_name (text): brand or product (e.g. Titan, Tanishq, Surf Excel).
- job_description_details (text): job/project description.
- job_notes (text): notes.
- language (text): e.g. English.
- production_house (text), studio (text): production info.
- qt (integer): quantity.
- length (text): e.g. 15sec, 20sec.
- fees (integer): amount in rupees.
- advance (numeric), added_3rd_party_cut (numeric).
- bill_no (text), bill_sent (text), paid (text): billing status.
- payment_date (date): when payment was received.
- poc_email (text), poc_name (text): contact.
- first_reminder_sent, second_reminder_sent, third_reminder_sent (timestamptz).
- payment_followup (text), payment_details (text), notes (text).
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
        Execute validated SQL (SELECT or INSERT) via direct Postgres connection.
        SELECT: returns {"ok": True, "rows": [...], "operation": "select"}.
        INSERT: returns {"ok": True, "rows": [...]} if RETURNING used, else {"ok": True, "rowcount": 1, "operation": "insert"}.
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
        if not upper.startswith("SELECT") and not upper.startswith("INSERT"):
            return {"ok": False, "error": "Only SELECT and INSERT are allowed."}

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
                    return {"ok": True, "rows": rows, "rowcount": rowcount, "operation": "insert"}
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
