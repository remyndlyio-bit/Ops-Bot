"""
Natural language → SQL via Gemini. Uses Supabase schema and conversation context.
"""

import json
import re
from datetime import date
from typing import Dict, Any, List, Optional
from utils.logger import logger

from services.gemini_service import GeminiService
from services.supabase_service import SupabaseService, SCHEMA_DESCRIPTION, JOB_ENTRIES_COLUMNS


def build_sql_prompt(
    message: str,
    schema_description: str,
    columns: List[str],
    conversation_history: Optional[List[Dict[str, str]]] = None,
) -> str:
    today = date.today().isoformat()
    columns_str = ", ".join(columns[:40])
    if len(columns) > 40:
        columns_str += f" (and {len(columns) - 40} more)"

    context_block = ""
    if conversation_history:
        lines = []
        for msg in conversation_history[-6:]:
            role = "User" if msg.get("role") == "user" else "Assistant"
            lines.append(f"{role}: {msg.get('content', '')}")
        context_block = "Recent conversation:\n" + "\n".join(lines) + "\n\n"

    return f"""You are a SQL generator for a single Postgres table. Output ONLY a single SELECT or INSERT statement. No explanation, no markdown.

TODAY'S DATE: {today}

SCHEMA:
{schema_description}

ALLOWED COLUMNS (use exactly): {columns_str}

TABLE: public.job_entries (columns id and created_at are auto-generated; do not include them in INSERT).

RULES FOR SELECT:
1. Use only columns from the list above. Use snake_case for column names.
2. Relative dates: "last month" = date between first and last day of previous month. "this year" = year = {today[:4]}. "last 7 days" = job_date >= CURRENT_DATE - 7.
3. "how many", "count" → SELECT COUNT(*) ... For "total fees", "sum" → SELECT SUM(fees) ...
4. "latest", "last job", "most recent" → ORDER BY job_date DESC LIMIT 1.
5. Client/brand: WHERE client_name ILIKE '%name%' or = 'Name'.
6. Return at most 50 rows; use LIMIT 50.

RULES FOR INSERT:
7. Use when the user wants to ADD a job, LOG a job, RECORD an entry, or CREATE a new row (e.g. "add a job for Garnier", "log: Xiaomi, 2000, 15sec", "new entry: client X, fees 5000").
8. INSERT INTO public.job_entries (col1, col2, ...) VALUES (val1, val2, ...). Use only columns that the user provided; omit id and created_at. Use NULL for missing optional fields or omit the column.
9. Quote text values with single quotes; escape single quotes by doubling. Dates as 'YYYY-MM-DD'. Numbers without quotes.
10. Prefer RETURNING * at the end of INSERT so the new row is returned.

11. Output ONLY the SQL, one statement, no semicolon at the end.

{context_block}User: {message}

SQL:"""


def generate_sql(
    message: str,
    gemini_service: GeminiService,
    supabase_service: SupabaseService,
    conversation_history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    """
    Generate a single SELECT SQL from user message.
    Returns {"sql": "SELECT ...", "_error": None} or {"sql": None, "_error": "..."}.
    """
    schema = supabase_service.get_schema()
    schema_description = schema["description"]
    columns = schema["columns"]

    prompt = build_sql_prompt(message, schema_description, columns, conversation_history)
    try:
        raw = gemini_service._call_api(prompt, generation_config={
            "temperature": 0,
            "maxOutputTokens": 1024,
        })
        if not raw or not raw.strip():
            return {"sql": None, "_error": "Empty response from AI."}
        sql = raw.strip()
        # Strip markdown code block if present
        if sql.startswith("```"):
            lines = sql.split("\n")
            if lines and lines[0].strip().startswith("```"):
                lines = lines[1:]
                if lines and lines[-1].strip() == "```":
                    lines = lines[:-1]
            sql = "\n".join(lines)
        sql = sql.rstrip(";").strip()
        if not sql.upper().startswith("SELECT") and not sql.upper().startswith("INSERT"):
            return {"sql": None, "_error": "AI did not return a SELECT or INSERT statement."}
        logger.info(f"Generated SQL: {sql[:200]}...")
        return {"sql": sql, "_error": None}
    except Exception as e:
        logger.error(f"SQL generation error: {e}")
        return {"sql": None, "_error": str(e)}
