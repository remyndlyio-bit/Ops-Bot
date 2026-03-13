"""
Builds the LLM prompt and calls the LLM in JSON-only mode.
Returns a structured query plan only; no SQL, formulas, or executable code.
Time ranges are computed by the AI and returned as absolute start/end dates.
"""
import json
from datetime import date
from typing import Dict, Any, List, Optional
from utils.logger import logger


def build_query_plan_prompt(
    message: str,
    schema_description: str,
    allowed_columns: List[str],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    date_column: Optional[str] = None,
) -> str:
    """Build the prompt for the LLM to return a query plan only."""
    context_section = ""
    if conversation_history and len(conversation_history) > 0:
        lines = ["Recent conversation:"]
        for msg in conversation_history:
            role = "User" if msg.get("role") == "user" else "Assistant"
            lines.append(f"{role}: {msg.get('content', '')}")
        context_section = "\n".join(lines) + "\n\n"

    columns_list = ", ".join(sorted(allowed_columns)[:50])
    if len(allowed_columns) > 50:
        columns_list += f", ... ({len(allowed_columns)} columns total)"

    today = date.today().isoformat()

    return (
        "You are a query planner for a data assistant. You return ONLY a structured JSON query plan. "
        "Do NOT return SQL, Sheets formulas, or any executable code.\n\n"
        "TODAY'S DATE (use this to compute any time ranges): " + today + "\n\n"
        "SCHEMA (allowed columns from the sheet):\n"
        f"{schema_description}\n\n"
        f"Allowed column names (use exactly): {columns_list}\n\n"
        "ALLOWED METRICS: sum, avg, min, max, count, value\n\n"
        "- Use metric \"value\" when the user wants the CONTENT of one column for a specific row (e.g. \"What was it about?\", \"What job was that?\"). "
        "Set column to the most relevant column (Job, Notes, or similar) and use filters (and optional time_range) to identify the row. No aggregation.\n\n"
        "TIME RANGES (you must compute dates yourself):\n"
        "- When the user asks for a time period (e.g. last quarter, this month, last year, last 30 days), "
        "output time_range with type \"absolute\" and value {\"start\": \"YYYY-MM-DD\", \"end\": \"YYYY-MM-DD\"}. "
        "Compute start and end from TODAY'S DATE above (e.g. last quarter = Q of previous quarter; this month = first day to last day of current month).\n"
        "- When the user asks for \"all time\", \"overall\", \"total\", or no period is mentioned, use time_range: null.\n\n"
        "OUTPUT FORMAT (return ONLY this JSON, no other text):\n"
        "{\n"
        '  "sheet": "sheet1",\n'
        '  "metric": "sum" | "avg" | "min" | "max" | "count" | "value",\n'
        '  "column": "<column name from schema>",\n'
        '  "filters": { "<column>": "<value> or [\"value1\", \"value2\", ...] or null" },\n'
        '  "time_range": { "type": "absolute", "value": { "start": "YYYY-MM-DD", "end": "YYYY-MM-DD" } } | null,\n'
        '  "group_by": "<column name or null>",\n'
        '  "limit": number | null,\n'
        '  "order": "asc" | "desc" | null,\n'
        '  "offset": number | null,\n'
        '  "confidence": "high" | "low",\n'
        '  "clarification_question": "string | null"\n'
        "}\n\n"
        "OPTIONAL FIELDS (include only when user intent implies them):\n"
        "- limit: max number of rows/groups to return. Use for \"top N\", \"first N\", \"bottom N\", \"lowest N\" (e.g. top 3 -> 3).\n"
        "- order: sort order for grouped or listed results. \"desc\" = highest/biggest first (default for \"top\"); \"asc\" = lowest/smallest first (use for \"bottom\", \"lowest\", \"least\").\n"
        "- offset: number of rows/groups to skip from the start. Use for \"skip first 2\", \"after the top 3\", \"second page\", etc.\n"
        "- time_range: null when user asks for \"all time\", \"overall\", \"total\", or no period mentioned.\n\n"
        "RULES:\n"
        "- Map natural language to the schema: 'earnings', 'billing', 'income' -> sum on Fees (or the numeric column).\n"
        "- 'Top 3 clients', 'top five' -> limit: 3 or 5, order: \"desc\". 'Bottom 3', 'lowest 5', 'least paying' -> limit: 3 or 5, order: \"asc\".\n"
        "- 'Last quarter', 'Q2', '3 months ago' -> time_range type \"absolute\" with start/end computed from today (e.g. last quarter: previous quarter boundaries).\n"
        "- 'Last year', 'previous year', 'past year', 'total billing for last year' -> time_range type \"absolute\" with start (TODAY'S_YEAR - 1)-01-01 and end (TODAY'S_YEAR - 1)-12-31. Example: if today is 2025-02-02, last year is 2024-01-01 to 2024-12-31.\n"
        "- 'This month', 'last month', 'December', 'last 30 days' -> time_range type \"absolute\" with computed YYYY-MM-DD start and end.\n"
        "- 'When did I do my last gig', 'when was my last job', 'latest gig', 'most recent job', 'last gig date' -> metric \"max\", column \"" + (date_column or "Date") + "\" (the date column), time_range null. This returns the most recent date.\n"
        "- 'When was the job for [Client] started', 'when did the [Client] job start', 'date of the Garnier job' -> same idea: metric \"max\", column \"" + (date_column or "Date") + "\", time_range null, and set filters to that client (e.g. filters: { \"Client Name\": \"Garnier\" }). This returns the date of that client's job.\n"
        "- List clients / distinct values -> metric \"count\", group_by the dimension column (e.g. Client Name).\n"
        "- If unclear or ambiguous, set confidence to \"low\" and include \"clarification_question\" with a short question.\n"
        "- Only use columns from the schema. Only use metrics from the list. Omit optional fields (or set null) when not needed.\n\n"
        "CONTEXT (use Recent conversation when the user refers to prior messages):\n"
        "- When the user says \"sum of these\", \"total of those\", \"all these\", \"the above\", \"those clients\", \"these amounts\", etc., "
        "look at the Assistant's LAST message in the conversation. If it listed specific items (e.g. client names with amounts like \"7up – ₹8,000\"), "
        "the user means ONLY those items. Set \"filters\" so the query is restricted to them.\n"
        "- When the user asks \"What was it about?\", \"What job was that?\", \"Tell me more\", \"What was that gig?\" and the Assistant's LAST message was a date (e.g. \"Your last gig was on 04 Apr 2025.\" or \"The job for Garnier was on 04 Apr 2025\"), "
        "the user wants the JOB (or Notes) for that gig. Use metric \"value\", column = the Job/Project/Role column from the schema, and set filters to identify that row: "
        "include the date in YYYY-MM-DD (e.g. 04 Apr 2025 -> 2025-04-04) and the client name if it was mentioned in the previous user message (e.g. Garnier). "
        "Example: filters: { \"Date\": \"2025-04-04\", \"Client Name\": \"Garnier\" }, column: \"Job\", metric: \"value\", time_range: null.\n"
        "- For a SINGLE entity use: \"filters\": { \"<column>\": \"<value>\" } (e.g. client_name: \"7up\").\n"
        "- For MULTIPLE entities from the list use: \"filters\": { \"<column>\": [\"value1\", \"value2\", \"value3\"] } (e.g. client_name: [\"7up\", \"Xiaomi\", \"Kotak\"]). "
        "Extract the exact names from the Assistant's message (the part before the amount or the bullet text).\n"
        "- Recent conversation is in chronological order; the last Assistant message is what the user usually means by \"these\" or \"those\".\n\n"
        f"{context_section}"
        f"Current user message:\n{message}\n\n"
        "Return ONLY valid JSON."
    )


def get_query_plan(
    message: str,
    gemini_service: Any,
    schema_description: str,
    allowed_columns: List[str],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    date_column: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Call the LLM to get a query plan. Returns parsed dict or error dict.
    Does not validate; validation is done by query_validator.
    """
    try:
        prompt = build_query_plan_prompt(
            message=message,
            schema_description=schema_description,
            allowed_columns=allowed_columns,
            conversation_history=conversation_history,
            date_column=date_column,
        )
        config = {"responseMimeType": "application/json", "temperature": 0, "maxOutputTokens": 800}
        raw = gemini_service._call_api(prompt, generation_config=config)
        if not raw:
            return {"_error": "Empty response from LLM."}
        raw = raw.strip()
        if raw.startswith("```"):
            lines = raw.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            raw = "\n".join(lines)
        plan = json.loads(raw)
        logger.info(f"Query plan from LLM: {json.dumps(plan)[:300]}...")
        return plan
    except json.JSONDecodeError as e:
        logger.error(f"Query plan JSON parse error: {e}")
        return {"_error": f"Invalid JSON from LLM: {e}"}
    except Exception as e:
        logger.error(f"Query plan LLM error: {e}")
        return {"_error": str(e)}
