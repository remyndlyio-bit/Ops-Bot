"""
Builds the LLM prompt and calls the LLM in JSON-only mode.
Returns a structured query plan only; no SQL, formulas, or executable code.
"""
import json
from typing import Dict, Any, List, Optional
from utils.logger import logger

# Relative time values the backend can resolve (see time_resolver.py)
RELATIVE_TIME_VALUES = (
    "last_quarter, this_quarter, last_month, this_month, "
    "ytd, this_year, last_7_days, last_30_days, last_90_days"
)


def build_query_plan_prompt(
    message: str,
    schema_description: str,
    allowed_columns: List[str],
    conversation_history: Optional[List[Dict[str, str]]] = None,
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

    return (
        "You are a query planner for a data assistant. You return ONLY a structured JSON query plan. "
        "Do NOT return SQL, Sheets formulas, or any executable code.\n\n"
        "SCHEMA (allowed columns from the sheet):\n"
        f"{schema_description}\n\n"
        f"Allowed column names (use exactly): {columns_list}\n\n"
        "ALLOWED METRICS: sum, avg, min, max, count\n\n"
        "TIME RANGES:\n"
        "- For relative periods use type \"relative\" and value one of: " + RELATIVE_TIME_VALUES + "\n"
        "- For a specific period use type \"absolute\" and value {\"start\": \"YYYY-MM-DD\", \"end\": \"YYYY-MM-DD\"}\n"
        "  (Do NOT compute dates yourself; use relative identifiers or leave to backend.)\n\n"
        "OUTPUT FORMAT (return ONLY this JSON, no other text):\n"
        "{\n"
        '  "sheet": "sheet1",\n'
        '  "metric": "sum" | "avg" | "min" | "max" | "count",\n'
        '  "column": "<column name from schema>",\n'
        '  "filters": { "<column>": "<value or null>" },\n'
        '  "time_range": { "type": "relative" | "absolute", "value": "<relative id or { start, end }>" },\n'
        '  "group_by": "<column name or null>",\n'
        '  "limit": number | null (optional; use for "top N" e.g. top 3 clients -> 3),\n'
        '  "confidence": "high" | "low",\n'
        '  "clarification_question": "optional; include when confidence is low"\n'
        "}\n\n"
        "RULES:\n"
        "- Map natural language to the schema: 'earnings', 'billing', 'income' -> sum on Fees (or the numeric column).\n"
        "- 'Top 3 clients', 'top five', 'top 10' -> set \"limit\" to that number (3, 5, 10) when using group_by.\n"
        "- 'Last quarter', 'Q2', '3 months ago' -> time_range type \"relative\", value \"last_quarter\" or \"last_90_days\".\n"
        "- 'This month', 'December' -> \"this_month\" or \"last_month\" or absolute range.\n"
        "- List clients / distinct values -> metric \"count\", group_by the dimension column (e.g. Client Name).\n"
        "- If unclear or ambiguous, set confidence to \"low\" and include \"clarification_question\" with a short question.\n"
        "- Only use columns from the schema. Only use metrics from the list.\n\n"
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
        )
        config = {"responseMimeType": "application/json", "temperature": 0, "maxOutputTokens": 1024}
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
