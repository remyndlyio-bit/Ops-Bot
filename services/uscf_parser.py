"""
Universal Spreadsheet Command Framework (USCF) Parser.
Converts natural language to structured JSON commands.

Operations: create, update, query, delete
Modes (for updates): set, increase, decrease, append, clear, increase_percent, decrease_percent
"""
import json
from datetime import date
from typing import Dict, Any, List, Optional
from utils.logger import logger


def build_uscf_prompt(
    message: str,
    columns: List[str],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> str:
    """Build the prompt for the AI to output USCF JSON."""
    today = date.today().isoformat()

    # Build conversation context
    context_lines = []
    if conversation_history:
        for msg in conversation_history[-6:]:  # Last 3 exchanges
            role = "User" if msg.get("role") == "user" else "Assistant"
            context_lines.append(f"{role}: {msg.get('content', '')}")
    conv_block = "\n".join(context_lines) if context_lines else "(no prior messages)"

    # Build session context (current entity being discussed, etc.)
    session_ctx = ""
    if context:
        ctx_parts = []
        if context.get("current_entity"):
            ctx_parts.append(f"Current entity: {context['current_entity']}")
        if context.get("current_filters"):
            ctx_parts.append(f"Current filters: {json.dumps(context['current_filters'])}")
        if context.get("last_result_date"):
            ctx_parts.append(f"Last mentioned date: {context['last_result_date']}")
        if context.get("last_result_client"):
            ctx_parts.append(f"Last mentioned client: {context['last_result_client']}")
        if ctx_parts:
            session_ctx = "Session context:\n" + "\n".join(ctx_parts) + "\n\n"

    columns_str = ", ".join(columns[:30])

    return f"""You are a Spreadsheet Command Interpreter.

Your job is to convert natural language into structured JSON commands.
Return ONLY valid JSON. No explanations.

TODAY'S DATE: {today}

AVAILABLE COLUMNS: {columns_str}

SUPPORTED OPERATIONS:
- create: Add a new row
- update: Modify existing row(s)
- query: Read/search data (counts, sums, filters, lookups)
- delete: Remove row(s)

UPDATE MODES:
- set: Replace value
- increase: Add to numeric value
- decrease: Subtract from numeric value
- increase_percent: Increase by percentage
- decrease_percent: Decrease by percentage
- append: Add text to existing (for notes)
- clear: Set to empty

QUERY TYPES (for operation=query):
- metric: count, sum, avg, min, max, value (single cell lookup)
- return_fields: which columns to return
- group_by: group results by column
- order: asc or desc
- limit: max results

OUTPUT SCHEMA:

For CREATE:
{{"operation": "create", "data": {{"column": "value", ...}}}}

For UPDATE:
{{"operation": "update", "filters": {{"column": "value"}}, "updates": [{{"field": "column", "mode": "set|increase|decrease|append|clear", "value": ...}}]}}

For QUERY:
{{"operation": "query", "filters": {{"column": "value"}}, "metric": "count|sum|avg|min|max|value", "column": "column_name", "return_fields": ["col1", "col2"], "group_by": "column", "order": "desc", "limit": 5, "time_range": {{"start": "YYYY-MM-DD", "end": "YYYY-MM-DD"}} }}

For DELETE:
{{"operation": "delete", "filters": {{"column": "value"}}}}

RULES:
1. Use ONLY columns from the list above.
2. For dates: compute from TODAY'S DATE. "last month" = first to last day of previous month. "last year" = {int(today[:4])-1}-01-01 to {int(today[:4])-1}-12-31.
3. Smart number parsing: "2k"=2000, "₹5000"=5000, "fifty"=50.
4. Context resolution: If user says "update it", "change that", "make it 2000", use conversation/session context to infer the entity and filters.
5. Implicit field detection: "mark as paid" → update status/paid column. "make it urgent" → update status.
6. For "how many jobs", "total billing", "count of clients" → use query with appropriate metric.
7. For "when was my last gig", "latest job date" → query with metric=max on date column.
8. For "what was that job about" → query with metric=value on job/notes column, using context filters.
9. If ambiguous, prefer the most reasonable interpretation. Don't ask for clarification unless truly impossible.
10. CRITICAL: Do NOT add default date filters (like today or yesterday) unless the user explicitly mentions a date. If no date is specified, use empty filters {{}} or time_range: null.
11. For follow-up questions like "And the brand name?" that reference a previous result, use the context filters from the session context if available.

{session_ctx}Conversation history:
{conv_block}

User message: {message}

JSON command:"""


def parse_uscf_command(
    message: str,
    gemini_service: Any,
    columns: List[str],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Call AI to parse user message into USCF command.
    Returns parsed dict or {"_error": "..."} on failure.
    """
    try:
        prompt = build_uscf_prompt(message, columns, conversation_history, context)
        config = {"responseMimeType": "application/json", "temperature": 0, "maxOutputTokens": 1024}
        raw = gemini_service._call_api(prompt, generation_config=config)
        if not raw:
            return {"_error": "Empty response from AI."}
        raw = raw.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            lines = raw.split("\n")
            if lines[0].startswith("```"):
                lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            raw = "\n".join(lines)
        cmd = json.loads(raw)
        logger.info(f"USCF command: {json.dumps(cmd)[:400]}...")
        return cmd
    except json.JSONDecodeError as e:
        logger.error(f"USCF JSON parse error: {e}")
        return {"_error": f"Invalid JSON from AI: {e}"}
    except Exception as e:
        logger.error(f"USCF parse error: {e}")
        return {"_error": str(e)}
