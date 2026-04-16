"""
Three-stage query planning system:
  Stage 1: Operation Classifier  (keyword + LLM fallback)
  Stage 2: Schema-Aware Planner  (LLM, dynamic columns only)
  Stage 3: Row Resolver           (deterministic, context-aware)
  + Column Validation
  + Plan → SQL conversion

Pipeline:
  User Message → Classify → Plan → Resolve Rows → Validate Columns → SQL → Execute
"""

import json
import re
from datetime import date
from typing import Dict, Any, List, Optional, Tuple
from utils.logger import logger


# ═══════════════════════════════════════════════════════════════════════════
# Stage 1 — Operation Classifier
# ═══════════════════════════════════════════════════════════════════════════

_CREATE_PATTERNS = [
    r"\badd\s+(?:a\s+)?(?:new\s+)?(?:job|entry|row)\b",
    r"\blog\s+(?:a\s+)?(?:job|entry)\b",
    r"\brecord\s+(?:a\s+)?(?:job|entry)\b",
    r"\bcreate\s+(?:a\s+)?(?:new\s+)?(?:job|entry|row)\b",
    r"\bnew\s+(?:job|entry)\b",
]

_UPDATE_PATTERNS = [
    r"\bmark\s+(?:as\s+)?paid\b",
    r"\bpayment\b.{0,40}\b(?:received|done|completed)\b",
    r"\breceived\s+payment\b",
    r"\bupdate\s+(?:the\s+)?(?:fees|email|contact|status|paid)\b",
    r"\bset\s+(?:the\s+)?(?:email|contact|fees|paid|status)\b",
    r"\bchange\s+(?:the\s+)?(?:fees|email|contact|status)\b",
    r"\bmodify\b",
    r"\badd\s+\S+@\S+\s+as\b",
]

_QUERY_PATTERNS = [
    r"\b(?:how\s+many|how\s+much|total|count|sum|average)\b",
    r"\b(?:show|list|get|fetch|what|when|which|who)\b",
    r"\b(?:latest|last|most\s+recent|top|bottom)\b",
    r"\?$",
]


def classify_operation(
    message: str,
    gemini_service=None,
    conversation_history: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, str]:
    """
    Stage 1: Lightweight operation classifier.
    Returns {"operation": "query"|"update"|"create", "confidence": "high"|"low"}.
    Keyword-first for speed; LLM fallback for ambiguous messages.
    """
    msg = message.strip().lower()

    for pat in _CREATE_PATTERNS:
        if re.search(pat, msg):
            return {"operation": "create", "confidence": "high"}
    if msg.startswith("+") and len(msg) > 1:
        return {"operation": "create", "confidence": "high"}

    for pat in _UPDATE_PATTERNS:
        if re.search(pat, msg):
            return {"operation": "update", "confidence": "high"}

    for pat in _QUERY_PATTERNS:
        if re.search(pat, msg):
            return {"operation": "query", "confidence": "high"}

    # LLM fallback for ambiguous messages
    if gemini_service:
        try:
            ctx = ""
            if conversation_history:
                lines = []
                for m in conversation_history[-4:]:
                    role = "User" if m.get("role") == "user" else "Assistant"
                    lines.append(f"{role}: {m.get('content', '')}")
                ctx = "\nRecent conversation:\n" + "\n".join(lines)

            prompt = (
                "Classify this user message into ONE operation type.\n"
                "Operations:\n"
                '- "query": retrieve/read data (questions, lookups, aggregations)\n'
                '- "update": modify existing data (mark paid, change fees, add email)\n'
                '- "create": add new data (add job, log entry, new row)\n\n'
                f"{ctx}\n"
                f"User message: {message}\n\n"
                'Return ONLY JSON: {{"operation":"query"|"update"|"create","confidence":"high"|"low"}}'
            )
            raw = gemini_service._call_api(prompt, generation_config={
                "responseMimeType": "application/json",
                "temperature": 0,
                "maxOutputTokens": 100,
            })
            if raw:
                result = json.loads(raw.strip())
                op = result.get("operation", "query")
                if op in ("query", "update", "create"):
                    logger.info(f"[CLASSIFIER] LLM classified as: {op}")
                    return result
        except Exception as e:
            logger.warning(f"[CLASSIFIER] LLM fallback failed: {e}")

    return {"operation": "query", "confidence": "low"}


# ═══════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════

def _detect_date_column(schema_description: str, allowed_columns: List[str]) -> Optional[str]:
    """Find the primary date column from schema description."""
    for line in schema_description.split("\n"):
        m = re.match(r"^-\s*(\w+)\s*\(date\)", line.strip())
        if m and m.group(1) in allowed_columns:
            return m.group(1)
    for candidate in ["job_date", "date", "created_at"]:
        if candidate in allowed_columns:
            return candidate
    return None


def _strip_markdown_json(raw: str) -> str:
    """Strip markdown code fences from LLM output."""
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.split("\n")
        if lines[0].startswith("```"):
            lines = lines[1:]
        if lines and lines[-1].strip() == "```":
            lines = lines[:-1]
        raw = "\n".join(lines)
    return raw


# ═══════════════════════════════════════════════════════════════════════════
# Stage 2 — Schema-Aware Operation Planner
# ═══════════════════════════════════════════════════════════════════════════

def _precompute_time_ranges() -> str:
    """Compute common time ranges so the AI doesn't have to do date math."""
    from datetime import timedelta
    today = date.today()

    # This month
    this_month_start = today.replace(day=1)

    # Last month
    last_month_end = this_month_start - timedelta(days=1)
    last_month_start = last_month_end.replace(day=1)

    # This quarter
    current_q_start_month = ((today.month - 1) // 3) * 3 + 1
    this_q_start = date(today.year, current_q_start_month, 1)

    # Last quarter
    if current_q_start_month == 1:
        last_q_start = date(today.year - 1, 10, 1)
        last_q_end = date(today.year - 1, 12, 31)
    else:
        lq_start_month = current_q_start_month - 3
        last_q_start = date(today.year, lq_start_month, 1)
        last_q_end = this_q_start - timedelta(days=1)

    # This year
    this_year_start = date(today.year, 1, 1)

    # Last year
    last_year_start = date(today.year - 1, 1, 1)
    last_year_end = date(today.year - 1, 12, 31)

    return (
        f"- 'this month' → start: {this_month_start.isoformat()}, end: {today.isoformat()}\n"
        f"- 'last month' → start: {last_month_start.isoformat()}, end: {last_month_end.isoformat()}\n"
        f"- 'this quarter' → start: {this_q_start.isoformat()}, end: {today.isoformat()}\n"
        f"- 'last quarter' → start: {last_q_start.isoformat()}, end: {last_q_end.isoformat()}\n"
        f"- 'this year' → start: {this_year_start.isoformat()}, end: {today.isoformat()}\n"
        f"- 'last year' → start: {last_year_start.isoformat()}, end: {last_year_end.isoformat()}\n"
    )


def _build_planner_prompt(
    message: str,
    operation: str,
    schema_description: str,
    allowed_columns: List[str],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    date_column: Optional[str] = None,
) -> str:
    today = date.today().isoformat()
    columns_list = ", ".join(sorted(allowed_columns)[:50])
    precomputed_ranges = _precompute_time_ranges()

    context_section = ""
    if conversation_history:
        lines = ["Recent conversation:"]
        for msg in conversation_history[-10:]:
            role = "User" if msg.get("role") == "user" else "Assistant"
            lines.append(f"{role}: {msg.get('content', '')}")
        context_section = "\n".join(lines) + "\n\n"

    op_guidance = {
        "query": (
            "OPERATION TYPE: QUERY (retrieve/read data)\n"
            "- Set metric to sum/avg/min/max/count/value as appropriate.\n"
            "- Set column to the target column for the metric.\n"
            "- Set filters for any WHERE conditions.\n"
            "- Set time_range for date-based filtering.\n"
            "- Set group_by for grouped results ('by client', 'per brand').\n"
            "- Set limit/order for top-N or bottom-N queries.\n"
            "- updates and values must be null.\n\n"
        ),
        "update": (
            "OPERATION TYPE: UPDATE (modify existing rows)\n"
            "- Set updates to {column: new_value} for fields to change.\n"
            "- Set filters to identify WHICH row(s) to update.\n"
            "- Use conversation context to resolve implicit references.\n"
            "- metric, column, group_by, values must be null.\n\n"
        ),
        "create": (
            "OPERATION TYPE: CREATE (insert new row)\n"
            "- Set values to {column: value} for the new row.\n"
            "- Extract all mentioned fields from the user message.\n"
            "- metric, column, group_by, updates must be null.\n\n"
        ),
    }

    dc = date_column or "job_date"

    return (
        "You are a DATA OPERATION PLANNER for a spreadsheet or database assistant.\n\n"
        "The schema may change dynamically between deployments.\n"
        "You MUST ONLY use columns provided in the schema description.\n"
        "Do NOT assume fixed column names like 'Fees' or 'Client Name'.\n"
        "Instead, infer the correct column using the schema descriptions.\n"
        "Return ONLY a structured JSON plan. Do NOT generate SQL.\n\n"
        f"TODAY'S DATE: {today}\n\n"
        f"SCHEMA:\n{schema_description}\n\n"
        f"ALLOWED COLUMNS (use exactly): {columns_list}\n\n"
        "ALLOWED METRICS: sum, avg, min, max, count, value\n\n"
        f"{op_guidance.get(operation, op_guidance['query'])}"
        "SEMANTIC COLUMN MAPPING:\n"
        "- 'earnings', 'billing', 'revenue', 'income' → the numeric payment/fees column.\n"
        "- 'client', 'brand', 'company' → the client/brand name column.\n"
        "- 'job', 'work', 'project', 'gig' → the job description column.\n"
        "- 'contact', 'email' → the contact/email column.\n"
        "- 'paid', 'payment' status → the payment status column.\n"
        "- NEVER invent column names not in the schema.\n\n"
        "TIME RANGES (use these EXACT dates — do NOT compute your own):\n"
        f"- Date column: '{dc}'\n"
        f"{precomputed_ranges}"
        "- 'all time', 'overall', no period → time_range: null.\n"
        "- 'latest', 'most recent', 'last job' → no time_range; use limit:1, order:'desc'.\n\n"
        "CONTEXT RESOLUTION:\n"
        "- 'this job', 'that client', 'these' → resolve from recent conversation.\n"
        "- 'sum of these', 'total of those' → extract items from assistant's last message.\n"
        "- If assistant showed a date, and user asks 'what was it about?', use metric 'value' "
        "on the job description column with date as filter.\n\n"
        "OUTPUT FORMAT (return ONLY this JSON):\n"
        "{\n"
        f'  "operation": "{operation}",\n'
        '  "sheet": "sheet1",\n'
        '  "metric": "sum"|"avg"|"min"|"max"|"count"|"value"|null,\n'
        '  "column": "<column from allowed_columns>"|null,\n'
        '  "filters": {"<column>": "<value>"|["v1","v2"]}|null,\n'
        '  "updates": {"<column>": "<new_value>"}|null,\n'
        '  "values": {"<column>": "<value>"}|null,\n'
        '  "time_range": {"type":"absolute","value":{"start":"YYYY-MM-DD","end":"YYYY-MM-DD"}}|null,\n'
        '  "group_by": "<column>"|null,\n'
        '  "limit": number|null,\n'
        '  "order": "asc"|"desc"|null,\n'
        '  "offset": number|null,\n'
        '  "confidence": "high"|"low",\n'
        '  "clarification_question": "string"|null\n'
        "}\n\n"
        "LANGUAGE: Users may write in English, Hindi (Devanagari), Roman Hindi, or Hinglish. "
        "Understand all. Examples: 'pichle mahine ki kamai' = last month earnings (metric:sum, column:fees, time_range:last month), "
        "'kitne client hain' = how many clients (metric:count, group_by:client_name). Always output JSON in English.\n\n"
        "RULES:\n"
        "- Only use columns from the schema. Only use metrics from the list.\n"
        "- 'Top N' → limit:N, order:'desc'. 'Bottom N' → limit:N, order:'asc'.\n"
        "- 'list all', 'show all', 'give all', 'view all', 'all records', 'all jobs', 'all entries' → set metric:null AND column:null (produces SELECT *).\n"
        "- If unclear, set confidence:'low' and include clarification_question.\n"
        "- For update: 'mark as paid' → updates: {paid_column: 'true'}.\n"
        "- For create: extract ALL mentioned fields into values dict.\n"
        "- 'k' means thousands (25k=25000), 'L'/'lac' means 100000 (1.5L=150000).\n\n"
        f"{context_section}"
        f"User message: {message}\n\n"
        "Return ONLY valid JSON."
    )


def build_operation_plan(
    message: str,
    operation: str,
    schema_description: str,
    allowed_columns: List[str],
    conversation_history: Optional[List[Dict[str, str]]] = None,
    date_column: Optional[str] = None,
    gemini_service=None,
) -> Dict[str, Any]:
    """Stage 2: Call LLM to produce structured operation plan."""
    if not gemini_service:
        return {"_error": "Gemini service not available."}

    prompt = _build_planner_prompt(
        message, operation, schema_description, allowed_columns,
        conversation_history, date_column,
    )
    try:
        raw = gemini_service._call_api(prompt, generation_config={
            "responseMimeType": "application/json",
            "temperature": 0,
            "maxOutputTokens": 800,
        })
        if not raw:
            return {"_error": "Empty response from LLM."}
        raw = _strip_markdown_json(raw)
        plan = json.loads(raw)
        plan["operation"] = operation
        logger.info(f"[PLANNER] Plan: {json.dumps(plan)[:300]}")
        return plan
    except json.JSONDecodeError as e:
        logger.error(f"[PLANNER] JSON parse error: {e}")
        return {"_error": f"Invalid JSON from LLM: {e}"}
    except Exception as e:
        logger.error(f"[PLANNER] LLM error: {e}")
        return {"_error": str(e)}


# ═══════════════════════════════════════════════════════════════════════════
# Stage 3 — Row Resolver
# ═══════════════════════════════════════════════════════════════════════════

def resolve_rows(
    plan: Dict[str, Any],
    user_id: str,
    supabase_service,
    conversation_context: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Stage 3: Deterministic row resolver for implicit references.
    Adds/refines filters when the plan targets specific rows but lacks identifiers.

    Resolution order:
      1. Conversation context (last_row_data / last_saved_job)
      2. Explicit entity already in filters (keep as-is)
      3. Fallback: target most recent row
    """
    op = plan.get("operation")
    if op == "query":
        return plan

    filters = plan.get("filters") or {}
    has_identifying_filter = any(
        k for k in filters if not k.startswith("_")
    )

    if has_identifying_filter:
        plan["filters"] = filters
        return plan

    if op in ("update",) and not has_identifying_filter:
        if conversation_context:
            last_row = conversation_context.get("last_row_data")
            last_job = conversation_context.get("last_saved_job")

            if last_row:
                if last_row.get("id"):
                    filters["id"] = str(last_row["id"])
                    logger.info(f"[ROW_RESOLVER] Resolved to id={filters['id']} from last_row_data")
                elif last_row.get("client_name"):
                    filters["client_name"] = last_row["client_name"]
                    if last_row.get("job_date"):
                        filters["job_date"] = str(last_row["job_date"])[:10]
                    logger.info(f"[ROW_RESOLVER] Resolved from last_row_data: {filters}")
            elif last_job:
                if last_job.get("db_client_name"):
                    filters["client_name"] = last_job["db_client_name"]
                elif last_job.get("brand_name"):
                    filters["client_name"] = last_job["brand_name"]
                if last_job.get("job_date"):
                    filters["job_date"] = str(last_job["job_date"])[:10]
                logger.info(f"[ROW_RESOLVER] Resolved from last_saved_job: {filters}")

        if not any(k for k in filters if not k.startswith("_")):
            logger.info("[ROW_RESOLVER] No context; will target most recent row")
            filters["_resolve_latest"] = True

        plan["filters"] = filters

    return plan


# ═══════════════════════════════════════════════════════════════════════════
# Column Validation
# ═══════════════════════════════════════════════════════════════════════════

def validate_plan_columns(
    plan: Dict[str, Any],
    allowed_columns: List[str],
) -> Tuple[bool, List[str]]:
    """
    Verify all column references in the plan exist in allowed_columns.
    Returns (valid, list_of_error_strings).
    """
    errors: List[str] = []
    allowed_set = set(allowed_columns)

    col = plan.get("column")
    if col and col not in allowed_set:
        errors.append(f"column '{col}'")

    gb = plan.get("group_by")
    if gb and gb not in allowed_set:
        errors.append(f"group_by '{gb}'")

    for key in (plan.get("filters") or {}):
        if key.startswith("_"):
            continue
        if key not in allowed_set:
            errors.append(f"filter '{key}'")

    for key in (plan.get("updates") or {}):
        if key not in allowed_set:
            errors.append(f"update '{key}'")

    for key in (plan.get("values") or {}):
        if key not in allowed_set:
            errors.append(f"value '{key}'")

    if errors:
        logger.warning(f"[VALIDATE] Unknown columns: {errors}")
    return len(errors) == 0, errors


# ═══════════════════════════════════════════════════════════════════════════
# Plan → SQL
# ═══════════════════════════════════════════════════════════════════════════

def _sql_quote(value) -> str:
    """Safely quote a value for SQL embedding."""
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "'true'" if value else "'false'"
    if isinstance(value, (int, float)):
        return str(value)
    s = str(value).replace("'", "''")
    return f"'{s}'"


def _is_numeric(val) -> bool:
    """Check if a value looks numeric."""
    try:
        float(str(val).replace(",", ""))
        return True
    except (ValueError, TypeError):
        return False


def _is_date(val) -> bool:
    """Check if a value looks like an ISO date."""
    return bool(re.match(r"^\d{4}-\d{2}-\d{2}$", str(val).strip()))


def _build_filter_clause(col: str, val, use_ilike: bool = True) -> str:
    """Build a single filter condition."""
    if isinstance(val, list):
        quoted = ", ".join(_sql_quote(v) for v in val)
        return f"{col} IN ({quoted})"
    if _is_numeric(val):
        return f"{col} = {val}"
    if _is_date(val):
        return f"{col} = {_sql_quote(val)}"
    if use_ilike:
        return f"{col} ILIKE {_sql_quote(val)}"
    return f"{col} = {_sql_quote(val)}"


def plan_to_sql(
    plan: Dict[str, Any],
    user_id: str,
    date_column: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Deterministic conversion of a validated plan to SQL.
    Returns {"sql": "...", "_error": None} or {"sql": None, "_error": "..."}.
    """
    op = plan.get("operation", "query")
    try:
        if op == "query":
            return _build_select(plan, user_id, date_column)
        elif op == "update":
            return _build_update(plan, user_id, date_column)
        elif op == "create":
            return _build_insert(plan, user_id)
        else:
            return {"sql": None, "_error": f"Unknown operation: {op}"}
    except Exception as e:
        logger.error(f"[PLAN_TO_SQL] Error: {e}")
        return {"sql": None, "_error": str(e)}


def _time_range_conditions(plan: Dict, dc: str) -> List[str]:
    """Extract time_range into WHERE conditions."""
    parts: List[str] = []
    tr = plan.get("time_range")
    if tr and isinstance(tr, dict):
        val = tr.get("value") or {}
        if val.get("start"):
            parts.append(f"{dc} >= {_sql_quote(val['start'])}")
        if val.get("end"):
            parts.append(f"{dc} <= {_sql_quote(val['end'])}")
    return parts


def _build_select(plan: Dict, user_id: str, date_column: Optional[str]) -> Dict[str, Any]:
    metric = plan.get("metric")
    column = plan.get("column")
    group_by = plan.get("group_by")
    limit = plan.get("limit")
    order = plan.get("order")
    offset = plan.get("offset")
    dc = date_column or "job_date"
    filters = plan.get("filters") or {}

    # SELECT clause
    if group_by:
        if metric and metric not in ("value", "count") and column:
            select = f"{group_by}, {metric.upper()}({column}) AS result"
        else:
            select = f"{group_by}, COUNT(*) AS result"
    elif metric == "count":
        select = "COUNT(*) AS result"
    elif metric in ("sum", "avg", "min", "max") and column:
        select = f"{metric.upper()}({column}) AS result"
    elif metric == "value" and column:
        select = column
    else:
        select = "*"

    # WHERE clause
    where = [f"user_id = {_sql_quote(user_id)}", '("isDeleted" IS NOT TRUE)']
    for col, val in filters.items():
        if col.startswith("_"):
            continue
        where.append(_build_filter_clause(col, val))
    where.extend(_time_range_conditions(plan, dc))
    where_str = " AND ".join(where)

    sql = f"SELECT {select} FROM public.job_entries WHERE {where_str}"

    if group_by:
        sql += f" GROUP BY {group_by}"

    if order:
        order_col = "result" if (metric and metric not in ("value",)) else (column or dc)
        sql += f" ORDER BY {order_col} {order.upper()}"
    elif not group_by and (metric == "value" or not metric):
        sql += f" ORDER BY {dc} DESC"

    if limit:
        sql += f" LIMIT {int(limit)}"
    elif not group_by and (not metric or metric == "value"):
        sql += " LIMIT 50"

    if offset:
        sql += f" OFFSET {int(offset)}"

    return {"sql": sql, "_error": None}


def _build_update(plan: Dict, user_id: str, date_column: Optional[str]) -> Dict[str, Any]:
    updates = plan.get("updates") or {}
    if not updates:
        return {"sql": None, "_error": "No updates specified in plan."}

    dc = date_column or "job_date"
    filters = dict(plan.get("filters") or {})
    resolve_latest = filters.pop("_resolve_latest", False)

    set_parts = [f"{col} = {_sql_quote(val)}" for col, val in updates.items()]
    set_clause = ", ".join(set_parts)

    where = [f"user_id = {_sql_quote(user_id)}", '("isDeleted" IS NOT TRUE)']
    for col, val in filters.items():
        if col.startswith("_"):
            continue
        where.append(_build_filter_clause(col, val))
    where.extend(_time_range_conditions(plan, dc))
    where_str = " AND ".join(where)

    if resolve_latest:
        sql = (
            f"UPDATE public.job_entries SET {set_clause} "
            f"WHERE id = (SELECT id FROM public.job_entries WHERE {where_str} "
            f"ORDER BY {dc} DESC LIMIT 1) "
            f"RETURNING *"
        )
    else:
        sql = f"UPDATE public.job_entries SET {set_clause} WHERE {where_str} RETURNING *"

    return {"sql": sql, "_error": None}


def _build_insert(plan: Dict, user_id: str) -> Dict[str, Any]:
    values = plan.get("values") or {}
    if not values:
        return {"sql": None, "_error": "No values specified for insert."}

    values["user_id"] = user_id
    cols = list(values.keys())
    vals = [_sql_quote(values[c]) for c in cols]

    sql = (
        f"INSERT INTO public.job_entries ({', '.join(cols)}) "
        f"VALUES ({', '.join(vals)}) RETURNING *"
    )
    return {"sql": sql, "_error": None}


# ═══════════════════════════════════════════════════════════════════════════
# Orchestrator
# ═══════════════════════════════════════════════════════════════════════════

def execute_query_plan(
    message: str,
    gemini_service,
    supabase_service,
    conversation_history: Optional[List[Dict[str, str]]] = None,
    user_id: Optional[str] = None,
    conversation_context: Optional[Dict] = None,
) -> Dict[str, Any]:
    """
    Full pipeline: Classify → Plan → Resolve → Validate → SQL.
    Returns {
        "sql": str | None,
        "plan": dict | None,
        "classification": dict,
        "clarification": str | None,
        "_error": str | None,
    }
    """
    schema = supabase_service.get_schema()
    schema_description = schema["description"]
    allowed_columns = schema["columns"]
    date_column = _detect_date_column(schema_description, allowed_columns)

    # Stage 1: Classify
    classification = classify_operation(message, gemini_service, conversation_history)
    operation = classification["operation"]
    logger.info(
        f"[PIPELINE] Stage 1: {operation} (confidence: {classification['confidence']})"
    )

    # Stage 2: Plan
    plan = build_operation_plan(
        message, operation, schema_description, allowed_columns,
        conversation_history, date_column, gemini_service,
    )
    if plan.get("_error"):
        return {
            "sql": None, "plan": plan, "classification": classification,
            "clarification": None, "_error": plan["_error"],
        }

    if plan.get("confidence") == "low" and plan.get("clarification_question"):
        return {
            "sql": None, "plan": plan, "classification": classification,
            "clarification": plan["clarification_question"], "_error": None,
        }

    # Stage 3: Row Resolver
    plan = resolve_rows(plan, user_id, supabase_service, conversation_context)
    logger.info(f"[PIPELINE] Stage 3 resolved: {json.dumps(plan)[:200]}")

    # Column Validation
    valid, errors = validate_plan_columns(plan, allowed_columns)
    if not valid:
        return {
            "sql": None, "plan": plan, "classification": classification,
            "clarification": None,
            "_error": f"Invalid columns: {', '.join(errors)}",
        }

    # Plan → SQL
    sql_result = plan_to_sql(plan, user_id, date_column)
    if sql_result.get("_error"):
        return {
            "sql": None, "plan": plan, "classification": classification,
            "clarification": None, "_error": sql_result["_error"],
        }

    sql = sql_result["sql"]
    logger.info(f"[PIPELINE] SQL: {sql[:200]}")

    return {
        "sql": sql,
        "plan": plan,
        "classification": classification,
        "clarification": None,
        "_error": None,
    }
