#!/usr/bin/env python3
"""
Live end-to-end tests using the real OpenRouter/Gemini API.

30 test cases across 10 categories — each validates the full pipeline:
  User message
    → execute_query_plan()               [Classify → Plan → SQL]
    → MockSupabaseService.execute_sql()  [realistic in-memory dataset]
    → build_clean_payload()
    → GeminiService.synthesize_response()
    → response quality assertions

Smart-capture cases use extract_job_fields() directly.

Usage:
    AI_KEY=sk-or-v1-... python3 tests/test_e2e_live.py
    AI_KEY=... E2E_MAX_TOKENS=800 python3 tests/test_e2e_live.py
"""

import os
import re
import sys
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

AI_KEY = os.environ.get("AI_KEY", "").strip()
if not AI_KEY:
    print("ERROR: Set AI_KEY env variable before running.\n"
          "  AI_KEY=sk-or-v1-... python3 tests/test_e2e_live.py")
    sys.exit(1)
os.environ["AI_KEY"] = AI_KEY
os.environ["STRICT_PLAN_VALIDATION"] = "1"

from services.gemini_service import GeminiService
from services.query_planner import execute_query_plan
from services.response_synthesis import build_clean_payload
from services.supabase_service import JOB_ENTRIES_COLUMNS, SCHEMA_DESCRIPTION

# Cap max_tokens per call — keeps budget-constrained keys working.
# Planner JSON is 200-500 tokens; synthesis is 50-150. 700 covers both.
_MAX_TOKENS_OVERRIDE = int(os.environ.get("E2E_MAX_TOKENS", "700"))

_original_call_api = GeminiService._call_api
def _patched_call_api(self, prompt, generation_config=None):
    gc = dict(generation_config or {})
    gc["maxOutputTokens"] = min(gc.get("maxOutputTokens", _MAX_TOKENS_OVERRIDE), _MAX_TOKENS_OVERRIDE)
    return _original_call_api(self, prompt, gc)
GeminiService._call_api = _patched_call_api

GREEN  = "\033[92m"; RED    = "\033[91m"
YELLOW = "\033[93m"; CYAN   = "\033[96m"
BOLD   = "\033[1m";  RESET  = "\033[0m"

TEST_USER_ID = "test_e2e_user_001"

# ─────────────────────────────────────────────────────────────────────────────
# Mock dataset — 8 rows covering all test scenarios
# ─────────────────────────────────────────────────────────────────────────────

MOCK_ROWS: List[Dict] = [
    {
        "id": 1, "user_id": TEST_USER_ID,
        "client_name": "Star Studios", "brand_name": "Nike",
        "job_date": "2026-03-15", "fees": 150000,
        "paid": "Yes", "bill_sent": "Yes",
        "poc_email": "contact@starstudios.com",
        "invoice_date": "2026-03-20", "bill_no": "INV-001",
        "job_description_details": "TVC 30sec + 4 cutdowns", "isDeleted": None,
    },
    {
        "id": 2, "user_id": TEST_USER_ID,
        "client_name": "Star Studios", "brand_name": "Adidas",
        "job_date": "2026-01-10", "fees": 200000,
        "paid": None, "bill_sent": "Yes",
        "poc_email": "contact@starstudios.com",
        "invoice_date": "2026-01-15", "bill_no": "INV-002",
        "job_description_details": "Brand film 60sec", "isDeleted": None,
    },
    {
        "id": 3, "user_id": TEST_USER_ID,
        "client_name": "Garnier India", "brand_name": "Garnier",
        "job_date": "2026-02-20", "fees": 80000,
        "paid": "Yes", "bill_sent": "Yes",
        "poc_email": "priya@garnier.com",
        "invoice_date": "2026-02-25", "bill_no": "INV-003",
        "job_description_details": "Product shoot", "isDeleted": None,
    },
    {
        "id": 4, "user_id": TEST_USER_ID,
        "client_name": "Pedigree Films", "brand_name": "Pedigree",
        "job_date": "2026-04-05", "fees": 120000,
        "paid": None, "bill_sent": None,
        "poc_email": None,
        "invoice_date": None, "bill_no": "INV-004",
        "job_description_details": "Pet food TVC", "isDeleted": None,
    },
    {
        "id": 5, "user_id": TEST_USER_ID,
        "client_name": "Garnier India", "brand_name": "L'Oréal",
        "job_date": "2026-05-08", "fees": 95000,
        "paid": None, "bill_sent": "Yes",
        "poc_email": "priya@garnier.com",
        "invoice_date": "2026-05-10", "bill_no": "INV-005",
        "job_description_details": "Skincare campaign", "isDeleted": None,
    },
    {
        "id": 6, "user_id": TEST_USER_ID,
        "client_name": "Samsung India", "brand_name": "Samsung",
        "job_date": "2026-06-10", "fees": 300000,
        "paid": None, "bill_sent": None,
        "poc_email": None,
        "invoice_date": None, "bill_no": "INV-006",
        "job_description_details": "Galaxy launch film", "isDeleted": None,
    },
    {
        "id": 7, "user_id": TEST_USER_ID,
        "client_name": "Garnier India", "brand_name": "Garnier Men",
        "job_date": "2026-03-01", "fees": 55000,
        "paid": None, "bill_sent": None,
        "poc_email": None,
        "invoice_date": None, "bill_no": "INV-007",
        "job_description_details": "Skincare product shoot", "isDeleted": None,
    },
    {
        "id": 8, "user_id": TEST_USER_ID,
        "client_name": "Maruti Suzuki", "brand_name": "Maruti",
        "job_date": "2026-02-05", "fees": 175000,
        "paid": "Yes", "bill_sent": "Yes",
        "poc_email": "ads@maruti.co.in",
        "invoice_date": "2026-02-10", "bill_no": "INV-008",
        "job_description_details": "Car launch TVC", "isDeleted": None,
    },
]


def _client_name(row: Dict) -> str:
    return row.get("client_name") or row.get("brand_name") or row.get("production_house") or ""


def _is_unpaid(row: Dict) -> bool:
    paid = (row.get("paid") or "").lower()
    return paid not in ("yes", "true", "1", "paid", "t")


def _bill_sent(row: Dict) -> bool:
    bs = (row.get("bill_sent") or "").lower()
    return bs in ("yes", "true", "1", "sent", "t")


# ─────────────────────────────────────────────────────────────────────────────
# Mock SupabaseService
# ─────────────────────────────────────────────────────────────────────────────

class MockSupabaseService:
    def get_schema(self) -> Dict[str, Any]:
        return {
            "table": "job_entries",
            "schema_name": "public",
            "columns": JOB_ENTRIES_COLUMNS,
            "description": SCHEMA_DESCRIPTION.strip(),
        }

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        su = sql.upper()

        def _extract_client(sql: str) -> str:
            m = re.search(r"ILIKE\s+'%([^']+)%'", sql, re.IGNORECASE)
            if m: return m.group(1).lower()
            m = re.search(r"ILIKE\s+'([^'%]+)'", sql, re.IGNORECASE)
            if m: return m.group(1).lower()
            return ""

        def _date_filter(rows, sql):
            """Apply job_date >= start AND job_date <= end if present."""
            start_m = re.search(r"job_date\s*>=\s*'(\d{4}-\d{2}-\d{2})'", sql, re.IGNORECASE)
            end_m   = re.search(r"job_date\s*<=\s*'(\d{4}-\d{2}-\d{2})'", sql, re.IGNORECASE)
            if start_m:
                rows = [r for r in rows if (r.get("job_date") or "") >= start_m.group(1)]
            if end_m:
                rows = [r for r in rows if (r.get("job_date") or "") <= end_m.group(1)]
            return rows

        def _paid_filter(rows, sql):
            if re.search(r"paid\s+IS\s+NULL\b|LOWER\(paid\)\s+NOT\s+IN|LOWER\(COALESCE\(paid[^)]*\)\)\s+NOT\s+IN|paid\s+NOT\s+IN", sql, re.IGNORECASE):
                return [r for r in rows if _is_unpaid(r)]
            if re.search(r"LOWER\(COALESCE\(paid[^)]*\)\)\s+IN\s+\('true'|paid\s*=\s*'[Yy]es'", sql, re.IGNORECASE):
                return [r for r in rows if not _is_unpaid(r)]
            return rows

        def _bill_filter(rows, sql):
            if re.search(r"LOWER\(COALESCE\(bill_sent[^)]*\)\)\s+IN\s+\('true'|bill_sent\s+IS\s+NOT\s+NULL", sql, re.IGNORECASE):
                return [r for r in rows if _bill_sent(r)]
            if re.search(r"bill_sent\s+IS\s+NULL|LOWER\(bill_sent\)\s+NOT\s+IN|LOWER\(COALESCE\(bill_sent", sql, re.IGNORECASE):
                # "pending invoice" — not yet sent
                if "NOT IN" in su or "IS NULL" in su:
                    return [r for r in rows if not _bill_sent(r)]
            return rows

        client_q = _extract_client(sql)
        rows = [r for r in MOCK_ROWS if not client_q or client_q in _client_name(r).lower()]

        # COUNT
        if "COUNT(" in su:
            rows = _date_filter(rows, sql)
            rows = _paid_filter(rows, sql)
            rows = _bill_filter(rows, sql)
            # poc_email IS NOT NULL
            if re.search(r"poc_email\s+IS\s+NOT\s+NULL", sql, re.IGNORECASE):
                rows = [r for r in rows if r.get("poc_email")]
            return {"ok": True, "rows": [{"result": len(rows)}], "operation": "select"}

        # AVG
        if "AVG(" in su:
            fees = [r["fees"] for r in rows if r.get("fees")]
            avg = round(sum(fees) / len(fees)) if fees else 0
            return {"ok": True, "rows": [{"result": avg}], "operation": "select"}

        # SUM with GROUP BY
        if "SUM(" in su and "GROUP BY" in su:
            rows = _date_filter(rows, sql)
            rows = _paid_filter(rows, sql)
            grouped: Dict[str, int] = {}
            for r in rows:
                key = _client_name(r) or "Unknown"
                grouped[key] = grouped.get(key, 0) + (r.get("fees") or 0)
            ordered = sorted(grouped.items(), key=lambda x: x[1], reverse=True)
            result_rows = [{"client_name": k, "result": v} for k, v in ordered]
            lm = re.search(r"LIMIT\s+(\d+)", sql, re.IGNORECASE)
            if lm: result_rows = result_rows[:int(lm.group(1))]
            return {"ok": True, "rows": result_rows, "operation": "select"}

        # SUM (scalar)
        if "SUM(" in su:
            rows = _date_filter(rows, sql)
            rows = _paid_filter(rows, sql)
            total = sum(r.get("fees") or 0 for r in rows)
            return {"ok": True, "rows": [{"result": total}], "operation": "select"}

        # SELECT *
        rows = _date_filter(rows, sql)
        rows = _paid_filter(rows, sql)
        rows = _bill_filter(rows, sql)
        if re.search(r"poc_email\s+IS\s+NULL", sql, re.IGNORECASE):
            rows = [r for r in rows if not r.get("poc_email")]
        lm = re.search(r"LIMIT\s+(\d+)", sql, re.IGNORECASE)
        if lm: rows = rows[:int(lm.group(1))]
        return {"ok": True, "rows": rows, "operation": "select"}

    def get_available_months_for_client(self, *a, **kw): return []
    def insert_job_entry(self, record): return {"ok": True, "rows": [{**record, "id": 9999, "bill_no": "INV-TEST"}]}


# ─────────────────────────────────────────────────────────────────────────────
# Test case definitions
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TestCase:
    id: str
    message: str
    category: str
    sql_must_contain:     List[str] = field(default_factory=list)
    sql_must_not_contain: List[str] = field(default_factory=list)
    response_must_match:  List[str] = field(default_factory=list)
    response_must_not_contain: List[str] = field(default_factory=list)
    expected_extracted:   Optional[Dict] = None
    skip_sql_check:       bool = False
    # Exact rows the mock DB should return — validates SQL correctness independent of NL synthesis.
    # Use for queries with deterministic results (no date-relative filters).
    expected_db_rows:     Optional[List[Dict]] = None


_NO_ERROR = ["Two ways I could read", "couldn't format", "couldn't quite work out"]

TESTS: List[TestCase] = [

    # ── CATEGORY 1: Simple Count (4) ─────────────────────────────────────────
    TestCase(
        id="C1-01", message="How many jobs have I done?",
        category="Count",
        sql_must_contain=["COUNT("],
        sql_must_not_contain=["SELECT *"],
        response_must_match=[r"\b8\b"],
        response_must_not_contain=_NO_ERROR,
        expected_db_rows=[{"result": 8}],
    ),
    TestCase(
        id="C1-02", message="How many total jobs do I have?",
        category="Count",
        sql_must_contain=["COUNT("],
        sql_must_not_contain=["SELECT *"],
        response_must_match=[r"\b8\b"],
        response_must_not_contain=_NO_ERROR,
        expected_db_rows=[{"result": 8}],
    ),
    TestCase(
        id="C1-03", message="How many unpaid invoices do I have?",
        category="Count",
        sql_must_contain=["COUNT("],
        response_must_match=[r"\b[1-9]\d*\b"],  # some non-zero number
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C1-04", message="Kitne jobs hain mere paas",
        category="Count — Hinglish",
        sql_must_contain=["COUNT("],
        response_must_match=[r"\b8\b|\b\d+\b"],
        response_must_not_contain=_NO_ERROR,
    ),

    # ── CATEGORY 2: SUM / Total (5) ──────────────────────────────────────────
    TestCase(
        id="C2-01", message="Total billing this year",
        category="SUM",
        sql_must_contain=["SUM("],
        # All 8 mock rows are in 2026: 150k+200k+80k+120k+95k+300k+55k+175k = 1,175,000
        response_must_match=[r"11[,.]?75[,.]?000|1[,.]?175[,.]?000|1175000"],
        response_must_not_contain=_NO_ERROR,
        expected_db_rows=[{"result": 1175000}],
    ),
    TestCase(
        id="C2-02", message="Earnings last quarter",
        category="SUM — date range",
        sql_must_contain=["SUM("],
        sql_must_not_contain=["SELECT *"],
        response_must_match=[r"₹\s*\d[\d,]*"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C2-03", message="Total earnings last month",
        category="SUM — last month",
        sql_must_contain=["SUM("],
        response_must_match=[r"₹\s*\d[\d,]*"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C2-04", message="How much have I earned from Garnier India?",
        category="SUM — client filter",
        sql_must_contain=["SUM(", "Garnier"],
        response_must_match=[r"₹\s*\d[\d,]*"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C2-05", message="Pichle mahine ki kamai kitni thi",
        category="SUM — Hinglish last month",
        sql_must_contain=["SUM("],
        response_must_match=[r"₹\s*\d[\d,]*"],
        response_must_not_contain=_NO_ERROR,
    ),

    # ── CATEGORY 3: GROUP BY + AVG (3) ───────────────────────────────────────
    TestCase(
        id="C3-01", message="Who is my biggest client?",
        category="GROUP BY — top client",
        sql_must_contain=["GROUP BY", "SUM(", "DESC"],
        response_must_match=[r"Star Studios|Samsung|Garnier"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C3-02", message="Average fees per job",
        category="AVG",
        sql_must_contain=["AVG("],
        # AVG of 8 rows = 1,175,000 / 8 = 146,875. Must contain the actual value,
        # not just any number — this catches the AVG→SUM refusal bug from HANDOFF.md.
        response_must_match=[r"1[,.]?46[,.]?875|146875"],
        response_must_not_contain=_NO_ERROR + ["total fees", "can't calculate", "cannot calculate", "don't calculate"],
        expected_db_rows=[{"result": 146875}],
    ),
    TestCase(
        id="C3-03", message="Top 3 clients by total revenue",
        category="GROUP BY — top N",
        sql_must_contain=["GROUP BY", "SUM(", "LIMIT"],
        response_must_match=[r"Star Studios|Samsung|Garnier|Maruti"],
        response_must_not_contain=_NO_ERROR,
    ),

    # ── CATEGORY 4: Client Payment Status (3) ────────────────────────────────
    TestCase(
        id="C4-01", message="How much does Star Studios owe me?",
        category="Client unpaid SUM",
        sql_must_contain=["SUM(", "Star Studios"],
        # Only row 2 (Adidas, paid=None) = 200,000. Row 1 is paid so excluded.
        response_must_match=[r"Star Studios", r"2[,.]?00[,.]?000|200[,.]?000|200000"],
        response_must_not_contain=_NO_ERROR,
        expected_db_rows=[{"result": 200000}],
    ),
    TestCase(
        id="C4-02", message="Star Studios se paisa aaya kya?",
        category="Client paid check — Hinglish",
        sql_must_contain=["Star Studios"],
        response_must_match=[r"₹\s*\d[\d,]*|paid|received|pending"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C4-03", message="What are my total outstanding payments?",
        category="Total unpaid SUM",
        sql_must_contain=["SUM("],
        response_must_match=[r"₹\s*\d[\d,]*"],
        response_must_not_contain=_NO_ERROR,
    ),

    # ── CATEGORY 5: Invoice / Bill Status (4) ────────────────────────────────
    TestCase(
        id="C5-01", message="List all unpaid invoices",
        category="Invoice — unpaid list",
        response_must_match=[r"Star Studios|Pedigree|Samsung|Garnier"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C5-02", message="Kiska invoice bhejna baki hai",
        category="Invoice — pending Hinglish",
        response_must_match=[r"Pedigree|Samsung|Garnier|Star Studios"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C5-03", message="Isme se invoice kitne logon ko bheja hai",
        category="Invoice — sent count Hinglish",
        sql_must_contain=["COUNT("],
        response_must_match=[r"\b\d+\b"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C5-04", message="How many invoices have I sent?",
        category="Invoice — sent count",
        sql_must_contain=["COUNT("],
        response_must_match=[r"\b\d+\b"],
        response_must_not_contain=_NO_ERROR,
    ),

    # ── CATEGORY 6: Date Range Queries (4) ───────────────────────────────────
    TestCase(
        id="C6-01", message="Show jobs from last month",
        category="Date — last month",
        response_must_match=[r"Garnier|L'Oréal|no jobs|₹"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C6-02", message="Jobs in Q1 this year",
        category="Date — Q1",
        response_must_match=[r"Star Studios|Garnier|Maruti|jobs|₹"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C6-03", message="What did I work on in February?",
        category="Date — specific month",
        response_must_match=[r"Garnier|Maruti|February|no jobs"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C6-04", message="Show jobs from this week",
        category="Date — this week",
        # Samsung row is Jun 10, 2026 (in week of Jun 8-14)
        response_must_match=[r"Samsung|no jobs|this week|June"],
        response_must_not_contain=_NO_ERROR,
    ),

    # ── CATEGORY 7: Client / Brand Filter (3) ────────────────────────────────
    TestCase(
        id="C7-01", message="Show my last 5 jobs",
        category="Basic list",
        sql_must_contain=["LIMIT"],
        response_must_match=[r"Star Studios|Garnier|Samsung|Pedigree|Maruti"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C7-02", message="Show all Garnier jobs",
        category="Client filter — Garnier",
        sql_must_contain=["Garnier"],
        response_must_match=[r"Garnier"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C7-03", message="Show Samsung jobs",
        category="Client filter — Samsung",
        sql_must_contain=["Samsung"],
        response_must_match=[r"Samsung|Galaxy|300"],
        response_must_not_contain=_NO_ERROR,
    ),

    # ── CATEGORY 8: Hinglish — Mixed Queries (3) ─────────────────────────────
    TestCase(
        id="C8-01", message="Kiska payment baki hai",
        category="Hinglish — unpaid list",
        response_must_match=[r"Star Studios|Pedigree|Samsung|Garnier|₹"],
        response_must_not_contain=_NO_ERROR,
    ),
    TestCase(
        id="C8-02", message="Total fees for Star Studios",
        category="Hinglish — client total",
        sql_must_contain=["SUM(", "Star Studios"],
        # Row 1 (150k) + Row 2 (200k) = 350,000
        response_must_match=[r"₹\s*3[,.]?50[,.]?000|₹\s*350"],
        response_must_not_contain=_NO_ERROR,
        expected_db_rows=[{"result": 350000}],
    ),
    TestCase(
        id="C8-03", message="Kaunse jobs ka invoice nahi gaya",
        category="Hinglish — invoice not sent",
        response_must_match=[r"Pedigree|Samsung|Garnier Men|no jobs"],
        response_must_not_contain=_NO_ERROR,
    ),

    # ── CATEGORY 9: Smart Capture — Bug 4 (2) ────────────────────────────────
    TestCase(
        id="C9-01", message="Add a job for Acme, 25k, shoot, paid",
        category="Smart Capture — paid flag",
        skip_sql_check=True,
        expected_extracted={"brand_name_nonempty": True, "fees": 25000, "paid_truthy": True},
        response_must_not_contain=["couldn't format"],
    ),
    TestCase(
        id="C9-02", message="Nike ka shoot kiya 10 February ko, 30 hazaar",
        category="Smart Capture — Hinglish",
        skip_sql_check=True,
        expected_extracted={"brand_name_nonempty": True, "fees": 30000},
        response_must_not_contain=["couldn't format"],
    ),

    # ── CATEGORY 10: Edge Cases (2) ──────────────────────────────────────────
    TestCase(
        id="C10-01", message="Can you book me an Uber?",
        category="Out of scope",
        skip_sql_check=True,
        # Should NOT say it can do it — must decline or redirect
        response_must_not_contain=["Uber", "book", "ride"],
        response_must_match=[r"job|invoice|payment|billing|record|track|log"],
    ),
    TestCase(
        id="C10-02", message="genrate invoce for Pedigree",
        category="Typo detection — invoice",
        skip_sql_check=True,
        # Should recognise as invoice request (not "Two ways I could read")
        response_must_not_contain=["Two ways I could read", "couldn't format"],
        response_must_match=[r"Pedigree|invoice|invoic"],
    ),
]


# ─────────────────────────────────────────────────────────────────────────────
# Test runners
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class TestResult:
    tc: TestCase
    sql: Optional[str]
    response: Optional[str]
    sql_pass: bool
    response_pass: bool
    sql_failures: List[str]
    response_failures: List[str]
    error: Optional[str]
    elapsed: float

    @property
    def overall_pass(self) -> bool:
        return self.error is None and self.sql_pass and self.response_pass


def _run_plan_retried(message, gemini, supabase):
    """Run execute_query_plan with one retry on JSON parse fluke."""
    result = execute_query_plan(message, gemini, supabase, user_id=TEST_USER_ID)
    if result.get("_error") and "JSON" in str(result.get("_error", "")):
        time.sleep(1.5)
        result = execute_query_plan(message, gemini, supabase, user_id=TEST_USER_ID)
    return result


def run_query_test(tc: TestCase, gemini, supabase) -> TestResult:
    t0 = time.time()
    sql = None; response = None
    sql_failures: List[str] = []; response_failures: List[str] = []
    try:
        plan_result = _run_plan_retried(tc.message, gemini, supabase)

        if plan_result.get("clarification"):
            sql = f"[CLARIFICATION] {plan_result['clarification']}"
            sql_failures.append("Planner returned clarification instead of SQL")
        elif plan_result.get("_error"):
            sql = f"[ERROR] {plan_result['_error'][:120]}"
            sql_failures.append(f"Planner error: {plan_result['_error'][:80]}")
        else:
            sql = plan_result.get("sql", "")

        if sql and not tc.skip_sql_check:
            su = sql.upper()
            for p in tc.sql_must_contain:
                if p.upper() not in su:
                    sql_failures.append(f"SQL missing '{p}'")
            for p in tc.sql_must_not_contain:
                if p.upper() in su:
                    sql_failures.append(f"SQL contains forbidden '{p}'")

        if sql and not sql.startswith("["):
            db = supabase.execute_sql(sql)
            rows = db.get("rows", []) if db.get("ok") else []

            if tc.expected_db_rows is not None and rows != tc.expected_db_rows:
                sql_failures.append(
                    f"DB result mismatch: expected {tc.expected_db_rows}, got {rows}"
                )

            payload = build_clean_payload(rows, "select")
            response = gemini.synthesize_response(payload, tc.message)
        else:
            response = sql or ""

        if response:
            for p in tc.response_must_not_contain:
                if p.lower() in response.lower():
                    response_failures.append(f"Response contains bad phrase: '{p}'")
            for p in tc.response_must_match:
                if not re.search(p, response, re.IGNORECASE):
                    response_failures.append(f"Missing expected pattern: '{p}'")
        else:
            response_failures.append("Empty response from synthesizer")

    except Exception as e:
        return TestResult(tc=tc, sql=sql, response=response,
                          sql_pass=False, response_pass=False,
                          sql_failures=sql_failures, response_failures=response_failures,
                          error=str(e), elapsed=time.time() - t0)

    return TestResult(tc=tc, sql=sql, response=response,
                      sql_pass=not sql_failures, response_pass=not response_failures,
                      sql_failures=sql_failures, response_failures=response_failures,
                      error=None, elapsed=time.time() - t0)


def run_smart_capture_test(tc: TestCase, gemini, supabase) -> TestResult:
    t0 = time.time()
    response_failures: List[str] = []
    try:
        extracted = gemini.extract_job_fields(tc.message)
        if extracted is None:
            time.sleep(1.5)
            extracted = gemini.extract_job_fields(tc.message)
        response = json.dumps(extracted, ensure_ascii=False) if extracted else "null"

        if tc.expected_extracted and extracted:
            if tc.expected_extracted.get("brand_name_nonempty") and not extracted.get("brand_name"):
                response_failures.append("brand_name not extracted")
            if "fees" in tc.expected_extracted:
                if extracted.get("fees") != tc.expected_extracted["fees"]:
                    response_failures.append(f"fees: expected {tc.expected_extracted['fees']}, got {extracted.get('fees')}")
            if tc.expected_extracted.get("paid_truthy"):
                if str(extracted.get("paid") or "").lower() not in ("true", "yes", "1", "paid"):
                    response_failures.append(f"paid not truthy (got {extracted.get('paid')!r})")
        elif not extracted:
            response_failures.append("extract_job_fields returned None")

        for p in tc.response_must_not_contain:
            if response and p.lower() in response.lower():
                response_failures.append(f"Response contains bad phrase: '{p}'")

    except Exception as e:
        return TestResult(tc=tc, sql=None, response=None,
                          sql_pass=True, response_pass=False,
                          sql_failures=[], response_failures=[str(e)],
                          error=str(e), elapsed=time.time() - t0)

    return TestResult(tc=tc, sql=None, response=response,
                      sql_pass=True, response_pass=not response_failures,
                      sql_failures=[], response_failures=response_failures,
                      error=None, elapsed=time.time() - t0)


def run_oos_test(tc: TestCase, gemini, supabase) -> TestResult:
    """Out-of-scope / edge-case tests — run through the planner and check response."""
    t0 = time.time()
    response_failures: List[str] = []
    sql = None
    try:
        plan_result = _run_plan_retried(tc.message, gemini, supabase)
        # May get clarification or SQL
        if plan_result.get("clarification"):
            response = plan_result["clarification"]
        elif plan_result.get("sql"):
            sql = plan_result["sql"]
            db = supabase.execute_sql(sql)
            rows = db.get("rows", []) if db.get("ok") else []
            payload = build_clean_payload(rows, "select")
            response = gemini.synthesize_response(payload, tc.message)
        elif plan_result.get("_error"):
            response = plan_result["_error"][:120]
        else:
            response = ""

        if response:
            for p in tc.response_must_not_contain:
                if p.lower() in response.lower():
                    response_failures.append(f"Response contains bad phrase: '{p}'")
            for p in tc.response_must_match:
                if not re.search(p, response, re.IGNORECASE):
                    response_failures.append(f"Missing expected pattern: '{p}'")
        else:
            response_failures.append("Empty response")

    except Exception as e:
        return TestResult(tc=tc, sql=sql, response=None,
                          sql_pass=True, response_pass=False,
                          sql_failures=[], response_failures=[str(e)],
                          error=str(e), elapsed=time.time() - t0)

    return TestResult(tc=tc, sql=sql, response=response,
                      sql_pass=True, response_pass=not response_failures,
                      sql_failures=[], response_failures=response_failures,
                      error=None, elapsed=time.time() - t0)


# ─────────────────────────────────────────────────────────────────────────────
# Output
# ─────────────────────────────────────────────────────────────────────────────

def print_result(r: TestResult):
    status = f"{GREEN}PASS{RESET}" if r.overall_pass else f"{RED}FAIL{RESET}"
    print(f"\n{BOLD}{r.tc.id:<10}{RESET}{CYAN}{r.tc.category:<35}{RESET}{status}  ({r.elapsed:.1f}s)")
    print(f"  {r.tc.message}")

    if not r.tc.skip_sql_check and r.sql:
        ok = f"{GREEN}✓{RESET}" if r.sql_pass else f"{RED}✗{RESET}"
        print(f"  SQL {ok}  {r.sql[:110].replace(chr(10),' ')}")
        for f in r.sql_failures:
            print(f"    {RED}→ {f}{RESET}")

    if r.response:
        ok = f"{GREEN}✓{RESET}" if r.response_pass else f"{RED}✗{RESET}"
        print(f"  Resp {ok} {(r.response or '')[:140].replace(chr(10),' ')}")
        for f in r.response_failures:
            print(f"    {RED}→ {f}{RESET}")

    if r.error:
        print(f"  {RED}Exception: {r.error}{RESET}")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

def main():
    print(f"\n{BOLD}{'═'*72}{RESET}")
    print(f"{BOLD}  Ops Bot — Live E2E Tests  ({len(TESTS)} cases){RESET}")
    print(f"  user_id={TEST_USER_ID}  |  max_tokens={_MAX_TOKENS_OVERRIDE}")
    print(f"{'═'*72}{RESET}")

    gemini  = GeminiService()
    supabase = MockSupabaseService()

    results: List[TestResult] = []
    by_category: Dict[str, List[TestResult]] = {}

    for tc in TESTS:
        print(f"  ▶ {tc.id}  {tc.message[:65]}...", end="", flush=True)

        if tc.expected_extracted is not None:
            r = run_smart_capture_test(tc, gemini, supabase)
        elif tc.category.startswith("Out of scope") or tc.category.startswith("Typo"):
            r = run_oos_test(tc, gemini, supabase)
        else:
            r = run_query_test(tc, gemini, supabase)

        results.append(r)
        cat = tc.category.split("—")[0].strip().split(" ")[0]
        by_category.setdefault(cat, []).append(r)
        print(f"\r", end="")
        print_result(r)
        time.sleep(0.25)

    # ── Summary ──────────────────────────────────────────────────────────────
    passed = sum(1 for r in results if r.overall_pass)
    failed = len(results) - passed

    print(f"\n{BOLD}{'═'*72}{RESET}")
    print(f"{BOLD}  Results by category:{RESET}")
    for cat, rs in by_category.items():
        cp = sum(1 for r in rs if r.overall_pass)
        bar = f"{GREEN}{'█'*cp}{RED}{'░'*(len(rs)-cp)}{RESET}"
        print(f"    {cat:<22} {bar}  {cp}/{len(rs)}")

    print(f"\n{BOLD}  Total: {GREEN}{passed} passed{RESET}{BOLD}, "
          f"{RED}{failed} failed{RESET}{BOLD} / {len(results)}{RESET}")

    if failed:
        print(f"\n  {BOLD}Failing:{RESET}")
        for r in results:
            if not r.overall_pass:
                issues = (r.sql_failures + r.response_failures)[:2]
                print(f"    {RED}✗{RESET} {r.tc.id} — {'; '.join(issues)}")

    print(f"{'═'*72}\n")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
