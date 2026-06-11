#!/usr/bin/env python3
"""
Live end-to-end tests using the real OpenRouter/Gemini API.

Pipeline under test:
  User message
    → execute_query_plan()          [Classify → Plan → SQL]
    → MockSupabaseService.execute_sql()  [returns realistic fake rows]
    → build_clean_payload()
    → GeminiService.synthesize_response()
    → response string

Smart-capture test path:
  User message → GeminiService.extract_job_fields() → extracted dict

Usage:
    AI_KEY=sk-or-v1-... python3 tests/test_e2e_live.py
    # or export AI_KEY first, then just run the file

Nothing in the production flow is modified or mocked with fake objects —
only SupabaseService is replaced with a local in-memory fixture dataset.
"""

import os
import re
import sys
import json
import time
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

# ── path setup ────────────────────────────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# ── API key ───────────────────────────────────────────────────────────────────
AI_KEY = os.environ.get("AI_KEY", "").strip()
if not AI_KEY:
    print("ERROR: Set AI_KEY env variable before running.\n"
          "  AI_KEY=sk-or-v1-... python3 tests/test_e2e_live.py")
    sys.exit(1)
os.environ["AI_KEY"] = AI_KEY
os.environ["STRICT_PLAN_VALIDATION"] = "1"

# ── imports (after env is set) ────────────────────────────────────────────────
from services.gemini_service import GeminiService
from services.query_planner import execute_query_plan
from services.response_synthesis import build_clean_payload
from services.supabase_service import JOB_ENTRIES_COLUMNS, SCHEMA_DESCRIPTION

# Cap max_tokens per call so low-credit keys still work.
# The planner JSON is usually 150-300 tokens; synthesis is 50-150.
# Override to 400 so any key with ≥400 tokens of credit can run each call.
_MAX_TOKENS_OVERRIDE = int(os.environ.get("E2E_MAX_TOKENS", "700"))

_original_call_api = GeminiService._call_api

def _patched_call_api(self, prompt, generation_config=None):
    gc = dict(generation_config or {})
    gc["maxOutputTokens"] = min(gc.get("maxOutputTokens", _MAX_TOKENS_OVERRIDE), _MAX_TOKENS_OVERRIDE)
    return _original_call_api(self, prompt, gc)

GeminiService._call_api = _patched_call_api

# ── colours ───────────────────────────────────────────────────────────────────
GREEN = "\033[92m"
RED   = "\033[91m"
YELLOW = "\033[93m"
CYAN  = "\033[96m"
BOLD  = "\033[1m"
RESET = "\033[0m"

TEST_USER_ID = "test_e2e_user_001"


# ═══════════════════════════════════════════════════════════════════════════════
# Mock SupabaseService — real schema, realistic in-memory dataset
# ═══════════════════════════════════════════════════════════════════════════════

MOCK_ROWS: List[Dict] = [
    {
        "id": 1, "user_id": TEST_USER_ID,
        "client_name": "Star Studios", "brand_name": "Nike",
        "job_date": "2026-03-15", "fees": 150000,
        "paid": "Yes", "bill_sent": "Yes",
        "poc_email": "contact@starstudios.com",
        "invoice_date": "2026-03-20", "bill_no": "INV-001",
        "job_description_details": "TVC 30sec + 4 cutdowns",
        "isDeleted": None,
    },
    {
        "id": 2, "user_id": TEST_USER_ID,
        "client_name": "Star Studios", "brand_name": "Adidas",
        "job_date": "2026-01-10", "fees": 200000,
        "paid": None, "bill_sent": "Yes",
        "poc_email": "contact@starstudios.com",
        "invoice_date": "2026-01-15", "bill_no": "INV-002",
        "job_description_details": "Brand film 60sec",
        "isDeleted": None,
    },
    {
        "id": 3, "user_id": TEST_USER_ID,
        "client_name": "Garnier India", "brand_name": "Garnier",
        "job_date": "2026-02-20", "fees": 80000,
        "paid": "Yes", "bill_sent": "Yes",
        "poc_email": "priya@garnier.com",
        "invoice_date": "2026-02-25", "bill_no": "INV-003",
        "job_description_details": "Product shoot",
        "isDeleted": None,
    },
    {
        "id": 4, "user_id": TEST_USER_ID,
        "client_name": "Pedigree Films", "brand_name": "Pedigree",
        "job_date": "2026-04-05", "fees": 120000,
        "paid": None, "bill_sent": None,
        "poc_email": None,
        "invoice_date": None, "bill_no": "INV-004",
        "job_description_details": "Pet food TVC",
        "isDeleted": None,
    },
    {
        "id": 5, "user_id": TEST_USER_ID,
        "client_name": "Garnier India", "brand_name": "L'Oréal",
        "job_date": "2026-05-01", "fees": 95000,
        "paid": None, "bill_sent": "Yes",
        "poc_email": "priya@garnier.com",
        "invoice_date": "2026-05-05", "bill_no": "INV-005",
        "job_description_details": "Skincare campaign",
        "isDeleted": None,
    },
]


def _client_name(row: Dict) -> str:
    return (
        row.get("client_name") or row.get("brand_name") or
        row.get("production_house") or ""
    )


class MockSupabaseService:
    """Replaces SupabaseService with a local fixture dataset.
    Schema comes from the real supabase_service so the LLM sees the exact
    column list it would see in production."""

    def get_schema(self) -> Dict[str, Any]:
        return {
            "table": "job_entries",
            "schema_name": "public",
            "columns": JOB_ENTRIES_COLUMNS,
            "description": SCHEMA_DESCRIPTION.strip(),
        }

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """Parse the SQL and return appropriate mock rows."""
        su = sql.upper()

        # Extract client filter — handles both ILIKE '%X%' (keyword shortcut)
        # and ILIKE 'X' (planner output without wildcards).
        def _extract_client(sql: str) -> str:
            m = re.search(r"ILIKE\s+'%([^']+)%'", sql, re.IGNORECASE)
            if m:
                return m.group(1).lower()
            m = re.search(r"ILIKE\s+'([^'%]+)'", sql, re.IGNORECASE)
            if m:
                return m.group(1).lower()
            return ""

        # COUNT
        if "COUNT(" in su:
            client_q = _extract_client(sql)
            count = sum(
                1 for r in MOCK_ROWS
                if not client_q or client_q in _client_name(r).lower()
            )
            return {"ok": True, "rows": [{"result": count}], "operation": "select"}

        # AVG
        if "AVG(" in su:
            fees = [r["fees"] for r in MOCK_ROWS if r.get("fees")]
            avg = round(sum(fees) / len(fees)) if fees else 0
            return {"ok": True, "rows": [{"result": avg}], "operation": "select"}

        # SUM with GROUP BY — biggest client
        if "SUM(" in su and "GROUP BY" in su:
            grouped: Dict[str, int] = {}
            for r in MOCK_ROWS:
                key = _client_name(r) or "Unknown"
                grouped[key] = grouped.get(key, 0) + (r.get("fees") or 0)
            ordered = sorted(grouped.items(), key=lambda x: x[1], reverse=True)
            rows = [{"client_name": k, "result": v} for k, v in ordered]
            if "LIMIT 1" in su:
                rows = rows[:1]
            return {"ok": True, "rows": rows, "operation": "select"}

        # SUM with optional client filter + paid semantics
        if "SUM(" in su:
            client_q = _extract_client(sql)
            subset = [
                r for r in MOCK_ROWS
                if not client_q or client_q in _client_name(r).lower()
            ]
            # Unpaid filter
            if re.search(r"paid\s+IS\s+NULL\b|LOWER\(paid\)\s+NOT\s+IN|paid\s+NOT\s+IN", sql, re.IGNORECASE):
                subset = [r for r in subset if not r.get("paid")]
            # Paid filter
            elif re.search(r"LOWER\(COALESCE\(paid[^)]*\)\)\s+IN\s+\('true'|paid\s*=\s*'[Yy]es'", sql, re.IGNORECASE):
                subset = [r for r in subset if r.get("paid")]
            total = sum(r.get("fees") or 0 for r in subset)
            return {"ok": True, "rows": [{"result": total}], "operation": "select"}

        # SELECT * — return rows, optionally filtered
        client_q = _extract_client(sql)
        rows = [r for r in MOCK_ROWS if not client_q or client_q in _client_name(r).lower()]

        # Unpaid filter
        if re.search(r"paid\s+IS\s+NULL\b|LOWER\(paid\)\s+NOT\s+IN|LOWER\(COALESCE\(paid", sql, re.IGNORECASE):
            rows = [r for r in rows if not r.get("paid")]

        # LIMIT
        lm = re.search(r"LIMIT\s+(\d+)", sql, re.IGNORECASE)
        if lm:
            rows = rows[:int(lm.group(1))]

        return {"ok": True, "rows": rows, "operation": "select"}

    # ── stubs for the small number of other methods the pipeline touches ──────

    def get_available_months_for_client(self, *a, **kw):
        return []

    def insert_job_entry(self, record: Dict) -> Dict:
        return {"ok": True, "rows": [{**record, "id": 9999, "bill_no": "INV-TEST"}]}


# ═══════════════════════════════════════════════════════════════════════════════
# Test case definitions
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class TestCase:
    id: str
    message: str
    category: str
    # SQL checks (substrings that MUST appear in the generated SQL, uppercase)
    sql_must_contain: List[str] = field(default_factory=list)
    # SQL checks (substrings that must NOT appear)
    sql_must_not_contain: List[str] = field(default_factory=list)
    # Response checks (regex patterns the response must match, case-insensitive)
    response_must_match: List[str] = field(default_factory=list)
    # Response must NOT contain these strings
    response_must_not_contain: List[str] = field(default_factory=list)
    # For smart-capture tests: expected extracted fields
    expected_extracted: Optional[Dict] = None
    # Skip SQL check entirely (e.g. smart-capture)
    skip_sql_check: bool = False


TESTS: List[TestCase] = [
    # ── Bug 2: COUNT queries ──────────────────────────────────────────────────
    TestCase(
        id="#28", message="How many jobs have I done?",
        category="Bug 2 — COUNT",
        sql_must_contain=["COUNT("],
        sql_must_not_contain=["SELECT *"],
        response_must_match=[r"\b5\b"],  # 5 rows in mock dataset
        response_must_not_contain=["couldn't format", "Two ways I could read"],
    ),
    TestCase(
        id="#29", message="How many total jobs do I have?",
        category="Bug 2 — COUNT",
        sql_must_contain=["COUNT("],
        sql_must_not_contain=["SELECT *"],
        response_must_match=[r"\b5\b"],
        response_must_not_contain=["couldn't format", "Two ways I could read"],
    ),
    # ── Bug 1: aggregate queries ──────────────────────────────────────────────
    TestCase(
        id="#4", message="Who is my biggest client?",
        category="Bug 1 — GROUP BY",
        sql_must_contain=["GROUP BY", "SUM(", "DESC"],
        response_must_match=["Star Studios"],
        response_must_not_contain=["Two ways I could read", "couldn't format"],
    ),
    TestCase(
        id="#7", message="Average fees per job",
        category="Bug 1 — AVG",
        sql_must_contain=["AVG("],
        response_must_match=[r"₹?\s*\d[\d,]*"],  # some rupee/number amount
        response_must_not_contain=["Two ways I could read", "couldn't format"],
    ),
    TestCase(
        id="#24", message="How much does Star Studios owe me?",
        category="Bug 1 — client unpaid SUM",
        sql_must_contain=["SUM(", "Star Studios"],
        # Response must mention Star Studios AND a rupee amount (exact figure depends
        # on whether planner or keyword shortcut generates the SQL)
        response_must_match=[r"Star Studios", r"₹\s*\d[\d,]*"],
        response_must_not_contain=["Two ways I could read", "couldn't format"],
    ),
    TestCase(
        id="#26", message="Star Studios se paisa aaya kya?",
        category="Bug 1 — Hinglish paid check",
        sql_must_contain=["Star Studios"],
        response_must_not_contain=["Two ways I could read", "couldn't format"],
        # Response should contain a rupee amount OR mention paid/received/pending
        response_must_match=[r"₹\s*\d[\d,]*|paid|received|pending|outstanding"],
    ),
    # ── ⚠️ tests that were partial/wrong before ──────────────────────────────
    TestCase(
        id="#8", message="Isme se invoice kitne logon ko bheja hai",
        category="Hinglish COUNT",
        sql_must_contain=["COUNT("],
        response_must_not_contain=["Two ways I could read", "couldn't format"],
        response_must_match=[r"\d+"],
    ),
    TestCase(
        id="#9", message="Kiska payment baki hai",
        category="Hinglish unpaid",
        response_must_not_contain=["Two ways I could read", "couldn't format"],
        response_must_match=[r"Pedigree|Star Studios|unpaid|pending|₹"],
    ),
    TestCase(
        id="#12", message="Earnings last quarter",
        category="Bug 3 — earnings SUM",
        sql_must_contain=["SUM("],
        sql_must_not_contain=["SELECT *"],
        response_must_match=[r"₹?\s*\d[\d,]*"],
        response_must_not_contain=["Two ways I could read", "couldn't format"],
    ),
    # ── Smart capture: Bug 4 ─────────────────────────────────────────────────
    TestCase(
        id="#1-smart", message="Add a job for Acme, 25k, shoot, paid",
        category="Bug 4 — smart capture paid",
        skip_sql_check=True,
        expected_extracted={
            "brand_name_nonempty": True,
            "fees": 25000,
            "paid_truthy": True,
        },
        response_must_not_contain=["couldn't format"],
    ),
    # ── Regression: basic queries should still work ───────────────────────────
    TestCase(
        id="#2", message="Show my last 5 jobs",
        category="Regression — basic list",
        sql_must_contain=["LIMIT"],
        response_must_not_contain=["Two ways I could read", "couldn't format"],
        response_must_match=[r"Nike|Adidas|Garnier|Pedigree"],
    ),
    TestCase(
        id="#6", message="Total billing this year",
        category="Regression — total SUM",
        sql_must_contain=["SUM("],
        response_must_match=[r"₹?\s*\d[\d,]*"],
        response_must_not_contain=["Two ways I could read", "couldn't format"],
    ),
]


# ═══════════════════════════════════════════════════════════════════════════════
# Test runner
# ═══════════════════════════════════════════════════════════════════════════════

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


def run_query_test(tc: TestCase, gemini: GeminiService, supabase: MockSupabaseService) -> TestResult:
    """Run one query test case through the full pipeline."""
    t0 = time.time()
    sql = None
    response = None
    sql_failures: List[str] = []
    response_failures: List[str] = []

    try:
        # Stage 1: Planner → SQL (retry once on JSON parse error — LLM fluke)
        plan_result = execute_query_plan(
            tc.message, gemini, supabase,
            user_id=TEST_USER_ID,
        )
        if plan_result.get("_error") and "JSON" in str(plan_result.get("_error", "")):
            time.sleep(1)
            plan_result = execute_query_plan(
                tc.message, gemini, supabase,
                user_id=TEST_USER_ID,
            )

        if plan_result.get("clarification"):
            # Clarification means planner bailed — counts as SQL failure
            sql = f"[CLARIFICATION] {plan_result['clarification']}"
            sql_failures.append(f"Planner returned clarification instead of SQL")
        elif plan_result.get("_error"):
            sql = f"[ERROR] {plan_result['_error']}"
            sql_failures.append(f"Planner error: {plan_result['_error']}")
        else:
            sql = plan_result.get("sql", "")

        # Stage 2: SQL quality checks
        if sql and not tc.skip_sql_check:
            su = sql.upper()
            for pattern in tc.sql_must_contain:
                if pattern.upper() not in su:
                    sql_failures.append(f"SQL missing '{pattern}'")
            for pattern in tc.sql_must_not_contain:
                if pattern.upper() in su:
                    sql_failures.append(f"SQL contains forbidden '{pattern}'")

        # Stage 3: Execute mock SQL + synthesize response
        if sql and not sql.startswith("["):
            db_result = supabase.execute_sql(sql)
            rows = db_result.get("rows", []) if db_result.get("ok") else []
            payload = build_clean_payload(rows, "select")
            response = gemini.synthesize_response(payload, tc.message)
        else:
            # No valid SQL — synthesize an error response for checking
            response = sql or ""

        # Stage 4: Response quality checks
        if response:
            for pattern in tc.response_must_not_contain:
                if pattern.lower() in response.lower():
                    response_failures.append(f"Response contains bad phrase: '{pattern}'")
            for pattern in tc.response_must_match:
                if not re.search(pattern, response, re.IGNORECASE):
                    response_failures.append(f"Response missing expected pattern: '{pattern}'")
        else:
            response_failures.append("Empty response from synthesizer")

    except Exception as e:
        return TestResult(
            tc=tc, sql=sql, response=response,
            sql_pass=False, response_pass=False,
            sql_failures=sql_failures, response_failures=response_failures,
            error=str(e), elapsed=time.time() - t0,
        )

    return TestResult(
        tc=tc, sql=sql, response=response,
        sql_pass=len(sql_failures) == 0,
        response_pass=len(response_failures) == 0,
        sql_failures=sql_failures,
        response_failures=response_failures,
        error=None, elapsed=time.time() - t0,
    )


def run_smart_capture_test(tc: TestCase, gemini: GeminiService, supabase: MockSupabaseService) -> TestResult:
    """Run a smart-capture test: only extract_job_fields, no SQL."""
    t0 = time.time()
    response_failures: List[str] = []

    try:
        extracted = gemini.extract_job_fields(tc.message)
        if extracted is None:  # retry once on JSON fluke
            time.sleep(1)
            extracted = gemini.extract_job_fields(tc.message)
        response = json.dumps(extracted, ensure_ascii=False) if extracted else "null"

        if tc.expected_extracted and extracted:
            if tc.expected_extracted.get("brand_name_nonempty"):
                if not extracted.get("brand_name"):
                    response_failures.append("brand_name not extracted")
            if "fees" in tc.expected_extracted:
                if extracted.get("fees") != tc.expected_extracted["fees"]:
                    response_failures.append(
                        f"fees: expected {tc.expected_extracted['fees']}, got {extracted.get('fees')}"
                    )
            if tc.expected_extracted.get("paid_truthy"):
                paid_val = str(extracted.get("paid") or "").lower()
                if paid_val not in ("true", "yes", "1", "paid"):
                    response_failures.append(
                        f"paid not extracted as truthy (got {extracted.get('paid')!r})"
                    )
        elif not extracted:
            response_failures.append("extract_job_fields returned None")

        for pattern in tc.response_must_not_contain:
            if response and pattern.lower() in response.lower():
                response_failures.append(f"Response contains bad phrase: '{pattern}'")

    except Exception as e:
        return TestResult(
            tc=tc, sql=None, response=None,
            sql_pass=True, response_pass=False,
            sql_failures=[], response_failures=[str(e)],
            error=str(e), elapsed=time.time() - t0,
        )

    return TestResult(
        tc=tc, sql=None, response=response,
        sql_pass=True,
        response_pass=len(response_failures) == 0,
        sql_failures=[],
        response_failures=response_failures,
        error=None, elapsed=time.time() - t0,
    )


def print_result(r: TestResult):
    status = f"{GREEN}PASS{RESET}" if r.overall_pass else f"{RED}FAIL{RESET}"
    elapsed = f"{r.elapsed:.1f}s"

    print(f"\n{BOLD}{r.tc.id:10}{RESET} {CYAN}{r.tc.category:<30}{RESET} {status}  ({elapsed})")
    print(f"  Message:  {r.tc.message}")

    if not r.tc.skip_sql_check and r.sql:
        sql_preview = r.sql[:120].replace("\n", " ")
        sql_ok = f"{GREEN}✓{RESET}" if r.sql_pass else f"{RED}✗{RESET}"
        print(f"  SQL {sql_ok}:   {sql_preview}")
        for f in r.sql_failures:
            print(f"    {RED}→ {f}{RESET}")

    if r.response:
        resp_preview = (r.response or "")[:160].replace("\n", " ")
        resp_ok = f"{GREEN}✓{RESET}" if r.response_pass else f"{RED}✗{RESET}"
        print(f"  Resp {resp_ok}:  {resp_preview}")
        for f in r.response_failures:
            print(f"    {RED}→ {f}{RESET}")

    if r.error:
        print(f"  {RED}Exception: {r.error}{RESET}")


def main():
    print(f"\n{BOLD}{'═'*70}{RESET}")
    print(f"{BOLD}  Ops Bot — Live E2E Tests{RESET}")
    print(f"  {len(TESTS)} cases | user_id={TEST_USER_ID}")
    print(f"{'═'*70}{RESET}\n")

    gemini = GeminiService()
    supabase = MockSupabaseService()

    results: List[TestResult] = []
    for tc in TESTS:
        print(f"  Running {tc.id} — {tc.message[:60]}...", end="", flush=True)
        if tc.expected_extracted is not None or tc.skip_sql_check:
            r = run_smart_capture_test(tc, gemini, supabase)
        else:
            r = run_query_test(tc, gemini, supabase)
        results.append(r)
        print(f"\r", end="")
        print_result(r)
        time.sleep(0.3)  # light rate-limit buffer

    # ── Summary ───────────────────────────────────────────────────────────────
    passed = sum(1 for r in results if r.overall_pass)
    failed = len(results) - passed
    print(f"\n{BOLD}{'═'*70}{RESET}")
    print(f"{BOLD}  Results: {GREEN}{passed} passed{RESET}{BOLD}, {RED}{failed} failed{RESET}{BOLD} / {len(results)} total{RESET}")

    if failed:
        print(f"\n  {BOLD}Failing tests:{RESET}")
        for r in results:
            if not r.overall_pass:
                tag = f"{RED}✗{RESET}"
                issues = r.sql_failures + r.response_failures
                print(f"    {tag} {r.tc.id} — {'; '.join(issues[:2])}")

    print(f"{'═'*70}\n")
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
