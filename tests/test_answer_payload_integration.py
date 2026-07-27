"""
WP-4 integration tests — the raw-card-dump fix, through the REAL
process_request cascade (router path AND LLM-planner path), not just
services/response_synthesis.py in isolation.

The behaviour this locks down: a FILTERED/scoped query (status question,
client lookup) must render as a headline+scope+support summary, not a raw
"Invoice No: / Invoice Date:" card dump (the IMG-3 confusion). An UNFILTERED
bulk "show all my jobs" ask must still get raw cards — that's the correct UX
for a genuine export and must not regress.

Before this file, NO test in the suite asserted on this distinction at all
(grep for "Invoice No:"/"_format_job_cards" across tests/ turns up only a
docstring comment) — the pre-existing suite could not have caught this
behaviour change either direction.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from unittest.mock import patch, MagicMock


class FakeMemory:
    def __init__(self):
        self._store = {}

    def get_user_memory(self, uid):
        return dict(self._store.get(uid, {}))

    def update_user_memory(self, uid, patch):
        self._store.setdefault(uid, {}).update(patch)

    def get_form_state(self, uid):
        return None

    def get_conversation_history(self, uid):
        return []

    def cancel_form(self, uid):
        pass

    def add_message(self, uid, role, content):
        pass


def _svc():
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()
    svc.memory = FakeMemory()
    svc.supabase = MagicMock()
    svc.gemini = MagicMock()
    svc.supabase.get_user_profile.return_value = {
        "ok": True, "data": {"onboarded_at": "2024-01-01", "name": "D"},
    }
    svc.gemini.is_invoice_action_request.return_value = False
    return svc


WILSON_ROW = {
    "id": "w1", "client_name": "Wilson ducktape", "brand_name": "kalaapuri",
    "poc_name": None, "poc_email": None, "fees": 40000, "paid": None,
    "bill_sent": None, "bill_no": "WIL-020726-01", "job_date": "2026-07-02",
}


class TestRouterPathFilteredQueryNoRawCards:
    """The router's unpaid_list route (and every scoped route besides
    list_jobs) must render the scoped summary, not raw cards."""

    def test_unpaid_list_route_gets_scoped_summary_not_raw_cards(self):
        svc = _svc()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [WILSON_ROW], "operation": "select"}
        svc.gemini.synthesize_response.return_value = ""  # force the fallback path
        r = svc.process_request("u1", "show unpaid invoices")
        assert "Invoice No:" not in r["response"]
        assert "Invoice Date:" not in r["response"]
        assert "Wilson ducktape" in r["response"]
        assert "WIL-020726-01" in r["response"]  # still surfaced, just not as a raw card

    def test_scoped_summary_states_the_scope(self):
        svc = _svc()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [WILSON_ROW], "operation": "select"}
        svc.gemini.synthesize_response.return_value = ""
        r = svc.process_request("u1", "show unpaid invoices")
        assert "unpaid" in r["response"].lower()

    def test_bulk_list_jobs_route_still_uses_raw_cards(self):
        """'show all my jobs' (unfiltered) is a genuine export -- must NOT
        regress to the summary renderer."""
        svc = _svc()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [WILSON_ROW], "operation": "select"}
        svc.gemini.synthesize_response.return_value = ""
        r = svc.process_request("u1", "show all my jobs")
        assert "Invoice No:" in r["response"]
        assert "Invoice Date:" in r["response"]


class TestPlannerPathFilteredQueryNoRawCards:
    """Same behaviour via the LLM-planner path (execute_query_plan)."""

    def _mock_plan(self, filters, group_by=None):
        return {
            "sql": "SELECT * FROM public.job_entries WHERE user_id='u1'",
            "plan": {"metric": None, "column": None, "filters": filters,
                     "time_range": None, "group_by": group_by, "order": None, "limit": None},
            "classification": {"operation": "query", "confidence": "high"},
            "clarification": None, "_error": None,
        }

    def test_client_filtered_query_gets_scoped_summary(self):
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name", "fees", "bill_no", "job_date"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = self._mock_plan({"client_name": "Wilson ducktape"})
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [WILSON_ROW], "operation": "select"}
            svc.gemini.synthesize_response.return_value = ""
            r = svc.process_request("u1", "do you have the recipient email for the Wilson job")
        assert "Invoice No:" not in r["response"]
        assert "Wilson ducktape" in r["response"]

    def test_unfiltered_query_still_uses_raw_cards(self):
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "client_name", "fees", "bill_no", "job_date"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = self._mock_plan({})
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [WILSON_ROW], "operation": "select"}
            svc.gemini.synthesize_response.return_value = ""
            r = svc.process_request("u1", "show me my jobs")
        assert "Invoice No:" in r["response"]


class TestAggregateFallbackUsesPlanNotKeywordGuess:
    """The old _format_aggregate_fallback guessed count-vs-money from message
    keywords ("how many" etc). The new one reads plan.metric directly -- must
    get an unusually-phrased count question right where a keyword guess could
    plausibly have failed."""

    def test_count_metric_correct_even_with_unusual_phrasing(self):
        svc = _svc()
        svc.supabase.get_schema.return_value = {
            "table": "job_entries", "schema_name": "public",
            "columns": ["id", "fees"], "description": "x",
        }
        with patch("services.intent_service.execute_query_plan") as mock_exec:
            mock_exec.return_value = {
                "sql": "SELECT COUNT(*) AS result FROM public.job_entries WHERE user_id='u1'",
                "plan": {"metric": "count", "column": None, "filters": {}, "time_range": None,
                         "group_by": None, "order": None, "limit": None},
                "classification": {"operation": "query", "confidence": "high"},
                "clarification": None, "_error": None,
            }
            svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"result": 7}], "operation": "select"}
            svc.gemini.synthesize_response.return_value = ""
            # Deliberately doesn't contain "how many"/"count"/"kitne" -- the
            # keywords the OLD fallback needed to guess "count" correctly.
            r = svc.process_request("u1", "tally up my active engagements this cycle")
        assert "7 job" in r["response"]
