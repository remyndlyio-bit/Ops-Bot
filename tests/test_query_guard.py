"""
Unit tests for services/query_guard.sql_reflects_message — the message<->SQL
consistency check. It must REJECT SQL that drops a qualifier the user stated, and
ACCEPT SQL that honours every qualifier (so it never blocks a correct route).
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from services.query_guard import sql_reflects_message as chk

COUNT_ALL = "SELECT COUNT(*) AS result FROM public.job_entries WHERE user_id='u' AND (\"isDeleted\" IS NOT TRUE)"
COUNT_PAID = COUNT_ALL + " AND LOWER(COALESCE(paid,'')) IN ('true','t','yes','1','paid')"
SUM_ALL = "SELECT SUM(fees) AS result FROM public.job_entries WHERE user_id='u' AND (\"isDeleted\" IS NOT TRUE)"
SUM_UNPAID = SUM_ALL + " AND (paid IS NULL OR LOWER(paid) NOT IN ('true','t','yes','1','paid'))"
SELECT_ALL = "SELECT * FROM public.job_entries WHERE user_id='u' AND (\"isDeleted\" IS NOT TRUE) ORDER BY job_date DESC LIMIT 25"
SELECT_UNPAID = "SELECT * FROM public.job_entries WHERE user_id='u' AND (paid IS NULL OR LOWER(paid) NOT IN ('true','t','yes','1','paid')) LIMIT 25"
SELECT_CLIENT = "SELECT * FROM public.job_entries WHERE user_id='u' AND client_name ILIKE '%garnier%' LIMIT 25"
SUM_CLIENT_UNPAID = "SELECT SUM(fees) AS result FROM public.job_entries WHERE user_id='u' AND (COALESCE(client_name,'') ILIKE '%star%') AND (paid IS NULL OR LOWER(paid) NOT IN ('yes'))"


class TestGuardRejects:
    """The exact bug class: SQL that ignores a stated qualifier."""

    def test_unpaid_count_without_paid_filter(self):
        ok, why = chk("How many unpaid jobs do I have?", COUNT_ALL)
        assert not ok and "paid" in why

    def test_how_much_unpaid_returned_as_rows(self):
        ok, why = chk("How much money is still unpaid?", SELECT_UNPAID)
        assert not ok  # value question answered with a row list

    def test_count_question_returned_as_rows(self):
        ok, _ = chk("How many unpaid jobs?", SELECT_UNPAID)
        assert not ok

    def test_client_jobs_without_client_filter(self):
        ok, why = chk("Show me Garnier jobs", SELECT_ALL)
        assert not ok and "garnier" in why.lower()

    def test_samsung_jobs_without_filter(self):
        ok, _ = chk("Show me Samsung jobs", SELECT_ALL)
        assert not ok

    def test_date_without_date_predicate(self):
        ok, _ = chk("How many jobs in March 2026?", COUNT_ALL)
        assert not ok


class TestGuardRejectsDispatchStatus:
    """P1 fix: the guard had NO vocabulary for invoice-dispatch language
    ("yet to invoice", "invoices sent") — only paid/unpaid. Confirmed live:
    'Who are you yet to send the invoice?' returned all 4 jobs unfiltered
    (3 of which already had an invoice date) and nothing caught it."""

    def test_the_exact_production_bug(self):
        ok, why = chk("Who are you yet to send the invoice?", SELECT_ALL)
        assert not ok and "invoice" in why.lower()

    def test_invoices_havent_gone_out(self):
        ok, _ = chk("which invoices haven't gone out yet", SELECT_ALL)
        assert not ok

    def test_invoices_sent_count_without_bill_sent(self):
        ok, _ = chk("how many invoices have I sent?", COUNT_ALL)
        assert not ok

    def test_still_pending_to_send(self):
        ok, _ = chk("invoices still pending to send", SELECT_ALL)
        assert not ok

    def test_with_bill_sent_filter_passes(self):
        sql = SELECT_ALL.replace("LIMIT 25", "AND bill_sent IS NULL LIMIT 25")
        assert chk("Who are you yet to send the invoice?", sql)[0]

    def test_sent_count_with_bill_sent_filter_passes(self):
        sql = COUNT_ALL + " AND LOWER(COALESCE(bill_sent,'')) IN ('true','t','yes','1','sent')"
        assert chk("how many invoices have I sent?", sql)[0]


class TestDispatchGuardNoFalsePositive:
    """The invoice/bill NOUN alone (no dispatch verb) must NOT trigger — asking
    for an invoice NUMBER or invoice DATE isn't asking about sent/pending
    status, so requiring bill_sent there would be a false clarification."""

    def test_invoice_number_lookup_not_flagged(self):
        sql = "SELECT bill_no FROM public.job_entries WHERE user_id='u' AND client_name ILIKE '%wilson%'"
        assert chk("what's the invoice number for Wilson", sql)[0]

    def test_invoice_date_lookup_not_flagged(self):
        sql = "SELECT invoice_date FROM public.job_entries WHERE user_id='u' AND client_name ILIKE '%nike%'"
        assert chk("what's the invoice date for Nike", sql)[0]

    def test_bare_list_not_flagged(self):
        assert chk("show me all my jobs", SELECT_ALL)[0]


class TestGuardAccepts:
    """Correct routes must pass untouched."""

    def test_bare_count(self):
        assert chk("How many jobs do I have?", COUNT_ALL)[0]

    def test_bare_total(self):
        assert chk("What's my total billing?", SUM_ALL)[0]

    def test_paid_count_with_filter(self):
        assert chk("How many jobs have I been paid for?", COUNT_PAID)[0]

    def test_unpaid_list_is_a_list(self):
        # "list unpaid invoices" is a genuine list request — must NOT be rejected
        assert chk("List my unpaid invoices", SELECT_UNPAID)[0]

    def test_client_jobs_with_filter(self):
        assert chk("Show me Garnier jobs", SELECT_CLIENT)[0]

    def test_client_owes_with_client_and_paid(self):
        assert chk("How much does Star owe me?", SUM_CLIENT_UNPAID)[0]

    def test_known_client_sharpens_detection(self):
        # "What about Acme Corp?" has no job-noun and no for/from, so the heuristic
        # alone misses the client — a known-client list catches it.
        msg = "What about Acme Corp?"
        assert chk(msg, SELECT_ALL)[0]  # heuristic alone: no client detected → passes
        # With the known-client list, an unfiltered SELECT is now rejected…
        assert not chk(msg, SELECT_ALL, known_clients=["Acme Corp"])[0]
        # …and the client-filtered SELECT is accepted.
        assert chk(msg, SELECT_CLIENT, known_clients=["Acme Corp"])[0]

    def test_bare_list_jobs_ok(self):
        assert chk("Show me all my jobs", SELECT_ALL)[0]


class TestStrictClientMode:
    """Fail-closed callers (Layer 2) disable the loose heuristic to avoid false
    clarifications — only the known-client list flags a client."""

    def test_heuristic_off_ignores_unknown_noun(self):
        # "freelance jobs" would trip the heuristic, but it's not a known client.
        assert chk("show me freelance jobs", SELECT_ALL, use_heuristic_client=False)[0]

    def test_strict_flags_known_client(self):
        ok, why = chk("show me garnier jobs", SELECT_ALL,
                      known_clients=["garnier"], use_heuristic_client=False)
        assert not ok and "garnier" in why.lower()

    def test_strict_still_enforces_status_and_count(self):
        assert not chk("how many unpaid jobs", COUNT_ALL, use_heuristic_client=False)[0]
        assert not chk("how much is unpaid", SELECT_UNPAID, use_heuristic_client=False)[0]


class TestLayer2Gate:
    """The intent_service Layer-2 gate: cached known-clients + fail-closed decision."""

    def _svc(self, names):
        from unittest.mock import patch, MagicMock
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.supabase = MagicMock()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"n": n} for n in names]}
        return svc

    def test_known_clients_cached(self):
        svc = self._svc(["garnier india", "garnier", "samsung"])
        kc = svc._known_clients("u1")
        assert "garnier" in kc and "samsung" in kc
        svc._known_clients("u1")  # second call hits the cache
        assert svc.supabase.execute_sql.call_count == 1

    def test_gate_blocks_dropped_client(self):
        svc = self._svc(["garnier india", "garnier"])
        ok, _ = svc._planner_sql_ok("show me garnier jobs", SELECT_ALL, "u1")
        assert not ok

    def test_gate_passes_filtered_sql(self):
        svc = self._svc(["garnier india", "garnier"])
        ok, _ = svc._planner_sql_ok("show me garnier jobs", SELECT_CLIENT, "u1")
        assert ok

    def test_gate_no_false_positive_on_bare_query(self):
        svc = self._svc(["garnier india", "garnier"])
        ok, _ = svc._planner_sql_ok("show me all my jobs", SELECT_ALL, "u1")
        assert ok
