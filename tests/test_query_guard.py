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
