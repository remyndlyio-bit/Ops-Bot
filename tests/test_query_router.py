"""
Tests for services/query_router.py — the deterministic-first query router.

The router is a PURE function (message → SQL + render kind), so these tests need
no DB, no LLM, no mocks. This is the whole point of the refactor: the common
query shapes are now verifiable in isolation, fast and deterministically.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from services.query_router import (
    route_common_query,
    format_client_list,
    format_payment_status,
    ROWS, AGGREGATE, CLIENT_LIST, PAYMENT_STATUS,
)

UID = "u1"


def _route(msg):
    return route_common_query(msg, UID)


# ── Routing: which route fires, and the SQL it produces ─────────────────────

class TestRouteSelection:
    def test_count_jobs(self):
        r = _route("How many jobs have I done?")
        assert r and r.name == "count_jobs" and r.render == AGGREGATE
        assert "count(*)" in r.sql.lower()

    def test_total_fees(self):
        r = _route("What is my total billing?")
        assert r and r.name == "total_fees" and r.render == AGGREGATE
        assert "sum(fees)" in r.sql.lower()

    def test_average_fees(self):
        r = _route("average fees per job")
        assert r and r.name == "average_fees"
        assert "avg(fees)" in r.sql.lower()

    def test_list_jobs(self):
        r = _route("Show all my jobs")
        assert r and r.name == "list_jobs" and r.render == ROWS
        assert "select *" in r.sql.lower() and "limit 25" in r.sql.lower()

    def test_last_job(self):
        r = _route("what was my last job")
        assert r and r.name == "last_job"
        assert "order by job_date desc" in r.sql.lower() and "limit 1" in r.sql.lower()

    def test_unpaid_list(self):
        r = _route("show me unpaid invoices")
        assert r and r.name == "unpaid_list" and r.render == ROWS

    def test_list_clients(self):
        r = _route("Show all my clients")
        assert r and r.name == "list_clients" and r.render == CLIENT_LIST
        assert "distinct" in r.sql.lower()

    def test_biggest_client(self):
        r = _route("Who is my biggest client?")
        assert r and r.name == "biggest_client" and r.render == ROWS
        assert "group by" in r.sql.lower() and "sum(fees)" in r.sql.lower()

    def test_earnings_by_client(self):
        r = _route("show earnings by client")
        assert r and r.name == "earnings_by_client"
        assert "group by" in r.sql.lower()

    def test_payment_status(self):
        r = _route("Has Nike paid?")
        assert r and r.name == "payment_status" and r.render == PAYMENT_STATUS
        assert r.meta["client"] == "nike"

    def test_client_owes(self):
        r = _route("How much does Star Studios owe me?")
        assert r and r.name == "client_owes" and r.render == AGGREGATE
        assert "sum(fees)" in r.sql.lower()
        # unpaid-only filter present
        assert "not in" in r.sql.lower()

    def test_date_lookup(self):
        r = _route("What did I do on 10 April?")
        assert r and r.name == "date_lookup"
        assert "job_date = '" in r.sql and "-04-10" in r.sql

    def test_hinglish_earnings(self):
        r = _route("kitna paisa aaya")
        assert r and r.name == "hinglish_earnings" and r.render == AGGREGATE


# ── The headline bug class: "highest paying job" must sort by FEES ──────────

class TestTopBottomJob:
    def test_highest_paying_sorts_by_fees_desc(self):
        r = _route("What was my highest paying job?")
        assert r and r.name == "top_bottom_job"
        assert "order by fees desc" in r.sql.lower()
        assert "order by job_date" not in r.sql.lower()

    def test_most_expensive_job(self):
        r = _route("most expensive job")
        assert r and r.name == "top_bottom_job"
        assert "order by fees desc" in r.sql.lower()

    def test_lowest_paying_sorts_asc(self):
        r = _route("my lowest paying job")
        assert r and r.name == "top_bottom_job"
        assert "order by fees asc" in r.sql.lower()

    def test_biggest_client_not_captured_as_job(self):
        """'biggest client' must route to the client aggregate, not the job intercept."""
        r = _route("who is my biggest client")
        assert r and r.name == "biggest_client"


# ── Guards: routes that must NOT fire (hand off to the planner) ─────────────

class TestNonMatches:
    @pytest.mark.parametrize("msg", [
        "Generate invoice for Nike",
        "Add a job for Acme, 25k, shoot",
        "What about this month?",
        "Can you book me an Uber?",
        "earnings last quarter",         # date-qualified sum → planner
        "total billing this year",       # date-qualified → planner
        "",
    ])
    def test_returns_none(self, msg):
        assert _route(msg) is None

    def test_count_clients_not_list(self):
        """'how many clients' is a count — must NOT hit the DISTINCT-list route."""
        r = _route("how many clients do I have")
        assert r is None or r.render != CLIENT_LIST


class TestScopeQualifierGuard:
    """Unfiltered aggregate/list routes must DEFER to the planner when a date
    period or specific client narrows the query — otherwise they return the
    grand total / all rows and silently drop the filter."""

    @pytest.mark.parametrize("msg", [
        "Total billing for Nike",          # client filter
        "Total billing this year",         # date filter
        "How many jobs this quarter?",     # date filter
        "How many jobs for Samsung",       # client filter
        "average fee for Garnier",         # client filter
        "Show jobs for Nike",              # client filter
        "Show my jobs for March",          # month filter
        "List all jobs this quarter",      # date filter
        "earnings in April",               # month filter
    ])
    def test_qualified_aggregates_defer_to_planner(self, msg):
        r = _route(msg)
        assert r is None or r.name not in (
            "total_fees", "count_jobs", "average_fees", "list_jobs"
        ), f"{msg!r} wrongly matched unfiltered route {r and r.name} (dropped the filter)"

    @pytest.mark.parametrize("msg,route", [
        ("How many jobs have I done?", "count_jobs"),
        ("Total billing", "total_fees"),
        ("what's my average fee", "average_fees"),
        ("Show all my jobs", "list_jobs"),
    ])
    def test_unqualified_aggregates_still_fire(self, msg, route):
        r = _route(msg)
        assert r is not None and r.name == route, (
            f"{msg!r} should still route to {route}, got {r and r.name}"
        )


# ── SQL safety: user_id is escaped, no obvious injection surface ─────────────

class TestSqlSafety:
    def test_user_id_quote_escaped(self):
        r = route_common_query("show all my jobs", "u1'; DROP TABLE x;--")
        assert r is not None
        # The single quote must be doubled (escaped), not left raw.
        assert "u1''; DROP TABLE x;--" in r.sql

    def test_client_name_quote_escaped(self):
        r = route_common_query("Has O'Brien paid?", UID)
        assert r is not None
        assert "o''brien" in r.sql.lower()


# ── Deterministic renderers ─────────────────────────────────────────────────

class TestRenderers:
    def test_client_list_all(self):
        out = format_client_list([{"client_name": "Nike"}, {"client_name": "Samsung"}], "all")
        assert "Your clients:" in out and "• Nike" in out and "• Samsung" in out

    def test_client_list_empty(self):
        assert "don't have any clients" in format_client_list([], "all").lower()

    def test_client_list_unpaid_header(self):
        out = format_client_list([{"client_name": "Acme"}], "unpaid")
        assert "haven't paid" in out.lower() and "• Acme" in out

    def test_payment_status_paid_in_full(self):
        rows = [{"client_name": "Nike", "fees": 30000, "paid": "Yes"}]
        out = format_payment_status(rows, {"client": "nike"})
        assert "paid in full" in out.lower() and "30,000" in out

    def test_payment_status_unpaid(self):
        rows = [{"client_name": "Nike", "fees": 30000, "paid": "No"}]
        out = format_payment_status(rows, {"client": "nike"})
        assert "hasn't paid" in out.lower() and "30,000" in out

    def test_payment_status_partial(self):
        rows = [
            {"client_name": "Nike", "fees": 30000, "paid": "Yes"},
            {"client_name": "Nike", "fees": 20000, "paid": "No"},
        ]
        out = format_payment_status(rows, {"client": "nike"})
        assert "30,000" in out and "20,000" in out and "outstanding" in out.lower()

    def test_payment_status_no_rows(self):
        out = format_payment_status([], {"client": "ghost"})
        assert "don't have any jobs" in out.lower()
