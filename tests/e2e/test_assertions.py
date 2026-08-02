"""
Offline tests for tests/e2e/assertions.py — NOT marked `live`, so they run
in normal CI.

Testing the test harness sounds circular, but it is the opposite: these
primitives decide what "the bot is working" MEANS. A buggy assertion is
worse than a missing one — `contains_amount` that silently never matches
would turn the whole e2e suite green while the product burns, and
`no_error` that misses the bot's real failure phrasing would do the same.

Emphasis is on the false-failure and false-PASS traps specifically:
Indian vs Western digit grouping, smart vs straight apostrophes, substring
collisions between amounts, and DB assertions with no database wired.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import pytest

from tests.e2e.assertions import (
    ScenarioContext, Outcome,
    contains, not_contains, matches, contains_number, contains_amount,
    contains_currency, no_error, operation_is, operation_in,
    row_created, row_count_is, all_of, any_of, run_assertions,
    normalise_text, normalise_digits, ERROR_PHRASES,
)


def ctx(response="", operation="query", db=None, user_id="e2etest:abc"):
    return ScenarioContext(
        result={"response": response, "operation": operation},
        user_id=user_id, db=db,
    )


class TestNormalisation:
    def test_digit_commas_stripped(self):
        assert normalise_digits("₹11,75,000") == "₹1175000"
        assert normalise_digits("₹1,175,000") == "₹1175000"

    def test_non_digit_commas_preserved(self):
        """A blanket comma strip would fuse list items and could create
        phantom number matches."""
        assert normalise_digits("3 jobs, 5 clients") == "3 jobs, 5 clients"

    def test_apostrophes_folded(self):
        assert normalise_text("couldn’t") == normalise_text("couldn't")

    def test_whitespace_and_case_folded(self):
        assert normalise_text("  Total   BILLING ") == "total billing"


class TestContains:
    def test_case_insensitive_by_default(self):
        assert contains("nike").check(ctx("Showing NIKE jobs")).passed

    def test_smart_apostrophe_matches_straight(self):
        assert contains("couldn't").check(ctx("Sorry, I couldn’t do that")).passed

    def test_case_sensitive_mode(self):
        assert not contains("nike", case_sensitive=True).check(ctx("NIKE")).passed

    def test_failure_detail_includes_actual_response(self):
        out = contains("Nike").check(ctx("Showing Acme jobs"))
        assert not out.passed
        assert "Acme" in out.detail  # diagnosable without a re-run


class TestNotContains:
    def test_passes_when_absent(self):
        assert not_contains("couldn't").check(ctx("Here are your 5 jobs")).passed

    def test_fails_when_present(self):
        assert not not_contains("couldn't").check(ctx("I couldn't find that")).passed

    def test_catches_smart_apostrophe_variant(self):
        """The bug this guards: the bot emits a curly apostrophe and the
        assertion reads green because it only knew the straight one."""
        assert not not_contains("couldn't").check(ctx("I couldn’t find that")).passed


class TestContainsNumber:
    @pytest.mark.parametrize("resp", ["You have 12 jobs", "₹25,000", "0 results"])
    def test_finds_digits(self, resp):
        assert contains_number().check(ctx(resp)).passed

    def test_fails_with_no_digits(self):
        assert not contains_number().check(ctx("No matching records")).passed


class TestContainsAmount:
    @pytest.mark.parametrize("resp", [
        "₹11,75,000",   # Indian grouping (what the bot emits)
        "₹1,175,000",   # Western grouping (what an LLM may emit)
        "1175000",      # raw
        "Total: ₹11,75,000 across 15 jobs",
    ])
    def test_matches_every_grouping_convention(self, resp):
        assert contains_amount(1175000).check(ctx(resp)).passed

    def test_does_not_match_a_different_number(self):
        assert not contains_amount(1175000).check(ctx("₹99,000")).passed

    def test_no_substring_collision(self):
        """2500 must NOT be 'found' just because 25000 is present — the
        false PASS that would let a wrong total slip through."""
        assert not contains_amount(2500).check(ctx("₹25,000")).passed

    def test_boundary_both_sides(self):
        assert not contains_amount(500).check(ctx("₹25,000")).passed
        assert contains_amount(500).check(ctx("₹500 due")).passed


class TestContainsCurrency:
    @pytest.mark.parametrize("resp", ["₹25,000", "Rs 25000", "INR 25000", "rs. 500"])
    def test_recognises_markers(self, resp):
        assert contains_currency().check(ctx(resp)).passed

    def test_fails_without_marker(self):
        assert not contains_currency().check(ctx("25000")).passed


class TestNoError:
    @pytest.mark.parametrize("phrase", ERROR_PHRASES)
    def test_catches_every_known_error_phrase(self, phrase):
        """Parametrised over the real list so adding a phrase to
        ERROR_PHRASES automatically gains coverage."""
        assert not no_error().check(ctx(f"Sorry, {phrase} right now")).passed

    def test_passes_on_a_real_answer(self):
        assert no_error().check(ctx("You have 15 jobs totalling ₹4,01,000")).passed

    def test_catches_the_known_synth_crash_message(self):
        """CLAUDE.md Bug 2's exact production symptom."""
        out = no_error().check(ctx("I found matching records but couldn't format the reply."))
        assert not out.passed


class TestOperation:
    def test_operation_is_match(self):
        assert operation_is("query").check(ctx(operation="query")).passed

    def test_operation_is_mismatch_reports_both(self):
        out = operation_is("query").check(ctx(operation="invoice_request"))
        assert not out.passed
        assert "query" in out.detail and "invoice_request" in out.detail

    def test_operation_in(self):
        assert operation_in(["query", "form_complete"]).check(ctx(operation="form_complete")).passed
        assert not operation_in(["query"]).check(ctx(operation="small_talk")).passed


class TestDbAssertionsFailLoudlyWithoutDb:
    """A DB assertion with no accessor must FAIL, not silently pass —
    a green no-op is worse than no assertion at all."""

    def test_row_created_without_db(self):
        out = row_created(client="Acme").check(ctx(db=None))
        assert not out.passed
        assert "ScenarioContext.db is None" in out.detail

    def test_row_count_without_db(self):
        out = row_count_is(3).check(ctx(db=None))
        assert not out.passed
        assert "ScenarioContext.db is None" in out.detail


class TestDbAssertionsWithFakeDb:
    def test_row_created_found(self):
        calls = []
        def fake_db(sql, params):
            calls.append((sql, params))
            return [{"client_name": "Acme", "fees": 25000, "paid": "Yes"}]
        assert row_created(client="Acme").check(ctx(db=fake_db)).passed

    def test_row_created_not_found(self):
        assert not row_created(client="Ghost").check(ctx(db=lambda s, p: [])).passed

    def test_always_scoped_to_the_scenario_user(self):
        """Must never be able to read another account's rows."""
        captured = {}
        def fake_db(sql, params):
            captured["sql"], captured["params"] = sql, params
            return []
        row_created(client="Acme").check(ctx(db=fake_db, user_id="e2etest:xyz"))
        assert "user_id = %s" in captured["sql"]
        assert captured["params"][0] == "e2etest:xyz"

    def test_excludes_soft_deleted_rows(self):
        captured = {}
        def fake_db(sql, params):
            captured["sql"] = sql
            return []
        row_created(client="Acme").check(ctx(db=fake_db))
        assert "isDeleted" in captured["sql"]

    def test_row_count_matches(self):
        assert row_count_is(3).check(ctx(db=lambda s, p: [{"n": 3}])).passed

    def test_row_count_mismatch_reports_actual(self):
        out = row_count_is(3).check(ctx(db=lambda s, p: [{"n": 5}]))
        assert not out.passed
        assert "found 5" in out.detail


class TestCombinators:
    def test_all_of_passes(self):
        assert all_of(contains_number(), no_error()).check(ctx("15 jobs")).passed

    def test_all_of_names_the_failing_sub(self):
        out = all_of(contains_number(), contains("Nike")).check(ctx("15 jobs"))
        assert not out.passed
        assert "contains('Nike')" in out.detail

    def test_any_of_passes_on_one(self):
        assert any_of(contains("Nike"), contains_number()).check(ctx("15 jobs")).passed

    def test_any_of_reports_all_failures(self):
        out = any_of(contains("Nike"), contains("Acme")).check(ctx("nothing here"))
        assert not out.passed
        assert "Nike" in out.detail and "Acme" in out.detail


class TestRunAssertions:
    def test_runs_all_not_short_circuiting(self):
        """A report showing all four checks with two failing beats one
        showing the first failure and hiding the rest."""
        results = run_assertions(
            ctx("15 jobs"),
            [contains_number(), contains("Nike"), no_error(), contains("Acme")],
        )
        assert len(results) == 4
        assert [r["passed"] for r in results] == [True, False, True, False]

    def test_a_raising_assertion_is_recorded_not_propagated(self):
        """One malformed scenario must not cost the other 133 results."""
        class Boom:
            description = "boom"
            def check(self, ctx):
                raise RuntimeError("kaboom")
        results = run_assertions(ctx("x"), [Boom(), contains_number()])
        assert results[0]["passed"] is False
        assert "RuntimeError" in results[0]["detail"]
        assert len(results) == 2  # the run continued

    def test_result_shape_is_report_ready(self):
        """Phase 4.3 serialises these straight to last_run.json."""
        import json
        results = run_assertions(ctx("15 jobs"), [contains_number()])
        assert set(results[0]) == {"assertion", "passed", "detail"}
        json.dumps(results)  # must be serialisable
