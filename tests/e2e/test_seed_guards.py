"""
Offline tests for tests/e2e/seed.py — deliberately NOT marked `live`.

The seeding itself needs a real database, but three things about it are
pure logic and belong in the default CI run:

  1. **The teardown safety guard.** seed.py holds the only DELETE
     statements in the suite. If its prefix guard ever regressed, a
     mis-scoped teardown could delete a real user's job history. That
     guard must be verified on every push, not only on nightly live runs.

  2. **Fixture/expectation consistency.** The derived constants
     (TOTAL_FEES, UNPAID_TOTAL, …) exist so scenario assertions can't drift
     from the seed data. Checking they actually agree with FIXTURE_ROWS is
     free and catches a bad edit immediately.

  3. **Fixture coverage.** Phase 4.1 specifies particular cases must exist
     (paid + unpaid, several clients, several months, a no-poc-email
     client, a no-job-date row). A future edit that quietly drops one would
     silently weaken every scenario built on it.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

import pytest

from tests.e2e import seed as seed_mod
from tests.e2e.seed import (
    E2E_USER_PREFIX, FIXTURE_ROWS, FEES_BY_CLIENT, UNPAID_BY_CLIENT,
    ROW_COUNT, TOTAL_FEES, PAID_TOTAL, UNPAID_TOTAL, PAID_COUNT, UNPAID_COUNT,
    CLIENTS, CLIENT_COUNT, BIGGEST_CLIENT, NO_EMAIL_COUNT, NO_DATE_COUNT,
    BILL_SENT_COUNT, BILL_NOT_SENT_COUNT,
    new_user_id, teardown, _assert_synthetic,
)


class TestTeardownSafetyGuard:
    """The one piece of the harness that issues DELETEs against a
    production table. It must be incapable of targeting a real user."""

    @pytest.mark.parametrize("real_looking_id", [
        "919876543210",              # a WhatsApp phone number
        "whatsapp:+919876543210",    # …in Twilio's form
        "751256859",                 # a Telegram chat_id
        "admin",
        "e2etest",                   # prefix WITHOUT the colon — not ours
        "not-e2etest:123",           # prefix present but not at the start
        "",
    ])
    def test_rejects_non_synthetic_ids(self, real_looking_id):
        with pytest.raises(ValueError, match="non-synthetic"):
            _assert_synthetic(real_looking_id)

    def test_rejects_none(self):
        with pytest.raises(ValueError, match="non-synthetic"):
            _assert_synthetic(None)

    def test_accepts_synthetic_id(self):
        _assert_synthetic(new_user_id())  # must not raise

    def test_teardown_raises_before_opening_a_connection(self, monkeypatch):
        """The guard must fire BEFORE any DB work. If it were checked after
        connecting, a teardown against a bad id on a misconfigured URL
        could still reach the database."""
        connect_calls = []
        monkeypatch.setattr(
            seed_mod, "_connect",
            lambda: connect_calls.append(1) or pytest.fail("connected before guard ran"),
        )
        with pytest.raises(ValueError, match="non-synthetic"):
            teardown("919876543210")
        assert connect_calls == []

    def test_seed_also_guards(self, monkeypatch):
        monkeypatch.setattr(
            seed_mod, "_connect",
            lambda: pytest.fail("connected before guard ran"),
        )
        with pytest.raises(ValueError, match="non-synthetic"):
            seed_mod.seed("919876543210")


class TestSyntheticIds:
    def test_has_expected_prefix(self):
        assert new_user_id().startswith(E2E_USER_PREFIX)

    def test_unique_per_call(self):
        assert len({new_user_id() for _ in range(50)}) == 50


class TestDerivedExpectationsMatchFixture:
    """Derived constants must agree with FIXTURE_ROWS — that agreement is
    the whole reason scenario assertions reference them instead of
    literals."""

    def test_row_count(self):
        assert ROW_COUNT == len(FIXTURE_ROWS) == 15

    def test_totals_partition_cleanly(self):
        assert PAID_TOTAL + UNPAID_TOTAL == TOTAL_FEES
        assert PAID_COUNT + UNPAID_COUNT == ROW_COUNT
        assert BILL_SENT_COUNT + BILL_NOT_SENT_COUNT == ROW_COUNT

    def test_total_matches_manual_sum(self):
        assert TOTAL_FEES == sum(r["fees"] for r in FIXTURE_ROWS)

    def test_per_client_totals_sum_to_grand_total(self):
        assert sum(FEES_BY_CLIENT.values()) == TOTAL_FEES

    def test_per_client_unpaid_sums_to_unpaid_total(self):
        assert sum(UNPAID_BY_CLIENT.values()) == UNPAID_TOTAL

    def test_biggest_client_really_is_biggest(self):
        assert FEES_BY_CLIENT[BIGGEST_CLIENT] == max(FEES_BY_CLIENT.values())

    def test_biggest_client_is_unambiguous(self):
        """A tie would make "who is my biggest client?" untestable — the
        bot could answer either name and both would be right."""
        top = max(FEES_BY_CLIENT.values())
        assert sum(1 for v in FEES_BY_CLIENT.values() if v == top) == 1

    def test_client_count(self):
        assert CLIENT_COUNT == len(CLIENTS) == 3


class TestFixtureCoverage:
    """Phase 4.1's required coverage cases."""

    def test_has_both_paid_and_unpaid(self):
        assert PAID_COUNT > 0 and UNPAID_COUNT > 0

    def test_has_multiple_clients(self):
        assert CLIENT_COUNT >= 3

    def test_has_a_client_with_no_poc_email(self):
        assert NO_EMAIL_COUNT > 0
        no_email_clients = {r["client_name"] for r in FIXTURE_ROWS if not r["poc_email"]}
        # Must be a client with NO email on ANY row — a client with a mix
        # wouldn't exercise "clients with no email" the way it's meant to.
        for client in no_email_clients:
            rows = [r for r in FIXTURE_ROWS if r["client_name"] == client]
            assert all(not r["poc_email"] for r in rows), (
                f"{client} has a mix of null and non-null poc_email — "
                f"the 'no email' fixture case needs a client with none at all"
            )

    def test_has_a_row_with_no_job_date(self):
        assert NO_DATE_COUNT == 1

    def test_spans_multiple_months(self):
        months = {r["job_date"][:7] for r in FIXTURE_ROWS if r["job_date"]}
        assert len(months) >= 3, f"expected >=3 distinct months, got {sorted(months)}"

    def test_has_both_sent_and_unsent_invoices(self):
        assert BILL_SENT_COUNT > 0 and BILL_NOT_SENT_COUNT > 0

    def test_every_row_has_fees(self):
        """Aggregates (sum/avg) would be silently wrong with a null fee."""
        assert all(isinstance(r["fees"], int) and r["fees"] > 0 for r in FIXTURE_ROWS)
