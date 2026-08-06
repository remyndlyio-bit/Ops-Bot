"""
P2-3 (PLAN_OF_ACTION.md): a >4-row list query used to answer with "Found 20
results — here's a spreadsheet with all of them" and nothing else — the
whole answer sat in an attachment, and chat had nothing to actually read.
_list_result_message is a pure function (no mocking needed): given the rows
a >4-row query pipeline branch was about to hand to _generate_jobs_excel, it
returns the chat-visible text that goes alongside that spreadsheet.
"""
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from services.intent_service import _list_result_message


def _job_row(i):
    return {
        "id": f"r{i}",
        "client_name": f"Client{i}",
        "brand_name": None,
        "poc_name": f"POC{i}",
        "poc_email": f"poc{i}@test.com",
        "fees": 10000 * i,
        "job_date": "2026-04-10",
        "invoice_date": None,
        "bill_no": f"INV-{i}",
    }


class TestFullJobRowsShowCardsAndCount:
    def test_shows_the_first_five_as_cards(self):
        rows = [_job_row(i) for i in range(1, 9)]  # 8 rows
        msg = _list_result_message(rows)
        for i in range(1, 6):
            assert f"Client{i}" in msg
        for i in range(6, 9):
            assert f"Client{i}" not in msg

    def test_mentions_the_total_count(self):
        rows = [_job_row(i) for i in range(1, 9)]
        msg = _list_result_message(rows)
        assert "8" in msg

    def test_says_how_many_more_are_in_the_spreadsheet(self):
        rows = [_job_row(i) for i in range(1, 9)]  # 8 rows -> 3 more beyond the 5 shown
        msg = _list_result_message(rows)
        assert "3 more" in msg

    def test_exactly_five_rows_has_no_more_count(self):
        rows = [_job_row(i) for i in range(1, 6)]  # exactly 5
        msg = _list_result_message(rows)
        assert "more" not in msg.lower()
        assert "attached" in msg.lower()

    def test_card_contains_real_field_values_not_placeholders(self):
        rows = [_job_row(i) for i in range(1, 6)]
        msg = _list_result_message(rows)
        assert "₹" in msg  # fees rendered as currency, not left blank
        assert "POC1" in msg and "poc1@test.com" in msg
        assert "INV-1" in msg

    def test_response_is_not_spreadsheet_only(self):
        """The specific regression: the reply must not be JUST the count +
        spreadsheet notice with nothing else to read in chat."""
        rows = [_job_row(i) for i in range(1, 9)]
        msg = _list_result_message(rows)
        assert len(msg) > len("Found 8 results — here's a spreadsheet with all of them.")


class TestNonJobRowsFallBackToPlainCount:
    """GROUP BY / aggregate-shaped rows don't have the fields
    _format_job_card needs (poc_name, invoice_date, bill_no, ...) — cards
    for those would render mostly em-dash placeholders, so this falls back
    to the plain count instead of pretending to show something useful."""

    def test_grouped_rows_get_plain_count_not_garbled_cards(self):
        rows = [{"client_name": f"Client{i}", "result": 10000 * i} for i in range(1, 8)]
        msg = _list_result_message(rows)
        assert msg == "Found 7 results — here's a spreadsheet with all of them."

    def test_empty_rows_does_not_crash(self):
        msg = _list_result_message([])
        assert "0 results" in msg
