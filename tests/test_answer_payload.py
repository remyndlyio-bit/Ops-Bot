"""
WP-4 of ASSISTANT_PLAN.md — the Answer contract.

build_answer_payload() is deterministic (no LLM, no SQL) — headline and
scope_note are derived from the SAME {filters, time_range} shape WP-1's
AnswerLedger already stores, so the spoken scope can never disagree with
what actually ran. This is what fixes "no summary / no supporting details"
by construction rather than by hoping a prompt tweak works.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest

from services.response_synthesis import build_answer_payload, render_answer_payload, AnswerPayload


def _scope(filters=None, time_range=None):
    return {"filters": filters or {}, "time_range": time_range}


JOB_ROW = {"id": "1", "client_name": "Nike", "brand_name": "Star Studios",
           "fees": 25000, "paid": "Yes", "bill_sent": "Yes", "bill_no": "INV-001",
           "job_date": "2026-03-01"}


class TestHeadlineConstruction:
    def test_sum_metric_headline_is_indian_currency(self):
        p = build_answer_payload(scope=_scope(), metric="sum", rows=[{"result": 1175000}])
        assert p.headline == "₹11,75,000"

    def test_count_metric_headline_pluralised(self):
        p = build_answer_payload(scope=_scope(), metric="count", rows=[{"result": 7}])
        assert p.headline == "7 jobs"

    def test_count_metric_singular_not_pluralised(self):
        p = build_answer_payload(scope=_scope(), metric="count", rows=[{"result": 1}])
        assert p.headline == "1 job"

    def test_avg_metric_headline(self):
        p = build_answer_payload(scope=_scope(), metric="avg", rows=[{"result": 146875}])
        assert p.headline == "₹1,46,875 average"

    def test_grouped_client_headline_is_the_client_name(self):
        p = build_answer_payload(scope=_scope(), metric="sum", group_by="client_name",
                                  rows=[{"client_name": "Maruti Suzuki", "result": 950000}])
        assert p.headline == "Maruti Suzuki"

    def test_list_headline_is_row_count(self):
        rows = [dict(JOB_ROW, id=str(i)) for i in range(3)]
        p = build_answer_payload(scope=_scope(), metric=None, rows=rows)
        assert p.headline == "3 results"

    def test_list_headline_singular(self):
        p = build_answer_payload(scope=_scope(), metric=None, rows=[JOB_ROW])
        assert p.headline == "1 result"

    def test_zero_sum_headline(self):
        p = build_answer_payload(scope=_scope(), metric="sum", rows=[{"result": 0}])
        assert p.headline == "₹0"

    def test_no_rows_sum_defaults_to_zero(self):
        p = build_answer_payload(scope=_scope(), metric="sum", rows=[])
        assert p.headline == "₹0"


class TestScopeNoteMatchesExecutedFilters:
    """The core correctness property: scope_note must always describe the
    SAME filters that were passed in, never a guess from message text."""

    def test_no_filters_says_both_paid_and_unpaid_all_time(self):
        p = build_answer_payload(scope=_scope(), metric="sum", rows=[{"result": 1}])
        assert "both paid and unpaid" in p.scope_note
        assert "all time" in p.scope_note

    def test_unpaid_filter_reflected(self):
        p = build_answer_payload(scope=_scope({"paid": "no"}), metric="sum", rows=[{"result": 1}])
        assert "unpaid" in p.scope_note.lower() and "only the UNPAID" in p.scope_note

    def test_client_filter_reflected(self):
        p = build_answer_payload(scope=_scope({"client_name": "Nike"}), metric="sum", rows=[{"result": 1}])
        assert "Nike" in p.scope_note

    def test_time_range_reflected(self):
        tr = {"type": "absolute", "value": {"start": "2026-01-01", "end": "2026-03-31"}}
        p = build_answer_payload(scope=_scope(time_range=tr), metric="sum", rows=[{"result": 1}])
        assert "2026-01-01" in p.scope_note

    def test_scope_note_never_disagrees_with_the_filters_it_was_given(self):
        """Property check across a spread of filter combos — scope_note must
        always be DERIVED from (not independent of) the passed-in scope."""
        combos = [
            {}, {"paid": "yes"}, {"paid": "no"}, {"client_name": "Acme"},
            {"bill_sent": "yes"}, {"bill_sent": "no"}, {"client_name": "Acme", "paid": "no"},
        ]
        for filters in combos:
            p = build_answer_payload(scope=_scope(filters), metric="sum", rows=[{"result": 100}])
            if filters.get("paid") == "no":
                assert "unpaid" in p.scope_note.lower()
            elif filters.get("paid") == "yes":
                assert "PAID" in p.scope_note
            if filters.get("client_name"):
                assert filters["client_name"] in p.scope_note


class TestSupportRows:
    def test_support_extracted_from_full_job_rows(self):
        p = build_answer_payload(scope=_scope(), metric=None, rows=[JOB_ROW])
        assert len(p.support) == 1
        s = p.support[0]
        assert s["client"] == "Nike" and s["bill_no"] == "INV-001"
        assert s["paid"] == "paid" and s["bill_sent"] == "invoiced"

    def test_unpaid_and_not_sent_rendered_correctly(self):
        row = dict(JOB_ROW, paid=None, bill_sent=None)
        p = build_answer_payload(scope=_scope(), metric=None, rows=[row])
        assert p.support[0]["paid"] == "unpaid"
        assert p.support[0]["bill_sent"] == "not invoiced"

    def test_support_capped_at_3(self):
        rows = [dict(JOB_ROW, id=str(i)) for i in range(10)]
        p = build_answer_payload(scope=_scope(), metric=None, rows=rows)
        assert len(p.support) == 3

    def test_remainder_counts_the_rest(self):
        rows = [dict(JOB_ROW, id=str(i)) for i in range(10)]
        p = build_answer_payload(scope=_scope(), metric=None, rows=rows)
        assert p.remainder == 7

    def test_no_remainder_when_3_or_fewer(self):
        rows = [dict(JOB_ROW, id=str(i)) for i in range(2)]
        p = build_answer_payload(scope=_scope(), metric=None, rows=rows)
        assert p.remainder == 0

    def test_pure_aggregate_row_has_no_support(self):
        """A {"result": N} row is not a job row -- nothing to show as support."""
        p = build_answer_payload(scope=_scope(), metric="sum", rows=[{"result": 500000}])
        assert p.support == []
        assert p.remainder == 0

    def test_amount_formatted_indian_style(self):
        row = dict(JOB_ROW, fees=1175000)
        p = build_answer_payload(scope=_scope(), metric=None, rows=[row])
        assert p.support[0]["amount"] == "₹11,75,000"

    def test_missing_fees_shown_as_dash(self):
        row = dict(JOB_ROW, fees=None)
        p = build_answer_payload(scope=_scope(), metric=None, rows=[row])
        assert p.support[0]["amount"] == "—"


class TestFollowupSuggestion:
    def test_unpaid_scope_suggests_reminders(self):
        p = build_answer_payload(scope=_scope({"paid": "no"}), metric="sum", rows=[{"result": 1}])
        assert p.followup and "remind" in p.followup.lower()

    def test_not_sent_scope_suggests_generating_invoices(self):
        p = build_answer_payload(scope=_scope({"bill_sent": "no"}), metric=None, rows=[JOB_ROW])
        assert p.followup and "invoice" in p.followup.lower()

    def test_bare_client_scope_suggests_payment_status(self):
        p = build_answer_payload(scope=_scope({"client_name": "Nike"}), metric="sum", rows=[{"result": 1}])
        assert p.followup and "payment" in p.followup.lower()

    def test_unfiltered_sum_suggests_paid_split(self):
        p = build_answer_payload(scope=_scope(), metric="sum", rows=[{"result": 1}])
        assert p.followup and "split" in p.followup.lower()

    def test_client_plus_paid_filter_has_no_generic_suggestion(self):
        """Client + paid together is already a specific-enough ask — the
        'want their payment status too' rule must not fire (paid is already
        answered) and no other rule matches, so followup should be None."""
        p = build_answer_payload(scope=_scope({"client_name": "Nike", "paid": "yes"}),
                                  metric="sum", rows=[{"result": 1}])
        assert p.followup is None


class TestRenderAnswerPayload:
    def test_headline_and_scope_always_present(self):
        p = build_answer_payload(scope=_scope(), metric="sum", rows=[{"result": 1175000}])
        text = render_answer_payload(p)
        assert "₹11,75,000" in text
        assert "both paid and unpaid" in text

    def test_support_rows_rendered_as_bullets(self):
        p = build_answer_payload(scope=_scope(), metric=None, rows=[JOB_ROW])
        text = render_answer_payload(p)
        assert "•" in text and "Nike" in text and "INV-001" in text

    def test_remainder_shown(self):
        rows = [dict(JOB_ROW, id=str(i)) for i in range(10)]
        p = build_answer_payload(scope=_scope(), metric=None, rows=rows)
        text = render_answer_payload(p)
        assert "+7 more" in text

    def test_followup_appended_when_present(self):
        p = build_answer_payload(scope=_scope({"paid": "no"}), metric="sum", rows=[{"result": 1}])
        text = render_answer_payload(p)
        assert text.strip().endswith(p.followup)

    def test_render_never_exceeds_whatsapp_budget(self):
        rows = [dict(JOB_ROW, id=str(i), client_name=f"Client {i} With A Genuinely Long Name Inc")
                for i in range(50)]
        p = build_answer_payload(scope=_scope(), metric=None, rows=rows)
        text = render_answer_payload(p)
        assert len(text) <= 900

    def test_manually_constructed_payload_renders_cleanly(self):
        p = AnswerPayload(headline="5 jobs", scope_note="all clients, all time",
                           support=[], remainder=0, followup=None)
        text = render_answer_payload(p)
        assert text == "5 jobs — all clients, all time."
