"""
WP-1 of ASSISTANT_PLAN.md — AnswerLedger.

The production transcript this fixes: "What's my total earning so far?" ->
"₹75,000" -> "Do these include, paid and unpaid?" was unanswerable because the
cached context for an aggregate is {"result": 75000} — no "paid" key to read.
The ledger stores the SCOPE (filters + time_range) the answer was computed
under, straight from the same plan that produced the SQL, and a deterministic
reader answers the follow-up without a fresh query or an LLM call.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock

from services.answer_ledger import (
    LedgerEntry, build_entry, append_entry, get_entries,
    is_scope_question, answer_scope_question, LEDGER_SCHEMA_VERSION,
    _format_inr,
)


class TestIndianCurrencyFormatting:
    """The bot's LLM-synthesized answers already use Indian digit grouping
    (knowledge/rules.py currency_is_inr: "Rs 1,75,000") — this reader has no
    LLM in the loop, so it needs its own formatter, or a scope-question reply
    about a number would show a DIFFERENT grouping than the original answer
    gave for the exact same value."""

    @pytest.mark.parametrize("n,expected", [
        (0, "0"), (1, "1"), (999, "999"),
        (1000, "1,000"), (12345, "12,345"),
        (175000, "1,75,000"),
        (1175000, "11,75,000"),          # the exact production transcript value
        (10000000, "1,00,00,000"),
        (123456789, "12,34,56,789"),
    ])
    def test_indian_grouping(self, n, expected):
        assert _format_inr(n) == expected

    def test_negative(self):
        assert _format_inr(-50000) == "-50,000"


def _plan(metric=None, filters=None, time_range=None):
    return {"metric": metric, "column": "fees" if metric else None,
            "filters": filters or {}, "time_range": time_range}


class TestBuildEntry:
    def test_aggregate_sum_kind_and_value(self):
        e = build_entry(question="total earnings", plan=_plan(metric="sum"),
                         rows=[{"result": 1175000}], response="₹11,75,000")
        assert e.kind == "aggregate"
        assert e.value == 1175000
        assert e.scope == {"filters": {}, "time_range": None}

    def test_count_is_also_aggregate(self):
        e = build_entry(question="how many jobs", plan=_plan(metric="count"),
                         rows=[{"result": 8}], response="8 jobs")
        assert e.kind == "aggregate" and e.value == 8

    def test_no_metric_is_a_list(self):
        rows = [{"id": "1", "client_name": "Nike"}, {"id": "2", "client_name": "Adidas"}]
        e = build_entry(question="show my jobs", plan=_plan(metric=None), rows=rows, response="2 jobs")
        assert e.kind == "list"
        assert e.value == 2
        assert e.row_ids == ["1", "2"]

    def test_filters_and_time_range_carried_through_verbatim(self):
        plan = _plan(metric="sum", filters={"client_name": "Nike", "paid": "no"},
                     time_range={"type": "absolute", "value": {"start": "2026-01-01", "end": "2026-03-31"}})
        e = build_entry(question="unpaid for nike in q1", plan=plan, rows=[{"result": 50000}], response="₹50,000")
        assert e.scope["filters"] == {"client_name": "Nike", "paid": "no"}
        assert e.scope["time_range"]["value"]["start"] == "2026-01-01"

    def test_none_plan_does_not_crash(self):
        e = build_entry(question="x", plan=None, rows=[], response="")
        assert e.kind == "list" and e.value == 0

    def test_surface_truncated_to_200_chars(self):
        e = build_entry(question="x", plan=_plan(metric="sum"), rows=[{"result": 1}],
                         response="a" * 500)
        assert len(e.surface) == 200

    def test_row_ids_skip_missing_id(self):
        rows = [{"id": "1"}, {"client_name": "no id here"}, {"id": "3"}]
        e = build_entry(question="x", plan=_plan(), rows=rows, response="")
        assert e.row_ids == ["1", "3"]


class TestSerializationRoundTrip:
    def test_to_dict_from_dict(self):
        e = LedgerEntry(question="q", kind="aggregate", scope={"filters": {}}, value=100)
        d = e.to_dict()
        e2 = LedgerEntry.from_dict(d)
        assert e2.question == "q" and e2.value == 100 and e2.turn_id == e.turn_id

    def test_from_dict_ignores_unknown_keys(self):
        """Forward-compat: a future field addition shouldn't crash old readers."""
        d = {"question": "q", "kind": "list", "scope": {}, "some_future_field": 123}
        e = LedgerEntry.from_dict(d)
        assert e.question == "q"

    def test_from_dict_handles_empty(self):
        e = LedgerEntry.from_dict({})
        assert e.question == ""


class TestAppendAndTrim:
    def _mem(self, initial=None):
        m = MagicMock()
        store = {"answer_ledger": initial} if initial else {}
        m.get_user_memory.return_value = store
        return m

    def test_first_append_creates_versioned_state(self):
        mem = self._mem()
        append_entry(mem, "u1", LedgerEntry(question="q1", kind="aggregate", scope={}))
        written = mem.update_user_memory.call_args.args[1]["answer_ledger"]
        assert written["v"] == LEDGER_SCHEMA_VERSION
        assert len(written["entries"]) == 1

    def test_trims_to_max_10(self):
        existing = {"v": LEDGER_SCHEMA_VERSION,
                    "entries": [LedgerEntry(question=f"q{i}", kind="list", scope={}).to_dict() for i in range(10)]}
        mem = self._mem(existing)
        append_entry(mem, "u1", LedgerEntry(question="q_new", kind="aggregate", scope={}))
        written = mem.update_user_memory.call_args.args[1]["answer_ledger"]
        assert len(written["entries"]) == 10
        assert written["entries"][-1]["question"] == "q_new"
        assert written["entries"][0]["question"] == "q1", "oldest entry must be dropped, not newest"

    def test_append_failure_does_not_raise(self):
        mem = MagicMock()
        mem.get_user_memory.side_effect = Exception("db down")
        append_entry(mem, "u1", LedgerEntry(question="q", kind="list", scope={}))  # must not raise

    def test_malformed_existing_state_starts_clean(self):
        mem = self._mem({"not": "the right shape"})
        append_entry(mem, "u1", LedgerEntry(question="q", kind="list", scope={}))
        written = mem.update_user_memory.call_args.args[1]["answer_ledger"]
        assert len(written["entries"]) == 1

    def test_old_schema_version_starts_clean_not_migrated_badly(self):
        mem = self._mem({"v": 999, "entries": [{"question": "old"}]})
        append_entry(mem, "u1", LedgerEntry(question="new", kind="list", scope={}))
        written = mem.update_user_memory.call_args.args[1]["answer_ledger"]
        assert len(written["entries"]) == 1
        assert written["entries"][0]["question"] == "new"


class TestGetEntries:
    def test_empty_when_no_ledger(self):
        assert get_entries({}) == []

    def test_returns_entries_in_order(self):
        state = {"v": LEDGER_SCHEMA_VERSION,
                 "entries": [{"question": "first", "kind": "list", "scope": {}},
                             {"question": "second", "kind": "aggregate", "scope": {}}]}
        entries = get_entries({"answer_ledger": state})
        assert [e.question for e in entries] == ["first", "second"]


class TestScopeQuestionDetection:
    @pytest.mark.parametrize("msg", [
        "Do these include, paid and unpaid?",
        "Does this include both paid and unpaid?",
        "does that include everything?",
        "Is this just the unpaid ones?",
        "Is that only Nike?",
        "What does that include?",
        "Is that all of them?",
    ])
    def test_recognised(self, msg):
        assert is_scope_question(msg)

    @pytest.mark.parametrize("msg", [
        "How many jobs do I have?",
        "Total billing this year",
        "Show me Nike jobs",
        "Mark this as paid",
    ])
    def test_not_recognised(self, msg):
        assert not is_scope_question(msg)


class TestAnswerScopeQuestion:
    def test_the_production_transcript_case(self):
        """The exact sequence: aggregate with NO filters -> scope question ->
        must confirm both paid and unpaid are included."""
        user_mem = {"answer_ledger": {
            "v": LEDGER_SCHEMA_VERSION,
            "entries": [build_entry(
                question="What's my total earning so far?",
                plan=_plan(metric="sum"),
                rows=[{"result": 1175000}],
                response="Your total billing comes to ₹11,75,000!",
            ).to_dict()],
        }}
        answer = answer_scope_question("Do these include, paid and unpaid?", user_mem)
        assert answer is not None
        assert "11,75,000" in answer
        assert "both paid and unpaid" in answer.lower()

    def test_unpaid_filtered_answer_says_only_unpaid(self):
        user_mem = {"answer_ledger": {"v": LEDGER_SCHEMA_VERSION, "entries": [
            build_entry(question="total unpaid", plan=_plan(metric="sum", filters={"paid": "no"}),
                        rows=[{"result": 50000}], response="₹50,000").to_dict()
        ]}}
        answer = answer_scope_question("does this include paid ones too?", user_mem)
        assert "only the unpaid" in answer.lower()

    def test_client_scoped_answer_names_the_client(self):
        user_mem = {"answer_ledger": {"v": LEDGER_SCHEMA_VERSION, "entries": [
            build_entry(question="nike total", plan=_plan(metric="sum", filters={"client_name": "Nike"}),
                        rows=[{"result": 300000}], response="₹300,000").to_dict()
        ]}}
        answer = answer_scope_question("is that only Nike?", user_mem)
        assert "nike" in answer.lower()

    def test_no_answer_when_message_not_scope_shaped(self):
        user_mem = {"answer_ledger": {"v": LEDGER_SCHEMA_VERSION, "entries": [
            build_entry(question="x", plan=_plan(metric="sum"), rows=[{"result": 1}], response="").to_dict()
        ]}}
        assert answer_scope_question("show me Nike jobs", user_mem) is None

    def test_no_answer_when_no_ledger_yet(self):
        assert answer_scope_question("Do these include paid and unpaid?", {}) is None

    def test_no_answer_when_last_entry_is_an_action(self):
        """A mark-paid confirmation has no 'scope' to clarify — must defer,
        not fabricate an answer from an unrelated concept."""
        user_mem = {"answer_ledger": {"v": LEDGER_SCHEMA_VERSION, "entries": [
            LedgerEntry(question="mark paid", kind="action", scope={}, value="Nike").to_dict()
        ]}}
        assert answer_scope_question("does this include paid and unpaid?", user_mem) is None

    def test_list_kind_answers_without_a_rupee_value(self):
        user_mem = {"answer_ledger": {"v": LEDGER_SCHEMA_VERSION, "entries": [
            build_entry(question="show jobs", plan=_plan(metric=None),
                        rows=[{"id": "1"}, {"id": "2"}], response="2 jobs").to_dict()
        ]}}
        answer = answer_scope_question("does that include paid and unpaid?", user_mem)
        assert answer is not None and "₹" not in answer

    def test_date_range_mentioned_when_present(self):
        plan = _plan(metric="sum", time_range={"type": "absolute",
                     "value": {"start": "2026-01-01", "end": "2026-03-31"}})
        user_mem = {"answer_ledger": {"v": LEDGER_SCHEMA_VERSION, "entries": [
            build_entry(question="q1 total", plan=plan, rows=[{"result": 100000}], response="").to_dict()
        ]}}
        answer = answer_scope_question("does this include everything?", user_mem)
        assert "2026-01-01" in answer

    def test_no_date_filter_says_all_time(self):
        user_mem = {"answer_ledger": {"v": LEDGER_SCHEMA_VERSION, "entries": [
            build_entry(question="total", plan=_plan(metric="sum"), rows=[{"result": 1}], response="").to_dict()
        ]}}
        answer = answer_scope_question("does this include everything?", user_mem)
        assert "all time" in answer.lower()

    def test_answers_from_the_most_recent_entry_not_an_older_one(self):
        entries = [
            build_entry(question="nike total", plan=_plan(metric="sum", filters={"client_name": "Nike"}),
                        rows=[{"result": 1}], response="").to_dict(),
            build_entry(question="total earnings", plan=_plan(metric="sum"),
                        rows=[{"result": 2}], response="").to_dict(),
        ]
        user_mem = {"answer_ledger": {"v": LEDGER_SCHEMA_VERSION, "entries": entries}}
        answer = answer_scope_question("does this include everything?", user_mem)
        assert "nike" not in answer.lower(), "must answer from the LAST entry, not an earlier one"
