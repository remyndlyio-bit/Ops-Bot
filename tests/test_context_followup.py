"""
Category 6 — Multi-turn Context & Follow-up.

Covers the mechanisms that decide whether a short/ambiguous message is answered
from cached context (`_try_answer_from_context`, `_is_followup_field_request`)
or reconstructed against a prior structured intent (`_reconstruct_message`),
plus disambiguation state surviving (or correctly NOT surviving) an unrelated
interruption.

This is the category that produced the sharpest live-transcript failure: a
QUESTION about a prior answer ("Do these include, paid and unpaid?") getting
misread as something else. Two distinct bugs lived in this territory —
  1. A dangling background flag treating the question as a reply (fixed in
     _handle_pending_audit_reply — see TestAuditReplyDoesNotHijackQuestions
     in tests/test_edge_cases.py).
  2. `_is_followup_field_request` bare-substring-matching column names, so
     "id" (a real column) matched inside the words "paid"/"unpaid" themselves
     and could route a scope question into a single-row field read.
This file locks down #2 and the surrounding context-resolution behavior.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch


def _make_svc():
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()
    svc.gemini = MagicMock()
    svc.email = MagicMock()
    svc.supabase = MagicMock()
    svc.memory = MagicMock()
    svc.supabase.get_user_profile.return_value = {
        "ok": True, "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "Test User"},
    }
    svc.supabase.db_url = "postgresql://fake"
    svc.gemini.is_history_question.return_value = False
    return svc


COLUMNS = ["id", "client_name", "job_date", "fees", "paid", "brand_name",
           "poc_email", "bill_sent", "invoice_date"]


class TestShortColumnNameFalsePositives:
    """Regression for the 'id' collision: 'paid' and 'unpaid' both literally
    contain the substring 'id', so a bare `in` check on column names made
    _is_followup_field_request return the 'id' column for almost any status
    question, before it ever reached the real 'paid' alias logic."""

    @pytest.mark.parametrize("msg", [
        "Do these include, paid and unpaid?",
        "Does this include both paid and unpaid?",
        "Are these all paid?",
        "is this paid?",
        "was this job paid?",
        "What about unpaid jobs?",
    ])
    def test_status_questions_never_resolve_to_id_column(self, msg):
        svc = _make_svc()
        result = svc._is_followup_field_request(msg, COLUMNS)
        assert result != "id", f"{msg!r} spuriously matched the 'id' column"

    def test_genuine_id_question_still_matches(self):
        svc = _make_svc()
        # A real request for the row id should still resolve to it.
        assert svc._is_followup_field_request("what's the id?", COLUMNS) == "id"

    def test_paid_question_resolves_to_paid_not_something_else(self):
        svc = _make_svc()
        assert svc._is_followup_field_request("is this paid?", COLUMNS) == "paid"
        assert svc._is_followup_field_request("was this job paid?", COLUMNS) == "paid"


class TestFollowupAnswersFromContext:
    """`_try_answer_from_context` — the short-circuit that serves a field value
    from cached last_row_data without a fresh DB round-trip. Must never
    fabricate an answer when the field genuinely isn't in the cached row."""

    def test_scope_question_after_aggregate_falls_through_safely(self):
        """The exact production shape: after an aggregate ('total earning'),
        the cached row is {'result': N} — no 'paid' key. A scope-clarifying
        question about that total must fall through to a fresh query, not
        answer with an unrelated/missing field."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "uscf_context": {"last_row_data": {"result": 75000}, "last_operation": "query"}
        }
        svc.memory.get_conversation_history.return_value = []
        resp = svc._try_answer_from_context("u1", "Do these include, paid and unpaid?", COLUMNS)
        assert resp is None

    def test_genuine_field_read_answers_from_cached_row(self):
        svc = _make_svc()
        row = {"client_name": "Nike", "fees": 25000, "paid": "Yes", "job_date": "2026-03-01"}
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": row}}
        svc.memory.get_conversation_history.return_value = []
        svc.gemini.synthesize_response.return_value = "The fee was ₹25,000."
        resp = svc._try_answer_from_context("u1", "what's the fee?", COLUMNS)
        assert resp == "The fee was ₹25,000."
        # Must NOT hit the DB for a value already cached.
        svc.supabase.execute_sql.assert_not_called()

    def test_history_question_skips_shortcut(self):
        """'is_history_question' (an AI check for questions about a PAST change,
        e.g. 'when did this become paid?') must force the full-row query path,
        not answer from the flat cached snapshot which has no history."""
        svc = _make_svc()
        row = {"client_name": "Nike", "paid": "Yes"}
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": row}}
        svc.gemini.is_history_question.return_value = True
        resp = svc._try_answer_from_context("u1", "when did this get marked paid?", COLUMNS)
        assert resp is None

    def test_no_context_returns_none(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        assert svc._try_answer_from_context("u1", "what's the fee?", COLUMNS) is None

    def test_standalone_query_skips_ai_history_check(self):
        """Live production latency finding: is_history_question() (an AI
        call) used to fire unconditionally whenever ANY row was cached,
        even for messages _is_followup_field_request already deterministically
        recognises as standalone (e.g. containing "this month") -- wasting a
        full LLM round trip whose result couldn't possibly change the
        outcome, since the function returns None either way once no field
        matches. Check the cheap deterministic path first."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "uscf_context": {"last_row_data": {"result": 3}}
        }
        resp = svc._try_answer_from_context("u1", "what are my number of jobs this month", COLUMNS)
        assert resp is None
        svc.gemini.is_history_question.assert_not_called()

    def test_field_absent_from_cached_row_falls_through(self):
        """Cached row from an aggregate/partial select doesn't have the asked
        field — must defer to a fresh query, never invent a value."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "uscf_context": {"last_row_data": {"client_name": "Nike", "result": 5}}
        }
        svc.memory.get_conversation_history.return_value = []
        resp = svc._try_answer_from_context("u1", "what's the fee?", COLUMNS)
        assert resp is None


class TestFollowupDetectionGuards:
    """_is_followup_field_request's guards against misclassifying standalone
    queries, mutations, and multi-record requests as single-field follow-ups."""

    def test_mutation_verbs_never_treated_as_field_read(self):
        svc = _make_svc()
        assert svc._is_followup_field_request("mark this as paid", COLUMNS) is None
        assert svc._is_followup_field_request("change the fee to 30000", COLUMNS) is None

    def test_group_by_client_not_a_followup(self):
        """Live production bug: "Show earnings by client" matched "client"
        as a field alias and got answered from a single cached row (whatever
        job happened to be cached from the PREVIOUS turn) instead of running
        the GROUP BY aggregate the user actually asked for -- the response
        described one client's one job as if it answered "earnings by
        client" for the whole account."""
        svc = _make_svc()
        assert svc._is_followup_field_request("Show earnings by client", COLUMNS) is None
        assert svc._is_followup_field_request("earnings by brand", COLUMNS) is None
        assert svc._is_followup_field_request("group by client", COLUMNS) is None

    def test_dated_aggregate_not_a_followup(self):
        svc = _make_svc()
        assert svc._is_followup_field_request("Total earnings last quarter?", COLUMNS) is None

    def test_client_scoped_list_not_a_followup(self):
        svc = _make_svc()
        assert svc._is_followup_field_request("show jobs for Nike", COLUMNS) is None

    def test_plural_scoped_query_not_a_followup(self):
        svc = _make_svc()
        assert svc._is_followup_field_request("how many invoices for Nike", COLUMNS) is None

    @pytest.mark.parametrize("msg", ["Fees?", "Date?", "Brand?"])
    def test_short_single_field_questions_detected(self, msg):
        svc = _make_svc()
        assert svc._is_followup_field_request(msg, COLUMNS) is not None


class TestReconstructMessage:
    """_reconstruct_message — merging a short/ambiguous message with the
    persisted last_intent. Self-contained messages must pass through
    unchanged; short ones should pick up context, never invent a client."""

    def test_self_contained_message_with_action_verb_unchanged(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {"client_name": "Nike", "operation": "show jobs", "entity": "jobs"}
        }
        msg = "Show me all Garnier invoices from March"
        result = svc._reconstruct_message("u1", msg, [])
        assert result == msg, "a self-contained query must not be rewritten using stale context"

    def test_no_last_intent_returns_message_unchanged(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        result = svc._reconstruct_message("u1", "this month", [])
        assert result == "this month"

    def test_short_reply_does_not_fabricate_a_client(self):
        """A short follow-up with NO last_intent client must not hallucinate one."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"last_intent": {"operation": "show jobs", "entity": "jobs"}}
        result = svc._reconstruct_message("u1", "this month", [])
        assert "nike" not in result.lower() and "garnier" not in result.lower()

    def test_new_question_containing_this_month_not_hijacked_by_stale_invoice_intent(self):
        """Live production bug: after generating an invoice for a client,
        asking an unrelated question that happens to contain 'this month'
        ("what are my number of jobs this month") got silently rewritten
        into "Generate invoice for <that client> for what are my number
        of jobs this month" -- Case 5 matched on the bare substring "this
        month" with no check that the message was actually a short/
        ambiguous fragment rather than a complete new question. The user
        asked for a job count and got sent someone else's invoice again."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "generate_invoice", "client_name": "Awesome Labs",
                "month": "February", "year": 2026, "entity": "invoice",
            }
        }
        msg = "what are my number of jobs this month"
        result = svc._reconstruct_message("u1", msg, [])
        assert result == msg
        assert "invoice" not in result.lower()
        assert "awesome labs" not in result.lower()

    def test_question_after_query_result_not_hijacked_either(self):
        """Same guard, different last_intent shape (entity=jobs, not invoice)
        -- a genuine question must never be merged with stale context."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {"operation": "show jobs", "client_name": "Nike", "entity": "jobs"}
        }
        msg = "how many jobs this month"
        result = svc._reconstruct_message("u1", msg, [])
        assert result == msg

    def test_standalone_aggregate_statement_not_hijacked_by_case5(self):
        """Live production bug found in a second scenario-suite run: unlike
        the question-shaped case above, "Last month earnings" and "My total
        billing this year" are DECLARATIVE (no '?', no question-starter word)
        so _looks_like_a_question doesn't catch them -- yet Case 5 still
        matched the bare substring "last month"/"this year" and rewrote them
        into an invoice-generation request for the stale client, discarding
        the user's actual aggregate question."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "ACTION_TRIGGER", "client_name": "Nike",
                "entity": "invoice", "pending_clarification": "month",
            }
        }
        for msg in ("Last month earnings", "My total billing this year"):
            result = svc._reconstruct_message("u1", msg, [])
            assert result == msg, f"{msg!r} was hijacked into: {result!r}"

    def test_bare_cancel_never_reconstructed_as_a_month_reply(self):
        """Live bug: with pending_clarification == 'month' armed (from an
        unrelated, much earlier invoice flow), a bare "cancel" -- meant to
        cancel something else entirely (e.g. account linking) -- got
        rewritten by Case 4 into "Generate invoice for Nike for cancel" and
        silently triggered a real invoice generation instead of reaching
        whatever cancel handler the user's message was actually meant for."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "ACTION_TRIGGER", "client_name": "Nike",
                "entity": "invoice", "pending_clarification": "month",
            }
        }
        for msg in ("cancel", "stop", "nevermind"):
            result = svc._reconstruct_message("u1", msg, [])
            assert result == msg, f"{msg!r} was hijacked into: {result!r}"

    def test_non_month_word_not_treated_as_month_reply_case1(self):
        """Case 1 (bare month reply) had no length guard: any message
        merely CONTAINING a month name, while pending == 'month', was
        treated as the reply -- so "ZZTEST Nike jobs in July" (a fresh,
        self-contained jobs-listing query) got rewritten into "Generate
        invoice for Nike for ZZTEST Nike jobs in July"."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "ACTION_TRIGGER", "client_name": "Nike",
                "entity": "invoice", "pending_clarification": "month",
            }
        }
        msg = "ZZTEST Nike jobs in July"
        result = svc._reconstruct_message("u1", msg, [])
        assert result == msg

    def test_genuine_short_month_reply_still_works(self):
        """Over-correction guard: a real bare month reply must still
        reconstruct correctly."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "generate_invoice", "client_name": "Nike",
                "entity": "invoice", "pending_clarification": "month",
            }
        }
        result = svc._reconstruct_message("u1", "March 2025", [])
        assert "nike" in result.lower() and "march 2025" in result.lower()

    def test_yes_not_treated_as_a_month_value(self):
        """Live bug: a bare "Yes" (meant to confirm an unrelated "want more
        details?" offer) got swept into Case 4's pending == 'month' branch
        and rewritten into "Generate invoice for Nike for Yes" purely
        because it was <=2 words -- Case 4 never checked the word actually
        looked like a month."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "ACTION_TRIGGER", "client_name": "Nike",
                "entity": "invoice", "pending_clarification": "month",
            }
        }
        result = svc._reconstruct_message("u1", "Yes", [])
        assert result == "Yes"

    def test_stale_confirm_alt_month_not_hijacked_by_unrelated_yes(self):
        """Live bug (#48): the bot asked 'did you mean February?' (persisting
        last_intent.pending_clarification='confirm_alt_month' for Nike/Feb),
        the conversation then moved on to something else entirely, and a bare
        'Yes' meant for that unrelated later question got rewritten into
        'Generate invoice for Nike for February' and silently triggered a
        real invoice generation — because Case 0 trusted the stale
        last_intent without checking whether the bot's last message was
        actually the alt-month prompt."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "generate_invoice", "client_name": "Nike",
                "month": "February", "year": 2026, "entity": "invoice",
                "pending_clarification": "confirm_alt_month",
            }
        }
        conversation_history = [
            {"role": "assistant", "content": "Would you like more details on that job?"},
        ]
        result = svc._reconstruct_message("u1", "Yes", conversation_history)
        assert result == "Yes"

    def test_genuine_confirm_alt_month_still_works(self):
        """Over-correction guard: when the bot's own last message really was
        the alt-month suggestion, a bare 'Yes' must still reconstruct into
        the invoice-generation request."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "operation": "generate_invoice", "client_name": "Nike",
                "month": "February", "year": 2026, "entity": "invoice",
                "pending_clarification": "confirm_alt_month",
            }
        }
        conversation_history = [
            {"role": "assistant", "content": (
                "I found no jobs for Nike in March 2026.\n\n"
                "I do have records for Nike in: February 2026."
            )},
        ]
        result = svc._reconstruct_message("u1", "Yes", conversation_history)
        assert "nike" in result.lower() and "february" in result.lower()


class TestDisambiguationInterruptedByUnrelatedMessage:
    """A pending disambiguation must not swallow an unrelated new question —
    it should clear itself and let the real pipeline answer it."""

    PENDING = {
        "type": "modify", "field": "fees", "value": 30000,
        "rows": [
            {"id": "a", "client_name": "Nike", "bill_no": "INV-1", "fees": 10000},
            {"id": "b", "client_name": "Nike", "bill_no": "INV-2", "fees": 20000},
        ],
    }

    def test_unrelated_question_clears_and_falls_through(self):
        svc = _make_svc()
        result = svc._handle_disambiguation_reply("u1", "what's my total earning so far?", self.PENDING)
        assert result is None, "an unrelated question must fall through, not be read as a row pick"
        cleared = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                   if "pending_disambiguation" in c.args[1]]
        assert cleared and cleared[0]["pending_disambiguation"] is None
        assert svc.supabase.execute_sql.call_count == 0, "no write should happen for an unrelated question"

    def test_cancel_clears_without_writing(self):
        svc = _make_svc()
        result = svc._handle_disambiguation_reply("u1", "nevermind", self.PENDING)
        assert result["operation"] == "query"
        assert svc.supabase.execute_sql.call_count == 0

    def test_out_of_range_number_reprompts_without_writing(self):
        svc = _make_svc()
        result = svc._handle_disambiguation_reply("u1", "99", self.PENDING)
        assert "between 1 and 2" in result["response"]
        assert svc.supabase.execute_sql.call_count == 0
