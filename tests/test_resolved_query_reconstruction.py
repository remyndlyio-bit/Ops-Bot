"""
TODO.md 1.4 item 4 — "replace `_reconstruct_message` with the classifier's
`resolved_query`".

A full swap is NOT safe, and these tests pin down why as much as they pin
down the change that was made:

* The classifier emits `resolved_query` as null for anything that isn't
  READ_QUERY/READ_AGGREGATE, but four of `_reconstruct_message`'s six cases
  are WRITE_INVOICE paths ("which month?" -> "March" -> Generate invoice).
  Replacing wholesale deletes those with no replacement.
* The messages `resolved_query` exists for ("what about this month?", "and
  last quarter?") never reach the function — `_looks_like_a_question`
  returns early on them.

So the change is narrow: where reconstruction ALREADY fires and reads a
client from the stored `last_intent`, a client freshly resolved by the
classifier for THIS turn wins instead. Nearly every incident described in
`_reconstruct_message`'s own comments is the stale-client shape — a client
left over from an unrelated flow silently replacing the user's request —
which is exactly what this addresses.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch


class FakeMemory:
    def __init__(self, mem=None):
        self._store = {"u1": dict(mem or {})}

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


def _svc(mem=None):
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()
    svc.memory = FakeMemory(mem)
    svc.supabase = MagicMock()
    svc.gemini = MagicMock()
    return svc


def _verdict(intent="READ_QUERY", client=None, **rq):
    resolved = None
    if client is not None or rq:
        resolved = {"client_name": client, "time_range": None, "metric_hint": None}
        resolved.update(rq)
    return {"intent": intent, "resolved_query": resolved, "confidence": 0.9,
            "parameters": {}, "raw_message": "x", "historical": False, "bulk": False}


STALE = {"last_intent": {"client_name": "StaleCorp", "operation": "query",
                         "entity": "job", "pending_clarification": ""}}


class TestResolvedQueryClientHelper:
    """The extraction helper is deliberately strict — anything that isn't a
    READ intent carrying a real client string yields None, so invoice paths
    can never be affected."""

    def test_extracts_client_for_read_query(self):
        svc = _svc()
        assert svc._resolved_query_client(_verdict("READ_QUERY", "Nike")) == "Nike"

    def test_extracts_client_for_read_aggregate(self):
        svc = _svc()
        assert svc._resolved_query_client(_verdict("READ_AGGREGATE", "Nike")) == "Nike"

    @pytest.mark.parametrize("intent", ["WRITE_INVOICE", "WRITE_CREATE", "WRITE_UPDATE",
                                        "WRITE_DELETE", "SMALL_TALK", "UNKNOWN"])
    def test_none_for_non_read_intents(self, intent):
        """The safety property the whole change rests on: invoice and other
        write flows must be untouched."""
        svc = _svc()
        assert svc._resolved_query_client(_verdict(intent, "Nike")) is None

    @pytest.mark.parametrize("bad", [None, {}, "not a dict", 42])
    def test_none_for_malformed_verdict(self, bad):
        svc = _svc()
        assert svc._resolved_query_client(bad) is None

    def test_none_when_resolved_query_absent(self):
        svc = _svc()
        assert svc._resolved_query_client(_verdict("READ_QUERY")) is None

    @pytest.mark.parametrize("client", [None, "", "   ", 42])
    def test_none_for_empty_or_non_string_client(self, client):
        svc = _svc()
        v = {"intent": "READ_QUERY", "resolved_query": {"client_name": client}}
        assert svc._resolved_query_client(v) is None

    def test_strips_whitespace(self):
        svc = _svc()
        v = {"intent": "READ_QUERY", "resolved_query": {"client_name": "  Nike  "}}
        assert svc._resolved_query_client(v) == "Nike"


class TestFreshClientBeatsStaleLastIntent:
    """The actual improvement: 'this month' reaches Case 5, which built its
    reconstruction from last_intent.client_name. A client the classifier
    resolved for THIS turn is preferred."""

    def test_resolved_client_used_instead_of_stale(self):
        svc = _svc(STALE)
        out = svc._reconstruct_message("u1", "this month", [],
                                       verdict=_verdict("READ_QUERY", "Nike"))
        assert "Nike" in out, out
        assert "StaleCorp" not in out, f"stale client leaked into reconstruction: {out}"

    def test_falls_back_to_last_intent_without_a_verdict(self):
        """v2 off (verdict=None) must behave exactly as before."""
        svc = _svc(STALE)
        out = svc._reconstruct_message("u1", "this month", [], verdict=None)
        assert "StaleCorp" in out, out

    def test_falls_back_when_verdict_has_no_resolved_query(self):
        svc = _svc(STALE)
        out = svc._reconstruct_message("u1", "this month", [],
                                       verdict=_verdict("READ_QUERY"))
        assert "StaleCorp" in out, out

    def test_write_intent_verdict_does_not_override(self):
        """A WRITE_INVOICE verdict must not steer a reconstruction, even if
        it somehow carries a resolved_query."""
        svc = _svc(STALE)
        out = svc._reconstruct_message("u1", "this month", [],
                                       verdict=_verdict("WRITE_INVOICE", "Nike"))
        assert "StaleCorp" in out and "Nike" not in out, out


class TestInvoiceReconstructionPathsUnchanged:
    """Regression guard for the reason a full swap was rejected: the
    invoice follow-up cases must behave identically with and without a
    verdict present."""

    INVOICE_MEM = {"last_intent": {"client_name": "Acme", "operation": "invoice",
                                   "entity": "invoice", "pending_clarification": "month",
                                   "month": "March"}}

    def test_month_reply_still_builds_invoice_command(self):
        svc = _svc(self.INVOICE_MEM)
        out = svc._reconstruct_message("u1", "March", [], verdict=None)
        assert "invoice" in out.lower() and "Acme" in out, out

    def test_month_reply_identical_with_a_read_verdict_present(self):
        """Even a READ verdict with a different client must not hijack the
        invoice reconstruction — pending_clarification=='month' wins."""
        svc_a = _svc(self.INVOICE_MEM)
        svc_b = _svc(self.INVOICE_MEM)
        without = svc_a._reconstruct_message("u1", "March", [], verdict=None)
        with_v = svc_b._reconstruct_message("u1", "March", [],
                                            verdict=_verdict("READ_QUERY", "Nike"))
        assert without == with_v, f"invoice path diverged: {without!r} vs {with_v!r}"


class TestExistingGuardsStillFireFirst:
    """The safety guards documented in _reconstruct_message (questions,
    cancel words, self-contained queries) must still short-circuit BEFORE
    any resolved_query logic — they exist because of real incidents."""

    @pytest.mark.parametrize("msg", [
        "what about this month?",   # the canonical resolved_query example
        "and last quarter?",
    ])
    def test_questions_return_unchanged(self, msg):
        svc = _svc(STALE)
        out = svc._reconstruct_message("u1", msg, [],
                                       verdict=_verdict("READ_QUERY", "Nike"))
        assert out == msg, f"a question was rewritten: {out!r}"

    @pytest.mark.parametrize("msg", ["cancel", "stop", "nevermind"])
    def test_cancel_words_return_unchanged(self, msg):
        svc = _svc(STALE)
        out = svc._reconstruct_message("u1", msg, [],
                                       verdict=_verdict("READ_QUERY", "Nike"))
        assert out == msg

    def test_standalone_query_returns_unchanged(self):
        svc = _svc(STALE)
        msg = "total earnings"
        out = svc._reconstruct_message("u1", msg, [],
                                       verdict=_verdict("READ_QUERY", "Nike"))
        assert out == msg
