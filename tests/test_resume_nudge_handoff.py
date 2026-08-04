"""
TODO.md 1.5 — SIDE_QUESTION for READ paths "currently loses the
resume-nudge".

When a user asks a side question mid-flow that the deterministic router
can't answer, dispatch_in_flow returns SHADOW_ONLY and legacy's LLM planner
answers it. Legacy knows nothing about the active flow, so the "still
waiting on X" reminder was dropped — the user got their answer and was left
stranded, with no cue that the bot was still waiting on them.

Fully owning that branch in v2 would mean lifting the entire ~1000-line
query cascade (follow-ups, ledger, invoice check, router, planner,
synthesis, export) into the dispatcher. Instead the nudge is carried across
the handoff: stashed for the turn, appended by process_request to whatever
legacy produced — the same user-visible result, without duplicating the
cascade.

These tests cover the carry mechanism itself, since the bugs it can cause
are cross-turn and cross-user leakage rather than anything visible in one
happy path.
"""

import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch


class FakeMemory:
    def __init__(self):
        self._store = {}

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


def _svc():
    with patch("services.intent_service.GeminiService"), \
         patch("services.intent_service.ResendEmailService"), \
         patch("services.intent_service.SupabaseService"), \
         patch("services.intent_service.MemoryService"):
        from services.intent_service import IntentService
        svc = IntentService()
    svc.memory = FakeMemory()
    svc.supabase = MagicMock()
    svc.gemini = MagicMock()
    return svc


NUDGE = "\n\nStill waiting on the job description — send it in one message."


class TestStashAndTake:
    def test_take_returns_what_was_stashed(self):
        svc = _svc()
        svc._stash_resume_nudge(NUDGE)
        assert svc._take_resume_nudge() == NUDGE

    def test_take_is_read_once(self):
        """Read-once is what stops the nudge being appended twice, or
        bleeding into a later turn."""
        svc = _svc()
        svc._stash_resume_nudge(NUDGE)
        assert svc._take_resume_nudge() == NUDGE
        assert svc._take_resume_nudge() is None

    def test_take_with_nothing_stashed(self):
        svc = _svc()
        assert svc._take_resume_nudge() is None


class TestProcessRequestAppendsTheNudge:
    def _impl(self, svc, response, operation="query"):
        """Stand in for the legacy cascade: stash a nudge (as the dispatcher
        would) then return a legacy-shaped reply."""
        def _fake(user_id, message):
            svc._stash_resume_nudge(NUDGE)
            return {"operation": operation, "response": response,
                    "trigger_invoice": False, "invoice_data": {}}
        return _fake

    def test_nudge_appended_to_legacy_reply(self):
        svc = _svc()
        svc._process_request_impl = self._impl(svc, "You have 12 jobs.")
        out = svc.process_request("u1", "how many jobs")
        assert out["response"] == "You have 12 jobs." + NUDGE

    def test_not_appended_twice_if_already_present(self):
        """Belt-and-braces: if legacy somehow already ended with the nudge
        (e.g. a future change appends it too), don't duplicate it."""
        svc = _svc()
        svc._process_request_impl = self._impl(svc, "You have 12 jobs." + NUDGE)
        out = svc.process_request("u1", "how many jobs")
        assert out["response"].count(NUDGE.strip()) == 1

    def test_not_appended_to_an_error_reply(self):
        """'Still waiting on X' stapled to a failure message is noise on
        top of a failure."""
        svc = _svc()
        svc._process_request_impl = self._impl(svc, "Something broke", operation="error")
        out = svc.process_request("u1", "how many jobs")
        assert NUDGE not in out["response"]

    def test_not_appended_to_an_empty_reply(self):
        svc = _svc()
        svc._process_request_impl = self._impl(svc, "   ")
        assert NUDGE not in (svc.process_request("u1", "x")["response"] or "")

    def test_no_nudge_means_reply_untouched(self):
        svc = _svc()
        svc._process_request_impl = lambda u, m: {
            "operation": "query", "response": "You have 12 jobs.",
            "trigger_invoice": False, "invoice_data": {},
        }
        assert svc.process_request("u1", "how many jobs")["response"] == "You have 12 jobs."


class TestNudgeDoesNotLeakAcrossTurns:
    """The failure mode this mechanism could introduce, so it's pinned
    explicitly: a nudge stashed but never consumed (an exception before the
    append point) must not surface on an unrelated later reply."""

    def test_stale_nudge_cleared_at_turn_start(self):
        svc = _svc()
        svc._stash_resume_nudge(NUDGE)          # left over from an earlier turn
        svc._process_request_impl = lambda u, m: {
            "operation": "query", "response": "Unrelated answer.",
            "trigger_invoice": False, "invoice_data": {},
        }
        out = svc.process_request("u1", "something else entirely")
        assert out["response"] == "Unrelated answer.", (
            "a stale nudge leaked onto a later, unrelated turn"
        )

    def test_second_turn_clean_after_a_nudged_turn(self):
        svc = _svc()
        calls = {"n": 0}

        def _impl(user_id, message):
            calls["n"] += 1
            if calls["n"] == 1:
                svc._stash_resume_nudge(NUDGE)
                return {"operation": "query", "response": "First answer.",
                        "trigger_invoice": False, "invoice_data": {}}
            return {"operation": "query", "response": "Second answer.",
                    "trigger_invoice": False, "invoice_data": {}}

        svc._process_request_impl = _impl
        first = svc.process_request("u1", "side question")
        second = svc.process_request("u1", "another message")
        assert first["response"] == "First answer." + NUDGE
        assert second["response"] == "Second answer."


class TestDispatcherStashesOnShadowHandoff:
    """The dispatcher end: a READ side question the router can't answer must
    stash the nudge before returning SHADOW_ONLY."""

    def test_stashes_before_shadow_only(self):
        from services.flow_dispatcher import dispatch_in_flow, SHADOW_ONLY
        from services.flow_machine import FLOW_SMART_CAPTURE_NEED_DESCRIPTION

        svc = _svc()
        svc.memory.update_user_memory("u1", {})
        stashed = []
        svc._stash_resume_nudge = lambda n: stashed.append(n)

        verdict = {
            "intent": "READ_QUERY", "flow_compatible": "SIDE_QUESTION",
            # Deliberately not a shape the deterministic router matches, so
            # the branch falls through to the legacy handoff.
            "raw_message": "remind me what the deadline policy was",
            "parameters": {}, "confidence": 0.9, "historical": False, "bulk": False,
        }
        result = dispatch_in_flow(
            verdict, intent_service=svc, user_id="u1",
            current_flow=FLOW_SMART_CAPTURE_NEED_DESCRIPTION,
            current_context={}, conversation_history=[],
        )
        assert result is SHADOW_ONLY
        assert stashed, "dispatcher returned SHADOW_ONLY without stashing a resume-nudge"
        assert "still waiting" in stashed[0].lower()
