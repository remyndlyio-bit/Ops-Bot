"""
WP-0 of ASSISTANT_PLAN.md — per-turn telemetry.

Covers: the LLM-call counter (including reentrant process_request calls,
which several flow-continuation paths use synchronously), fallback-response
detection, and the FLOW_MACHINE_V2 canary-allowlist parsing.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch

from utils import telemetry
from utils.telemetry import (
    Turn, start_turn, note_llm_call, current_llm_calls, is_fallback_response,
    note_route, current_route, log_turn,
)


@pytest.fixture(autouse=True)
def _reset_counter():
    """Contextvars persist across tests in the same process unless reset."""
    telemetry._llm_call_count.set(None)
    telemetry._current_route.set(None)
    yield
    telemetry._llm_call_count.set(None)
    telemetry._current_route.set(None)


class TestLlmCallCounter:
    def test_no_active_turn_is_none(self):
        assert current_llm_calls() is None

    def test_note_call_outside_turn_is_a_safe_noop(self):
        note_llm_call()  # must not raise
        assert current_llm_calls() is None

    def test_counts_calls_within_a_turn(self):
        with Turn("u1") as t:
            note_llm_call()
            note_llm_call()
            note_llm_call()
            t.operation = "query"
        # counter is reset to None after the outermost turn exits
        assert current_llm_calls() is None

    def test_counter_visible_during_the_turn(self):
        with Turn("u1"):
            note_llm_call()
            assert current_llm_calls() == 1
            note_llm_call()
            assert current_llm_calls() == 2

    def test_reset_between_separate_turns(self):
        with Turn("u1"):
            note_llm_call()
            note_llm_call()
        with Turn("u1"):
            assert current_llm_calls() == 0, "a new turn must not inherit the previous turn's count"


class TestReentrantTurn:
    """process_request calls itself synchronously to resume a gated flow
    (_resume_invoice_flow and friends) — one user-visible turn, not two."""

    def test_nested_turn_does_not_reset_the_counter(self):
        with Turn("u1") as outer:
            note_llm_call()
            with Turn("u1") as inner:
                note_llm_call()
                note_llm_call()
            # after the inner (nested) turn exits, the count must still
            # reflect ALL three calls — not reset to 0 by the inner start.
            assert current_llm_calls() == 3
            outer.operation = "resumed"

    def test_only_outermost_turn_logs(self):
        calls = []
        with patch("utils.telemetry.log_turn", side_effect=lambda **kw: calls.append(kw)):
            with Turn("u1") as outer:
                note_llm_call()
                with Turn("u1"):
                    note_llm_call()
                outer.operation = "final_op"
                outer.response_text = "final answer"
        assert len(calls) == 1, f"expected exactly one telemetry line, got {len(calls)}"
        assert calls[0]["operation"] == "final_op"
        assert calls[0]["llm_calls"] == 2

    def test_nested_turn_logs_on_exception_only_at_outer_level(self):
        calls = []
        with patch("utils.telemetry.log_turn", side_effect=lambda **kw: calls.append(kw)):
            with pytest.raises(ValueError):
                with Turn("u1"):
                    with Turn("u1"):
                        raise ValueError("boom")
        assert len(calls) == 1
        assert "ValueError" in calls[0]["error"]

    def test_deeply_nested_still_one_log_line(self):
        """Three levels deep (a resumed flow that itself resumes another) —
        still exactly one line, matching how many real webhook calls happened."""
        calls = []
        with patch("utils.telemetry.log_turn", side_effect=lambda **kw: calls.append(kw)):
            with Turn("u1"):
                note_llm_call()
                with Turn("u1"):
                    note_llm_call()
                    with Turn("u1"):
                        note_llm_call()
        assert len(calls) == 1
        assert calls[0]["llm_calls"] == 3


class TestRouteTagging:
    """WP-5 item 3: measure the AnswerLedger fast-path's real share. A
    contextvar (not a Turn attribute set by the caller after the fact,
    and NOT an IntentService instance attribute — that's a process-wide
    singleton shared across concurrent requests, so instance state would
    race between two users' messages) so code deep inside process_request
    (the ledger short-circuit) can tag the route without threading a Turn
    reference all the way down."""

    def test_no_route_outside_a_turn(self):
        assert current_route() is None

    def test_note_route_visible_within_the_turn(self):
        with Turn("u1"):
            note_route("ledger")
            assert current_route() == "ledger"

    def test_route_reaches_the_log_line(self):
        calls = []
        with patch("utils.telemetry.log_turn", side_effect=lambda **kw: calls.append(kw)):
            with Turn("u1") as t:
                note_route("ledger")
                t.operation = "query"
        assert calls[0]["route"] == "ledger"

    def test_reset_between_separate_turns(self):
        with Turn("u1"):
            note_route("ledger")
        with Turn("u1"):
            assert current_route() is None, "a new turn must not inherit the previous turn's route"

    def test_direct_turn_route_override_takes_precedence(self):
        """Turn.route can still be set directly (e.g. by a future call site
        that already has the Turn object in scope) and wins over the
        contextvar."""
        calls = []
        with patch("utils.telemetry.log_turn", side_effect=lambda **kw: calls.append(kw)):
            with Turn("u1") as t:
                note_route("ledger")
                t.route = "explicit_override"
        assert calls[0]["route"] == "explicit_override"

    def test_no_route_noted_logs_unclassified_in_the_rendered_line(self):
        calls = []
        with patch.object(telemetry, "log_turn", wraps=log_turn) as spy:
            with patch.object(telemetry.logger, "info") as mock_info:
                with Turn("u1") as t:
                    t.operation = "query"
        line = mock_info.call_args.args[0]
        assert "route=unclassified" in line

    def test_nested_turn_route_still_reported_by_outer(self):
        """A route noted inside a nested (resumed-flow) continuation must
        still surface on the outer turn's single log line — same reentrancy
        contract as llm_calls."""
        calls = []
        with patch("utils.telemetry.log_turn", side_effect=lambda **kw: calls.append(kw)):
            with Turn("u1"):
                with Turn("u1"):
                    note_route("ledger")
        assert len(calls) == 1
        assert calls[0]["route"] == "ledger"


class TestLlmCallsAlertThreshold:
    def test_alert_logged_when_over_threshold(self):
        with patch.object(telemetry.logger, "warning") as mock_warn:
            log_turn(user_id="u1", turn_ms=100, operation="query", fallback=False, llm_calls=3)
        alert_calls = [c for c in mock_warn.call_args_list if "TELEMETRY_ALERT" in c.args[0]]
        assert len(alert_calls) == 1

    def test_no_alert_at_exactly_the_threshold(self):
        with patch.object(telemetry.logger, "warning") as mock_warn:
            log_turn(user_id="u1", turn_ms=100, operation="query", fallback=False, llm_calls=2)
        alert_calls = [c for c in mock_warn.call_args_list if "TELEMETRY_ALERT" in c.args[0]]
        assert alert_calls == []

    def test_no_alert_when_llm_calls_unknown(self):
        with patch.object(telemetry.logger, "warning") as mock_warn:
            log_turn(user_id="u1", turn_ms=100, operation="query", fallback=False, llm_calls=None)
        alert_calls = [c for c in mock_warn.call_args_list if "TELEMETRY_ALERT" in c.args[0]]
        assert alert_calls == []

    def test_alert_fires_through_the_real_turn_end_to_end(self):
        with patch.object(telemetry.logger, "warning") as mock_warn:
            with Turn("u1") as t:
                note_llm_call()
                note_llm_call()
                note_llm_call()
                t.operation = "query"
        alert_calls = [c for c in mock_warn.call_args_list if "TELEMETRY_ALERT" in c.args[0]]
        assert len(alert_calls) == 1


class TestFallbackDetection:
    @pytest.mark.parametrize("text", [
        "I couldn't format the reply properly.",
        "Two ways I could read that — which did you mean?",
        "Hmm, I couldn't quite work out what you're asking.",
        "Could you specify the date range?",
    ])
    def test_known_fallback_phrases_detected(self, text):
        assert is_fallback_response(text)

    @pytest.mark.parametrize("text", [
        "Your total earnings so far are ₹1,175,000.",
        "Star Studios owes you ₹200,000.",
        "",
        None,
    ])
    def test_real_answers_not_flagged(self, text):
        assert not is_fallback_response(text)


class TestProcessRequestWiring:
    """The public process_request wrapper must not change behaviour — only
    observe it — and must survive telemetry logging failures."""

    def _svc(self):
        with patch("services.intent_service.GeminiService"), \
             patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), \
             patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.gemini = MagicMock()
        svc.supabase = MagicMock()
        svc.memory = MagicMock()
        svc.supabase.get_user_profile.return_value = {
            "ok": True, "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "Test"},
        }
        svc.memory.get_form_state.return_value = None
        svc.memory.get_conversation_history.return_value = []
        svc.memory.get_user_memory.return_value = {}
        return svc

    def test_wrapper_returns_impl_result_unchanged(self):
        svc = self._svc()
        expected = {"operation": "stub", "response": "hi", "trigger_invoice": False, "invoice_data": {}}
        svc._process_request_impl = MagicMock(return_value=expected)
        result = svc.process_request("u1", "hello")
        assert result == expected

    def test_wrapper_logs_exactly_once(self):
        svc = self._svc()
        svc._process_request_impl = MagicMock(
            return_value={"operation": "query", "response": "ok", "trigger_invoice": False, "invoice_data": {}}
        )
        calls = []
        with patch("utils.telemetry.log_turn", side_effect=lambda **kw: calls.append(kw)):
            svc.process_request("u1", "hello")
        assert len(calls) == 1
        assert calls[0]["operation"] == "query"
        assert calls[0]["fallback"] is False

    def test_wrapper_flags_fallback_response(self):
        svc = self._svc()
        svc._process_request_impl = MagicMock(
            return_value={"operation": "query", "response": "Two ways I could read that.",
                          "trigger_invoice": False, "invoice_data": {}}
        )
        calls = []
        with patch("utils.telemetry.log_turn", side_effect=lambda **kw: calls.append(kw)):
            svc.process_request("u1", "hello")
        assert calls[0]["fallback"] is True

    def test_logging_failure_never_breaks_the_turn(self):
        svc = self._svc()
        expected = {"operation": "query", "response": "ok", "trigger_invoice": False, "invoice_data": {}}
        svc._process_request_impl = MagicMock(return_value=expected)
        with patch("utils.telemetry.log_turn", side_effect=Exception("logging backend down")):
            result = svc.process_request("u1", "hello")
        assert result == expected


class TestFlowMachineV2CanaryAllowlist:
    def test_unset_is_disabled(self, monkeypatch):
        from services.intent_service import _flow_machine_v2_enabled_for
        monkeypatch.delenv("FLOW_MACHINE_V2", raising=False)
        assert _flow_machine_v2_enabled_for("u1") is False

    @pytest.mark.parametrize("val", ["1", "true", "True", "yes", "on"])
    def test_truthy_values_enable_globally(self, monkeypatch, val):
        from services.intent_service import _flow_machine_v2_enabled_for
        monkeypatch.setenv("FLOW_MACHINE_V2", val)
        assert _flow_machine_v2_enabled_for("any_user_id") is True

    @pytest.mark.parametrize("val", ["0", "false", "no", "off", ""])
    def test_falsy_values_disable_globally(self, monkeypatch, val):
        from services.intent_service import _flow_machine_v2_enabled_for
        monkeypatch.setenv("FLOW_MACHINE_V2", val)
        assert _flow_machine_v2_enabled_for("any_user_id") is False

    def test_comma_list_enables_only_listed_users(self, monkeypatch):
        from services.intent_service import _flow_machine_v2_enabled_for
        monkeypatch.setenv("FLOW_MACHINE_V2", "751256859, 919876543")
        assert _flow_machine_v2_enabled_for("751256859") is True
        assert _flow_machine_v2_enabled_for("919876543") is True
        assert _flow_machine_v2_enabled_for("000000000") is False

    def test_canary_list_whitespace_tolerant(self, monkeypatch):
        from services.intent_service import _flow_machine_v2_enabled_for
        monkeypatch.setenv("FLOW_MACHINE_V2", "  751256859  ,919876543 ")
        assert _flow_machine_v2_enabled_for("751256859") is True

    def test_single_id_canary(self, monkeypatch):
        from services.intent_service import _flow_machine_v2_enabled_for
        monkeypatch.setenv("FLOW_MACHINE_V2", "751256859")
        assert _flow_machine_v2_enabled_for("751256859") is True
        assert _flow_machine_v2_enabled_for("other_user") is False
