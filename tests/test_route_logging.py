"""
TODO.md 0.3 — "[ROUTE] <checkpoint> claimed message: ..." on every dispatch,
and 0.4 — the same name mirrored into telemetry's `route=` field.

Why this file exists: an audit found 0.3 shipped 12 [ROUTE] lines but no
test asserting ANY of them, and two live checkpoints (`small_talk`,
`no_op_cancel`) had no line at all. The whole point of 0.3 is that when a
message gets hijacked in production you can grep one prefix and see which
checkpoint ate it — a checkpoint that returns silently defeats that, and
without a test nothing stops the next one from doing the same.

The legacy-path tests below force FLOW_MACHINE_V2 off. Both `small_talk`
and `no_op_cancel` sit in the legacy cascade (small-talk is explicitly
gated behind `if not _flow_machine_v2_enabled_for(...)`), so with v2 on the
classifier answers first and these checkpoints are never reached. Pinning
the flag keeps the tests deterministic and testing what they claim.
"""

import logging
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
    svc.supabase.get_user_profile.return_value = {
        "ok": True, "data": {"onboarded_at": "2024-01-01", "name": "D"},
    }
    svc.gemini.is_invoice_action_request.return_value = False
    svc.gemini.is_new_query_not_response.return_value = False
    return svc


def _route_lines(caplog):
    return [r.getMessage() for r in caplog.records if "[ROUTE]" in r.getMessage()]


class TestPlainQueryEmitsExactlyOneRouteLine:
    """0.3's specified test: exactly one [ROUTE] line per turn. More than
    one means two checkpoints both claimed the message, which is precisely
    the hijack class this logging exists to expose."""

    def test_exactly_one_route_line(self, caplog):
        svc = _svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True, "operation": "select",
            "rows": [{"result": 12}],
        }
        with caplog.at_level(logging.INFO):
            svc.process_request("u1", "how many jobs do I have")
        lines = _route_lines(caplog)
        assert len(lines) == 1, f"expected exactly 1 [ROUTE] line, got {len(lines)}: {lines}"


class TestSmallTalkRouteLine:
    """Newly added — this checkpoint returned silently before."""

    def test_small_talk_logs_route(self, caplog):
        svc = _svc()
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False), \
             patch.object(type(svc), "_detect_small_talk", return_value="Hey there!"), \
             caplog.at_level(logging.INFO):
            result = svc.process_request("u1", "hello")
        assert result["operation"] == "small_talk"
        assert any("small_talk claimed message" in l for l in _route_lines(caplog)), (
            f"no [ROUTE] small_talk line; got: {_route_lines(caplog)}"
        )

    def test_small_talk_sets_telemetry_route(self, caplog):
        """0.4: the same name must reach the [TELEMETRY] line, so
        'what % of turns get hijacked' is answerable from logs."""
        svc = _svc()
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False), \
             patch.object(type(svc), "_detect_small_talk", return_value="Hey there!"), \
             caplog.at_level(logging.INFO):
            svc.process_request("u1", "hello")
        telemetry = [r.getMessage() for r in caplog.records if "[TELEMETRY]" in r.getMessage()]
        assert telemetry, "no [TELEMETRY] line emitted"
        assert "route=small_talk" in telemetry[-1], telemetry[-1]


class TestNoOpCancelRouteLine:
    """Newly added — 'cancel' with nothing pending also returned silently."""

    def test_no_op_cancel_logs_route(self, caplog):
        svc = _svc()
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False), \
             caplog.at_level(logging.INFO):
            result = svc.process_request("u1", "cancel")
        assert result["operation"] == "no_op_cancel"
        assert any("no_op_cancel claimed message" in l for l in _route_lines(caplog)), (
            f"no [ROUTE] no_op_cancel line; got: {_route_lines(caplog)}"
        )

    def test_no_op_cancel_sets_telemetry_route(self, caplog):
        svc = _svc()
        with patch("services.intent_service._flow_machine_v2_enabled_for", return_value=False), \
             caplog.at_level(logging.INFO):
            svc.process_request("u1", "cancel")
        telemetry = [r.getMessage() for r in caplog.records if "[TELEMETRY]" in r.getMessage()]
        assert telemetry and "route=no_op_cancel" in telemetry[-1], telemetry[-1] if telemetry else "none"


class TestRouteLineFormatIsUniform:
    """0.3 specifies ONE format so the logs stay greppable. A line that
    drifts from it still 'logs something' but breaks the tooling the task
    exists to enable."""

    def test_all_route_lines_share_the_format(self):
        path = os.path.join(os.path.dirname(__file__), "..", "services", "intent_service.py")
        with open(path, encoding="utf-8") as fh:
            src = fh.read().split("\n")
        # Only real log STATEMENTS — a comment mentioning [ROUTE] (e.g. one
        # explaining the grep this enables) is not a route line.
        bad = [
            f"line {i+1}: {l.strip()}"
            for i, l in enumerate(src)
            if "[ROUTE]" in l and "logger." in l and "claimed message" not in l
        ]
        assert not bad, "[ROUTE] lines not matching '<name> claimed message: ...':\n  " + "\n  ".join(bad)

    def test_every_route_log_has_a_matching_note_route(self):
        """0.3 and 0.4 are meant to stay in lockstep: whatever is logged is
        what telemetry reports. A [ROUTE] line without a note_route() call
        beside it means the log and the metric disagree."""
        path = os.path.join(os.path.dirname(__file__), "..", "services", "intent_service.py")
        with open(path, encoding="utf-8") as fh:
            src = fh.read().split("\n")
        missing = []
        for i, line in enumerate(src):
            if "[ROUTE]" not in line or "logger." not in line:
                continue  # comments mentioning [ROUTE] aren't dispatch sites
            window = " ".join(src[max(0, i - 2): i + 3])
            if "note_route(" not in window:
                missing.append(f"line {i+1}: {line.strip()}")
        assert not missing, "[ROUTE] log without a nearby note_route():\n  " + "\n  ".join(missing)
