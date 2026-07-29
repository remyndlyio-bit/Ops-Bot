"""
tests/test_process_request_safety_net.py
==========================================

Regression coverage for a live/scripted-test finding: _process_request_impl
(~2000 lines, dozens of exit points across onboarding, smart-capture,
invoice, and query branches) occasionally returned None instead of a
response dict, non-deterministically -- it reproduced for different,
unrelated message shapes across different test runs, pointing at something
transient rather than one fixed code-path bug. The immediate crash was in
process_request's wrapper:

    t.operation = result.get("operation")
    AttributeError: 'NoneType' object has no attribute 'get'

Whatever the deeper cause, the user got ZERO reply for that turn -- a hard
crash instead of even a generic error message. process_request now treats
any non-dict return from _process_request_impl as a bug to log and recover
from, not something to propagate. These tests pin that safety net down
directly, independent of whatever eventually causes the None.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

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
    return svc


class TestNoneResultSafetyNet:
    def test_none_result_does_not_crash(self):
        svc = _make_svc()
        svc._process_request_impl = MagicMock(return_value=None)
        result = svc.process_request("u1", "anything")
        assert isinstance(result, dict)

    def test_none_result_returns_a_real_response_string(self):
        svc = _make_svc()
        svc._process_request_impl = MagicMock(return_value=None)
        result = svc.process_request("u1", "anything")
        assert isinstance(result.get("response"), str) and result["response"].strip()

    def test_none_result_response_shape_matches_normal_dict(self):
        """Downstream code (main.py's webhook handler) reads operation,
        response, trigger_invoice, invoice_data unconditionally -- the
        fallback must have all of them, not just 'response'."""
        svc = _make_svc()
        svc._process_request_impl = MagicMock(return_value=None)
        result = svc.process_request("u1", "anything")
        for key in ("operation", "response", "trigger_invoice", "invoice_data"):
            assert key in result, f"fallback dict missing {key!r}"
        assert result["trigger_invoice"] is False
        assert result["invoice_data"] == {}

    def test_normal_dict_result_passes_through_unchanged(self):
        """Over-correction guard: a well-formed result must not be touched."""
        svc = _make_svc()
        real_result = {"operation": "query", "response": "42 jobs", "trigger_invoice": False, "invoice_data": {}}
        svc._process_request_impl = MagicMock(return_value=real_result)
        result = svc.process_request("u1", "anything")
        assert result == real_result

    def test_other_falsy_non_dict_values_also_handled(self):
        """Belt and suspenders: an empty string or list would crash the
        same way None did -- must be treated identically."""
        for bad_value in (None, "", [], 0, False):
            svc = _make_svc()
            svc._process_request_impl = MagicMock(return_value=bad_value)
            result = svc.process_request("u1", "anything")
            assert isinstance(result, dict) and result.get("response")
