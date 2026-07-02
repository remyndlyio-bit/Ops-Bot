"""
Tests for the billed-vs-received clarify flow: the pure detector, plus the
answer-with-offer handler and the reply resolver on a mocked IntentService.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import patch, MagicMock
from services.clarify import detect_value_fork, resolve_reply

KC = ["garnier india", "garnier", "pepsi", "samsung"]


class TestDetector:
    def test_made_from_client_forks(self):
        assert detect_value_fork("how much have I made from Garnier", KC) == {"fork": "billed_vs_received", "client": "garnier"}

    def test_earned_from_client_forks(self):
        assert detect_value_fork("what have I earned from Pepsi?", KC)["client"] == "pepsi"

    def test_hinglish_kamaya_forks(self):
        assert detect_value_fork("Samsung se kitna kamaya", KC)["client"] == "samsung"

    @pytest.mark.parametrize("msg", [
        "how much have I billed Garnier",        # billed specified
        "how much received from Garnier",        # received specified
        "how much does Garnier owe me",          # owed — different question
        "total unpaid for Garnier",              # status specified
    ])
    def test_specified_reading_no_fork(self, msg):
        assert detect_value_fork(msg, KC) is None

    def test_no_client_no_fork(self):
        assert detect_value_fork("how much have I earned", KC) is None

    def test_no_value_word_no_fork(self):
        assert detect_value_fork("show me Garnier jobs", KC) is None

    @pytest.mark.parametrize("msg", [
        "how much did I earn from Garnier last quarter",   # last <period>
        "earnings from Samsung this month",                # this <period>
        "what have I made from Pepsi in 2026",             # year
        "Samsung se pichle mahine kitna kamaya",           # hinglish period
        "how much have I earned from Garnier this year",   # this year
        "total made from Pepsi in March",                  # month name
    ])
    def test_dated_value_query_no_fork(self, msg):
        # The fork's SQL is all-time; a date window means the planner must answer,
        # else the fork silently drops the date and returns an all-time figure.
        assert detect_value_fork(msg, KC) is None

    def test_resolve_reply(self):
        assert resolve_reply("received please") == "received"
        assert resolve_reply("the billed total") == "billed"
        assert resolve_reply("hmm not sure") is None


class TestHandler:
    def _svc(self):
        with patch("services.intent_service.GeminiService"), patch("services.intent_service.ResendEmailService"), \
             patch("services.intent_service.SupabaseService"), patch("services.intent_service.MemoryService"):
            from services.intent_service import IntentService
            svc = IntentService()
        svc.supabase = MagicMock(); svc.memory = MagicMock()
        svc.memory.get_user_memory.return_value = {}

        def exec_sql(sql):
            s = sql.lower()
            if "distinct" in s and "union" in s:
                return {"ok": True, "rows": [{"n": "garnier india"}, {"n": "garnier"}]}
            if "sum(fees)" in s:
                if "'true','t','yes'" in s.replace(" ", ""):   # received (PAID_TRUE present)
                    return {"ok": True, "rows": [{"r": 80000}]}
                return {"ok": True, "rows": [{"r": 230000}]}    # billed
            return {"ok": True, "rows": []}
        svc.supabase.execute_sql.side_effect = exec_sql
        return svc

    def test_offers_alternative_when_billed_differs(self):
        svc = self._svc()
        r = svc._handle_value_fork("u1", "how much have I made from Garnier", "u1")
        assert r is not None
        assert "230,000" in r["response"] and "80,000" in r["response"] and "received" in r["response"].lower()
        # the pending choice was stored
        stored = [c.args[1] for c in svc.memory.update_user_memory.call_args_list if "pending_value_fork" in c.args[1]]
        assert stored and stored[-1]["pending_value_fork"]["billed"] == 230000

    def test_resolve_returns_received(self):
        svc = self._svc()
        svc.memory.get_user_memory.return_value = {"pending_value_fork": {"client": "Garnier", "billed": 230000, "received": 80000}}
        r = svc._resolve_value_fork("u1", "received")
        assert r is not None and "80,000" in r["response"] and "received" in r["response"].lower()

    def test_no_fork_for_specified_reading(self):
        svc = self._svc()
        assert svc._handle_value_fork("u1", "how much have I billed Garnier", "u1") is None
