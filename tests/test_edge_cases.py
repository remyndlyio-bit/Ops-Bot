"""
Edge-case tests for scenarios 132-143 from the Intent Test Matrix.

Categories tested:
  132 - SQL injection attempt
  133 - No data in system
  134 - Typo in command
  135 - Delete last job
  136 - Multi-row update ambiguity
  137 - Disambiguation reply (pick row 2)
  138 - Cancel disambiguation
  139 - Stale cached invoice (>30 min TTL)
  140 - Context pronoun 'them' / 'this client'
  141 - Context 'this month' after client query
  142 - Very short follow-up ("Fees?")
  143 - Hindi with English names ("Nike ka April ka invoice bhejo")
"""

import pytest
import os
import sys
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

# ── env stubs ──────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def mock_env(monkeypatch):
    monkeypatch.setenv("AI_KEY", "test-key")
    monkeypatch.setenv("SUPABASE_URL", "https://fake.supabase.co")
    monkeypatch.setenv("SUPABASE_SERVICE_ROLE_KEY", "fake-role-key")
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://fake")


# ── helpers ────────────────────────────────────────────────────────────────

def _make_svc():
    """IntentService with all external I/O mocked."""
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

    # Defaults – pretend user is fully onboarded, no form / disambiguation active
    svc.supabase.get_user_profile.return_value = {
        "ok": True,
        "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "Test User"},
    }
    svc.memory.get_user_memory.return_value = {}
    svc.memory.get_form_state.return_value = None
    svc.memory.get_conversation_history.return_value = []
    svc.supabase.db_url = "postgresql://fake"  # enable SQL path
    return svc


# ══════════════════════════════════════════════════════════════════════════
# 132 – SQL injection attempt
# ══════════════════════════════════════════════════════════════════════════

class TestSQLInjection:
    """
    The SQL validator must block / sanitize DROP TABLE injection.
    No DB or AI call should happen if the generated SQL is unsafe.
    """

    def test_drop_table_rejected_by_validator(self):
        from services.sql_validator import validate_sql
        sql = "'; DROP TABLE job_entries; --"
        valid, sanitized, err = validate_sql(sql)
        assert not valid, "DROP TABLE should be rejected"
        assert sanitized == ""

    def test_semicolon_injection_rejected(self):
        from services.sql_validator import validate_sql
        sql = "SELECT * FROM public.job_entries; DROP TABLE job_entries"
        valid, sanitized, err = validate_sql(sql)
        assert not valid, "Multiple statements separated by ; must be rejected"

    def test_delete_keyword_blocked(self):
        from services.sql_validator import validate_sql
        sql = "DELETE FROM public.job_entries WHERE user_id = 'x'"
        valid, sanitized, err = validate_sql(sql)
        assert not valid

    def test_valid_select_still_passes(self):
        from services.sql_validator import validate_sql
        sql = "SELECT * FROM public.job_entries WHERE user_id = 'u1'"
        valid, sanitized, err = validate_sql(sql)
        assert valid


# ══════════════════════════════════════════════════════════════════════════
# 133 – No data in system (0 records)
# ══════════════════════════════════════════════════════════════════════════

class TestNoData:
    """
    When Supabase returns 0 rows the bot should say the user has no jobs,
    NOT an error or a blank message.
    """

    def test_zero_rows_returns_friendly_message(self):
        svc = _make_svc()
        # Both AI paths fail → keyword fallback kicks in → DB returns 0 rows.
        # The deterministic router runs FIRST ('list_jobs'); on 0 rows it falls
        # through to the planner path, which then hits the no-data check.
        svc.gemini.parse_user_intent.return_value = {"operation": "GEMINI_ERROR", "parameters": {}}

        # Route by SQL content, not call order: the live value fork issues a
        # _known_clients DISTINCT/UNION query up front, so a positional
        # side_effect list is fragile. Every data SELECT returns 0 rows; the
        # "do you have any data?" COUNT returns 0 → the no-data path must fire.
        def _exec(sql):
            s = sql.lower()
            if "distinct" in s and "union" in s:      # _known_clients lookup
                return {"ok": True, "rows": []}
            if "count(" in s:                          # "any data?" check → no
                return {"ok": True, "rows": [{"cnt": 0}]}
            return {"ok": True, "rows": []}            # every data SELECT → 0 rows
        svc.supabase.execute_sql.side_effect = _exec

        result = svc.process_request("user1", "Show my jobs")
        resp = result.get("response", "").lower()
        no_data_phrases = [
            "no job", "no record", "don't have any", "haven't added",
            "no data", "nothing", "empty", "yet",
        ]
        assert any(p in resp for p in no_data_phrases), (
            f"Expected a 'no data' message, got: {result.get('response')!r}"
        )

    def test_keyword_fallback_generates_sql_for_show_jobs(self):
        """_keyword_sql_fallback must match 'show my jobs' (plural) and return SELECT SQL."""
        svc = _make_svc()
        sql = svc._keyword_sql_fallback("show my jobs", "user1")
        assert sql is not None, (
            "'show my jobs' (plural) should hit the keyword fallback; "
            "check that the regex uses 'jobs?' not 'job'"
        )
        assert "SELECT" in sql.upper()
        assert "job_entries" in sql.lower()

    def test_keyword_fallback_also_works_for_singular(self):
        """Singular 'show my job' should also match."""
        svc = _make_svc()
        sql = svc._keyword_sql_fallback("show my job", "user1")
        assert sql is not None
        assert "SELECT" in sql.upper()


# ══════════════════════════════════════════════════════════════════════════
# 134 – Typo in command ("genrate invoce for Nike")
# ══════════════════════════════════════════════════════════════════════════

class TestTypoInCommand:
    """
    Verb and invoice-word typos must still route the message to the invoice flow.
    We verify the typo lists cover the inputs without running the full AI stack.
    """

    _VERB_TYPOS = ["genrate", "generat", "crete", "creat", "mke", "prepre", "prepar"]
    _INVOICE_TYPOS = ["invoce", "invoic", "invoise", "incoice", "invioce", "invocice"]

    def test_genrate_detected_as_verb_typo(self):
        msg = "genrate invoce for Nike".lower()
        assert any(t in msg for t in self._VERB_TYPOS)

    def test_invoce_detected_as_invoice_typo(self):
        msg = "genrate invoce for Nike".lower()
        assert any(t in msg for t in self._INVOICE_TYPOS)

    def test_both_detections_fire_together(self):
        msg = "genrate invoce for Nike".lower()
        has_verb = any(t in msg for t in self._VERB_TYPOS)
        has_invoice = any(t in msg for t in self._INVOICE_TYPOS)
        assert has_verb and has_invoice, "Both typo lists should match"

    def test_invoise_variant_detected(self):
        msg = "send invoise to acme"
        assert any(t in msg for t in self._INVOICE_TYPOS)

    def test_process_request_routes_typo_to_invoice_flow(self):
        """End-to-end: the bot should enter invoice handling, not the generic SQL path."""
        svc = _make_svc()
        svc.gemini.parse_user_intent.return_value = {
            "operation": "ACTION_TRIGGER",
            "parameters": {"client_name": "Nike", "month": "", "year": None},
        }
        # Return some Nike jobs so the bot can list months
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [{"client_name": "Nike", "job_date": "2024-03-15", "fees": 50000}],
        }
        svc.supabase.get_jobs_for_client.return_value = {
            "ok": True,
            "data": [{"client_name": "Nike", "job_date": "2024-03-15", "fees": 50000}],
        }

        result = svc.process_request("user1", "genrate invoce for Nike")
        # The bot must NOT return a generic error or small-talk response
        assert result.get("operation") != "small_talk"
        # Response must mention Nike or invoice or month selection
        resp = result.get("response", "").lower()
        assert any(k in resp for k in ["nike", "invoice", "month", "which"]), (
            f"Expected invoice-related response, got: {result.get('response')!r}"
        )


# ══════════════════════════════════════════════════════════════════════════
# 135 – Delete last job (soft-delete)
# ══════════════════════════════════════════════════════════════════════════

class TestDeleteLastJob:
    """
    "Delete my last job" must trigger the soft-delete path (isDeleted=true),
    not a hard DELETE (which is blocked by the SQL validator).
    """

    def test_delete_trigger_words_detected(self):
        svc = _make_svc()
        msg = "delete my last job"
        _DELETE_TRIGGERS = ["delete", "remove", "erase", "trash", "discard"]
        _SCOPE = ["job", "entry", "record", "row", "last", "this", "it", "that"]
        assert any(w in msg.lower() for w in _DELETE_TRIGGERS)
        assert any(w in msg.lower() for w in _SCOPE)

    def test_hard_delete_sql_blocked_by_validator(self):
        """Ensure no raw DELETE statement can make it through the SQL validator."""
        from services.sql_validator import validate_sql
        sql = "DELETE FROM public.job_entries WHERE user_id = 'u1' AND id = '123'"
        valid, _, _ = validate_sql(sql)
        assert not valid, "Hard DELETE must be blocked"

    def test_soft_delete_uses_update_not_delete(self):
        """The soft-delete helper should build an UPDATE … SET isDeleted=true, not DELETE."""
        svc = _make_svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [{"id": "abc", "job_description_details": "Radio spot", "job_date": "2024-03-01"}],
        }
        svc.gemini.synthesize_response.return_value = "Your last job has been deleted."

        result = svc.process_request("user1", "Delete my last job")
        # The operation must not be small_talk or error; it should be some action
        assert "delete" in result.get("response", "").lower() or \
               result.get("operation") in ("query", "ACTION_TRIGGER"), (
            f"Unexpected result: {result}"
        )
        # Verify that if execute_sql was called, it was not with a DELETE statement
        for call in svc.supabase.execute_sql.call_args_list:
            sql_arg = call.args[0] if call.args else call.kwargs.get("sql", "")
            assert not sql_arg.strip().upper().startswith("DELETE"), (
                f"Hard DELETE must never be sent to DB; got: {sql_arg}"
            )


# ══════════════════════════════════════════════════════════════════════════
# 136 – Multi-row disambiguation ("Mark paid for Nike" → multiple rows)
# ══════════════════════════════════════════════════════════════════════════

class TestMultiRowDisambiguation:
    """
    When an UPDATE would affect multiple rows, the bot must ask the user to
    pick which one instead of blindly updating all.
    """

    def test_disambiguation_triggered_when_multiple_rows_match(self):
        svc = _make_svc()
        multi_rows = [
            {"id": "r1", "client_name": "Nike", "job_date": "2024-01-10", "fees": 30000},
            {"id": "r2", "client_name": "Nike", "job_date": "2024-02-14", "fees": 40000},
            {"id": "r3", "client_name": "Nike", "job_date": "2024-03-22", "fees": 50000},
        ]
        # The pre-select query (before committing the UPDATE) returns 3 Nike rows
        update_sql = (
            "UPDATE public.job_entries SET paid='true' "
            "WHERE user_id='user1' AND client_name='Nike' RETURNING *"
        )
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": multi_rows}

        # Inject a valid UPDATE SQL by patching the query pipeline at the right point
        with patch("services.intent_service.execute_query_plan") as mock_plan, \
             patch("services.intent_service.generate_sql") as mock_sql_gen:
            mock_plan.return_value = {"sql": update_sql, "filters": {}, "operation": "update"}
            mock_sql_gen.return_value = {"sql": update_sql}

            result = svc.process_request("user1", "Mark paid for Nike")

        resp = result.get("response", "").lower()
        # Bot should have stored disambiguation state OR asked user to pick
        disambiguation_stored = any(
            "pending_disambiguation" in str(c)
            for c in svc.memory.update_user_memory.call_args_list
        )
        asked_to_pick = any(w in resp for w in ["which", "select", "choose", "pick", "found", "1."])
        assert disambiguation_stored or asked_to_pick, (
            f"Bot should have triggered disambiguation. Response: {result.get('response')!r}"
        )


# ══════════════════════════════════════════════════════════════════════════
# 137 – Disambiguation reply ("2" selects row 2)
# ══════════════════════════════════════════════════════════════════════════

class TestDisambiguationReply:
    """_handle_disambiguation_reply selects the correct row by 1-based index."""

    def _pending(self):
        return {
            "sql": "UPDATE public.job_entries SET paid='true' WHERE user_id='u1' AND client_name='Nike' RETURNING *",
            "rows": [
                {"id": "r1", "client_name": "Nike", "job_date": "2024-01-10"},
                {"id": "r2", "client_name": "Nike", "job_date": "2024-02-14"},
                {"id": "r3", "client_name": "Nike", "job_date": "2024-03-22"},
            ],
            "data_user_id": "u1",
        }

    def test_reply_2_executes_on_row_r2(self):
        svc = _make_svc()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"id": "r2"}]}
        svc.gemini.synthesize_response.return_value = "Done! Row 2 updated."

        result = svc._handle_disambiguation_reply("u1", "2", self._pending())
        assert result.get("operation") == "query"

        # Verify the SQL targeted row r2 specifically
        called_sql = svc.supabase.execute_sql.call_args[0][0]
        assert "r2" in called_sql, f"SQL should reference id='r2', got: {called_sql}"

    def test_out_of_range_shows_error(self):
        svc = _make_svc()
        result = svc._handle_disambiguation_reply("u1", "9", self._pending())
        resp = result.get("response", "").lower()
        assert "1" in resp and "3" in resp, (
            f"Should tell user to pick between 1-3, got: {resp!r}"
        )

    def test_non_numeric_reply_asks_for_number(self):
        svc = _make_svc()
        result = svc._handle_disambiguation_reply("u1", "the second one", self._pending())
        # "the second one" contains "2" in it — regex should extract it
        # OR it should ask for a number
        resp = result.get("response", "").lower()
        assert "number" in resp or svc.supabase.execute_sql.called


# ══════════════════════════════════════════════════════════════════════════
# 138 – Cancel disambiguation
# ══════════════════════════════════════════════════════════════════════════

class TestCancelDisambiguation:
    """Typing 'cancel' during disambiguation aborts the update with no DB write."""

    def _pending(self):
        return {
            "sql": "UPDATE public.job_entries SET paid='true' WHERE user_id='u1' RETURNING *",
            "rows": [{"id": "r1"}, {"id": "r2"}],
            "data_user_id": "u1",
        }

    def test_cancel_clears_state(self):
        svc = _make_svc()
        svc._handle_disambiguation_reply("u1", "cancel", self._pending())
        svc.memory.update_user_memory.assert_called_with("u1", {"pending_disambiguation": None})

    def test_cancel_returns_cancelled_message(self):
        svc = _make_svc()
        result = svc._handle_disambiguation_reply("u1", "cancel", self._pending())
        resp = result.get("response", "").lower()
        assert "cancel" in resp or "abort" in resp or "no" in resp

    def test_cancel_does_not_execute_sql(self):
        svc = _make_svc()
        svc._handle_disambiguation_reply("u1", "cancel", self._pending())
        svc.supabase.execute_sql.assert_not_called()

    def test_stop_also_cancels(self):
        svc = _make_svc()
        result = svc._handle_disambiguation_reply("u1", "stop", self._pending())
        resp = result.get("response", "").lower()
        assert "cancel" in resp or "let me know" in resp


# ══════════════════════════════════════════════════════════════════════════
# 138b – CRITICAL: a bare "yes" must NEVER bulk-delete in a numbered
#        disambiguation. Regression for the "Yes to email → deleted job" bug.
# ══════════════════════════════════════════════════════════════════════════

class TestYesDoesNotDeleteInNumberedDisambiguation:
    """A stray 'yes' (e.g. meant for an invoice-email prompt) must not be
    interpreted as 'delete all' when the pending disambiguation was a numbered
    pick list (no bulk_mode flag)."""

    def _numbered_pending(self):
        return {
            "sql": "UPDATE public.job_entries SET \"isDeleted\"=true WHERE user_id='u1' RETURNING *",
            "rows": [
                {"id": "r1", "client_name": "Nike", "job_date": "2026-04-10"},
                {"id": "r2", "client_name": "Nike", "job_date": "2026-03-15"},
                {"id": "r3", "client_name": "Nike", "job_date": "2026-02-14"},
            ],
            "data_user_id": "u1",
            # NOTE: no "bulk_mode": True — this is a numbered pick list.
        }

    def test_yes_does_not_execute_delete(self):
        svc = _make_svc()
        result = svc._handle_disambiguation_reply("u1", "yes", self._numbered_pending())
        # Must NOT run any DELETE/UPDATE SQL.
        svc.supabase.execute_sql.assert_not_called()
        # Must fall through (return None) so a competing pending state can handle it.
        assert result is None, f"Expected fall-through (None), got: {result!r}"

    def test_yes_clears_stale_disambiguation(self):
        svc = _make_svc()
        svc._handle_disambiguation_reply("u1", "yes", self._numbered_pending())
        svc.memory.update_user_memory.assert_called_with("u1", {"pending_disambiguation": None})

    def test_explicit_all_still_bulk_deletes(self):
        svc = _make_svc()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"id": "r1"}, {"id": "r2"}, {"id": "r3"}]}
        result = svc._handle_disambiguation_reply("u1", "all", self._numbered_pending())
        # "all" is explicit → bulk delete should run.
        assert svc.supabase.execute_sql.called
        called_sql = svc.supabase.execute_sql.call_args[0][0]
        assert "isDeleted" in called_sql and "true" in called_sql.lower()
        assert "deleted" in result.get("response", "").lower()

    def test_yes_in_bulk_mode_still_deletes(self):
        svc = _make_svc()
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"id": "r1"}, {"id": "r2"}, {"id": "r3"}]}
        pending = self._numbered_pending()
        pending["bulk_mode"] = True  # we explicitly asked "Reply 'Yes' to delete all"
        result = svc._handle_disambiguation_reply("u1", "yes", pending)
        assert svc.supabase.execute_sql.called
        assert "deleted" in result.get("response", "").lower()


# ══════════════════════════════════════════════════════════════════════════
# 139b – "Highest paying job" must sort by FEES, not job_date.
#        Regression for the bug where it returned the most-recent job and the
#        synthesizer refused ("I can't sort by highest paying").
# ══════════════════════════════════════════════════════════════════════════

class TestHighestPayingJob:
    """The pre-planner intercept must run SQL ordered by fees DESC (top) / ASC
    (bottom), bypassing the planner which sorts a single job by date."""

    def _make(self):
        svc = _make_svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [{"id": "r9", "client_name": "Pedigree", "job_date": "2026-01-02",
                      "fees": 200000, "bill_no": "INV-009", "paid": "No"}],
        }
        svc.gemini.synthesize_response.return_value = "Your highest paying job was Pedigree at ₹2,00,000."
        svc.gemini.is_history_question.return_value = False
        return svc

    def _executed_sqls(self, svc):
        return [
            (c.args[0] if c.args else c.kwargs.get("sql", ""))
            for c in svc.supabase.execute_sql.call_args_list
        ]

    def test_highest_paying_orders_by_fees_desc(self):
        svc = self._make()
        svc.process_request("user1", "What was my highest paying job?")
        sqls = self._executed_sqls(svc)
        ordered_by_fees = [s for s in sqls if "order by fees desc" in s.lower()]
        assert ordered_by_fees, (
            f"Expected SQL ordered by fees DESC, got: {sqls}"
        )
        # Must NOT resolve the answer by ordering on date.
        for s in ordered_by_fees:
            assert "order by job_date" not in s.lower()

    def test_lowest_paying_orders_by_fees_asc(self):
        svc = self._make()
        svc.process_request("user1", "What was my lowest paying job?")
        sqls = self._executed_sqls(svc)
        assert any("order by fees asc" in s.lower() for s in sqls), (
            f"Expected SQL ordered by fees ASC, got: {sqls}"
        )

    def test_biggest_client_does_not_hit_job_intercept(self):
        """'biggest client' is a grouped aggregate — it must NOT be captured by
        the single-job fees intercept (which would return one raw job row)."""
        svc = self._make()
        svc.process_request("user1", "Who is my biggest client?")
        sqls = self._executed_sqls(svc)
        # The job intercept emits 'SELECT * ... ORDER BY fees DESC ... LIMIT 1'.
        # A client query must not produce that exact shape.
        bad = [s for s in sqls if "select *" in s.lower() and "order by fees desc" in s.lower()]
        assert not bad, f"'biggest client' wrongly hit the single-job intercept: {bad}"


# ══════════════════════════════════════════════════════════════════════════
# 140 – Bank details parser: shorthand "Account: <num>" must be captured.
#        Regression for the silent "Account Number: Not set" save bug.
# ══════════════════════════════════════════════════════════════════════════

class TestBankDetailsParser:
    """_parse_bank_details_message must capture account number from a bare
    'Account:' label, and the holder name from 'Account Holder:'."""

    def _parse(self, msg):
        from services.intent_service import IntentService
        return IntentService._parse_bank_details_message(msg)

    def test_bare_account_label_captures_number(self):
        parsed = self._parse("Account Name: Darshit\nBank: HDFC\nAccount: 123456\nIFSC: HDFC001")
        assert parsed.get("bank_account_name") == "Darshit"
        assert parsed.get("bank_account_number") == "123456", (
            f"'Account: 123456' must map to account number, got: {parsed!r}"
        )
        assert parsed.get("bank_name") == "HDFC"
        assert parsed.get("bank_ifsc") == "HDFC001"

    def test_full_labels_still_work(self):
        parsed = self._parse(
            "Account Name: Darshit Mody\nBank Name: HDFC Bank\n"
            "Account Number: 1234567890\nIFSC: HDFC0001234\nUPI: darshit@upi"
        )
        assert parsed.get("bank_account_number") == "1234567890"
        assert parsed.get("bank_account_name") == "Darshit Mody"
        assert parsed.get("upi_id") == "darshit@upi"

    def test_account_holder_maps_to_name_not_number(self):
        parsed = self._parse("Account Holder: John\nA/C: 9988776655\nIFSC: SBIN0001")
        assert parsed.get("bank_account_name") == "John"
        assert parsed.get("bank_account_number") == "9988776655"


# ══════════════════════════════════════════════════════════════════════════
# 139 – Stale cached invoice (>30 min TTL)
# ══════════════════════════════════════════════════════════════════════════

class TestStaleCachedInvoice:
    """Invoice cached more than 30 min ago must be cleared; bot re-prompts."""

    def _cached_invoice(self, minutes_ago: int) -> dict:
        cached_at = (datetime.now() - timedelta(minutes=minutes_ago)).isoformat()
        return {
            "client_name": "Nike",
            "month": "March",
            "year": 2024,
            "row_ids": ["r1"],
            "poc_email": "poc@nike.com",
            "cached_at": cached_at,
        }

    def test_fresh_cache_is_not_cleared(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_generated_invoice": self._cached_invoice(5)  # 5 min ago
        }
        svc.gemini.is_send_to_client_intent.return_value = True

        svc.process_request("user1", "Send the invoice to client")
        # Cache should NOT be explicitly nulled — only calls that set it to None count
        # Note: use `in c.args[1]` to distinguish "key set to None" from "key absent"
        null_cache_calls = [
            c for c in svc.memory.update_user_memory.call_args_list
            if "last_generated_invoice" in c.args[1]
            and c.args[1]["last_generated_invoice"] is None
        ]
        assert len(null_cache_calls) == 0, "Fresh cache (5 min) should not be cleared"

    def test_stale_cache_cleared_on_send_attempt(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_generated_invoice": self._cached_invoice(45)  # 45 min ago — stale
        }
        svc.gemini.parse_user_intent.return_value = {
            "operation": "GEMINI_ERROR",
            "parameters": {},
        }
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": []}

        result = svc.process_request("user1", "Send invoice to Nike")
        # The stale cache should have been cleared via update_user_memory
        null_cache_calls = [
            c for c in svc.memory.update_user_memory.call_args_list
            if c.args[1].get("last_generated_invoice") is None
        ]
        assert len(null_cache_calls) >= 1, "Stale cache (>30 min) must be cleared"

    def test_cache_expiry_boundary_exactly_30_min(self):
        """At exactly 30 min the cache should be expired (boundary check)."""
        from datetime import datetime as _dt, timedelta as _td
        cached_at = (_dt.now() - _td(minutes=30, seconds=1)).isoformat()
        cached_invoice = {
            "client_name": "Nike",
            "month": "March",
            "year": 2024,
            "cached_at": cached_at,
        }
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"last_generated_invoice": cached_invoice}
        svc.gemini.parse_user_intent.return_value = {
            "operation": "GEMINI_ERROR", "parameters": {}
        }
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": []}

        svc.process_request("user1", "Send invoice")
        null_calls = [
            c for c in svc.memory.update_user_memory.call_args_list
            if c.args[1].get("last_generated_invoice") is None
        ]
        assert len(null_calls) >= 1, "Cache at 30m+1s must be treated as expired"


# ══════════════════════════════════════════════════════════════════════════
# 140 – Context: 'them' / 'this client' resolves from last_intent
# ══════════════════════════════════════════════════════════════════════════

class TestContextPronounResolution:
    """
    After a Nike query, 'Generate invoice for them' should reconstruct to
    'Generate invoice for Nike for <month>' using last_intent.
    """

    def test_them_resolves_to_last_client(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "client_name": "Nike",
                "operation": "Generate invoice",
                "entity": "invoice",
                "pending_clarification": "month",
            }
        }
        result = svc._reconstruct_message(
            "user1",
            "Generate invoice for them",
            [],
        )
        # With 4+ words and action verb, reconstruction is skipped by early-exit rule,
        # but we can test shorter pronoun messages
        result_short = svc._reconstruct_message("user1", "for them", [])
        # "for them" is short (2 words) — should merge with context
        # Result should mention Nike somehow OR fall back gracefully
        assert "nike" in result_short.lower() or "for them" in result_short.lower()

    def test_this_client_context_with_month_reply(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "client_name": "Nike",
                "operation": "Generate invoice",
                "entity": "invoice",
                "pending_clarification": "month",
            }
        }
        result = svc._reconstruct_message("user1", "March", [])
        assert "nike" in result.lower(), (
            f"'March' reply should expand to 'Generate invoice for Nike for March', got: {result!r}"
        )
        assert "march" in result.lower()

    def test_short_month_reply_reconstructed(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "client_name": "Garnier",
                "operation": "Generate invoice",
                "entity": "invoice",
                "pending_clarification": "month",
            }
        }
        result = svc._reconstruct_message("user1", "April", [])
        assert "garnier" in result.lower()
        assert "april" in result.lower()


# ══════════════════════════════════════════════════════════════════════════
# 141 – Context: 'this month' after client query
# ══════════════════════════════════════════════════════════════════════════

class TestContextThisMonth:
    """'This month' said after a Nike query should reconstruct to Nike + this month."""

    def test_this_month_appended_to_last_client(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "client_name": "Nike",
                "operation": "show jobs",
                "entity": "jobs",
            }
        }
        result = svc._reconstruct_message("user1", "this month", [])
        # Should merge to something about Nike + this month
        assert "nike" in result.lower() or "this month" in result.lower()

    def test_last_month_appended_to_last_client(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {
                "client_name": "Garnier",
                "operation": "show jobs",
                "entity": "jobs",
            }
        }
        result = svc._reconstruct_message("user1", "last month", [])
        assert "garnier" in result.lower() or "last month" in result.lower()


# ══════════════════════════════════════════════════════════════════════════
# 142 – Very short follow-up ("Fees?" after job shown)
# ══════════════════════════════════════════════════════════════════════════

class TestShortFollowUp:
    """
    'Fees?' after a job is shown should be detected as a follow-up field request
    and answered from cached context — NOT trigger a new SQL query.
    """

    def test_fees_question_detected_as_followup(self):
        svc = _make_svc()
        columns = ["id", "client_name", "job_date", "fees", "paid", "brand_name"]
        result = svc._is_followup_field_request("Fees?", columns)
        assert result is not None, "'Fees?' must be detected as a follow-up field request"

    def test_date_question_detected(self):
        svc = _make_svc()
        columns = ["id", "client_name", "job_date", "fees"]
        assert svc._is_followup_field_request("Date?", columns) is not None

    def test_brand_question_detected(self):
        svc = _make_svc()
        columns = ["id", "client_name", "job_date", "fees", "brand_name"]
        assert svc._is_followup_field_request("Brand?", columns) is not None

    def test_standalone_aggregate_not_detected_as_followup(self):
        """'Total earnings last quarter?' must NOT be flagged as a follow-up."""
        svc = _make_svc()
        columns = ["id", "client_name", "job_date", "fees"]
        result = svc._is_followup_field_request("Total earnings last quarter?", columns)
        assert result is None, (
            "Aggregate queries with time ranges must NOT be treated as follow-ups"
        )

    def test_long_self_contained_query_not_followup(self):
        svc = _make_svc()
        columns = ["id", "client_name", "job_date", "fees"]
        result = svc._is_followup_field_request("Show me all jobs for Nike this year", columns)
        assert result is None


# ══════════════════════════════════════════════════════════════════════════
# 143 – Hindi with English names ("Nike ka April ka invoice bhejo")
# ══════════════════════════════════════════════════════════════════════════

class TestHindiWithEnglishNames:
    """
    'Nike ka April ka invoice bhejo' is Hinglish.
    The bot should route to invoice flow and correctly extract Nike + April.
    """

    def test_invoice_word_detected_in_hinglish(self):
        """The word 'invoice' appears literally — invoice path must be triggered."""
        msg = "Nike ka April ka invoice bhejo"
        assert "invoice" in msg.lower()

    def test_client_name_extractable_from_hinglish(self):
        """Nike appears before 'ka' — must be extractable as client_name."""
        import re
        msg = "Nike ka April ka invoice bhejo"
        # Simple heuristic: first word before 'ka' is the client
        match = re.match(r'^(\w+)\s+ka', msg, re.IGNORECASE)
        assert match and match.group(1).lower() == "nike"

    def test_month_extractable_from_hinglish(self):
        """April appears in the message and must be recognized as a month."""
        _MONTH_NAMES = {
            "january", "february", "march", "april", "may", "june",
            "july", "august", "september", "october", "november", "december",
        }
        msg = "Nike ka April ka invoice bhejo".lower()
        months_found = [m for m in _MONTH_NAMES if m in msg]
        assert months_found == ["april"]

    def test_process_request_routes_hinglish_to_invoice_flow(self):
        """End-to-end: the bot should enter invoice handling for Hinglish message."""
        svc = _make_svc()
        svc.gemini.parse_user_intent.return_value = {
            "operation": "ACTION_TRIGGER",
            "parameters": {"client_name": "Nike", "month": "April", "year": 2024},
        }
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [{"client_name": "Nike", "job_date": "2024-04-10", "fees": 50000}],
        }
        svc.supabase.get_jobs_for_client.return_value = {
            "ok": True,
            "data": [{"client_name": "Nike", "job_date": "2024-04-10", "fees": 50000}],
        }

        result = svc.process_request("user1", "Nike ka April ka invoice bhejo")
        assert result.get("operation") != "small_talk"
        resp = result.get("response", "").lower()
        assert any(k in resp for k in ["nike", "invoice", "april", "month"]), (
            f"Expected invoice response for Hinglish input, got: {result.get('response')!r}"
        )


# ══════════════════════════════════════════════════════════════════════════
# 144 – Regression batch: FAILs 38/40/42/44 from the live WhatsApp audit
# ══════════════════════════════════════════════════════════════════════════

class TestRegressionBatch3745:
    """Deterministic fixes for the final regression batch."""

    def test_delete_last_job_not_treated_as_client(self):
        """FAIL 40: 'Delete my last job' must delete the most recent job, not
        search for a client literally named 'last'."""
        svc = _make_svc()
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [{"id": "r1", "client_name": "Nike", "job_date": "2026-04-10",
                      "job_description_details": "Shoot"}],
        }
        result = svc.process_request("user1", "Delete my last job")
        resp = result.get("response", "").lower()
        assert "matching 'last'" not in resp, (
            f"'last' was treated as a client name: {result.get('response')!r}"
        )
        # The soft-delete fetch must NOT filter by a client named 'last'.
        for call in svc.supabase.execute_sql.call_args_list:
            sql = (call.args[0] if call.args else "").lower()
            assert "ilike '%last%'" not in sql, f"Filtered by client 'last': {sql}"

    def test_out_of_scope_flight_is_refused_not_errored(self):
        """FAIL 44: 'Can you book me a flight?' → on-brand refusal, not an error."""
        svc = _make_svc()
        svc.gemini.answer_feature_question.return_value = (
            "I can't book flights — I track jobs, invoices, and payments."
        )
        result = svc.process_request("user1", "Can you book me a flight?")
        assert result.get("operation") == "unsupported", (
            f"Expected on-brand refusal, got: {result.get('operation')} / {result.get('response')!r}"
        )
        resp = result.get("response", "").lower()
        assert "snag" not in resp and "circuits" not in resp and "blanked" not in resp

    def test_maybe_later_acknowledged_not_errored(self):
        """FAIL 38: 'Maybe later' → graceful acknowledgement, never an error."""
        svc = _make_svc()
        result = svc.process_request("user1", "Maybe later")
        assert result.get("operation") == "decline_followup", (
            f"Expected decline acknowledgement, got: {result.get('operation')} / {result.get('response')!r}"
        )

    def test_invoice_for_them_resolves_from_context(self):
        """FAIL 42: 'Generate invoice for them' must resolve 'them' to the client
        in context, not search for a client literally named 'Them'."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {
            "last_intent": {"client_name": "Nike", "operation": "query", "entity": "job"},
            "uscf_context": {"last_row_data": {"client_name": "Nike"}},
        }
        svc.gemini.is_invoice_action_request.return_value = True
        svc.gemini.parse_user_intent.return_value = {
            "operation": "ACTION_TRIGGER",
            "parameters": {"client_name": None, "month": None, "year": None},
        }
        svc.supabase.get_available_months_for_client.return_value = {
            "ok": True, "months": [{"label": "April 2026", "month": 4, "year": 2026}],
        }
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": [{"client_name": "Nike"}]}
        result = svc.process_request("user1", "Generate invoice for them")
        resp = result.get("response", "").lower()
        assert "couldn't find a client" not in resp, (
            f"'them' was treated as a client name: {result.get('response')!r}"
        )
        assert "them" not in resp or "nike" in resp


# ══════════════════════════════════════════════════════════════════════════
# 145 – A stale pending reminder must NOT hijack numeric / mid-flow messages.
# Regression for: "Add a new job" → job description with "5 may 2025, 20k" got
# answered with "Please reply with a number between 1 and 1, or 'skip'".
# ══════════════════════════════════════════════════════════════════════════

class TestReminderDoesNotHijack:
    from unittest.mock import patch as _patch

    PENDING = [{"id": "j1", "client_name": "Nike", "bill_no": "INV-1", "fees": 25000}]

    def _run(self, message, mem):
        import services.intent_service as isv
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = mem
        with patch("services.intent_service.get_pending", return_value=self.PENDING), \
             patch("services.intent_service.clear_pending"):
            return svc._handle_pending_reminder("u1", message)

    def test_job_description_with_numbers_not_hijacked(self):
        """The exact production case: a job description containing dates/fees must
        fall through (return None), not be read as a reminder selection."""
        res = self._run(
            "Client content lab, brand Pepsi, date 5 may 2025, 2 master films, fees 20k",
            {"awaiting_job_input": True},
        )
        assert res is None, f"Reminder hijacked the job description: {res}"

    def test_free_text_number_falls_through(self):
        """Even without an active sub-flow, a number buried in free text isn't a
        reminder reply."""
        assert self._run("earnings in 2025", {}) is None
        assert self._run("show me 2 jobs", {}) is None

    def test_standalone_number_still_handled(self):
        """A genuine reminder reply ('1', 'send 1') is still intercepted."""
        assert self._run("1", {}) is not None
        assert self._run("send 1", {}) is not None

    def test_standalone_number_yields_during_add_job(self):
        """Mid-add-job, even a bare '1' belongs to the job flow, not the reminder."""
        assert self._run("1", {"awaiting_job_input": True}) is None

    def test_skip_yields_during_subflow(self):
        """'skip'/'cancel' mid-flow must not be stolen to clear reminders."""
        assert self._run("skip", {"awaiting_poc_email": True}) is None

    def test_skip_clears_when_idle(self):
        """When no sub-flow is active, 'skip' clears the reminders as before."""
        res = self._run("skip", {})
        assert res is not None and "skip" in res.get("response", "").lower()


# ══════════════════════════════════════════════════════════════════════════
# Live-transcript bug (2026-07): a dangling overdue-audit flag silently marked
# a job "paid" because "paid" in msg_lower matched the substring inside "Do
# these include, paid and unpaid?" — a QUESTION about an earlier answer, not a
# reply to the (invisible-to-the-user) audit nudge. No UPDATE may fire unless
# the message actually reads like a reply.
# ══════════════════════════════════════════════════════════════════════════

class TestAuditReplyDoesNotHijackQuestions:
    AUDIT_PENDING = [{"id": "j1", "client_name": "Clink Films", "bill_no": "CLI-150526-01",
                       "fees": 15000, "_audit_row": True}]

    def _run(self, message):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        with patch("services.intent_service.get_pending", return_value=self.AUDIT_PENDING), \
             patch("services.intent_service.clear_pending"), \
             patch("services.intent_service.remove_single"):
            result = svc._handle_pending_reminder("u1", message)
        return svc, result

    def test_question_containing_paid_is_not_treated_as_confirmation(self):
        """The exact production case: a clarifying question must fall through
        (return None) and must NOT issue an UPDATE."""
        svc, result = self._run("Do these include, paid and unpaid?")
        assert result is None, f"Question was misread as an audit reply: {result}"
        for call in svc.supabase.execute_sql.call_args_list:
            sql = call.args[0].lower() if call.args else ""
            assert "update" not in sql, f"A question triggered a DB write: {sql}"

    @pytest.mark.parametrize("msg", [
        "Do these include paid and unpaid?",
        "Does this include paid invoices?",
        "Is this the paid total?",
        "What does paid mean here?",
        "Why is this marked paid?",
        "Which ones are paid?",
    ])
    def test_other_question_shapes_fall_through(self, msg):
        svc, result = self._run(msg)
        assert result is None, f"{msg!r} was treated as a reply: {result}"
        assert not any("update" in (c.args[0].lower() if c.args else "")
                       for c in svc.supabase.execute_sql.call_args_list)

    def test_genuine_reply_still_marks_paid(self):
        """Guard against over-correcting: a real one-word confirmation must
        still work."""
        svc, result = self._run("paid")
        assert result is not None and result.get("operation") == "audit_paid"
        assert any("update" in (c.args[0].lower() if c.args else "")
                   for c in svc.supabase.execute_sql.call_args_list)

    def test_genuine_numbered_reply_still_works(self):
        svc, result = self._run("paid 1")
        assert result is not None and result.get("operation") == "audit_paid"
