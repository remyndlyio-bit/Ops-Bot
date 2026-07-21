"""
Category 3 — Record Correction & Mutation.

Covers `_handle_modify_intent` (the B -> A "change a field" flow) end to end:
field resolution, row targeting (pinned context / client-filter lookup /
disambiguation), value normalisation, the actual UPDATE, and the guardrails
around all of it (whitelist, SQL escaping, no-write-on-no-parse).

This is the flow that sits next to the audit-reply mark-paid bug (fixed in
_handle_pending_audit_reply) but is a DIFFERENT code path — general field
edits ("change the fee to 30k", "mark this as paid" outside the reminder
flow) go through here instead. Before this file it had zero test coverage.

Every UPDATE assertion checks the actual SQL string passed to
supabase.execute_sql — not just the response text — because the failure mode
we care about is a wrong or unintended DATABASE WRITE, not a wrong reply.
"""
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest
from unittest.mock import MagicMock, patch


def _make_svc():
    """IntentService with all external I/O mocked (mirrors tests/test_edge_cases.py)."""
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
    return svc


def _updates(svc):
    """All UPDATE SQL strings actually sent to the DB."""
    out = []
    for call in svc.supabase.execute_sql.call_args_list:
        sql = call.args[0] if call.args else ""
        if sql.strip().upper().startswith("UPDATE"):
            out.append(sql)
    return out


PINNED_ROW = {"id": "row-1", "client_name": "Nike", "brand_name": "Star Studios", "fees": 25000}


class TestFieldUpdateHappyPath:
    def test_change_fee_on_pinned_row(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": PINNED_ROW}}
        svc.gemini.extract_modify_intent.return_value = {"field": "fee", "value": "30000"}
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"fees": 25000, "notes": ""}]},   # pre-fetch old value
            {"ok": True, "rows": [{**PINNED_ROW, "fees": 30000}]},  # UPDATE ... RETURNING *
        ]
        result = svc._handle_modify_intent("u1", "change the fee to 30k", {})
        assert result["operation"] == "modify_success"
        upd = _updates(svc)
        assert len(upd) == 1
        assert "30000" in upd[0] and "'row-1'" in upd[0]
        assert '"fees"' in upd[0]

    def test_mark_paid_via_modify_flow(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": PINNED_ROW}}
        svc.gemini.extract_modify_intent.return_value = {"field": "paid", "value": "yes"}
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"paid": None, "notes": ""}]},
            {"ok": True, "rows": [{**PINNED_ROW, "paid": "Yes"}]},
        ]
        result = svc._handle_modify_intent("u1", "mark this as paid", {})
        assert result["operation"] == "modify_success"
        upd = _updates(svc)
        assert "'Yes'" in upd[0] and '"paid"' in upd[0]

    def test_mark_unpaid_normalises_to_no(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": PINNED_ROW}}
        svc.gemini.extract_modify_intent.return_value = {"field": "status", "value": "unpaid"}
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"paid": "Yes", "notes": ""}]},
            {"ok": True, "rows": [{**PINNED_ROW, "paid": "No"}]},
        ]
        result = svc._handle_modify_intent("u1", "mark this unpaid", {})
        assert result["operation"] == "modify_success"
        upd = _updates(svc)
        # "unpaid" isn't the literal yes/true/1/paid/y set, so it normalises to 'No'.
        assert "'No'" in upd[0]

    def test_notes_history_appended_not_overwritten(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": PINNED_ROW}}
        svc.gemini.extract_modify_intent.return_value = {"field": "fee", "value": "40000"}
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"fees": 25000, "notes": "[01 Jan 2026] fees: 20000 -> 25000"}]},
            {"ok": True, "rows": [{**PINNED_ROW, "fees": 40000}]},
        ]
        svc._handle_modify_intent("u1", "change fee to 40000", {})
        upd = _updates(svc)[0]
        assert "20000" in upd and "40000" in upd, "prior history line must survive, new line appended"


class TestNoFieldParsed:
    def test_no_field_no_context_falls_through(self):
        """Ambiguous message + no pinned row: yield to the normal pipeline, don't guess."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        svc.gemini.extract_modify_intent.return_value = {}
        result = svc._handle_modify_intent("u1", "update it", {})
        assert result is None
        assert _updates(svc) == []

    def test_no_field_with_pinned_row_prompts(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": PINNED_ROW}}
        svc.gemini.extract_modify_intent.return_value = {}
        result = svc._handle_modify_intent("u1", "update it", {})
        assert result["operation"] == "modify_prompt"
        assert "nike" in result["response"].lower() or "this job" in result["response"].lower()
        assert _updates(svc) == [], "must not write until a field/value is actually known"

    def test_a_question_never_extracts_into_a_write(self):
        """Guard for the class of bug fixed in _handle_pending_audit_reply, one
        layer over: if the LLM extractor correctly returns nothing for a
        question ('do these include paid and unpaid?'), no UPDATE fires."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": PINNED_ROW}}
        svc.gemini.extract_modify_intent.return_value = None
        result = svc._handle_modify_intent("u1", "Do these include, paid and unpaid?", {})
        assert _updates(svc) == []
        assert result is None or result.get("operation") != "modify_success"


class TestRowTargeting:
    def test_client_filter_no_match(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        svc.gemini.extract_modify_intent.return_value = {
            "field": "fee", "value": "30000", "client_filter": "Ghostcorp",
        }
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": []}
        result = svc._handle_modify_intent("u1", "change Ghostcorp's fee to 30k", {})
        assert result["operation"] == "modify_no_match"
        assert _updates(svc) == []

    def test_client_filter_multiple_matches_disambiguates(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        mem = svc.memory
        svc.gemini.extract_modify_intent.return_value = {
            "field": "fee", "value": "30000", "client_filter": "Nike",
        }
        svc.supabase.execute_sql.return_value = {
            "ok": True,
            "rows": [
                {"id": "a", "client_name": "Nike", "bill_no": "INV-1", "fees": 10000},
                {"id": "b", "client_name": "Nike", "bill_no": "INV-2", "fees": 20000},
            ],
        }
        result = svc._handle_modify_intent("u1", "change Nike's fee to 30k", {})
        assert result["operation"] == "modify_disambiguate"
        assert _updates(svc) == [], "must not write to any row until the user picks one"
        stored = [c.args[1] for c in mem.update_user_memory.call_args_list
                  if "pending_disambiguation" in c.args[1]]
        assert stored and stored[-1]["pending_disambiguation"]["type"] == "modify"
        assert stored[-1]["pending_disambiguation"]["field"] == "fees"

    def test_client_filter_single_match_updates_it(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        svc.gemini.extract_modify_intent.return_value = {
            "field": "fee", "value": "30000", "client_filter": "Nike",
        }
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"id": "solo", "client_name": "Nike", "bill_no": "INV-1", "fees": 10000}]},
            {"ok": True, "rows": [{"fees": 10000, "notes": ""}]},
            {"ok": True, "rows": [{"id": "solo", "fees": 30000}]},
        ]
        result = svc._handle_modify_intent("u1", "change Nike's fee to 30k", {})
        assert result["operation"] == "modify_success"
        upd = _updates(svc)
        assert len(upd) == 1 and "'solo'" in upd[0]


class TestValueValidation:
    def test_unparseable_fee_rejected_no_write(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": PINNED_ROW}}
        svc.gemini.extract_modify_intent.return_value = {"field": "fee", "value": "banana"}
        result = svc._handle_modify_intent("u1", "change the fee to banana", {})
        assert result["operation"] == "modify_bad_value"
        assert _updates(svc) == []

    def test_field_outside_whitelist_ignored(self):
        """A field the LLM might hallucinate (not in _MODIFY_ALLOWED_FIELDS) must
        normalise to None, not silently pass through to raw SQL."""
        svc = _make_svc()
        assert svc._normalize_modify_field("user_id") is None
        assert svc._normalize_modify_field("isDeleted") is None
        assert svc._normalize_modify_field("id") is None

    @pytest.mark.parametrize("alias,expected", [
        ("fee", "fees"), ("amount", "fees"), ("cost", "fees"),
        ("payment_status", "paid"), ("status", "paid"),
        ("email", "poc_email"), ("contact_email", "poc_email"),
        ("poc", "poc_name"), ("contact", "poc_name"),
        ("client", "client_name"), ("brand", "brand_name"),
        ("date", "job_date"),
    ])
    def test_field_aliases_normalise_correctly(self, alias, expected):
        svc = _make_svc()
        assert svc._normalize_modify_field(alias) == expected


class TestSqlSafety:
    def test_client_filter_quote_is_escaped(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {}
        svc.gemini.extract_modify_intent.return_value = {
            "field": "fee", "value": "30000", "client_filter": "O'Brien",
        }
        svc.supabase.execute_sql.return_value = {"ok": True, "rows": []}
        svc._handle_modify_intent("u1", "change O'Brien's fee to 30k", {})
        lookup_sql = svc.supabase.execute_sql.call_args_list[0].args[0]
        assert "o''brien" in lookup_sql.lower()

    def test_value_quote_is_escaped_in_update(self):
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": PINNED_ROW}}
        svc.gemini.extract_modify_intent.return_value = {"field": "poc_name", "value": "O'Brien"}
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"poc_name": "Old", "notes": ""}]},
            {"ok": True, "rows": [{**PINNED_ROW, "poc_name": "O'Brien"}]},
        ]
        svc._handle_modify_intent("u1", "change poc to O'Brien", {})
        upd = _updates(svc)[0]
        assert "o''brien" in upd.lower()
        assert "o'brien" not in upd.lower().replace("o''brien", "")


class TestModifyDisambiguationReply:
    """A modify-triggered disambiguation ('change Nike's fee to 30k' matched 2
    rows) stores {type:'modify', field, value, rows} — NOT the generic
    {sql, updates} shape a delete-disambiguation stores. Before this fix,
    picking a number here ran execute_sql('') (a no-op: pending.get('sql','')
    is empty, so the WHERE-clause regex substitution has nothing to replace)
    and then told the user 'Done — updated', with NO actual write. A silent,
    confidently wrong success message — worse than a crash."""

    PENDING = {
        "type": "modify", "field": "fees", "value": 30000,
        "rows": [
            {"id": "a", "client_name": "Nike", "bill_no": "INV-1", "fees": 10000},
            {"id": "b", "client_name": "Nike", "bill_no": "INV-2", "fees": 20000},
        ],
    }

    def test_numbered_pick_actually_writes_the_field(self):
        svc = _make_svc()
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"fees": 20000, "notes": ""}]},
            {"ok": True, "rows": [{"id": "b", "fees": 30000}]},
        ]
        result = svc._handle_disambiguation_reply("u1", "2", self.PENDING)
        assert result["operation"] == "modify_success"
        upd = _updates(svc)
        assert len(upd) == 1, "must issue exactly one real UPDATE, not a no-op"
        assert "'b'" in upd[0] and "30000" in upd[0] and '"fees"' in upd[0]

    def test_never_calls_execute_sql_with_empty_string(self):
        """The exact bug: pending.get('sql', '') defaulting to '' and being
        executed as-is."""
        svc = _make_svc()
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"fees": 10000, "notes": ""}]},
            {"ok": True, "rows": [{"id": "a", "fees": 30000}]},
        ]
        svc._handle_disambiguation_reply("u1", "1", self.PENDING)
        for call in svc.supabase.execute_sql.call_args_list:
            assert call.args[0].strip() != "", "executed empty SQL — the modify never actually applied"

    def test_pending_disambiguation_cleared_after_pick(self):
        svc = _make_svc()
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"fees": 10000, "notes": ""}]},
            {"ok": True, "rows": [{"id": "a", "fees": 30000}]},
        ]
        svc._handle_disambiguation_reply("u1", "1", self.PENDING)
        cleared = [c.args[1] for c in svc.memory.update_user_memory.call_args_list
                   if "pending_disambiguation" in c.args[1]]
        assert cleared and cleared[0]["pending_disambiguation"] is None


class TestContextRefreshAfterUpdate:
    def test_successful_update_refreshes_uscf_context(self):
        """A field read right after an update must see the NEW value, not the
        stale pre-update row."""
        svc = _make_svc()
        svc.memory.get_user_memory.return_value = {"uscf_context": {"last_row_data": PINNED_ROW}}
        svc.gemini.extract_modify_intent.return_value = {"field": "fee", "value": "99999"}
        svc.supabase.execute_sql.side_effect = [
            {"ok": True, "rows": [{"fees": 25000, "notes": ""}]},
            {"ok": True, "rows": [{**PINNED_ROW, "fees": 99999}]},
        ]
        svc._handle_modify_intent("u1", "change fee to 99999", {})
        ctx_updates = [c.args[1].get("uscf_context") for c in svc.memory.update_user_memory.call_args_list
                       if "uscf_context" in c.args[1]]
        assert ctx_updates, "context must be refreshed after a successful update"
        assert ctx_updates[-1]["last_row_data"]["fees"] == 99999
