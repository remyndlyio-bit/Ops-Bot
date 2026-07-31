"""
tests/test_awaiting_state.py
=============================

Regression coverage for the "stuck flow swallows everything" bug found in a
134-scenario live test run: arming one awaiting_* conversational state (e.g.
"update bank details") used to be a single-key memory write that never
cleared any OTHER awaiting_* flag still active from an earlier, unfinished
flow (e.g. awaiting_invoice_poc_email left over from a stalled invoice
generation). Both flags ended up True at once. _process_request_impl checks
these flags in a fixed order and returns on the first match, so whichever
flag happened to be earlier in that order silently won -- for 15+
consecutive turns in the observed run, bank-details input, an account-link
reply, and a plain reminders command all got answered as if they were
replies to a stuck invoice-email prompt.

_arm_awaiting() is the fix: it clears every known awaiting_* flag before
setting the one being armed, so at most one is ever True. These tests pin
that contract down directly, independent of any single call site.
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


class TestArmAwaitingMutualExclusivity:
    def test_arming_a_flag_clears_all_others(self):
        svc = _make_svc()
        svc._arm_awaiting("u1", "awaiting_bank_details")
        patch = svc.memory.update_user_memory.call_args.args[1]
        for flag in svc._AWAITING_FLAGS:
            if flag == "awaiting_bank_details":
                assert patch[flag] is True
            else:
                assert patch[flag] is False, f"{flag} should be explicitly cleared"

    def test_extra_context_keys_are_preserved(self):
        svc = _make_svc()
        svc._arm_awaiting("u1", "awaiting_invoice_month", {
            "pending_invoice_client": "Nike", "pending_invoice_send_email": True,
        })
        patch = svc.memory.update_user_memory.call_args.args[1]
        assert patch["awaiting_invoice_month"] is True
        assert patch["pending_invoice_client"] == "Nike"
        assert patch["pending_invoice_send_email"] is True

    def test_single_write_call(self):
        """One memory write per arm -- not a read-then-write race that could
        interleave with a concurrent turn."""
        svc = _make_svc()
        svc._arm_awaiting("u1", "awaiting_link_id")
        assert svc.memory.update_user_memory.call_count == 1

    def test_no_call_sites_left_and_awaiting_flags_is_empty(self):
        """_arm_awaiting has zero callers left -- awaiting_modify_field (the
        last boolean legacy flag) was migrated to its own FlowMachine flow.
        Guards against a NEW awaiting_* flag being introduced without also
        being added to _AWAITING_FLAGS (the original bug class this test
        pinned down) -- if a call site reappears, this catches it, and
        _AWAITING_FLAGS itself should be non-empty again at that point."""
        import re
        src = open(os.path.join(os.path.dirname(__file__), "..",
                                 "services", "intent_service.py")).read()
        armed = set(re.findall(r'_arm_awaiting\(\s*[^,]+,\s*"(awaiting_[a-z_]+)"', src))
        assert armed == set(), "expected zero remaining _arm_awaiting call sites"
        assert _make_svc()._AWAITING_FLAGS == ()


class TestStuckFlowNoLongerSwallowsUnrelatedInput:
    """End-to-end shape of the originally reported bug: arming bank-details
    while an unrelated flow's state is still active must let the NEXT
    message reach the bank-details handler, not the stale one.

    Every boolean legacy flag has been migrated to its own FlowMachine flow
    now (awaiting_modify_field was the last one), so there's no longer a
    boolean flag left to demonstrate the ORIGINAL bug shape against --
    FlowMachine's own set_state() unconditionally overwrites whatever flow
    was previously active, so the mutual-exclusivity guarantee this test
    used to pin down at the legacy-flag layer now holds structurally,
    by construction, at the FlowMachine layer instead."""

    def test_arming_bank_details_sets_flow_machine_state(self):
        svc = _make_svc()
        svc.flow_machine = MagicMock()
        svc.memory.get_user_memory.return_value = {}
        svc._prompt_bank_details_format("u1", "update my bank details")

        from services.flow_machine import FLOW_BANK_DETAILS
        svc.flow_machine.set_state.assert_called_once_with("u1", FLOW_BANK_DETAILS, {})


class TestPendingDisambiguationMutualExclusivity:
    """pending_disambiguation is a SEPARATE state mechanism from the 14
    awaiting_* flags (a numbered "which one did you mean?" list, not a
    boolean) and wasn't covered by the original _arm_awaiting fix. A live
    scenario-suite run found a stale disambiguation list from an earlier
    turn still active, and it took precedence (checked before any
    awaiting_* flag) over a freshly-armed awaiting_link_id, swallowing the
    account-linking reply as if it were a disambiguation pick."""

    def test_arm_awaiting_clears_stale_disambiguation(self):
        svc = _make_svc()
        svc._arm_awaiting("u1", "awaiting_link_id")
        patch = svc.memory.update_user_memory.call_args.args[1]
        assert patch["pending_disambiguation"] is None

    def test_arm_disambiguation_clears_all_awaiting_flags(self):
        svc = _make_svc()
        svc._arm_disambiguation("u1", {"rows": [{"id": 1}], "sql": "UPDATE ..."})
        patch = svc.memory.update_user_memory.call_args.args[1]
        for flag in svc._AWAITING_FLAGS:
            assert patch[flag] is False, f"{flag} should be cleared when arming disambiguation"
        assert patch["pending_disambiguation"] == {"rows": [{"id": 1}], "sql": "UPDATE ..."}

    def test_end_to_end_stale_disambiguation_no_longer_blocks_link_reply(self):
        """The exact reported shape: a stale disambiguation list (from an
        earlier "mark paid" ambiguity) must not swallow a subsequent
        account-linking ID reply.

        Phase 2.3: LINK_ACCOUNT is FlowMachine-only now (no legacy
        awaiting_link_id flag) — verify via flow_machine.set_state instead,
        pending_disambiguation clearing is unaffected."""
        svc = _make_svc()
        svc.flow_machine = MagicMock()
        svc.memory.get_user_memory.return_value = {
            "pending_disambiguation": {"rows": [{"id": 1}, {"id": 2}], "sql": "UPDATE ..."},
        }
        svc._handle_link_account("u1", "link my telegram account")
        from services.flow_machine import FLOW_LINK_ACCOUNT
        svc.flow_machine.set_state.assert_called_once_with("u1", FLOW_LINK_ACCOUNT, {})
        patch = svc.memory.update_user_memory.call_args.args[1]
        assert patch["pending_disambiguation"] is None

    def test_every_pending_disambiguation_call_site_goes_through_arm_disambiguation(self):
        """Guards against a new disambiguation arm-site being added directly
        via update_user_memory again, silently reopening this gap."""
        import re
        src = open(os.path.join(os.path.dirname(__file__), "..",
                                 "services", "intent_service.py")).read()
        direct_arms = re.findall(r'update_user_memory\([^)]*\{\s*\n?\s*"pending_disambiguation":\s*\{', src)
        assert not direct_arms, "found a pending_disambiguation arm-site bypassing _arm_disambiguation"


def _make_onboarded_svc():
    svc = _make_svc()
    svc.supabase.get_user_profile.return_value = {
        "ok": True, "data": {"onboarded_at": "2024-01-01T00:00:00", "name": "Test User"},
    }
    svc.supabase.db_url = "postgresql://fake"
    svc.gemini.is_history_question.return_value = False
    svc.memory.get_form_state.return_value = None
    # A truthy MagicMock() here would make every "is this a new query"
    # check succeed by default, masking exactly the bug these tests pin
    # down -- default it to False like the real "not a new query" case.
    svc.gemini.is_new_query_not_response.return_value = False
    return svc


class TestIntentShiftGuardCommandShapeGate:
    """Live bugs (#86, #87, #95): the "is this a new query?" AI classifier,
    consulted whenever an awaiting_* single-question state is active, would
    occasionally misclassify a perfectly valid ANSWER as a new command --
    a raw email address, a malformed email, "Account: 12345" for bank
    details -- none of which look like conversational replies to a
    classifier with no shape prior. Once misclassified, the pending state
    was cleared and the reply fell all the way through to the generic SQL
    pipeline: POC-email replies landed in "I couldn't find a matching
    record to update", and bank-details replies triggered a 41-row
    disambiguation list.

    The original fix gated the AI call behind a cheap surface-shape check
    (a "?" or a command verb at the start of the message). That whole
    mechanism (the _PENDING_STATES dict + surface-shape gate +
    is_new_query_not_response escape hatch) is now DELETED --
    awaiting_invoice_month was its last member (every other flag it once
    covered was already migrated to FlowMachine in earlier passes). The
    identical "question-shaped -> don't treat as a reply" distinction is
    made by the v2 classifier's per-flow flow_compatible guidance now
    (services/classifier.py), reached via dispatch_in_flow before legacy
    code is ever consulted."""

    def test_poc_email_reply_never_asks_the_ai_classifier(self):
        svc = _make_onboarded_svc()
        svc.memory.get_user_memory.return_value = {
            "pending_poc_email_client": "Star Studios",
            "pending_poc_email_row_ids": [1],
        }
        svc.supabase.execute_sql.return_value = {"ok": True, "rowcount": 1}
        svc.process_request("u1", "rahul@starstudios.com")
        svc.gemini.is_new_query_not_response.assert_not_called()

    def test_bank_details_reply_never_asks_the_ai_classifier(self):
        svc = _make_onboarded_svc()
        svc.memory.get_user_memory.return_value = {"awaiting_bank_details": True}
        svc.process_request("u1", "Account: 12345")
        svc.gemini.is_new_query_not_response.assert_not_called()

    def test_intent_shift_guard_mechanism_is_gone(self):
        """No boolean legacy flag has a _PENDING_STATES-style entry left at
        all -- confirms the whole guard block (and its is_new_query_not_
        response escape hatch) was deleted, not just emptied."""
        import inspect
        from services.intent_service import IntentService
        src = inspect.getsource(IntentService._process_request_impl)
        assert "_PENDING_STATES = {" not in src
        assert "_COMMAND_LIKE_STARTS" not in src
