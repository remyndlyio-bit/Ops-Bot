"""
Tests for the DB-backed MemoryService.

The point of the DB backing is that per-user state (awaiting_* flags,
conversation, form state) survives a redeploy and is shared across multiple app
instances. These tests use a fake psycopg2 connection over a shared in-memory
store to simulate two app instances talking to the same database.
"""
import os
import sys
import json
import importlib

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import pytest


# ── Fake psycopg2 over a shared dict ────────────────────────────────────────
class _FakeCursor:
    def __init__(self, store):
        self.store = store
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        s = " ".join(sql.strip().split()).upper()
        if s.startswith("CREATE TABLE"):
            return
        if s.startswith("SELECT"):
            uid = params[0]
            self._result = (self.store[uid],) if uid in self.store else None
        elif s.startswith("INSERT"):
            uid, payload = params[0], params[1]
            self.store[uid] = json.loads(payload)

    def fetchone(self):
        return self._result


class _FakeConn:
    def __init__(self, store):
        self.store = store
        self.closed = 0
        self.autocommit = False

    def cursor(self):
        return _FakeCursor(self.store)

    def close(self):
        self.closed = 1


@pytest.fixture
def db_backed(monkeypatch):
    """A shared store + a factory that builds MemoryService instances bound to it."""
    store = {}
    monkeypatch.setenv("SUPABASE_DB_URL", "postgresql://fake/db")

    import psycopg2
    monkeypatch.setattr(psycopg2, "connect", lambda *a, **k: _FakeConn(store))

    import utils.memory_service as m
    importlib.reload(m)

    def make():
        return m.MemoryService(file_path="/tmp/should_not_be_used.json")

    yield store, make
    importlib.reload(m)  # restore for other tests


class TestDbBackedMemory:
    def test_uses_db(self, db_backed):
        store, make = db_backed
        svc = make()
        assert svc._db_ok is True

    def test_update_then_get_roundtrips_through_db(self, db_backed):
        store, make = db_backed
        svc = make()
        svc.update_user_memory("u1", {"awaiting_invoice_address": True, "pending_invoice": {"client_name": "X"}})
        assert "u1" in store and store["u1"]["awaiting_invoice_address"] is True
        assert svc.get_user_memory("u1")["awaiting_invoice_address"] is True

    def test_state_survives_a_new_instance_redeploy(self, db_backed):
        """The bug: an awaiting_* flag set before a redeploy vanished. With the DB,
        a fresh instance (new container) still sees it."""
        store, make = db_backed
        inst_a = make()
        inst_a.update_user_memory("wa1", {"awaiting_invoice_address": True})

        inst_b = make()  # simulates a redeployed / second container
        assert inst_b.get_user_memory("wa1").get("awaiting_invoice_address") is True, \
            "awaiting state must survive across instances via the shared DB"

    def test_conversation_and_form_via_db(self, db_backed):
        store, make = db_backed
        svc = make()
        svc.add_message("u2", "user", "hello")
        svc.add_message("u2", "assistant", "hi there")
        assert len(svc.get_conversation_history("u2")) == 2

        svc.start_form("u2", [], {"form_type": "smart_capture_confirm", "values": {"a": 1}})
        # A second instance sees the active form.
        assert make().get_form_state("u2") is not None
        make().cancel_form("u2")
        assert make().get_form_state("u2") is None

    def test_update_is_shallow_merge_not_overwrite(self, db_backed):
        store, make = db_backed
        svc = make()
        svc.update_user_memory("u3", {"name": "Darshit", "awaiting_bank_details": True})
        svc.update_user_memory("u3", {"awaiting_bank_details": False})
        mem = svc.get_user_memory("u3")
        assert mem["name"] == "Darshit"               # untouched key preserved
        assert mem["awaiting_bank_details"] is False  # updated key changed


class TestStartFormClearsCompetingPendingState:
    """P0-2 (PLAN_OF_ACTION.md): a form and a disambiguation list / invoice-
    send confirmation are mutually exclusive "what is the bot waiting for"
    states. Before this, starting a form left a stale pending_disambiguation
    or pending_send_invoice sitting in memory — a leftover disambiguation
    list from an earlier, unrelated turn could then out-rank the form the
    user was just shown, swallowing their reply. start_form is the single
    choke point ~10 call sites go through, so fixing it here covers all of
    them at once (the mirror-image fix lives in
    IntentService._arm_disambiguation)."""

    def test_start_form_clears_pending_disambiguation(self, db_backed):
        store, make = db_backed
        svc = make()
        svc.update_user_memory("u1", {"pending_disambiguation": {"rows": [{"id": "a"}]}})
        svc.start_form("u1", ["field_a"])
        assert svc.get_user_memory("u1")["pending_disambiguation"] is None

    def test_start_form_clears_pending_send_invoice(self, db_backed):
        store, make = db_backed
        svc = make()
        svc.update_user_memory("u1", {"pending_send_invoice": {"client_name": "Nike"}})
        svc.start_form("u1", ["field_a"])
        assert svc.get_user_memory("u1")["pending_send_invoice"] is None

    def test_start_form_with_override_also_clears_competing_state(self, db_backed):
        store, make = db_backed
        svc = make()
        svc.update_user_memory("u1", {
            "pending_disambiguation": {"rows": [{"id": "a"}]},
            "pending_send_invoice": {"client_name": "Nike"},
        })
        svc.start_form("u1", [], form_override={"form_type": "smart_capture_confirm", "values": {}})
        mem = svc.get_user_memory("u1")
        assert mem["pending_disambiguation"] is None
        assert mem["pending_send_invoice"] is None
        assert mem["form"]["active"] is True  # the form itself still gets set


class TestPerTurnMemoryCache:
    """P1-4 (PLAN_OF_ACTION.md): one DB read per (user, turn) instead of one
    per call site. _db_get/_db_set are the real DB-hitting methods (still
    called exactly the same way whether the cache is involved or not) — spy
    on them directly rather than modifying the shared db_backed fixture."""

    def _spy(self, svc, monkeypatch):
        calls = {"get": 0, "set": 0}
        real_get = svc._db_get
        real_set = svc._db_set

        def counting_get(uid):
            calls["get"] += 1
            return real_get(uid)

        def counting_set(uid, payload):
            calls["set"] += 1
            return real_set(uid, payload)

        monkeypatch.setattr(svc, "_db_get", counting_get)
        monkeypatch.setattr(svc, "_db_set", counting_set)
        return calls

    def test_second_get_in_the_same_turn_is_a_cache_hit(self, db_backed, monkeypatch):
        store, make = db_backed
        svc = make()
        # Seed the backing store directly (bypassing svc) so the cache
        # starts genuinely cold — going through svc.update_user_memory
        # first would already prime it via _write_raw, making every
        # get_user_memory below a hit regardless of whether reads cache.
        store["u1"] = {"name": "Darshit"}
        calls = self._spy(svc, monkeypatch)

        svc.get_user_memory("u1")
        svc.get_user_memory("u1")
        svc.get_user_memory("u1")

        assert calls["get"] == 1, "only the FIRST get_user_memory should hit the DB"

    def test_reset_turn_cache_forces_a_fresh_read(self, db_backed, monkeypatch):
        store, make = db_backed
        svc = make()
        store["u1"] = {"name": "Darshit"}
        calls = self._spy(svc, monkeypatch)

        svc.get_user_memory("u1")
        svc.reset_turn_cache()
        svc.get_user_memory("u1")

        assert calls["get"] == 2

    def test_write_immediately_primes_the_cache_no_read_needed(self, db_backed, monkeypatch):
        """update_user_memory's own _write_raw populates the cache from its
        own known-fresh result — the get_user_memory right after a write
        doesn't even need the one read the cold-cache case above does."""
        store, make = db_backed
        svc = make()
        svc.update_user_memory("u1", {"name": "Darshit"})
        calls = self._spy(svc, monkeypatch)

        svc.get_user_memory("u1")

        assert calls["get"] == 0

    def test_switching_users_never_returns_the_wrong_users_data(self, db_backed):
        """The cache holds ONE (user_id, payload) slot per thread — correct
        for a real turn, which only ever touches one user_id throughout,
        but worth pinning down explicitly: switching users mid-sequence
        must never return stale data from whichever user was cached
        before, even though it costs that user a fresh read next time."""
        store, make = db_backed
        svc = make()
        svc.update_user_memory("u1", {"name": "A"})
        svc.update_user_memory("u2", {"name": "B"})

        assert svc.get_user_memory("u1")["name"] == "A"
        assert svc.get_user_memory("u2")["name"] == "B"
        assert svc.get_user_memory("u1")["name"] == "A"

    def test_a_write_via_start_form_is_immediately_visible_to_a_later_get(self, db_backed):
        """The exact bug this design has to avoid: start_form (and
        set_form_value / advance_form_step / complete_form / cancel_form /
        add_message) write through _write_raw directly, NOT through
        update_user_memory — an earlier version of this cache only
        refreshed on update_user_memory and went stale the instant any of
        those other writers ran."""
        store, make = db_backed
        svc = make()
        svc.get_user_memory("u1")  # populate the cache with the pre-form state
        svc.start_form("u1", ["field_a"])
        mem = svc.get_user_memory("u1")
        assert mem["form"]["active"] is True

    def test_add_message_write_is_immediately_visible_to_a_later_get(self, db_backed):
        store, make = db_backed
        svc = make()
        svc.get_user_memory("u1")
        svc.add_message("u1", "user", "hello")
        assert len(svc.get_conversation_history("u1")) == 1

    def test_update_user_memory_never_uses_a_stale_cached_read_as_its_base(self, db_backed, monkeypatch):
        """update_user_memory must always read fresh before merging — two
        call sites (main.py's background invoice-send tasks) write from
        outside any turn boundary and never call reset_turn_cache(), so a
        cached pre-write read as the merge base could silently drop a
        concurrent change. Simulated here by mutating the store directly
        (as if a different process/thread had written) between two
        update_user_memory calls that never call reset_turn_cache()."""
        store, make = db_backed
        svc = make()
        svc.update_user_memory("u1", {"name": "Darshit"})
        svc.get_user_memory("u1")  # populate the cache

        # A "concurrent" write that bypasses this svc instance entirely —
        # store is the shared backing dict both instances in db_backed talk to.
        store["u1"]["industry"] = "Video Production"

        svc.update_user_memory("u1", {"company_name": "Remyndly"})
        mem = svc.get_user_memory("u1")
        assert mem["industry"] == "Video Production", (
            "update_user_memory must not have overwritten the concurrent "
            "write with a stale cached value"
        )
        assert mem["company_name"] == "Remyndly"
