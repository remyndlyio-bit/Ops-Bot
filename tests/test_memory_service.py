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
