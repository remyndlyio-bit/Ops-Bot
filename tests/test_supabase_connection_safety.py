"""
tests/test_supabase_connection_safety.py
=========================================

Regression coverage for a fix-that-made-things-worse:

An earlier change made SupabaseService reuse one persistent, lock-guarded
psycopg2 connection across all calls (to cut the fresh-connect-per-call
TCP+TLS cost). In production this made latency WORSE, not better: Supabase's
pooler URL runs PgBouncer in *transaction mode*, which is designed for
short connect -> query -> disconnect usage, not a long-lived idle client
connection. The pooler/network silently dropped the idle connection; the
next query hung on a dead socket with no timeout configured; and because
that one connection was lock-guarded, EVERY other concurrent user's
request queued up behind that single hung call (Railway logs: a "Hi"
small-talk turn took 40s, vs ~11-17s before -- worse, and now affecting
every concurrent user instead of just the slow one).

Reverted to connect-per-call (matches this file's own header comment on
why the transaction-mode pooler URL must be used), hardened with
connect_timeout (bounds a hung TCP/TLS handshake) and statement_timeout
(bounds a hung/slow query on an otherwise-established connection) so a
bad connection fails fast instead of hanging -- without bringing back a
shared connection that can serialize unrelated users behind one hang.

These tests exist so nobody "optimizes" this back into a persistent
connection without noticing why it was reverted.
"""
import os
import sys
import types

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class _FakeCursor:
    def __init__(self, conn):
        self._conn = conn

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self._conn.executed.append(sql)

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    @property
    def rowcount(self):
        return 0


class _FakeConn:
    def __init__(self):
        self.closed = 0
        self.autocommit = False
        self.executed = []

    def cursor(self, cursor_factory=None):
        return _FakeCursor(self)

    def close(self):
        self.closed = 1


class _FakePsycopg2Module(types.ModuleType):
    def __init__(self):
        super().__init__("psycopg2")
        self.connect_calls = []
        self.OperationalError = type("OperationalError", (Exception,), {})

        class extras:
            RealDictCursor = object()

        self.extras = extras

    def connect(self, db_url, **kwargs):
        self.connect_calls.append(kwargs)
        return _FakeConn()


@pytest.fixture
def fake_psycopg2(monkeypatch):
    fake = _FakePsycopg2Module()
    monkeypatch.setitem(sys.modules, "psycopg2", fake)
    monkeypatch.setitem(sys.modules, "psycopg2.extras", fake.extras)
    return fake


def _make_service():
    from services.supabase_service import SupabaseService
    svc = SupabaseService()
    svc.db_url = "postgres://fake"
    return svc


class TestNoPersistentConnection:
    """SupabaseService must NOT keep any connection-caching instance state --
    each call opens and (on the happy path) closes its own connection,
    matching transaction-mode PgBouncer's expected usage pattern."""

    def test_no_reused_connection_attribute(self):
        svc = _make_service()
        assert not hasattr(svc, "_conn")

    def test_no_lock_serializing_calls(self):
        svc = _make_service()
        assert not hasattr(svc, "_db_lock")

    def test_each_call_opens_its_own_connection(self, fake_psycopg2, monkeypatch):
        monkeypatch.setattr("services.supabase_service.JOB_ENTRIES_COLUMNS",
                             ["id", "created_at", "user_id"])
        svc = _make_service()
        svc.get_user_profile("u1")
        svc.get_user_profile("u1")
        svc.get_user_profile("u1")
        assert len(fake_psycopg2.connect_calls) == 3


class TestConnectionTimeouts:
    """Every connect() call must bound both connection setup and query
    execution time, so a bad network path or dead-but-not-yet-detected
    connection fails fast instead of hanging (and, pre-revert, hanging
    everyone behind a shared lock)."""

    def test_connect_timeout_set(self, fake_psycopg2):
        svc = _make_service()
        svc.get_user_profile("u1")
        assert fake_psycopg2.connect_calls
        assert fake_psycopg2.connect_calls[0].get("connect_timeout") == 5

    def test_statement_timeout_set(self, fake_psycopg2):
        svc = _make_service()
        svc.get_user_profile("u1")
        opts = fake_psycopg2.connect_calls[0].get("options") or ""
        assert "statement_timeout" in opts
