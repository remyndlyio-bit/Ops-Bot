"""
tests/test_supabase_connection_reuse.py
========================================

Regression tests for SupabaseService._cursor().

Root cause this guards against: every SupabaseService DB method used to
open a brand-new psycopg2.connect() (a full TCP+TLS handshake to the
Supabase pooler) and close it again on every single call. Railway logs
showed turns doing several DB touches taking 11-17s end to end -- a
"Hi" small-talk turn alone took 11.2s for a single LLM call, which
pointed at connection setup overhead rather than the LLM. Fixed by
reusing one lazily-created connection per SupabaseService instance
(mirrors the pattern utils/memory_service.py already uses), guarded by
a lock for thread safety, with reconnect-on-drop so a stale/dropped
connection (pooler idle timeout, restart) doesn't wedge every future
call until the process restarts.
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
        if self._conn.raise_on_execute:
            exc = self._conn.raise_on_execute
            self._conn.raise_on_execute = None
            raise exc

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    @property
    def rowcount(self):
        return 0


class _FakeConn:
    _id_counter = 0

    def __init__(self):
        _FakeConn._id_counter += 1
        self.id = _FakeConn._id_counter
        self.closed = 0
        self.autocommit = False
        self.executed = []
        self.raise_on_execute = None

    def cursor(self, cursor_factory=None):
        return _FakeCursor(self)

    def close(self):
        self.closed = 1


class _FakePsycopg2Module(types.ModuleType):
    def __init__(self):
        super().__init__("psycopg2")
        self.connections_made = []
        self.OperationalError = type("OperationalError", (Exception,), {})

        class extras:
            RealDictCursor = object()

        self.extras = extras

    def connect(self, db_url):
        conn = _FakeConn()
        self.connections_made.append(conn)
        return conn


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


class TestConnectionReuse:
    def test_reuses_same_connection_across_calls(self, fake_psycopg2):
        """The bug: N calls used to mean N fresh TCP+TLS handshakes."""
        svc = _make_service()
        with svc._cursor() as cur:
            cur.execute("SELECT 1")
        with svc._cursor() as cur:
            cur.execute("SELECT 2")
        with svc._cursor() as cur:
            cur.execute("SELECT 3")
        assert len(fake_psycopg2.connections_made) == 1

    def test_sets_autocommit_once(self, fake_psycopg2):
        svc = _make_service()
        with svc._cursor():
            pass
        assert fake_psycopg2.connections_made[0].autocommit is True

    def test_reconnects_after_local_close(self, fake_psycopg2):
        svc = _make_service()
        with svc._cursor():
            pass
        svc._conn.close()
        with svc._cursor():
            pass
        assert len(fake_psycopg2.connections_made) == 2

    def test_discards_and_reconnects_on_operational_error(self, fake_psycopg2):
        """A silently-dropped server-side connection (pooler idle timeout)
        must not wedge every subsequent call -- the failing connection is
        discarded and the next call transparently reconnects."""
        svc = _make_service()
        with svc._cursor():
            pass
        first_conn = fake_psycopg2.connections_made[0]
        first_conn.raise_on_execute = fake_psycopg2.OperationalError(
            "server closed the connection unexpectedly"
        )

        with pytest.raises(fake_psycopg2.OperationalError):
            with svc._cursor() as cur:
                cur.execute("SELECT 1")

        assert svc._conn is None
        assert first_conn.closed == 1

        with svc._cursor() as cur:
            cur.execute("SELECT 1")
        assert len(fake_psycopg2.connections_made) == 2

    def test_non_operational_error_does_not_discard_connection(self, fake_psycopg2):
        """A normal query error (bad SQL, missing column) shouldn't tear
        down a perfectly healthy connection -- only OperationalError does."""
        svc = _make_service()
        with svc._cursor():
            pass
        conn = svc._conn
        conn.raise_on_execute = ValueError("bad column")

        with pytest.raises(ValueError):
            with svc._cursor() as cur:
                cur.execute("SELECT bogus")

        assert svc._conn is conn
        assert conn.closed == 0
