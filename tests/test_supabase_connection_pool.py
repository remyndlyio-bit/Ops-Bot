"""
P1-4 (PLAN_OF_ACTION.md): the optional, flag-gated connection pool.

Off by default — tests/test_supabase_connection_safety.py is the permanent
regression guard for that default (each call opens and closes its own
connection, exactly as before this existed). This file covers the OTHER
branch: DB_CONNECTION_POOL=1, a real pool with health-checked reuse.

Deliberately NOT a repeat of the incident test_supabase_connection_safety.py
guards against — that incident was ONE lock-guarded connection with no
pooling and no health check. This is a genuine multi-connection pool
(psycopg2.pool.ThreadedConnectionPool) that validates a connection with a
cheap SELECT 1 before handing it back and discards (never returns to the
pool) anything that fails that check.
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
        if getattr(self._conn, "_dead", False):
            raise OperationalError("server closed the connection unexpectedly")
        self._conn.executed.append(sql)

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    @property
    def rowcount(self):
        return 0


class OperationalError(Exception):
    pass


class _FakeConn:
    """A little state machine: `_dead=True` makes any query on this
    connection raise, simulating PgBouncer having silently dropped it
    while it sat idle in the pool."""

    def __init__(self, conn_id, dead=False):
        self.conn_id = conn_id
        self.closed = 0
        self.autocommit = False
        self.executed = []
        self._dead = dead

    def cursor(self, cursor_factory=None):
        return _FakeCursor(self)

    def close(self):
        self.closed = 1


class _FakePool:
    """Minimal stand-in for psycopg2.pool.ThreadedConnectionPool: hands out
    connections from a scripted list, records putconn calls (and whether
    they closed the connection or genuinely returned it)."""

    def __init__(self, connections):
        self._available = list(connections)
        self._next_id = len(connections)
        self.putconn_calls = []  # (conn, close) pairs

    def getconn(self):
        if self._available:
            return self._available.pop(0)
        conn = _FakeConn(self._next_id)
        self._next_id += 1
        return conn

    def putconn(self, conn, close=False):
        self.putconn_calls.append((conn, close))
        if close:
            conn.close()


@pytest.fixture
def svc(monkeypatch):
    monkeypatch.setenv("DB_CONNECTION_POOL", "1")
    fake_psycopg2 = types.ModuleType("psycopg2")
    fake_psycopg2.OperationalError = OperationalError
    fake_psycopg2.connect = lambda *a, **k: (_ for _ in ()).throw(
        AssertionError("psycopg2.connect must not be called when pooling is enabled")
    )
    monkeypatch.setitem(sys.modules, "psycopg2", fake_psycopg2)

    import services.supabase_service as mod
    monkeypatch.setattr(mod, "_POOLS", {}, raising=False)

    from services.supabase_service import SupabaseService
    s = SupabaseService()
    s.db_url = "postgres://fake"
    return s


def _install_pool(monkeypatch, pool):
    import services.supabase_service as mod
    monkeypatch.setattr(mod, "_get_pool", lambda db_url: pool)


class TestPoolDisabledByDefault:
    def test_flag_unset_means_disabled(self, monkeypatch):
        monkeypatch.delenv("DB_CONNECTION_POOL", raising=False)
        from services.supabase_service import _pool_enabled
        assert _pool_enabled() is False

    @pytest.mark.parametrize("value", ["1", "true", "True", "yes", "on"])
    def test_recognised_truthy_values_enable_it(self, monkeypatch, value):
        monkeypatch.setenv("DB_CONNECTION_POOL", value)
        from services.supabase_service import _pool_enabled
        assert _pool_enabled() is True

    @pytest.mark.parametrize("value", ["0", "false", "no", "off", ""])
    def test_falsy_values_stay_disabled(self, monkeypatch, value):
        monkeypatch.setenv("DB_CONNECTION_POOL", value)
        from services.supabase_service import _pool_enabled
        assert _pool_enabled() is False


class TestHealthyConnectionReused:
    def test_get_connection_returns_wrapper_on_healthy_conn(self, svc, monkeypatch):
        pool = _FakePool([_FakeConn(1)])
        _install_pool(monkeypatch, pool)
        conn = svc._get_connection()
        from services.supabase_service import _PooledConnWrapper
        assert isinstance(conn, _PooledConnWrapper)

    def test_close_returns_connection_to_pool_not_closing_it(self, svc, monkeypatch):
        real = _FakeConn(1)
        pool = _FakePool([real])
        _install_pool(monkeypatch, pool)
        conn = svc._get_connection()
        conn.close()
        assert pool.putconn_calls == [(real, False)]
        assert real.closed == 0, "a healthy connection must be returned, not closed"

    def test_double_close_is_a_no_op(self, svc, monkeypatch):
        real = _FakeConn(1)
        pool = _FakePool([real])
        _install_pool(monkeypatch, pool)
        conn = svc._get_connection()
        conn.close()
        conn.close()
        assert len(pool.putconn_calls) == 1

    def test_attribute_access_passes_through(self, svc, monkeypatch):
        real = _FakeConn(1)
        pool = _FakePool([real])
        _install_pool(monkeypatch, pool)
        conn = svc._get_connection()
        conn.autocommit = True
        assert real.autocommit is True
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
        assert real.executed[-1] == "SELECT 1"


class TestStaleConnectionDiscardedAndRetried:
    def test_dead_connection_discarded_fresh_one_used(self, svc, monkeypatch):
        dead = _FakeConn(1, dead=True)
        healthy = _FakeConn(2, dead=False)
        pool = _FakePool([dead, healthy])
        _install_pool(monkeypatch, pool)

        conn = svc._get_connection()

        # The dead connection was discarded (closed, not returned healthy).
        assert (dead, True) in pool.putconn_calls
        # The wrapper now holds the SECOND (healthy) connection.
        conn.autocommit = True
        assert healthy.autocommit is True
        assert dead.autocommit is False

    def test_two_dead_connections_in_a_row_raises(self, svc, monkeypatch):
        pool = _FakePool([_FakeConn(1, dead=True), _FakeConn(2, dead=True)])
        _install_pool(monkeypatch, pool)
        with pytest.raises(Exception):
            svc._get_connection()

    def test_every_pooled_connection_gets_the_health_check_query(self, svc, monkeypatch):
        healthy = _FakeConn(1)
        pool = _FakePool([healthy])
        _install_pool(monkeypatch, pool)
        svc._get_connection()
        assert "SELECT 1" in healthy.executed


class TestPutconnFailureFallsBackToDirectClose:
    def test_putconn_exception_closes_connection_instead(self, svc, monkeypatch):
        real = _FakeConn(1)

        class _BrokenPool(_FakePool):
            def putconn(self, conn, close=False):
                raise RuntimeError("pool is shutting down")

        pool = _BrokenPool([real])
        _install_pool(monkeypatch, pool)
        conn = svc._get_connection()
        conn.close()  # must not raise
        assert real.closed == 1


class TestGetPoolConstruction:
    """_get_pool itself — the piece the tests above bypass by monkeypatching
    _get_pool directly. Covers the actual ThreadedConnectionPool
    construction: sizing, and that it's shared (one pool per db_url, not a
    fresh one per call)."""

    def test_pool_sized_per_plan(self, monkeypatch):
        import services.supabase_service as mod
        monkeypatch.setattr(mod, "_POOLS", {})
        calls = []

        class _StubPool:
            def __init__(self, minconn, maxconn, db_url, **kw):
                calls.append((minconn, maxconn, db_url, kw))

        fake_pool_module = types.ModuleType("psycopg2.pool")
        fake_pool_module.ThreadedConnectionPool = _StubPool
        monkeypatch.setitem(sys.modules, "psycopg2.pool", fake_pool_module)

        mod._get_pool("postgres://fake")
        assert len(calls) == 1
        minconn, maxconn, db_url, kw = calls[0]
        assert (minconn, maxconn) == (1, 10)
        assert db_url == "postgres://fake"
        assert kw.get("connect_timeout") == 5
        assert "statement_timeout" in kw.get("options", "")

    def test_pool_reused_across_calls_for_same_db_url(self, monkeypatch):
        import services.supabase_service as mod
        monkeypatch.setattr(mod, "_POOLS", {})
        calls = []

        class _StubPool:
            def __init__(self, minconn, maxconn, db_url, **kw):
                calls.append(db_url)

        fake_pool_module = types.ModuleType("psycopg2.pool")
        fake_pool_module.ThreadedConnectionPool = _StubPool
        monkeypatch.setitem(sys.modules, "psycopg2.pool", fake_pool_module)

        p1 = mod._get_pool("postgres://fake")
        p2 = mod._get_pool("postgres://fake")
        assert p1 is p2
        assert len(calls) == 1, "a second call for the same db_url must not build a new pool"
