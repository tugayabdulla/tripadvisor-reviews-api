import threading
from contextlib import contextmanager
from psycopg2 import OperationalError, InterfaceError
from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import RealDictCursor
from .config import settings

_pool: ThreadedConnectionPool | None = None
_reset_lock = threading.Lock()


def open_pool():
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            settings.pool_min, settings.pool_max,
            host=settings.db_host, port=settings.db_port, dbname=settings.db_name,
            user=settings.db_user, password=settings.db_pass,
        )


def close_pool():
    global _pool
    if _pool is not None:
        _pool.closeall()
        _pool = None


def _reset_pool():
    """Rebuild the whole pool. After Postgres restarts (host reboot, upgrade, OOM)
    every pooled connection is dead; throwing them all away and reconnecting is the
    only clean recovery."""
    with _reset_lock:
        close_pool()
        open_pool()


@contextmanager
def cursor():
    if _pool is None:
        raise RuntimeError("pool not opened")
    conn = _pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            yield cur
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        _pool.putconn(conn)


def _run(sql: str, params: tuple, fetch):
    """Execute with one transparent retry: if a pooled connection is stale (e.g.
    Postgres was restarted out from under us), discard it, rebuild the pool, and try
    once more — so the API self-heals instead of serving 'db:down' until a manual
    restart."""
    last_err: Exception | None = None
    for attempt in (1, 2):
        if _pool is None:
            open_pool()
        conn = _pool.getconn()
        try:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(sql, params)
                result = fetch(cur)
            conn.commit()
            _pool.putconn(conn)
            return result
        except (OperationalError, InterfaceError) as e:
            last_err = e
            _pool.putconn(conn, close=True)  # never return a dead connection to the pool
            if attempt == 1:
                _reset_pool()
                continue
            raise
        except Exception:
            try:
                conn.rollback()
            finally:
                _pool.putconn(conn)
            raise
    raise last_err  # unreachable, but keeps type checkers happy


def fetch_all(sql: str, params: tuple = ()) -> list[dict]:
    return _run(sql, params, lambda cur: [dict(r) for r in cur.fetchall()])


def fetch_one(sql: str, params: tuple = ()) -> dict | None:
    def _one(cur):
        r = cur.fetchone()
        return dict(r) if r else None
    return _run(sql, params, _one)
