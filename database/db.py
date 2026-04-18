"""
DB connection pool — psycopg2 기반.
"""
from contextlib import contextmanager
from typing import Generator

import psycopg2
import psycopg2.extras
from psycopg2.pool import ThreadedConnectionPool

from config.settings import settings

_pool: ThreadedConnectionPool | None = None


def get_pool() -> ThreadedConnectionPool:
    global _pool
    if _pool is None:
        _pool = ThreadedConnectionPool(
            minconn=2,
            maxconn=20,
            dsn=settings.db_dsn,
            cursor_factory=psycopg2.extras.RealDictCursor,
            options="-c search_path=public,stage,core,service",
        )
    return _pool


@contextmanager
def get_conn() -> Generator[psycopg2.extensions.connection, None, None]:
    pool = get_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        pool.putconn(conn)


@contextmanager
def get_cursor(conn=None):
    if conn is not None:
        yield conn.cursor()
    else:
        with get_conn() as c:
            yield c.cursor()
