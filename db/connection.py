import asyncpg
from contextlib import asynccontextmanager
from settings import settings


_pool: asyncpg.Pool | None = None


async def init_pool() -> None:
    global _pool
    _pool = await asyncpg.create_pool(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        min_size=2,
        max_size=10,
        statement_cache_size=0,
    )


async def close_pool() -> None:
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("DB pool is not initialized. Call init_pool() first.")
    return _pool


@asynccontextmanager
async def get_conn():
    pool = get_pool()
    async with pool.acquire() as conn:
        yield conn
