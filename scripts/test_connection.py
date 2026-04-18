"""
DB 연결 테스트 스크립트
실행: python scripts/test_connection.py
"""
import asyncio
import sys
from pathlib import Path

import asyncpg

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from settings import settings


async def main() -> None:
    print(f"Connecting to {settings.db_host}:{settings.db_port}/{settings.db_name} ...")
    try:
        conn = await asyncpg.connect(
            host=settings.db_host,
            port=settings.db_port,
            database=settings.db_name,
            user=settings.db_user,
            password=settings.db_password,
            statement_cache_size=0,
        )
    except Exception as e:
        print(f"[fail] 연결 실패: {e}")
        sys.exit(1)

    try:
        # 버전
        pg_version = await conn.fetchval("SELECT version()")
        print(f"[ok]   PostgreSQL: {pg_version}")

        # PostGIS
        try:
            postgis_version = await conn.fetchval("SELECT PostGIS_Version()")
            print(f"[ok]   PostGIS: {postgis_version}")
        except Exception:
            print("[warn] PostGIS extension이 설치되지 않았습니다. init_db.py를 먼저 실행하세요.")

        # 스키마 확인
        schemas = await conn.fetch(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name IN ('stage','core','service','user') "
            "ORDER BY schema_name"
        )
        found = [r["schema_name"] for r in schemas]
        expected = {"stage", "core", "service", "user"}
        for s in sorted(expected):
            mark = "[ok]  " if s in found else "[miss]"
            print(f"{mark} schema: {s}")

        if expected == set(found):
            print("\n연결 및 스키마 확인 완료!")
        else:
            missing = expected - set(found)
            print(f"\n[warn] 누락된 스키마: {missing}. init_db.py를 실행하세요.")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
