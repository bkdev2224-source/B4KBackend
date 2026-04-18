"""
Phase 2-4: 전수 수집 실행

TourAPICollector.full_load("ko") 실행
- 카테고리별 전체 페이지 수집
- sync_runs 이력 기록
- 체크포인트 저장 (중단 시 재시작 가능)

실행:
  python scripts/run_collector.py           # full load
  python scripts/run_collector.py --resume  # 중단 지점부터 재시작

"""
import asyncio
import sys
from pathlib import Path

import asyncpg

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from settings import settings
from collector.adapters.tourapi import TourAPICollector


async def main() -> None:
    resume = "--resume" in sys.argv
    mode = "fetch_updated (재시작)" if resume else "full_load"
    print(f"=== Phase 2-4: TourAPI 전수 수집 [{mode}] ===\n")

    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        statement_cache_size=0,
    )
    try:
        collector = TourAPICollector(conn)

        if resume:
            total = await collector.fetch_updated("ko")
        else:
            total = await collector.full_load("ko")

        print(f"\n수집 완료: {total:,}건")

        # sync_runs 최신 이력 출력
        rows = await conn.fetch(
            """
            SELECT id, run_type, language_code, status, records_collected,
                   started_at, finished_at, error_message
            FROM stage.sync_runs
            ORDER BY id DESC
            LIMIT 5
            """
        )
        print("\n[sync_runs 최근 5건]")
        for r in rows:
            elapsed = ""
            if r["finished_at"] and r["started_at"]:
                sec = (r["finished_at"] - r["started_at"]).total_seconds()
                elapsed = f"  ({sec:.0f}초)"
            print(
                f"  id={r['id']}  {r['run_type']}  {r['language_code']}  "
                f"{r['status']}  {r['records_collected']}건{elapsed}"
            )
            if r["error_message"]:
                print(f"    error: {r['error_message']}")

        # raw_documents 건수 출력
        cnt = await conn.fetchval("SELECT COUNT(*) FROM stage.raw_documents")
        print(f"\n[raw_documents 전체 건수]: {cnt:,}건")

    finally:
        await conn.close()

    print("\n=== 완료 ===")


if __name__ == "__main__":
    asyncio.run(main())
