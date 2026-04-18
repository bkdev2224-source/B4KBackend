"""
Phase 4-1: GPT-4.1 mini 번역 실행 스크립트

실행 예시:
  # 전체 번역 (translation_fill_queue 전량)
  python scripts/run_translation.py

  # 테스트: 5개 POI만 처리, DB 기록 없이 결과 출력
  python scripts/run_translation.py --dry-run --max-poi 5

  # 최대 100개 POI만 처리 (단계적 실행)
  python scripts/run_translation.py --max-poi 100

  # 번역 완료율만 조회 (번역 실행 없음)
  python scripts/run_translation.py --coverage
"""
import asyncio
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import asyncpg
from settings import settings
from etl.gpt_translator import run_translation_worker, get_translation_coverage

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

LANG_NAMES = {
    "ko": "한국어", "en": "영어", "ja": "일본어",
    "zh-CN": "중국어(간체)", "zh-TW": "중국어(번체)", "th": "태국어",
    "pt-BR": "포르투갈어(브라질)", "es": "스페인어", "de": "독일어",
    "fr": "프랑스어", "ru": "러시아어",
}


async def print_coverage(conn: asyncpg.Connection) -> None:
    coverage = await get_translation_coverage(conn)
    total_poi = await conn.fetchval("SELECT COUNT(*) FROM core.poi WHERE is_active = TRUE")
    queue_cnt = await conn.fetchval("SELECT COUNT(DISTINCT poi_id) FROM core.translation_fill_queue")

    print(f"\n{'─'*45}")
    print(f"  전체 POI: {total_poi:,}개   번역 대기 큐: {queue_cnt:,}건")
    print(f"{'─'*45}")
    for lang, rate in sorted(coverage.items(), key=lambda x: -x[1]):
        bar = "█" * int(rate * 20)
        name = LANG_NAMES.get(lang, lang)
        print(f"  {lang:8s}  {name:20s}  {bar:<20s}  {rate*100:5.1f}%")
    print(f"{'─'*45}\n")


async def main() -> None:
    dry_run  = "--dry-run"  in sys.argv
    coverage_only = "--coverage" in sys.argv
    max_poi: int | None = None
    for arg in sys.argv:
        if arg.startswith("--max-poi="):
            max_poi = int(arg.split("=")[1])
        elif arg == "--max-poi" and sys.argv.index(arg) + 1 < len(sys.argv):
            max_poi = int(sys.argv[sys.argv.index(arg) + 1])

    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        statement_cache_size=0,
    )

    try:
        if coverage_only:
            print("\n=== 번역 완료율 조회 ===")
            await print_coverage(conn)
            return

        print(f"\n=== Phase 4-1: GPT-4.1 mini 번역 시작{'  [DRY RUN]' if dry_run else ''} ===")
        if max_poi:
            print(f"  최대 POI: {max_poi}개")

        await print_coverage(conn)

        stats = await run_translation_worker(conn, max_poi=max_poi, dry_run=dry_run)

        print(f"\n=== 번역 완료 ===")
        print(f"  처리: {stats['processed']:,}개  오류: {stats['errors']:,}개")

        if not dry_run:
            print("\n=== 번역 후 완료율 ===")
            await print_coverage(conn)

    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
