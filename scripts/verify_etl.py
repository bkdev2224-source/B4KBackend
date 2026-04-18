"""
Phase 3-5: ETL 검증
실행: python scripts/verify_etl.py

확인 항목:
  1. core.poi 건수 및 quality 분포
  2. 경복궁 POI 상세 확인
  3. translation_fill_queue 언어별 적재 건수
  4. core.poi_images 건수 + Cloudinary URL 샘플
"""
import asyncio
import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

import asyncpg
from settings import settings


async def main() -> None:
    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        statement_cache_size=0,
    )

    print("=" * 55)
    print("Phase 3-5 ETL 검증")
    print("=" * 55)

    # 1. core.poi 건수 + quality 분포
    total_poi = await conn.fetchval("SELECT COUNT(*) FROM core.poi")
    print(f"\n[1] core.poi 총 건수: {total_poi:,}")
    rows = await conn.fetch(
        "SELECT quality, COUNT(*) AS cnt FROM core.poi GROUP BY quality ORDER BY cnt DESC"
    )
    for r in rows:
        print(f"     quality={r['quality']}: {r['cnt']:,}")

    # 2. 경복궁 POI 상세
    gyeongbok = await conn.fetchrow(
        """
        SELECT id, name_ko, address_ko, quality,
               ST_X(geom) AS lon, ST_Y(geom) AS lat
        FROM core.poi
        WHERE name_ko LIKE '%경복궁%'
        LIMIT 1
        """
    )
    print("\n[2] 경복궁 POI")
    if gyeongbok:
        print(f"     id      : {gyeongbok['id']}")
        print(f"     name_ko : {gyeongbok['name_ko']}")
        print(f"     address : {gyeongbok['address_ko']}")
        print(f"     quality : {gyeongbok['quality']}")
        print(f"     좌표    : ({gyeongbok['lon']}, {gyeongbok['lat']})")

        # 경복궁 번역 큐 언어 확인
        queue_langs = await conn.fetch(
            """
            SELECT language_code, COUNT(*) AS cnt
            FROM core.translation_fill_queue
            WHERE poi_id = $1
            GROUP BY language_code
            ORDER BY language_code
            """,
            gyeongbok["id"],
        )
        print(f"     translation_fill_queue 언어 수: {len(queue_langs)}")
        if queue_langs:
            langs = [r["language_code"] for r in queue_langs]
            print(f"     언어 목록: {', '.join(langs)}")
    else:
        print("     ※ 경복궁 데이터 없음")

    # 3. translation_fill_queue 언어별 건수
    print("\n[3] translation_fill_queue 언어별 건수")
    queue_rows = await conn.fetch(
        """
        SELECT language_code, COUNT(*) AS cnt
        FROM core.translation_fill_queue
        GROUP BY language_code
        ORDER BY language_code
        """
    )
    total_queue = 0
    for r in queue_rows:
        print(f"     {r['language_code']:8s}: {r['cnt']:,}")
        total_queue += r["cnt"]
    print(f"     합계      : {total_queue:,}")

    # 4. core.poi_images
    total_images = await conn.fetchval("SELECT COUNT(*) FROM core.poi_images")
    print(f"\n[4] core.poi_images 건수: {total_images:,}")
    if total_images > 0:
        sample = await conn.fetchrow(
            """
            SELECT cloudinary_public_id, secure_url, thumbnail_url, webp_url
            FROM core.poi_images
            LIMIT 1
            """
        )
        print(f"     샘플 public_id : {sample['cloudinary_public_id']}")
        print(f"     secure_url     : {sample['secure_url'][:80]}...")
        print(f"     thumbnail_url  : {sample['thumbnail_url'][:80]}...")
        print(f"     webp_url       : {sample['webp_url'][:80]}...")
    else:
        print("     ※ 이미지 없음 (Cloudinary 미설정 또는 미실행)")

    await conn.close()
    print("\n" + "=" * 55)
    print("검증 완료")
    print("=" * 55)


if __name__ == "__main__":
    asyncio.run(main())
