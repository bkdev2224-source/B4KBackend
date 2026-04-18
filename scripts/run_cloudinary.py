"""
Cloudinary 이미지 업로드 (Phase 3-4, 별도 실행)
실행:
  python scripts/run_cloudinary.py             # 미업로드 전체 처리
  python scripts/run_cloudinary.py --limit 500 # 최대 500건만

특징:
  - 이미 poi_images에 있는 poi는 스킵 (재실행 안전)
  - asyncio.Semaphore로 동시 업로드 수 제한 (기본 5)
  - 업로드 실패해도 다음 건 계속 진행
"""
import asyncio
import json
import sys
from pathlib import Path

import asyncpg

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from settings import settings
from etl.cloudinary_pipeline import _configure_cloudinary, _upload_image

CONCURRENCY = 5   # 동시 업로드 수
BATCH_SIZE  = 200


async def run(limit: int | None = None) -> None:
    if not settings.cloudinary_url:
        print("CLOUDINARY_URL이 .env에 없습니다.")
        return

    _configure_cloudinary(settings.cloudinary_url)

    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        statement_cache_size=0,
    )

    # 아직 이미지가 없는 poi 중 firstimage 있는 것 조회
    source_id = await conn.fetchval(
        "SELECT id FROM stage.api_sources WHERE name = 'tourapi'"
    )

    total_done = 0
    offset = 0
    sem = asyncio.Semaphore(CONCURRENCY)

    print(f"=== Cloudinary 업로드 시작 (limit={limit or '무제한'}, concurrency={CONCURRENCY}) ===\n")

    while True:
        if limit and total_done >= limit:
            break

        fetch_size = BATCH_SIZE
        if limit:
            fetch_size = min(BATCH_SIZE, limit - total_done)

        rows = await conn.fetch(
            """
            SELECT r.raw_json
            FROM stage.raw_documents r
            JOIN core.poi p ON p.source_ids->>'tourapi' = r.raw_json->>'contentid'
            WHERE r.source_id = $1
              AND r.language_code = 'ko'
              AND r.raw_json->>'firstimage' IS NOT NULL
              AND r.raw_json->>'firstimage' <> ''
              AND NOT EXISTS (
                  SELECT 1 FROM core.poi_images i WHERE i.poi_id = p.id
              )
            ORDER BY p.id
            LIMIT $2 OFFSET $3
            """,
            source_id, fetch_size, offset,
        )
        if not rows:
            break

        async def upload_one(row: asyncpg.Record) -> None:
            nonlocal total_done
            raw = json.loads(row["raw_json"])
            cid = str(raw.get("contentid", ""))
            poi_id = await conn.fetchval(
                "SELECT id FROM core.poi WHERE source_ids->>'tourapi' = $1", cid
            )
            if poi_id is None:
                return

            original_url = (raw.get("firstimage") or "").strip()
            if not original_url:
                return

            async with sem:
                meta = await _upload_image(original_url, poi_id)

            if meta is None:
                return

            await conn.execute(
                """
                INSERT INTO core.poi_images
                    (poi_id, cloudinary_public_id, secure_url,
                     thumbnail_url, webp_url, original_url,
                     width, height, format, is_primary)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, TRUE)
                ON CONFLICT DO NOTHING
                """,
                poi_id,
                meta["public_id"],
                meta["secure_url"],
                meta["thumbnail_url"],
                meta["webp_url"],
                meta["original_url"],
                meta["width"],
                meta["height"],
                meta["format"],
            )
            total_done += 1
            if total_done % 10 == 0:
                print(f"  {total_done}건 업로드", end="\r")

        await asyncio.gather(*[upload_one(r) for r in rows])
        offset += len(rows)

    await conn.close()
    print(f"\n=== 완료: {total_done}건 업로드 ===")


if __name__ == "__main__":
    limit = None
    for i, arg in enumerate(sys.argv[1:]):
        if arg == "--limit" and i + 2 <= len(sys.argv) - 1:
            limit = int(sys.argv[i + 2])
    asyncio.run(run(limit=limit))
