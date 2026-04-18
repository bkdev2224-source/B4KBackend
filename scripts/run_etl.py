"""
Phase 3: Stage → Core ETL
실행:
  python scripts/run_etl.py              # 전체 (다국어 원본 포함)
  python scripts/run_etl.py --ko-only    # 첫 실행용: ko 원본만 처리
  python scripts/run_etl.py --skip-images

--ko-only: Phase 2에서 ko만 수집한 경우 사용.
  step 2 (translations)를 ko만 실행하고 나머지 언어는 건너뜀.
  → translation_fill_queue에 10개 언어 적재는 정상 수행됨.
  → Phase 4(GPT 번역)에서 채워질 예정.

순서:
  1. stage.raw_documents (ko) → core.poi upsert
  2. stage.raw_documents (언어별) → core.poi_translations upsert
  3. 누락 번역 → translation_fill_queue 적재
  4. Cloudinary 이미지 파이프라인 (CLOUDINARY_URL 설정 시)
  5. raw_documents.is_processed = TRUE 마킹
"""
import asyncio
import json
import sys
from pathlib import Path

import asyncpg

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from settings import settings
from etl.transform import normalize_poi, normalize_translation
from etl.fill_queue import enqueue_missing_translations
from etl.cloudinary_pipeline import process_poi_images

BATCH_SIZE = 500
LANGUAGES = ["ko", "en", "ja", "zh-CN", "zh-TW", "th", "pt-BR", "es", "de", "fr", "ru"]


# ─────────────────────────────────────────
# Step 1: ko raw_documents → core.poi
# ─────────────────────────────────────────
async def etl_poi(conn: asyncpg.Connection, source_id: int) -> int:
    offset = 0
    total = 0

    while True:
        rows = await conn.fetch(
            """
            SELECT id, raw_json FROM stage.raw_documents
            WHERE source_id = $1 AND language_code = 'ko'
            ORDER BY id
            LIMIT $2 OFFSET $3
            """,
            source_id, BATCH_SIZE, offset,
        )
        if not rows:
            break

        batch = []
        for row in rows:
            raw = json.loads(row["raw_json"])
            p = normalize_poi(raw)
            source_ids = json.dumps({"tourapi": p["external_id"]})

            geom = None
            if p["lon"] and p["lat"]:
                geom = f"SRID=4326;POINT({p['lon']} {p['lat']})"

            batch.append((
                source_ids,
                p["name_ko"],
                p["address_ko"],
                geom,
                p["category_code"],
                p["content_type_id"],
                p["phone"],
                p["quality"],
            ))

        await conn.executemany(
            """
            INSERT INTO core.poi
                (source_ids, name_ko, address_ko, geom,
                 category_code, content_type_id, phone, quality)
            VALUES ($1, $2, $3,
                    CASE WHEN $4::text IS NOT NULL
                         THEN ST_GeomFromEWKT($4::text) ELSE NULL END,
                    $5, $6, $7, $8)
            ON CONFLICT ((source_ids->>'tourapi')) WHERE source_ids ? 'tourapi'
            DO NOTHING
            """,
            batch,
        )

        total += len(batch)
        offset += BATCH_SIZE
        print(f"  [poi] {total:,}건 처리", end="\r")

    print(f"  [poi] {total:,}건 완료")
    return total


# ─────────────────────────────────────────
# Step 2: 전 언어 → core.poi_translations
# ─────────────────────────────────────────
async def build_contentid_map(conn: asyncpg.Connection) -> dict[str, int]:
    """contentid → poi.id 매핑 딕셔너리"""
    rows = await conn.fetch(
        "SELECT id, source_ids->>'tourapi' AS cid FROM core.poi WHERE source_ids ? 'tourapi'"
    )
    return {r["cid"]: r["id"] for r in rows}


async def etl_translations(
    conn: asyncpg.Connection,
    source_id: int,
    cid_map: dict,
    ko_only: bool = False,
) -> int:
    total = 0
    langs = ["ko"] if ko_only else LANGUAGES

    for lang in langs:
        offset = 0
        lang_count = 0

        while True:
            rows = await conn.fetch(
                """
                SELECT raw_json FROM stage.raw_documents
                WHERE source_id = $1 AND language_code = $2
                ORDER BY id
                LIMIT $3 OFFSET $4
                """,
                source_id, lang, BATCH_SIZE, offset,
            )
            if not rows:
                break

            batch = []
            for row in rows:
                raw = json.loads(row["raw_json"])
                cid = str(raw.get("contentid", ""))
                poi_id = cid_map.get(cid)
                if poi_id is None:
                    continue

                t = normalize_translation(raw)
                batch.append((poi_id, lang, t["name"], t["address"]))

            if batch:
                await conn.executemany(
                    """
                    INSERT INTO core.poi_translations
                        (poi_id, language_code, name, address, source)
                    VALUES ($1, $2, $3, $4, 'api')
                    ON CONFLICT (poi_id, language_code)
                    DO UPDATE SET name = EXCLUDED.name,
                                  address = EXCLUDED.address,
                                  updated_at = NOW()
                    """,
                    batch,
                )

            lang_count += len(rows)
            offset += BATCH_SIZE

        print(f"  [translations] {lang}: {lang_count:,}건")
        total += lang_count

    return total


# ─────────────────────────────────────────
# Step 3: 누락 번역 → fill_queue
# ─────────────────────────────────────────
async def etl_fill_queue(conn: asyncpg.Connection) -> None:
    # SQL로 한 번에 누락 언어 계산 후 일괄 삽입
    all_langs = ["en", "ja", "zh-CN", "zh-TW", "th", "pt-BR", "es", "de", "fr", "ru"]
    langs_literal = ", ".join(f"'{l}'" for l in all_langs)

    await conn.execute(
        f"""
        INSERT INTO core.translation_fill_queue (poi_id, language_code, field)
        SELECT p.id, l.lang, f.field
        FROM core.poi p
        CROSS JOIN (VALUES {', '.join(f"('{l}')" for l in all_langs)}) AS l(lang)
        CROSS JOIN (VALUES ('name'), ('address')) AS f(field)
        WHERE NOT EXISTS (
            SELECT 1 FROM core.poi_translations t
            WHERE t.poi_id = p.id AND t.language_code = l.lang
        )
        ON CONFLICT (poi_id, language_code, field) DO NOTHING
        """
    )
    count = await conn.fetchval("SELECT COUNT(*) FROM core.translation_fill_queue")
    print(f"  [fill_queue] {count:,}건 적재 완료")


# ─────────────────────────────────────────
# Step 5: is_processed 마킹
# ─────────────────────────────────────────
async def mark_processed(conn: asyncpg.Connection, source_id: int) -> None:
    await conn.execute(
        "UPDATE stage.raw_documents SET is_processed = TRUE WHERE source_id = $1",
        source_id,
    )
    print("  [mark] raw_documents is_processed = TRUE")


# ─────────────────────────────────────────
# Main
# ─────────────────────────────────────────
async def main(skip_images: bool = False, ko_only: bool = False) -> None:
    print("=== Phase 3: ETL 시작 ===\n")

    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        statement_cache_size=0,
    )

    source_id = await conn.fetchval(
        "SELECT id FROM stage.api_sources WHERE name = 'tourapi'"
    )

    # 이미 완료된 단계는 주석 처리 가능
    print("1) core.poi upsert (한국어 기준)")
    await etl_poi(conn, source_id)

    print("\n2) core.poi_translations upsert (전 언어)")
    if ko_only:
        print("  [ko-only] ko 원본만 저장, 나머지는 Phase 4(GPT)에서 처리")
    cid_map = await build_contentid_map(conn)
    print(f"  poi 매핑: {len(cid_map):,}건")
    await etl_translations(conn, source_id, cid_map, ko_only=ko_only)

    print("\n3) translation_fill_queue 적재")
    await etl_fill_queue(conn)

    print("\n4) Cloudinary 이미지 파이프라인")
    print("  [skip] 이미지 업로드는 scripts/run_cloudinary.py 에서 별도 실행")

    print("\n5) raw_documents 처리 완료 마킹")
    await mark_processed(conn, source_id)

    await conn.close()
    print("\n=== ETL 완료 ===")


if __name__ == "__main__":
    skip = "--skip-images" in sys.argv
    ko_only = "--ko-only" in sys.argv
    asyncio.run(main(skip_images=skip, ko_only=ko_only))
