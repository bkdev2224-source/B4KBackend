"""
CSV 파일 → stage.raw_documents 일괄 적재
실행: python scripts/load_csv.py
"""
import asyncio
import csv
import json
import sys
from pathlib import Path

import asyncpg

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from settings import settings

CSV_FILES = {
    "ko":    ROOT / "tour_kor.csv",
    "en":    ROOT / "tour_eng.csv",
    "ja":    ROOT / "tour_jpn.csv",
    "zh-CN": ROOT / "tour_chs.csv",
    "zh-TW": ROOT / "tour_cht.csv",
    "fr":    ROOT / "tour_fre.csv",
    "de":    ROOT / "tour_ger.csv",
    "es":    ROOT / "tour_spn.csv",
    "ru":    ROOT / "tour_rus.csv",
}

BATCH_SIZE = 500


async def load_csv(conn: asyncpg.Connection, source_id: int, language_code: str, csv_path: Path) -> int:
    # sync_run 등록
    run_id = await conn.fetchval(
        """
        INSERT INTO stage.sync_runs (source_id, run_type, language_code)
        VALUES ($1, 'full_load', $2) RETURNING id
        """,
        source_id, language_code,
    )

    rows = []
    total = 0

    with open(csv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append((
                source_id,
                row["contentid"],
                language_code,
                json.dumps(row, ensure_ascii=False),
                run_id,
            ))

            if len(rows) >= BATCH_SIZE:
                await _insert_batch(conn, rows)
                total += len(rows)
                rows = []

    if rows:
        await _insert_batch(conn, rows)
        total += len(rows)

    # sync_run 완료
    await conn.execute(
        """
        UPDATE stage.sync_runs
        SET status = 'done', records_collected = $1, finished_at = NOW()
        WHERE id = $2
        """,
        total, run_id,
    )

    return total


async def _insert_batch(conn: asyncpg.Connection, rows: list) -> None:
    await conn.executemany(
        """
        INSERT INTO stage.raw_documents
            (source_id, external_id, language_code, raw_json, sync_run_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (source_id, external_id, language_code)
        DO UPDATE SET raw_json = EXCLUDED.raw_json,
                      sync_run_id = EXCLUDED.sync_run_id,
                      collected_at = NOW()
        """,
        rows,
    )


async def main() -> None:
    print("=== CSV → stage.raw_documents 적재 ===\n")

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

    grand_total = 0
    for lang, path in CSV_FILES.items():
        if not path.exists():
            print(f"[skip] {path.name} 없음")
            continue
        print(f"[{lang}] {path.name} 적재 중...", end=" ", flush=True)
        count = await load_csv(conn, source_id, lang, path)
        print(f"{count:,}건")
        grand_total += count

    await conn.close()
    print(f"\n=== 완료: 총 {grand_total:,}건 ===")


if __name__ == "__main__":
    asyncio.run(main())
