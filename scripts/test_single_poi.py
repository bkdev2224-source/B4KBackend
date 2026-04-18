"""
Phase 2-3: 단일 POI 테스트 (경복궁)

1. TourAPI areaBasedList2 → page=1, numOfRows=1 로 첫 번째 관광지 1건 수집
2. raw_documents에 저장 확인
3. raw_json 내용 출력 (육안 검증)

실행: python scripts/test_single_poi.py
"""
import asyncio
import json
import sys
from pathlib import Path

import asyncpg
import httpx

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from settings import settings

TOURAPI_BASE = "https://apis.data.go.kr/B551011/KorService2"
CONTENT_TYPE_ID = "12"  # 관광지


async def fetch_one_poi(api_key: str) -> dict:
    url = f"{TOURAPI_BASE}/areaBasedList2"
    params = {
        "serviceKey": api_key,
        "numOfRows": 1,
        "pageNo": 1,
        "MobileOS": "ETC",
        "MobileApp": "B4KDataBase",
        "_type": "json",
        "arrange": "A",
        "contentTypeId": CONTENT_TYPE_ID,
    }
    async with httpx.AsyncClient() as client:
        r = await client.get(url, params=params, timeout=30)
        r.raise_for_status()
        data = r.json()

    header = data["response"]["header"]
    if header["resultCode"] != "0000":
        raise RuntimeError(f"TourAPI 오류: {header['resultCode']} - {header['resultMsg']}")

    body = data["response"]["body"]
    items = body.get("items", {}).get("item")
    if not items:
        raise RuntimeError("수집된 항목이 없습니다.")

    return items[0] if isinstance(items, list) else items


async def save_raw_document(conn: asyncpg.Connection, source_id: int, item: dict, run_id: int) -> None:
    await conn.execute(
        """
        INSERT INTO stage.raw_documents
            (source_id, external_id, language_code, raw_json, sync_run_id)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (source_id, external_id, language_code)
        DO UPDATE SET raw_json = EXCLUDED.raw_json,
                      sync_run_id = EXCLUDED.sync_run_id,
                      collected_at = NOW()
        """,
        source_id, str(item["contentid"]), "ko", json.dumps(item, ensure_ascii=False), run_id,
    )


async def main() -> None:
    print("=== Phase 2-3: 단일 POI 테스트 ===\n")

    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        statement_cache_size=0,
    )

    try:
        # source_id 조회
        source_id = await conn.fetchval(
            "SELECT id FROM stage.api_sources WHERE name = 'tourapi'"
        )
        if not source_id:
            raise RuntimeError("stage.api_sources에 'tourapi' 없음 → scripts/seed_data.py 먼저 실행")

        # sync_run 시작
        run_id = await conn.fetchval(
            "INSERT INTO stage.sync_runs (source_id, run_type, language_code) VALUES ($1, 'full_load', 'ko') RETURNING id",
            source_id,
        )
        print(f"sync_run_id: {run_id}")

        # API 호출
        print("TourAPI 호출 중...")
        item = await fetch_one_poi(settings.tour_api_key)
        print(f"수집 완료: contentId={item['contentid']}  title={item.get('title', '')}")

        # DB 저장
        await save_raw_document(conn, source_id, item, run_id)
        print("raw_documents 저장 완료")

        # sync_run 종료
        await conn.execute(
            "UPDATE stage.sync_runs SET status='done', records_collected=1, finished_at=NOW() WHERE id=$1",
            run_id,
        )

        # 저장된 행 조회 및 출력
        row = await conn.fetchrow(
            """
            SELECT external_id, language_code, collected_at, raw_json
            FROM stage.raw_documents
            WHERE source_id=$1 AND external_id=$2 AND language_code='ko'
            """,
            source_id, str(item["contentid"]),
        )
        print(f"\n[raw_documents 조회]")
        print(f"  external_id   : {row['external_id']}")
        print(f"  language_code : {row['language_code']}")
        print(f"  collected_at  : {row['collected_at']}")
        print(f"\n[raw_json 내용]")
        parsed = json.loads(row["raw_json"])
        print(json.dumps(parsed, ensure_ascii=False, indent=2))

    finally:
        await conn.close()

    print("\n=== 2-3 테스트 완료 ===")


if __name__ == "__main__":
    asyncio.run(main())
