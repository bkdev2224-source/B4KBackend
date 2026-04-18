"""
Dedup 검토 큐 수동 처리 스크립트

  python scripts/review_dedup.py

각 항목마다:
  m  → 병합 (기존 TourAPI 장소에 MOIS 소스 연결)
  n  → 신규 (별개 장소로 신규 insert)
  s  → 스킵 (나중에 처리)
  q  → 종료
"""
import sys
import json
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

from database.db import get_conn

SEP = "-" * 60


def fetch_review_queue() -> list[dict]:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT place_id, name, address, source_name, dedup_review_meta
              FROM core.places
             WHERE dedup_status = 'review'
               AND dedup_review_meta IS NOT NULL
             ORDER BY place_id
            """
        )
        return list(cur.fetchall())


def do_merge(place_id: int, meta: dict) -> None:
    """기존 장소에 MOIS 소스 연결."""
    source_name = meta["raw_source_name"]
    source_id   = meta["raw_source_id"]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO core.place_source_ids (place_id, source_name, source_id)
            VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
            """,
            (place_id, source_name, source_id),
        )
        cur.execute(
            """
            UPDATE core.places
               SET dedup_status = 'confirmed', dedup_review_meta = NULL
             WHERE place_id = %s
            """,
            (place_id,),
        )
        cur.execute(
            """
            UPDATE stage.raw_documents
               SET sync_status = 'processed', processed_at = now()
             WHERE source_name = %s AND source_id = %s
            """,
            (source_name, source_id),
        )
    print(f"  >> 병합 완료: place_id={place_id} ← {source_name}:{source_id}")


def do_insert_new(place_id: int, meta: dict) -> None:
    """raw_documents를 'new'로 되돌려 normalizer가 신규 insert하게 함."""
    source_name = meta["raw_source_name"]
    source_id   = meta["raw_source_id"]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE stage.raw_documents
               SET sync_status = 'new', processed_at = NULL
             WHERE source_name = %s AND source_id = %s
            """,
            (source_name, source_id),
        )
        cur.execute(
            """
            UPDATE core.places
               SET dedup_status = 'auto', dedup_review_meta = NULL
             WHERE place_id = %s
            """,
            (place_id,),
        )
    print(f"  >> 신규 등록 예약: {source_name}:{source_id} → normalize 재실행 필요")


def main():
    items = fetch_review_queue()
    if not items:
        print("검토 큐가 비어 있습니다.")
        return

    print(f"\n검토 대기 {len(items)}건\n")
    merged = new = skipped = 0

    for i, item in enumerate(items, 1):
        meta = item["dedup_review_meta"] or {}
        print(SEP)
        print(f"[{i}/{len(items)}]")
        print(f"  [기존 TourAPI]  place_id={item['place_id']}")
        print(f"    이름:    {item['name']}")
        print(f"    주소:    {item['address']}")
        print(f"  [신규 MOIS]     {meta.get('raw_source_name')}:{meta.get('raw_source_id')}")
        print(f"    이름:    {meta.get('raw_name')}")
        print(f"    주소:    {meta.get('raw_address')}")
        print(f"    유사도:  {meta.get('score')}")
        print()

        while True:
            choice = input("  선택 (m=병합 / n=신규 / s=스킵 / q=종료): ").strip().lower()
            if choice == "m":
                do_merge(item["place_id"], meta)
                merged += 1
                break
            elif choice == "n":
                do_insert_new(item["place_id"], meta)
                new += 1
                break
            elif choice == "s":
                skipped += 1
                break
            elif choice == "q":
                print(f"\n종료. 병합={merged} 신규={new} 스킵={skipped}")
                return
            else:
                print("  m / n / s / q 중 하나를 입력하세요.")

    print(SEP)
    print(f"\n완료. 병합={merged} | 신규={new} | 스킵={skipped}")
    if new > 0:
        print("신규 선택한 항목은 아래 명령어로 정규화하세요:")
        print("  python scripts/run_phase2.py --normalize")


if __name__ == "__main__":
    main()
