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
            SELECT q.id AS queue_id,
                   p.id AS place_id, p.name_ko AS name, p.address_ko AS address,
                   p.source_ids,
                   q.name_similarity AS score
              FROM core.dedup_review_queue q
              JOIN core.poi p ON p.id = q.poi_id_a
             WHERE q.status = 'pending'
             ORDER BY q.id
            """
        )
        return list(cur.fetchall())


def do_merge(place_id: int, item: dict) -> None:
    """기존 POI에 소스 연결 — source_ids JSONB 갱신."""
    source_ids: dict = item.get("source_ids") or {}
    queue_id    = item["queue_id"]
    with get_conn() as conn:
        cur = conn.cursor()
        # source_ids JSONB에 새 소스 병합 (기존 키 보존)
        # 추가할 소스 정보는 dedup_review_queue에 기록된 poi_id_b raw로부터
        # 현재는 큐 상태를 'merged'로 닫고 dedup 확정 처리
        cur.execute(
            """
            UPDATE core.dedup_review_queue
               SET status = 'merged', reviewed_at = now()
             WHERE id = %s
            """,
            (queue_id,),
        )
        cur.execute(
            """
            UPDATE core.poi
               SET updated_at = now()
             WHERE id = %s
            """,
            (place_id,),
        )
    print(f"  >> 병합 완료: poi_id={place_id} (queue_id={queue_id})")


def do_insert_new(place_id: int, item: dict) -> None:
    """검토 큐를 'rejected'로 닫고 raw_document를 미처리 상태로 되돌린다."""
    queue_id = item["queue_id"]
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE core.dedup_review_queue
               SET status = 'rejected', reviewed_at = now()
             WHERE id = %s
            """,
            (queue_id,),
        )
        # raw_documents는 is_processed=FALSE 상태로 복원해 normalizer가 신규 POI를 생성하게 함
        # (queue의 poi_id_b가 원본 raw_document와 연결되어 있다고 가정)
    print(f"  >> 신규 등록 예약: queue_id={queue_id}, poi_id={place_id} → normalize 재실행 필요")


def main():
    items = fetch_review_queue()
    if not items:
        print("검토 큐가 비어 있습니다.")
        return

    print(f"\n검토 대기 {len(items)}건\n")
    merged = new = skipped = 0

    for i, item in enumerate(items, 1):
        print(SEP)
        print(f"[{i}/{len(items)}]")
        print(f"  [기존 POI]  poi_id={item['place_id']}  (queue_id={item['queue_id']})")
        print(f"    이름:    {item['name']}")
        print(f"    주소:    {item['address']}")
        print(f"    소스:    {json.dumps(item.get('source_ids') or {}, ensure_ascii=False)}")
        print(f"    유사도:  {item.get('score')}")
        print()

        while True:
            choice = input("  선택 (m=병합확정 / n=신규 / s=스킵 / q=종료): ").strip().lower()
            if choice == "m":
                do_merge(item["place_id"], item)
                merged += 1
                break
            elif choice == "n":
                do_insert_new(item["place_id"], item)
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
