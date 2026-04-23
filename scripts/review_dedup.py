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
import re
from collections import Counter
from pathlib import Path

_NUMBERED_BRANCH_RE = re.compile(r"\d+호점")

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
                   q.name_similarity AS score,
                   q.raw_doc_id,
                   rd.raw_json AS candidate_raw
              FROM core.dedup_review_queue q
              JOIN core.poi p ON p.id = q.poi_id_a
              LEFT JOIN stage.raw_documents rd ON rd.id = q.raw_doc_id
             WHERE q.status = 'pending'
             ORDER BY q.id
            """
        )
        return list(cur.fetchall())


def do_merge(item: dict) -> None:
    """기존 POI에 MOIS 소스 연결 후 raw_document를 processed 처리."""
    place_id = item["place_id"]
    queue_id = item["queue_id"]
    raw_doc_id = item.get("raw_doc_id")
    candidate: dict = item.get("candidate_raw") or {}

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE core.dedup_review_queue
               SET status = 'merged', reviewed_at = now()
             WHERE id = %s
            """,
            (queue_id,),
        )
        # MOIS source_id를 기존 POI의 source_ids JSONB에 추가
        mois_external_id = (
            candidate.get("관리번호") or candidate.get("MGTNO") or ""
        ).strip()
        if mois_external_id:
            cur.execute(
                """
                UPDATE core.poi
                   SET source_ids = source_ids || jsonb_build_object('mois', %s),
                       updated_at = now()
                 WHERE id = %s
                """,
                (mois_external_id, place_id),
            )
        else:
            cur.execute(
                "UPDATE core.poi SET updated_at = now() WHERE id = %s",
                (place_id,),
            )
        # raw_document 처리 완료로 표시
        if raw_doc_id:
            cur.execute(
                "UPDATE stage.raw_documents SET is_processed = TRUE WHERE id = %s",
                (raw_doc_id,),
            )
    print(f"  >> 병합 완료: poi_id={place_id} (queue_id={queue_id})")


def do_insert_new(item: dict) -> None:
    """검토 큐를 'rejected'로 닫고 raw_document를 미처리 상태로 복원해 normalizer가 신규 POI를 만들게 한다."""
    queue_id = item["queue_id"]
    raw_doc_id = item.get("raw_doc_id")
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
        if raw_doc_id:
            cur.execute(
                "UPDATE stage.raw_documents SET is_processed = FALSE WHERE id = %s",
                (raw_doc_id,),
            )
    print(f"  >> 신규 등록 예약: queue_id={queue_id} → normalize 재실행 필요")


def korean_char_overlap(name_a: str, name_b: str) -> float:
    """두 이름 사이의 한글 글자 겹침 비율 (0.0 ~ 1.0).
    한글 음절만 추출해 다중집합 교집합 / max(len_a, len_b) 로 계산.
    예) '스타벅스 강남점' vs '스타벅스 강남' → 6/7 = 0.857
        '롯데리아 홍대점' vs '롯데리아 홍대입구점' → 7/9 = 0.778
    """
    a = [c for c in name_a if "가" <= c <= "힣"]
    b = [c for c in name_b if "가" <= c <= "힣"]
    if not a or not b:
        return 0.0
    common = sum((Counter(a) & Counter(b)).values())
    return common / max(len(a), len(b))


def _fmt(val) -> str:
    return str(val) if val else "(없음)"


def main():
    items = fetch_review_queue()
    if not items:
        print("검토 큐가 비어 있습니다.")
        return

    print(f"\n검토 대기 {len(items)}건\n")
    merged = new = skipped = auto_merged = 0
    AUTO_MERGE_OVERLAP = 0.90  # 한글 글자 겹침 비율 임계값

    for i, item in enumerate(items, 1):
        candidate: dict = item.get("candidate_raw") or {}
        # 후보에서 이름/주소 추출 (MOIS 및 기타 소스 공통)
        cand_name = (
            candidate.get("사업장명") or candidate.get("BPLCNM") or
            candidate.get("name") or candidate.get("title") or ""
        )
        cand_addr = (
            candidate.get("도로명전체주소") or candidate.get("RDNWHLADDR") or
            candidate.get("지번전체주소") or candidate.get("SITEWHLADDR") or
            candidate.get("address") or candidate.get("addr1") or "(없음)"
        )
        cand_phone = candidate.get("소재지전화") or candidate.get("SITETEL") or ""
        cand_source = candidate.get("_source") or "mois"

        # ── N호점 충돌: 한쪽에만 있으면 자동 신규 등록 ────────────────
        existing_name = item.get("name") or ""
        has_branch_a = bool(_NUMBERED_BRANCH_RE.search(existing_name))
        has_branch_b = bool(_NUMBERED_BRANCH_RE.search(cand_name))
        if has_branch_a != has_branch_b:
            print(SEP)
            print(
                f"[{i}/{len(items)}]  AUTO-NEW  N호점 충돌  "
                f"(queue_id={item['queue_id']})"
            )
            print(f"  기존: {_fmt(existing_name)}  →  후보: {_fmt(cand_name) if cand_name else '(없음)'}")
            do_insert_new(item)
            new += 1
            continue

        # ── 한글 글자 겹침 자동 병합 판정 ──────────────────────────────
        overlap = korean_char_overlap(existing_name, cand_name)
        if overlap >= AUTO_MERGE_OVERLAP:
            print(SEP)
            print(
                f"[{i}/{len(items)}]  AUTO-MERGE  한글겹침={overlap:.0%}  "
                f"유사도={item.get('score')}  (queue_id={item['queue_id']})"
            )
            print(f"  기존: {_fmt(item['name'])}  →  후보: {_fmt(cand_name) if cand_name else '(없음)'}")
            do_merge(item)
            auto_merged += 1
            merged += 1
            continue
        # ────────────────────────────────────────────────────────────────

        print(SEP)
        print(
            f"[{i}/{len(items)}]  한글겹침={overlap:.0%}  유사도={item.get('score')}  "
            f"(queue_id={item['queue_id']})"
        )
        print()
        print(f"  [기존 POI]  poi_id={item['place_id']}")
        print(f"    이름:    {_fmt(item['name'])}")
        print(f"    주소:    {_fmt(item['address'])}")
        print(f"    소스:    {json.dumps(item.get('source_ids') or {}, ensure_ascii=False)}")
        print()
        print(f"  [신규 후보]  raw_doc_id={item.get('raw_doc_id')}  (source={cand_source})")
        print(f"    이름:    {_fmt(cand_name) if cand_name else '(없음)'}")
        print(f"    주소:    {cand_addr}")
        if cand_phone:
            print(f"    전화:    {cand_phone}")
        print()

        while True:
            choice = input("  선택 (m=병합확정 / n=신규 / s=스킵 / q=종료): ").strip().lower()
            if choice == "m":
                do_merge(item)
                merged += 1
                break
            elif choice == "n":
                do_insert_new(item)
                new += 1
                break
            elif choice == "s":
                skipped += 1
                break
            elif choice == "q":
                print(f"\n종료. 자동병합={auto_merged} | 병합={merged} 신규={new} 스킵={skipped}")
                return
            else:
                print("  m / n / s / q 중 하나를 입력하세요.")

    print(SEP)
    print(f"\n완료. 자동병합={auto_merged} | 수동병합={merged - auto_merged} | 신규={new} | 스킵={skipped}")
    if new > 0:
        print("신규 선택한 항목은 아래 명령어로 정규화하세요:")
        print("  python scripts/run_phase2.py --normalize")


if __name__ == "__main__":
    main()
