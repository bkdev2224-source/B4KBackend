"""
07_security.sql 단독 마이그레이션 스크립트
  python scripts/apply_security.py          # dry-run (실제 적용 안 함, 검증만)
  python scripts/apply_security.py --apply  # 실제 DB에 적용

기존 데이터 보존: ALTER TABLE ADD COLUMN IF NOT EXISTS, CREATE TABLE IF NOT EXISTS,
                  DROP POLICY IF EXISTS + CREATE POLICY 등 모두 멱등(idempotent)
주의: user.reviews에 UNIQUE(user_id, place_id) 제약 추가됨
      중복 row 있으면 실패 → 아래 사전 체크로 확인
"""
import sys
import traceback
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
import psycopg2
from config.settings import settings

SEP = "-" * 60
DDL_DIR = Path(__file__).parent.parent / "db" / "ddl"
# 이번 마이그레이션에서 적용할 파일 순서 (기존 00~05는 이미 적용됨)
MIGRATION_FILES = [
    DDL_DIR / "06_translation.sql",   # translation_rules, translation_glossary
    DDL_DIR / "07_security.sql",       # RLS + Role + 보안 함수
]

# 적용 전 사전 체크 쿼리
PRE_CHECKS = [
    (
        "user.reviews 중복 (user_id, place_id) 확인",
        """
        SELECT COUNT(*) FROM (
            SELECT user_id, place_id, COUNT(*)
            FROM "user".reviews
            GROUP BY user_id, place_id
            HAVING COUNT(*) > 1
        ) t
        """,
        lambda n: (n == 0, f"중복 {n}건 존재 → 제약 추가 전 제거 필요"),
    ),
    (
        "user.users.supabase_uid 컬럼 존재 여부",
        """
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = 'user'
          AND table_name   = 'users'
          AND column_name  = 'supabase_uid'
        """,
        lambda n: (True, "이미 존재" if n > 0 else "신규 추가 예정"),
    ),
    (
        "api 스키마 존재 여부",
        """
        SELECT COUNT(*) FROM information_schema.schemata
        WHERE schema_name = 'api'
        """,
        lambda n: (True, "이미 존재" if n > 0 else "신규 생성 예정"),
    ),
]


def run_checks(cur) -> bool:
    print(f"\n{'체크':>6}  사전 검증")
    print(SEP)
    all_ok = True
    for label, sql, validator in PRE_CHECKS:
        cur.execute(sql)
        n = cur.fetchone()[0]
        ok, msg = validator(n)
        status = "OK  " if ok else "FAIL"
        print(f"  [{status}] {label}: {msg}")
        if not ok:
            all_ok = False
    print(SEP)
    return all_ok


def main() -> int:
    parser = argparse.ArgumentParser(description="07_security.sql 마이그레이션")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="실제 DB에 적용 (없으면 dry-run)",
    )
    args = parser.parse_args()

    for f in MIGRATION_FILES:
        if not f.exists():
            print(f"!! FAILED  DDL 파일 없음: {f}")
            return 1

    print(SEP)
    print(">> START   마이그레이션 (06_translation + 07_security)")
    print(f"           host={settings.db_host}:{settings.db_port}  db={settings.db_name}")
    print(f"           파일: {[f.name for f in MIGRATION_FILES]}")
    print(f"           mode={'APPLY' if args.apply else 'DRY-RUN (검증만)'}")
    print(SEP)

    try:
        conn = psycopg2.connect(settings.db_dsn)
        conn.autocommit = False
        cur = conn.cursor()
    except Exception as exc:
        print(f"!! FAILED  DB 연결 실패: {exc}")
        return 1

    # 사전 체크
    ok = run_checks(cur)
    if not ok:
        print("!! ABORTED 사전 검증 실패. 위 항목 해결 후 재시도.")
        conn.close()
        return 1

    if not args.apply:
        print("\n  DRY-RUN 완료. 실제 적용하려면: python scripts/apply_security.py --apply")
        conn.close()
        return 0

    # 실제 적용
    print("\n>> APPLY   마이그레이션 적용 중...")
    try:
        cur.execute("SET statement_timeout = '60s';")
        for f in MIGRATION_FILES:
            print(f"   [{f.name}] 적용 중...")
            cur.execute(f.read_text(encoding="utf-8"))
            print(f"   [{f.name}] 완료")
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print(SEP)
        print(f"!! FAILED  {type(exc).__name__}: {exc}")
        print(f"   {traceback.format_exc().strip().splitlines()[-1]}")
        print(SEP)
        conn.close()
        return 1

    conn.close()

    print("OK DONE    마이그레이션 완료")
    print(SEP)
    print("  적용 내용:")
    print("  - user.users.supabase_uid UUID 컬럼 추가")
    print("  - user.reviews UNIQUE(user_id, place_id) 제약 추가")
    print("  - 전 테이블 RLS 활성화 (stage/core/service/user/ai/api)")
    print("  - anon/authenticated 권한 재설정")
    print("  - api.* SECURITY DEFINER 함수 생성")
    print("  - api.audit_log + api.rate_limit_counter 테이블 생성")
    print(SEP)
    print("  다음 단계: Supabase 대시보드 → Authentication → Users에서")
    print("  신규 유저의 supabase_uid를 user.users에 연결 로직 추가 필요")
    print(SEP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
