"""
Phase 0 실행 스크립트 — DB 초기화
  python scripts/init_db.py          # 스키마 생성 (없으면 생성, 있으면 유지)
  python scripts/init_db.py --reset  # 전체 데이터 삭제 후 스키마 재생성
"""
import argparse
import sys
import traceback
from pathlib import Path

# Windows 콘솔 UTF-8
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

import psycopg2
from config.settings import settings

SEP = "-" * 60

TRUNCATE_SQL = """
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='core' AND table_name='translation_fill_queue') THEN
        TRUNCATE core.translation_fill_queue CASCADE;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='core' AND table_name='place_translations') THEN
        TRUNCATE core.place_translations CASCADE;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='core' AND table_name='place_images') THEN
        TRUNCATE core.place_images CASCADE;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='core' AND table_name='place_source_ids') THEN
        TRUNCATE core.place_source_ids CASCADE;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='core' AND table_name='places') THEN
        TRUNCATE core.places CASCADE;
    END IF;
    IF EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema='stage' AND table_name='raw_documents') THEN
        TRUNCATE stage.raw_documents CASCADE;
    END IF;
END$$;
"""

RESET_SQL = """
DROP SCHEMA IF EXISTS ai      CASCADE;
DROP SCHEMA IF EXISTS "user"  CASCADE;
DROP SCHEMA IF EXISTS service CASCADE;
DROP SCHEMA IF EXISTS core    CASCADE;
DROP SCHEMA IF EXISTS stage   CASCADE;
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="DB 초기화")
    parser.add_argument(
        "--reset",
        action="store_true",
        help="전체 데이터 삭제 후 스키마 재생성 (주의: 되돌릴 수 없음)",
    )
    args = parser.parse_args()

    schema_sql = (
        Path(__file__).parent.parent / "database" / "schema.sql"
    ).read_text(encoding="utf-8")

    try:
        conn = psycopg2.connect(settings.db_dsn)
        conn.autocommit = True
        cur = conn.cursor()
    except Exception as exc:
        print(SEP)
        print(f"!! FAILED [0-1] DB 연결 실패")
        print(f"   {type(exc).__name__}: {exc}")
        print(f"   원인: {traceback.format_exc().strip().splitlines()[-1]}")
        print(SEP)
        return 1

    # ── RESET ──────────────────────────────────────────────────────
    if args.reset:
        print(SEP)
        print(">> RESET  모든 스키마 삭제 중... (stage / core / service / user / ai)")
        print(SEP)
        try:
            cur.execute("SET statement_timeout = 0;")
            print("   [1/2] 대용량 테이블 데이터 삭제 중...")
            cur.execute(TRUNCATE_SQL)
            print("   [2/2] 스키마 삭제 중...")
            cur.execute(RESET_SQL)
            print("OK DONE   스키마 삭제 완료")
        except Exception as exc:
            print(f"!! FAILED 스키마 삭제 실패: {exc}")
            conn.close()
            return 1

    # ── INIT ───────────────────────────────────────────────────────
    print(SEP)
    print(">> START  [0-1] DB 스키마 생성")
    print(f"          host={settings.db_host}:{settings.db_port}  db={settings.db_name}")
    print(SEP)

    try:
        cur.execute(schema_sql)
        conn.close()
    except Exception as exc:
        print(SEP)
        print(f"!! FAILED [0-1] {type(exc).__name__}: {exc}")
        print(f"   원인: {traceback.format_exc().strip().splitlines()[-1]}")
        print(SEP)
        return 1

    print("OK DONE   [0-1] stage / core / service / user / ai 스키마 생성 완료")
    print(SEP)
    return 0


if __name__ == "__main__":
    sys.exit(main())
