"""
Step 1-1  관광공사 CSV 수집기
  - TourApiCollector : CSV → stage.raw_documents 적재
"""
from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import psycopg2.extras

from database.db import get_conn
from utils.csv_inspector import CsvSchemaInspector

logger = logging.getLogger(__name__)

# 관광공사 CSV 기본 컬럼 매핑
TOURAPI_FIELD_MAP = {
    "contentid":       "source_id",
    "contenttypeid":   "source_category",
    "title":           "name",
    "addr1":           "address",
    "addr2":           "address_detail",
    "mapx":            "lng",
    "mapy":            "lat",
    "tel":             "phone",
    "overview":        "description",
    "areacode":        "region_code",
    "firstimage":      "image_url",
    "firstimage2":     "thumbnail_url",
    "modifiedtime":    "modified_at",
    "createdtime":     "created_at_src",
}

_LOG_INTERVAL = 1_000


class TourApiCollector:
    """
    관광공사 관광정보 CSV → stage.raw_documents 적재.

    Usage:
        collector = TourApiCollector()
        run_id = collector.run_full("/database/csv/tourapi/tour_kor.csv")
    """

    SOURCE_NAME = "tourapi"

    def __init__(self):
        self.inspector = CsvSchemaInspector()

    # ── 공개 API ──────────────────────────────────────────────────────────────

    def run_full(self, csv_path: str | Path) -> int:
        """최초 전체 수집 — CSV 전량을 stage에 적재하고 run_id 반환."""
        csv_path = Path(csv_path)

        logger.info("[1-1] CSV 스키마 분석 중...")
        schema = self.inspector.inspect(csv_path)
        total_rows = schema["row_count"]
        logger.info("[1-1] 전체 수집 시작 | 파일: %s | 대상: %d행", csv_path.name, total_rows)

        run_id = self._create_sync_run("full")
        logger.info("[1-1] sync_run 등록 완료 | run_id=%d", run_id)
        logger.info("[1-1] DB 적재 시작 (배치 1,000행 단위)...")

        new_cnt = mod_cnt = processed = 0

        with get_conn() as conn:
            for batch in self._csv_batches(csv_path, batch_size=1000):
                n, m = self._upsert_batch(conn, batch)
                new_cnt += n
                mod_cnt += m
                processed += len(batch)
                conn.commit()

                if processed % _LOG_INTERVAL == 0 or processed == total_rows:
                    pct = processed / total_rows * 100 if total_rows else 0
                    logger.info(
                        "[1-1] 진행 %d/%d (%.1f%%) | 신규 %d | 변경 %d",
                        processed, total_rows, pct, new_cnt, mod_cnt,
                    )

            logger.info("[1-1] sync_run 상태 업데이트 중...")
            self._update_sync_run(conn, run_id, "success", new_cnt, mod_cnt, 0)
            self._update_sync_state(conn, run_id)

        logger.info(
            "[1-1] 완료 | 신규 %d | 변경 %d | 합계 %d | run_id=%d",
            new_cnt, mod_cnt, new_cnt + mod_cnt, run_id,
        )
        return run_id

    def run_incremental(self, csv_path: str | Path, since: datetime) -> int:
        """증분 수집 — since 이후 modifiedtime 행만 처리."""
        csv_path = Path(csv_path)
        logger.info(
            "[1-1] 증분 수집 시작 | 파일: %s | since: %s",
            csv_path.name, since.isoformat(),
        )

        run_id = self._create_sync_run("incremental")
        logger.info("[1-1] sync_run 등록 | run_id=%d", run_id)

        new_cnt = mod_cnt = processed = 0

        with get_conn() as conn:
            for batch in self._csv_batches(csv_path, batch_size=1000, since=since):
                n, m = self._upsert_batch(conn, batch)
                new_cnt += n
                mod_cnt += m
                processed += len(batch)

                if processed % _LOG_INTERVAL == 0:
                    logger.info(
                        "[1-1] 진행 %d행 처리 | 신규 %d | 변경 %d",
                        processed, new_cnt, mod_cnt,
                    )

            self._update_sync_run(conn, run_id, "success", new_cnt, mod_cnt, 0)
            self._update_sync_state(conn, run_id)

        logger.info(
            "[1-1] 증분 완료 | 신규 %d | 변경 %d | run_id=%d",
            new_cnt, mod_cnt, run_id,
        )
        return run_id

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _csv_batches(
        self,
        csv_path: Path,
        batch_size: int = 1000,
        since: datetime | None = None,
    ) -> Iterator[list[dict]]:
        with open(csv_path, encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            reader.fieldnames = [h.strip().lower() for h in (reader.fieldnames or [])]

            batch: list[dict] = []
            for row in reader:
                row = {k: (v.strip() if v else None) for k, v in row.items()}

                if since and row.get("modifiedtime"):
                    try:
                        mod_dt = datetime.strptime(row["modifiedtime"], "%Y%m%d%H%M%S")
                        mod_dt = mod_dt.replace(tzinfo=timezone.utc)
                        if mod_dt <= since:
                            continue
                    except ValueError:
                        pass

                batch.append(row)
                if len(batch) >= batch_size:
                    yield batch
                    batch = []

            if batch:
                yield batch

    def _upsert_batch(self, conn, rows: list[dict]) -> tuple[int, int]:
        """stage.raw_documents에 upsert. 반환: (신규 수, 변경 수)"""
        new_cnt = mod_cnt = 0
        cur = conn.cursor()

        sql = """
            INSERT INTO stage.raw_documents (source_name, source_id, raw_data, sync_status, collected_at)
            VALUES %s
            ON CONFLICT (source_name, source_id) DO UPDATE
              SET raw_data    = EXCLUDED.raw_data,
                  sync_status = CASE
                                  WHEN stage.raw_documents.raw_data = EXCLUDED.raw_data THEN stage.raw_documents.sync_status
                                  ELSE 'modified'
                                END,
                  collected_at = now()
            RETURNING (xmax = 0) AS is_insert
        """

        # 1. 한 번에 밀어넣을 데이터 리스트 생성
        data_list = []
        for row in rows:
            source_id = row.get("contentid") or row.get("id") or ""
            if not source_id:
                continue

            normalized = self._normalize_row(row)
            # 템플릿: (source_name, source_id, raw_data)
            data_list.append((
                self.SOURCE_NAME, 
                source_id, 
                json.dumps(normalized, ensure_ascii=False)
            ))

        if not data_list:
            return 0, 0

        # 2. execute_values를 이용해 단 1번의 쿼리로 처리 (압도적 속도 향상)
        # 템플릿에 'new'와 now()를 포함시켜 매핑해줍니다.
        results = psycopg2.extras.execute_values(
            cur, 
            sql, 
            data_list, 
            template="(%s, %s, %s, 'new', now())",
            fetch=True
        )

        # 3. RETURNING 결과 집계
        for res in results:
            if res["is_insert"]:  # [0]을 ["is_insert"]로 변경!
                new_cnt += 1
            else:
                mod_cnt += 1

        return new_cnt, mod_cnt

    def _normalize_row(self, row: dict) -> dict:
        """원천 행을 정규화된 딕셔너리로 변환. raw_data JSONB에 저장."""
        out: dict[str, Any] = {"_source": "tourapi"}
        for src_key, dst_key in TOURAPI_FIELD_MAP.items():
            if src_key in row:
                out[dst_key] = row[src_key]

        # 누락 키는 원천 그대로 보존
        for k, v in row.items():
            if k not in TOURAPI_FIELD_MAP:
                out[k] = v

        return out

    def _create_sync_run(self, run_type: str) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO stage.sync_runs (source_name, run_type, status)
                VALUES (%s, %s, 'running')
                RETURNING id
                """,
                (self.SOURCE_NAME, run_type),
            )
            return cur.fetchone()["id"]

    def _update_sync_run(
        self,
        conn,
        run_id: int,
        status: str,
        new_cnt: int,
        mod_cnt: int,
        del_cnt: int,
    ) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE stage.sync_runs
               SET status = %s, finished_at = now(),
                   new_count = %s, modified_count = %s, deleted_count = %s
             WHERE id = %s
            """,
            (status, new_cnt, mod_cnt, del_cnt, run_id),
        )

    def _update_sync_state(self, conn, run_id: int) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE stage.source_sync_state
               SET last_synced_at = now(), last_run_id = %s
             WHERE source_name = %s
            """,
            (run_id, self.SOURCE_NAME),
        )
