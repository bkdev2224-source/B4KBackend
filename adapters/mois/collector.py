"""
Step 2-1  행정안전부 (지역데이터) CSV 수집기
  - 인허가 데이터 CSV → stage.raw_documents (source_name='mois')
  - EPSG:5174 (TM 좌표) → WGS84(4326) pyproj 자동 변환
  - 영업상태구분코드 필터: '01'(영업) 만 적재 (폐업·휴업 제외)
  - CsvSchemaInspector 재사용 (Phase 1 설계)
"""
from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

import pandas as pd
import psycopg2.extras
from pyproj import Transformer

from utils.csv_inspector import CsvSchemaInspector
from database.db import get_conn

logger = logging.getLogger(__name__)

# EPSG:5174 (구 TM 중부원점) → WGS84
_TM_TO_WGS84 = Transformer.from_crs("EPSG:5174", "EPSG:4326", always_xy=True)

# 영업상태구분코드 → '01' 만 허용 (영업 중)
OPEN_STATUS_CODE = {"01", "1"}  # CSV='01', API 응답='1' 둘 다 허용

# 행안부 CSV 컬럼 매핑 (공공데이터포털 지역데이터 표준 컬럼)
MOIS_FIELD_MAP = {
    "관리번호":            "source_id",
    "사업장명":            "name",
    "도로명전체주소":       "address",
    "지번전체주소":        "address_jibun",
    "도로명우편번호":       "postal_code",
    "소재지전화":          "phone",
    "업태구분명":          "source_category",
    "영업상태구분코드":     "status_code",
    "영업상태명":          "status_name",
    "좌표정보(x)":         "coord_x",
    "좌표정보(y)":         "coord_y",
    "위도":               "lat_wgs84",
    "경도":               "lng_wgs84",
    "시도명":              "region_sido",
    "시군구명":            "region_sigungu",
    "행정구역코드":         "region_code",
    "최종수정시점":         "modified_at",
    "데이터갱신일자":       "updated_date",
}

# 알리아스 — 데이터셋마다 헤더명이 조금씩 다름
MOIS_FIELD_ALIASES: dict[str, str] = {
    "좌표정보x":              "coord_x",
    "좌표정보y":              "coord_y",
    "좌표정보(x)":            "coord_x",
    "좌표정보(y)":            "coord_y",
    "좌표정보(X)":            "coord_x",
    "좌표정보(Y)":            "coord_y",
    "좌표정보x(epsg5174)":    "coord_x",
    "좌표정보y(epsg5174)":    "coord_y",
    "x좌표":                 "coord_x",
    "y좌표":                 "coord_y",
    "도로명주소":              "address",
    "사업장도로명주소":         "address",
    "소재지도로명주소":         "address",
    "소재지전체주소":           "address",
}


def _alias(col: str) -> str:
    return MOIS_FIELD_ALIASES.get(col, col)


class MoisCollector:
    """
    행정안전부 인허가 CSV → stage.raw_documents 적재.

    Usage:
        collector = MoisCollector()
        run_id = collector.run_full("/data/mois_restaurant.csv")
    """

    SOURCE_NAME = "mois"

    def __init__(self):
        self.inspector = CsvSchemaInspector()

    # ── 공개 API ──────────────────────────────────────────────────────────────

    def run_full(self, csv_path: str | Path) -> int:
        """최초 전체 수집. run_id 반환."""
        csv_path = Path(csv_path)
        schema = self.inspector.inspect(csv_path)
        logger.info("MOIS CSV 전체 수집 시작: %s (%d 행)", csv_path.name, schema["row_count"])

        run_id = self._create_sync_run("full")
        new_cnt = mod_cnt = skip_cnt = total = 0

        with get_conn() as conn:
            for batch in self._csv_batches(csv_path):
                for row in batch:
                    total += 1
                    if not self._is_open(row):
                        skip_cnt += 1
                        continue
                    normalized = self._normalize_row(row)
                    is_new = self._upsert(conn, normalized)
                    if is_new:
                        new_cnt += 1
                    else:
                        mod_cnt += 1
                    if total % 1_000 == 0:
                        logger.info(
                            "[2-1] %d행 처리 중 | 신규 %d | 변경 %d | 스킵(비영업) %d",
                            total, new_cnt, mod_cnt, skip_cnt,
                        )

            self._update_sync_run(conn, run_id, "success", new_cnt, mod_cnt, 0)
            self._update_sync_state(conn, run_id)

        logger.info(
            "[2-1] 완료 | 신규 %d | 변경 %d | 스킵(비영업) %d",
            new_cnt, mod_cnt, skip_cnt,
        )
        return run_id

    def run_incremental(self, csv_path: str | Path, since: datetime) -> int:
        """증분 수집 — since 이후 최종수정시점 행만 처리."""
        csv_path = Path(csv_path)
        run_id = self._create_sync_run("incremental")
        new_cnt = mod_cnt = skip_cnt = 0

        with get_conn() as conn:
            for batch in self._csv_batches(csv_path, since=since):
                for row in batch:
                    if not self._is_open(row):
                        skip_cnt += 1
                        continue
                    normalized = self._normalize_row(row)
                    is_new = self._upsert(conn, normalized)
                    if is_new:
                        new_cnt += 1
                    else:
                        mod_cnt += 1

            self._update_sync_run(conn, run_id, "success", new_cnt, mod_cnt, 0)
            self._update_sync_state(conn, run_id)

        logger.info(
            "MOIS 증분 수집 완료: 신규 %d, 변경 %d, 건너뜀(비영업) %d",
            new_cnt, mod_cnt, skip_cnt,
        )
        return run_id

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _csv_batches(
        self,
        csv_path: Path,
        batch_size: int = 1000,
        since: datetime | None = None,
    ) -> Iterator[list[dict]]:
        encodings = ["utf-8-sig", "cp949", "euc-kr"]
        fh = None
        reader = None

        for enc in encodings:
            try:
                fh = open(csv_path, encoding=enc, newline="")
                reader = csv.DictReader(fh)
                # 헤더 정규화 (공백·대소문자·알리아스)
                raw_headers = reader.fieldnames or []
                reader.fieldnames = [
                    _alias(h.strip()) for h in raw_headers
                ]
                # 첫 행 읽어 인코딩 검증
                first = next(iter(reader), None)
                if first is not None and "사업장명" not in raw_headers:
                    # 컬럼이 전혀 없으면 인코딩 실패로 간주
                    pass
                break
            except (UnicodeDecodeError, StopIteration):
                if fh:
                    fh.close()
                fh = None

        if fh is None or reader is None:
            raise ValueError(f"CSV 인코딩 감지 실패: {csv_path}")

        try:
            batch: list[dict] = []
            # first row already consumed — reset
            fh.seek(0)
            reader = csv.DictReader(fh)
            reader.fieldnames = [_alias(h.strip()) for h in (reader.fieldnames or [])]
            next(reader)  # skip header row

            for row in reader:
                row = {k: (v.strip() if v else None) for k, v in row.items()}

                if since:
                    mod_val = row.get("modified_at") or row.get("updated_date")
                    if mod_val:
                        try:
                            mod_dt = datetime.strptime(mod_val[:14], "%Y%m%d%H%M%S")
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
        finally:
            fh.close()

    def _is_open(self, row: dict) -> bool:
        """영업상태구분코드 = '01' (영업 중) 인 경우만 True."""
        code = row.get("status_code") or row.get("영업상태구분코드") or ""
        # 코드가 없으면 (구형 데이터) 영업상태명으로 판단
        if not code:
            name = row.get("status_name") or row.get("영업상태명") or ""
            return "영업" in name and "폐업" not in name and "휴업" not in name
        return code.strip() in OPEN_STATUS_CODE

    def _normalize_row(self, row: dict) -> dict[str, Any]:
        """CSV 행 → 정규화 딕셔너리 (raw_data JSONB에 저장)."""
        out: dict[str, Any] = {"_source": "mois"}

        for src_key, dst_key in MOIS_FIELD_MAP.items():
            # 원본 키와 알리아스 모두 시도
            val = row.get(_alias(src_key)) or row.get(src_key)
            if val is not None:
                out[dst_key] = val

        # 나머지 원천 컬럼도 보존
        mapped_keys = set(MOIS_FIELD_MAP.values()) | {"_source"}
        for k, v in row.items():
            if k not in mapped_keys and v:
                out[k] = v

        # 좌표 처리
        out.update(self._resolve_coords(row))
        return out

    def _resolve_coords(self, row: dict) -> dict[str, Any]:
        """
        1순위: 위도/경도 (WGS84 직접 제공)
        2순위: 좌표정보(x/y) → EPSG:5174 → WGS84 변환
        """
        # WGS84 직접 제공
        lat_str = row.get("lat_wgs84") or row.get("위도")
        lng_str = row.get("lng_wgs84") or row.get("경도")
        if lat_str and lng_str:
            try:
                lat, lng = float(lat_str), float(lng_str)
                if 33.0 <= lat <= 38.9 and 124.0 <= lng <= 132.0:
                    return {"lat": lat, "lng": lng, "coord_crs": "WGS84"}
            except (ValueError, TypeError):
                pass

        # EPSG:5174 TM 좌표 변환
        x_str = row.get("coord_x") or row.get("좌표정보(x)")
        y_str = row.get("coord_y") or row.get("좌표정보(y)")
        if x_str and y_str:
            try:
                x_tm, y_tm = float(x_str), float(y_str)
                # TM 좌표 범위 검증 (대략 100,000 ~ 900,000 범위)
                if 100_000 < x_tm < 900_000 and 100_000 < y_tm < 900_000:
                    lng, lat = _TM_TO_WGS84.transform(x_tm, y_tm)
                    if 33.0 <= lat <= 38.9 and 124.0 <= lng <= 132.0:
                        return {
                            "lat": round(lat, 7),
                            "lng": round(lng, 7),
                            "coord_crs": "EPSG:5174→WGS84",
                        }
            except Exception as exc:
                logger.debug("좌표 변환 실패 (x=%s y=%s): %s", x_str, y_str, exc)

        return {}

    def _upsert(self, conn, data: dict) -> bool:
        """stage.raw_documents upsert. True=신규, False=변경."""
        source_id = data.get("source_id") or ""
        if not source_id:
            return False

        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO stage.raw_documents (source_name, source_id, raw_data, sync_status, collected_at)
            VALUES (%s, %s, %s, 'new', now())
            ON CONFLICT (source_name, source_id) DO UPDATE
              SET raw_data     = EXCLUDED.raw_data,
                  sync_status  = CASE
                                   WHEN stage.raw_documents.raw_data = EXCLUDED.raw_data
                                   THEN stage.raw_documents.sync_status
                                   ELSE 'modified'
                                 END,
                  collected_at = now()
            RETURNING (xmax = 0) AS is_insert
            """,
            (self.SOURCE_NAME, source_id, json.dumps(data, ensure_ascii=False)),
        )
        result = cur.fetchone()
        return bool(result and result["is_insert"])

    def _create_sync_run(self, run_type: str) -> int:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO stage.sync_runs (source_name, run_type, status)
                VALUES (%s, %s, 'running') RETURNING id
                """,
                (self.SOURCE_NAME, run_type),
            )
            return cur.fetchone()["id"]

    def _update_sync_run(
        self, conn, run_id: int, status: str, new_cnt: int, mod_cnt: int, del_cnt: int
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
