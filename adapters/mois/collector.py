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
                # 배치 단위로 커밋 — 대용량 파일에서 트랜잭션이 무한히 커지는 것 방지
                conn.commit()
                if total % 1_000 == 0 or total == 0:
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
        logger.info(
            "MOIS CSV 증분 수집 시작: %s (since=%s)",
            csv_path.name, since.strftime("%Y-%m-%d %H:%M:%S"),
        )
        run_id = self._create_sync_run("incremental")
        new_cnt = mod_cnt = skip_cnt = total = 0

        with get_conn() as conn:
            for batch in self._csv_batches(csv_path, since=since):
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
                # 배치 단위로 커밋 — 대용량 파일에서 트랜잭션이 무한히 커지는 것 방지
                conn.commit()
                if total % 1_000 == 0 or total == 0:
                    logger.info(
                        "[2-1] 증분 %d행 처리 중 | 신규 %d | 변경 %d | 스킵(비영업) %d",
                        total, new_cnt, mod_cnt, skip_cnt,
                    )

            self._update_sync_run(conn, run_id, "success", new_cnt, mod_cnt, 0)
            self._update_sync_state(conn, run_id)

        logger.info(
            "[2-1] 증분 수집 완료 | 신규 %d | 변경 %d | 스킵(비영업) %d",
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

        detected_enc: str | None = None
        for enc in encodings:
            try:
                logger.debug("[2-1] 인코딩 시도: %s (%s)", enc, csv_path.name)
                fh = open(csv_path, encoding=enc, newline="")
                reader = csv.DictReader(fh)
                # 헤더 정규화 (공백·대소문자·알리아스)
                raw_headers = reader.fieldnames or []
                # 첫 행 읽어 인코딩 검증 (UnicodeDecodeError 여기서 터짐)
                first = next(iter(reader), None)
                # 핵심 컬럼 "사업장명"이 헤더에 없으면 인코딩 불일치로 간주
                if first is not None and "사업장명" not in raw_headers:
                    logger.debug("[2-1] 헤더에 '사업장명' 없음 → 인코딩 불일치: %s", enc)
                    fh.close()
                    fh = None
                    continue
                detected_enc = enc
                logger.debug("[2-1] 인코딩 확정: %s", enc)
                break
            except (UnicodeDecodeError, StopIteration):
                logger.debug("[2-1] 인코딩 실패: %s, 다음 시도...", enc)
                if fh:
                    fh.close()
                fh = None

        if fh is None or reader is None or detected_enc is None:
            logger.error("[2-1] CSV 인코딩 감지 실패 (시도: %s): %s", encodings, csv_path)
            raise ValueError(f"CSV 인코딩 감지 실패: {csv_path}")

        try:
            batch: list[dict] = []
            # 인코딩 검증 시 소비된 첫 행을 복구하기 위해 파일 처음으로 되감음.
            # seek(0) 후 DictReader를 새로 만들면 헤더를 자동으로 다시 읽으므로
            # next(reader) 로 헤더를 수동으로 건너뛸 필요 없음.
            fh.seek(0)
            reader = csv.DictReader(fh)
            reader.fieldnames = [_alias(h.strip()) for h in (reader.fieldnames or [])]

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
                logger.debug(
                    "[2-1] WGS84 범위 초과 (lat=%.5f, lng=%.5f), TM 변환 시도",
                    lat, lng,
                )
            except (ValueError, TypeError) as e:
                logger.debug("[2-1] WGS84 좌표 파싱 오류 (lat=%s, lng=%s): %s", lat_str, lng_str, e)

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
                    logger.debug(
                        "[2-1] TM 변환 후 WGS84 범위 초과 (lat=%.5f, lng=%.5f)",
                        lat, lng,
                    )
                else:
                    logger.debug("[2-1] TM 좌표 범위 초과 → 좌표 없음 (x=%s, y=%s)", x_str, y_str)
            except Exception as exc:
                logger.debug("[2-1] 좌표 변환 실패 (x=%s, y=%s): %s", x_str, y_str, exc)

        return {}

    def _get_source_id(self, conn) -> int:
        """stage.api_sources에서 source name으로 정수 ID 반환. 없으면 자동 등록."""
        cur = conn.cursor()
        cur.execute("SELECT id FROM stage.api_sources WHERE name = %s", (self.SOURCE_NAME,))
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute(
            "INSERT INTO stage.api_sources (name, display_name) VALUES (%s, %s) RETURNING id",
            (self.SOURCE_NAME, self.SOURCE_NAME),
        )
        conn.commit()
        return cur.fetchone()["id"]

    def _upsert(self, conn, data: dict) -> bool:
        """stage.raw_documents upsert. True=신규, False=변경."""
        external_id = str(data.get("source_id") or "").strip()
        if not external_id:
            logger.warning("[2-1] 관리번호(source_id) 없음 → 스킵 (name=%s)", data.get("name"))
            return False

        api_source_id = self._get_source_id(conn)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO stage.raw_documents
                (source_id, external_id, raw_json, is_processed, collected_at)
            VALUES (%s, %s, %s, FALSE, now())
            ON CONFLICT (source_id, external_id, language_code) DO UPDATE
              SET raw_json     = EXCLUDED.raw_json,
                  is_processed = CASE
                                   WHEN stage.raw_documents.raw_json = EXCLUDED.raw_json
                                   THEN stage.raw_documents.is_processed
                                   ELSE FALSE
                                 END,
                  collected_at = now()
            RETURNING (xmax = 0) AS is_insert
            """,
            (api_source_id, external_id, json.dumps(data, ensure_ascii=False)),
        )
        result = cur.fetchone()
        return bool(result and result["is_insert"])

    def _create_sync_run(self, run_type: str) -> int:
        _type_map = {"full": "full_load", "incremental": "fetch_updated"}
        db_run_type = _type_map.get(run_type, run_type)
        with get_conn() as conn:
            source_id = self._get_source_id(conn)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO stage.sync_runs (source_id, run_type, status)
                VALUES (%s, %s, 'running') RETURNING id
                """,
                (source_id, db_run_type),
            )
            conn.commit()
            return cur.fetchone()["id"]

    def _update_sync_run(
        self, conn, run_id: int, status: str, new_cnt: int, mod_cnt: int, del_cnt: int = 0
    ) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE stage.sync_runs
               SET status = %s, finished_at = now(),
                   records_collected = %s
             WHERE id = %s
            """,
            (status, new_cnt + mod_cnt, run_id),
        )

    def _update_sync_state(self, conn, run_id: int) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE stage.source_sync_state
               SET last_synced_at = now()
             WHERE source_id = (SELECT id FROM stage.api_sources WHERE name = %s)
            """,
            (self.SOURCE_NAME,),
        )
