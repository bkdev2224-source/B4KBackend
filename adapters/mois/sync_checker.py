"""
Step 2-3  행정안전부 Sync 판단 — 지역데이터 history API
  - BASE_DATE 기준 변경 이력 조회
  - 업종별 OPN_ATMY_GRP_CD 코드로 필터
  - sync_status 'new' | 'modified' 기록 → stage.raw_documents 적재
  - 영업 상태가 아닌 항목은 is_active=false 처리

API 예시:
  GET https://apis.data.go.kr/1741000/{serviceId}/history
    ?cond[BASE_DATE::EQ]=20260101
    &cond[OPN_ATMY_GRP_CD::EQ]=30000000
    &pageIndex=1&pageSize=1000&resultType=json
"""
from __future__ import annotations

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)

# 업종별 서비스 ID + OPN_ATMY_GRP_CD 코드 매핑
# key: domain_map "mois" 카테고리명
# value: (service_id, opn_atmy_grp_cd)
#   service_id: 공공데이터포털 행안부 서비스 경로 식별자
#   opn_atmy_grp_cd: 인허가 업종 그룹 코드
MOIS_SERVICE_MAP: dict[str, tuple[str, str]] = {
    # 식품
    "관광식당":       ("tourist_restaurants",            "07_24_01_P"),
    "일반음식점":     ("general_restaurants",            "07_24_02_P"),
    "휴게음식점":     ("rest_cafes",                     "07_24_03_P"),
    "제과점영업":     ("bakeries",                       "07_24_04_P"),
    # 문화/공연
    "공연장":         ("performance_halls",              "03_06_01_P"),
    "관광공연장업":   ("tourist_performance_halls",      "03_06_02_P"),
    "노래연습장업":   ("karaoke_rooms",                  "07_28_01_P"),
    # 숙박
    "관광숙박업":     ("tourist_accommodations",         "07_27_01_P"),
    "관광펜션업":     ("tourist_pensions",               "07_27_02_P"),
    "숙박업":         ("lodgings",                       "07_27_03_P"),
    # 레저/체육
    "스키장":         ("ski_resorts",                    "10_36_01_P"),
    "등록체육시설업": ("registered_sports_facilities",   "10_36_02_P"),
    "종합체육시설업": ("comprehensive_sports_facilities", "10_36_03_P"),
    "목욕장업":       ("public_baths",                   "07_26_01_P"),
    # 미용/건강
    "미용업":         ("beauty_salons",                  "07_25_01_P"),
    "안경업":         ("optical_shops",                  "07_04_04_P"),
}

# 영업 중으로 판단할 영업상태 코드 목록
OPEN_CODES = {"01", "1"}

MOIS_HISTORY_BASE = "https://apis.data.go.kr/1741000"


class MoisSyncChecker:
    """
    행안부 지역데이터 history API로 변경분을 감지해 stage에 등록.

    Usage:
        checker = MoisSyncChecker()
        run_id = checker.run()                          # 전체 업종
        run_id = checker.run(categories=["일반음식점"]) # 특정 업종만
    """

    SOURCE_NAME = "mois"
    PAGE_SIZE = 1000

    def __init__(self):
        self.api_key = settings.mois_api_key if hasattr(settings, "mois_api_key") else ""
        if not self.api_key:
            logger.warning("MOIS_API_KEY 미설정 — sync_checker가 API를 호출하지 못합니다.")

    # ── 공개 API ──────────────────────────────────────────────────────────────

    def run(self, categories: list[str] | None = None) -> int:
        """
        categories: None이면 MOIS_SERVICE_MAP 전체 순회.
        run_id 반환.
        """
        last_synced = self._get_last_synced_at()
        base_date = self._to_base_date(last_synced)
        run_id = self._create_sync_run()
        new_cnt = mod_cnt = 0

        targets = categories or list(MOIS_SERVICE_MAP.keys())
        error_cnt = 0

        try:
            for category in targets:
                service_info = MOIS_SERVICE_MAP.get(category)
                if not service_info:
                    logger.warning("알 수 없는 MOIS 카테고리: %s", category)
                    continue
                service_id, grp_cd = service_info
                try:
                    n, m = self._sync_category(service_id, grp_cd, base_date)
                    new_cnt += n
                    mod_cnt += m
                    logger.info("MOIS sync 완료: %s → 신규 %d, 변경 %d", category, n, m)
                    time.sleep(0.2)     # rate-limit 배려
                except Exception as exc:
                    error_cnt += 1
                    logger.error("MOIS sync 실패 (category=%s): %s", category, exc)

            final_status = "partial" if error_cnt else "success"
            with get_conn() as conn:
                self._update_sync_run(conn, run_id, final_status, new_cnt, mod_cnt)
                self._update_sync_state(conn, run_id)
        except Exception as fatal_exc:
            logger.exception("MOIS 치명적 오류 — sync_run failed로 기록: %s", fatal_exc)
            with get_conn() as conn:
                self._update_sync_run(conn, run_id, "failed", new_cnt, mod_cnt)
            raise

        logger.info("MOIS 전체 Sync 완료: 신규 %d, 변경 %d", new_cnt, mod_cnt)
        return run_id

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _sync_category(
        self,
        service_id: str,
        grp_cd: str,
        base_date: str,
    ) -> tuple[int, int]:
        """단일 업종 전체 페이지 순회. (신규, 변경) 수 반환."""
        new_cnt = mod_cnt = 0
        page = 1

        while True:
            items = self._fetch_page(service_id, grp_cd, base_date, page)
            if not items:
                break

            with get_conn() as conn:
                for item in items:
                    status = self._classify(item)
                    if status == "deleted":
                        self._deactivate(conn, item)
                    elif status in ("new", "modified"):
                        self._upsert_raw(conn, item, status)
                        if status == "new":
                            new_cnt += 1
                        else:
                            mod_cnt += 1

            if len(items) < self.PAGE_SIZE:
                break
            page += 1

        return new_cnt, mod_cnt

    def _classify(self, item: dict[str, Any]) -> str:
        """'new' | 'modified' | 'deleted' 반환."""
        status_code = str(item.get("TRDSTATEGBN") or item.get("영업상태구분코드") or "")
        data_div = str(item.get("DTLSTATEGBN") or item.get("데이터갱신구분") or "")

        # 폐업/휴업/취소
        if status_code and status_code not in OPEN_CODES:
            return "deleted"

        # 데이터갱신구분: I=신규, U=변경, D=삭제
        if data_div == "I":
            return "new"
        if data_div == "U":
            return "modified"
        if data_div == "D":
            return "deleted"

        # 구분 코드 없으면 기본 modified
        return "modified"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_page(
        self,
        service_id: str,
        grp_cd: str,
        base_date: str,
        page: int,
    ) -> list[dict[str, Any]]:
        url = f"{MOIS_HISTORY_BASE}/{service_id}/history"
        params = {
            "serviceKey":  self.api_key,
            "resultType":  "json",
            "pageIndex":   page,
            "pageSize":    self.PAGE_SIZE,
            f"cond[BASE_DATE::EQ]": base_date,
            f"cond[OPN_ATMY_GRP_CD::EQ]": grp_cd,
        }
        resp = httpx.get(url, params=params, timeout=30)
        resp.raise_for_status()

        body = resp.json()
        # 응답 구조: { "body": { "items": [...], "totalCount": N } }
        items = (
            body.get("body", {}).get("items") or
            body.get("response", {}).get("body", {}).get("items", {}).get("item", [])
        )
        if isinstance(items, dict):
            items = [items]
        return items or []

    def _get_api_source_id(self, conn) -> int:
        cur = conn.cursor()
        cur.execute("SELECT id FROM stage.api_sources WHERE name = %s", (self.SOURCE_NAME,))
        row = cur.fetchone()
        if row:
            return row["id"]
        cur.execute(
            "INSERT INTO stage.api_sources (name) VALUES (%s) RETURNING id",
            (self.SOURCE_NAME,),
        )
        conn.commit()
        return cur.fetchone()["id"]

    def _upsert_raw(self, conn, item: dict[str, Any], status: str) -> None:
        external_id = str(
            item.get("MGTNO") or item.get("관리번호") or item.get("id") or ""
        ).strip()
        if not external_id:
            return

        api_source_id = self._get_api_source_id(conn)
        normalized = self._normalize_item(item)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO stage.raw_documents
                (source_id, external_id, raw_json, is_processed, collected_at)
            VALUES (%s, %s, %s, FALSE, now())
            ON CONFLICT (source_id, external_id, language_code) DO UPDATE
              SET raw_json     = EXCLUDED.raw_json,
                  is_processed = FALSE,
                  collected_at = now()
            """,
            (api_source_id, external_id, json.dumps(normalized, ensure_ascii=False)),
        )

    def _deactivate(self, conn, item: dict[str, Any]) -> None:
        """폐업·취소 항목 → core.poi is_active=false."""
        external_id = str(
            item.get("MGTNO") or item.get("관리번호") or ""
        ).strip()
        if not external_id:
            return
        api_source_id = self._get_api_source_id(conn)
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE core.poi
               SET is_active = FALSE, updated_at = now()
             WHERE source_ids ? %s
               AND source_ids->>%s = %s
            """,
            (self.SOURCE_NAME, self.SOURCE_NAME, external_id),
        )

    @staticmethod
    def _normalize_item(item: dict[str, Any]) -> dict[str, Any]:
        """API 응답 키를 CSV 표준 컬럼명으로 변환."""
        # API 응답 키 (영문 대문자) → CSV 표준 한글 키 매핑
        KEY_MAP = {
            "MGTNO":        "관리번호",
            "BPLCNM":       "사업장명",
            "RDNWHLADDR":   "도로명전체주소",
            "SITEWHLADDR":  "지번전체주소",
            "SITETEL":      "소재지전화",
            "UPTAENM":      "업태구분명",
            "TRDSTATEGBN":  "영업상태구분코드",
            "TRDSTATENM":   "영업상태명",
            "X":            "좌표정보(x)",
            "Y":            "좌표정보(y)",
            "LASTMODTS":    "최종수정시점",
            "DTLSTATEGBN":  "데이터갱신구분",
            "SIDONM":       "시도명",
            "SIGUNGUNM":    "시군구명",
        }
        out: dict[str, Any] = {"_source": "mois"}
        for k, v in item.items():
            mapped = KEY_MAP.get(k, k)
            out[mapped] = v
        return out

    # ── 상태 관리 ─────────────────────────────────────────────────────────────

    @staticmethod
    def _to_base_date(last_synced: datetime | None) -> str:
        """마지막 sync 시각 → BASE_DATE 문자열 (YYYYMMDD)."""
        if last_synced is None:
            # 최초 수집: 전체 이력 (충분히 과거 날짜)
            return "19900101"
        return last_synced.strftime("%Y%m%d")

    def _get_last_synced_at(self) -> datetime | None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT ss.last_synced_at
                  FROM stage.source_sync_state ss
                  JOIN stage.api_sources s ON s.id = ss.source_id
                 WHERE s.name = %s
                """,
                (self.SOURCE_NAME,),
            )
            row = cur.fetchone()
            return row["last_synced_at"] if row else None

    def _create_sync_run(self) -> int:
        with get_conn() as conn:
            api_source_id = self._get_api_source_id(conn)
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO stage.sync_runs (source_id, run_type, status)
                VALUES (%s, 'fetch_updated', 'running') RETURNING id
                """,
                (api_source_id,),
            )
            conn.commit()
            return cur.fetchone()["id"]

    def _update_sync_run(
        self, conn, run_id: int, status: str, new_cnt: int, mod_cnt: int
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
