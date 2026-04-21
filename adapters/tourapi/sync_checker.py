"""
Step 1-2  관광공사 Sync 판단 — areaBasedSyncList2 API
  - 현재 날짜 기준으로 변경 목록 조회
  - 신규(createdtime) → INSERT, 변경(modifiedtime) → UPDATE
  - stage.raw_documents에 sync_status 반영
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

AREA_CODES = [1, 2, 3, 4, 5, 6, 7, 8, 31, 32, 33, 34, 35, 36, 37, 38, 39]
CONTENT_TYPE_IDS = [12, 14, 25, 28, 32, 38, 39, 75, 76]


class TourApiSyncChecker:
    """
    areaBasedSyncList2 API를 사용해 변경분을 stage에 등록.

    Usage:
        checker = TourApiSyncChecker()
        run_id = checker.run()
    """

    SOURCE_NAME = "tourapi"
    BASE_URL = settings.tourapi_base_url
    PAGE_SIZE = 1000

    def __init__(self):
        self.api_key = settings.tourapi_key
        if not self.api_key:
            logger.warning("[1-2] TOURAPI_KEY 미설정 — API 호출 불가")

    # ── 공개 API ──────────────────────────────────────────────────────────────

    def run(self) -> int:
        """전체 지역·카테고리를 순회하며 변경분 감지. run_id 반환."""
        last_synced = self._get_last_synced_at()
        run_id = self._create_sync_run()

        total_tasks = len(AREA_CODES) * len(CONTENT_TYPE_IDS)
        logger.info(
            "[1-2] Sync 시작 | last_synced=%s | 지역 %d개 x 콘텐츠타입 %d개 = %d 조합",
            last_synced.isoformat() if last_synced else "최초",
            len(AREA_CODES), len(CONTENT_TYPE_IDS), total_tasks,
        )
        logger.info("[1-2] sync_run 등록 | run_id=%d", run_id)

        new_cnt = mod_cnt = done = error_cnt = 0

        try:
            for area_code in AREA_CODES:
                for ct_id in CONTENT_TYPE_IDS:
                    done += 1
                    try:
                        n, m = self._sync_area_type(area_code, ct_id, last_synced)
                        new_cnt += n
                        mod_cnt += m
                        if n or m:
                            logger.info(
                                "[1-2] area=%2d ct=%2d | 신규 %d | 변경 %d  (%d/%d)",
                                area_code, ct_id, n, m, done, total_tasks,
                            )
                        time.sleep(0.1)
                    except Exception as exc:
                        error_cnt += 1
                        logger.error(
                            "[1-2] 실패 area=%d ct=%d (%d/%d): %s",
                            area_code, ct_id, done, total_tasks, exc,
                        )

            final_status = "partial" if error_cnt else "success"
            with get_conn() as conn:
                self._update_sync_run(conn, run_id, final_status, new_cnt, mod_cnt)
                self._update_sync_state(conn, run_id)
        except Exception as fatal_exc:
            logger.exception("[1-2] 치명적 오류 — sync_run failed로 기록: %s", fatal_exc)
            with get_conn() as conn:
                self._update_sync_run(conn, run_id, "failed", new_cnt, mod_cnt)
            raise

        logger.info(
            "[1-2] 완료 | 신규 %d | 변경 %d | 오류 %d건 | run_id=%d",
            new_cnt, mod_cnt, error_cnt, run_id,
        )
        return run_id

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _sync_area_type(
        self,
        area_code: int,
        content_type_id: int,
        last_synced: datetime | None,
    ) -> tuple[int, int]:
        new_cnt = mod_cnt = 0
        page = 1

        while True:
            items = self._fetch_page(area_code, content_type_id, page)
            if not items:
                break

            with get_conn() as conn:
                for item in items:
                    status = self._classify(item, last_synced)
                    if status:
                        self._upsert_raw(conn, item, status)
                        if status == "new":
                            new_cnt += 1
                            logger.info("  👉 [신규 추가] %s (ID: %s)", item.get("title", "이름없음"), item.get("contentid"))
                        else:
                            mod_cnt += 1
                            logger.info("  👉 [내용 수정] %s (ID: %s)", item.get("title", "이름없음"), item.get("contentid"))

            if len(items) < self.PAGE_SIZE:
                break

            logger.debug(
                "[1-2] 페이징 area=%d ct=%d page=%d → 다음 페이지",
                area_code, content_type_id, page,
            )
            page += 1

        return new_cnt, mod_cnt

    def _classify(
        self,
        item: dict[str, Any],
        last_synced: datetime | None,
    ) -> str | None:
        """'new' | 'modified' | None(변경없음) 반환."""
        if last_synced is None:
            return "new"

        def parse_ts(s: str | None) -> datetime | None:
            if not s:
                return None
            for fmt in ("%Y%m%d%H%M%S", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(s, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    pass
            return None

        created_at  = parse_ts(item.get("createdtime"))
        modified_at = parse_ts(item.get("modifiedtime"))

        if created_at and created_at > last_synced:
            return "new"
        if modified_at and modified_at > last_synced:
            return "modified"
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    def _fetch_page(
        self,
        area_code: int,
        content_type_id: int,
        page: int,
    ) -> list[dict[str, Any]]:
        params = {
            "serviceKey":    self.api_key,
            "MobileOS":      "ETC",
            "MobileApp":     "B4K",
            "_type":         "json",
            "areaCode":      area_code,
            "contentTypeId": content_type_id,
            "numOfRows":     self.PAGE_SIZE,
            "pageNo":        page,
            "showflag":      1,
        }
        url = f"{self.BASE_URL}/areaBasedSyncList2"
        resp = httpx.get(url, params=params, timeout=30)
        resp.raise_for_status()

        body  = resp.json()
        # 1. body 노드까지 안전하게 접근
        body_node = body.get("response", {}).get("body", {})
        items_node = body_node.get("items", {})
        
        # 2. 방어 로직: items_node가 딕셔너리가 아니면(예: "") 빈 리스트 반환
        if not isinstance(items_node, dict):
            return []
            
        # 3. 정상적인 경우 item 추출
        items = items_node.get("item", [])
        
        # 4. 결과가 1건일 경우 dict로 오기 때문에 list로 감싸줌
        if isinstance(items, dict):
            items = [items]
            
        return items

    def _get_api_source_id(self, conn) -> int:
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

    def _upsert_raw(self, conn, item: dict[str, Any], status: str) -> None:
        external_id = str(item.get("contentid", "")).strip()
        if not external_id:
            return

        api_source_id = self._get_api_source_id(conn)
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
            (api_source_id, external_id, json.dumps(item, ensure_ascii=False)),
        )

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

    def _update_sync_run(self, conn, run_id: int, status: str, new_cnt: int, mod_cnt: int) -> None:
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
