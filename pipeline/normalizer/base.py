"""
Step 1-3  Normalizer  (배치 최적화)
  - 500건을 쿼리 5개로 처리 (기존 ~6,500개 → 5개)
  - stage.raw_documents -> core.places batch upsert
  - place_source_ids / place_images / translation_fill_queue batch insert
  - addr1(address) 없는 행, name 없는 행 스킵
"""
from __future__ import annotations

import logging
from typing import Any

import psycopg2.extras

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)

_LANGS = settings.supported_languages


class PlaceNormalizer:

    def run(self, source_name: str, batch_size: int = 5000) -> int:
        logger.info("[1-3] Normalizer 시작 | source=%s | batch=%d", source_name, batch_size)

        total = skip_count = error_count = 0

        with get_conn() as conn:
            while True:
                logger.info("[1-3] DB에서 대기 데이터 조회 중...")
                rows = self._fetch_pending(conn, source_name, batch_size)
                if not rows:
                    logger.info("[1-3] 대기 데이터 없음 — 완료")
                    break

                logger.info("[1-3] %d건 페치 완료 | 정규화 시작...", len(rows))

                try:
                    n, s = self._process_batch(conn, rows)
                    total       += n
                    skip_count  += s
                    conn.commit()
                    logger.info(
                        "[1-3] 배치 완료 | 이번 +%d | 누적 성공 %d | 누적 스킵 %d",
                        n, total, skip_count,
                    )
                except Exception as exc:
                    conn.rollback()
                    error_count += 1
                    logger.error("[1-3] ERROR: %s: %s", type(exc).__name__, exc)
                    # 에러 행들 error 상태로 마킹 후 중단
                    self._mark_all_error(conn, [r["id"] for r in rows])
                    conn.commit()
                    raise exc

        logger.info("[1-3] 완료 | 성공 %d | 스킵 %d | 에러 %d", total, skip_count, error_count)
        return total

    # ── 배치 처리 핵심 ────────────────────────────────────────────────────────

    def _process_batch(self, conn, rows: list[dict]) -> tuple[int, int]:
        """한 배치를 쿼리 5개로 처리. (처리 수, 스킵 수) 반환."""

        valid: list[dict] = []
        skip_ids: list[int] = []

        for row in rows:
            data    = row["raw_data"]
            name    = (data.get("name")    or data.get("title") or "").strip()
            address = (data.get("address") or data.get("addr1") or "").strip()
            if not name or not address:
                skip_ids.append(row["id"])
            else:
                valid.append(row)

        logger.info("[1-3]   유효 %d / 스킵 %d", len(valid), len(skip_ids))

        # ① 스킵 행 일괄 processed 처리
        if skip_ids:
            self._mark_all_processed(conn, skip_ids)

        if not valid:
            return 0, len(skip_ids)

        # ② core.places 배치 upsert → place_id 맵 반환
        logger.info("[1-3]   ② places upsert (%d건)...", len(valid))
        place_id_map = self._batch_upsert_places(conn, valid)

        # ③ place_source_ids 배치 insert
        logger.info("[1-3]   ③ source_ids insert...")
        self._batch_insert_source_ids(conn, valid, place_id_map)

        # ④ place_images 배치 insert
        logger.info("[1-3]   ④ images insert...")
        self._batch_insert_images(conn, valid, place_id_map)

        # ⑤ translation_fill_queue 배치 insert
        logger.info("[1-3]   ⑤ translation queue insert (%d건)...", len(valid) * len(_LANGS))
        self._batch_enqueue_translations(conn, valid, place_id_map)

        # ⑥ raw_documents processed 처리
        logger.info("[1-3]   ⑥ mark processed...")
        self._mark_all_processed(conn, [r["id"] for r in valid])

        return len(valid), len(skip_ids)

    # ── 배치 쿼리 ─────────────────────────────────────────────────────────────

    def _batch_upsert_places(
        self, conn, rows: list[dict]
    ) -> dict[tuple[str, str], int]:
        """core.places 배치 upsert. {(source_name, source_id): place_id} 반환."""
        data_list = []
        for row in rows:
            data        = row["raw_data"]
            source_name = row["source_name"]
            source_id   = row["source_id"]

            lat = self._safe_float(data.get("lat") or data.get("mapy"))
            lng = self._safe_float(data.get("lng") or data.get("mapx"))
            coords_wkt = (
                f"SRID=4326;POINT({lng} {lat})"
                if lat and lng and self._valid_wgs84(lat, lng)
                else None
            )
            name    = (data.get("name")    or data.get("title") or "").strip()
            address = (data.get("address") or data.get("addr1") or "").strip() or None
            quality = self._calc_quality(name, address, coords_wkt)

            data_list.append((
                source_name,
                source_id,
                name,
                address,
                (data.get("address_detail") or data.get("addr2") or "").strip() or None,
                coords_wkt,
                data.get("phone") or data.get("tel") or None,
                data.get("description") or data.get("overview") or None,
                data.get("source_category") or data.get("contenttypeid") or None,
                data.get("region_code") or data.get("areacode") or None,
                quality,
            ))

        cur = conn.cursor()
        results = psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO core.places
                (source_name, source_id, name, address, address_detail,
                 coords, phone, description, source_category, region_code,
                 quality_score, is_active)
            VALUES %s
            ON CONFLICT (source_name, source_id) DO UPDATE SET
                name            = EXCLUDED.name,
                address         = EXCLUDED.address,
                address_detail  = EXCLUDED.address_detail,
                coords          = EXCLUDED.coords,
                phone           = EXCLUDED.phone,
                description     = EXCLUDED.description,
                source_category = EXCLUDED.source_category,
                region_code     = EXCLUDED.region_code,
                quality_score   = EXCLUDED.quality_score,
                updated_at      = now()
            RETURNING place_id, source_name, source_id
            """,
            data_list,
            template="(%s,%s,%s,%s,%s,%s::geography,%s,%s,%s,%s,%s,TRUE)",
            fetch=True,
        )
        return {(r["source_name"], r["source_id"]): r["place_id"] for r in results}

    def _batch_insert_source_ids(
        self, conn, rows: list[dict], place_id_map: dict
    ) -> None:
        data_list = []
        for row in rows:
            key      = (row["source_name"], row["source_id"])
            place_id = place_id_map.get(key)
            if not place_id:
                continue
            data_list.append((
                place_id,
                row["source_name"],
                row["source_id"],
                self._detect_id_format(row["source_id"]),
            ))

        if not data_list:
            return
        cur = conn.cursor()
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO core.place_source_ids (place_id, source_name, source_id, id_format)
            VALUES %s
            ON CONFLICT (source_name, source_id) DO NOTHING
            """,
            data_list,
        )

    def _batch_insert_images(
        self, conn, rows: list[dict], place_id_map: dict
    ) -> None:
        data_list = []
        for row in rows:
            img_url = row["raw_data"].get("image_url") or row["raw_data"].get("firstimage")
            if not img_url:
                continue
            key      = (row["source_name"], row["source_id"])
            place_id = place_id_map.get(key)
            if place_id:
                data_list.append((place_id, img_url))

        if not data_list:
            return
        cur = conn.cursor()
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO core.place_images (place_id, original_url, is_primary, upload_status)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            [(pid, url, True, "pending") for pid, url in data_list],
        )

    def _batch_enqueue_translations(
        self, conn, rows: list[dict], place_id_map: dict
    ) -> None:
        data_list = []
        for row in rows:
            key      = (row["source_name"], row["source_id"])
            place_id = place_id_map.get(key)
            if not place_id:
                continue
            is_retrans   = row["sync_status"] == "modified"
            triggered_by = "update" if is_retrans else "new"
            for lang in _LANGS:
                data_list.append((place_id, lang, triggered_by, is_retrans))

        if not data_list:
            return
        cur = conn.cursor()
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO core.translation_fill_queue
                (place_id, lang, status, triggered_by, is_retranslation)
            VALUES %s
            ON CONFLICT (place_id, lang) DO UPDATE
              SET status           = 'pending',
                  triggered_by     = EXCLUDED.triggered_by,
                  is_retranslation = EXCLUDED.is_retranslation,
                  updated_at       = now()
            """,
            [(pid, lang, "pending", tb, ir) for pid, lang, tb, ir in data_list],
        )

    def _fetch_pending(self, conn, source_name: str, limit: int) -> list[dict]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, source_name, source_id, raw_data, sync_status
              FROM stage.raw_documents
             WHERE source_name = %s
               AND sync_status IN ('new', 'modified')
               AND processed_at IS NULL
             ORDER BY id ASC
             LIMIT %s
            """,
            (source_name, limit),
        )
        return list(cur.fetchall())

    def _mark_all_processed(self, conn, raw_ids: list[int]) -> None:
        if not raw_ids:
            return
        cur = conn.cursor()
        cur.execute(
            "UPDATE stage.raw_documents SET sync_status='processed', processed_at=now() WHERE id = ANY(%s)",
            (raw_ids,),
        )

    def _mark_all_error(self, conn, raw_ids: list[int]) -> None:
        if not raw_ids:
            return
        cur = conn.cursor()
        cur.execute(
            "UPDATE stage.raw_documents SET sync_status='error', processed_at=now() WHERE id = ANY(%s)",
            (raw_ids,),
        )

    # ── 유틸 ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_float(val: Any) -> float | None:
        try:
            return float(val) if val else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _valid_wgs84(lat: float, lng: float) -> bool:
        return 33.0 <= lat <= 38.9 and 124.0 <= lng <= 132.0

    @staticmethod
    def _calc_quality(name: str, address: str | None, coords: str | None) -> float:
        score = 0.0
        if name:    score += 0.4
        if address: score += 0.3
        if coords:  score += 0.3
        return round(score, 2)

    @staticmethod
    def _detect_id_format(source_id: str) -> str:
        if source_id.isdigit():                                return "numeric"
        if len(source_id) == 36 and source_id.count("-") == 4: return "uuid"
        if "_" in source_id:                                    return "prefixed"
        return "alpha_numeric"
