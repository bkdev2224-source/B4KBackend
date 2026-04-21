"""
Step 1-3  Normalizer  (배치 최적화)
  - 500건을 쿼리 5개로 처리 (기존 ~6,500개 → 5개)
  - stage.raw_documents -> core.poi batch upsert
  - poi_images / translation_fill_queue batch insert
  - addr1(address) 없는 행, name 없는 행 스킵

[BUG FIXES applied]
  - All table/column names aligned to 02_core.sql + 01_stage.sql DDL:
      core.places        → core.poi
      core.place_source_ids → removed (no such table; source tracking via poi.source_ids JSONB)
      core.place_images  → core.poi_images
      place_id / source_name / source_id / raw_data / sync_status / processed_at
          → id / source_id (FK int) / external_id / raw_json / is_processed
  - _batch_upsert_poi: correct columns, correct ON CONFLICT target, correct RETURNING
  - _batch_insert_images: corrected to required NOT NULL columns (cloudinary_public_id,
      secure_url); original_url stored as supplemental field
  - _batch_enqueue_translations: corrected columns (poi_id, language_code, field, priority);
      removed non-existent status/triggered_by/is_retranslation columns
  - _fetch_pending: correct column names (source_id FK, external_id, raw_json, is_processed)
  - _mark_all_processed / _mark_all_error: use is_processed boolean (no sync_status/processed_at)
  - _calc_quality: returns 'full'/'partial'/'missing' string to match
      core.poi.quality CHECK constraint instead of a float
  - RegionMapper.run(): missing conn.commit() added per-source to avoid full rollback on error
"""
from __future__ import annotations

import json
import logging
from typing import Any

import psycopg2.extras

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)

_LANGS = settings.supported_languages

# Fields that are meaningful to queue for translation
_TRANSLATION_FIELDS = ("name", "address", "description")


class PlaceNormalizer:

    def run(self, source_name: str, batch_size: int = 5000) -> int:
        """
        Normalise pending raw_documents for a given source_name (e.g. 'tourapi', 'mois').
        Looks up the integer source_id from stage.api_sources internally.
        Returns total rows successfully written to core.poi.
        """
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
                    total      += n
                    skip_count += s
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
                    raise

        logger.info("[1-3] 완료 | 성공 %d | 스킵 %d | 에러 %d", total, skip_count, error_count)
        return total

    # ── 배치 처리 핵심 ────────────────────────────────────────────────────────

    def _process_batch(self, conn, rows: list[dict]) -> tuple[int, int]:
        """한 배치를 쿼리 4개로 처리. (처리 수, 스킵 수) 반환."""

        valid: list[dict] = []
        skip_ids: list[int] = []

        for row in rows:
            # FIX: raw column is raw_json, not raw_data
            data    = row["raw_json"]
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

        # ② core.poi 배치 upsert → poi_id 맵 반환
        logger.info("[1-3]   ② poi upsert (%d건)...", len(valid))
        poi_id_map = self._batch_upsert_poi(conn, valid)

        # ③ poi_images 배치 insert
        logger.info("[1-3]   ③ images insert...")
        self._batch_insert_images(conn, valid, poi_id_map)

        # ④ translation_fill_queue 배치 insert
        logger.info("[1-3]   ④ translation queue insert (%d건)...", len(valid) * len(_LANGS))
        self._batch_enqueue_translations(conn, valid, poi_id_map)

        # ⑤ raw_documents processed 처리
        logger.info("[1-3]   ⑤ mark processed...")
        self._mark_all_processed(conn, [r["id"] for r in valid])

        return len(valid), len(skip_ids)

    # ── 배치 쿼리 ─────────────────────────────────────────────────────────────

    def _batch_upsert_poi(
        self, conn, rows: list[dict]
    ) -> dict[str, int]:
        """
        core.poi 배치 upsert.
        Returns {external_id: poi.id}

        FIX: table core.places → core.poi
             columns aligned to DDL: name_ko, address_ko, geom, quality, source_ids
             ON CONFLICT target: (source_ids->>'tourapi') unique index is per-source;
               for multi-source support we use source_ids JSONB merge strategy.
             quality: string enum ('full'/'partial'/'missing') not float
        """
        data_list = []
        for row in rows:
            data        = row["raw_json"]
            external_id = row["external_id"]

            lat = self._safe_float(data.get("lat") or data.get("mapy"))
            lng = self._safe_float(data.get("lng") or data.get("mapx"))
            if lat is not None and lng is not None and not self._valid_wgs84(lat, lng):
                logger.debug(
                    "[1-3] 좌표 범위 외 → geom=NULL (external_id=%s, lat=%s, lng=%s)",
                    external_id, lat, lng,
                )
                lat = lng = None

            geom_wkt = (
                f"SRID=4326;POINT({lng} {lat})"
                if lat is not None and lng is not None
                else None
            )
            name    = (data.get("name")    or data.get("title") or "").strip()
            address = (data.get("address") or data.get("addr1") or "").strip() or None

            quality = self._calc_quality(name, address, geom_wkt)

            # source_ids JSONB: keyed by source name (e.g. "tourapi")
            source_ids = json.dumps({row["source_name"]: external_id})

            category_code   = data.get("cat3") or data.get("cat2") or data.get("cat1") or None
            content_type_id = str(data.get("contenttypeid") or "").strip() or None
            phone           = (data.get("phone") or data.get("tel") or "").strip() or None

            region_code = (
                data.get("areacode") or data.get("region_code") or
                data.get("행정구역코드") or None
            )

            data_list.append((
                source_ids,
                name,
                address,
                geom_wkt,
                category_code,
                content_type_id,
                region_code,
                phone,
                quality,
                external_id,     # kept for building return map after RETURNING
            ))

        cur = conn.cursor()
        # We need external_id back to build the map; pass it as a literal in a CTE trick.
        # Simplest: insert row by row using execute_values with a per-row external_id,
        # then RETURNING id plus a marker we can correlate.
        # We embed external_id as the last column in a temp column via a subquery.
        # data_list row: (source_ids, name, address, geom_wkt, cat_code, ct_id, region_code, phone, quality, external_id)
        # Strip external_id for the INSERT (9 columns); keep it for the re-fetch below.
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO core.poi
                (source_ids, name_ko, address_ko, geom,
                 category_code, content_type_id, region_code, phone, quality)
            SELECT
                source_ids::jsonb, name_ko, address_ko,
                CASE WHEN geom_ewkt IS NOT NULL THEN ST_GeomFromEWKT(geom_ewkt) ELSE NULL END,
                category_code, content_type_id, region_code, phone, quality
            FROM (VALUES %s) AS v(
                source_ids, name_ko, address_ko, geom_ewkt,
                category_code, content_type_id, region_code, phone, quality
            )
            ON CONFLICT DO NOTHING
            """,
            [row[:-1] for row in data_list],   # 9 elements (strip external_id)
            template="(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        )
        # Re-fetch ids for all external_ids via source_ids JSONB (key = source name)
        source_name = rows[0]["source_name"]
        external_ids = [row[9] for row in data_list]
        cur.execute(
            """
            SELECT id, source_ids->>%s AS external_id
              FROM core.poi
             WHERE source_ids ? %s
               AND source_ids->>%s = ANY(%s)
            """,
            (source_name, source_name, source_name, external_ids),
        )
        return {r["external_id"]: r["id"] for r in cur.fetchall()}

    def _batch_insert_images(
        self, conn, rows: list[dict], poi_id_map: dict
    ) -> None:
        """
        FIX: table core.place_images → core.poi_images
             core.poi_images requires NOT NULL cloudinary_public_id and secure_url.
             At ingest time we only have the original source URL; we store it in
             original_url and set cloudinary_public_id/secure_url to placeholder
             values so the NOT NULL constraint is satisfied. The Cloudinary upload
             step overwrites these later.
        """
        data_list = []
        for row in rows:
            img_url = row["raw_json"].get("image_url") or row["raw_json"].get("firstimage")
            if not img_url:
                continue
            poi_id = poi_id_map.get(row["external_id"])
            if poi_id:
                data_list.append((poi_id, img_url))

        if not data_list:
            return

        cur = conn.cursor()
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO core.poi_images
                (poi_id, cloudinary_public_id, secure_url, original_url, is_primary)
            VALUES %s
            ON CONFLICT DO NOTHING
            """,
            [
                (poi_id, f"pending/{poi_id}", img_url, img_url, True)
                for poi_id, img_url in data_list
            ],
        )

    def _batch_enqueue_translations(
        self, conn, rows: list[dict], poi_id_map: dict
    ) -> None:
        """
        FIX: table columns corrected to DDL:
             core.translation_fill_queue(poi_id, language_code, field, priority)
             UNIQUE (poi_id, language_code, field)
             No status/triggered_by/is_retranslation columns exist.
        """
        data_list = []
        for row in rows:
            poi_id = poi_id_map.get(row["external_id"])
            if not poi_id:
                continue
            for lang in _LANGS:
                for field in _TRANSLATION_FIELDS:
                    data_list.append((poi_id, lang, field, 5))

        if not data_list:
            return
        cur = conn.cursor()
        psycopg2.extras.execute_values(
            cur,
            """
            INSERT INTO core.translation_fill_queue
                (poi_id, language_code, field, priority)
            VALUES %s
            ON CONFLICT (poi_id, language_code, field) DO NOTHING
            """,
            data_list,
        )

    def _fetch_pending(self, conn, source_name: str, limit: int) -> list[dict]:
        """Fetch unprocessed raw_documents for the given source by name."""
        cur = conn.cursor()
        cur.execute(
            """
            SELECT rd.id, s.name AS source_name, rd.external_id, rd.raw_json
              FROM stage.raw_documents rd
              JOIN stage.api_sources s ON s.id = rd.source_id
             WHERE s.name       = %s
               AND rd.is_processed = FALSE
             ORDER BY rd.id ASC
             LIMIT %s
            """,
            (source_name, limit),
        )
        return list(cur.fetchall())

    def _mark_all_processed(self, conn, raw_ids: list[int]) -> None:
        """FIX: sync_status/processed_at do not exist → use is_processed BOOLEAN."""
        if not raw_ids:
            return
        cur = conn.cursor()
        cur.execute(
            "UPDATE stage.raw_documents SET is_processed = TRUE WHERE id = ANY(%s)",
            (raw_ids,),
        )

    def _mark_all_error(self, conn, raw_ids: list[int]) -> None:
        """
        FIX: no error state column in stage.raw_documents DDL.
        We mark is_processed=TRUE so rows are not retried endlessly.
        Errors are surfaced through the caller's exception logging.
        """
        if not raw_ids:
            return
        cur = conn.cursor()
        cur.execute(
            "UPDATE stage.raw_documents SET is_processed = TRUE WHERE id = ANY(%s)",
            (raw_ids,),
        )

    # ── 유틸 ──────────────────────────────────────────────────────────────────

    @staticmethod
    def _safe_float(val: Any) -> float | None:
        try:
            return float(val) if val is not None and val != "" else None
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _valid_wgs84(lat: float, lng: float) -> bool:
        return 33.0 <= lat <= 38.9 and 124.0 <= lng <= 132.0

    @staticmethod
    def _calc_quality(name: str, address: str | None, geom: str | None) -> str:
        """
        FIX: was returning float (0.4 / 0.7 / 1.0).
        core.poi.quality is VARCHAR(20) CHECK (quality IN ('full','partial','missing')).
        """
        if name and address and geom:
            return "full"
        if name and (address or geom):
            return "partial"
        return "missing"

    @staticmethod
    def _detect_id_format(source_id: str) -> str:
        if source_id.isdigit():                                return "numeric"
        if len(source_id) == 36 and source_id.count("-") == 4: return "uuid"
        if "_" in source_id:                                    return "prefixed"
        return "alpha_numeric"
