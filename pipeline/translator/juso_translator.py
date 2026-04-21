"""
주소정보누리집 API — 도로명 주소 한→영 변환
  - core.places.address (한국어 도로명) → place_translations.address (lang='en')
  - 이미 영문 주소가 있는 place는 건너뜀
  - 멱등성: ON CONFLICT (place_id, lang) DO UPDATE (address만 갱신)
"""
from __future__ import annotations

import logging
import time

import httpx

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)

_JUSO_TIMEOUT = 10.0
_BATCH_SIZE   = 100


class JusoAddressTranslator:
    """
    Usage:
        count = JusoAddressTranslator().run()
    """

    def run(self) -> int:
        pending = self._fetch_pending()
        if not pending:
            logger.info("[Juso] 번역 대기 주소 없음")
            return 0

        logger.info("[Juso] 도로명 주소 영문 변환 시작: %d건", len(pending))
        success = 0

        for row in pending:
            en_address = self._translate(row["address"])
            if en_address:
                self._upsert_en_address(row["place_id"], en_address)
                success += 1
            else:
                logger.warning("[Juso] 변환 실패 place_id=%d address=%s", row["place_id"], row["address"][:40])
            time.sleep(0.05)  # API 부하 방지

        logger.info("[Juso] 완료: %d / %d 건 성공", success, len(pending))
        return success

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _fetch_pending(self) -> list[dict]:
        """영문 주소가 없는 poi 목록 조회."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT p.id AS place_id, p.address_ko AS address
                  FROM core.poi p
                 WHERE p.address_ko IS NOT NULL
                   AND p.is_active = TRUE
                   AND NOT EXISTS (
                       SELECT 1
                         FROM core.poi_translations t
                        WHERE t.poi_id = p.id
                          AND t.language_code = 'en'
                          AND t.address IS NOT NULL
                   )
                 LIMIT %s
                """,
                (_BATCH_SIZE,),
            )
            return list(cur.fetchall())

    def _translate(self, address: str) -> str | None:
        """주소정보누리집 API로 한국어 도로명주소 → 영문 변환."""
        try:
            resp = httpx.get(
                settings.juso_api_url,
                params={
                    "currentPage": 1,
                    "countPerPage": 1,
                    "keyword": address,
                    "confmKey": settings.juso_api_key,
                    "resultType": "json",
                },
                timeout=_JUSO_TIMEOUT,
            )
            resp.raise_for_status()
            data = resp.json()
            items = data.get("results", {}).get("juso", [])
            if items:
                item = items[0]
                # 영문 주소: 도로명(영문) + 건물번호
                en_road   = item.get("engRoadAddr", "")
                en_jibun  = item.get("engJibunAddr", "")
                return en_road or en_jibun or None
        except Exception as exc:
            logger.error("[Juso] API 오류 (address=%s): %s", address[:40], exc)
        return None

    def _upsert_en_address(self, place_id: int, en_address: str) -> None:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO core.poi_translations (poi_id, language_code, address, source)
                VALUES (%s, 'en', %s, 'juso')
                ON CONFLICT (poi_id, language_code) DO UPDATE
                  SET address    = EXCLUDED.address,
                      source     = EXCLUDED.source,
                      updated_at = now()
                """,
                (place_id, en_address),
            )
