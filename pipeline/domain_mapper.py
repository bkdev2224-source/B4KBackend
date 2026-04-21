"""
Step 1-4a  Domain Mapper
  - domain_map.json 기반으로 core.places.display_domain 업데이트
  - 파이프라인 코드 변경 없이 JSON 수정만으로 도메인 추가/변경 가능
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

import psycopg2.extras

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)


class DomainMapper:
    """
    Usage:
        mapper = DomainMapper()
        mapper.run()                       # 전체 갱신
        mapper.run(source_name='tourapi')  # 특정 소스만
    """

    def __init__(self, map_path: Path | None = None):
        path = map_path or settings.domain_map_path
        with open(path, encoding="utf-8") as f:
            self._domain_map: dict[str, dict[str, list[str]]] = json.load(f)

    def run(self, source_name: str | None = None) -> int:
        """갱신된 레코드 수 반환."""
        total = 0
        with get_conn() as conn:
            for domain, sources in self._domain_map.items():
                for src, categories in sources.items():
                    if source_name and src != source_name:
                        continue
                    try:
                        cnt = self._update_domain(conn, domain, src, categories)
                        # ✅ 각 도메인/소스별로 성공 시 즉시 확정
                        conn.commit()  
                        if cnt > 0:
                            logger.info("Domain 매핑 완료: %s -> %s (%d건)", src, domain, cnt)
                        total += cnt
                    except Exception as exc:
                        # ✅ 에러 발생 시 해당 유닛만 롤백하고 계속 진행
                        conn.rollback()
                        logger.error("DomainMapper 오류 (domain=%s, src=%s): %s", domain, src, exc)

            # domain_map에 없는 source_category → display_domain = NULL
            if not source_name:
                try:
                    self._clear_unmapped(conn)
                    conn.commit()
                except Exception as exc:
                    conn.rollback()
                    logger.error("DomainMapper 클리어 오류: %s", exc)

        logger.info("DomainMapper 최종 완료: %d건 갱신", total)
        return total

    def _update_domain(
        self,
        conn,
        domain: str,
        source_name: str,
        categories: list[str],
    ) -> int:
        cur = conn.cursor()
        # tourapi uses content_type_id ("39","12" …); mois/mcst use category_code (업태구분명 etc.)
        if source_name == "tourapi":
            cur.execute(
                """
                UPDATE core.poi
                   SET display_domain = %s
                 WHERE source_ids ? %s
                   AND content_type_id = ANY(%s)
                   AND (display_domain IS DISTINCT FROM %s)
                """,
                (domain, source_name, categories, domain),
            )
        else:
            cur.execute(
                """
                UPDATE core.poi
                   SET display_domain = %s
                 WHERE source_ids ? %s
                   AND category_code = ANY(%s)
                   AND (display_domain IS DISTINCT FROM %s)
                """,
                (domain, source_name, categories, domain),
            )
        return cur.rowcount

    def _clear_unmapped(self, conn) -> None:
        """도메인 맵에 등록되지 않은 POI → display_domain = NULL."""
        all_pairs: list[tuple[str, str]] = []
        for domain, sources in self._domain_map.items():
            for src, cats in sources.items():
                for cat in cats:
                    all_pairs.append((src, cat))

        if not all_pairs:
            return

        cur = conn.cursor()
        psycopg2.extras.execute_values(
            cur,
            """
            UPDATE core.poi
               SET display_domain = NULL
             WHERE display_domain IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM (VALUES %s) AS v(src, cat)
                    WHERE (
                            (v.src = 'tourapi' AND source_ids ? v.src AND content_type_id = v.cat)
                         OR (v.src != 'tourapi' AND source_ids ? v.src AND category_code = v.cat)
                          )
               )
            """,
            all_pairs,
        )
        if cur.rowcount:
            logger.info("도메인 매핑 해제: %d건", cur.rowcount)
