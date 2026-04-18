"""
Step 1-4b  Region Mapper
  - region_map.json 기반으로 core.places.display_region 업데이트
  - 소스별 지역코드(areaCode, 행정구역명 등)를 통합 관리
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)


class RegionMapper:
    """
    Usage:
        mapper = RegionMapper()
        mapper.run()
        mapper.run(source_name='tourapi')
    """

    def __init__(self, map_path: Path | None = None):
        path = map_path or settings.region_map_path
        with open(path, encoding="utf-8") as f:
            raw: dict[str, dict] = json.load(f)

        # 소스별 코드 → (region_code, display_ko) 역방향 인덱스 구성
        # { source_name: { raw_code: (region_code, ko_name) } }
        self._index: dict[str, dict[str, tuple[str, str]]] = {}
        for region_code, info in raw.items():
            ko = info.get("ko", "")
            for key, val in info.items():
                if key.endswith("_code"):
                    source = key.replace("_code", "")  # 'mois', 'mcst'
                    self._index.setdefault(source, {})[val] = (region_code, ko)
            # tourapi: areaCode는 region_code 자체
            self._index.setdefault("tourapi", {})[region_code] = (region_code, ko)

        self._region_code_to_ko: dict[str, str] = {
            rc: info["ko"] for rc, info in raw.items() if "ko" in info
        }

    def run(self, source_name: str | None = None) -> int:
        """갱신된 레코드 수 반환."""
        total = 0
        with get_conn() as conn:
            sources = [source_name] if source_name else list(self._index.keys())
            for src in sources:
                mapping = self._index.get(src, {})
                for raw_code, (region_code, ko) in mapping.items():
                    cnt = self._update_region(conn, src, raw_code, ko)
                    total += cnt

        logger.info("RegionMapper 완료: %d건 갱신", total)
        return total

    def _update_region(
        self,
        conn,
        source_name: str,
        region_code: str,
        display_region: str,
    ) -> int:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE core.places
               SET display_region = %s
             WHERE source_name = %s
               AND region_code  = %s
               AND (display_region IS DISTINCT FROM %s)
            """,
            (display_region, source_name, region_code, display_region),
        )
        return cur.rowcount
