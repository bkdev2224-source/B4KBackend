"""
Step 2-4  Dedup 3단계 앙상블 (공통 파이프라인 — Phase 2부터 사용)
  0. 좌표 null → 신규 INSERT
  1. PostGIS 공간 필터 — 반경 50m 이내 후보 추출
  2. 상호명 가중 앙상블 (임계값 0.82)
       Jaro-Winkler  × 0.40
       Token Sort Ratio × 0.35
       자모분리 Levenshtein × 0.25
  3. 소스 우선순위 병합
       0.92+ → 자동 병합
       0.82~0.92 → review 큐
       <0.82 → 신규 INSERT
"""
from __future__ import annotations

import logging
import re
from typing import Any

from fuzzywuzzy import fuzz
from jaro import jaro_winkler_metric

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)

# 소스 우선순위 (높을수록 우선)
SOURCE_PRIORITY = {"tourapi": 3, "mcst": 2, "mois": 1, "crawl": 0}


class DedupEnsemble:
    """
    Usage:
        dedup = DedupEnsemble()
        dedup.run(source_name='mois')   # 새 소스 추가 시
    """

    def run(self, source_name: str) -> dict[str, int]:
        results = {"merged": 0, "review": 0, "inserted": 0}
        pending = self._fetch_unprocessed(source_name)

        for raw in pending:
            data = raw["raw_data"]
            lat  = self._safe_float(data.get("lat") or data.get("mapy"))
            lng  = self._safe_float(data.get("lng") or data.get("mapx"))
            name = (data.get("name") or data.get("title") or "").strip()

            # Step 0: 좌표 없음 → 신규
            if not lat or not lng:
                self._insert_new(raw, source_name)
                results["inserted"] += 1
                continue

            # Step 1: 공간 필터
            candidates = self._spatial_candidates(lat, lng)
            if not candidates:
                self._insert_new(raw, source_name)
                results["inserted"] += 1
                continue

            # Step 2: 앙상블 스코어
            best_score, best_candidate = self._score_candidates(name, candidates)

            # Step 3: 분기
            if best_score >= settings.dedup_auto_merge_threshold:
                self._merge(raw, best_candidate, source_name)
                results["merged"] += 1
            elif best_score >= settings.dedup_review_threshold:
                self._queue_review(raw, best_candidate, best_score, source_name)
                results["review"] += 1
            else:
                self._insert_new(raw, source_name)
                results["inserted"] += 1

        logger.info("Dedup 완료 (source=%s): %s", source_name, results)
        return results

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _fetch_unprocessed(self, source_name: str) -> list[dict]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT id, source_name, source_id, raw_data, sync_status
                  FROM stage.raw_documents
                 WHERE source_name = %s
                   AND sync_status IN ('new', 'modified')
                   AND processed_at IS NULL
                """,
                (source_name,),
            )
            return list(cur.fetchall())

    def _spatial_candidates(self, lat: float, lng: float) -> list[dict]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT place_id, name, source_name, coords
                  FROM core.places
                 WHERE ST_DWithin(
                         coords,
                         ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                         %s
                       )
                   AND is_active = TRUE
                """,
                (lng, lat, settings.dedup_spatial_radius_m),
            )
            return list(cur.fetchall())

    def _score_candidates(
        self, name: str, candidates: list[dict]
    ) -> tuple[float, dict]:
        best_score = 0.0
        best = candidates[0]
        normalized_name = self._normalize_name(name)

        for cand in candidates:
            cand_name = self._normalize_name(cand["name"] or "")
            score = self._ensemble_score(normalized_name, cand_name)
            if score > best_score:
                best_score = score
                best = cand

        return best_score, best

    @staticmethod
    def _ensemble_score(a: str, b: str) -> float:
        if not a or not b:
            return 0.0
        jw   = jaro_winkler_metric(a, b)
        tsr  = fuzz.token_sort_ratio(a, b) / 100.0
        jamo_a = DedupEnsemble._to_jamo(a)
        jamo_b = DedupEnsemble._to_jamo(b)
        lev  = 1.0 - (
            _levenshtein(jamo_a, jamo_b) / max(len(jamo_a), len(jamo_b), 1)
        )
        return jw * 0.40 + tsr * 0.35 + lev * 0.25

    @staticmethod
    def _normalize_name(name: str) -> str:
        name = re.sub(r"[^\w가-힣]", " ", name).lower().strip()
        return re.sub(r"\s+", " ", name)

    @staticmethod
    def _to_jamo(text: str) -> str:
        """한글을 자모 분리해 문자열로 변환 (간단 구현)."""
        CHOSUNG = list("ㄱㄲㄴㄷㄸㄹㅁㅂㅃㅅㅆㅇㅈㅉㅊㅋㅌㅍㅎ")
        JUNGSUNG = list("ㅏㅐㅑㅒㅓㅔㅕㅖㅗㅘㅙㅚㅛㅜㅝㅞㅟㅠㅡㅢㅣ")
        JONGSUNG = list(" ㄱㄲㄳㄴㄵㄶㄷㄹㄺㄻㄼㄽㄾㄿㅀㅁㅂㅄㅅㅆㅇㅈㅊㅋㅌㅍㅎ")
        result = []
        for ch in text:
            code = ord(ch)
            if 0xAC00 <= code <= 0xD7A3:
                offset = code - 0xAC00
                result.append(CHOSUNG[offset // 588])
                result.append(JUNGSUNG[(offset % 588) // 28])
                jong = JONGSUNG[offset % 28]
                if jong != " ":
                    result.append(jong)
            else:
                result.append(ch)
        return "".join(result)

    @staticmethod
    def _safe_float(val: Any) -> float | None:
        try:
            return float(val) if val else None
        except (ValueError, TypeError):
            return None

    def _merge(self, raw: dict, candidate: dict, source_name: str) -> None:
        place_id  = candidate["place_id"]
        source_id = raw["source_id"]
        with get_conn() as conn:
            cur = conn.cursor()
            # place_source_ids에 새 소스 연결
            cur.execute(
                """
                INSERT INTO core.place_source_ids (place_id, source_name, source_id)
                VALUES (%s, %s, %s) ON CONFLICT DO NOTHING
                """,
                (place_id, source_name, source_id),
            )
            # 보완 필드 업데이트 (우선순위 낮은 소스는 빈 필드만 채움)
            data = raw["raw_data"]
            if SOURCE_PRIORITY.get(source_name, 0) < SOURCE_PRIORITY.get(candidate["source_name"], 0):
                cur.execute(
                    """
                    UPDATE core.places
                       SET phone       = COALESCE(phone,       %s),
                           description = COALESCE(description, %s),
                           updated_at  = now()
                     WHERE place_id = %s
                    """,
                    (data.get("phone"), data.get("description"), place_id),
                )
            cur.execute(
                "UPDATE stage.raw_documents SET sync_status='processed', processed_at=now() WHERE id=%s",
                (raw["id"],),
            )

    def _queue_review(
        self, raw: dict, candidate: dict, score: float, source_name: str
    ) -> None:
        import json as _json
        review_meta = {
            "candidate_place_id": candidate["place_id"],
            "candidate_name":     candidate.get("name"),
            "raw_source_name":    source_name,
            "raw_source_id":      raw["source_id"],
            "raw_name":           (raw["raw_data"].get("name") or raw["raw_data"].get("title") or ""),
            "raw_address":        (raw["raw_data"].get("address") or raw["raw_data"].get("addr1") or ""),
            "score":              round(score, 4),
        }
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE core.places
                   SET dedup_status = 'review',
                       dedup_review_meta = %s
                 WHERE place_id = %s
                """,
                (_json.dumps(review_meta, ensure_ascii=False), candidate["place_id"]),
            )
            cur.execute(
                "UPDATE stage.raw_documents SET sync_status='review', processed_at=now() WHERE id=%s",
                (raw["id"],),
            )
        logger.info(
            "Review 큐 등록: source_id=%s ↔ place_id=%s (score=%.3f)",
            raw["source_id"], candidate["place_id"], score,
        )

    def _insert_new(self, raw: dict, source_name: str) -> None:
        """normalizer에 위임 — raw_status를 'new'로 되돌려 normalizer가 처리하게 함."""
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "UPDATE stage.raw_documents SET sync_status='new', processed_at=NULL WHERE id=%s",
                (raw["id"],),
            )


def _levenshtein(a: str, b: str) -> int:
    if len(a) < len(b):
        a, b = b, a
    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, 1):
        curr = [i]
        for j, cb in enumerate(b, 1):
            curr.append(min(prev[j] + 1, curr[-1] + 1, prev[j - 1] + (ca != cb)))
        prev = curr
    return prev[-1]
