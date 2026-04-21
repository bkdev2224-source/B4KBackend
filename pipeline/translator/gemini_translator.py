"""
Gemini 번역기 — en / ja / th / pt-BR (name · description)
  - 토큰 예산 기반 다중 place 배치: 1 API 호출에 N개 place 동시 번역
    → 시스템 프롬프트 오버헤드를 호출당 1회로 최소화
  - 언어별 시스템 프롬프트에 DB 번역 규칙 주입 (core.translation_rules)
  - 언어별 청크를 ThreadPoolExecutor로 병렬 처리
  - 멱등성: ON CONFLICT (place_id, lang) DO UPDATE
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import google.generativeai as genai

from config.settings import settings
from database.db import get_conn
from pipeline.translator._utils import load_prompt_additions, split_by_token_budget

logger = logging.getLogger(__name__)

_GEMINI_LANGS = {"en", "ja", "th", "pt-BR"}
_MAX_WORKERS  = 20

LANG_NAMES = {
    "en":    "English",
    "ja":    "Japanese",
    "th":    "Thai",
    "pt-BR": "Brazilian Portuguese",
}

_BASE_SYSTEM = (
    "You are a professional translator specializing in K-culture tourism content. "
    "You receive a JSON object where each key is a place ID (integer string) and each value "
    "contains fields to translate (name, description). "
    "Translate name and description accurately and naturally into the target language. "
    "Preserve proper nouns (brand names, place names) where appropriate. "
    "Return ONLY a JSON object with the same place ID keys and the same field structure."
)

genai.configure(api_key=settings.gemini_api_key)


class GeminiBatchTranslator:
    """
    Usage:
        count = GeminiBatchTranslator().run()
    """

    def run(self) -> int:
        rows = self._fetch_pending()
        if not rows:
            logger.info("[Gemini] 번역 대기 항목 없음")
            return 0

        logger.info("[Gemini] 번역 시작: %d건 (%s)", len(rows), "/".join(_GEMINI_LANGS))

        # 언어별 그룹화
        by_lang: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            by_lang[row["lang"]].append(row)

        # 언어별 모델 생성 (시스템 프롬프트에 규칙 주입) + 토큰 청크 분할
        work_items: list[tuple[str, list[dict], genai.GenerativeModel]] = []
        for lang, lang_rows in by_lang.items():
            rules_text = load_prompt_additions(lang)
            model = genai.GenerativeModel(
                model_name=settings.gemini_translation_model,
                system_instruction=_BASE_SYSTEM + rules_text,
                generation_config=genai.GenerationConfig(
                    response_mime_type="application/json",
                ),
            )
            for chunk in split_by_token_budget(lang_rows, settings.translation_token_budget):
                work_items.append((lang, chunk, model))

        logger.info("[Gemini] 청크 %d개 병렬 처리 (max_workers=%d)", len(work_items), _MAX_WORKERS)

        all_results: list[tuple[dict, dict | None]] = []
        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {
                pool.submit(self._translate_chunk, lang, chunk, model): (lang, chunk)
                for lang, chunk, model in work_items
            }
            for future in as_completed(futures):
                lang, chunk = futures[future]
                try:
                    all_results.extend(future.result())
                except Exception as exc:
                    logger.error("[Gemini] 청크 오류 lang=%s chunk=%d건: %s", lang, len(chunk), exc)
                    all_results.extend((row, None) for row in chunk)

        success = self._save_results(all_results)
        logger.info("[Gemini] 완료: %d / %d 건 성공", success, len(rows))
        return success

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _fetch_pending(self) -> list[dict]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT q.id AS queue_id, q.place_id, q.lang, q.is_retranslation,
                       p.name, p.description
                  FROM core.translation_fill_queue q
                  JOIN core.places p ON p.place_id = q.place_id
                 WHERE q.status = 'pending'
                   AND q.lang = ANY(%s)
                 ORDER BY q.lang, q.id
                 LIMIT %s
                """,
                (list(_GEMINI_LANGS), settings.translation_batch_size),
            )
            return list(cur.fetchall())

    def _translate_chunk(
        self,
        lang: str,
        rows: list[dict],
        model: genai.GenerativeModel,
    ) -> list[tuple[dict, dict | None]]:
        """
        여러 place를 하나의 API 호출로 번역.
        입력: {"place_id": {"name": ..., "description": ...}, ...}
        출력: 동일 구조의 번역 결과
        """
        batch_input: dict[str, dict] = {}
        for row in rows:
            fields: dict[str, str] = {}
            if row["name"]:
                fields["name"] = row["name"]
            if row["description"]:
                fields["description"] = row["description"]
            if fields:
                batch_input[str(row["place_id"])] = fields

        if not batch_input:
            return [(row, {}) for row in rows]

        prompt = (
            f"Translate all fields into {LANG_NAMES[lang]}.\n\n"
            f"Place data (keys are place IDs — do not translate or modify the keys):\n"
            f"{json.dumps(batch_input, ensure_ascii=False)}\n\n"
            f"Return a JSON object using the exact same place IDs as keys."
        )

        response = model.generate_content(prompt)
        translated_batch: dict[str, dict] = json.loads(response.text)

        return [
            (row, translated_batch.get(str(row["place_id"])))
            for row in rows
        ]

    def _save_results(self, results: list[tuple[dict, dict | None]]) -> int:
        success = 0
        with get_conn() as conn:
            for row, translated in results:
                if not translated:
                    self._mark_error(conn, row["queue_id"])
                    continue
                try:
                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO core.place_translations
                            (place_id, lang, name, description, model_used, is_retranslation)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (place_id, lang) DO UPDATE
                          SET name             = EXCLUDED.name,
                              description      = EXCLUDED.description,
                              model_used       = EXCLUDED.model_used,
                              is_retranslation = EXCLUDED.is_retranslation,
                              translated_at    = now()
                        """,
                        (
                            row["place_id"], row["lang"],
                            translated.get("name"),
                            translated.get("description"),
                            settings.gemini_translation_model,
                            row["is_retranslation"],
                        ),
                    )
                    cur.execute(
                        """
                        UPDATE core.translation_fill_queue
                           SET status = 'completed', provider = 'gemini', updated_at = now()
                         WHERE id = %s
                        """,
                        (row["queue_id"],),
                    )
                    success += 1
                except Exception as exc:
                    logger.error("[Gemini] 저장 오류 place_id=%d: %s", row["place_id"], exc)
                    self._mark_error(conn, row["queue_id"])
        return success

    @staticmethod
    def _mark_error(conn, queue_id: int) -> None:
        cur = conn.cursor()
        cur.execute(
            "UPDATE core.translation_fill_queue SET status='error', updated_at=now() WHERE id=%s",
            (queue_id,),
        )
