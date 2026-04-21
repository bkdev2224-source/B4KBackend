"""
번역 파이프라인 오케스트레이터
  ① 주소 (한→영): 주소정보누리집 API  → place_translations.address (lang='en')
  ② zh-CN/zh-TW : DeepSeek           → place_translations (name, description)
  ③ en/ja/th    : Gemini             → place_translations (name, description)
  ④ 엔티티 번역  : DeepSeek + Gemini  → entity_translations (name, description)
  ⑤ 스냅샷 갱신  : DB 트리거가 자동 처리

  도로명 주소는 ko·en만 보유. ja/zh-CN/zh-TW/th는 address 번역하지 않음.
"""
from __future__ import annotations

import json
import logging
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

import google.generativeai as genai
from openai import OpenAI

from config.settings import settings
from database.db import get_conn
from pipeline.translator._utils import load_prompt_additions, split_by_token_budget
from pipeline.translator.deepseek_translator import DeepSeekTranslator
from pipeline.translator.gemini_translator import GeminiBatchTranslator
from pipeline.translator.juso_translator import JusoAddressTranslator

logger = logging.getLogger(__name__)

_DEEPSEEK_LANGS = {"zh-CN", "zh-TW"}
_GEMINI_LANGS   = {"en", "ja", "th", "pt-BR"}

_LANG_NAMES = {
    "zh-CN": "Simplified Chinese",
    "zh-TW": "Traditional Chinese",
    "en":    "English",
    "ja":    "Japanese",
    "th":    "Thai",
    "pt-BR": "Brazilian Portuguese",
}

_ENTITY_SYSTEM = (
    "You are a professional translator specializing in K-culture content. "
    "You receive a JSON object where each key is an entity ID (integer string) and each value "
    "contains fields to translate (name, description). "
    "Translate name and description accurately and naturally into the target language. "
    "Preserve proper nouns, artist names, and brand names where appropriate. "
    "Return ONLY a JSON object with the same entity ID keys and the same field structure."
)


class EntityTranslationRunner:
    """
    entity_translation_queue → entity_translations 번역 처리기.
    DeepSeek(zh-CN/zh-TW) + Gemini(en/ja/th/pt-BR) 를 동일 청크 패턴으로 실행.

    Usage:
        count = EntityTranslationRunner().run()
    """

    _MAX_WORKERS = 10

    def run(self) -> int:
        rows = self._fetch_pending()
        if not rows:
            logger.info("[EntityTranslation] 번역 대기 항목 없음")
            return 0

        logger.info("[EntityTranslation] 번역 시작: %d건", len(rows))

        by_lang: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            by_lang[row["language_code"]].append(row)

        work_items: list[tuple[str, list[dict], str]] = []
        for lang, lang_rows in by_lang.items():
            rules_text = load_prompt_additions(lang)
            for chunk in split_by_token_budget(lang_rows, settings.translation_token_budget):
                work_items.append((lang, chunk, _ENTITY_SYSTEM + rules_text))

        all_results: list[tuple[dict, dict | None]] = []
        with ThreadPoolExecutor(max_workers=self._MAX_WORKERS) as pool:
            futures = {
                pool.submit(self._translate_chunk, lang, chunk, system_prompt): (lang, chunk)
                for lang, chunk, system_prompt in work_items
            }
            for future in as_completed(futures):
                lang, chunk = futures[future]
                try:
                    all_results.extend(future.result())
                except Exception as exc:
                    logger.error("[EntityTranslation] 청크 오류 lang=%s chunk=%d건: %s", lang, len(chunk), exc)
                    all_results.extend((row, None) for row in chunk)

        success = self._save_results(all_results)
        logger.info("[EntityTranslation] 완료: %d / %d 건 성공", success, len(rows))
        return success

    def _fetch_pending(self) -> list[dict]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT q.id AS queue_id, q.entity_id, q.language_code, q.is_retranslation,
                       e.canonical_name AS name, e.description_ko AS description
                  FROM core.entity_translation_queue q
                  JOIN core.entities e ON e.id = q.entity_id
                 WHERE q.status = 'pending'
                 ORDER BY q.language_code, q.id
                 LIMIT %s
                """,
                (settings.translation_batch_size,),
            )
            return list(cur.fetchall())

    def _translate_chunk(
        self,
        lang: str,
        rows: list[dict],
        system_prompt: str,
    ) -> list[tuple[dict, dict | None]]:
        batch_input: dict[str, dict] = {}
        for row in rows:
            fields: dict[str, str] = {}
            if row["name"]:
                fields["name"] = row["name"]
            if row["description"]:
                fields["description"] = row["description"]
            if fields:
                batch_input[str(row["entity_id"])] = fields

        if not batch_input:
            return [(row, {}) for row in rows]

        user_prompt = (
            f"Translate all fields into {_LANG_NAMES[lang]}.\n\n"
            f"Entity data (keys are entity IDs — do not translate or modify the keys):\n"
            f"{json.dumps(batch_input, ensure_ascii=False)}\n\n"
            f"Return a JSON object using the exact same entity IDs as keys."
        )

        if lang in _DEEPSEEK_LANGS:
            client = OpenAI(
                api_key=settings.deepseek_api_key,
                base_url=settings.deepseek_base_url,
            )
            response = client.chat.completions.create(
                model=settings.deepseek_translation_model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user",   "content": user_prompt},
                ],
                response_format={"type": "json_object"},
            )
            translated_batch: dict[str, dict] = json.loads(
                response.choices[0].message.content or "{}"
            )
        else:
            model = genai.GenerativeModel(
                model_name=settings.gemini_translation_model,
                system_instruction=system_prompt,
                generation_config=genai.GenerationConfig(
                    response_mime_type="application/json",
                ),
            )
            response = model.generate_content(user_prompt)
            translated_batch = json.loads(response.text)

        return [
            (row, translated_batch.get(str(row["entity_id"])))
            for row in rows
        ]

    def _save_results(self, results: list[tuple[dict, dict | None]]) -> int:
        success = 0
        with get_conn() as conn:
            for row, translated in results:
                cur = conn.cursor()
                cur.execute("SAVEPOINT sp_entity")
                if not translated:
                    cur.execute("RELEASE SAVEPOINT sp_entity")
                    self._mark_error(conn, row["queue_id"])
                    continue
                try:
                    lang = row["language_code"]
                    model_used = (
                        settings.deepseek_translation_model if lang in _DEEPSEEK_LANGS
                        else settings.gemini_translation_model
                    )
                    provider = "deepseek" if lang in _DEEPSEEK_LANGS else "gemini"
                    cur.execute(
                        """
                        INSERT INTO core.entity_translations
                            (entity_id, language_code, name, description, model_used, is_retranslation)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (entity_id, language_code) DO UPDATE
                          SET name             = EXCLUDED.name,
                              description      = EXCLUDED.description,
                              model_used       = EXCLUDED.model_used,
                              is_retranslation = EXCLUDED.is_retranslation,
                              translated_at    = now()
                        """,
                        (
                            row["entity_id"], lang,
                            translated.get("name"),
                            translated.get("description"),
                            model_used,
                            row["is_retranslation"],
                        ),
                    )
                    cur.execute(
                        """
                        UPDATE core.entity_translation_queue
                           SET status = 'completed', provider = %s, updated_at = now()
                         WHERE id = %s
                        """,
                        (provider, row["queue_id"]),
                    )
                    cur.execute("RELEASE SAVEPOINT sp_entity")
                    success += 1
                except Exception as exc:
                    logger.error("[EntityTranslation] 저장 오류 entity_id=%d: %s", row["entity_id"], exc)
                    cur.execute("ROLLBACK TO SAVEPOINT sp_entity")
                    cur.execute("RELEASE SAVEPOINT sp_entity")
                    self._mark_error(conn, row["queue_id"])
        return success

    @staticmethod
    def _mark_error(conn, queue_id: int) -> None:
        cur = conn.cursor()
        cur.execute(
            "UPDATE core.entity_translation_queue SET status='error', updated_at=now() WHERE id=%s",
            (queue_id,),
        )


class TranslationOrchestrator:
    """
    Usage:
        result = TranslationOrchestrator().run()
        # result = {"address_en": 120, "deepseek": 340, "gemini": 510, "entities": 80}
    """

    def run(self) -> dict[str, int]:
        logger.info("=== 번역 파이프라인 시작 ===")

        addr_count     = JusoAddressTranslator().run()
        deepseek_count = DeepSeekTranslator().run()
        gemini_count   = GeminiBatchTranslator().run()
        entity_count   = EntityTranslationRunner().run()

        result = {
            "address_en": addr_count,
            "deepseek":   deepseek_count,
            "gemini":     gemini_count,
            "entities":   entity_count,
        }
        logger.info("=== 번역 파이프라인 완료: %s ===", result)
        return result


# 하위 호환 alias — 기존 스크립트에서 BatchTranslator를 import하는 경우 대비
BatchTranslator = TranslationOrchestrator
