"""
Phase 4-1: GPT-4.1 mini 번역 워커
translation_fill_queue → GPT-4.1 mini → poi_translations upsert

동작 방식:
  - fill_queue를 poi_id 단위로 묶어 배치 처리
  - POI 하나당 GPT 호출 1회 (모든 언어 동시 번역 → 비용 최적화)
  - 결과를 poi_translations에 upsert (source='gpt-4.1-mini', needs_review=True)
  - 처리 완료 항목은 fill_queue에서 삭제
"""
import asyncio
import json
import logging
from typing import Any

import asyncpg
from openai import AsyncOpenAI

from settings import settings

logger = logging.getLogger(__name__)

LANG_NAMES = {
    "en":    "English",
    "ja":    "Japanese",
    "zh-CN": "Simplified Chinese",
    "zh-TW": "Traditional Chinese",
    "th":    "Thai",
    "pt-BR": "Brazilian Portuguese",
    "es":    "Spanish",
    "de":    "German",
    "fr":    "French",
    "ru":    "Russian",
}

# poi_id 기준 한 번에 처리할 POI 수
POI_BATCH_SIZE = 20
# GPT 호출 재시도 횟수
MAX_RETRIES = 3


# ─────────────────────────────────────────────────────────────
# 프롬프트
# ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a professional translator specializing in K-culture tourism content.
Translate Korean place names and addresses into the requested languages.

Rules:
- Keep proper nouns (brand names, place names) in their well-known romanized/localized form when a standard exists.
- For place names without a standard translation, transliterate naturally.
- For addresses, adapt the format naturally for each target language (you may keep Korean district/city names transliterated).
- Return ONLY a valid JSON object, no markdown, no explanation.
"""

def _build_user_prompt(name_ko: str, address_ko: str | None, languages: list[str]) -> str:
    lang_list = "\n".join(f'  "{lang}": {{"name": "", "address": ""}}' for lang in languages)
    addr_part = address_ko or ""
    return f"""\
Source (Korean):
  name: {name_ko}
  address: {addr_part}

Translate into each language and return JSON exactly like this (fill in the values):
{{
{lang_list}
}}
"""


# ─────────────────────────────────────────────────────────────
# GPT 호출
# ─────────────────────────────────────────────────────────────

async def _call_gpt(
    client: AsyncOpenAI,
    name_ko: str,
    address_ko: str | None,
    languages: list[str],
) -> dict[str, dict[str, str]]:
    """GPT-4.1-mini 호출 → {lang: {name, address}} 반환"""
    for attempt in range(MAX_RETRIES):
        try:
            response = await client.chat.completions.create(
                model="gpt-4.1-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user",   "content": _build_user_prompt(name_ko, address_ko, languages)},
                ],
                response_format={"type": "json_object"},
                temperature=0.1,
            )
            raw = response.choices[0].message.content or "{}"
            result: dict[str, Any] = json.loads(raw)
            # 반환값 정제: 요청한 언어만 포함, 각 필드를 str로 강제
            out: dict[str, dict[str, str]] = {}
            for lang in languages:
                entry = result.get(lang, {})
                out[lang] = {
                    "name":    str(entry.get("name") or "").strip(),
                    "address": str(entry.get("address") or "").strip(),
                }
            return out
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("GPT 응답 파싱 실패 (시도 %d/%d): %s", attempt + 1, MAX_RETRIES, e)
            if attempt == MAX_RETRIES - 1:
                raise
            await asyncio.sleep(2 ** attempt)
        except Exception as e:
            logger.warning("GPT 호출 오류 (시도 %d/%d): %s", attempt + 1, MAX_RETRIES, e)
            if attempt == MAX_RETRIES - 1:
                raise
            await asyncio.sleep(2 ** attempt)
    return {}  # 도달 불가


# ─────────────────────────────────────────────────────────────
# DB 작업
# ─────────────────────────────────────────────────────────────

async def _fetch_queue_batch(
    conn: asyncpg.Connection, limit: int
) -> list[dict]:
    """fill_queue에서 poi_id 기준 배치 조회"""
    rows = await conn.fetch(
        """
        SELECT DISTINCT poi_id
        FROM core.translation_fill_queue
        ORDER BY poi_id
        LIMIT $1
        """,
        limit,
    )
    if not rows:
        return []

    poi_ids = [r["poi_id"] for r in rows]

    # 각 poi_id 별 pending 언어 목록
    detail_rows = await conn.fetch(
        """
        SELECT poi_id, language_code, field
        FROM core.translation_fill_queue
        WHERE poi_id = ANY($1::bigint[])
        ORDER BY poi_id, language_code
        """,
        poi_ids,
    )

    # poi_id → {lang: [fields]} 구조로 정리
    poi_map: dict[int, dict[str, set]] = {}
    for r in detail_rows:
        pid = r["poi_id"]
        lang = r["language_code"]
        field = r["field"]
        poi_map.setdefault(pid, {}).setdefault(lang, set()).add(field)

    # core.poi에서 한국어 원본 조회
    poi_rows = await conn.fetch(
        "SELECT id, name_ko, address_ko FROM core.poi WHERE id = ANY($1::bigint[])",
        poi_ids,
    )
    poi_source = {r["id"]: {"name_ko": r["name_ko"], "address_ko": r["address_ko"]} for r in poi_rows}

    return [
        {
            "poi_id":    pid,
            "name_ko":   poi_source[pid]["name_ko"],
            "address_ko": poi_source[pid]["address_ko"],
            "languages": list(lang_fields.keys()),
        }
        for pid, lang_fields in poi_map.items()
        if pid in poi_source
    ]


async def _upsert_translations(
    conn: asyncpg.Connection,
    poi_id: int,
    translations: dict[str, dict[str, str]],
) -> None:
    """번역 결과를 poi_translations에 upsert"""
    batch = []
    for lang, fields in translations.items():
        name    = fields.get("name") or None
        address = fields.get("address") or None
        if name or address:
            batch.append((poi_id, lang, name, address))

    if not batch:
        return

    await conn.executemany(
        """
        INSERT INTO core.poi_translations
            (poi_id, language_code, name, address, source, needs_review)
        VALUES ($1, $2, $3, $4, 'gpt-4.1-mini', TRUE)
        ON CONFLICT (poi_id, language_code)
        DO UPDATE SET
            name         = EXCLUDED.name,
            address      = EXCLUDED.address,
            source       = 'gpt-4.1-mini',
            needs_review = TRUE,
            updated_at   = NOW()
        """,
        batch,
    )


async def _delete_queue_entries(
    conn: asyncpg.Connection,
    poi_id: int,
    languages: list[str],
) -> None:
    """처리 완료된 큐 항목 삭제"""
    await conn.execute(
        """
        DELETE FROM core.translation_fill_queue
        WHERE poi_id = $1 AND language_code = ANY($2::text[])
        """,
        poi_id, languages,
    )


# ─────────────────────────────────────────────────────────────
# 메인 워커
# ─────────────────────────────────────────────────────────────

async def run_translation_worker(
    conn: asyncpg.Connection,
    *,
    max_poi: int | None = None,
    dry_run: bool = False,
) -> dict[str, int]:
    """
    번역 워커 실행

    Args:
        conn:    asyncpg 커넥션
        max_poi: 처리할 최대 POI 수 (None = 전체)
        dry_run: True이면 DB 쓰기 없이 GPT만 호출해 결과 출력

    Returns:
        {"processed": n, "skipped": n, "errors": n}
    """
    if not settings.openai_api_key:
        raise RuntimeError("OPENAI_API_KEY가 설정되지 않았습니다.")

    client = AsyncOpenAI(api_key=settings.openai_api_key)

    total_queue = await conn.fetchval("SELECT COUNT(DISTINCT poi_id) FROM core.translation_fill_queue")
    limit = min(max_poi, total_queue) if max_poi else total_queue
    logger.info("번역 대기 POI: %d건 (처리 예정: %d건)", total_queue, limit)

    stats = {"processed": 0, "skipped": 0, "errors": 0}
    processed = 0

    while True:
        remaining = (limit - processed) if max_poi else POI_BATCH_SIZE
        if remaining <= 0:
            break

        batch = await _fetch_queue_batch(conn, min(POI_BATCH_SIZE, remaining))
        if not batch:
            break

        for item in batch:
            poi_id    = item["poi_id"]
            name_ko   = item["name_ko"]
            address_ko = item["address_ko"]
            languages = item["languages"]

            try:
                translations = await _call_gpt(client, name_ko, address_ko, languages)

                if dry_run:
                    print(f"\n[dry_run] poi_id={poi_id} ({name_ko})")
                    for lang, fields in translations.items():
                        print(f"  {lang}: name={fields['name']!r}  address={fields['address']!r}")
                else:
                    async with conn.transaction():
                        await _upsert_translations(conn, poi_id, translations)
                        await _delete_queue_entries(conn, poi_id, languages)

                stats["processed"] += 1
                processed += 1

                if processed % 50 == 0 or processed == limit:
                    logger.info("진행: %d / %d POI 완료", processed, limit)

            except Exception as e:
                logger.error("poi_id=%d 번역 실패: %s", poi_id, e)
                stats["errors"] += 1

        if max_poi and processed >= max_poi:
            break

    remaining_queue = await conn.fetchval("SELECT COUNT(DISTINCT poi_id) FROM core.translation_fill_queue")
    logger.info(
        "완료 — 처리: %d, 오류: %d, 잔여 큐: %d",
        stats["processed"], stats["errors"], remaining_queue,
    )
    return stats


# ─────────────────────────────────────────────────────────────
# 번역 완료율 조회
# ─────────────────────────────────────────────────────────────

async def get_translation_coverage(conn: asyncpg.Connection) -> dict[str, float]:
    """언어별 번역 완료율 반환 (0.0 ~ 1.0)"""
    total_poi = await conn.fetchval("SELECT COUNT(*) FROM core.poi WHERE is_active = TRUE")
    if not total_poi:
        return {}

    rows = await conn.fetch(
        """
        SELECT language_code, COUNT(*) AS cnt
        FROM core.poi_translations
        WHERE poi_id IN (SELECT id FROM core.poi WHERE is_active = TRUE)
        GROUP BY language_code
        """
    )
    return {r["language_code"]: round(r["cnt"] / total_poi, 4) for r in rows}
