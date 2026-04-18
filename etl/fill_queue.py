"""
번역 보완 대기열 관리
"""
import asyncpg

ALL_LANGUAGES = ["en", "ja", "zh-CN", "zh-TW", "th", "pt-BR", "es", "de", "fr", "ru"]


async def enqueue_missing_translations(
    conn: asyncpg.Connection,
    poi_id: int,
    existing_languages: set[str],
) -> None:
    """번역이 없는 언어를 translation_fill_queue에 추가"""
    missing = [lang for lang in ALL_LANGUAGES if lang not in existing_languages]
    if not missing:
        return

    rows = [(poi_id, lang, "name") for lang in missing] + \
           [(poi_id, lang, "address") for lang in missing]

    await conn.executemany(
        """
        INSERT INTO core.translation_fill_queue (poi_id, language_code, field)
        VALUES ($1, $2, $3)
        ON CONFLICT (poi_id, language_code, field) DO NOTHING
        """,
        rows,
    )
