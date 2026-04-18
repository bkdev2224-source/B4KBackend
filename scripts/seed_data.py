"""
Phase 1-4: 기본 Seed 데이터 INSERT (v3)
- core.supported_languages: ko + 10개 언어
- core.k_culture_tags: 기본 태그
- stage.api_sources: TourAPI 등록

실행: python scripts/seed_data.py
"""
import asyncio
import json
import sys
from pathlib import Path

import asyncpg

ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(ROOT))

from settings import settings

# ko + 10개 번역 언어
LANGUAGES = [
    ("ko",    "한국어"),
    ("en",    "English"),
    ("ja",    "日本語"),
    ("zh-CN", "简体中文"),
    ("zh-TW", "繁體中文"),
    ("th",    "ภาษาไทย"),
    ("pt-BR", "Português (Brasil)"),
    ("es",    "Español"),
    ("de",    "Deutsch"),
    ("fr",    "Français"),
    ("ru",    "Русский"),
]

K_CULTURE_TAGS = [
    ("kpop",             "K-POP",    "K-POP",            "music"),
    ("kbeauty",          "K-뷰티",   "K-Beauty",         "beauty"),
    ("kdrama",           "K-드라마", "K-Drama",          "entertainment"),
    ("kfood",            "K-푸드",   "K-Food",           "food"),
    ("hanbok",           "한복",     "Hanbok",           "culture"),
    ("hallyu",           "한류",     "Hallyu",           "culture"),
    ("idol",             "아이돌",   "Idol",             "music"),
    ("filming-location", "촬영지",   "Filming Location", "entertainment"),
    ("webtoon",          "웹툰",     "Webtoon",          "entertainment"),
    ("kmusical",         "뮤지컬",   "K-Musical",        "music"),
]

TOURAPI_BASE = "https://apis.data.go.kr/B551011"

API_SOURCES = [
    {
        "name":        "tourapi",
        "source_type": "api",
        "description": "한국관광공사 TourAPI (한국어만 수집)",
        "base_url":    f"{TOURAPI_BASE}/KorService2",
        "config": {
            "language_urls": {
                "ko": f"{TOURAPI_BASE}/KorService2"
            }
        },
    },
]


async def seed(conn: asyncpg.Connection) -> None:
    # 1. supported_languages
    for code, name in LANGUAGES:
        await conn.execute(
            """
            INSERT INTO core.supported_languages (code, name)
            VALUES ($1, $2)
            ON CONFLICT (code) DO NOTHING
            """,
            code, name,
        )
    print(f"[ok] supported_languages: {len(LANGUAGES)}개 언어")

    # 2. k_culture_tags
    for slug, name_ko, name_en, category in K_CULTURE_TAGS:
        await conn.execute(
            """
            INSERT INTO core.k_culture_tags (slug, name_ko, name_en, category)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (slug) DO NOTHING
            """,
            slug, name_ko, name_en, category,
        )
    print(f"[ok] k_culture_tags: {len(K_CULTURE_TAGS)}개 태그")

    # 3. api_sources
    for src in API_SOURCES:
        await conn.execute(
            """
            INSERT INTO stage.api_sources
                (name, source_type, base_url, description, config)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (name) DO UPDATE
                SET source_type = EXCLUDED.source_type,
                    base_url    = EXCLUDED.base_url,
                    description = EXCLUDED.description,
                    config      = EXCLUDED.config
            """,
            src["name"], src["source_type"], src["base_url"],
            src["description"], json.dumps(src["config"]),
        )
    print(f"[ok] api_sources: {len(API_SOURCES)}개 소스")


async def main() -> None:
    print("=== Phase 1-4: Seed 데이터 INSERT ===\n")
    conn = await asyncpg.connect(
        host=settings.db_host,
        port=settings.db_port,
        database=settings.db_name,
        user=settings.db_user,
        password=settings.db_password,
        statement_cache_size=0,
    )
    try:
        await seed(conn)
    finally:
        await conn.close()
    print("\n=== 완료 ===")


if __name__ == "__main__":
    asyncio.run(main())
