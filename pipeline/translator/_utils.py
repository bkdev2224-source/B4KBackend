"""
번역기 공용 유틸리티 — 토큰 추정, 청크 분할, 번역 규칙 로드
"""
from __future__ import annotations

from database.db import get_conn

# 1 토큰 ≈ 2자 (한국어·영어 혼합 보수적 추정)
_CHARS_PER_TOKEN = 2
# 시스템 프롬프트 + JSON 구조 오버헤드 (고정)
_FIXED_OVERHEAD_TOKENS = 500


def estimate_tokens(text: str) -> int:
    return max(1, len(text) // _CHARS_PER_TOKEN)


def split_by_token_budget(rows: list[dict], budget: int) -> list[list[dict]]:
    """
    토큰 예산에 맞게 rows를 청크로 분할.
    각 row의 name + description 길이로 입력+출력 토큰을 추정한다.
    """
    chunks: list[list[dict]] = []
    current: list[dict] = []
    used = _FIXED_OVERHEAD_TOKENS

    for row in rows:
        text = (row.get("name") or "") + " " + (row.get("description") or "")
        cost = estimate_tokens(text) * 2  # 입력 + 출력 합산
        if current and used + cost > budget:
            chunks.append(current)
            current = [row]
            used = _FIXED_OVERHEAD_TOKENS + cost
        else:
            current.append(row)
            used += cost

    if current:
        chunks.append(current)
    return chunks


def load_translation_rules(lang: str) -> str:
    """
    core.translation_rules에서 해당 언어 + 전체 공통 규칙을 로드.
    반환값은 시스템 프롬프트 뒤에 붙일 텍스트 블록 (없으면 빈 문자열).
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT rule_text
              FROM core.translation_rules
             WHERE is_active = TRUE
               AND (lang IS NULL OR lang = %s)
             ORDER BY priority DESC, id
            """,
            (lang,),
        )
        rules = [row["rule_text"] for row in cur.fetchall()]

    if not rules:
        return ""
    return "\n\nAdditional translation rules (must follow exactly):\n" + "\n".join(
        f"- {r}" for r in rules
    )


def load_translation_glossary(lang: str) -> str:
    """
    core.translation_glossary에서 해당 언어의 고정 번역 용어집을 로드.
    반환값은 시스템 프롬프트 뒤에 붙일 텍스트 블록 (없으면 빈 문자열).
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT term_ko, translation
              FROM core.translation_glossary
             WHERE is_active = TRUE
               AND lang = %s
             ORDER BY priority DESC, id
            """,
            (lang,),
        )
        terms = cur.fetchall()

    if not terms:
        return ""
    lines = "\n".join(f"- {row['term_ko']} → {row['translation']}" for row in terms)
    return (
        "\n\nGlossary (these terms MUST be translated exactly as specified below — "
        "do not paraphrase or invent alternatives):\n" + lines
    )


def load_prompt_additions(lang: str) -> str:
    """rules + glossary를 하나의 블록으로 합쳐 반환."""
    return load_translation_rules(lang) + load_translation_glossary(lang)
