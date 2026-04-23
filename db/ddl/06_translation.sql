-- 06_translation.sql: 번역 규칙 · 용어집 레이어
-- core.translation_rules  — 번역기 프롬프트에 주입되는 행동 지침
-- core.translation_glossary — 단어 단위 1:1 고정 대응표
-- 두 테이블 모두 코드 배포 없이 DB에서 직접 관리한다.

-- ─────────────────────────────────────────
-- 1. 번역 규칙
-- ─────────────────────────────────────────
-- rule_type 가이드:
--   preserve : 원문 유지 (브랜드명, 공식 영문 아티스트명 등)
--   term     : 특정 단어의 번역 방식 지정 (떡볶이→tteokbokki)
--   style    : 문체·어조 지침 (일본어 경어체 등)
--   format   : 출력 형식 (로마자 병기, 괄호 표기 등)
-- lang = NULL : 전 언어 공통, 'en'/'ja' 등 : 해당 언어 전용

CREATE TABLE IF NOT EXISTS core.translation_rules (
    id         BIGSERIAL    PRIMARY KEY,
    rule_type  TEXT         NOT NULL
               CHECK (rule_type IN ('term', 'style', 'format', 'preserve')),
    lang       TEXT,                        -- NULL = 전 언어 공통
    rule_text  TEXT         NOT NULL,       -- 프롬프트에 직접 삽입할 영어 규칙 문장
    example    TEXT,                        -- 예시 (선택)
    priority   SMALLINT     NOT NULL DEFAULT 0,  -- 높을수록 먼저 삽입 (0~10)
    is_active  BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_translation_rules_lang
    ON core.translation_rules(lang) WHERE is_active;

-- ─────────────────────────────────────────
-- 2. 번역 용어집
-- ─────────────────────────────────────────
-- rules와의 차이:
--   rules    = 번역 행동 지침 (어떻게 번역할지)
--   glossary = 단어 단위 1:1 고정 대응표 (무엇으로 번역할지)
-- 입력 경로: CSV/JSON 파일 → scripts/load_glossary.py → DB

CREATE TABLE IF NOT EXISTS core.translation_glossary (
    id          BIGSERIAL    PRIMARY KEY,
    term_ko     TEXT         NOT NULL,      -- 한국어 원문 표현
    lang        TEXT         NOT NULL,      -- 대상 언어 (en|ja|zh-CN|zh-TW|th|pt-BR)
    translation TEXT         NOT NULL,      -- 고정 번역어
    category    TEXT,                       -- 음식|관광지|브랜드|행정구역|아티스트|팬덤|시설 등
    note        TEXT,                       -- 관리자 메모 (출처, 공식 여부 등)
    priority    SMALLINT     NOT NULL DEFAULT 0,
    is_active   BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (term_ko, lang)
);

CREATE INDEX IF NOT EXISTS idx_glossary_lang
    ON core.translation_glossary(lang) WHERE is_active;
CREATE INDEX IF NOT EXISTS idx_glossary_term
    ON core.translation_glossary(term_ko);
CREATE INDEX IF NOT EXISTS idx_glossary_category
    ON core.translation_glossary(category) WHERE is_active;
