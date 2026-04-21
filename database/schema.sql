-- =============================================================================
-- K-Culture Platform — Full Schema DDL
-- Phase 0: stage · core · service · user · ai
-- =============================================================================

-- ── Extensions ───────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS vector;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS unaccent;

-- ── Schemas ───────────────────────────────────────────────────────────────────
CREATE SCHEMA IF NOT EXISTS stage;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS service;
CREATE SCHEMA IF NOT EXISTS "user";
CREATE SCHEMA IF NOT EXISTS ai;


-- =============================================================================
-- STAGE SCHEMA — 원천 데이터 그대로 적재
-- =============================================================================

-- 소스 등록 테이블
CREATE TABLE IF NOT EXISTS stage.api_sources (
    source_name   TEXT PRIMARY KEY,               -- 'tourapi' | 'mois' | 'mcst' | 'crawl'
    display_name  TEXT NOT NULL,
    base_url      TEXT,
    description   TEXT,
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

INSERT INTO stage.api_sources (source_name, display_name, base_url) VALUES
    ('tourapi', '한국관광공사', 'https://apis.data.go.kr/B551011/KorService1'),
    ('mois',    '행정안전부',   'https://www.localdata.go.kr'),
    ('mcst',    '문화체육관광부', 'https://www.mcst.go.kr'),
    ('crawl',   '크롤링',       NULL)
ON CONFLICT DO NOTHING;


-- 원천 데이터 — 소스에서 수집한 RAW 데이터를 그대로 보존
CREATE TABLE IF NOT EXISTS stage.raw_documents (
    id            BIGSERIAL PRIMARY KEY,
    source_name   TEXT NOT NULL REFERENCES stage.api_sources(source_name),
    source_id     TEXT NOT NULL,                  -- 원천 시스템의 고유 ID
    raw_data      JSONB NOT NULL,                 -- 원천 데이터 전체 (변경 없이 저장)
    sync_status   TEXT NOT NULL DEFAULT 'new'     -- 'new' | 'modified' | 'deleted' | 'processed' | 'error' | 'review'
                  CHECK (sync_status IN ('new', 'modified', 'deleted', 'processed', 'error', 'review')),
    collected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    processed_at  TIMESTAMPTZ,
    UNIQUE (source_name, source_id)
);

CREATE INDEX IF NOT EXISTS idx_raw_documents_source    ON stage.raw_documents (source_name, sync_status);
CREATE INDEX IF NOT EXISTS idx_raw_documents_collected ON stage.raw_documents (collected_at);


-- 마지막 sync 시각 — 소스별 증분 수집 기준점
CREATE TABLE IF NOT EXISTS stage.source_sync_state (
    source_name    TEXT PRIMARY KEY REFERENCES stage.api_sources(source_name),
    last_synced_at TIMESTAMPTZ,
    last_run_id    BIGINT,
    extra          JSONB                          -- 소스별 추가 상태 (areaCode cursor 등)
);

INSERT INTO stage.source_sync_state (source_name) VALUES
    ('tourapi'), ('mois'), ('mcst'), ('crawl')
ON CONFLICT DO NOTHING;


-- 수집 실행 이력
CREATE TABLE IF NOT EXISTS stage.sync_runs (
    id              BIGSERIAL PRIMARY KEY,
    source_name     TEXT NOT NULL REFERENCES stage.api_sources(source_name),
    run_type        TEXT NOT NULL DEFAULT 'incremental'  -- 'full' | 'incremental'
                    CHECK (run_type IN ('full', 'incremental')),
    status          TEXT NOT NULL DEFAULT 'running'
                    CHECK (status IN ('running', 'success', 'failed', 'partial')),
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at     TIMESTAMPTZ,
    new_count       INT NOT NULL DEFAULT 0,
    modified_count  INT NOT NULL DEFAULT 0,
    deleted_count   INT NOT NULL DEFAULT 0,
    error_message   TEXT,
    meta            JSONB
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_source ON stage.sync_runs (source_name, started_at DESC);


-- =============================================================================
-- CORE SCHEMA — 정규화·병합된 마스터 데이터
-- =============================================================================

CREATE TABLE IF NOT EXISTS core.places (
    place_id          BIGSERIAL PRIMARY KEY,
    name              TEXT NOT NULL,
    address           TEXT,
    address_detail    TEXT,
    coords            GEOGRAPHY(POINT, 4326),    -- WGS84
    phone             TEXT,
    description       TEXT,

    -- 원천 메타
    source_name       TEXT NOT NULL,             -- 최초 등록 소스
    source_id         TEXT NOT NULL,             -- 최초 소스의 ID
    source_category   TEXT,                      -- 원천 카테고리 코드 (null 허용)
    region_code       TEXT,                      -- 원천 지역코드

    -- 매핑 결과
    display_domain    TEXT,                      -- 'kfood' | 'kbeauty' | null
    display_region    TEXT,                      -- '서울' | '부산' | null

    -- 품질
    quality_score     NUMERIC(3,2) DEFAULT 0.0   -- 0.0 ~ 1.0 (좌표·이름·주소 완결성)
                      CHECK (quality_score BETWEEN 0 AND 1),
    is_active         BOOLEAN NOT NULL DEFAULT TRUE,
    is_publishable    BOOLEAN NOT NULL DEFAULT FALSE,  -- en 번역 완료 시 TRUE

    -- 검토 대기
    dedup_status      TEXT DEFAULT 'auto'
                      CHECK (dedup_status IN ('auto', 'review', 'confirmed')),
    dedup_review_meta JSONB,

    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),

    UNIQUE (source_name, source_id)
);

CREATE INDEX IF NOT EXISTS idx_places_coords         ON core.places USING GIST (coords);
CREATE INDEX IF NOT EXISTS idx_places_domain         ON core.places (display_domain) WHERE is_active;
CREATE INDEX IF NOT EXISTS idx_places_region         ON core.places (display_region) WHERE is_active;
CREATE INDEX IF NOT EXISTS idx_places_source         ON core.places (source_name, source_id);
CREATE INDEX IF NOT EXISTS idx_places_publishable    ON core.places (is_publishable) WHERE is_active;


-- 멀티소스 ID 연결 — 동일 장소가 여러 소스에 등록된 경우
CREATE TABLE IF NOT EXISTS core.place_source_ids (
    id            BIGSERIAL PRIMARY KEY,
    place_id      BIGINT NOT NULL REFERENCES core.places(place_id) ON DELETE CASCADE,
    source_name   TEXT NOT NULL REFERENCES stage.api_sources(source_name),
    source_id     TEXT NOT NULL,
    id_format     TEXT,                          -- 'numeric' | 'alpha_numeric' | 'prefixed' | 'uuid'
    UNIQUE (source_name, source_id)
);

CREATE INDEX IF NOT EXISTS idx_place_source_ids_place ON core.place_source_ids (place_id);

-- 번역
-- address 컬럼은 lang='en' 행에만 값이 들어간다 (주소정보누리집 API 한→영 변환).
-- 다른 언어(ja/zh-CN/zh-TW/th)는 도로명 주소를 번역하지 않으므로 address=NULL.
CREATE TABLE IF NOT EXISTS core.place_translations (
    id              BIGSERIAL PRIMARY KEY,
    place_id        BIGINT NOT NULL REFERENCES core.places(place_id) ON DELETE CASCADE,
    lang            TEXT NOT NULL,               -- 'en' | 'ja' | 'zh-CN' | 'zh-TW' | 'th'
    name            TEXT,
    address         TEXT,                        -- en만 사용 (주소정보누리집 API)
    description     TEXT,
    translated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    model_used      TEXT,                        -- 'gemini-2.5-flash' | 'deepseek-chat' | 'juso_api'
    is_retranslation BOOLEAN NOT NULL DEFAULT FALSE,
    UNIQUE (place_id, lang)
);

CREATE INDEX IF NOT EXISTS idx_translations_place ON core.place_translations (place_id);
CREATE INDEX IF NOT EXISTS idx_translations_lang  ON core.place_translations (lang);


-- 번역 규칙 — 번역기 시스템 프롬프트에 주입되는 규칙 (lang NULL = 모든 언어 공통)
-- rule_type: 'term'(용어통일) | 'style'(문체) | 'format'(형식) | 'preserve'(원문유지)
CREATE TABLE IF NOT EXISTS core.translation_rules (
    id          BIGSERIAL PRIMARY KEY,
    rule_type   TEXT NOT NULL CHECK (rule_type IN ('term', 'style', 'format', 'preserve')),
    lang        TEXT,                      -- NULL = 전 언어 공통
    rule_text   TEXT NOT NULL,             -- 프롬프트에 삽입할 규칙 문장 (영어)
    example     TEXT,                      -- 예시 (선택)
    priority    SMALLINT NOT NULL DEFAULT 0,  -- 높을수록 먼저 삽입
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_translation_rules_lang ON core.translation_rules (lang) WHERE is_active;


-- 번역 용어집 — 특정 단어·표현의 언어별 고정 번역 (프롬프트 glossary 섹션에 주입)
-- rules와의 차이: rules는 행동 지침(문체·형식), glossary는 단어 단위 1:1 대응표
CREATE TABLE IF NOT EXISTS core.translation_glossary (
    id          BIGSERIAL PRIMARY KEY,
    term_ko     TEXT NOT NULL,              -- 한국어 원문 표현
    lang        TEXT NOT NULL,              -- 대상 언어 ('en','ja','zh-CN','zh-TW','th','pt-BR')
    translation TEXT NOT NULL,             -- 고정 번역어
    category    TEXT,                      -- 분류 (예: '음식', '관광지', '브랜드', '행정구역')
    note        TEXT,                      -- 관리자 메모 (선택)
    priority    SMALLINT NOT NULL DEFAULT 0,  -- 높을수록 먼저 주입
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (term_ko, lang)
);

CREATE INDEX IF NOT EXISTS idx_glossary_lang ON core.translation_glossary (lang) WHERE is_active;
CREATE INDEX IF NOT EXISTS idx_glossary_term ON core.translation_glossary (term_ko);


-- 이미지 — Cloudinary CDN
CREATE TABLE IF NOT EXISTS core.place_images (
    id              BIGSERIAL PRIMARY KEY,
    place_id        BIGINT NOT NULL REFERENCES core.places(place_id) ON DELETE CASCADE,
    original_url    TEXT NOT NULL,
    cloudinary_url  TEXT,
    public_id       TEXT UNIQUE,
    width           INT,
    height          INT,
    format          TEXT DEFAULT 'webp',
    is_primary      BOOLEAN NOT NULL DEFAULT FALSE,
    upload_status   TEXT NOT NULL DEFAULT 'pending'
                    CHECK (upload_status IN ('pending', 'uploaded', 'error', 'skipped')),
    error_count     INT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_place_images_place  ON core.place_images (place_id);
CREATE INDEX IF NOT EXISTS idx_place_images_status ON core.place_images (upload_status);


-- 번역 대기 큐 — 신규/변경 place의 name·description 번역 작업 추적
-- address 번역(한→영, Juso API)은 이 큐를 거치지 않고 JusoAddressTranslator가 직접 처리한다.
-- provider: 'gemini' (en/ja/th) | 'deepseek' (zh-CN/zh-TW)
CREATE TABLE IF NOT EXISTS core.translation_fill_queue (
    id              BIGSERIAL PRIMARY KEY,
    place_id        BIGINT NOT NULL REFERENCES core.places(place_id) ON DELETE CASCADE,
    lang            TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'submitted', 'completed', 'error')),
    triggered_by    TEXT NOT NULL DEFAULT 'new'  -- 'new' | 'update' | 'manual'
                    CHECK (triggered_by IN ('new', 'update', 'manual')),
    is_retranslation BOOLEAN NOT NULL DEFAULT FALSE,
    provider        TEXT,                        -- 'gemini' | 'deepseek'
    job_id          TEXT,                        -- Batch Job ID (provider별)
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (place_id, lang)                      -- ON CONFLICT DO NOTHING → 멱등성
);

CREATE INDEX IF NOT EXISTS idx_trans_queue_status ON core.translation_fill_queue (status);
CREATE INDEX IF NOT EXISTS idx_trans_queue_place  ON core.translation_fill_queue (place_id);


-- =============================================================================
-- SERVICE SCHEMA — 서비스 응답 전용 스냅샷 (JOIN 없이 단건 조회)
-- =============================================================================

CREATE TABLE IF NOT EXISTS service.places_snapshot (
    place_id           BIGINT PRIMARY KEY REFERENCES core.places(place_id) ON DELETE CASCADE,
    name_ko            TEXT,
    name_en            TEXT,
    name_ja            TEXT,
    name_zh_cn         TEXT,
    name_zh_tw         TEXT,
    name_th            TEXT,
    name_pt_br         TEXT,
    address_ko         TEXT,
    address_en         TEXT,              -- 주소정보누리집 API 한→영 변환
    description_ko     TEXT,
    description_en     TEXT,
    description_ja     TEXT,
    description_zh_cn  TEXT,
    description_zh_tw  TEXT,
    description_th     TEXT,
    description_pt_br  TEXT,
    coords_lat         FLOAT,
    coords_lng         FLOAT,
    display_domain     TEXT,
    display_region     TEXT,
    source_category    TEXT,
    quality_score      NUMERIC(3,2),
    primary_image_url  TEXT,
    is_publishable     BOOLEAN,
    updated_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_snapshot_domain  ON service.places_snapshot (display_domain) WHERE is_publishable;
CREATE INDEX IF NOT EXISTS idx_snapshot_region  ON service.places_snapshot (display_region) WHERE is_publishable;


-- pgvector 임베딩 검색 인덱스 (B4KChatAI가 읽음)
CREATE TABLE IF NOT EXISTS service.search_index (
    place_id    BIGINT PRIMARY KEY REFERENCES core.places(place_id) ON DELETE CASCADE,
    embedding   vector(1536),                    -- text-embedding-3-small
    indexed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_search_embedding ON service.search_index
    USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);


-- =============================================================================
-- USER SCHEMA
-- =============================================================================

CREATE TABLE IF NOT EXISTS "user".users (
    user_id     BIGSERIAL PRIMARY KEY,
    email       TEXT UNIQUE NOT NULL,
    password_hash TEXT,
    provider    TEXT DEFAULT 'local',            -- 'local' | 'google' | 'kakao'
    provider_id TEXT,
    name        TEXT,
    preferred_lang TEXT DEFAULT 'ko',
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS "user".bookmarks (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES "user".users(user_id) ON DELETE CASCADE,
    place_id    BIGINT NOT NULL REFERENCES core.places(place_id) ON DELETE CASCADE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, place_id)
);

CREATE INDEX IF NOT EXISTS idx_bookmarks_place ON "user".bookmarks (place_id);

CREATE TABLE IF NOT EXISTS "user".reviews (
    id          BIGSERIAL PRIMARY KEY,
    user_id     BIGINT NOT NULL REFERENCES "user".users(user_id) ON DELETE CASCADE,
    place_id    BIGINT NOT NULL REFERENCES core.places(place_id) ON DELETE CASCADE,
    rating      SMALLINT CHECK (rating BETWEEN 1 AND 5),
    content     TEXT,
    lang        TEXT DEFAULT 'ko',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reviews_place ON "user".reviews (place_id);


-- =============================================================================
-- AI SCHEMA — 챗봇 세션/메시지/일정 (B4KChatAI 서비스가 읽고 씀)
-- =============================================================================

CREATE TABLE IF NOT EXISTS ai.chat_sessions (
    session_id   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      BIGINT REFERENCES "user".users(user_id) ON DELETE SET NULL,
    lang         TEXT DEFAULT 'ko',
    started_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_active  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_user ON ai.chat_sessions (user_id);

CREATE TABLE IF NOT EXISTS ai.chat_messages (
    id          BIGSERIAL PRIMARY KEY,
    session_id  UUID NOT NULL REFERENCES ai.chat_sessions(session_id) ON DELETE CASCADE,
    role        TEXT NOT NULL CHECK (role IN ('user', 'assistant', 'tool')),
    content     TEXT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_session ON ai.chat_messages (session_id, created_at);

CREATE TABLE IF NOT EXISTS ai.itineraries (
    itinerary_id  BIGSERIAL PRIMARY KEY,
    session_id    UUID REFERENCES ai.chat_sessions(session_id) ON DELETE SET NULL,
    user_id       BIGINT REFERENCES "user".users(user_id) ON DELETE SET NULL,
    title         TEXT,
    travel_date   DATE,
    region        TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_itineraries_user    ON ai.itineraries (user_id);
CREATE INDEX IF NOT EXISTS idx_itineraries_session ON ai.itineraries (session_id);

CREATE TABLE IF NOT EXISTS ai.itinerary_items (
    id             BIGSERIAL PRIMARY KEY,
    itinerary_id   BIGINT NOT NULL REFERENCES ai.itineraries(itinerary_id) ON DELETE CASCADE,
    place_id       BIGINT REFERENCES core.places(place_id) ON DELETE SET NULL,
    visit_order    SMALLINT NOT NULL,
    estimated_time TEXT,
    note           TEXT
);

CREATE INDEX IF NOT EXISTS idx_itinerary_items_itinerary ON ai.itinerary_items (itinerary_id);
CREATE INDEX IF NOT EXISTS idx_itinerary_items_place     ON ai.itinerary_items (place_id);


-- =============================================================================
-- TRIGGERS — 번역 완료 시 places_snapshot 자동 갱신
-- =============================================================================

CREATE OR REPLACE FUNCTION service.refresh_snapshot_for_place(p_place_id BIGINT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_place     core.places%ROWTYPE;
    v_img_url   TEXT;
BEGIN
    SELECT * INTO v_place FROM core.places WHERE place_id = p_place_id;
    SELECT cloudinary_url INTO v_img_url
      FROM core.place_images
     WHERE place_id = p_place_id AND is_primary AND upload_status = 'uploaded'
     LIMIT 1;

    INSERT INTO service.places_snapshot (
        place_id, display_domain, display_region, source_category,
        quality_score, is_publishable, primary_image_url, updated_at,
        coords_lat, coords_lng,
        name_ko,    name_en,    name_ja,    name_zh_cn, name_zh_tw, name_th,    name_pt_br,
        address_ko, address_en,
        description_ko, description_en, description_ja,
        description_zh_cn, description_zh_tw, description_th, description_pt_br
    )
    SELECT
        v_place.place_id,
        v_place.display_domain,
        v_place.display_region,
        v_place.source_category,
        v_place.quality_score,
        v_place.is_publishable,
        v_img_url,
        now(),
        ST_Y(v_place.coords::geometry),
        ST_X(v_place.coords::geometry),
        v_place.name,
        MAX(CASE WHEN lang='en'    THEN name END),
        MAX(CASE WHEN lang='ja'    THEN name END),
        MAX(CASE WHEN lang='zh-CN' THEN name END),
        MAX(CASE WHEN lang='zh-TW' THEN name END),
        MAX(CASE WHEN lang='th'    THEN name END),
        MAX(CASE WHEN lang='pt-BR' THEN name END),
        v_place.address,
        MAX(CASE WHEN lang='en'    THEN address END),
        v_place.description,
        MAX(CASE WHEN lang='en'    THEN description END),
        MAX(CASE WHEN lang='ja'    THEN description END),
        MAX(CASE WHEN lang='zh-CN' THEN description END),
        MAX(CASE WHEN lang='zh-TW' THEN description END),
        MAX(CASE WHEN lang='th'    THEN description END),
        MAX(CASE WHEN lang='pt-BR' THEN description END)
    FROM core.place_translations
    WHERE place_id = p_place_id
    ON CONFLICT (place_id) DO UPDATE SET
        name_ko            = EXCLUDED.name_ko,
        name_en            = EXCLUDED.name_en,
        name_ja            = EXCLUDED.name_ja,
        name_zh_cn         = EXCLUDED.name_zh_cn,
        name_zh_tw         = EXCLUDED.name_zh_tw,
        name_th            = EXCLUDED.name_th,
        name_pt_br         = EXCLUDED.name_pt_br,
        address_ko         = EXCLUDED.address_ko,
        address_en         = EXCLUDED.address_en,
        description_ko     = EXCLUDED.description_ko,
        description_en     = EXCLUDED.description_en,
        description_ja     = EXCLUDED.description_ja,
        description_zh_cn  = EXCLUDED.description_zh_cn,
        description_zh_tw  = EXCLUDED.description_zh_tw,
        description_th     = EXCLUDED.description_th,
        description_pt_br  = EXCLUDED.description_pt_br,
        display_domain     = EXCLUDED.display_domain,
        display_region     = EXCLUDED.display_region,
        source_category    = EXCLUDED.source_category,
        quality_score      = EXCLUDED.quality_score,
        is_publishable     = EXCLUDED.is_publishable,
        primary_image_url  = EXCLUDED.primary_image_url,
        updated_at         = now();
END;
$$;


CREATE OR REPLACE FUNCTION service.trg_refresh_snapshot()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM service.refresh_snapshot_for_place(NEW.place_id);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_translation_snapshot ON core.place_translations;
CREATE TRIGGER trg_translation_snapshot
    AFTER INSERT OR UPDATE ON core.place_translations
    FOR EACH ROW EXECUTE FUNCTION service.trg_refresh_snapshot();


-- en 번역 완료 시 is_publishable = true 자동 설정
CREATE OR REPLACE FUNCTION core.trg_set_publishable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.lang = 'en' AND NEW.name IS NOT NULL THEN
        UPDATE core.places SET is_publishable = TRUE WHERE place_id = NEW.place_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_en_publishable ON core.place_translations;
CREATE TRIGGER trg_en_publishable
    AFTER INSERT OR UPDATE ON core.place_translations
    FOR EACH ROW EXECUTE FUNCTION core.trg_set_publishable();
