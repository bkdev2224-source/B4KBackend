-- 05_entities.sql: K-Culture 엔티티 레이어
-- 엔티티 범위: kpop_artist · kbeauty_brand · kdrama_show
-- 그 외(kfood, kfashion 등)는 core.k_culture_tags 에서 태그로 관리
-- POI와 독립된 엔티티를 관리하고, poi_entity_map / event_entity_map 으로 연결

-- ─────────────────────────────────────────
-- 1. 엔티티 마스터
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.entities (
    id             SERIAL       PRIMARY KEY,
    slug           VARCHAR(100) NOT NULL UNIQUE,
    canonical_name VARCHAR(300) NOT NULL,           -- 공식 한국어명
    name_en        VARCHAR(300),
    entity_type    VARCHAR(50)  NOT NULL
                   CHECK (entity_type IN ('kpop_artist', 'kbeauty_brand', 'kdrama_show')),
    description_ko TEXT,
    is_active      BOOLEAN      NOT NULL DEFAULT TRUE,
    -- 타입별 구조화 메타데이터
    -- kpop_artist : {"company":"SM", "debut":"2020-11-17", "fandom":"MY", "is_group":true, "members":["카리나","지젤"]}
    -- kbeauty_brand: {"parent_company":"Amorepacific", "founded":2000, "origin":"제주"}
    -- kdrama_show  : {"broadcaster":"tvN", "air_date":"2023-01", "episodes":16, "genre":"로맨스"}
    metadata       JSONB        NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entities_type
    ON core.entities(entity_type);
CREATE INDEX IF NOT EXISTS idx_entities_active
    ON core.entities(is_active) WHERE is_active = TRUE;
CREATE INDEX IF NOT EXISTS idx_entities_metadata
    ON core.entities USING GIN(metadata);

-- updated_at 자동 갱신 트리거
CREATE OR REPLACE FUNCTION core.trg_entities_set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_entities_updated_at ON core.entities;
CREATE TRIGGER trg_entities_updated_at
    BEFORE UPDATE ON core.entities
    FOR EACH ROW EXECUTE FUNCTION core.trg_entities_set_updated_at();

-- ─────────────────────────────────────────
-- 2. 별칭 (검색·매칭용)
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.entity_aliases (
    entity_id  INTEGER      NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    alias      VARCHAR(300) NOT NULL,
    lang       VARCHAR(10)  NOT NULL DEFAULT 'ko',
    alias_type VARCHAR(30)  NOT NULL DEFAULT 'name'
               CHECK (alias_type IN ('name', 'abbr', 'fandom', 'former_name')),
    PRIMARY KEY (entity_id, alias)
);

CREATE INDEX IF NOT EXISTS idx_entity_aliases_alias
    ON core.entity_aliases(alias);
CREATE INDEX IF NOT EXISTS idx_entity_aliases_entity
    ON core.entity_aliases(entity_id);

-- ─────────────────────────────────────────
-- 3. SNS 링크
-- 플랫폼당 복수 계정 허용 (그룹 공식 + 멤버별 + 서브 채널 등)
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.entity_sns (
    id         BIGSERIAL    PRIMARY KEY,
    entity_id  INTEGER      NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    platform   VARCHAR(50)  NOT NULL
               CHECK (platform IN ('instagram', 'youtube', 'x', 'tiktok', 'weverse',
                                   'facebook', 'line', 'kakao', 'spotify', 'melon')),
    url        TEXT         NOT NULL,
    handle     VARCHAR(200),
    label      VARCHAR(100),           -- NULL=공식, '카리나', 'Beyond Live', '티저 채널' 등
    is_primary BOOLEAN      NOT NULL DEFAULT FALSE  -- 플랫폼별 대표 계정
);

-- label이 NOT NULL인 경우 (named 계정): (entity_id, platform, label) 조합 유일
CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_sns_unique_labeled
    ON core.entity_sns(entity_id, platform, label)
    WHERE label IS NOT NULL;

-- label이 NULL인 경우 (공식 계정): platform당 1개만 허용
-- PostgreSQL UNIQUE constraint는 NULL을 동등 비교하지 않으므로 별도 partial index로 보장
CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_sns_unique_official
    ON core.entity_sns(entity_id, platform)
    WHERE label IS NULL;

CREATE INDEX IF NOT EXISTS idx_entity_sns_entity
    ON core.entity_sns(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_sns_platform
    ON core.entity_sns(platform);

-- ─────────────────────────────────────────
-- 4. 엔티티 간 관계
-- ─────────────────────────────────────────
-- 방향: entity_id_a → entity_id_b (relation 이름이 방향 내포)
--   member_of    : 카리나(a) → aespa(b)
--   subsidiary_of: Innisfree(a) → Amorepacific(b)
--   signed_to    : aespa(a) → SM(b)
--   features_cast: 드라마(a) → 배우(b)
--   ost_by       : 드라마(a) → 아티스트(b)
--   collab_with  : 대칭 관계 (순서 무관)

CREATE TABLE IF NOT EXISTS core.entity_entity_map (
    entity_id_a  INTEGER      NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    entity_id_b  INTEGER      NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    relation     VARCHAR(50)  NOT NULL
                 CHECK (relation IN (
                     'member_of',      -- 멤버 → 그룹
                     'subsidiary_of',  -- 계열사 → 모회사
                     'signed_to',      -- 아티스트 → 소속사
                     'features_cast',  -- 드라마 → 출연 배우
                     'ost_by',         -- 드라마 → OST 아티스트
                     'collab_with'     -- 협업 (대칭)
                 )),
    since        DATE,                 -- 관계 시작일 (데뷔·계약·방영)
    until        DATE,                 -- 관계 종료일 NULL=현재 유효
    is_active    BOOLEAN      NOT NULL DEFAULT TRUE,
    PRIMARY KEY (entity_id_a, entity_id_b, relation),
    CHECK (entity_id_a <> entity_id_b)
);

CREATE INDEX IF NOT EXISTS idx_entity_entity_map_a
    ON core.entity_entity_map(entity_id_a);
CREATE INDEX IF NOT EXISTS idx_entity_entity_map_b
    ON core.entity_entity_map(entity_id_b);
CREATE INDEX IF NOT EXISTS idx_entity_entity_map_relation
    ON core.entity_entity_map(relation);

-- ─────────────────────────────────────────
-- 5. 이미지
-- poi_images 패턴 동일, image_type으로 용도 구분
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.entity_images (
    id                   BIGSERIAL    PRIMARY KEY,
    entity_id            INTEGER      NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    cloudinary_public_id TEXT         UNIQUE,
    secure_url           TEXT         NOT NULL,
    thumbnail_url        TEXT,
    original_url         TEXT,
    image_type           VARCHAR(50)  NOT NULL DEFAULT 'photo'
                         CHECK (image_type IN (
                             'photo',        -- 일반 사진
                             'album_cover',  -- kpop_artist 앨범 재킷
                             'poster',       -- kdrama_show 포스터
                             'product',      -- kbeauty_brand 제품 이미지
                             'logo',         -- 브랜드 로고
                             'banner'        -- 배너/커버
                         )),
    caption              TEXT,
    is_primary           BOOLEAN      NOT NULL DEFAULT FALSE,
    upload_status        VARCHAR(20)  NOT NULL DEFAULT 'pending'
                         CHECK (upload_status IN ('pending', 'uploaded', 'error', 'skipped')),
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entity_images_entity
    ON core.entity_images(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_images_status
    ON core.entity_images(upload_status);

-- ─────────────────────────────────────────
-- 6. 뉴스·소식
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.entity_news (
    id           BIGSERIAL    PRIMARY KEY,
    entity_id    INTEGER      NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    title        TEXT         NOT NULL,
    summary      TEXT,
    url          TEXT,
    source       VARCHAR(100),   -- 'naver', 'weverse', 'melon', 'manual' 등
    published_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_entity_news_entity
    ON core.entity_news(entity_id, published_at DESC);

-- ─────────────────────────────────────────
-- 7. POI ↔ 엔티티 연결
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.poi_entity_map (
    poi_id     BIGINT      NOT NULL REFERENCES core.poi(id) ON DELETE CASCADE,
    entity_id  INTEGER     NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    relation   VARCHAR(50) NOT NULL DEFAULT 'associated'
               CHECK (relation IN (
                   'official_store',   -- 공식 판매처·플래그십
                   'popup',            -- 팝업 스토어·한정 행사
                   'concert_venue',    -- 공연장
                   'filming_location', -- 촬영지
                   'associated'        -- 기타 연관
               )),
    is_auto    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (poi_id, entity_id, relation)
);

CREATE INDEX IF NOT EXISTS idx_poi_entity_map_entity
    ON core.poi_entity_map(entity_id);
CREATE INDEX IF NOT EXISTS idx_poi_entity_map_poi
    ON core.poi_entity_map(poi_id);
CREATE INDEX IF NOT EXISTS idx_poi_entity_map_relation
    ON core.poi_entity_map(relation);

-- ─────────────────────────────────────────
-- 8. 이벤트 ↔ 엔티티 연결
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.event_entity_map (
    event_id   BIGINT      NOT NULL REFERENCES core.events(id) ON DELETE CASCADE,
    entity_id  INTEGER     NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    relation   VARCHAR(50) NOT NULL DEFAULT 'associated'
               CHECK (relation IN ('performer', 'organizer', 'sponsor', 'associated')),
    is_auto    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (event_id, entity_id, relation)
);

CREATE INDEX IF NOT EXISTS idx_event_entity_map_entity
    ON core.event_entity_map(entity_id);
CREATE INDEX IF NOT EXISTS idx_event_entity_map_event
    ON core.event_entity_map(event_id);

-- ─────────────────────────────────────────
-- 9. 다국어 번역
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS core.entity_translations (
    id               BIGSERIAL    PRIMARY KEY,
    entity_id        INTEGER      NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    language_code    VARCHAR(10)  NOT NULL,
    name             TEXT,
    description      TEXT,
    translated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    model_used       TEXT,
    is_retranslation BOOLEAN      NOT NULL DEFAULT FALSE,
    UNIQUE (entity_id, language_code)
);

CREATE INDEX IF NOT EXISTS idx_entity_translations_entity
    ON core.entity_translations(entity_id);
CREATE INDEX IF NOT EXISTS idx_entity_translations_lang
    ON core.entity_translations(language_code);

-- 번역 대기 큐
CREATE TABLE IF NOT EXISTS core.entity_translation_queue (
    id            BIGSERIAL    PRIMARY KEY,
    entity_id     INTEGER      NOT NULL REFERENCES core.entities(id) ON DELETE CASCADE,
    language_code VARCHAR(10)  NOT NULL,
    status        VARCHAR(20)  NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'submitted', 'completed', 'error')),
    triggered_by  VARCHAR(20)  NOT NULL DEFAULT 'new'
                  CHECK (triggered_by IN ('new', 'update', 'manual')),
    provider      VARCHAR(20),
    job_id        TEXT,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (entity_id, language_code)
);

CREATE INDEX IF NOT EXISTS idx_entity_trans_queue_status
    ON core.entity_translation_queue(status);
CREATE INDEX IF NOT EXISTS idx_entity_trans_queue_entity
    ON core.entity_translation_queue(entity_id);
