-- Phase 1-2 + 1-3: Core 스키마 DDL (v3)
-- 정제·중복제거·번역 완료된 마스터 데이터 레이어

-- ─────────────────────────────────────────
-- 1-2: 장소 관련 테이블
-- ─────────────────────────────────────────

-- 지원 언어 목록 (poi_translations 에서 참조)
CREATE TABLE IF NOT EXISTS core.supported_languages (
    code      VARCHAR(10)  PRIMARY KEY,
    name      VARCHAR(100) NOT NULL,
    is_active BOOLEAN      NOT NULL DEFAULT TRUE
);

INSERT INTO core.supported_languages (code, name) VALUES
    ('ko',    '한국어'),
    ('en',    'English'),
    ('ja',    '日本語'),
    ('zh-CN', '简体中文'),
    ('zh-TW', '繁體中文'),
    ('th',    'ภาษาไทย'),
    ('pt-BR', 'Português (Brasil)')
ON CONFLICT (code) DO NOTHING;

-- K-culture 태그
-- category 예시: 'kfood', 'kfashion', 'kbeauty_product', 'ktourism', 'kcultural'
-- parent_tag_id 로 계층 구성 가능 (한식 → 분식 → 떡볶이)
CREATE TABLE IF NOT EXISTS core.k_culture_tags (
    id             SERIAL       PRIMARY KEY,
    slug           VARCHAR(100) NOT NULL UNIQUE,
    name_ko        VARCHAR(200) NOT NULL,
    name_en        VARCHAR(200),
    category       VARCHAR(50),
    parent_tag_id  INTEGER      REFERENCES core.k_culture_tags(id) ON DELETE SET NULL,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_k_culture_tags_parent
    ON core.k_culture_tags(parent_tag_id);

-- 건물 (POI 상위 컨테이너, 선택적)
CREATE TABLE IF NOT EXISTS core.buildings (
    id         BIGSERIAL    PRIMARY KEY,
    name       VARCHAR(500),
    address    TEXT,
    geom       GEOMETRY(Point, 4326),
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_buildings_geom
    ON core.buildings USING GIST(geom);

-- POI 마스터
CREATE TABLE IF NOT EXISTS core.poi (
    id              BIGSERIAL    PRIMARY KEY,
    source_ids      JSONB        NOT NULL DEFAULT '{}',  -- {"tourapi":"123", "mois":"456"}
    name_ko         VARCHAR(500) NOT NULL,
    address_ko      TEXT,
    geom            GEOMETRY(Point, 4326),
    category_code   VARCHAR(50),    -- 소스 원본 카테고리 코드 (tourapi cat1~3, mois 업태구분명)
    content_type_id VARCHAR(20),    -- TourAPI contenttypeid (12,14,25,28,32,38,39,75,76)
    region_code     VARCHAR(20),    -- 지역 코드 (tourapi areacode, mois 행정구역코드)
    phone           VARCHAR(100),
    homepage        TEXT,
    display_domain  VARCHAR(50),    -- kfood|kbeauty|ktourism|kshopping|kleisure 등
    display_region  VARCHAR(100),   -- 서울|부산|제주 등 한국어 지역명
    quality         VARCHAR(20)  NOT NULL DEFAULT 'missing'
                    CHECK (quality IN ('full', 'partial', 'missing')),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    building_id     BIGINT       REFERENCES core.buildings(id),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_poi_geom
    ON core.poi USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_poi_source_ids
    ON core.poi USING GIN(source_ids);
CREATE INDEX IF NOT EXISTS idx_poi_active
    ON core.poi(is_active) WHERE is_active = TRUE;
-- 단일 소스 중복 방지: TourAPI contentid 기준 unique
CREATE UNIQUE INDEX IF NOT EXISTS idx_poi_tourapi_contentid
    ON core.poi ((source_ids->>'tourapi'))
    WHERE source_ids ? 'tourapi';

-- POI 다국어 번역
CREATE TABLE IF NOT EXISTS core.poi_translations (
    id            BIGSERIAL   PRIMARY KEY,
    poi_id        BIGINT      NOT NULL REFERENCES core.poi(id) ON DELETE CASCADE,
    language_code VARCHAR(10) NOT NULL REFERENCES core.supported_languages(code),
    name          TEXT,
    address       TEXT,
    description   TEXT,
    source        VARCHAR(20) NOT NULL DEFAULT 'api'
                  CHECK (source IN ('api', 'gpt', 'gpt-4.1-mini', 'deepseek', 'gemini', 'juso')),
    needs_review  BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (poi_id, language_code)
);

CREATE INDEX IF NOT EXISTS idx_poi_translations_poi
    ON core.poi_translations(poi_id);

-- POI 이미지 (Cloudinary 메타)
CREATE TABLE IF NOT EXISTS core.poi_images (
    id                   BIGSERIAL   PRIMARY KEY,
    poi_id               BIGINT      NOT NULL REFERENCES core.poi(id) ON DELETE CASCADE,
    cloudinary_public_id TEXT        NOT NULL,
    secure_url           TEXT        NOT NULL,
    thumbnail_url        TEXT,           -- 400×300 crop
    webp_url             TEXT,           -- WebP 변환 URL
    original_url         TEXT,           -- TourAPI 원본 URL
    width                INTEGER,
    height               INTEGER,
    format               VARCHAR(20),
    is_primary           BOOLEAN     NOT NULL DEFAULT FALSE,
    upload_status        VARCHAR(20) NOT NULL DEFAULT 'pending'
                         CHECK (upload_status IN ('pending', 'uploaded', 'error', 'skipped')),
    error_count          INTEGER     NOT NULL DEFAULT 0,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_poi_images_poi
    ON core.poi_images(poi_id);

-- POI <-> 태그 매핑
CREATE TABLE IF NOT EXISTS core.poi_tag_map (
    poi_id  BIGINT  NOT NULL REFERENCES core.poi(id) ON DELETE CASCADE,
    tag_id  INTEGER NOT NULL REFERENCES core.k_culture_tags(id) ON DELETE CASCADE,
    is_auto BOOLEAN NOT NULL DEFAULT TRUE,
    PRIMARY KEY (poi_id, tag_id)
);

-- ─────────────────────────────────────────
-- 1-3: 운영 관련 테이블
-- ─────────────────────────────────────────

-- 공연·이벤트
CREATE TABLE IF NOT EXISTS core.events (
    id            BIGSERIAL    PRIMARY KEY,
    source_ids    JSONB        NOT NULL DEFAULT '{}',
    poi_id        BIGINT       REFERENCES core.poi(id),
    name_ko       VARCHAR(500) NOT NULL,
    start_date    DATE,
    end_date      DATE,
    venue_name_ko VARCHAR(500),
    geom          GEOMETRY(Point, 4326),
    category      VARCHAR(100),
    is_active     BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_date
    ON core.events(start_date, end_date);

-- 번역 보완 대기열
CREATE TABLE IF NOT EXISTS core.translation_fill_queue (
    id            BIGSERIAL   PRIMARY KEY,
    poi_id        BIGINT      NOT NULL REFERENCES core.poi(id) ON DELETE CASCADE,
    language_code VARCHAR(10) NOT NULL REFERENCES core.supported_languages(code),
    field         VARCHAR(50) NOT NULL,
    priority      INTEGER     NOT NULL DEFAULT 5,
    status        VARCHAR(20) NOT NULL DEFAULT 'pending'
                  CHECK (status IN ('pending', 'completed', 'error')),
    provider      VARCHAR(20),
    is_retranslation BOOLEAN  NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (poi_id, language_code, field)
);

CREATE INDEX IF NOT EXISTS idx_translation_fill_queue_pending
    ON core.translation_fill_queue(language_code)
    WHERE status = 'pending';

-- 중복 검토 대기열
CREATE TABLE IF NOT EXISTS core.dedup_review_queue (
    id              BIGSERIAL   PRIMARY KEY,
    poi_id_a        BIGINT      NOT NULL REFERENCES core.poi(id),
    poi_id_b        BIGINT               REFERENCES core.poi(id),  -- NULL = 아직 정규화 전
    raw_doc_id      BIGINT               REFERENCES stage.raw_documents(id) ON DELETE SET NULL,
    distance_m      FLOAT,
    name_similarity FLOAT,
    status          VARCHAR(20) NOT NULL DEFAULT 'pending'
                    CHECK (status IN ('pending', 'merged', 'rejected')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reviewed_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dedup_review_queue_poi_a
    ON core.dedup_review_queue(poi_id_a);
CREATE INDEX IF NOT EXISTS idx_dedup_review_queue_raw_doc
    ON core.dedup_review_queue(raw_doc_id) WHERE raw_doc_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_dedup_review_queue_status
    ON core.dedup_review_queue(status) WHERE status = 'pending';

-- 동기화 로그
CREATE TABLE IF NOT EXISTS core.sync_log (
    id           BIGSERIAL   PRIMARY KEY,
    sync_run_id  INTEGER     REFERENCES stage.sync_runs(id),
    poi_inserted INTEGER     NOT NULL DEFAULT 0,
    poi_updated  INTEGER     NOT NULL DEFAULT 0,
    poi_skipped  INTEGER     NOT NULL DEFAULT 0,
    errors       INTEGER     NOT NULL DEFAULT 0,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sync_log_sync_run
    ON core.sync_log(sync_run_id);
