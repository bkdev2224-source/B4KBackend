-- Phase 1-1: Stage 스키마 DDL (v3)
-- 외부 API / 크롤러 / 파일에서 수집한 원본 데이터를 보관하는 레이어

-- API 소스 목록
CREATE TABLE IF NOT EXISTS stage.api_sources (
    id           SERIAL      PRIMARY KEY,
    name         VARCHAR(100) NOT NULL UNIQUE,
    source_type  VARCHAR(20) NOT NULL DEFAULT 'api'
                 CHECK (source_type IN ('api', 'crawler', 'file')),
    base_url     TEXT,
    description  TEXT,
    health       VARCHAR(20) NOT NULL DEFAULT 'up'
                 CHECK (health IN ('up', 'down', 'unknown')),
    config       JSONB       NOT NULL DEFAULT '{}',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- API 키 관리
CREATE TABLE IF NOT EXISTS stage.api_keys (
    id          SERIAL      PRIMARY KEY,
    source_id   INTEGER     NOT NULL REFERENCES stage.api_sources(id),
    key_value   TEXT        NOT NULL,
    is_active   BOOLEAN     NOT NULL DEFAULT TRUE,
    daily_limit INTEGER,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_source
    ON stage.api_keys(source_id);

-- 소스별 언어별 동기화 상태 (체크포인트)
CREATE TABLE IF NOT EXISTS stage.source_sync_state (
    id             SERIAL      PRIMARY KEY,
    source_id      INTEGER     NOT NULL REFERENCES stage.api_sources(id),
    language_code  VARCHAR(10) NOT NULL DEFAULT 'ko',
    last_synced_at TIMESTAMPTZ,
    last_page      INTEGER     NOT NULL DEFAULT 0,
    total_count    INTEGER,
    status         VARCHAR(20) NOT NULL DEFAULT 'idle'
                   CHECK (status IN ('idle', 'running', 'done', 'failed')),
    checkpoint     JSONB       NOT NULL DEFAULT '{}',
    UNIQUE (source_id, language_code)
);

-- 수집 실행 이력
CREATE TABLE IF NOT EXISTS stage.sync_runs (
    id                SERIAL      PRIMARY KEY,
    source_id         INTEGER     NOT NULL REFERENCES stage.api_sources(id),
    run_type          VARCHAR(20) NOT NULL
                      CHECK (run_type IN ('full_load', 'fetch_updated')),
    language_code     VARCHAR(10),
    started_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at       TIMESTAMPTZ,
    status            VARCHAR(20) NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'done', 'failed')),
    records_collected INTEGER     NOT NULL DEFAULT 0,
    error_message     TEXT
);

CREATE INDEX IF NOT EXISTS idx_sync_runs_source
    ON stage.sync_runs(source_id);

-- API에서 받은 원본 JSON 저장
CREATE TABLE IF NOT EXISTS stage.raw_documents (
    id            BIGSERIAL    PRIMARY KEY,
    source_id     INTEGER      NOT NULL REFERENCES stage.api_sources(id),
    external_id   VARCHAR(200) NOT NULL,
    language_code VARCHAR(10)  NOT NULL DEFAULT 'ko',
    raw_json      JSONB        NOT NULL,
    sync_run_id   INTEGER      REFERENCES stage.sync_runs(id),
    is_processed  BOOLEAN      NOT NULL DEFAULT FALSE,
    collected_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (source_id, external_id, language_code)
);

CREATE INDEX IF NOT EXISTS idx_raw_documents_source
    ON stage.raw_documents(source_id);
CREATE INDEX IF NOT EXISTS idx_raw_documents_sync_run
    ON stage.raw_documents(sync_run_id);
CREATE INDEX IF NOT EXISTS idx_raw_documents_unprocessed
    ON stage.raw_documents(is_processed) WHERE is_processed = FALSE;
