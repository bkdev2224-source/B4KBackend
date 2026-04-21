-- 04_user.sql: User 스키마 + AI 스키마 (v3)

-- ─────────────────────────────────────────
-- USER 스키마
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS "user".users (
    id             BIGSERIAL    PRIMARY KEY,
    email          TEXT         UNIQUE NOT NULL,
    password_hash  TEXT,
    provider       TEXT         NOT NULL DEFAULT 'local'
                   CHECK (provider IN ('local', 'google', 'kakao')),
    provider_id    TEXT,
    name           TEXT,
    preferred_lang TEXT         NOT NULL DEFAULT 'ko',
    is_active      BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS "user".bookmarks (
    id         BIGSERIAL    PRIMARY KEY,
    user_id    BIGINT       NOT NULL REFERENCES "user".users(id) ON DELETE CASCADE,
    place_id   BIGINT       NOT NULL REFERENCES core.poi(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, place_id)
);

CREATE INDEX IF NOT EXISTS idx_bookmarks_user  ON "user".bookmarks (user_id);
CREATE INDEX IF NOT EXISTS idx_bookmarks_place ON "user".bookmarks (place_id);

CREATE TABLE IF NOT EXISTS "user".reviews (
    id         BIGSERIAL    PRIMARY KEY,
    user_id    BIGINT       NOT NULL REFERENCES "user".users(id) ON DELETE CASCADE,
    place_id   BIGINT       NOT NULL REFERENCES core.poi(id) ON DELETE CASCADE,
    rating     SMALLINT     CHECK (rating BETWEEN 1 AND 5),
    content    TEXT,
    lang       TEXT         NOT NULL DEFAULT 'ko',
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_reviews_user  ON "user".reviews (user_id);
CREATE INDEX IF NOT EXISTS idx_reviews_place ON "user".reviews (place_id);

-- ─────────────────────────────────────────
-- AI 스키마
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS ai.chat_sessions (
    session_id  UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id     BIGINT       REFERENCES "user".users(id) ON DELETE SET NULL,
    lang        TEXT         NOT NULL DEFAULT 'ko',
    started_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_active TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_sessions_user ON ai.chat_sessions (user_id);

CREATE TABLE IF NOT EXISTS ai.chat_messages (
    id         BIGSERIAL    PRIMARY KEY,
    session_id UUID         NOT NULL REFERENCES ai.chat_sessions(session_id) ON DELETE CASCADE,
    role       TEXT         NOT NULL CHECK (role IN ('user', 'assistant', 'tool')),
    content    TEXT         NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_chat_messages_session ON ai.chat_messages (session_id, created_at);

CREATE TABLE IF NOT EXISTS ai.itineraries (
    id          BIGSERIAL    PRIMARY KEY,
    session_id  UUID         REFERENCES ai.chat_sessions(session_id) ON DELETE SET NULL,
    user_id     BIGINT       REFERENCES "user".users(id) ON DELETE SET NULL,
    title       TEXT,
    travel_date DATE,
    region      TEXT,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_itineraries_user    ON ai.itineraries (user_id);
CREATE INDEX IF NOT EXISTS idx_itineraries_session ON ai.itineraries (session_id);

CREATE TABLE IF NOT EXISTS ai.itinerary_items (
    id             BIGSERIAL  PRIMARY KEY,
    itinerary_id   BIGINT     NOT NULL REFERENCES ai.itineraries(id) ON DELETE CASCADE,
    place_id       BIGINT     REFERENCES core.poi(id) ON DELETE SET NULL,
    visit_order    SMALLINT   NOT NULL,
    estimated_time TEXT,
    note           TEXT
);

CREATE INDEX IF NOT EXISTS idx_itinerary_items_itinerary ON ai.itinerary_items (itinerary_id);
CREATE INDEX IF NOT EXISTS idx_itinerary_items_place     ON ai.itinerary_items (place_id);
