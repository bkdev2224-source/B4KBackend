-- ══════════════════════════════════════════════════════════════════════════════
-- 07_security.sql  —  B4K Database Security Layer
-- ──────────────────────────────────────────────────────────────────────────────
-- 목표:
--   1. 클라이언트(anon / authenticated)에서 어떤 테이블에도 직접 write 불가
--   2. 모든 쓰기는 api.* SECURITY DEFINER 함수를 통해서만 허용
--   3. stage.* / core.*  → 클라이언트 완전 차단 (백엔드 role만 접근)
--   4. service.*         → 클라이언트 SELECT 전용 (is_publishable=TRUE 필터)
--   5. user.* / ai.*     → 자신의 row만 SELECT, 쓰기는 함수 경유
--   6. backend_api role  → BYPASSRLS, FastAPI asyncpg 전용
--   7. 감사 로그(api.audit_log) + 레이트 리밋(api.rate_limit_counter)
-- ══════════════════════════════════════════════════════════════════════════════


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 0: 사전 준비 — api 스키마 + supabase_uid 컬럼
-- ══════════════════════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS api;

-- Supabase auth.uid() ↔ user.users 연결을 위한 UUID 컬럼
-- auth.uid()는 Supabase Auth에서 발급한 UUID (JWT sub 클레임)
ALTER TABLE "user".users
    ADD COLUMN IF NOT EXISTS supabase_uid UUID UNIQUE;

CREATE INDEX IF NOT EXISTS idx_users_supabase_uid
    ON "user".users (supabase_uid);

-- user.reviews: 사용자당 장소당 1개 리뷰 제약 (upsert 지원)
-- 기존 중복 데이터가 있으면 먼저 제거 후 실행
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'uq_reviews_user_place'
    ) THEN
        ALTER TABLE "user".reviews
            ADD CONSTRAINT uq_reviews_user_place UNIQUE (user_id, place_id);
    END IF;
END $$;


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 1: 역할(Role) 정의
-- ══════════════════════════════════════════════════════════════════════════════

-- backend_api: FastAPI asyncpg 전용 role — RLS 우회, 전체 스키마 접근
-- Supabase 프로젝트라면 service_role 키를 사용하는 커넥션이 이 역할을 수행
-- 자체 PostgreSQL이라면 아래 CREATE ROLE 활성화
-- DO $$ BEGIN
--     CREATE ROLE backend_api LOGIN PASSWORD 'REPLACE_WITH_STRONG_PASSWORD';
-- EXCEPTION WHEN duplicate_object THEN NULL; END $$;

-- backend_api에 RLS 우회 권한 부여 (슈퍼유저 권한 필요)
-- ALTER ROLE backend_api BYPASSRLS;
-- ALTER ROLE backend_api CREATEROLE;  -- 필요 시


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 2: 권한 초기화 (클린 슬레이트)
-- anon / authenticated / PUBLIC 에서 모든 기존 권한 제거
-- ══════════════════════════════════════════════════════════════════════════════

-- 스키마 USAGE 제거
REVOKE ALL ON SCHEMA stage   FROM anon, authenticated, PUBLIC;
REVOKE ALL ON SCHEMA core    FROM anon, authenticated, PUBLIC;
REVOKE ALL ON SCHEMA service FROM anon, authenticated, PUBLIC;
REVOKE ALL ON SCHEMA "user"  FROM anon, authenticated, PUBLIC;
REVOKE ALL ON SCHEMA ai      FROM anon, authenticated, PUBLIC;
REVOKE ALL ON SCHEMA api     FROM anon, authenticated, PUBLIC;

-- 테이블 권한 제거
REVOKE ALL ON ALL TABLES IN SCHEMA stage   FROM anon, authenticated, PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA core    FROM anon, authenticated, PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA service FROM anon, authenticated, PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA "user"  FROM anon, authenticated, PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA ai      FROM anon, authenticated, PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA api     FROM anon, authenticated, PUBLIC;


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 3: RLS 활성화 (전 테이블)
-- ══════════════════════════════════════════════════════════════════════════════

-- ── stage 스키마 ──
ALTER TABLE stage.api_sources        ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage.api_keys           ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage.source_sync_state  ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage.sync_runs          ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage.raw_documents      ENABLE ROW LEVEL SECURITY;

-- ── core 스키마 ──
ALTER TABLE core.supported_languages     ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.k_culture_tags          ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.buildings               ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.poi                     ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.poi_translations        ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.poi_images              ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.poi_tag_map             ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.events                  ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.translation_fill_queue  ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.dedup_review_queue      ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.sync_log                ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.translation_rules       ENABLE ROW LEVEL SECURITY;
ALTER TABLE core.translation_glossary    ENABLE ROW LEVEL SECURITY;

-- ── service 스키마 ──
ALTER TABLE service.places_snapshot  ENABLE ROW LEVEL SECURITY;
ALTER TABLE service.search_index     ENABLE ROW LEVEL SECURITY;

-- ── user 스키마 ──
ALTER TABLE "user".users      ENABLE ROW LEVEL SECURITY;
ALTER TABLE "user".bookmarks  ENABLE ROW LEVEL SECURITY;
ALTER TABLE "user".reviews    ENABLE ROW LEVEL SECURITY;

-- ── ai 스키마 ──
ALTER TABLE ai.chat_sessions    ENABLE ROW LEVEL SECURITY;
ALTER TABLE ai.chat_messages    ENABLE ROW LEVEL SECURITY;
ALTER TABLE ai.itineraries      ENABLE ROW LEVEL SECURITY;
ALTER TABLE ai.itinerary_items  ENABLE ROW LEVEL SECURITY;

-- FORCE RLS: 테이블 소유자도 RLS를 우회하지 못하게 강제
-- (service_role / BYPASSRLS role 제외)
ALTER TABLE service.places_snapshot  FORCE ROW LEVEL SECURITY;
ALTER TABLE "user".users             FORCE ROW LEVEL SECURITY;
ALTER TABLE "user".bookmarks         FORCE ROW LEVEL SECURITY;
ALTER TABLE "user".reviews           FORCE ROW LEVEL SECURITY;
ALTER TABLE ai.chat_sessions         FORCE ROW LEVEL SECURITY;
ALTER TABLE ai.chat_messages         FORCE ROW LEVEL SECURITY;
ALTER TABLE ai.itineraries           FORCE ROW LEVEL SECURITY;
ALTER TABLE ai.itinerary_items       FORCE ROW LEVEL SECURITY;


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 4: 스키마 USAGE + 테이블 SELECT 권한 부여
-- ══════════════════════════════════════════════════════════════════════════════

-- anon: 공개 서비스 데이터만 읽기
GRANT USAGE ON SCHEMA service TO anon;
GRANT SELECT ON service.places_snapshot TO anon;
GRANT SELECT ON service.search_index    TO anon;

-- authenticated: 공개 서비스 + 사용자 데이터 읽기
GRANT USAGE ON SCHEMA service TO authenticated;
GRANT USAGE ON SCHEMA "user"  TO authenticated;
GRANT USAGE ON SCHEMA ai      TO authenticated;
GRANT USAGE ON SCHEMA api     TO authenticated;

GRANT SELECT ON service.places_snapshot TO authenticated;
GRANT SELECT ON service.search_index    TO authenticated;
GRANT SELECT ON "user".users            TO authenticated;
GRANT SELECT ON "user".bookmarks        TO authenticated;
GRANT SELECT ON "user".reviews          TO authenticated;
GRANT SELECT ON ai.chat_sessions        TO authenticated;
GRANT SELECT ON ai.chat_messages        TO authenticated;
GRANT SELECT ON ai.itineraries          TO authenticated;
GRANT SELECT ON ai.itinerary_items      TO authenticated;

-- api 스키마 함수 실행 권한 (나중에 함수별로 GRANT EXECUTE 추가)
GRANT USAGE ON SCHEMA api TO authenticated;


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 5: RLS 정책 정의
-- ══════════════════════════════════════════════════════════════════════════════

-- ── 헬퍼: 현재 인증된 사용자의 내부 ID 조회 ──
-- 함수 먼저 생성해야 policy USING 절에서 사용 가능
CREATE OR REPLACE FUNCTION api.current_user_id()
RETURNS BIGINT
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = "user", public
AS $$
    SELECT id FROM "user".users WHERE supabase_uid = auth.uid() LIMIT 1;
$$;

GRANT EXECUTE ON FUNCTION api.current_user_id() TO authenticated;


-- ── service.places_snapshot ──
-- 공개(is_publishable=TRUE)인 장소만 anon/authenticated에 노출
DROP POLICY IF EXISTS "snapshot_public_read" ON service.places_snapshot;
CREATE POLICY "snapshot_public_read"
    ON service.places_snapshot
    FOR SELECT
    TO anon, authenticated
    USING (is_publishable = TRUE);

-- ── service.search_index ──
-- publishable POI의 임베딩만 접근 가능
DROP POLICY IF EXISTS "search_index_public_read" ON service.search_index;
CREATE POLICY "search_index_public_read"
    ON service.search_index
    FOR SELECT
    TO anon, authenticated
    USING (
        EXISTS (
            SELECT 1 FROM service.places_snapshot s
            WHERE s.place_id = search_index.place_id
              AND s.is_publishable = TRUE
        )
    );

-- ── user.users ──
-- 자신의 row만 읽기 가능, 직접 수정 불가 (update_profile 함수 경유)
DROP POLICY IF EXISTS "users_own_select" ON "user".users;
CREATE POLICY "users_own_select"
    ON "user".users
    FOR SELECT
    TO authenticated
    USING (supabase_uid = auth.uid());

-- ── user.bookmarks ──
-- 자신의 북마크만 조회, INSERT/UPDATE/DELETE 없음 (함수 경유)
DROP POLICY IF EXISTS "bookmarks_own_select" ON "user".bookmarks;
CREATE POLICY "bookmarks_own_select"
    ON "user".bookmarks
    FOR SELECT
    TO authenticated
    USING (user_id = api.current_user_id());

-- ── user.reviews ──
-- 자신의 리뷰만 조회, 타인 리뷰는 서비스 스냅샷을 통해 집계로만 노출
DROP POLICY IF EXISTS "reviews_own_select" ON "user".reviews;
CREATE POLICY "reviews_own_select"
    ON "user".reviews
    FOR SELECT
    TO authenticated
    USING (user_id = api.current_user_id());

-- ── ai.chat_sessions ──
DROP POLICY IF EXISTS "chat_sessions_own_select" ON ai.chat_sessions;
CREATE POLICY "chat_sessions_own_select"
    ON ai.chat_sessions
    FOR SELECT
    TO authenticated
    USING (user_id = api.current_user_id());

-- ── ai.chat_messages ──
DROP POLICY IF EXISTS "chat_messages_own_select" ON ai.chat_messages;
CREATE POLICY "chat_messages_own_select"
    ON ai.chat_messages
    FOR SELECT
    TO authenticated
    USING (
        session_id IN (
            SELECT session_id FROM ai.chat_sessions
            WHERE user_id = api.current_user_id()
        )
    );

-- ── ai.itineraries ──
DROP POLICY IF EXISTS "itineraries_own_select" ON ai.itineraries;
CREATE POLICY "itineraries_own_select"
    ON ai.itineraries
    FOR SELECT
    TO authenticated
    USING (user_id = api.current_user_id());

-- ── ai.itinerary_items ──
DROP POLICY IF EXISTS "itinerary_items_own_select" ON ai.itinerary_items;
CREATE POLICY "itinerary_items_own_select"
    ON ai.itinerary_items
    FOR SELECT
    TO authenticated
    USING (
        itinerary_id IN (
            SELECT id FROM ai.itineraries
            WHERE user_id = api.current_user_id()
        )
    );

-- ── stage.* / core.* ──
-- 정책 없음 = anon/authenticated 접근 완전 차단 (RLS 활성화 상태에서 기본 deny)
-- backend_api(BYPASSRLS) 또는 service_role만 접근 가능


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 6: 감사 로그 + 레이트 리밋 테이블
-- ══════════════════════════════════════════════════════════════════════════════

-- 감사 로그: SECURITY DEFINER 함수에서 기록
CREATE TABLE IF NOT EXISTS api.audit_log (
    id          BIGSERIAL    PRIMARY KEY,
    supabase_uid UUID        NOT NULL,
    action      TEXT        NOT NULL,           -- 'bookmark_add'|'bookmark_remove'|'review_upsert'|'review_delete'|'profile_update'
    target_id   BIGINT,                         -- poi_id 또는 review_id 등
    detail      JSONB       NOT NULL DEFAULT '{}',
    ip_address  INET,                           -- PostgREST request.headers 활용 시 채워짐
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_uid
    ON api.audit_log (supabase_uid, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_action
    ON api.audit_log (action, created_at DESC);

-- 레이트 리밋: 시간당 사용자별 액션 카운터
CREATE TABLE IF NOT EXISTS api.rate_limit_counter (
    supabase_uid UUID        NOT NULL,
    action       TEXT        NOT NULL,
    window_start TIMESTAMPTZ NOT NULL,          -- 1시간 단위 버킷
    count        INTEGER     NOT NULL DEFAULT 0,
    PRIMARY KEY (supabase_uid, action, window_start)
);

CREATE INDEX IF NOT EXISTS idx_rate_limit_uid_action
    ON api.rate_limit_counter (supabase_uid, action, window_start);


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 7: SECURITY DEFINER 함수 — 클라이언트 쓰기 진입점
-- ══════════════════════════════════════════════════════════════════════════════

-- ── 내부 헬퍼: 레이트 리밋 체크 ──
-- 시간당 최대 호출 횟수를 초과하면 예외 발생
CREATE OR REPLACE FUNCTION api._check_rate_limit(
    p_uid    UUID,
    p_action TEXT,
    p_limit  INTEGER DEFAULT 60        -- 기본 시간당 60회
)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = api, public
AS $$
DECLARE
    v_window TIMESTAMPTZ := date_trunc('hour', NOW());
    v_count  INTEGER;
BEGIN
    INSERT INTO api.rate_limit_counter (supabase_uid, action, window_start, count)
    VALUES (p_uid, p_action, v_window, 1)
    ON CONFLICT (supabase_uid, action, window_start)
    DO UPDATE SET count = api.rate_limit_counter.count + 1
    RETURNING count INTO v_count;

    IF v_count > p_limit THEN
        RAISE EXCEPTION 'Rate limit exceeded: % per hour for action %', p_limit, p_action
            USING ERRCODE = '54000';
    END IF;
END;
$$;


-- ── 내부 헬퍼: 감사 로그 기록 ──
CREATE OR REPLACE FUNCTION api._write_audit(
    p_uid      UUID,
    p_action   TEXT,
    p_target   BIGINT   DEFAULT NULL,
    p_detail   JSONB    DEFAULT '{}'
)
RETURNS VOID
LANGUAGE sql SECURITY DEFINER
SET search_path = api, public
AS $$
    INSERT INTO api.audit_log (supabase_uid, action, target_id, detail)
    VALUES (p_uid, p_action, p_target, p_detail);
$$;


-- ────────────────────────────────────────────────────────────────────────────
-- 7-1. 북마크 토글  api.toggle_bookmark(p_poi_id)
-- 없으면 추가, 있으면 삭제. 반환: {action: 'added'|'removed', poi_id: N}
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION api.toggle_bookmark(p_poi_id BIGINT)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = "user", api, public
AS $$
DECLARE
    v_uid     UUID   := auth.uid();
    v_user_id BIGINT;
    v_exists  BOOLEAN;
    v_action  TEXT;
BEGIN
    -- 인증 확인
    IF v_uid IS NULL THEN
        RAISE EXCEPTION 'Authentication required' USING ERRCODE = '42501';
    END IF;

    v_user_id := api.current_user_id();
    IF v_user_id IS NULL THEN
        RAISE EXCEPTION 'User profile not found' USING ERRCODE = '42501';
    END IF;

    -- 레이트 리밋: 시간당 120회
    PERFORM api._check_rate_limit(v_uid, 'bookmark_toggle', 120);

    -- POI 존재 + 공개 여부 확인 (is_publishable 체크)
    IF NOT EXISTS (
        SELECT 1 FROM service.places_snapshot
        WHERE place_id = p_poi_id AND is_publishable = TRUE
    ) THEN
        RAISE EXCEPTION 'Place not found or not published' USING ERRCODE = '22023';
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM "user".bookmarks
        WHERE user_id = v_user_id AND place_id = p_poi_id
    ) INTO v_exists;

    IF v_exists THEN
        DELETE FROM "user".bookmarks
        WHERE user_id = v_user_id AND place_id = p_poi_id;
        v_action := 'removed';
    ELSE
        INSERT INTO "user".bookmarks (user_id, place_id)
        VALUES (v_user_id, p_poi_id);
        v_action := 'added';
    END IF;

    PERFORM api._write_audit(v_uid, 'bookmark_' || v_action, p_poi_id);

    RETURN jsonb_build_object('action', v_action, 'poi_id', p_poi_id);
END;
$$;

GRANT EXECUTE ON FUNCTION api.toggle_bookmark(BIGINT) TO authenticated;
COMMENT ON FUNCTION api.toggle_bookmark IS
    '북마크 토글. 인증 필수. 레이트리밋 120/hr. POI 공개 여부 검증.';


-- ────────────────────────────────────────────────────────────────────────────
-- 7-2. 리뷰 작성/수정  api.upsert_review(p_poi_id, p_rating, p_content, p_lang)
-- 반환: {review_id: N, poi_id: N, is_update: bool}
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION api.upsert_review(
    p_poi_id  BIGINT,
    p_rating  SMALLINT,
    p_content TEXT     DEFAULT NULL,
    p_lang    TEXT     DEFAULT 'ko'
)
RETURNS JSONB
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = "user", api, public
AS $$
DECLARE
    v_uid       UUID   := auth.uid();
    v_user_id   BIGINT;
    v_review_id BIGINT;
    v_is_update BOOLEAN := FALSE;
BEGIN
    IF v_uid IS NULL THEN
        RAISE EXCEPTION 'Authentication required' USING ERRCODE = '42501';
    END IF;

    v_user_id := api.current_user_id();
    IF v_user_id IS NULL THEN
        RAISE EXCEPTION 'User profile not found' USING ERRCODE = '42501';
    END IF;

    -- 입력 검증
    IF p_rating NOT BETWEEN 1 AND 5 THEN
        RAISE EXCEPTION 'rating must be between 1 and 5' USING ERRCODE = '22023';
    END IF;
    IF p_lang NOT IN ('ko','en','ja','zh-CN','zh-TW','th','pt-BR') THEN
        RAISE EXCEPTION 'Unsupported language code' USING ERRCODE = '22023';
    END IF;
    IF p_content IS NOT NULL AND length(p_content) > 2000 THEN
        RAISE EXCEPTION 'Review content too long (max 2000 chars)' USING ERRCODE = '22023';
    END IF;

    -- 레이트 리밋: 시간당 20회
    PERFORM api._check_rate_limit(v_uid, 'review_upsert', 20);

    -- POI 공개 여부 확인
    IF NOT EXISTS (
        SELECT 1 FROM service.places_snapshot
        WHERE place_id = p_poi_id AND is_publishable = TRUE
    ) THEN
        RAISE EXCEPTION 'Place not found or not published' USING ERRCODE = '22023';
    END IF;

    SELECT EXISTS (
        SELECT 1 FROM "user".reviews
        WHERE user_id = v_user_id AND place_id = p_poi_id
    ) INTO v_is_update;

    INSERT INTO "user".reviews (user_id, place_id, rating, content, lang, updated_at)
    VALUES (v_user_id, p_poi_id, p_rating, p_content, p_lang, NOW())
    ON CONFLICT ON CONSTRAINT uq_reviews_user_place
    DO UPDATE SET
        rating     = EXCLUDED.rating,
        content    = EXCLUDED.content,
        lang       = EXCLUDED.lang,
        updated_at = NOW()
    RETURNING id INTO v_review_id;

    PERFORM api._write_audit(
        v_uid, 'review_upsert', p_poi_id,
        jsonb_build_object('review_id', v_review_id, 'rating', p_rating, 'is_update', v_is_update)
    );

    RETURN jsonb_build_object(
        'review_id', v_review_id,
        'poi_id',    p_poi_id,
        'is_update', v_is_update
    );
END;
$$;

GRANT EXECUTE ON FUNCTION api.upsert_review(BIGINT, SMALLINT, TEXT, TEXT) TO authenticated;
COMMENT ON FUNCTION api.upsert_review IS
    '리뷰 작성/수정. 사용자당 장소당 1개. 레이트리밋 20/hr. 내용 2000자 제한.';


-- ────────────────────────────────────────────────────────────────────────────
-- 7-3. 리뷰 삭제  api.delete_review(p_review_id)
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION api.delete_review(p_review_id BIGINT)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = "user", api, public
AS $$
DECLARE
    v_uid     UUID   := auth.uid();
    v_user_id BIGINT;
BEGIN
    IF v_uid IS NULL THEN
        RAISE EXCEPTION 'Authentication required' USING ERRCODE = '42501';
    END IF;

    v_user_id := api.current_user_id();
    IF v_user_id IS NULL THEN
        RAISE EXCEPTION 'User profile not found' USING ERRCODE = '42501';
    END IF;

    PERFORM api._check_rate_limit(v_uid, 'review_delete', 30);

    DELETE FROM "user".reviews
    WHERE id = p_review_id AND user_id = v_user_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Review not found or not owned by current user'
            USING ERRCODE = '42501';
    END IF;

    PERFORM api._write_audit(v_uid, 'review_delete', p_review_id);
END;
$$;

GRANT EXECUTE ON FUNCTION api.delete_review(BIGINT) TO authenticated;


-- ────────────────────────────────────────────────────────────────────────────
-- 7-4. 프로필 업데이트  api.update_profile(p_name, p_preferred_lang)
-- name, preferred_lang 필드만 허용. 민감 필드(email, password_hash, provider) 차단.
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION api.update_profile(
    p_name           TEXT DEFAULT NULL,
    p_preferred_lang TEXT DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = "user", api, public
AS $$
DECLARE
    v_uid     UUID   := auth.uid();
    v_user_id BIGINT;
BEGIN
    IF v_uid IS NULL THEN
        RAISE EXCEPTION 'Authentication required' USING ERRCODE = '42501';
    END IF;

    v_user_id := api.current_user_id();
    IF v_user_id IS NULL THEN
        RAISE EXCEPTION 'User profile not found' USING ERRCODE = '42501';
    END IF;

    IF p_preferred_lang IS NOT NULL
       AND p_preferred_lang NOT IN ('ko','en','ja','zh-CN','zh-TW','th','pt-BR') THEN
        RAISE EXCEPTION 'Unsupported language code' USING ERRCODE = '22023';
    END IF;
    IF p_name IS NOT NULL AND length(p_name) > 100 THEN
        RAISE EXCEPTION 'Name too long (max 100 chars)' USING ERRCODE = '22023';
    END IF;

    PERFORM api._check_rate_limit(v_uid, 'profile_update', 10);

    UPDATE "user".users
    SET
        name           = COALESCE(p_name, name),
        preferred_lang = COALESCE(p_preferred_lang, preferred_lang)
    WHERE id = v_user_id;

    PERFORM api._write_audit(
        v_uid, 'profile_update', NULL,
        jsonb_build_object('name_changed', p_name IS NOT NULL, 'lang_changed', p_preferred_lang IS NOT NULL)
    );
END;
$$;

GRANT EXECUTE ON FUNCTION api.update_profile(TEXT, TEXT) TO authenticated;


-- ────────────────────────────────────────────────────────────────────────────
-- 7-5. 채팅 세션 생성  api.create_chat_session(p_lang)
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION api.create_chat_session(p_lang TEXT DEFAULT 'ko')
RETURNS UUID
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ai, api, public
AS $$
DECLARE
    v_uid        UUID   := auth.uid();
    v_user_id    BIGINT;
    v_session_id UUID;
BEGIN
    IF v_uid IS NULL THEN
        RAISE EXCEPTION 'Authentication required' USING ERRCODE = '42501';
    END IF;

    v_user_id := api.current_user_id();
    IF v_user_id IS NULL THEN
        RAISE EXCEPTION 'User profile not found' USING ERRCODE = '42501';
    END IF;

    IF p_lang NOT IN ('ko','en','ja','zh-CN','zh-TW','th','pt-BR') THEN
        RAISE EXCEPTION 'Unsupported language code' USING ERRCODE = '22023';
    END IF;

    -- 세션 생성 레이트리밋: 시간당 30회
    PERFORM api._check_rate_limit(v_uid, 'chat_session_create', 30);

    INSERT INTO ai.chat_sessions (user_id, lang)
    VALUES (v_user_id, p_lang)
    RETURNING session_id INTO v_session_id;

    RETURN v_session_id;
END;
$$;

GRANT EXECUTE ON FUNCTION api.create_chat_session(TEXT) TO authenticated;


-- ────────────────────────────────────────────────────────────────────────────
-- 7-6. 채팅 메시지 추가  api.add_chat_message(p_session_id, p_role, p_content)
-- ────────────────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION api.add_chat_message(
    p_session_id UUID,
    p_role       TEXT,
    p_content    TEXT
)
RETURNS BIGINT
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = ai, api, public
AS $$
DECLARE
    v_uid    UUID   := auth.uid();
    v_msg_id BIGINT;
BEGIN
    IF v_uid IS NULL THEN
        RAISE EXCEPTION 'Authentication required' USING ERRCODE = '42501';
    END IF;

    IF p_role NOT IN ('user', 'assistant', 'tool') THEN
        RAISE EXCEPTION 'Invalid role: must be user, assistant, or tool' USING ERRCODE = '22023';
    END IF;
    IF length(p_content) > 32000 THEN
        RAISE EXCEPTION 'Message too long (max 32000 chars)' USING ERRCODE = '22023';
    END IF;

    -- 세션 소유권 확인
    IF NOT EXISTS (
        SELECT 1 FROM ai.chat_sessions
        WHERE session_id = p_session_id
          AND user_id = api.current_user_id()
    ) THEN
        RAISE EXCEPTION 'Session not found or not owned' USING ERRCODE = '42501';
    END IF;

    -- 메시지 레이트리밋: 시간당 300회
    PERFORM api._check_rate_limit(v_uid, 'chat_message_add', 300);

    INSERT INTO ai.chat_messages (session_id, role, content)
    VALUES (p_session_id, p_role, p_content)
    RETURNING id INTO v_msg_id;

    -- 세션 last_active 갱신
    UPDATE ai.chat_sessions
    SET last_active = NOW()
    WHERE session_id = p_session_id;

    RETURN v_msg_id;
END;
$$;

GRANT EXECUTE ON FUNCTION api.add_chat_message(UUID, TEXT, TEXT) TO authenticated;


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 8: 오래된 레이트 리밋 레코드 자동 삭제 함수
-- (pg_cron 또는 APScheduler에서 주기적으로 호출)
-- ══════════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE FUNCTION api.cleanup_rate_limits()
RETURNS INTEGER
LANGUAGE sql SECURITY DEFINER
SET search_path = api, public
AS $$
    WITH deleted AS (
        DELETE FROM api.rate_limit_counter
        WHERE window_start < NOW() - INTERVAL '2 hours'
        RETURNING 1
    )
    SELECT count(*)::INTEGER FROM deleted;
$$;

-- 감사 로그 90일 보관 후 자동 삭제
CREATE OR REPLACE FUNCTION api.cleanup_audit_log()
RETURNS INTEGER
LANGUAGE sql SECURITY DEFINER
SET search_path = api, public
AS $$
    WITH deleted AS (
        DELETE FROM api.audit_log
        WHERE created_at < NOW() - INTERVAL '90 days'
        RETURNING 1
    )
    SELECT count(*)::INTEGER FROM deleted;
$$;


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 9: api 스키마 보안 잠금
-- ══════════════════════════════════════════════════════════════════════════════

-- audit_log, rate_limit_counter는 클라이언트가 직접 읽을 수 없음
ALTER TABLE api.audit_log           ENABLE ROW LEVEL SECURITY;
ALTER TABLE api.rate_limit_counter  ENABLE ROW LEVEL SECURITY;
ALTER TABLE api.audit_log           FORCE ROW LEVEL SECURITY;
ALTER TABLE api.rate_limit_counter  FORCE ROW LEVEL SECURITY;
-- 정책 없음 = anon/authenticated 접근 완전 차단


-- ══════════════════════════════════════════════════════════════════════════════
-- SECTION 10: 검증 쿼리 (실행 후 결과 확인용)
-- ══════════════════════════════════════════════════════════════════════════════
-- 실행하여 모든 테이블에 RLS가 켜졌는지 확인:
--
-- SELECT schemaname, tablename, rowsecurity, forcerlspolicy
-- FROM   pg_tables
-- WHERE  schemaname IN ('stage','core','service','user','ai','api')
-- ORDER  BY schemaname, tablename;
--
-- 함수 목록 확인:
-- SELECT routine_schema, routine_name
-- FROM   information_schema.routines
-- WHERE  routine_schema IN ('api','service')
--   AND  routine_type = 'FUNCTION'
-- ORDER  BY 1, 2;
