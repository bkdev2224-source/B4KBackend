-- 03_service.sql: Service 스냅샷 레이어 (v3)
-- core.poi 기준으로 다국어 flattened 스냅샷 제공

-- ─────────────────────────────────────────
-- 서비스 스냅샷 (JOIN 없이 단건 조회)
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS service.places_snapshot (
    place_id           BIGINT       PRIMARY KEY REFERENCES core.poi(id) ON DELETE CASCADE,
    name_ko            TEXT,
    name_en            TEXT,
    name_ja            TEXT,
    name_zh_cn         TEXT,
    name_zh_tw         TEXT,
    name_th            TEXT,
    name_pt_br         TEXT,
    address_ko         TEXT,
    address_en         TEXT,
    description_ko     TEXT,
    description_en     TEXT,
    description_ja     TEXT,
    description_zh_cn  TEXT,
    description_zh_tw  TEXT,
    description_th     TEXT,
    description_pt_br  TEXT,
    coords_lat         FLOAT,
    coords_lng         FLOAT,
    display_domain     TEXT,                   -- 주 도메인 (1개)
    domains            TEXT[],                 -- 전체 도메인 배열 (display_domain + 태그 카테고리)
    display_region     TEXT,
    source_category    TEXT,
    quality_score      NUMERIC(3,2),
    primary_image_url  TEXT,
    is_publishable     BOOLEAN      NOT NULL DEFAULT FALSE,
    updated_at         TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_snapshot_domain
    ON service.places_snapshot (display_domain) WHERE is_publishable = TRUE;
CREATE INDEX IF NOT EXISTS idx_snapshot_domains
    ON service.places_snapshot USING GIN (domains);
CREATE INDEX IF NOT EXISTS idx_snapshot_region
    ON service.places_snapshot (display_region) WHERE is_publishable = TRUE;
CREATE INDEX IF NOT EXISTS idx_snapshot_quality
    ON service.places_snapshot (quality_score DESC NULLS LAST);

-- ─────────────────────────────────────────
-- pgvector 임베딩 검색 인덱스
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS service.search_index (
    place_id   BIGINT       PRIMARY KEY REFERENCES core.poi(id) ON DELETE CASCADE,
    embedding  vector(1536),
    indexed_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_search_embedding
    ON service.search_index USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);

-- ─────────────────────────────────────────
-- 스냅샷 갱신 함수
-- poi_translations INSERT/UPDATE 및 poi_tag_map 변경 시 호출
-- ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION service.refresh_snapshot_for_poi(p_poi_id BIGINT)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
    v_poi     core.poi%ROWTYPE;
    v_img_url TEXT;
    v_quality NUMERIC(3,2);
    v_domains TEXT[];
BEGIN
    SELECT * INTO v_poi FROM core.poi WHERE id = p_poi_id;
    IF NOT FOUND THEN RETURN; END IF;

    -- quality VARCHAR → NUMERIC 변환 (API 호환)
    v_quality := CASE v_poi.quality
        WHEN 'full'    THEN 1.00
        WHEN 'partial' THEN 0.50
        ELSE 0.00
    END;

    SELECT secure_url INTO v_img_url
      FROM core.poi_images
     WHERE poi_id = p_poi_id AND is_primary = TRUE AND upload_status = 'uploaded'
     LIMIT 1;

    -- domains 배열: display_domain(주 도메인) + poi_tag_map 태그 카테고리 합산
    SELECT ARRAY_AGG(DISTINCT cat ORDER BY cat) INTO v_domains
    FROM (
        SELECT v_poi.display_domain AS cat
        WHERE  v_poi.display_domain IS NOT NULL
        UNION ALL
        SELECT t.category
          FROM core.k_culture_tags t
          JOIN core.poi_tag_map m ON m.tag_id = t.id
         WHERE m.poi_id = p_poi_id
           AND t.category IS NOT NULL
    ) sub;

    INSERT INTO service.places_snapshot (
        place_id, display_domain, domains, display_region, source_category,
        quality_score, is_publishable, primary_image_url, updated_at,
        coords_lat, coords_lng,
        name_ko,    name_en,    name_ja,    name_zh_cn, name_zh_tw, name_th,    name_pt_br,
        address_ko, address_en,
        description_ko, description_en, description_ja,
        description_zh_cn, description_zh_tw, description_th, description_pt_br
    )
    SELECT
        p_poi_id,
        v_poi.display_domain,
        v_domains,
        v_poi.display_region,
        v_poi.category_code,
        v_quality,
        (MAX(CASE WHEN t.language_code = 'en' THEN t.name END) IS NOT NULL),
        v_img_url,
        NOW(),
        CASE WHEN v_poi.geom IS NOT NULL THEN ST_Y(v_poi.geom) ELSE NULL END,
        CASE WHEN v_poi.geom IS NOT NULL THEN ST_X(v_poi.geom) ELSE NULL END,
        v_poi.name_ko,
        MAX(CASE WHEN t.language_code = 'en'    THEN t.name END),
        MAX(CASE WHEN t.language_code = 'ja'    THEN t.name END),
        MAX(CASE WHEN t.language_code = 'zh-CN' THEN t.name END),
        MAX(CASE WHEN t.language_code = 'zh-TW' THEN t.name END),
        MAX(CASE WHEN t.language_code = 'th'    THEN t.name END),
        MAX(CASE WHEN t.language_code = 'pt-BR' THEN t.name END),
        v_poi.address_ko,
        MAX(CASE WHEN t.language_code = 'en'    THEN t.address END),
        MAX(CASE WHEN t.language_code = 'ko'    THEN t.description END),
        MAX(CASE WHEN t.language_code = 'en'    THEN t.description END),
        MAX(CASE WHEN t.language_code = 'ja'    THEN t.description END),
        MAX(CASE WHEN t.language_code = 'zh-CN' THEN t.description END),
        MAX(CASE WHEN t.language_code = 'zh-TW' THEN t.description END),
        MAX(CASE WHEN t.language_code = 'th'    THEN t.description END),
        MAX(CASE WHEN t.language_code = 'pt-BR' THEN t.description END)
    FROM core.poi_translations t
    WHERE t.poi_id = p_poi_id
    ON CONFLICT (place_id) DO UPDATE SET
        name_ko           = EXCLUDED.name_ko,
        name_en           = EXCLUDED.name_en,
        name_ja           = EXCLUDED.name_ja,
        name_zh_cn        = EXCLUDED.name_zh_cn,
        name_zh_tw        = EXCLUDED.name_zh_tw,
        name_th           = EXCLUDED.name_th,
        name_pt_br        = EXCLUDED.name_pt_br,
        address_ko        = EXCLUDED.address_ko,
        address_en        = EXCLUDED.address_en,
        description_ko    = EXCLUDED.description_ko,
        description_en    = EXCLUDED.description_en,
        description_ja    = EXCLUDED.description_ja,
        description_zh_cn = EXCLUDED.description_zh_cn,
        description_zh_tw = EXCLUDED.description_zh_tw,
        description_th    = EXCLUDED.description_th,
        description_pt_br = EXCLUDED.description_pt_br,
        display_domain    = EXCLUDED.display_domain,
        domains           = EXCLUDED.domains,
        display_region    = EXCLUDED.display_region,
        source_category   = EXCLUDED.source_category,
        quality_score     = EXCLUDED.quality_score,
        is_publishable    = EXCLUDED.is_publishable,
        primary_image_url = EXCLUDED.primary_image_url,
        updated_at        = NOW();
END;
$$;

-- poi_translations 변경 시 스냅샷 갱신
CREATE OR REPLACE FUNCTION service.trg_refresh_snapshot()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM service.refresh_snapshot_for_poi(NEW.poi_id);
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_translation_snapshot ON core.poi_translations;
CREATE TRIGGER trg_translation_snapshot
    AFTER INSERT OR UPDATE ON core.poi_translations
    FOR EACH ROW EXECUTE FUNCTION service.trg_refresh_snapshot();

-- poi_tag_map 변경 시 스냅샷 domains 재계산
CREATE OR REPLACE FUNCTION service.trg_refresh_snapshot_on_tag()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    PERFORM service.refresh_snapshot_for_poi(
        CASE TG_OP WHEN 'DELETE' THEN OLD.poi_id ELSE NEW.poi_id END
    );
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_tag_map_snapshot ON core.poi_tag_map;
CREATE TRIGGER trg_tag_map_snapshot
    AFTER INSERT OR UPDATE OR DELETE ON core.poi_tag_map
    FOR EACH ROW EXECUTE FUNCTION service.trg_refresh_snapshot_on_tag();

-- en 번역 완료 시 is_publishable 자동 갱신
CREATE OR REPLACE FUNCTION service.trg_set_publishable()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.language_code = 'en' AND NEW.name IS NOT NULL THEN
        UPDATE service.places_snapshot
           SET is_publishable = TRUE
         WHERE place_id = NEW.poi_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_en_publishable ON core.poi_translations;
CREATE TRIGGER trg_en_publishable
    AFTER INSERT OR UPDATE ON core.poi_translations
    FOR EACH ROW EXECUTE FUNCTION service.trg_set_publishable();
