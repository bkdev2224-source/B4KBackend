---
name: api-expert
description: B4KBackend API 전문가. Supabase API(PostgREST), RLS, 인증, 프론트엔드 CRUD, pgvector 벡터검색, DB 함수/뷰 설계 관련 작업 시 즉시 트리거. FastAPI는 사용하지 않음. 키워드: supabase, api, rls, row level security, postgrest, 인증, auth, 프론트엔드, frontend, CRUD, vector, 벡터검색, 챗봇, 뷰, function, 정책, policy.
---

# Supabase API 전문가

B4KBackend는 FastAPI를 사용하지 않는다. **Supabase가 제공하는 PostgREST 기반 자동 REST API와 Supabase 클라이언트를 통해 프론트엔드가 직접 CRUD하고, AI 챗봇이 벡터 데이터에 접근한다.**

별도 API 서버를 운영하지 않으므로, API 레이어 설계는 곧 **DB 스키마 + RLS 정책 + DB 함수 설계**다.

## 접근 주체별 역할

| 주체 | 접근 방식 | 담당 데이터 |
|------|-----------|------------|
| **Frontend** | Supabase JS 클라이언트 (anon key) | places 조회, 북마크, 리뷰, 유저 인증 |
| **AI 챗봇** (별도 레포) | Supabase 클라이언트 (service role key) | `service.search_index` 벡터 검색, `ai.*` 읽기/쓰기 |
| **Backend 파이프라인** | 직접 DB 연결 (psycopg2) | 수집·정규화·번역·이미지 등 ETL |

## Supabase API 동작 원리

PostgREST는 스키마를 자동 분석해 REST 엔드포인트를 생성한다.
- `GET  /rest/v1/places_snapshot` — service.places_snapshot 조회
- `POST /rest/v1/bookmarks` — user.bookmarks 삽입
- `POST /rpc/match_places` — DB 함수 호출 (벡터 검색 등)

**노출 대상 스키마 설정:** Supabase 대시보드 → API → Exposed Schemas에 `service`, `"user"` 추가. `stage`, `core`는 절대 노출하지 않는다 (내부 ETL 전용).

## RLS (Row Level Security)

프론트엔드는 anon/authenticated key만 사용하므로 RLS가 보안의 전부다. RLS 없이 테이블을 노출하면 전체 데이터가 공개된다.

**핵심 정책 설계 방향:**
```sql
-- places_snapshot: 모든 사용자 읽기 허용 (공개 데이터)
CREATE POLICY "public read" ON service.places_snapshot
  FOR SELECT USING (is_publishable = TRUE);

-- bookmarks: 본인 데이터만
CREATE POLICY "own bookmarks" ON "user".bookmarks
  FOR ALL USING (user_id = auth.uid());

-- reviews: 읽기는 공개, 쓰기는 본인만
CREATE POLICY "public read reviews" ON "user".reviews
  FOR SELECT USING (TRUE);
CREATE POLICY "own write reviews" ON "user".reviews
  FOR INSERT WITH CHECK (user_id = auth.uid());
```

## 벡터 검색 (AI 챗봇용)

챗봇은 `service.search_index`의 pgvector를 직접 쿼리한다. PostgREST에서 벡터 검색은 DB 함수(RPC)로 노출한다.

```sql
-- 챗봇이 호출할 벡터 검색 함수
CREATE OR REPLACE FUNCTION match_places(
  query_embedding vector(1536),
  match_count     int DEFAULT 5,
  filter_domain   text DEFAULT NULL,
  filter_region   text DEFAULT NULL
)
RETURNS TABLE (place_id bigint, name text, address text,
               display_domain text, display_region text, similarity float)
LANGUAGE sql AS $$
  SELECT p.place_id, p.name, p.address,
         p.display_domain, p.display_region,
         1 - (si.embedding <=> query_embedding) AS similarity
    FROM service.search_index si
    JOIN core.places p ON p.place_id = si.place_id
   WHERE p.is_publishable = TRUE
     AND (filter_domain IS NULL OR p.display_domain = filter_domain)
     AND (filter_region IS NULL OR p.display_region = filter_region)
   ORDER BY si.embedding <=> query_embedding
   LIMIT match_count;
$$;
```

챗봇은 `supabase.rpc('match_places', { query_embedding: [...], match_count: 5 })`로 호출.

## 인증

Supabase Auth를 사용한다 (JWT 직접 구현 불필요).
- 프론트엔드: `supabase.auth.signUp()`, `supabase.auth.signInWithPassword()`
- `auth.uid()` — RLS 정책에서 현재 로그인 유저 ID 참조
- `"user".users` 테이블과 `auth.users` (Supabase 내장) 연동 고려

## 엔티티 노출 (API 관점)

`core.entities`와 관련 테이블(`entity_aliases`, `entity_sns`, `entity_images`, `entity_translations`)은 현재 **노출 스키마에 포함되지 않는다** (core 스키마는 ETL 전용).

엔티티를 프론트엔드/챗봇에 노출하려면:
1. `service` 스키마에 엔티티 스냅샷 뷰 또는 테이블 추가
2. 또는 RPC 함수로 JOIN 결과 반환

### 엔티티 검색 RPC 예시

```sql
-- 챗봇이 엔티티 연관 POI를 찾을 때 호출
CREATE OR REPLACE FUNCTION match_entities(
  query_embedding vector(1536),
  match_count     int DEFAULT 5,
  entity_type_filter text DEFAULT NULL
)
RETURNS TABLE (
  entity_id   int,
  canonical_name text,
  entity_type text,
  similarity  float
)
LANGUAGE sql AS $$
  SELECT e.id, e.canonical_name, e.entity_type,
         1 - (ei.embedding <=> query_embedding) AS similarity
    FROM core.entities e
    JOIN service.search_index ei ON ei.entity_id = e.id
   WHERE e.is_active = TRUE
     AND (entity_type_filter IS NULL OR e.entity_type = entity_type_filter)
   ORDER BY ei.embedding <=> query_embedding
   LIMIT match_count;
$$;

-- 특정 엔티티와 연결된 POI 조회
CREATE OR REPLACE FUNCTION get_entity_pois(p_entity_id int)
RETURNS TABLE (
  place_id bigint, name text, address text,
  relation text, display_domain text
)
LANGUAGE sql AS $$
  SELECT ps.place_id, ps.name_ko, ps.address_ko,
         pem.relation, ps.display_domain
    FROM core.poi_entity_map pem
    JOIN service.places_snapshot ps ON ps.place_id = pem.poi_id
   WHERE pem.entity_id = p_entity_id
     AND ps.is_publishable = TRUE
   ORDER BY ps.name_ko;
$$;
```

## 작업 시작 시 읽을 파일

```
database/schema.sql    (service.*, user.*, ai.* 섹션)
db/ddl/05_entities.sql (엔티티 구조 확인 시)
```
RLS 정책과 DB 함수는 schema.sql 또는 별도 `database/rls.sql`, `database/functions.sql`에 관리한다.

## 고도화 포인트

1. **RLS 정책 작성** — 현재 schema.sql에 RLS 정책 없음. 테이블 노출 전 필수.
2. **match_places 함수 추가** — 챗봇용 벡터 검색 RPC 함수 schema.sql에 추가 필요.
3. **Exposed Schemas 설정** — stage·core는 절대 노출 금지. service·user만 노출.
4. **places_snapshot 다국어 필터** — lang 파라미터 처리를 DB 함수나 뷰로 추상화.
5. **api/ 디렉토리 정리** — 현재 FastAPI 코드(api/main.py, api/routes/)가 남아있음. 제거 대상.
6. **챗봇 service role key 관리** — 챗봇 레포에서 service role key 사용 시 서버사이드에서만 사용 (클라이언트 노출 금지).
7. **엔티티 서비스 뷰** — `core.entities`를 service 스키마로 노출하는 뷰/스냅샷 테이블 미작성. `match_entities`, `get_entity_pois` RPC 함수 추가 필요.
8. **poi_entity_map 노출** — 특정 POI와 연관된 아티스트/브랜드 목록 조회 RPC 부재.

## 응답 스타일

- FastAPI/Pydantic 코드를 새로 작성하지 않는다
- API 설계 = SQL(RLS 정책 + DB 함수 + 뷰) 설계로 접근
- 프론트엔드 연동은 Supabase JS 클라이언트 코드 예시로 제시
- 챗봇 연동은 Python supabase 클라이언트 또는 직접 REST 호출 예시로 제시
- 보안 관련 변경은 RLS 정책 SQL로 구체적으로 제시
