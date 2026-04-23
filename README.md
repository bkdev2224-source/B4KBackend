# B4K Database — 실행 명령어 정리

## 환경 설정

```bash
# 가상환경 활성화
venv\Scripts\activate

# 패키지 설치 (최초 1회)
pip install -r requirements.txt
```

---

## 스키마 구조

```
stage.*     원천 RAW 데이터 보존 (수집 원본)
core.*      정규화·병합 마스터 데이터
service.*   서빙 스냅샷 (다국어 flattened, JOIN 없이 단건 조회)
user.*      사용자 계정, 북마크, 리뷰
ai.*        챗봇 세션, 임베딩, 일정
api.*       보안 레이어 (SECURITY DEFINER 함수, 감사 로그, 레이트 리밋)
```

### DDL 파일 순서

| 파일 | 내용 |
|------|------|
| `db/ddl/00_schemas.sql` | 스키마 + 익스텐션 생성 |
| `db/ddl/01_stage.sql` | stage.raw_documents, sync_runs 등 |
| `db/ddl/02_core.sql` | core.poi, poi_translations, dedup_review_queue 등 |
| `db/ddl/03_service.sql` | service.places_snapshot, search_index, 갱신 트리거 |
| `db/ddl/04_user.sql` | user.users, bookmarks, reviews / ai.* |
| `db/ddl/05_entities.sql` | core.entities, entity_* 엔티티 계열 |
| `db/ddl/06_translation.sql` | core.translation_rules, translation_glossary |
| `db/ddl/07_security.sql` | RLS, Role, SECURITY DEFINER 함수, 감사 로그 |

---

## DB 초기화 (Phase 0)

```bash
# 스키마 생성 (최초 1회, 00~07 전체 DDL 자동 적용)
python scripts/init_db.py

# 전체 리셋 후 재생성 (주의: 모든 데이터 삭제)
python scripts/init_db.py --reset
```

> **기존 DB에 신규 DDL 부분 적용 (06_translation + 07_security)**
> ```bash
> python scripts/apply_security.py          # dry-run (검증만)
> python scripts/apply_security.py --apply  # 실제 적용
> ```

---

## Phase 1 — TourAPI 파이프라인

```bash
# [1-1] CSV → stage.raw_documents
python scripts/run_phase1.py --csv C:\Project\b4kdatabase\database\csv\tourapi\tour_kor.csv

# [1-2] TourAPI sync (변경분 감지)
python scripts/run_phase1.py --sync

# [1-3] 정규화 + 도메인/지역 매핑
python scripts/run_phase1.py --normalize

# [1-7] 번역 실행
python scripts/run_phase1.py --translate

# [1-5] Cloudinary 이미지 업로드
python scripts/run_phase1.py --images

# [1-10] pgvector 임베딩 인덱스 구축
python scripts/run_phase1.py --index

# 전체 한 번에
python scripts/run_phase1.py --csv C:\Project\b4kdatabase\database\csv\tourapi\tour_kor.csv --all
```

---

## Phase 2 — MOIS 파이프라인

```bash
# [2-1] CSV → stage (database/csv/mois/ 폴더 내 전체, 영업 중인 것만 적재)
python scripts/run_phase2.py --csv "C:\Project\b4kdatabase\database\csv\mois"

# [2-4] TourAPI 데이터와 중복 검사 → 병합 또는 신규 분류
python scripts/run_phase2.py --dedup

# [2-4b] 신규 분류된 것만 정규화 + 도메인/지역 매핑
python scripts/run_phase2.py --normalize

# [2-3] MOIS history API sync (변경분 감지)
python scripts/run_phase2.py --sync

# [2-7] 번역 실행
python scripts/run_phase2.py --translate

# 테스트 (CSV + dedup + normalize 한 번에)
python scripts/run_phase2.py --csv "C:\Project\b4kdatabase\database\csv\mois" --dedup --normalize
```

---

## Dedup (중복 제거) 로직

### 자동 파이프라인 흐름

```
Step 0: 좌표 없음 → 신규 INSERT
Step 1: PostGIS 50m 반경 공간 필터 → 후보 없음이면 신규 INSERT
Step 2: 앙상블 스코어 계산
          Jaro-Winkler   × 0.40
          Token Sort     × 0.35
          자모 Levenshtein × 0.25
Step 2.5: N호점 충돌 감지 (신규 추가)
          한쪽에만 "N호점" 패턴 → 스코어 무관하게 신규 INSERT
          예) '스타벅스 강남역점' vs '스타벅스 강남역2호점' → 신규
Step 3: 임계값 분기
          score ≥ 0.92 → 자동 병합
          0.82 ≤ score < 0.92 → dedup_review_queue 등록
          score < 0.82 → 신규 INSERT
```

### 검토 큐 수동 처리

```bash
python scripts/review_dedup.py
```

큐 항목마다 아래 순서로 자동 판정 → 해당 없으면 수동 선택:

1. **N호점 충돌** — 한쪽에만 "N호점" → `AUTO-NEW` (자동 신규 등록)
2. **한글 글자 겹침 ≥ 90%** — `AUTO-MERGE` (자동 병합)
3. **나머지** — 수동 선택
   - `m` — 병합 (같은 장소, 기존 POI에 소스 연결)
   - `n` — 신규 (다른 장소, 별도 insert)
   - `s` — 스킵 (나중에 처리)
   - `q` — 종료

신규(`n`) 선택 후 정규화 필요:
```bash
python scripts/run_phase2.py --normalize
```

---

## 번역 용어집 관리

```bash
# CSV/JSON → core.translation_glossary 적재
python scripts/load_glossary.py --file path/to/glossary.csv

# 번역 규칙 확인 (Supabase SQL Editor)
SELECT * FROM core.translation_rules WHERE is_active = TRUE ORDER BY priority DESC;
SELECT * FROM core.translation_glossary WHERE lang = 'en' ORDER BY priority DESC;
```

---

## 보안 레이어 (07_security.sql)

클라이언트(anon / authenticated)에서 테이블에 직접 write 불가. 모든 쓰기는 `api.*` 함수 경유.

### 클라이언트 노출 함수 (Supabase RPC)

| 함수 | 레이트리밋 | 설명 |
|------|-----------|------|
| `api.toggle_bookmark(poi_id)` | 120/hr | 북마크 추가/삭제 토글 |
| `api.upsert_review(poi_id, rating, content, lang)` | 20/hr | 리뷰 작성/수정 |
| `api.delete_review(review_id)` | 30/hr | 리뷰 삭제 |
| `api.update_profile(name, lang)` | 10/hr | 프로필 수정 |
| `api.create_chat_session(lang)` | 30/hr | 채팅 세션 생성 |
| `api.add_chat_message(session_id, role, content)` | 300/hr | 메시지 추가 |

### 스키마별 클라이언트 접근

| 스키마 | anon | authenticated |
|--------|------|---------------|
| `stage.*` | 차단 | 차단 |
| `core.*` | 차단 | 차단 |
| `service.*` | SELECT (공개 POI만) | SELECT |
| `user.*` | 차단 | 자신의 row SELECT |
| `ai.*` | 차단 | 자신의 row SELECT |
| `api.*` | 차단 | 차단 |

### 감사 로그 / 레이트리밋 정리 (APScheduler 등록 필요)

```python
# scripts/scheduler.py 또는 APScheduler에 추가
scheduler.add_job(
    lambda: db.execute("SELECT api.cleanup_rate_limits()"),
    'interval', hours=1
)
scheduler.add_job(
    lambda: db.execute("SELECT api.cleanup_audit_log()"),
    'cron', hour=3
)
```

---

## Supabase SQL 쿼리 모음

### 현황 확인

```sql
-- 소스별 장소 수
SELECT source_ids, COUNT(*) FROM core.poi GROUP BY source_ids;

-- stage 적재 현황
SELECT s.name AS source, rd.is_processed, COUNT(*)
FROM stage.raw_documents rd
JOIN stage.api_sources s ON s.id = rd.source_id
GROUP BY s.name, rd.is_processed
ORDER BY s.name;

-- 번역 큐 현황
SELECT language_code, status, COUNT(*)
FROM core.translation_fill_queue
GROUP BY language_code, status
ORDER BY language_code, status;

-- RLS 적용 상태 전체 확인
SELECT schemaname, tablename, rowsecurity, forcerlspolicy
FROM pg_tables
WHERE schemaname IN ('stage','core','service','user','ai','api')
ORDER BY schemaname, tablename;
```

### Dedup 결과 확인

```sql
-- 병합된 장소 (여러 소스가 연결된 것)
SELECT id, name_ko, source_ids
FROM core.poi
WHERE source_ids ? 'tourapi' AND source_ids ? 'mois'
ORDER BY id;

-- 검토 큐 현황
SELECT status, COUNT(*)
FROM core.dedup_review_queue
GROUP BY status;

-- 검토 대기 항목 상세
SELECT q.id, p.name_ko AS poi_name, q.name_similarity, q.distance_m, q.created_at
FROM core.dedup_review_queue q
JOIN core.poi p ON p.id = q.poi_id_a
WHERE q.status = 'pending'
ORDER BY q.name_similarity DESC;
```

### 데이터 초기화

```sql
-- 미처리 raw_documents 재처리 (특정 소스)
UPDATE stage.raw_documents
SET is_processed = FALSE
WHERE is_processed = TRUE
  AND source_id = (SELECT id FROM stage.api_sources WHERE name = 'mois');

-- 검토 큐 초기화 (pending → 재검토)
UPDATE core.dedup_review_queue SET status = 'pending' WHERE status = 'rejected';
```

### 데이터 삭제 (FK 의존성 순)

```sql
TRUNCATE core.translation_fill_queue CASCADE;
TRUNCATE core.dedup_review_queue     CASCADE;
TRUNCATE core.poi_translations       CASCADE;
TRUNCATE service.places_snapshot     CASCADE;
TRUNCATE core.poi_images             CASCADE;
TRUNCATE core.poi_tag_map            CASCADE;
TRUNCATE core.poi                    CASCADE;
TRUNCATE stage.raw_documents         CASCADE;
```

---

## 번역 지원 언어

| 코드 | 언어 |
|------|------|
| `ko` | 한국어 (원본, 번역 대상 아님) |
| `en` | 영어 |
| `ja` | 일본어 |
| `zh-CN` | 중국어 간체 |
| `zh-TW` | 중국어 번체 |
| `th` | 태국어 |
| `pt-BR` | 포르투갈어 (브라질) |

> `en` 번역 완료 시 `service.places_snapshot.is_publishable = TRUE` 자동 설정 (트리거).

---

## 지원 언어 추가 방법

1. `core.supported_languages` — INSERT로 언어 코드 추가
2. `db/ddl/03_service.sql` — `places_snapshot` 컬럼 추가 (`name_xx`, `description_xx`)
3. `service.refresh_snapshot_for_poi()` 함수 — SELECT/UPDATE에 컬럼 추가
4. `scripts/apply_security.py --apply` — 변경 적용
