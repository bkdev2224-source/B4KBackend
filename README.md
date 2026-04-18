# B4K Database — 실행 명령어 정리

## 환경 설정

```bash
# 가상환경 활성화
venv\Scripts\activate

# 패키지 설치 (최초 1회)
pip install -r requirements.txt
```

---

## DB 초기화 (Phase 0)

> 데이터 삭제가 필요한 경우 Supabase 대시보드에서 아래 순서로 Truncate 먼저 실행
> 1. `core.translation_fill_queue`
> 2. `core.place_translations`
> 3. `service.places_snapshot`
> 4. `core.place_images`
> 5. `core.place_source_ids`
> 6. `core.places`
> 7. `stage.raw_documents`

```bash
# 스키마 생성 (최초 1회 또는 Truncate 후)
python scripts/init_db.py
```

---

## Phase 1 — TourAPI 파이프라인

### 단계별 실행

```bash
# [1-1] CSV → stage.raw_documents
python scripts/run_phase1.py --csv C:\Project\b4kdatabase\database\csv\tourapi\tour_kor.csv

# [1-2] TourAPI sync (변경분 감지)
python scripts/run_phase1.py --sync

# [1-3] 정규화 + 도메인/지역 매핑 (1-4a, 1-4b 포함)
python scripts/run_phase1.py --normalize

# [1-7] 번역 Batch 제출 (OpenAI 키 필요)
python scripts/run_phase1.py --translate

# [1-7] 번역 결과 수거
python scripts/run_phase1.py --collect

# [1-5] Cloudinary 이미지 업로드
python scripts/run_phase1.py --images

# [1-10] pgvector 임베딩 인덱스 구축
python scripts/run_phase1.py --index
```

### 전체 한 번에 실행 (OpenAI 키 있을 때)

```bash
python scripts/run_phase1.py --csv C:\Project\b4kdatabase\database\csv\tourapi\tour_kor.csv --all
```

---

## Phase 2 — MOIS 파이프라인

> CSV 파일은 `database/csv/mois/` 폴더에 넣으면 자동으로 전체 처리됩니다.

### 단계별 실행

```bash
# [2-1] CSV → stage (폴더 내 모든 CSV 처리, 영업 중인 것만 적재)
python scripts/run_phase2.py --csv "C:\Project\b4kdatabase\database\csv\mois"

# [2-4] TourAPI 데이터와 중복 검사 → 병합 또는 신규 분류
python scripts/run_phase2.py --dedup

# [2-4b] 신규 분류된 것만 core 정규화 + 도메인/지역 매핑
python scripts/run_phase2.py --normalize

# [2-3] MOIS history API sync (변경분 감지, MOIS_API_KEY 필요)
python scripts/run_phase2.py --sync

# [2-7] 번역 Batch 제출 (OpenAI 키 필요)
python scripts/run_phase2.py --translate

# [2-7] 번역 결과 수거
python scripts/run_phase2.py --collect
```

### Dedup 검토 큐 수동 처리

```bash
python scripts/review_dedup.py
```

각 항목마다 기존 TourAPI 장소와 신규 MOIS 데이터를 나란히 보여주고 선택:
- `m` — 병합 (같은 장소, 기존에 MOIS 소스 연결)
- `n` — 신규 (다른 장소, 별도 insert)
- `s` — 스킵 (나중에 처리)
- `q` — 종료

신규(`n`) 선택 후 정규화 필요:
```bash
python scripts/run_phase2.py --normalize
```

### 테스트 (CSV 이식 + 중복 검사 + 정규화 한 번에)

```bash
python scripts/run_phase2.py --csv "C:\Project\b4kdatabase\database\csv\mois" --dedup --normalize
```

### 결과 확인 (Supabase SQL Editor)

```sql
-- 소스별 장소 수
SELECT source_name, COUNT(*) FROM core.places GROUP BY source_name;

-- MOIS에서 TourAPI와 병합된 장소
SELECT p.place_id, p.name, s.source_name
FROM core.places p
JOIN core.place_source_ids s ON p.place_id = s.place_id
WHERE s.source_name = 'mois'
LIMIT 20;

-- stage 적재 현황 (영업 필터링 후 결과)
SELECT sync_status, COUNT(*)
FROM stage.raw_documents
WHERE source_name = 'mois'
GROUP BY sync_status;
```

---

## Supabase SQL 쿼리 모음

### 현황 확인

```sql
-- 소스별 장소 수
SELECT source_name, COUNT(*) FROM core.places GROUP BY source_name;

-- stage 적재 현황 (소스별 sync_status)
SELECT source_name, sync_status, COUNT(*)
FROM stage.raw_documents
GROUP BY source_name, sync_status
ORDER BY source_name, sync_status;

-- 번역 큐 현황
SELECT lang, status, COUNT(*)
FROM core.translation_fill_queue
GROUP BY lang, status
ORDER BY lang, status;
```

### Dedup 결과 확인

```sql
-- 병합된 장소 (여러 소스가 연결된 것)
SELECT
    p.place_id,
    p.name,
    p.address,
    STRING_AGG(s.source_name || ':' || s.source_id, ' | ') AS sources
FROM core.places p
JOIN core.place_source_ids s ON p.place_id = s.place_id
GROUP BY p.place_id, p.name, p.address
HAVING COUNT(DISTINCT s.source_name) > 1
ORDER BY p.place_id;

-- 검토 큐 (score 0.82~0.92, 수동 확인 필요)
SELECT place_id, name, address, source_name, dedup_status
FROM core.places
WHERE dedup_status = 'review'
ORDER BY place_id;
```

### 데이터 초기화

```sql
-- 에러 상태 행 재처리 (특정 소스)
UPDATE stage.raw_documents
SET sync_status = 'new', processed_at = NULL
WHERE sync_status = 'error'
  AND source_name = 'mois';  -- 전체: AND 조건 제거

-- 검토 큐 초기화
UPDATE core.places SET dedup_status = NULL WHERE dedup_status = 'review';
```

### 데이터 삭제 (Truncate 순서 — FK 의존성 순)

```sql
TRUNCATE core.translation_fill_queue CASCADE;
TRUNCATE core.place_translations     CASCADE;
TRUNCATE service.places_snapshot     CASCADE;
TRUNCATE core.place_images           CASCADE;
TRUNCATE core.place_source_ids       CASCADE;
TRUNCATE core.places                 CASCADE;
TRUNCATE stage.raw_documents         CASCADE;
```

---

## 재실행 시 (에러 상태 행 초기화)

Supabase SQL Editor에서 실행:

```sql
UPDATE stage.raw_documents
SET sync_status = 'new', processed_at = NULL
WHERE sync_status = 'error';
```

---

## 번역 언어

| 코드 | 언어 |
|------|------|
| `en` | 영어 |
| `ja` | 일본어 |
| `zh-CN` | 중국어 간체 |
| `zh-TW` | 중국어 번체 |
| `th` | 태국어 |

> 한국어(`ko`)는 원본 언어로 번역 대상이 아닙니다. `core.places`에서 직접 snapshot에 저장됩니다.

---

## 지원 언어 추가 방법

1. `config/settings.py` — `supported_languages` 리스트에 언어 코드 추가
2. `database/schema.sql` — `service.places_snapshot` 컬럼 추가 (`name_xx`, `description_xx`)
3. `database/schema.sql` — `refresh_snapshot_for_place()` 함수 INSERT/UPDATE에 컬럼 추가
4. `api/routes/places.py` — `LANG_COL`, `DESC_COL` 딕셔너리에 추가
5. DB 리셋 후 재초기화
