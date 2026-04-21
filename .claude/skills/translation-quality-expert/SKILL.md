---
name: translation-quality-expert
description: B4KBackend 번역 품질 전문가. 용어집(glossary)과 번역 규칙(rules)을 DB에 구축·관리하고, 번역기 프롬프트에 주입하는 방식을 설계한다. 키워드: 용어집, glossary, 번역 규칙, translation_rules, translation_glossary, 용어 통일, 번역 품질, 고유명사, 브랜드, 음식명, 관광지명, 문체, preserve, term, style, format, 규칙 추가, 규칙 수정, 용어 추가, 번역 일관성, 아티스트, kpop, kbeauty, 엔티티번역, entity_translations.
---

# 번역 품질 전문가

B4KBackend의 번역 품질을 DB에서 중앙 관리하는 전문가.  
**규칙(rules)** 과 **용어집(glossary)** 두 테이블을 운용하며, 코드 배포 없이 번역 결과를 제어한다.

## 두 테이블의 역할 분리

| 구분 | 테이블 | 목적 | 예시 |
|------|--------|------|------|
| **규칙** | `core.translation_rules` | 번역 행동 지침 — 문체·형식·보존 방침 | "Use polite formal Japanese" |
| **용어집** | `core.translation_glossary` | 단어 단위 1:1 고정 대응표 | 떡볶이 → tteokbokki |

**언제 rules를 쓰고 언제 glossary를 쓰는가:**
- 어떻게 번역할지 **방식·방침**이면 → `rules`
- 특정 단어를 **정확히 어떻게 옮길지** 고정이면 → `glossary`
- 브랜드명·고유명사 목록처럼 단어-번역 쌍이 많아지면 glossary가 훨씬 관리하기 쉽다

## 담당 파일

- `database/schema.sql` — 두 테이블 DDL (place 번역 관련)
- `db/ddl/05_entities.sql` — `core.entity_translations`, `core.entity_translation_queue` DDL
- `pipeline/translator/_utils.py` — `load_translation_rules`, `load_translation_glossary`, `load_prompt_additions`
- `pipeline/translator/gemini_translator.py` — `load_prompt_additions(lang)` 호출
- `pipeline/translator/deepseek_translator.py` — `load_prompt_additions(lang)` 호출

## 엔티티 번역 적용 범위

**glossary와 rules는 place 번역뿐 아니라 entity 번역에도 동일하게 적용된다.**  
`EntityTranslationRunner`도 동일한 `load_prompt_additions(lang)`을 사용하므로, glossary에 추가한 K-pop 아티스트명·브랜드명은 엔티티 번역 시에도 즉시 반영된다.

### 엔티티 타입별 glossary 우선순위

| 엔티티 타입 | 핵심 glossary 카테고리 | 예시 |
|------------|----------------------|------|
| `kpop_artist` | 아티스트명, 팬덤명, 앨범/곡명 | aespa → 에스파, ARMY → 아미 |
| `kbeauty_brand` | 브랜드명, 제품 라인명 | Innisfree → 이니스프리, COSRX → 코스알엑스 |
| `kdrama_show` | 드라마/영화 제목, 배역명 | 오징어게임 → Squid Game |

### K-pop 아티스트명 glossary 예시

```sql
-- K-pop 아티스트: 한국어 원문 ↔ 공식 영문 표기 (en 기준)
INSERT INTO core.translation_glossary (term_ko, lang, translation, category, note)
VALUES
  ('에스파',   'en', 'aespa',   '아티스트', '공식 영문 표기'),
  ('방탄소년단','en', 'BTS',     '아티스트', '공식 영문 약칭'),
  ('블랙핑크', 'en', 'BLACKPINK','아티스트', '공식 영문 표기'),
  ('뉴진스',   'en', 'NewJeans', '아티스트', '공식 영문 표기'),
  ('세븐틴',   'en', 'SEVENTEEN','아티스트', '공식 영문 표기');

-- 팬덤명 고정
INSERT INTO core.translation_glossary (term_ko, lang, translation, category)
VALUES
  ('아미',  'en', 'ARMY',  '팬덤'),
  ('블링크','en', 'BLINK', '팬덤'),
  ('카리나','en', 'Karina','멤버명');

-- K-beauty 브랜드
INSERT INTO core.translation_glossary (term_ko, lang, translation, category, note)
VALUES
  ('이니스프리',  'en', 'Innisfree', '브랜드', '공식 영문 브랜드명'),
  ('에뛰드하우스','en', 'Etude House','브랜드', '공식 영문 브랜드명'),
  ('설화수',      'en', 'Sulwhasoo', '브랜드', '공식 영문 브랜드명'),
  ('코스알엑스',  'en', 'COSRX',     '브랜드', '공식 영문 약칭');
```

### 아티스트명 preserve rule (전 언어 공통)

```sql
-- 영문 아티스트명을 다른 언어 번역 시에도 원문 유지
INSERT INTO core.translation_rules (rule_type, lang, rule_text, priority)
VALUES
  ('preserve', NULL,
   'For K-pop group or artist names that have an official English name (e.g., aespa, BTS, BLACKPINK, NewJeans), keep the official English name regardless of target language unless a widely-used localized form exists.',
   9);

-- K-drama 공식 제목 보존
INSERT INTO core.translation_rules (rule_type, lang, rule_text, priority)
VALUES
  ('preserve', NULL,
   'For K-drama or K-movie titles with an official English title registered on Netflix/Disney+, use the official English title (e.g., 오징어게임 → Squid Game, 이상한 변호사 우영우 → Extraordinary Attorney Woo).',
   9);
```

## core.translation_rules 스키마

```sql
rule_type  TEXT   -- 'term' | 'style' | 'format' | 'preserve'
lang       TEXT   -- NULL = 전 언어 공통, 'en'/'ja' 등 = 언어별 전용
rule_text  TEXT   -- 프롬프트에 직접 삽입할 영어 규칙 문장
example    TEXT   -- 예시 (선택)
priority   SMALLINT  -- 높을수록 먼저 삽입
is_active  BOOLEAN
```

### rule_type 가이드

| type | 사용 상황 | 예시 |
|------|-----------|------|
| `preserve` | 원문 유지·그대로 사용 | 브랜드명 한글 그대로 |
| `term` | 특정 단어의 번역 방식 지정 | 떡볶이→tteokbokki |
| `style` | 문체·어조 지침 | 일본어 경어체 |
| `format` | 출력 형식 | 괄호 표기, 로마자 병기 |

### rules SQL 예시

```sql
-- 전 언어 공통: 한국 지명 로마자 표기 기준 적용
INSERT INTO core.translation_rules (rule_type, lang, rule_text, priority)
VALUES ('format', NULL, 'For Korean place names, use the official Revised Romanization of Korean (e.g., Gyeongbokgung, not Kyungbokgung)', 10);

-- 일본어 전용: 경어체
INSERT INTO core.translation_rules (rule_type, lang, rule_text, priority)
VALUES ('style', 'ja', 'Use polite formal Japanese (敬語・丁寧語体) throughout', 8);

-- 영어 전용: 음식명 로마자 표기
INSERT INTO core.translation_rules (rule_type, lang, rule_text, example, priority)
VALUES ('term', 'en', 'Use McCune–Reischauer or standard romanization for food names not in the glossary', '순대 → sundae (not soondae)', 5);

-- 전 언어 공통: 가게 이름 원문 병기
INSERT INTO core.translation_rules (rule_type, lang, rule_text, priority)
VALUES ('preserve', NULL, 'Keep the Korean store/restaurant name in parentheses after translation when translating the name field (e.g., "Myeongdong Gyoja (명동교자)")', 7);
```

## core.translation_glossary 스키마

```sql
term_ko     TEXT NOT NULL   -- 한국어 원문 표현
lang        TEXT NOT NULL   -- 대상 언어
translation TEXT NOT NULL   -- 고정 번역어
category    TEXT            -- '음식' | '관광지' | '브랜드' | '행정구역' | '시설' 등
note        TEXT            -- 관리자 메모
priority    SMALLINT        -- 높을수록 먼저 주입
is_active   BOOLEAN
UNIQUE (term_ko, lang)
```

### glossary SQL 예시

```sql
-- 음식명 영어 고정
INSERT INTO core.translation_glossary (term_ko, lang, translation, category)
VALUES
  ('떡볶이', 'en', 'tteokbokki', '음식'),
  ('삼겹살', 'en', 'samgyeopsal', '음식'),
  ('비빔밥', 'en', 'bibimbap', '음식'),
  ('순두부찌개', 'en', 'sundubu-jjigae', '음식'),
  ('막걸리', 'en', 'makgeolli', '음식');

-- 관광지 일본어 고정
INSERT INTO core.translation_glossary (term_ko, lang, translation, category)
VALUES
  ('경복궁', 'ja', '景福宮（キョンボックン）', '관광지'),
  ('남산타워', 'ja', 'ソウルタワー（南山）', '관광지');

-- 행정구역 중국어(간체) 표기
INSERT INTO core.translation_glossary (term_ko, lang, translation, category)
VALUES
  ('서울특별시', 'zh-CN', '首尔特别市', '행정구역'),
  ('제주도', 'zh-CN', '济州岛', '행정구역');

-- 특정 브랜드는 전 언어 공통으로 영문명 유지
-- (전 언어를 한 번에 처리하려면 언어별로 각각 삽입해야 함)
INSERT INTO core.translation_glossary (term_ko, lang, translation, category, note)
VALUES
  ('스타벅스', 'en', 'Starbucks', '브랜드', '공식 영문 브랜드명'),
  ('스타벅스', 'ja', 'スターバックス', '브랜드', '공식 일본어 브랜드명'),
  ('스타벅스', 'zh-CN', '星巴克', '브랜드', '공식 중문 브랜드명');
```

### 모든 언어에 glossary 항목 일괄 삽입 (헬퍼 쿼리)

```sql
-- 음식명을 6개 언어에 한꺼번에 추가할 때
INSERT INTO core.translation_glossary (term_ko, lang, translation, category)
VALUES
  ('삼겹살', 'en',    'samgyeopsal',    '음식'),
  ('삼겹살', 'ja',    '三枚肉（サムギョプサル）', '음식'),
  ('삼겹살', 'zh-CN', '五花肉',          '음식'),
  ('삼겹살', 'zh-TW', '五花肉',          '음식'),
  ('삼겹살', 'th',    'ซัมยอปซัล',       '음식'),
  ('삼겹살', 'pt-BR', 'samgyeopsal',    '음식')
ON CONFLICT (term_ko, lang) DO UPDATE
  SET translation = EXCLUDED.translation,
      is_active   = TRUE;
```

## 프롬프트 주입 순서 (번역기 내부)

```
시스템 프롬프트
  + load_translation_rules(lang)   ← 행동 지침 (rules)
  + load_translation_glossary(lang) ← 단어 고정 대응표 (glossary)
```

`load_prompt_additions(lang)` 한 번 호출로 둘 다 합쳐서 반환됨.

## 번역 규칙·용어집 관리 원칙

1. **규칙 문장은 반드시 영어로** — LLM이 영어 지시를 더 정확히 따른다
2. **glossary 항목은 구체적으로** — "한식당" 같은 상위 카테고리보다 "삼겹살", "순대" 같은 구체 단어
3. **priority는 0~10 범위** — 중요한 규칙은 8~10, 일반 규칙은 0~5
4. **is_active = FALSE로 비활성화** — 삭제 대신 비활성화해 이력 보존
5. **lang = NULL (rules 전용)** — glossary는 항상 언어 명시 필수

## 번역 품질 점검 쿼리

```sql
-- 활성 규칙 전체 조회
SELECT rule_type, lang, priority, rule_text
  FROM core.translation_rules
 WHERE is_active ORDER BY priority DESC, lang NULLS FIRST;

-- 용어집 카테고리별 집계
SELECT category, lang, COUNT(*) AS cnt
  FROM core.translation_glossary
 WHERE is_active
 GROUP BY category, lang
 ORDER BY category, lang;

-- 특정 단어의 모든 언어 번역 확인
SELECT lang, translation, category, note
  FROM core.translation_glossary
 WHERE term_ko = '떡볶이' AND is_active
 ORDER BY lang;

-- glossary에 있지만 일부 언어 누락된 단어 찾기
SELECT term_ko, COUNT(DISTINCT lang) AS lang_count
  FROM core.translation_glossary
 WHERE is_active
 GROUP BY term_ko
 HAVING COUNT(DISTINCT lang) < 6
 ORDER BY lang_count;
```

## 고도화 포인트

1. **카테고리별 자동 제안** — 새 place가 추가될 때 name에서 glossary 미등록 단어 추출
2. **번역 일관성 검증** — place_translations와 glossary를 JOIN해 용어 위반 케이스 탐지
3. **번역가 리뷰 UI** — Supabase 대시보드 + RLS로 외부 번역가가 glossary 제안/승인
4. **우선순위 자동 조정** — 빈출 단어에 높은 priority 자동 부여
