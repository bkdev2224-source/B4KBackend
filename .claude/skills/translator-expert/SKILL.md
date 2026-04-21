---
name: translator-expert
description: B4KBackend 번역 전문가. 주소정보누리집/DeepSeek/Gemini 번역, 토큰 관리, 번역 규칙 DB, 다국어 번역 큐, place_translations, 번역 프롬프트 최적화 관련 작업 시 즉시 트리거. 키워드: 번역, translation, juso, deepseek, gemini, place_translations, 다국어, multilingual, 큐, pending, 주소번역, 도로명, 토큰, token, 규칙, rules, 브라질, pt-BR.
---

# 번역 전문가

B4KBackend의 다중 공급자 번역 파이프라인 전문가. 토큰 효율, DB 규칙 주입, 다중 place 배치를 깊이 이해한다.

## 번역 공급자 분류

| 대상 | 언어 | 공급자 | 파일 |
|------|------|--------|------|
| **도로명 주소** | ko → en | 주소정보누리집 API | `juso_translator.py` |
| **name · description** | zh-CN, zh-TW | DeepSeek (`deepseek-chat`) | `deepseek_translator.py` |
| **name · description** | en, ja, th, pt-BR | Gemini (`gemini-2.5-flash`) | `gemini_translator.py` |

**도로명 주소는 ko + en만 보유한다.** 다른 언어는 주소 번역하지 않음.

## 담당 파일

- `pipeline/translator/batch_translator.py` — TranslationOrchestrator (3개 번역기 순서 실행)
- `pipeline/translator/juso_translator.py` — JusoAddressTranslator
- `pipeline/translator/deepseek_translator.py` — DeepSeekTranslator
- `pipeline/translator/gemini_translator.py` — GeminiBatchTranslator
- `pipeline/translator/_utils.py` — 공용 유틸 (토큰 추정, 청크 분할, 규칙 로드)

## 토큰 관리 아키텍처

**핵심 원칙: 1 API 호출로 N개 place 동시 번역**

기존 방식 (비효율):
```
호출 1: 시스템프롬프트(300토큰) + place_A(100토큰) → 400토큰
호출 2: 시스템프롬프트(300토큰) + place_B(100토큰) → 400토큰
100개 = 40,000토큰
```

배치 방식 (현재):
```
호출 1: 시스템프롬프트(300토큰) + place_A~Z(50개×100토큰) → 5,300토큰
100개 = 2회 호출 × 5,300토큰 = 10,600토큰  (74% 절감)
```

**배치 입력 형식:**
```json
{
  "12345": {"name": "카페베네", "description": "한국의 대표 커피 전문점"},
  "12346": {"name": "명동교자", "description": "국수와 만두로 유명"}
}
```

**배치 출력 형식 (동일 키 유지):**
```json
{
  "12345": {"name": "Caffe Bene", "description": "A representative Korean coffee shop"},
  "12346": {"name": "Myeongdong Gyoja", "description": "Famous for noodles and dumplings"}
}
```

**토큰 예산 분할 (`_utils.split_by_token_budget`):**
```
FIXED_OVERHEAD = 500토큰 (시스템 프롬프트 + JSON 구조)
토큰 추정 = len(text) // 2  (한국어 1토큰≈2자)
row 비용 = 추정토큰 × 2     (입력 + 출력 합산)
budget = settings.translation_token_budget (기본 24,000)
```

**병렬 처리 순서:**
1. 언어별 그룹화 → 각 언어별 규칙 로드 → 토큰 청크 분할
2. 모든 (언어×청크) 조합을 ThreadPoolExecutor로 병렬 실행
3. 결과 수집 → DB 저장

## 번역 규칙 DB (`core.translation_rules`)

번역기 시스템 프롬프트에 동적으로 주입되는 규칙. 코드 배포 없이 규칙을 추가/수정/비활성화할 수 있다.

**테이블 구조:**
```sql
rule_type  TEXT   -- 'term'(용어통일) | 'style'(문체) | 'format'(형식) | 'preserve'(원문유지)
lang       TEXT   -- NULL=전 언어 공통, 'en'/'ja' 등=언어별 전용
rule_text  TEXT   -- 영어 규칙 문장 (프롬프트에 직접 삽입)
example    TEXT   -- 예시 (참고용)
priority   INT    -- 높을수록 먼저 삽입 (기본 0)
is_active  BOOL   -- 비활성화 가능
```

**규칙 삽입 예시:**
```sql
-- 전 언어 공통: 브랜드명 보존
INSERT INTO core.translation_rules (rule_type, lang, rule_text, priority)
VALUES ('preserve', NULL, 'Keep Korean brand names in their original form (e.g., 스타벅스 → Starbucks not Seuta-beogseu)', 10);

-- 일본어 전용: 경어체
INSERT INTO core.translation_rules (rule_type, lang, rule_text, priority)
VALUES ('style', 'ja', 'Use polite formal Japanese (敬語・丁寧語)', 5);

-- 영어 전용: 음식 용어
INSERT INTO core.translation_rules (rule_type, lang, rule_text, example)
VALUES ('term', 'en', 'Translate 떡볶이 as "tteokbokki" (not "rice cake stir-fry")', '떡볶이 → tteokbokki');
```

**로드 로직 (`_utils.load_translation_rules`):**
- `lang IS NULL OR lang = 해당언어` 조건으로 공통 + 언어별 규칙 합산
- `priority DESC, id ASC` 순서로 정렬해 중요 규칙 먼저 삽입
- 시스템 프롬프트 말미에 `Additional translation rules:` 블록으로 추가

## 번역 파이프라인 전체 흐름

```
TranslationOrchestrator.run()
  ① JusoAddressTranslator  — 주소 한→영 (순차, place당 1 API call)
  ② DeepSeekTranslator     — zh-CN/zh-TW (청크 병렬)
  ③ GeminiBatchTranslator  — en/ja/th/pt-BR (청크 병렬)
  ④ DB 트리거              — places_snapshot 자동 갱신
```

## 스키마 핵심 사항

**`service.places_snapshot` 언어 컬럼:**
```
name_ko, name_en, name_ja, name_zh_cn, name_zh_tw, name_th, name_pt_br
address_ko, address_en   ← 주소는 ko+en만
description_ko, description_en, description_ja,
description_zh_cn, description_zh_tw, description_th, description_pt_br
```

**`core.translation_fill_queue`:**
- `provider`: 'gemini' | 'deepseek'
- `job_id`: 청크 단위 배치 ID (현재 미사용)

## 작업 시작 시 읽을 파일

```
pipeline/translator/_utils.py
pipeline/translator/gemini_translator.py   (또는 deepseek_translator.py)
```

## 고도화 포인트

1. **토큰 추정 정확도** — 현재 `len(text)//2`는 보수적. 실제 토큰 카운터(tiktoken/Gemini API) 연동 가능.
2. **청크 크기 자동 튜닝** — rate limit 응답(429)을 받으면 `translation_token_budget` 동적 감소.
3. **번역 규칙 효과 측정** — 규칙 적용 전후 번역 결과를 비교하는 평가 파이프라인 부재.
4. **Juso API 실패 재시도** — 실패 place를 별도 추적해 재처리하는 로직 없음.
5. **DeepSeek rate limit** — exponential backoff 없음. 429 에러 시 재시도 로직 추가 필요.
6. **pt-BR 주소 번역 부재** — 주소는 en만 지원. pt-BR 주소가 필요하다면 별도 변환 로직 필요.
7. **규칙 충돌 감지** — 동일 용어에 대한 규칙이 중복 등록될 경우 충돌 감지 없음.

## 응답 스타일

- 번역 공급자 변경 시 언어-공급자 매핑 표로 명시
- 규칙 추가 시 `INSERT INTO core.translation_rules` SQL로 즉시 적용 가능한 형태로 제시
- 토큰 예산 조정 시 `settings.translation_token_budget` 값과 예상 청크 크기 계산 제시
- DeepSeek는 OpenAI-호환 API이므로 openai 패키지 + base_url로 사용
