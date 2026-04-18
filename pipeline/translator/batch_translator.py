"""
Step 1-7  AI Batch 번역 파이프라인 (GPT-4.1 mini)
  ① JSONL 생성  — translation_fill_queue (status=pending) 전량
  ② Batch 제출  — OpenAI Files API + Batch Job (24h window)
  ③ 결과 수거   — cron 폴링: completed 감지 → place_translations upsert
  ④ 스냅샷 갱신 — trigger가 자동 처리 (schema.sql 참조)

  cost: GPT-4.1 mini → 50% 절감
  멱등성: ON CONFLICT DO NOTHING (큐) / ON CONFLICT DO UPDATE (번역 결과)
"""
from __future__ import annotations

import json
import logging
import tempfile
from pathlib import Path
from typing import Any

from openai import OpenAI

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)

client = OpenAI(api_key=settings.openai_api_key)

LANG_NAMES = {
    "ko": "Korean", "en": "English", "ja": "Japanese",
    "zh-CN": "Simplified Chinese", "zh-TW": "Traditional Chinese",
    "fr": "French", "de": "German", "es": "Spanish",
    "th": "Thai", "vi": "Vietnamese",
}

SYSTEM_PROMPT = (
    "You are a professional translator specializing in K-culture tourism content. "
    "Translate the given JSON fields accurately and naturally into the target language. "
    "Preserve proper nouns (brand names, place names) where appropriate. "
    "Return ONLY a JSON object with the same keys as the input."
)


class BatchTranslator:
    """
    Usage:
        translator = BatchTranslator()

        # ① 큐 → JSONL → Batch 제출
        job_ids = translator.submit()

        # ③ 완료 수거 (cron에서 주기적 호출)
        translator.collect()
    """

    def submit(self) -> list[str]:
        """pending 항목을 JSONL 파일로 만들어 OpenAI Batch 제출. job_id 목록 반환."""
        rows = self._fetch_pending()
        if not rows:
            logger.info("번역 큐가 비어 있습니다.")
            return []

        batches = self._split_batches(rows, settings.translation_batch_size)
        job_ids: list[str] = []

        for idx, batch in enumerate(batches):
            jsonl_path = self._build_jsonl(batch)
            job_id = self._submit_batch(jsonl_path)
            job_ids.append(job_id)
            self._mark_submitted(batch, job_id)
            logger.info("Batch 제출 #%d: %d건 → job_id=%s", idx + 1, len(batch), job_id)

        return job_ids

    def collect(self) -> int:
        """완료된 Batch Job을 수거해 place_translations에 저장. 처리 건수 반환."""
        job_ids = self._get_submitted_job_ids()
        total = 0

        for job_id in job_ids:
            try:
                job = client.batches.retrieve(job_id)
                if job.status != "completed":
                    logger.debug("job_id=%s status=%s — 대기 중", job_id, job.status)
                    continue

                output_file_id = job.output_file_id
                content = client.files.content(output_file_id).text
                total += self._process_output(content, job_id)

            except Exception as exc:
                logger.error("Batch 수거 오류 (job_id=%s): %s", job_id, exc)

        logger.info("번역 수거 완료: %d건", total)
        return total

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _fetch_pending(self) -> list[dict]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                SELECT q.id AS queue_id, q.place_id, q.lang, q.is_retranslation,
                       p.name, p.address, p.description
                  FROM core.translation_fill_queue q
                  JOIN core.places p ON p.place_id = q.place_id
                 WHERE q.status = 'pending'
                 ORDER BY q.id
                """
            )
            return list(cur.fetchall())

    def _split_batches(self, rows: list[dict], size: int) -> list[list[dict]]:
        return [rows[i: i + size] for i in range(0, len(rows), size)]

    def _build_jsonl(self, rows: list[dict]) -> Path:
        tmp = tempfile.NamedTemporaryFile(
            delete=False, suffix=".jsonl", mode="w", encoding="utf-8"
        )
        for row in rows:
            lang = row["lang"]
            custom_id = f"place_{row['place_id']}_{lang}"
            fields = {}
            if row["name"]:
                fields["name"] = row["name"]
            if row["address"]:
                fields["address"] = row["address"]
            if row["description"]:
                fields["description"] = row["description"]

            request = {
                "custom_id": custom_id,
                "method": "POST",
                "url": "/v1/chat/completions",
                "body": {
                    "model": settings.openai_translation_model,
                    "messages": [
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {
                            "role": "user",
                            "content": (
                                f"Translate the following fields into {LANG_NAMES.get(lang, lang)}.\n"
                                f"Input JSON:\n{json.dumps(fields, ensure_ascii=False)}"
                            ),
                        },
                    ],
                    "response_format": {"type": "json_object"},
                },
            }
            tmp.write(json.dumps(request, ensure_ascii=False) + "\n")

        tmp.flush()
        return Path(tmp.name)

    def _submit_batch(self, jsonl_path: Path) -> str:
        with open(jsonl_path, "rb") as f:
            uploaded = client.files.create(file=f, purpose="batch")

        batch_job = client.batches.create(
            input_file_id=uploaded.id,
            endpoint="/v1/chat/completions",
            completion_window="24h",
        )
        jsonl_path.unlink(missing_ok=True)
        return batch_job.id

    def _mark_submitted(self, rows: list[dict], job_id: str) -> None:
        queue_ids = [r["queue_id"] for r in rows]
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                """
                UPDATE core.translation_fill_queue
                   SET status = 'submitted', batch_job_id = %s, updated_at = now()
                 WHERE id = ANY(%s)
                """,
                (job_id, queue_ids),
            )

    def _get_submitted_job_ids(self) -> list[str]:
        with get_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT DISTINCT batch_job_id FROM core.translation_fill_queue WHERE status = 'submitted'"
            )
            return [row["batch_job_id"] for row in cur.fetchall() if row["batch_job_id"]]

    def _process_output(self, content: str, job_id: str) -> int:
        cnt = 0
        with get_conn() as conn:
            for line in content.strip().splitlines():
                if not line.strip():
                    continue
                try:
                    result = json.loads(line)
                    custom_id: str = result["custom_id"]
                    body = result.get("response", {}).get("body", {})
                    message = body.get("choices", [{}])[0].get("message", {}).get("content", "{}")
                    translated: dict[str, Any] = json.loads(message)

                    # custom_id = "place_{id}_{lang}"
                    parts = custom_id.split("_", 2)
                    place_id = int(parts[1])
                    lang = parts[2]

                    cur = conn.cursor()
                    cur.execute(
                        """
                        INSERT INTO core.place_translations
                            (place_id, lang, name, address, description, model_used, is_retranslation)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (place_id, lang) DO UPDATE
                          SET name             = EXCLUDED.name,
                              address          = EXCLUDED.address,
                              description      = EXCLUDED.description,
                              model_used       = EXCLUDED.model_used,
                              is_retranslation = EXCLUDED.is_retranslation,
                              translated_at    = now()
                        """,
                        (
                            place_id, lang,
                            translated.get("name"),
                            translated.get("address"),
                            translated.get("description"),
                            settings.openai_translation_model,
                            result.get("is_retranslation", False),
                        ),
                    )

                    # 큐 완료 처리
                    cur.execute(
                        """
                        UPDATE core.translation_fill_queue
                           SET status = 'completed', updated_at = now()
                         WHERE place_id = %s AND lang = %s AND batch_job_id = %s
                        """,
                        (place_id, lang, job_id),
                    )
                    cnt += 1

                except Exception as exc:
                    logger.error("번역 결과 파싱 오류 (line=%s): %s", line[:80], exc)

        return cnt
