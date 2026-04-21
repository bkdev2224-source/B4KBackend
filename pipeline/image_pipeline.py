"""
Step 1-5  Cloudinary 이미지 파이프라인
  - core.place_images (upload_status='pending') → Cloudinary 업로드
  - 썸네일 400×300 WebP crop
  - 실패 시 error_count 누적 (최대 3회)
"""
from __future__ import annotations

import hashlib
import logging

import cloudinary
import cloudinary.uploader

from config.settings import settings
from database.db import get_conn

logger = logging.getLogger(__name__)

cloudinary.config(
    cloud_name=settings.cloudinary_cloud_name,
    api_key=settings.cloudinary_api_key,
    api_secret=settings.cloudinary_api_secret,
    secure=True,
)

MAX_RETRIES = 3
BATCH_SIZE = 100
_LOG_INTERVAL = 20  # 배치 내 진행 출력 간격


class ImagePipeline:
    """
    Usage:
        pipeline = ImagePipeline()
        pipeline.run()
    """

    def run(self) -> dict[str, int]:
        results = {"uploaded": 0, "skipped": 0, "error": 0}
        batch_num = 0
        with get_conn() as conn:
            while True:
                rows = self._fetch_pending(conn)
                if not rows:
                    if batch_num == 0:
                        logger.info("[1-5] 업로드 대기 이미지 없음")
                    break
                batch_num += 1
                logger.info("[1-5] 배치 %d 시작: %d건 처리", batch_num, len(rows))
                for idx, row in enumerate(rows):
                    outcome = self._process(conn, row)
                    results[outcome] += 1
                    if (idx + 1) % _LOG_INTERVAL == 0:
                        logger.info(
                            "[1-5] 배치 %d: %d / %d 처리 중 | 업로드 %d | 스킵 %d | 오류 %d",
                            batch_num, idx + 1, len(rows),
                            results["uploaded"], results["skipped"], results["error"],
                        )
        logger.info("[1-5] 이미지 파이프라인 완료: %s", results)
        return results

    # ── 내부 ──────────────────────────────────────────────────────────────────

    def _fetch_pending(self, conn) -> list[dict]:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id, poi_id AS place_id, original_url, error_count
              FROM core.poi_images
             WHERE upload_status = 'pending'
               AND error_count < %s
             LIMIT %s
            """,
            (MAX_RETRIES, BATCH_SIZE),
        )
        return list(cur.fetchall())

    def _process(self, conn, row: dict) -> str:
        url = row["original_url"]
        public_id = self._make_public_id(row["place_id"], url)

        try:
            result = cloudinary.uploader.upload(
                url,
                public_id=public_id,
                overwrite=False,
                format="webp",
                transformation=[
                    {"width": 400, "height": 300, "crop": "fill", "gravity": "auto"},
                ],
                folder="kculture/places",
            )
            cdn_url = result["secure_url"]
            self._mark_uploaded(conn, row["id"], cdn_url, public_id, result.get("width"), result.get("height"))
            logger.debug("[1-5] 업로드 성공 (image_id=%s, place_id=%s)", row["id"], row["place_id"])
            return "uploaded"

        except cloudinary.exceptions.Error as e:
            if "already exists" in str(e).lower():
                existing_url = f"https://res.cloudinary.com/{settings.cloudinary_cloud_name}/image/upload/kculture/places/{public_id}.webp"
                self._mark_uploaded(conn, row["id"], existing_url, public_id, 400, 300)
                logger.debug("[1-5] public_id 중복 → 기존 URL 재사용 (image_id=%s)", row["id"])
                return "skipped"
            new_err_cnt = row["error_count"] + 1
            self._mark_error(conn, row["id"], new_err_cnt)
            logger.warning(
                "[1-5] Cloudinary 업로드 실패 (image_id=%s, place_id=%s, retry=%d/%d): %s",
                row["id"], row["place_id"], new_err_cnt, MAX_RETRIES, e,
            )
            return "error"
        except Exception as exc:
            new_err_cnt = row["error_count"] + 1
            self._mark_error(conn, row["id"], new_err_cnt)
            logger.error(
                "[1-5] 이미지 처리 오류 (image_id=%s, place_id=%s, retry=%d/%d): %s",
                row["id"], row["place_id"], new_err_cnt, MAX_RETRIES, exc,
            )
            return "error"

    @staticmethod
    def _make_public_id(place_id: int, url: str) -> str:
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        return f"place_{place_id}_{url_hash}"

    @staticmethod
    def _mark_uploaded(conn, img_id: int, cdn_url: str, public_id: str, w: int | None, h: int | None) -> None:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE core.poi_images
               SET cloudinary_public_id = %s,
                   secure_url           = %s,
                   width                = %s,
                   height               = %s,
                   upload_status        = 'uploaded'
             WHERE id = %s
            """,
            (public_id, cdn_url, w, h, img_id),
        )

    @staticmethod
    def _mark_error(conn, img_id: int, error_count: int) -> None:
        cur = conn.cursor()
        new_status = "error" if error_count >= MAX_RETRIES else "pending"
        cur.execute(
            "UPDATE core.poi_images SET error_count=%s, upload_status=%s WHERE id=%s",
            (error_count, new_status, img_id),
        )
