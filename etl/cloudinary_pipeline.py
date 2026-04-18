"""
Cloudinary 이미지 파이프라인 (Phase 3-4)
TourAPI 원본 URL → Cloudinary 업로드 → core.poi_images 저장

흐름:
  stage.raw_documents (ko) → firstimage URL 추출
  → Cloudinary 업로드 (fetch upload)
  → 썸네일 400×300 crop URL 생성
  → WebP URL 생성
  → core.poi_images upsert
"""

import asyncio
import json
import logging
from typing import Optional

import asyncpg

logger = logging.getLogger(__name__)


def _configure_cloudinary(cloudinary_url: str) -> None:
    """
    cloudinary://api_key:api_secret@cloud_name 형식 파싱 후 설정.
    SDK의 cloudinary_url= 파라미터가 일부 버전에서 동작하지 않아 직접 파싱.
    """
    import re
    import cloudinary
    m = re.match(r"cloudinary://([^:]+):([^@]+)@(.+)", cloudinary_url)
    if not m:
        raise ValueError(f"CLOUDINARY_URL 형식 오류: {cloudinary_url!r}")
    api_key, api_secret, cloud_name = m.groups()
    cloudinary.config(
        cloud_name=cloud_name,
        api_key=api_key,
        api_secret=api_secret,
        secure=True,
    )


def _build_thumbnail_url(public_id: str) -> str:
    import cloudinary
    return cloudinary.CloudinaryImage(public_id).build_url(
        width=400, height=300, crop="fill", fetch_format="auto", quality="auto"
    )


def _build_webp_url(public_id: str) -> str:
    import cloudinary
    return cloudinary.CloudinaryImage(public_id).build_url(
        format="webp", quality="auto"
    )


def _upload_sync(original_url: str, public_id: str) -> dict:
    import cloudinary.uploader
    return cloudinary.uploader.upload(
        original_url,
        public_id=public_id,
        overwrite=False,
        resource_type="image",
    )


async def _upload_image(original_url: str, poi_id: int) -> Optional[dict]:
    """
    원본 URL을 Cloudinary에 업로드 후 메타 반환.
    실패 시 None.
    """
    public_id = f"k_culture/poi/{poi_id}"
    try:
        result = await asyncio.to_thread(_upload_sync, original_url, public_id)
        pid = result["public_id"]
        return {
            "public_id": pid,
            "secure_url": result["secure_url"],
            "thumbnail_url": _build_thumbnail_url(pid),
            "webp_url": _build_webp_url(pid),
            "original_url": original_url,
            "width": result.get("width"),
            "height": result.get("height"),
            "format": result.get("format"),
        }
    except Exception as e:
        logger.warning("Cloudinary 업로드 실패 poi_id=%s url=%.80s: %s", poi_id, original_url, e)
        return None


async def process_poi_images(
    conn: asyncpg.Connection,
    cid_map: dict[str, int],
    source_id: int,
    cloudinary_url: str,
    batch_size: int = 50,
) -> int:
    """
    stage.raw_documents (ko, firstimage 있는 것) → Cloudinary 업로드 → poi_images 저장.

    Args:
        conn:           asyncpg connection
        cid_map:        contentid → poi.id 매핑
        source_id:      stage.api_sources.id (tourapi)
        cloudinary_url: CLOUDINARY_URL 환경변수 값
        batch_size:     한 번에 처리할 raw_documents 행 수

    Returns:
        업로드 성공 건수
    """
    _configure_cloudinary(cloudinary_url)
    total = 0
    offset = 0

    while True:
        rows = await conn.fetch(
            """
            SELECT raw_json
            FROM stage.raw_documents
            WHERE source_id = $1
              AND language_code = 'ko'
              AND raw_json->>'firstimage' IS NOT NULL
              AND raw_json->>'firstimage' <> ''
            ORDER BY id
            LIMIT $2 OFFSET $3
            """,
            source_id, batch_size, offset,
        )
        if not rows:
            break

        for row in rows:
            raw = json.loads(row["raw_json"])
            cid = str(raw.get("contentid", ""))
            poi_id = cid_map.get(cid)
            if poi_id is None:
                continue

            # 이미 이미지가 있으면 스킵
            existing = await conn.fetchval(
                "SELECT id FROM core.poi_images WHERE poi_id = $1 LIMIT 1",
                poi_id,
            )
            if existing:
                continue

            original_url = (raw.get("firstimage") or "").strip()
            if not original_url:
                continue

            meta = await _upload_image(original_url, poi_id)
            if meta is None:
                continue

            await conn.execute(
                """
                INSERT INTO core.poi_images
                    (poi_id, cloudinary_public_id, secure_url,
                     thumbnail_url, webp_url, original_url,
                     width, height, format, is_primary)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, TRUE)
                ON CONFLICT DO NOTHING
                """,
                poi_id,
                meta["public_id"],
                meta["secure_url"],
                meta["thumbnail_url"],
                meta["webp_url"],
                meta["original_url"],
                meta["width"],
                meta["height"],
                meta["format"],
            )
            total += 1
            if total % 10 == 0:
                print(f"  [cloudinary] {total}건 업로드", end="\r")

            await asyncio.sleep(0.2)  # Cloudinary 무료 플랜 rate limit 고려

        offset += batch_size

    print(f"  [cloudinary] {total}건 업로드 완료")
    return total
