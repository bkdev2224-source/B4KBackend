"""
Step 1-8  서비스 API — Places
  GET /places          목록·필터·페이징
  GET /places/{id}     상세
  GET /places/search   키워드·좌표 검색
"""
from __future__ import annotations

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from database.db import get_conn

router = APIRouter(prefix="/places", tags=["places"])


# ── 응답 스키마 ───────────────────────────────────────────────────────────────

class PlaceItem(BaseModel):
    place_id:       int
    name:           str | None
    address:        str | None
    coords_lat:     float | None
    coords_lng:     float | None
    display_domain: str | None
    display_region: str | None
    primary_image_url: str | None
    quality_score:  float | None


class PlaceDetail(PlaceItem):
    description:    str | None
    source_category: str | None


class PlaceList(BaseModel):
    total:   int
    page:    int
    size:    int
    items:   list[PlaceItem]


# ── 헬퍼 ──────────────────────────────────────────────────────────────────────

LANG_COL = {
    "ko": "name_ko", "en": "name_en", "ja": "name_ja",
    "zh-CN": "name_zh_cn", "zh-TW": "name_zh_tw", "th": "name_th",
}
ADDR_COL  = {"ko": "address_ko", "en": "address_en"}
DESC_COL  = {
    "ko": "description_ko", "en": "description_en",
    "ja": "description_ja", "zh-CN": "description_zh_cn",
    "zh-TW": "description_zh_tw", "th": "description_th",
}


def _name_col(lang: str) -> str:
    return LANG_COL.get(lang, "name_en") or "name_en"


def _addr_col(lang: str) -> str:
    return ADDR_COL.get(lang, "address_en") or "address_en"


def _desc_col(lang: str) -> str:
    return DESC_COL.get(lang, "description_en") or "description_en"


def _row_to_item(row: dict, lang: str) -> dict[str, Any]:
    return {
        "place_id":         row["place_id"],
        "name":             row.get(_name_col(lang)) or row.get("name_en"),
        "address":          row.get(_addr_col(lang)) or row.get("address_en"),
        "coords_lat":       row.get("coords_lat"),
        "coords_lng":       row.get("coords_lng"),
        "display_domain":   row.get("display_domain"),
        "display_region":   row.get("display_region"),
        "primary_image_url": row.get("primary_image_url"),
        "quality_score":    row.get("quality_score"),
    }


def _row_to_detail(row: dict, lang: str) -> dict[str, Any]:
    d = _row_to_item(row, lang)
    d["description"]    = row.get(_desc_col(lang)) or row.get("description_en")
    d["source_category"] = row.get("source_category")
    return d


# ── 엔드포인트 ────────────────────────────────────────────────────────────────

@router.get("", response_model=PlaceList)
def list_places(
    domain: str | None = Query(None, description="kfood|kbeauty|ktourism|kshopping|kleisure"),
    region: str | None = Query(None, description="서울|부산|제주 ..."),
    lang:   str        = Query("ko"),
    page:   int        = Query(1, ge=1),
    size:   int        = Query(20, ge=1, le=100),
):
    offset = (page - 1) * size
    where: list[str] = ["is_publishable = TRUE"]
    params: list[Any] = []

    if domain:
        where.append("display_domain = %s")
        params.append(domain)
    if region:
        where.append("display_region = %s")
        params.append(region)

    where_sql = " AND ".join(where)

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(f"SELECT COUNT(*) AS cnt FROM service.places_snapshot WHERE {where_sql}", params)
        total = cur.fetchone()["cnt"]

        cur.execute(
            f"""
            SELECT * FROM service.places_snapshot
             WHERE {where_sql}
             ORDER BY quality_score DESC NULLS LAST, place_id
             LIMIT %s OFFSET %s
            """,
            params + [size, offset],
        )
        rows = cur.fetchall()

    return PlaceList(
        total=total,
        page=page,
        size=size,
        items=[PlaceItem(**_row_to_item(r, lang)) for r in rows],
    )


@router.get("/search", response_model=PlaceList)
def search_places(
    q:      str        = Query(..., min_length=1),
    domain: str | None = Query(None),
    region: str | None = Query(None),
    lat:    float | None = Query(None),
    lng:    float | None = Query(None),
    lang:   str        = Query("ko"),
    page:   int        = Query(1, ge=1),
    size:   int        = Query(20, ge=1, le=100),
):
    offset = (page - 1) * size
    name_col = _name_col(lang)

    where: list[str] = ["s.is_publishable = TRUE"]
    params: list[Any] = []

    # 키워드 검색 (pg_trgm 또는 ILIKE)
    where.append(f"(s.{name_col} ILIKE %s OR s.address_ko ILIKE %s)")
    like = f"%{q}%"
    params.extend([like, like])

    if domain:
        where.append("s.display_domain = %s")
        params.append(domain)
    if region:
        where.append("s.display_region = %s")
        params.append(region)

    where_sql = " AND ".join(where)

    # 좌표 기반 정렬
    order_sql = "s.quality_score DESC NULLS LAST"
    if lat and lng:
        where.append("p.geom IS NOT NULL")
        order_sql = f"ST_Distance(p.geom::geography, ST_SetSRID(ST_MakePoint({lng}, {lat}), 4326)::geography)"

    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            f"""
            SELECT s.*, ST_Y(p.geom) AS geom_lat, ST_X(p.geom) AS geom_lng
              FROM service.places_snapshot s
              JOIN core.poi p ON p.id = s.place_id
             WHERE {where_sql}
             ORDER BY {order_sql}
             LIMIT %s OFFSET %s
            """,
            params + [size, offset],
        )
        rows = cur.fetchall()

        cur.execute(
            f"SELECT COUNT(*) AS cnt FROM service.places_snapshot s JOIN core.poi p ON p.id = s.place_id WHERE {where_sql}",
            params,
        )

    return PlaceList(
        total=total,
        page=page,
        size=size,
        items=[PlaceItem(**_row_to_item(r, lang)) for r in rows],
    )


@router.get("/{place_id}", response_model=PlaceDetail)
def get_place(place_id: int, lang: str = Query("ko")):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "SELECT * FROM service.places_snapshot WHERE place_id = %s",
            (place_id,),
        )
        row = cur.fetchone()

    if not row:
        raise HTTPException(status_code=404, detail="Place not found")

    return PlaceDetail(**_row_to_detail(row, lang))
