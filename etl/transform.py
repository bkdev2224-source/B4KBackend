"""
TourAPI raw JSON → core 스키마 정규화
"""


def normalize_poi(raw: dict) -> dict:
    """raw_documents.raw_json → core.poi 삽입용 dict (한국어 기준)"""
    mapx = raw.get("mapx") or ""
    mapy = raw.get("mapy") or ""
    try:
        lon = float(mapx) if mapx else None
        lat = float(mapy) if mapy else None
    except (ValueError, TypeError):
        lon = lat = None

    name = (raw.get("title") or "").strip()
    address = (raw.get("addr1") or "").strip()
    has_coords = lon is not None and lat is not None

    if name and address and has_coords:
        quality = "full"
    elif name and (address or has_coords):
        quality = "partial"
    else:
        quality = "missing"

    cat = raw.get("cat3") or raw.get("cat2") or raw.get("cat1") or ""

    return {
        "external_id":     str(raw["contentid"]),
        "name_ko":         name,
        "address_ko":      address or None,
        "lon":             lon,
        "lat":             lat,
        "category_code":   cat or None,
        "content_type_id": str(raw.get("contenttypeid") or ""),
        "phone":           (raw.get("tel") or "").strip() or None,
        "quality":         quality,
    }


def normalize_translation(raw: dict) -> dict:
    """raw_documents.raw_json → core.poi_translations 삽입용 dict"""
    return {
        "name":    (raw.get("title") or "").strip() or None,
        "address": (raw.get("addr1") or "").strip() or None,
    }
