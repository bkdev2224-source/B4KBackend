"""
중복 제거 로직
- Phase 3 (단일 소스): contentid 기준 — 사실상 중복 없음
- Phase 5 (멀티 소스): PostGIS 50m + Jaro-Winkler 0.85 → dedup_review_queue
"""
import asyncpg


async def check_duplicate(
    conn: asyncpg.Connection,
    name: str,
    lon: float,
    lat: float,
    distance_m: float = 50.0,
    name_threshold: float = 0.85,
) -> int | None:
    """
    기존 poi 중 같은 장소로 의심되는 것의 id 반환.
    없으면 None.
    """
    if lon is None or lat is None or not name:
        return None

    row = await conn.fetchrow(
        """
        SELECT id, name_ko,
               ST_Distance(geom::geography,
                           ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography) AS dist_m
        FROM core.poi
        WHERE ST_DWithin(
                  geom::geography,
                  ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                  $3
              )
        ORDER BY dist_m
        LIMIT 1
        """,
        lon, lat, distance_m,
    )
    if row is None:
        return None

    similarity = _jaro_winkler(name, row["name_ko"] or "")
    if similarity >= name_threshold:
        return row["id"]

    return None


async def enqueue_dedup(
    conn: asyncpg.Connection,
    poi_id_a: int,
    poi_id_b: int,
    distance_m: float,
    similarity: float,
) -> None:
    await conn.execute(
        """
        INSERT INTO core.dedup_review_queue
            (poi_id_a, poi_id_b, distance_m, name_similarity)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT DO NOTHING
        """,
        poi_id_a, poi_id_b, distance_m, similarity,
    )


def _jaro_winkler(s1: str, s2: str) -> float:
    """Jaro-Winkler 유사도 (0~1)"""
    if s1 == s2:
        return 1.0
    if not s1 or not s2:
        return 0.0

    match_dist = max(len(s1), len(s2)) // 2 - 1
    match_dist = max(0, match_dist)

    s1_matches = [False] * len(s1)
    s2_matches = [False] * len(s2)
    matches = 0
    transpositions = 0

    for i, c1 in enumerate(s1):
        start = max(0, i - match_dist)
        end = min(i + match_dist + 1, len(s2))
        for j in range(start, end):
            if s2_matches[j] or c1 != s2[j]:
                continue
            s1_matches[i] = s2_matches[j] = True
            matches += 1
            break

    if matches == 0:
        return 0.0

    k = 0
    for i, matched in enumerate(s1_matches):
        if not matched:
            continue
        while not s2_matches[k]:
            k += 1
        if s1[i] != s2[k]:
            transpositions += 1
        k += 1

    jaro = (matches / len(s1) + matches / len(s2) + (matches - transpositions / 2) / matches) / 3

    prefix = 0
    for c1, c2 in zip(s1[:4], s2[:4]):
        if c1 == c2:
            prefix += 1
        else:
            break

    return jaro + prefix * 0.1 * (1 - jaro)
