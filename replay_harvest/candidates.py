from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import duckdb

from .config import RATING_BUCKETS, SAMPLE_GROUP_BALANCED


@dataclass(frozen=True)
class RatingBucket:
    name: str
    minimum: int | None
    maximum: int | None


BUCKETS = [RatingBucket(*row) for row in RATING_BUCKETS]


def bucket_for_rating(rating: int | None) -> str | None:
    if rating is None:
        return None
    for bucket in BUCKETS:
        if bucket.minimum is not None and rating < bucket.minimum:
            continue
        if bucket.maximum is not None and rating >= bucket.maximum:
            continue
        return bucket.name
    return None


def _bucket_predicate(bucket: RatingBucket) -> str:
    parts = ["p.rating IS NOT NULL"]
    if bucket.minimum is not None:
        parts.append(f"p.rating >= {bucket.minimum}")
    if bucket.maximum is not None:
        parts.append(f"p.rating < {bucket.maximum}")
    return " AND ".join(parts)


def _optional_filters(season: int | None, patch: str | None) -> tuple[str, list[object]]:
    filters = ["g.kind = 'rm_1v1'"]
    params: list[object] = []
    if season is not None:
        filters.append("g.season = ?")
        params.append(season)
    if patch is not None:
        filters.append("g.patch = ?")
        params.append(patch)
    return " AND ".join(filters), params


def label_balanced_candidates(
    conn: duckdb.DuckDBPyConnection,
    limit: int = 10_000,
    season: int | None = None,
    patch: str | None = None,
    sample_group: str = SAMPLE_GROUP_BALANCED,
) -> dict[str, int]:
    """Label a roughly even rating-bucket sample in replay_candidate_labels."""
    per_bucket = max(1, limit // len(BUCKETS))
    remainder = max(0, limit - per_bucket * len(BUCKETS))
    now = datetime.utcnow()
    inserted: dict[str, int] = {}
    selected_game_ids: set[int] = set()
    base_filter, params = _optional_filters(season, patch)

    for idx, bucket in enumerate(BUCKETS):
        bucket_limit = per_bucket + (1 if idx < remainder else 0)
        excluded_sql = ""
        excluded_params: list[object] = []
        if selected_game_ids:
            placeholders = ",".join(["?"] * len(selected_game_ids))
            excluded_sql = f"AND g.game_id NOT IN ({placeholders})"
            excluded_params = list(selected_game_ids)

        rows = conn.execute(
            f"""
            SELECT DISTINCT g.game_id
            FROM games g
            JOIN participants p ON p.game_id = g.game_id
            WHERE {base_filter}
              AND {_bucket_predicate(bucket)}
              AND NOT EXISTS (
                  SELECT 1
                  FROM replay_candidate_labels l
                  WHERE l.game_id = g.game_id
                    AND l.sample_group = ?
              )
              AND NOT EXISTS (
                  SELECT 1
                  FROM replay_downloads d
                  WHERE d.game_id = g.game_id
                    AND d.status = 'downloaded'
              )
              {excluded_sql}
            ORDER BY g.started_at DESC NULLS LAST, g.game_id DESC
            LIMIT ?
            """,
            [*params, sample_group, *excluded_params, bucket_limit],
        ).fetchall()

        game_ids = [int(row[0]) for row in rows]
        selected_game_ids.update(game_ids)
        inserted[bucket.name] = len(game_ids)
        if not game_ids:
            continue

        conn.executemany(
            """
            INSERT OR IGNORE INTO replay_candidate_labels
                (game_id, sample_group, reason, priority, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            [
                (
                    game_id,
                    sample_group,
                    f"balanced_rating_bucket:{bucket.name}",
                    idx,
                    now,
                )
                for game_id in game_ids
            ],
        )

    return inserted


def summarize_labels(conn: duckdb.DuckDBPyConnection) -> list[dict[str, object]]:
    rows = conn.execute(
        """
        SELECT sample_group, reason, count(*) AS games
        FROM replay_candidate_labels
        GROUP BY sample_group, reason
        ORDER BY sample_group, reason
        """
    ).fetchall()
    return [
        {"sample_group": row[0], "reason": row[1], "games": int(row[2])}
        for row in rows
    ]


def game_ids_for_labels(
    conn: duckdb.DuckDBPyConnection,
    sample_group: str,
    limit: int,
) -> Iterable[int]:
    rows = conn.execute(
        """
        SELECT l.game_id
        FROM replay_candidate_labels l
        LEFT JOIN replay_downloads d ON d.game_id = l.game_id
        WHERE l.sample_group = ?
          AND coalesce(d.status, '') NOT IN ('downloaded', 'assigned')
        ORDER BY l.priority ASC, l.created_at ASC, l.game_id DESC
        LIMIT ?
        """,
        [sample_group, limit],
    ).fetchall()
    for (game_id,) in rows:
        yield int(game_id)

