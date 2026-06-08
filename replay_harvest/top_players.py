from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
from typing import Any, Callable
from urllib.request import Request, urlopen

import duckdb

from .config import (
    AOE4WORLD_LEADERBOARD_URL,
    AOE4WORLD_PLAYER_URL,
    SAMPLE_GROUP_TOP100,
    USER_AGENT,
)


JsonFetcher = Callable[[str], Any]


@dataclass(frozen=True)
class LeaderboardPlayer:
    profile_id: int
    rank: int | None
    rating: int | None


def fetch_json(url: str) -> Any:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []
    for key in ("players", "leaderboard", "leaderboards", "items", "data"):
        value = payload.get(key)
        if isinstance(value, list):
            return [x for x in value if isinstance(x, dict)]
    return []


def _profile_id(record: dict[str, Any]) -> int | None:
    for key in ("profile_id", "profileId", "id"):
        value = record.get(key)
        if value is not None:
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
    player = record.get("player")
    if isinstance(player, dict):
        return _profile_id(player)
    return None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def parse_leaderboard(payload: Any, limit: int) -> list[LeaderboardPlayer]:
    players: list[LeaderboardPlayer] = []
    for idx, record in enumerate(_records(payload), start=1):
        profile_id = _profile_id(record)
        if profile_id is None:
            continue
        rank = _int_or_none(record.get("rank") or record.get("leaderboard_rank")) or idx
        rating = _int_or_none(record.get("rating") or record.get("elo") or record.get("mmr"))
        players.append(LeaderboardPlayer(profile_id=profile_id, rank=rank, rating=rating))
        if len(players) >= limit:
            break
    return players


def parse_alt_profile_ids(payload: Any, own_profile_id: int) -> set[int]:
    ids = {own_profile_id}
    if not isinstance(payload, dict):
        return ids
    candidates = []
    for key in ("alts", "alternate_accounts", "smurfs", "linked_accounts"):
        value = payload.get(key)
        if isinstance(value, list):
            candidates.extend(value)
    for item in candidates:
        if isinstance(item, dict):
            profile_id = _profile_id(item)
        else:
            profile_id = _int_or_none(item)
        if profile_id is not None:
            ids.add(profile_id)
    return ids


def discover_top_identities(
    fetcher: JsonFetcher = fetch_json,
    leaderboard_url: str = AOE4WORLD_LEADERBOARD_URL,
    player_url_template: str = AOE4WORLD_PLAYER_URL,
    canonical_limit: int = 100,
    overfetch: int = 250,
) -> list[tuple[int, int, int | None, int | None, str]]:
    """Return rows: canonical_profile_id, profile_id, rank, rating, source."""
    leaderboard: list[LeaderboardPlayer] = []
    page = 1
    while len(leaderboard) < overfetch:
        payload = fetcher(leaderboard_url.format(page=page))
        page_players = parse_leaderboard(payload, limit=overfetch - len(leaderboard))
        if not page_players:
            break
        leaderboard.extend(page_players)
        page += 1
    rows: list[tuple[int, int, int | None, int | None, str]] = []
    seen_profiles: set[int] = set()
    canonical_count = 0

    for player in leaderboard:
        if player.profile_id in seen_profiles:
            continue
        try:
            player_payload = fetcher(player_url_template.format(profile_id=player.profile_id))
            linked_ids = parse_alt_profile_ids(player_payload, player.profile_id)
            source = "aoe4world"
        except Exception:
            linked_ids = {player.profile_id}
            source = "aoe4world_no_alt_data"

        if linked_ids & seen_profiles:
            seen_profiles.update(linked_ids)
            continue

        canonical_id = min(linked_ids)
        canonical_count += 1
        for profile_id in sorted(linked_ids):
            rows.append((canonical_id, profile_id, player.rank, player.rating, source))
        seen_profiles.update(linked_ids)

        if canonical_count >= canonical_limit:
            break

    return rows


def label_top100_games(
    conn: duckdb.DuckDBPyConnection,
    fetcher: JsonFetcher = fetch_json,
) -> dict[str, int]:
    now = datetime.utcnow()
    identities = discover_top_identities(fetcher=fetcher)
    if identities:
        conn.executemany(
            """
            INSERT OR REPLACE INTO top_player_identities
                (canonical_profile_id, profile_id, leaderboard_rank, rating, source, discovered_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [(*row, now) for row in identities],
        )

    rows = conn.execute(
        """
        SELECT DISTINCT p.game_id
        FROM participants p
        JOIN games g ON g.game_id = p.game_id
        JOIN top_player_identities t ON t.profile_id = p.profile_id
        WHERE g.kind = 'rm_1v1'
        """
    ).fetchall()
    game_ids = [int(row[0]) for row in rows]
    if game_ids:
        for game_id in game_ids:
            conn.execute(
                """
                INSERT INTO replay_candidate_labels
                    (game_id, sample_group, reason, priority, created_at)
                SELECT ?, ?, ?, ?, ?
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM replay_candidate_labels
                    WHERE game_id = ? AND sample_group = ?
                )
                """,
                [
                    game_id,
                    SAMPLE_GROUP_TOP100,
                    "top100_linked_identity",
                    0,
                    now,
                    game_id,
                    SAMPLE_GROUP_TOP100,
                ],
            )

    return {"identities": len(identities), "games": len(game_ids)}
