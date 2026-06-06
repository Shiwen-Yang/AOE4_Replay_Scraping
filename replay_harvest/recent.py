from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import time
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb

from .config import SAMPLE_GROUP_RECENT, USER_AGENT


JsonFetcher = Callable[[str], Any]


def fetch_json(url: str) -> Any:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def seed_profile_ids(conn: duckdb.DuckDBPyConnection, limit: int) -> list[int]:
    rows = conn.execute(
        """
        SELECT p.profile_id
        FROM participants p
        JOIN games g ON g.game_id = p.game_id
        WHERE g.kind = 'rm_1v1'
          AND p.rating IS NOT NULL
        GROUP BY p.profile_id
        ORDER BY max(p.rating) DESC NULLS LAST, count(*) DESC, max(g.started_at) DESC
        LIMIT ?
        """,
        [limit],
    ).fetchall()
    return [int(row[0]) for row in rows]


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).replace(tzinfo=None)


def _flat_players(game: dict[str, Any]) -> list[dict[str, Any]]:
    players = []
    for team in game.get("teams") or []:
        for entry in team or []:
            player = entry.get("player") if isinstance(entry, dict) else None
            if isinstance(player, dict):
                players.append(player)
    return players


def _result(value: str | None) -> bool | None:
    if value == "win":
        return True
    if value == "loss":
        return False
    return None


def insert_game(conn: duckdb.DuckDBPyConnection, game: dict[str, Any], source_file: str) -> bool:
    players = _flat_players(game)
    if len(players) != 2 or game.get("kind") != "rm_1v1":
        return False
    game_id = int(game["game_id"])
    conn.execute(
        """
        INSERT OR IGNORE INTO games
            (game_id, started_at, finished_at, duration, map_id, map, kind, server, patch, season, source_file)
        VALUES (?, ?, NULL, ?, NULL, ?, ?, ?, ?, ?, ?)
        """,
        [
            game_id,
            _parse_time(game.get("started_at")),
            game.get("duration"),
            game.get("map"),
            game.get("kind"),
            game.get("server"),
            str(game.get("patch")) if game.get("patch") is not None else None,
            game.get("season"),
            source_file,
        ],
    )
    rows = []
    for player in players:
        rows.append(
            [
                game_id,
                int(player["profile_id"]),
                _result(player.get("result")),
                player.get("civilization"),
                player.get("civilization_randomized"),
                player.get("rating"),
                player.get("rating_diff"),
                player.get("mmr"),
                player.get("mmr_diff"),
                player.get("input_type"),
            ]
        )
    conn.executemany(
        """
        INSERT OR IGNORE INTO participants
            (game_id, profile_id, result, civilization, civilization_randomized,
             rating, rating_diff, mmr, mmr_diff, input_type)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    conn.executemany(
        """
        UPDATE participants
        SET
            result = coalesce(result, ?),
            civilization = coalesce(civilization, ?),
            civilization_randomized = coalesce(civilization_randomized, ?),
            rating = coalesce(rating, ?),
            rating_diff = coalesce(rating_diff, ?),
            mmr = coalesce(mmr, ?),
            mmr_diff = coalesce(mmr_diff, ?),
            input_type = coalesce(input_type, ?)
        WHERE game_id = ? AND profile_id = ?
        """,
        [
            [
                row[2],
                row[3],
                row[4],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                game_id,
                row[1],
            ]
            for row in rows
        ],
    )
    return True


def label_games(
    conn: duckdb.DuckDBPyConnection,
    game_ids: set[int],
    group: str,
    reason: str = "recent_player_games",
    priority: int = 0,
) -> int:
    if not game_ids:
        return 0
    now = datetime.utcnow()
    before = conn.execute(
        "SELECT count(*) FROM replay_candidate_labels WHERE sample_group = ?",
        [group],
    ).fetchone()[0]
    conn.executemany(
        """
        INSERT OR IGNORE INTO replay_candidate_labels
            (game_id, sample_group, reason, priority, created_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        [(game_id, group, reason, priority, now) for game_id in sorted(game_ids)],
    )
    after = conn.execute(
        "SELECT count(*) FROM replay_candidate_labels WHERE sample_group = ?",
        [group],
    ).fetchone()[0]
    return int(after - before)


def discover_recent_games(
    conn: duckdb.DuckDBPyConnection,
    seed_limit: int = 200,
    per_player: int = 25,
    days: int = 10,
    sleep_seconds: float = 1.0,
    group: str = SAMPLE_GROUP_RECENT,
    fetcher: JsonFetcher = fetch_json,
) -> dict[str, int]:
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    game_ids: set[int] = set()
    fetched_players = 0
    inserted_games = 0
    failures = 0

    for profile_id in seed_profile_ids(conn, seed_limit):
        params = urlencode(
            {
                "leaderboard": "rm_solo",
                "limit": per_player,
                "include_alts": "true",
                "since": since,
            }
        )
        url = f"https://aoe4world.com/api/v0/players/{profile_id}/games?{params}"
        try:
            payload = fetcher(url)
            fetched_players += 1
            games = payload.get("games", []) if isinstance(payload, dict) else []
            for game in games:
                try:
                    if insert_game(conn, game, "aoe4world_recent_player_games"):
                        game_id = int(game["game_id"])
                        game_ids.add(game_id)
                        inserted_games += 1
                except Exception:
                    failures += 1
        except Exception:
            failures += 1
        time.sleep(sleep_seconds)

    labeled = label_games(conn, game_ids, group, reason="recent_player_games", priority=0)
    return {
        "seed_players": seed_limit,
        "fetched_players": fetched_players,
        "inserted_games": inserted_games,
        "unique_games": len(game_ids),
        "labeled": labeled,
        "failures": failures,
    }
