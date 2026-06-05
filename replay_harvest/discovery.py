from __future__ import annotations

import random
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.parse import urlencode

import duckdb

from .candidates import game_ids_for_labels
from .config import SAMPLE_GROUP_RECENT, USER_AGENT
from .recent import fetch_json, insert_game, label_games

JsonFetcher = Callable[[str], Any]

LEADERBOARD_URL = "https://aoe4world.com/api/v0/leaderboards/rm_solo?page={page}"
PLAYER_GAMES_URL = "https://aoe4world.com/api/v0/players/{profile_id}/games"

# (name, page_start, page_end_exclusive, priority)
RANKED_TIERS = [
    ("elite",    2,   24, 1),   # rank 51–1200
    ("high",    24,  114, 2),   # rank 1200–5700
    ("mid",    114,  225, 3),   # rank 5700–11250
    ("low_mid", 225, 344, 4),   # rank 11250–17200
    ("low",    344,  544, 5),   # rank 17200+
]


def _fetch_player_games(
    profile_id: int,
    since: str,
    per_player: int,
    fetcher: JsonFetcher,
) -> list[dict]:
    params = urlencode({
        "leaderboard": "rm_solo",
        "limit": per_player,
        "since": since,
    })
    url = f"{PLAYER_GAMES_URL.format(profile_id=profile_id)}?{params}"
    try:
        payload = fetcher(url)
        return payload.get("games", []) if isinstance(payload, dict) else []
    except Exception:
        return []


def _pending_games_query(
    conn: duckdb.DuckDBPyConnection,
    group: str,
    limit: int,
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT
            l.game_id,
            l.reason,
            g.map,
            g.season,
            g.patch,
            CAST(g.started_at AS VARCHAR) AS started_at,
            (
                SELECT p.profile_id
                FROM participants p
                WHERE p.game_id = l.game_id
                ORDER BY p.rating DESC NULLS LAST, p.profile_id ASC
                LIMIT 1
            ) AS profile_id
        FROM replay_candidate_labels l
        LEFT JOIN replay_downloads d ON d.game_id = l.game_id
        LEFT JOIN games g ON g.game_id = l.game_id
        WHERE l.sample_group = ?
          AND coalesce(d.status, '') NOT IN ('downloaded', 'assigned')
        ORDER BY l.priority ASC, l.created_at ASC, l.game_id DESC
        LIMIT ?
        """,
        [group, limit],
    ).fetchall()
    return [
        {
            "game_id": int(row[0]),
            "tier": row[1],
            "map": row[2],
            "season": row[3],
            "patch": row[4],
            "started_at": row[5],
            "profile_id": int(row[6]) if row[6] is not None else None,
        }
        for row in rows
    ]


PHASES_TOTAL = 1 + len(RANKED_TIERS)  # top50 + 5 tiers = 6


def discover_tiered_games(
    conn: duckdb.DuckDBPyConnection,
    days: int = 7,
    target_per_tier: int = 100,
    per_player: int = 25,
    sleep_seconds: float = 1.0,
    group: str = SAMPLE_GROUP_RECENT,
    fetcher: JsonFetcher = fetch_json,
    on_phase: Any = None,  # callable(phase_name, phases_done, phases_total)
) -> dict:
    since = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    summary: dict[str, Any] = {}
    total_new: int = 0

    def _phase_done(name: str, done: int) -> None:
        if on_phase:
            on_phase(name, done, PHASES_TOTAL)

    # ── Phase 1: top 50 — complete coverage ───────────────────────────────────
    try:
        top_page = fetcher(LEADERBOARD_URL.format(page=1))
        time.sleep(sleep_seconds)
        top50_players = (top_page.get("players") or [])[:50]
    except Exception:
        time.sleep(sleep_seconds)
        top50_players = []

    top50_game_ids: set[int] = set()
    for player in top50_players:
        profile_id = player.get("profile_id")
        if not profile_id:
            continue
        games = _fetch_player_games(int(profile_id), since, per_player, fetcher)
        time.sleep(sleep_seconds)
        for game in games:
            try:
                if insert_game(conn, game, "aoe4world_tiered_discovery"):
                    top50_game_ids.add(int(game["game_id"]))
            except Exception:
                pass

    new_top50 = label_games(conn, top50_game_ids, group, reason="tier:top50", priority=0)
    total_new += new_top50
    summary["top50"] = {"players": len(top50_players), "games_found": len(top50_game_ids)}
    _phase_done("top50", 1)

    # ── Phase 2: equal-count stratified tiers ─────────────────────────────────
    for tier_idx, (tier_name, page_start, page_end, priority) in enumerate(RANKED_TIERS):
        tier_game_ids: set[int] = set()
        n_collected = 0

        all_pages = list(range(page_start, page_end))
        random.shuffle(all_pages)

        for page_num in all_pages:
            if n_collected >= target_per_tier:
                break
            try:
                page_data = fetcher(LEADERBOARD_URL.format(page=page_num))
                time.sleep(sleep_seconds)
            except Exception:
                time.sleep(sleep_seconds)
                continue

            players = list(page_data.get("players") or [])
            random.shuffle(players)

            for player in players:
                if n_collected >= target_per_tier:
                    break
                profile_id = player.get("profile_id")
                if not profile_id:
                    continue
                games = _fetch_player_games(int(profile_id), since, per_player, fetcher)
                time.sleep(sleep_seconds)
                for game in games:
                    try:
                        if insert_game(conn, game, "aoe4world_tiered_discovery"):
                            gid = int(game["game_id"])
                            tier_game_ids.add(gid)
                            n_collected += 1
                    except Exception:
                        pass

        new_in_tier = label_games(conn, tier_game_ids, group, reason=f"tier:{tier_name}", priority=priority)
        total_new += new_in_tier
        summary[tier_name] = {"games_found": len(tier_game_ids)}
        _phase_done(tier_name, 2 + tier_idx)

    # ── Build pending list ─────────────────────────────────────────────────────
    games_list = _pending_games_query(conn, group, limit=100_000)
    return {
        **summary,
        "total_new": total_new,
        "total_pending": len(games_list),
        "games": games_list,
    }


def get_pending_games(
    conn: duckdb.DuckDBPyConnection,
    group: str = SAMPLE_GROUP_RECENT,
    limit: int = 50_000,
) -> dict:
    games_list = _pending_games_query(conn, group, limit=limit)
    return {"games": games_list, "total": len(games_list)}


def get_assigned_games(
    conn: duckdb.DuckDBPyConnection,
    group: str = SAMPLE_GROUP_RECENT,
    limit: int = 50_000,
) -> dict:
    rows = conn.execute(
        """
        SELECT
            d.game_id,
            l.reason,
            g.map,
            g.season,
            g.patch,
            CAST(g.started_at AS VARCHAR) AS started_at,
            (
                SELECT p.profile_id
                FROM participants p
                WHERE p.game_id = d.game_id
                ORDER BY p.rating DESC NULLS LAST, p.profile_id ASC
                LIMIT 1
            ) AS profile_id
        FROM replay_downloads d
        LEFT JOIN games g ON g.game_id = d.game_id
        LEFT JOIN replay_candidate_labels l
               ON l.game_id = d.game_id AND l.sample_group = ?
        WHERE d.status = 'assigned'
        ORDER BY l.priority ASC NULLS LAST, d.downloaded_at ASC
        LIMIT ?
        """,
        [group, limit],
    ).fetchall()
    games_list = [
        {
            "game_id": int(row[0]),
            "tier": row[1],
            "map": row[2],
            "season": row[3],
            "patch": row[4],
            "started_at": row[5],
            "profile_id": int(row[6]) if row[6] is not None else None,
        }
        for row in rows
    ]
    return {"games": games_list, "total": len(games_list)}
