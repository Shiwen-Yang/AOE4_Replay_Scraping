from __future__ import annotations

import random
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode

import duckdb

from .candidates import game_ids_for_labels
from .config import RATING_BUCKETS, SAMPLE_GROUP_RECENT, USER_AGENT
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

MMR_TIERS = [row[0] for row in RATING_BUCKETS]
GAP_BUCKETS = ["0-50", "51-100", "101-200", ">200"]
HORIZON_DAYS = [3, 7, 14, 30]
PER_PLAYER_LIMIT = 50
ZERO_ACCEPTED_COOLDOWN = timedelta(hours=48)
ZERO_GAMES_COOLDOWN = timedelta(hours=96)
TOP_CLOSE_GAP_BUCKETS = {"0-50", "51-100"}
TOP50_CLOSE_REASON = "quota:top50_close"

_TIER_WEIGHT = {"elite": 5, "high": 4, "mid": 3, "mid_low": 2, "low": 1}
_GAP_WEIGHT = {"0-50": 4, "51-100": 3, "101-200": 2, ">200": 1}
_MMR_TO_RANKED_TIER = {"mid_low": "low_mid"}


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


def _cell_for_values(avg_mmr: float | None, mmr_gap: float | None) -> tuple[str, str] | None:
    if avg_mmr is None or mmr_gap is None:
        return None
    tier = None
    for name, minimum, maximum in RATING_BUCKETS:
        if minimum is not None and avg_mmr < minimum:
            continue
        if maximum is not None and avg_mmr >= maximum:
            continue
        tier = name
        break
    if tier is None:
        return None
    if mmr_gap <= 50:
        gap = "0-50"
    elif mmr_gap <= 100:
        gap = "51-100"
    elif mmr_gap <= 200:
        gap = "101-200"
    else:
        gap = ">200"
    return tier, gap


def _profile_id_set(values: Any) -> set[int]:
    ids: set[int] = set()
    for value in values or []:
        if value is None:
            continue
        try:
            ids.add(int(value))
        except (TypeError, ValueError):
            continue
    return ids


def _game_players(game: dict[str, Any]) -> list[dict[str, Any]]:
    players = []
    for team in game.get("teams") or []:
        for entry in team or []:
            player = entry.get("player") if isinstance(entry, dict) else None
            if isinstance(player, dict):
                players.append(player)
    return players


def _game_shape_issue(game: Any) -> str | None:
    if not isinstance(game, dict):
        return "bad_payload"
    if "game_id" not in game:
        return "missing_game_id"
    if game.get("kind") != "rm_1v1":
        return "bad_kind"
    players = _game_players(game)
    if len(players) != 2:
        return "bad_player_count"
    try:
        for player in players:
            int(player["profile_id"])
    except (KeyError, TypeError, ValueError):
        return "missing_profile_id"
    if any(player.get("mmr") is None and player.get("rating") is None for player in players):
        return "missing_rating"
    return None


def _bump_unbucketable(
    stats: dict[str, int],
    sample_stats: dict[str, int],
    reason: str,
) -> None:
    stats["unbucketable"] += 1
    sample_stats["unbucketable"] += 1
    stats[f"unbucketable_{reason}"] = stats.get(f"unbucketable_{reason}", 0) + 1


def _classified_from_values(
    avg_mmr: float | None,
    mmr_gap: float | None,
    profile_ids: set[int],
    top50_profile_ids: set[int] | None = None,
    include_top50: bool = True,
) -> dict[str, Any] | None:
    cell = _cell_for_values(avg_mmr, mmr_gap)
    if cell is None:
        return None
    top50_involved = bool(top50_profile_ids and profile_ids & top50_profile_ids)
    if top50_involved:
        if include_top50 and mmr_gap is not None and mmr_gap < 100:
            return {
                "match_tier": "top50",
                "gap_bucket": "0-100",
                "avg_mmr": avg_mmr,
                "mmr_gap": mmr_gap,
                "top50_involved": True,
            }
        return None
    tier, gap = cell
    return {
        "match_tier": tier,
        "gap_bucket": gap,
        "avg_mmr": avg_mmr,
        "mmr_gap": mmr_gap,
        "top50_involved": False,
    }


def game_cell(conn: duckdb.DuckDBPyConnection, game_id: int) -> dict[str, Any] | None:
    return classified_game_cell(conn, game_id, top50_profile_ids=set(), include_top50=False)


def classified_game_cell(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    top50_profile_ids: set[int] | None = None,
    include_top50: bool = True,
) -> dict[str, Any] | None:
    row = conn.execute(
        """
        SELECT
            avg(coalesce(mmr, rating)) AS avg_mmr,
            max(coalesce(mmr, rating)) - min(coalesce(mmr, rating)) AS mmr_gap,
            count(coalesce(mmr, rating)) AS rated_players,
            list(profile_id) AS profile_ids
        FROM participants
        WHERE game_id = ?
        """,
        [game_id],
    ).fetchone()
    if not row or row[2] != 2:
        return None
    avg_mmr = float(row[0]) if row[0] is not None else None
    mmr_gap = float(row[1]) if row[1] is not None else None
    return _classified_from_values(
        avg_mmr,
        mmr_gap,
        _profile_id_set(row[3]),
        top50_profile_ids=top50_profile_ids,
        include_top50=include_top50,
    )


def current_top50_profile_ids(fetcher: JsonFetcher = fetch_json) -> set[int]:
    payload = fetcher(LEADERBOARD_URL.format(page=1))
    players = payload.get("players", []) if isinstance(payload, dict) else []
    ids: set[int] = set()
    for player in players[:50]:
        if not isinstance(player, dict):
            continue
        profile_id = player.get("profile_id")
        if profile_id is None:
            continue
        try:
            ids.add(int(profile_id))
        except (TypeError, ValueError):
            continue
    return ids


def _cell_sql_select() -> str:
    return """
        (
            SELECT avg(coalesce(p.mmr, p.rating))
            FROM participants p
            WHERE p.game_id = l.game_id
        ) AS avg_mmr,
        (
            SELECT max(coalesce(p.mmr, p.rating)) - min(coalesce(p.mmr, p.rating))
            FROM participants p
            WHERE p.game_id = l.game_id
        ) AS mmr_gap
    """


def _enrich_game_row(row: tuple[Any, ...], top50_profile_ids: set[int] | None = None) -> dict[str, Any]:
    avg_mmr = float(row[7]) if row[7] is not None else None
    mmr_gap = float(row[8]) if row[8] is not None else None
    cell = None
    if row[1] == TOP50_CLOSE_REASON:
        cell = {"match_tier": "top50", "gap_bucket": "0-100"}
    else:
        raw_cell = _cell_for_values(avg_mmr, mmr_gap)
        cell = {"match_tier": raw_cell[0], "gap_bucket": raw_cell[1]} if raw_cell else None
    return {
        "game_id": int(row[0]),
        "tier": row[1],
        "map": row[2],
        "season": row[3],
        "patch": row[4],
        "started_at": row[5],
        "profile_id": int(row[6]) if row[6] is not None else None,
        "avg_mmr": round(avg_mmr, 1) if avg_mmr is not None else None,
        "mmr_gap": round(mmr_gap, 1) if mmr_gap is not None else None,
        "match_tier": cell["match_tier"] if cell else None,
        "gap_bucket": cell["gap_bucket"] if cell else None,
    }


def _empty_grid(value: int = 0) -> dict[str, dict[str, int]]:
    return {tier: {gap: value for gap in GAP_BUCKETS} for tier in MMR_TIERS}


def normalize_quota_grid(quota_grid: dict[str, Any]) -> dict[str, dict[str, int]]:
    normalized = _empty_grid()
    for tier in MMR_TIERS:
        source_row = quota_grid.get(tier, {}) if isinstance(quota_grid, dict) else {}
        for gap in GAP_BUCKETS:
            try:
                normalized[tier][gap] = max(0, int(source_row.get(gap, 0)))
            except (TypeError, ValueError, AttributeError):
                normalized[tier][gap] = 0
    return normalized


def normalize_horizon_days(horizon_days: Any = None) -> list[int]:
    source = horizon_days if horizon_days is not None else HORIZON_DAYS
    normalized: list[int] = []
    for value in source or []:
        try:
            days = int(value)
        except (TypeError, ValueError):
            continue
        if days <= 0 or days > 365 or days in normalized:
            continue
        normalized.append(days)
    return normalized or list(HORIZON_DAYS)


def quota_distribution(
    conn: duckdb.DuckDBPyConnection,
    group: str = SAMPLE_GROUP_RECENT,
    top50_profile_ids: set[int] | None = None,
) -> dict[str, dict[str, int]]:
    rows = conn.execute(
        """
        SELECT l.game_id
        FROM replay_candidate_labels l
        LEFT JOIN replay_downloads d ON d.game_id = l.game_id
        LEFT JOIN replay_unobtainable_games u ON u.game_id = l.game_id
        WHERE l.sample_group = ?
          AND coalesce(d.status, '') != 'unobtainable'
          AND u.game_id IS NULL
        """,
        [group],
    ).fetchall()
    distribution = _empty_grid()
    for (game_id,) in rows:
        cell = classified_game_cell(
            conn,
            int(game_id),
            top50_profile_ids=top50_profile_ids,
            include_top50=False,
        )
        if cell:
            distribution[cell["match_tier"]][cell["gap_bucket"]] += 1
    return distribution


def quota_inventory(
    conn: duckdb.DuckDBPyConnection,
    group: str = SAMPLE_GROUP_RECENT,
    top50_profile_ids: set[int] | None = None,
) -> dict[str, Any]:
    rows = conn.execute(
        """
        SELECT l.game_id
        FROM replay_candidate_labels l
        LEFT JOIN replay_downloads d ON d.game_id = l.game_id
        LEFT JOIN replay_unobtainable_games u ON u.game_id = l.game_id
        WHERE l.sample_group = ?
          AND coalesce(d.status, '') != 'unobtainable'
          AND u.game_id IS NULL
        """,
        [group],
    ).fetchall()
    matrix = _empty_grid()
    top50_close = 0
    for (game_id,) in rows:
        cell = classified_game_cell(
            conn,
            int(game_id),
            top50_profile_ids=top50_profile_ids,
            include_top50=True,
        )
        if not cell:
            continue
        if cell["match_tier"] == "top50":
            top50_close += 1
        else:
            matrix[cell["match_tier"]][cell["gap_bucket"]] += 1
    return {"matrix": matrix, "top50_close": top50_close}


def quota_deficits(
    targets: dict[str, dict[str, int]],
    distribution: dict[str, dict[str, int]],
) -> dict[str, dict[str, int]]:
    return {
        tier: {
            gap: max(0, targets[tier][gap] - distribution.get(tier, {}).get(gap, 0))
            for gap in GAP_BUCKETS
        }
        for tier in MMR_TIERS
    }


def _flatten_grid(grid: dict[str, dict[str, int]]) -> dict[str, int]:
    return {f"{tier}:{gap}": grid[tier][gap] for tier in MMR_TIERS for gap in GAP_BUCKETS}


def _priority_cells(grid: dict[str, dict[str, int]]) -> list[tuple[str, str, int]]:
    cells = [
        (tier, gap, count)
        for tier in MMR_TIERS
        for gap, count in grid[tier].items()
        if count > 0
    ]
    cells.sort(
        key=lambda item: (
            -item[2],
            -_GAP_WEIGHT[item[1]],
            -_TIER_WEIGHT[item[0]],
        )
    )
    return cells


def _tier_order(deficits: dict[str, dict[str, int]], targets: dict[str, dict[str, int]]) -> list[str]:
    active = deficits if _priority_cells(deficits) else targets
    tiers = list(MMR_TIERS)
    tiers.sort(
        key=lambda tier: (
            -sum(active[tier].values()),
            -_TIER_WEIGHT[tier],
        )
    )
    return [tier for tier in tiers if sum(targets[tier].values()) > 0]


def _should_accept_cell(
    tier: str,
    gap: str,
    targets: dict[str, dict[str, int]],
    deficits: dict[str, dict[str, int]],
) -> bool:
    if targets[tier][gap] <= 0:
        return False
    if tier == "elite" and gap not in TOP_CLOSE_GAP_BUCKETS:
        return False
    if deficits[tier][gap] > 0:
        return True
    if _priority_cells(deficits):
        return False
    total_target = sum(sum(row.values()) for row in targets.values())
    if total_target <= 0:
        return False
    return random.random() < (targets[tier][gap] / total_target)


def _record_fetch_error(stats: dict[str, int], exc: Exception) -> None:
    stats["fetch_failures"] += 1
    if isinstance(exc, HTTPError) and exc.code == 429:
        stats["http_429"] += 1
    elif isinstance(exc, HTTPError):
        stats["http_other"] += 1
    elif isinstance(exc, (TimeoutError, URLError)):
        stats["network_errors"] += 1


def _sleep_or_stop(seconds: float, stop_event: threading.Event | None) -> bool:
    if seconds <= 0:
        return bool(stop_event and stop_event.is_set())
    if stop_event:
        return stop_event.wait(seconds)
    time.sleep(seconds)
    return False


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _profile_window_cooldown_active(
    conn: duckdb.DuckDBPyConnection,
    profile_id: int,
    horizon_days: int,
    now: datetime,
) -> bool:
    row = conn.execute(
        """
        SELECT sampled_at, games_checked, accepted
        FROM replay_discovery_profile_windows
        WHERE profile_id = ? AND horizon_days = ?
        """,
        [profile_id, horizon_days],
    ).fetchone()
    if not row:
        return False
    sampled_at, games_checked, accepted = row
    if sampled_at is None:
        return False
    if int(accepted or 0) > 0:
        return False
    cooldown = ZERO_GAMES_COOLDOWN if int(games_checked or 0) == 0 else ZERO_ACCEPTED_COOLDOWN
    return now - _as_utc(sampled_at) < cooldown


def _record_profile_window_sample(
    conn: duckdb.DuckDBPyConnection,
    profile_id: int,
    horizon_days: int,
    sampled_at: datetime,
    sample_stats: dict[str, int],
) -> None:
    conn.execute(
        "DELETE FROM replay_discovery_profile_windows WHERE profile_id = ? AND horizon_days = ?",
        [profile_id, horizon_days],
    )
    conn.execute(
        """
        INSERT INTO replay_discovery_profile_windows
            (profile_id, horizon_days, sampled_at, games_checked, accepted,
             quota_rejected, duplicates, unbucketable)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            profile_id,
            horizon_days,
            sampled_at,
            sample_stats.get("games_checked", 0),
            sample_stats.get("accepted", 0),
            sample_stats.get("quota_rejected", 0),
            sample_stats.get("duplicates", 0),
            sample_stats.get("unbucketable", 0),
        ],
    )


def _pending_games_query(
    conn: duckdb.DuckDBPyConnection,
    group: str,
    limit: int,
) -> list[dict]:
    rows = conn.execute(
        f"""
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
            ) AS profile_id,
            {_cell_sql_select()}
        FROM replay_candidate_labels l
        LEFT JOIN replay_downloads d ON d.game_id = l.game_id
        LEFT JOIN replay_unobtainable_games u ON u.game_id = l.game_id
        LEFT JOIN games g ON g.game_id = l.game_id
        WHERE l.sample_group = ?
          AND coalesce(d.status, '') NOT IN ('downloaded', 'assigned', 'unobtainable')
          AND u.game_id IS NULL
        ORDER BY l.priority ASC, l.created_at ASC, l.game_id DESC
        LIMIT ?
        """,
        [group, limit],
    ).fetchall()
    return [_enrich_game_row(row) for row in rows]


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


def discover_quota_games(
    conn: duckdb.DuckDBPyConnection,
    quota_grid: dict[str, Any],
    top50_target: int = 0,
    top50_profile_ids: set[int] | None = None,
    sleep_seconds: float = 1.0,
    group: str = SAMPLE_GROUP_RECENT,
    fetcher: JsonFetcher = fetch_json,
    on_status: Any = None,
    stop_event: threading.Event | None = None,
    max_api_calls: int | None = None,
    horizon_days: list[int] | None = None,
) -> dict[str, Any]:
    targets = normalize_quota_grid(quota_grid)
    horizons = normalize_horizon_days(horizon_days)
    top50_target = max(0, int(top50_target or 0))
    top50_profile_ids = set(top50_profile_ids or [])
    if sum(sum(row.values()) for row in targets.values()) <= 0 and top50_target <= 0:
        raise ValueError("quota grid or top-50 target must contain at least one positive target")

    stats = {
        "api_calls": 0,
        "fetch_failures": 0,
        "http_429": 0,
        "http_other": 0,
        "network_errors": 0,
        "profiles_scanned": 0,
        "profiles_skipped_cooldown": 0,
        "games_checked": 0,
        "quota_rejected": 0,
        "duplicates": 0,
        "unbucketable": 0,
        "accepted": 0,
        "new_labels": 0,
        "top50_accepted": 0,
        "top50_gap_rejected": 0,
    }
    accepted_by_cell = _empty_grid()
    accepted_top50 = 0
    seen_game_ids: set[int] = set()

    def can_call() -> bool:
        return max_api_calls is None or stats["api_calls"] < max_api_calls

    def emit(status: str, phase: str, horizon: int | None = None) -> None:
        if not on_status:
            return
        inventory = quota_inventory(conn, group, top50_profile_ids=top50_profile_ids)
        distribution = inventory["matrix"]
        deficits = quota_deficits(targets, distribution)
        on_status(
            {
                "status": status,
                "phase": phase,
                "horizon_days": horizon,
                "targets": targets,
                "distribution": distribution,
                "deficits": deficits,
                "top50_target": top50_target,
                "top50_inventory": inventory["top50_close"],
                "top50_deficit": max(0, top50_target - inventory["top50_close"]),
                "horizons": horizons,
                "accepted_top50": accepted_top50,
                "accepted_by_cell": accepted_by_cell,
                "stats": dict(stats),
            }
        )

    emit("running", "starting")

    while can_call() and not (stop_event and stop_event.is_set()):
        made_progress = False
        inventory = quota_inventory(conn, group, top50_profile_ids=top50_profile_ids)
        distribution = inventory["matrix"]
        deficits = quota_deficits(targets, distribution)
        tier_order = _tier_order(deficits, targets)
        wants_top50 = top50_target > inventory["top50_close"] and bool(top50_profile_ids)
        if not tier_order and not wants_top50:
            break

        for horizon in horizons:
            if not can_call() or (stop_event and stop_event.is_set()):
                break
            since = (datetime.now(timezone.utc) - timedelta(days=horizon)).isoformat()
            emit("running", "scanning", horizon)

            top50_deficit = max(
                0,
                top50_target - quota_inventory(conn, group, top50_profile_ids=top50_profile_ids)["top50_close"],
            )
            if top50_deficit > 0 and top50_profile_ids:
                profile_ids = list(top50_profile_ids)
                random.shuffle(profile_ids)
                for profile_id in profile_ids:
                    if top50_deficit <= 0 or not can_call() or (stop_event and stop_event.is_set()):
                        break
                    now = datetime.now(timezone.utc)
                    if _profile_window_cooldown_active(conn, profile_id, horizon, now):
                        stats["profiles_skipped_cooldown"] += 1
                        continue
                    params = urlencode({
                        "leaderboard": "rm_solo",
                        "limit": PER_PLAYER_LIMIT,
                        "since": since,
                    })
                    url = f"{PLAYER_GAMES_URL.format(profile_id=profile_id)}?{params}"
                    stats["api_calls"] += 1
                    emit("running", "fetching_top50_games", horizon)
                    try:
                        payload = fetcher(url)
                    except Exception as exc:
                        _record_fetch_error(stats, exc)
                        if _sleep_or_stop(sleep_seconds, stop_event):
                            break
                        continue
                    if _sleep_or_stop(sleep_seconds, stop_event):
                        break

                    games = payload.get("games", []) if isinstance(payload, dict) else []
                    stats["profiles_scanned"] += 1
                    sample_stats = {
                        "games_checked": 0,
                        "accepted": 0,
                        "quota_rejected": 0,
                        "duplicates": 0,
                        "unbucketable": 0,
                    }
                    for game in games:
                        if top50_deficit <= 0:
                            break
                        stats["games_checked"] += 1
                        sample_stats["games_checked"] += 1
                        try:
                            game_id = int(game["game_id"])
                        except (KeyError, TypeError, ValueError):
                            _bump_unbucketable(stats, sample_stats, "missing_game_id")
                            continue
                        shape_issue = _game_shape_issue(game)
                        if shape_issue:
                            _bump_unbucketable(stats, sample_stats, shape_issue)
                            continue
                        if game_id in seen_game_ids:
                            stats["duplicates"] += 1
                            sample_stats["duplicates"] += 1
                            continue
                        seen_game_ids.add(game_id)
                        try:
                            if not insert_game(conn, game, "aoe4world_quota_discovery"):
                                _bump_unbucketable(stats, sample_stats, "insert_rejected")
                                continue
                        except Exception:
                            _bump_unbucketable(stats, sample_stats, "insert_error")
                            continue

                        cell = classified_game_cell(
                            conn,
                            game_id,
                            top50_profile_ids=top50_profile_ids,
                            include_top50=True,
                        )
                        if not cell or cell["match_tier"] != "top50":
                            stats["quota_rejected"] += 1
                            stats["top50_gap_rejected"] += 1
                            sample_stats["quota_rejected"] += 1
                            continue

                        inserted = label_games(
                            conn,
                            {game_id},
                            group,
                            reason=TOP50_CLOSE_REASON,
                            priority=0,
                        )
                        if inserted:
                            made_progress = True
                            stats["new_labels"] += inserted
                        stats["accepted"] += 1
                        stats["top50_accepted"] += 1
                        sample_stats["accepted"] += 1
                        accepted_top50 += 1
                        top50_deficit -= 1
                    _record_profile_window_sample(conn, profile_id, horizon, now, sample_stats)

            for tier_name in tier_order:
                if not can_call() or (stop_event and stop_event.is_set()):
                    break
                ranked_name = _MMR_TO_RANKED_TIER.get(tier_name, tier_name)
                ranked_tier = next((row for row in RANKED_TIERS if row[0] == ranked_name), None)
                if ranked_tier is None:
                    continue
                _, page_start, page_end, priority = ranked_tier
                pages = list(range(page_start, page_end))
                random.shuffle(pages)

                for page_num in pages:
                    if not can_call() or (stop_event and stop_event.is_set()):
                        break
                    stats["api_calls"] += 1
                    emit("running", "fetching_leaderboard", horizon)
                    try:
                        page_data = fetcher(LEADERBOARD_URL.format(page=page_num))
                    except Exception as exc:
                        _record_fetch_error(stats, exc)
                        if _sleep_or_stop(sleep_seconds, stop_event):
                            break
                        continue
                    if _sleep_or_stop(sleep_seconds, stop_event):
                        break

                    players = list(page_data.get("players") or []) if isinstance(page_data, dict) else []
                    random.shuffle(players)

                    for player in players:
                        if not can_call() or (stop_event and stop_event.is_set()):
                            break
                        profile_id = player.get("profile_id") if isinstance(player, dict) else None
                        if not profile_id:
                            continue
                        profile_id = int(profile_id)
                        now = datetime.now(timezone.utc)
                        if _profile_window_cooldown_active(conn, profile_id, horizon, now):
                            stats["profiles_skipped_cooldown"] += 1
                            continue
                        params = urlencode({
                            "leaderboard": "rm_solo",
                            "limit": PER_PLAYER_LIMIT,
                            "since": since,
                        })
                        url = f"{PLAYER_GAMES_URL.format(profile_id=profile_id)}?{params}"
                        stats["api_calls"] += 1
                        emit("running", "fetching_profile_games", horizon)
                        try:
                            payload = fetcher(url)
                        except Exception as exc:
                            _record_fetch_error(stats, exc)
                            if _sleep_or_stop(sleep_seconds, stop_event):
                                break
                            continue
                        if _sleep_or_stop(sleep_seconds, stop_event):
                            break

                        games = payload.get("games", []) if isinstance(payload, dict) else []
                        stats["profiles_scanned"] += 1
                        sample_stats = {
                            "games_checked": 0,
                            "accepted": 0,
                            "quota_rejected": 0,
                            "duplicates": 0,
                            "unbucketable": 0,
                        }
                        for game in games:
                            stats["games_checked"] += 1
                            sample_stats["games_checked"] += 1
                            try:
                                game_id = int(game["game_id"])
                            except (KeyError, TypeError, ValueError):
                                _bump_unbucketable(stats, sample_stats, "missing_game_id")
                                continue
                            shape_issue = _game_shape_issue(game)
                            if shape_issue:
                                _bump_unbucketable(stats, sample_stats, shape_issue)
                                continue
                            if game_id in seen_game_ids:
                                stats["duplicates"] += 1
                                sample_stats["duplicates"] += 1
                                continue
                            seen_game_ids.add(game_id)
                            try:
                                if not insert_game(conn, game, "aoe4world_quota_discovery"):
                                    _bump_unbucketable(stats, sample_stats, "insert_rejected")
                                    continue
                            except Exception:
                                _bump_unbucketable(stats, sample_stats, "insert_error")
                                continue

                            cell = classified_game_cell(
                                conn,
                                game_id,
                                top50_profile_ids=top50_profile_ids,
                                include_top50=False,
                            )
                            if not cell:
                                _bump_unbucketable(stats, sample_stats, "classification_failed")
                                continue
                            cell_tier = cell["match_tier"]
                            gap = cell["gap_bucket"]
                            distribution = quota_distribution(
                                conn,
                                group,
                                top50_profile_ids=top50_profile_ids,
                            )
                            deficits = quota_deficits(targets, distribution)
                            if not _should_accept_cell(cell_tier, gap, targets, deficits):
                                stats["quota_rejected"] += 1
                                sample_stats["quota_rejected"] += 1
                                continue

                            reason = f"quota:{cell_tier}:{gap}"
                            inserted = label_games(conn, {game_id}, group, reason=reason, priority=priority)
                            if inserted:
                                made_progress = True
                                stats["new_labels"] += inserted
                            stats["accepted"] += 1
                            sample_stats["accepted"] += 1
                            accepted_by_cell[cell_tier][gap] += 1
                        _record_profile_window_sample(conn, profile_id, horizon, now, sample_stats)

            if made_progress:
                break
        if not made_progress and not can_call():
            break

    final_status = "stopped" if stop_event and stop_event.is_set() else "done"
    emit(final_status, "finished")
    inventory = quota_inventory(conn, group, top50_profile_ids=top50_profile_ids)
    distribution = inventory["matrix"]
    games_list = _pending_games_query(conn, group, limit=100_000)
    return {
        "targets": targets,
        "distribution": distribution,
        "deficits": quota_deficits(targets, distribution),
        "top50_target": top50_target,
        "top50_inventory": inventory["top50_close"],
        "top50_deficit": max(0, top50_target - inventory["top50_close"]),
        "horizons": horizons,
        "accepted_top50": accepted_top50,
        "accepted_by_cell": accepted_by_cell,
        "stats": stats,
        "total_new": stats["new_labels"],
        "total_pending": len(games_list),
        "games": games_list,
        "stopped": final_status == "stopped",
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
        f"""
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
            ) AS profile_id,
            {_cell_sql_select()}
        FROM replay_downloads d
        LEFT JOIN games g ON g.game_id = d.game_id
        LEFT JOIN replay_unobtainable_games u ON u.game_id = d.game_id
        LEFT JOIN replay_candidate_labels l
               ON l.game_id = d.game_id AND l.sample_group = ?
        WHERE d.status = 'assigned'
          AND u.game_id IS NULL
        ORDER BY l.priority ASC NULLS LAST, d.downloaded_at ASC
        LIMIT ?
        """,
        [group, limit],
    ).fetchall()
    games_list = [_enrich_game_row(row) for row in rows]
    return {"games": games_list, "total": len(games_list)}
