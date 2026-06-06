from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import json
import time
from typing import Any, Callable
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import duckdb

from .config import USER_AGENT
from .recent import _flat_players, _result


JsonFetcher = Callable[[str], Any]

AOE4WORLD_GAME_URL = "https://aoe4world.com/api/v0/players/{profile_id}/games/{game_id}"
OFFICIAL_HISTORY_URL = "https://aoe-api.worldsedgelink.com/community/leaderboard/getRecentMatchHistory"


@dataclass(frozen=True)
class Outcome:
    winner_profile_id: int
    loser_profile_id: int


def fetch_json(url: str) -> Any:
    req = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def parse_aoe4world_outcome(payload: Any, game_id: int | None = None) -> Outcome | None:
    game = _unwrap_game_payload(payload)
    if game_id is not None and game.get("game_id") is not None and int(game["game_id"]) != game_id:
        return None
    players = _flat_players(game)
    rows = []
    for player in players:
        profile_id = player.get("profile_id")
        result = _result(player.get("result"))
        if profile_id is None or result is None:
            continue
        rows.append((int(profile_id), result))
    return _outcome_from_rows(rows)


def parse_official_outcome(payload: Any, game_id: int | None = None) -> Outcome | None:
    reports = _official_reports(payload, game_id)
    rows = []
    for report in reports:
        profile_id = report.get("profile_id")
        resulttype = report.get("resulttype")
        if profile_id is None or resulttype is None:
            continue
        if int(resulttype) == 1:
            rows.append((int(profile_id), True))
        elif int(resulttype) == 0:
            rows.append((int(profile_id), False))
    return _outcome_from_rows(rows)


def hydrate_outcomes(
    conn: duckdb.DuckDBPyConnection,
    limit: int = 100,
    sample_group: str | None = None,
    sleep_seconds: float = 1.0,
    fetcher: JsonFetcher = fetch_json,
    use_official_fallback: bool = True,
) -> dict[str, int]:
    counts = {"filled": 0, "already_labeled": 0, "unresolved": 0, "conflict": 0, "failed": 0}
    for game_id, profile_id in games_missing_valid_outcomes(conn, limit=limit, sample_group=sample_group):
        outcome = _fetch_aoe4world_outcome(game_id, profile_id, fetcher)
        source = "aoe4world"
        error = None
        if outcome is None and use_official_fallback:
            outcome = _fetch_official_outcome(game_id, profile_id, fetcher)
            source = "official_recent_match_history"
        if outcome is None:
            status = "unresolved"
            _record_fetch(conn, game_id, source, profile_id, status, None, "outcome_not_found")
            counts[status] += 1
        else:
            status = apply_outcome(conn, game_id, outcome)
            _record_fetch(conn, game_id, source, profile_id, status, outcome, error)
            counts[status] = counts.get(status, 0) + 1
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
    return counts


def games_missing_valid_outcomes(
    conn: duckdb.DuckDBPyConnection,
    limit: int,
    sample_group: str | None = None,
) -> list[tuple[int, int]]:
    group_join = ""
    group_filter = ""
    params: list[object] = []
    if sample_group is not None:
        group_join = "JOIN replay_candidate_labels l ON l.game_id = d.game_id"
        group_filter = "AND l.sample_group = ?"
        params.append(sample_group)
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT
            d.game_id,
            coalesce(d.profile_id_used, min(p.profile_id)) AS profile_id,
            min(d.downloaded_at) AS first_downloaded_at
        FROM replay_downloads d
        {group_join}
        LEFT JOIN participants p ON p.game_id = d.game_id
        WHERE d.status = 'downloaded'
          {group_filter}
        GROUP BY d.game_id, d.profile_id_used
        HAVING count(DISTINCT p.profile_id) != 2
            OR count(CASE WHEN p.result = true THEN 1 END) != 1
            OR count(CASE WHEN p.result = false THEN 1 END) != 1
        ORDER BY first_downloaded_at ASC NULLS LAST, d.game_id ASC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [(int(row[0]), int(row[1])) for row in rows if row[1] is not None]


def apply_outcome(conn: duckdb.DuckDBPyConnection, game_id: int, outcome: Outcome) -> str:
    desired = {
        outcome.winner_profile_id: True,
        outcome.loser_profile_id: False,
    }
    rows = conn.execute(
        """
        SELECT profile_id, result
        FROM participants
        WHERE game_id = ?
        """,
        [game_id],
    ).fetchall()
    existing = {int(profile_id): result for profile_id, result in rows}
    if set(existing) != set(desired):
        return "conflict"
    if all(existing[profile_id] == result for profile_id, result in desired.items()):
        return "already_labeled"
    for profile_id, result in desired.items():
        current = existing[profile_id]
        if current is not None and current != result:
            return "conflict"
    conn.executemany(
        """
        UPDATE participants
        SET result = ?
        WHERE game_id = ? AND profile_id = ?
        """,
        [(result, game_id, profile_id) for profile_id, result in desired.items()],
    )
    return "filled"


def training_label_rows(
    conn: duckdb.DuckDBPyConnection,
    sample_group: str | None = None,
    limit: int | None = None,
) -> list[dict[str, Any]]:
    group_filter = ""
    params: list[object] = []
    if sample_group is not None:
        group_filter = "AND d.sample_group = ?"
        params.append(sample_group)
    limit_sql = ""
    if limit is not None:
        limit_sql = "LIMIT ?"
        params.append(limit)
    rows = conn.execute(
        f"""
        SELECT
            d.game_id,
            d.raw_path,
            max(CASE WHEN p.result = true THEN p.profile_id END) AS winner_profile_id,
            max(CASE WHEN p.result = false THEN p.profile_id END) AS loser_profile_id,
            max(CASE WHEN p.result = true THEN p.civilization END) AS winner_civilization,
            max(CASE WHEN p.result = false THEN p.civilization END) AS loser_civilization,
            max(CASE WHEN p.result = true THEN p.rating END) AS winner_rating,
            max(CASE WHEN p.result = false THEN p.rating END) AS loser_rating,
            g.map,
            g.patch,
            g.season,
            g.duration,
            d.sample_group
        FROM replay_downloads d
        JOIN games g ON g.game_id = d.game_id
        JOIN participants p ON p.game_id = d.game_id
        WHERE d.status = 'downloaded'
          AND g.kind = 'rm_1v1'
          {group_filter}
        GROUP BY d.game_id, d.raw_path, g.map, g.patch, g.season, g.duration, d.sample_group
        HAVING count(DISTINCT p.profile_id) = 2
           AND count(CASE WHEN p.result = true THEN 1 END) = 1
           AND count(CASE WHEN p.result = false THEN 1 END) = 1
        ORDER BY d.game_id ASC
        {limit_sql}
        """,
        params,
    ).fetchall()
    keys = [
        "game_id",
        "raw_path",
        "winner_profile_id",
        "loser_profile_id",
        "winner_civilization",
        "loser_civilization",
        "winner_rating",
        "loser_rating",
        "map",
        "patch",
        "season",
        "duration",
        "sample_group",
    ]
    return [dict(zip(keys, row)) for row in rows]


def _fetch_aoe4world_outcome(game_id: int, profile_id: int, fetcher: JsonFetcher) -> Outcome | None:
    url = AOE4WORLD_GAME_URL.format(profile_id=profile_id, game_id=game_id)
    try:
        return parse_aoe4world_outcome(fetcher(url), game_id=game_id)
    except Exception:
        return None


def _fetch_official_outcome(game_id: int, profile_id: int, fetcher: JsonFetcher) -> Outcome | None:
    params = urlencode({"title": "age4", "profile_ids": f"[{profile_id}]"})
    url = f"{OFFICIAL_HISTORY_URL}?{params}"
    try:
        return parse_official_outcome(fetcher(url), game_id=game_id)
    except Exception:
        return None


def _record_fetch(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    source: str,
    profile_id: int,
    status: str,
    outcome: Outcome | None,
    error: str | None,
) -> None:
    conn.execute(
        """
        INSERT OR REPLACE INTO replay_outcome_fetches
            (game_id, source, profile_id_used, fetched_at, status,
             winner_profile_id, loser_profile_id, last_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            game_id,
            source,
            profile_id,
            datetime.utcnow(),
            status,
            outcome.winner_profile_id if outcome else None,
            outcome.loser_profile_id if outcome else None,
            error,
        ],
    )


def _unwrap_game_payload(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        for key in ("game", "data"):
            value = payload.get(key)
            if isinstance(value, dict):
                return value
        return payload
    return {}


def _official_reports(payload: Any, game_id: int | None) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        return []
    histories = payload.get("matchhistoryreportresults")
    if isinstance(histories, list):
        rows = [row for row in histories if isinstance(row, dict)]
        if game_id is None:
            return rows
        matching = [
            row for row in rows
            if row.get("matchhistory_id") is not None and int(row["matchhistory_id"]) == game_id
        ]
        if any(row.get("matchhistory_id") is not None for row in rows):
            return matching
        return rows
    for key in ("matchhistory", "matches", "items", "data"):
        value = payload.get(key)
        if not isinstance(value, list):
            continue
        for match in value:
            if not isinstance(match, dict):
                continue
            match_id = match.get("matchhistory_id") or match.get("id") or match.get("game_id")
            if game_id is not None and match_id is not None and int(match_id) != game_id:
                continue
            reports = match.get("matchhistoryreportresults") or match.get("report_results")
            if isinstance(reports, list):
                return [row for row in reports if isinstance(row, dict)]
    return []


def _outcome_from_rows(rows: list[tuple[int, bool]]) -> Outcome | None:
    winners = [profile_id for profile_id, result in rows if result is True]
    losers = [profile_id for profile_id, result in rows if result is False]
    if len(winners) != 1 or len(losers) != 1:
        return None
    return Outcome(winner_profile_id=winners[0], loser_profile_id=losers[0])
