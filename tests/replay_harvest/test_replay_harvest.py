from __future__ import annotations

import gzip
from urllib.error import HTTPError
from pathlib import Path
import subprocess

import duckdb

from replay_harvest.candidates import bucket_for_rating, label_balanced_candidates
from replay_harvest.downloader import download_group, download_one
from replay_harvest.discovery import discover_quota_games, game_cell, quota_deficits, quota_distribution, quota_inventory
from replay_harvest.outcomes import (
    Outcome,
    apply_outcome,
    hydrate_outcomes,
    parse_aoe4world_outcome,
    parse_official_outcome,
    training_label_rows,
)
from replay_harvest.parser import parse_one
from replay_harvest.recent import discover_recent_games, insert_game as insert_recent_game
from replay_harvest.schema import init_schema
from replay_harvest.top_players import label_top100_games, parse_alt_profile_ids, parse_leaderboard


def make_conn():
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE games (
            game_id BIGINT PRIMARY KEY,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            duration INTEGER,
            map_id BIGINT,
            map VARCHAR,
            kind VARCHAR,
            server VARCHAR,
            patch VARCHAR,
            season INTEGER,
            source_file VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE participants (
            game_id BIGINT NOT NULL,
            profile_id BIGINT NOT NULL,
            result BOOLEAN,
            civilization VARCHAR,
            civilization_randomized BOOLEAN,
            rating INTEGER,
            rating_diff INTEGER,
            mmr INTEGER,
            mmr_diff INTEGER,
            input_type VARCHAR,
            PRIMARY KEY (game_id, profile_id)
        )
        """
    )
    init_schema(conn)
    return conn


def insert_game(conn, game_id: int, rating_a: int, rating_b: int | None = None, season: int = 11):
    if rating_b is None:
        rating_b = rating_a
    conn.execute(
        """
        INSERT INTO games VALUES
        (?, '2025-01-01 00:00:00', '2025-01-01 00:30:00', 1800, 1, 'Dry Arabia',
         'rm_1v1', 'server', '1.0', ?, 'test')
        """,
        [game_id, season],
    )
    conn.execute(
        """
        INSERT INTO participants VALUES
        (?, ?, true, 'English', false, ?, 0, ?, 0, 'keyboard'),
        (?, ?, false, 'French', false, ?, 0, ?, 0, 'keyboard')
        """,
        [game_id, game_id * 10 + 1, rating_a, rating_a, game_id, game_id * 10 + 2, rating_b, rating_b],
    )


def test_schema_creation_creates_replay_tables():
    conn = make_conn()
    tables = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    assert "replay_downloads" in tables
    assert "replay_candidate_labels" in tables
    assert "replay_unobtainable_games" in tables
    assert "replay_discovery_profile_windows" in tables
    assert "top_player_identities" in tables
    assert "replay_parse_runs" in tables
    assert "replay_outcome_fetches" in tables


def test_bucket_boundaries():
    assert bucket_for_rating(728) == "low"
    assert bucket_for_rating(729) == "mid_low"
    assert bucket_for_rating(903) == "mid_low"
    assert bucket_for_rating(904) == "mid"
    assert bucket_for_rating(1071) == "mid"
    assert bucket_for_rating(1072) == "high"
    assert bucket_for_rating(1399) == "high"
    assert bucket_for_rating(1400) == "elite"


def test_label_balanced_candidates_excludes_downloaded_games():
    conn = make_conn()
    for game_id, rating in enumerate([700, 800, 950, 1200, 1500], start=1):
        insert_game(conn, game_id, rating)
    conn.execute(
        """
        INSERT INTO replay_downloads
        VALUES (1, 11, 'x', current_date, current_timestamp, 'downloaded', 10, 'hash',
                'test', 'balanced_10k', 1, NULL)
        """
    )

    counts = label_balanced_candidates(conn, limit=5)
    labeled = {row[0] for row in conn.execute("SELECT game_id FROM replay_candidate_labels").fetchall()}

    assert counts["low"] == 0
    assert labeled == {2, 3, 4, 5}


def test_parse_top_player_payloads_and_alts():
    leaderboard = {"players": [{"profile_id": 10, "rank": 1, "rating": 2000}]}
    players = parse_leaderboard(leaderboard, limit=1)
    assert players[0].profile_id == 10
    assert players[0].rank == 1
    assert players[0].rating == 2000

    assert parse_alt_profile_ids({"alts": [{"profile_id": 11}, 12]}, 10) == {10, 11, 12}


def test_label_top100_games_dedupes_alt_identity():
    conn = make_conn()
    insert_game(conn, 1, 2000)
    insert_game(conn, 2, 2100)
    conn.execute("UPDATE participants SET profile_id = 10 WHERE game_id = 1 AND result = true")
    conn.execute("UPDATE participants SET profile_id = 11 WHERE game_id = 2 AND result = true")

    def fetcher(url: str):
        if "leaderboards" in url:
            return {"players": [{"profile_id": 10, "rank": 1, "rating": 2200}]}
        return {"alts": [{"profile_id": 11}]}

    counts = label_top100_games(conn, fetcher=fetcher)
    assert counts["identities"] == 2
    assert counts["games"] == 2
    labels = conn.execute("SELECT count(*) FROM replay_candidate_labels WHERE sample_group = 'top100_complete'").fetchone()[0]
    assert labels == 2


def test_download_one_writes_file_and_records_status(tmp_path: Path):
    conn = make_conn()
    insert_game(conn, 123, 1500)
    conn.execute(
        """
        INSERT INTO replay_candidate_labels
        VALUES (123, 'balanced_10k', 'test', 0, current_timestamp)
        """
    )
    payload = gzip.compress(b"replay")

    status = download_one(
        conn,
        123,
        "balanced_10k",
        raw_root=tmp_path,
        fetcher=lambda game_id, profile_id: payload,
    )

    assert status == "downloaded"
    row = conn.execute(
        "SELECT status, size_bytes, raw_path FROM replay_downloads WHERE game_id = 123"
    ).fetchone()
    assert row[0] == "downloaded"
    assert row[1] == len(payload)
    assert Path(row[2]).exists()


def test_download_group_handles_429_without_crashing(tmp_path: Path):
    conn = make_conn()
    insert_game(conn, 124, 1500)
    conn.execute(
        """
        INSERT INTO replay_candidate_labels
        VALUES (124, 'balanced_10k', 'test', 0, current_timestamp)
        """
    )

    def fetcher(game_id, profile_id):
        raise HTTPError("url", 429, "Too Many Requests", hdrs=None, fp=None)

    counts = download_group(
        conn,
        "balanced_10k",
        limit=1,
        sleep_min=0,
        sleep_max=0,
        raw_root=tmp_path,
        fetcher=fetcher,
        retry_pause_seconds=0,
    )

    assert counts["failed"] == 1
    row = conn.execute("SELECT status, last_error FROM replay_downloads WHERE game_id = 124").fetchone()
    assert row == ("failed", "http_429")


def test_parse_one_records_success(tmp_path: Path):
    conn = make_conn()
    raw_path = tmp_path / "AgeIV_Replay_123.gz"
    raw_path.write_bytes(gzip.compress(b"replay"))

    def runner(cmd, capture_output, text):
        output_dir = Path(cmd[cmd.index("--output") + 1])
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "intent_timeline.jsonl").write_text("{}\n{}\n", encoding="utf-8")
        return subprocess.CompletedProcess(cmd, 0, stdout="ok", stderr="")

    status = parse_one(
        conn,
        123,
        raw_path,
        output_root=tmp_path / "parsed",
        parser_version="test-parser",
        runner=runner,
    )

    assert status == "parsed"
    row = conn.execute(
        "SELECT status, event_count FROM replay_parse_runs WHERE game_id = 123 AND parser_version = 'test-parser'"
    ).fetchone()
    assert row == ("parsed", 2)


def test_discover_recent_games_inserts_and_labels_current_games():
    conn = make_conn()
    insert_game(conn, 1, 2000)
    conn.execute("UPDATE participants SET profile_id = 6943917 WHERE game_id = 1 AND result = true")

    payload = {
        "games": [
            {
                "game_id": 236420367,
                "started_at": "2026-06-03T14:44:10.000Z",
                "duration": 1127,
                "map": "Cliffside",
                "kind": "rm_1v1",
                "server": "UK",
                "patch": 10604,
                "season": 13,
                "teams": [
                    [{"player": {"profile_id": 10427060, "result": "loss", "civilization": "english", "rating": 1645}}],
                    [{"player": {"profile_id": 6943917, "result": "win", "civilization": "ottomans", "rating": 2120}}],
                ],
            }
        ]
    }

    counts = discover_recent_games(
        conn,
        seed_limit=1,
        per_player=5,
        days=10,
        sleep_seconds=0,
        fetcher=lambda url: payload,
    )

    assert counts["unique_games"] == 1
    assert counts["labeled"] == 1
    game = conn.execute("SELECT kind, season, patch FROM games WHERE game_id = 236420367").fetchone()
    assert game == ("rm_1v1", 13, "10604")


def test_insert_game_backfills_null_participant_results():
    conn = make_conn()
    conn.execute(
        """
        INSERT INTO games VALUES
        (236420367, '2026-06-03 14:44:10', NULL, 1127, NULL, 'Cliffside',
         'rm_1v1', 'UK', '10604', 13, 'old')
        """
    )
    conn.execute(
        """
        INSERT INTO participants VALUES
        (236420367, 10427060, NULL, 'english', NULL, NULL, NULL, NULL, NULL, NULL),
        (236420367, 6943917, NULL, 'ottomans', NULL, NULL, NULL, NULL, NULL, NULL)
        """
    )
    payload = {
        "game_id": 236420367,
        "started_at": "2026-06-03T14:44:10.000Z",
        "duration": 1127,
        "map": "Cliffside",
        "kind": "rm_1v1",
        "server": "UK",
        "patch": 10604,
        "season": 13,
        "teams": [
            [{"player": {"profile_id": 10427060, "result": "loss", "civilization": "english", "rating": 1645}}],
            [{"player": {"profile_id": 6943917, "result": "win", "civilization": "ottomans", "rating": 2120}}],
        ],
    }

    assert insert_recent_game(conn, payload, "aoe4world_test")
    rows = conn.execute(
        """
        SELECT profile_id, result, rating
        FROM participants
        WHERE game_id = 236420367
        ORDER BY profile_id
        """
    ).fetchall()

    assert rows == [(6943917, True, 2120), (10427060, False, 1645)]


def test_insert_game_works_without_table_constraints():
    conn = duckdb.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE games (
            game_id BIGINT,
            started_at TIMESTAMP,
            finished_at TIMESTAMP,
            duration INTEGER,
            map_id BIGINT,
            map VARCHAR,
            kind VARCHAR,
            server VARCHAR,
            patch VARCHAR,
            season INTEGER,
            source_file VARCHAR
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE participants (
            game_id BIGINT,
            profile_id BIGINT,
            result BOOLEAN,
            civilization VARCHAR,
            civilization_randomized BOOLEAN,
            rating INTEGER,
            rating_diff INTEGER,
            mmr INTEGER,
            mmr_diff INTEGER,
            input_type VARCHAR
        )
        """
    )
    payload = {
        "game_id": 236420368,
        "started_at": "2026-06-03T14:44:10.000Z",
        "duration": 1127,
        "map": "Cliffside",
        "kind": "rm_1v1",
        "server": "UK",
        "patch": 10604,
        "season": 13,
        "teams": [
            [{"player": {"profile_id": 10427060, "result": "loss", "civilization": "english", "rating": 1645}}],
            [{"player": {"profile_id": 6943917, "result": "win", "civilization": "ottomans", "rating": 2120}}],
        ],
    }

    assert insert_recent_game(conn, payload, "aoe4world_test")
    assert insert_recent_game(conn, payload, "aoe4world_test")
    assert conn.execute("SELECT count(*) FROM games").fetchone()[0] == 1
    assert conn.execute("SELECT count(*) FROM participants").fetchone()[0] == 2


def test_parse_outcomes_from_api_payloads():
    aoe4world_payload = {
        "game": {
            "game_id": 10,
            "teams": [
                [{"player": {"profile_id": 1, "result": "win"}}],
                [{"player": {"profile_id": 2, "result": "loss"}}],
            ],
        }
    }
    official_payload = {
        "matches": [
            {
                "matchhistory_id": 10,
                "matchhistoryreportresults": [
                    {"profile_id": 1, "resulttype": 1},
                    {"profile_id": 2, "resulttype": 0},
                ],
            }
        ]
    }

    assert parse_aoe4world_outcome(aoe4world_payload, game_id=10) == Outcome(1, 2)
    assert parse_official_outcome(official_payload, game_id=10) == Outcome(1, 2)


def test_apply_outcome_fills_nulls_and_preserves_conflicts():
    conn = make_conn()
    insert_game(conn, 20, 1500)
    conn.execute("UPDATE participants SET result = NULL WHERE game_id = 20")

    assert apply_outcome(conn, 20, Outcome(201, 202)) == "filled"
    rows = conn.execute(
        "SELECT profile_id, result FROM participants WHERE game_id = 20 ORDER BY profile_id"
    ).fetchall()
    assert rows == [(201, True), (202, False)]
    assert apply_outcome(conn, 20, Outcome(202, 201)) == "conflict"
    rows = conn.execute(
        "SELECT profile_id, result FROM participants WHERE game_id = 20 ORDER BY profile_id"
    ).fetchall()
    assert rows == [(201, True), (202, False)]


def test_hydrate_outcomes_records_fetch_and_training_labels(tmp_path: Path):
    conn = make_conn()
    insert_game(conn, 30, 1500)
    conn.execute("UPDATE participants SET result = NULL WHERE game_id = 30")
    conn.execute(
        """
        INSERT INTO replay_downloads
        VALUES (30, 301, ?, current_date, current_timestamp, 'downloaded', 10, 'hash',
                'test', 'recent_rm_1v1', 1, NULL)
        """,
        [str(tmp_path / "AgeIV_Replay_30.gz")],
    )

    def fetcher(url: str):
        return {
            "game_id": 30,
            "teams": [
                [{"player": {"profile_id": 301, "result": "win", "civilization": "English", "rating": 1500}}],
                [{"player": {"profile_id": 302, "result": "loss", "civilization": "French", "rating": 1500}}],
            ],
        }

    counts = hydrate_outcomes(conn, limit=10, sleep_seconds=0, fetcher=fetcher)
    labels = training_label_rows(conn)
    fetch_row = conn.execute(
        """
        SELECT status, winner_profile_id, loser_profile_id
        FROM replay_outcome_fetches
        WHERE game_id = 30 AND source = 'aoe4world'
        """
    ).fetchone()

    assert counts["filled"] == 1
    assert fetch_row == ("filled", 301, 302)
    assert labels[0]["game_id"] == 30
    assert labels[0]["winner_profile_id"] == 301
    assert labels[0]["loser_profile_id"] == 302


def test_game_cell_uses_mmr_then_rating_and_buckets_gap():
    conn = make_conn()
    insert_game(conn, 40, 1000)
    conn.execute("UPDATE participants SET mmr = 1500 WHERE game_id = 40 AND profile_id = 401")
    conn.execute("UPDATE participants SET mmr = 1540 WHERE game_id = 40 AND profile_id = 402")

    cell = game_cell(conn, 40)

    assert cell["match_tier"] == "elite"
    assert cell["gap_bucket"] == "0-50"
    assert cell["avg_mmr"] == 1520
    assert cell["mmr_gap"] == 40


def test_quota_distribution_excludes_unobtainable_games():
    conn = make_conn()
    insert_game(conn, 41, 1100)
    insert_game(conn, 42, 1100)
    conn.execute(
        """
        INSERT INTO replay_candidate_labels VALUES
        (41, 'recent_rm_1v1', 'quota:high:0-50', 0, current_timestamp),
        (42, 'recent_rm_1v1', 'quota:high:0-50', 0, current_timestamp)
        """
    )
    conn.execute(
        """
        INSERT INTO replay_unobtainable_games
        VALUES (42, current_timestamp, 'manual_skip', NULL, 'manual')
        """
    )

    distribution = quota_distribution(conn)
    deficits = quota_deficits(
        {"low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
         "mid_low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
         "mid": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
         "high": {"0-50": 2, "51-100": 0, "101-200": 0, ">200": 0},
         "elite": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0}},
        distribution,
    )

    assert distribution["high"]["0-50"] == 1
    assert deficits["high"]["0-50"] == 1


def test_quota_inventory_separates_top50_close_games_from_matrix():
    conn = make_conn()
    insert_game(conn, 43, 1500, 1540)
    insert_game(conn, 44, 1500, 1540)
    conn.execute("UPDATE participants SET profile_id = 4301 WHERE game_id = 43 AND profile_id = 431")
    conn.execute(
        """
        INSERT INTO replay_candidate_labels VALUES
        (43, 'recent_rm_1v1', 'quota:top50_close', 0, current_timestamp),
        (44, 'recent_rm_1v1', 'quota:elite:0-50', 1, current_timestamp)
        """
    )

    inventory = quota_inventory(conn, top50_profile_ids={4301})

    assert inventory["top50_close"] == 1
    assert inventory["matrix"]["elite"]["0-50"] == 1


def test_discover_quota_games_accepts_matching_quota_cell():
    conn = make_conn()
    quota = {
        "low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid_low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "high": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "elite": {"0-50": 1, "51-100": 0, "101-200": 0, ">200": 0},
    }
    game_payload = {
        "game_id": 236420367,
        "started_at": "2026-06-03T14:44:10.000Z",
        "duration": 1127,
        "map": "Cliffside",
        "kind": "rm_1v1",
        "server": "UK",
        "patch": 10604,
        "season": 13,
        "teams": [
            [{"player": {"profile_id": 10427060, "result": "loss", "civilization": "english", "rating": 1500}}],
            [{"player": {"profile_id": 6943917, "result": "win", "civilization": "ottomans", "rating": 1530}}],
        ],
    }

    def fetcher(url: str):
        if "leaderboards" in url:
            return {"players": [{"profile_id": 10427060}]}
        return {"games": [game_payload]}

    result = discover_quota_games(
        conn,
        quota,
        sleep_seconds=0,
        fetcher=fetcher,
        max_api_calls=2,
        horizon_days=[5],
    )

    assert result["horizons"] == [5]
    assert result["total_new"] == 1
    assert result["distribution"]["elite"]["0-50"] == 1
    assert result["stats"]["api_calls"] == 2
    assert result["stats"]["profiles_scanned"] == 1
    assert result["stats"]["games_checked"] == 1
    assert result["stats"]["accepted"] == 1


def test_discover_quota_games_accepts_top50_close_target():
    conn = make_conn()
    quota = {
        "low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid_low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "high": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "elite": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
    }
    game_payload = {
        "game_id": 90,
        "started_at": "2026-06-03T14:44:10.000Z",
        "duration": 1127,
        "map": "Cliffside",
        "kind": "rm_1v1",
        "server": "UK",
        "patch": 10604,
        "season": 13,
        "teams": [
            [{"player": {"profile_id": 9001, "result": "loss", "civilization": "english", "rating": 1800}}],
            [{"player": {"profile_id": 9002, "result": "win", "civilization": "ottomans", "rating": 1860}}],
        ],
    }

    def fetcher(url: str):
        return {"games": [game_payload]}

    result = discover_quota_games(
        conn,
        quota,
        top50_target=1,
        top50_profile_ids={9001},
        sleep_seconds=0,
        fetcher=fetcher,
        max_api_calls=1,
    )

    assert result["top50_inventory"] == 1
    assert result["distribution"]["elite"]["51-100"] == 0
    assert result["stats"]["top50_accepted"] == 1
    assert result["total_new"] == 1


def test_discover_quota_games_rejects_top50_wide_gap_target():
    conn = make_conn()
    quota = {
        "low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid_low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "high": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "elite": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
    }
    game_payload = {
        "game_id": 91,
        "started_at": "2026-06-03T14:44:10.000Z",
        "duration": 1127,
        "map": "Cliffside",
        "kind": "rm_1v1",
        "server": "UK",
        "patch": 10604,
        "season": 13,
        "teams": [
            [{"player": {"profile_id": 9101, "result": "loss", "civilization": "english", "rating": 1800}}],
            [{"player": {"profile_id": 9102, "result": "win", "civilization": "ottomans", "rating": 2050}}],
        ],
    }

    def fetcher(url: str):
        return {"games": [game_payload]}

    result = discover_quota_games(
        conn,
        quota,
        top50_target=1,
        top50_profile_ids={9101},
        sleep_seconds=0,
        fetcher=fetcher,
        max_api_calls=1,
    )

    assert result["top50_inventory"] == 0
    assert result["stats"]["top50_gap_rejected"] == 1
    assert result["total_new"] == 0


def test_discover_quota_games_reports_unbucketable_reason():
    conn = make_conn()
    quota = {
        "low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid_low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "high": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "elite": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
    }
    game_payload = {
        "game_id": 92,
        "started_at": "2026-06-03T14:44:10.000Z",
        "duration": 1127,
        "map": "Cliffside",
        "kind": "rm_1v1",
        "server": "UK",
        "patch": 10604,
        "season": 13,
        "teams": [
            [{"player": {"profile_id": 9201, "result": "loss", "civilization": "english"}}],
            [{"player": {"profile_id": 9202, "result": "win", "civilization": "ottomans", "rating": 1200}}],
        ],
    }

    def fetcher(url: str):
        return {"games": [game_payload]}

    result = discover_quota_games(
        conn,
        quota,
        top50_target=1,
        top50_profile_ids={9201},
        sleep_seconds=0,
        fetcher=fetcher,
        max_api_calls=1,
    )

    assert result["stats"]["unbucketable"] == 1
    assert result["stats"]["unbucketable_missing_rating"] == 1
    assert result["total_new"] == 0



def test_discover_quota_games_cools_down_zero_accepted_profile_windows():
    conn = make_conn()
    quota = {
        "low": {"0-50": 1, "51-100": 0, "101-200": 0, ">200": 0},
        "mid_low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "high": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "elite": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
    }
    game_payload = {
        "game_id": 50,
        "started_at": "2026-06-03T14:44:10.000Z",
        "duration": 1127,
        "map": "Cliffside",
        "kind": "rm_1v1",
        "server": "UK",
        "patch": 10604,
        "season": 13,
        "teams": [
            [{"player": {"profile_id": 5001, "result": "loss", "civilization": "english", "rating": 1500}}],
            [{"player": {"profile_id": 5002, "result": "win", "civilization": "ottomans", "rating": 1530}}],
        ],
    }

    def fetcher(url: str):
        if "leaderboards" in url:
            return {"players": [{"profile_id": 5001}]}
        return {"games": [game_payload]}

    first = discover_quota_games(conn, quota, sleep_seconds=0, fetcher=fetcher, max_api_calls=2)
    second = discover_quota_games(conn, quota, sleep_seconds=0, fetcher=fetcher, max_api_calls=2)
    conn.execute(
        """
        UPDATE replay_discovery_profile_windows
        SET sampled_at = current_timestamp - INTERVAL 49 HOURS
        WHERE profile_id = 5001 AND horizon_days = 3
        """
    )
    third = discover_quota_games(conn, quota, sleep_seconds=0, fetcher=fetcher, max_api_calls=2)

    assert first["stats"]["profiles_scanned"] == 1
    assert first["stats"]["games_checked"] == 1
    assert first["stats"]["quota_rejected"] == 1
    assert first["stats"]["accepted"] == 0
    assert second["stats"]["profiles_scanned"] == 0
    assert second["stats"]["profiles_skipped_cooldown"] > 0
    assert third["stats"]["profiles_scanned"] == 1


def test_discover_quota_games_cools_down_empty_profile_windows_longer():
    conn = make_conn()
    quota = {
        "low": {"0-50": 1, "51-100": 0, "101-200": 0, ">200": 0},
        "mid_low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "high": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "elite": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
    }

    def fetcher(url: str):
        if "leaderboards" in url:
            return {"players": [{"profile_id": 6001}]}
        return {"games": []}

    first = discover_quota_games(conn, quota, sleep_seconds=0, fetcher=fetcher, max_api_calls=2)
    second = discover_quota_games(conn, quota, sleep_seconds=0, fetcher=fetcher, max_api_calls=2)
    conn.execute(
        """
        UPDATE replay_discovery_profile_windows
        SET sampled_at = current_timestamp - INTERVAL 97 HOURS
        WHERE profile_id = 6001 AND horizon_days = 3
        """
    )
    third = discover_quota_games(conn, quota, sleep_seconds=0, fetcher=fetcher, max_api_calls=2)

    assert first["stats"]["profiles_scanned"] == 1
    assert first["stats"]["games_checked"] == 0
    assert second["stats"]["profiles_scanned"] == 0
    assert second["stats"]["profiles_skipped_cooldown"] > 0
    assert third["stats"]["profiles_scanned"] == 1


def test_discover_quota_games_does_not_cool_down_accepted_profile_windows():
    conn = make_conn()
    quota = {
        "low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid_low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "high": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "elite": {"0-50": 1, "51-100": 0, "101-200": 0, ">200": 0},
    }
    game_payload = {
        "game_id": 70,
        "started_at": "2026-06-03T14:44:10.000Z",
        "duration": 1127,
        "map": "Cliffside",
        "kind": "rm_1v1",
        "server": "UK",
        "patch": 10604,
        "season": 13,
        "teams": [
            [{"player": {"profile_id": 7001, "result": "loss", "civilization": "english", "rating": 1500}}],
            [{"player": {"profile_id": 7002, "result": "win", "civilization": "ottomans", "rating": 1530}}],
        ],
    }

    def fetcher(url: str):
        if "leaderboards" in url:
            return {"players": [{"profile_id": 7001}]}
        return {"games": [game_payload]}

    first = discover_quota_games(conn, quota, sleep_seconds=0, fetcher=fetcher, max_api_calls=2)
    second = discover_quota_games(conn, quota, sleep_seconds=0, fetcher=fetcher, max_api_calls=2)

    assert first["stats"]["accepted"] == 1
    assert second["stats"]["profiles_scanned"] == 1
    assert second["stats"]["profiles_skipped_cooldown"] == 0


def test_discover_quota_games_rejects_wide_gap_elite_matches():
    conn = make_conn()
    quota = {
        "low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid_low": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "mid": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "high": {"0-50": 0, "51-100": 0, "101-200": 0, ">200": 0},
        "elite": {"0-50": 0, "51-100": 0, "101-200": 1, ">200": 1},
    }
    game_payload = {
        "game_id": 80,
        "started_at": "2026-06-03T14:44:10.000Z",
        "duration": 1127,
        "map": "Cliffside",
        "kind": "rm_1v1",
        "server": "UK",
        "patch": 10604,
        "season": 13,
        "teams": [
            [{"player": {"profile_id": 8001, "result": "loss", "civilization": "english", "rating": 1500}}],
            [{"player": {"profile_id": 8002, "result": "win", "civilization": "ottomans", "rating": 1800}}],
        ],
    }

    def fetcher(url: str):
        if "leaderboards" in url:
            return {"players": [{"profile_id": 8001}]}
        return {"games": [game_payload]}

    result = discover_quota_games(conn, quota, sleep_seconds=0, fetcher=fetcher, max_api_calls=2)

    assert result["stats"]["games_checked"] == 1
    assert result["stats"]["quota_rejected"] == 1
    assert result["stats"]["accepted"] == 0
    assert result["total_new"] == 0
