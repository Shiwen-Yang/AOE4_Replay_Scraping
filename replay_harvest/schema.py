from __future__ import annotations

import duckdb


DDL = [
    """
    CREATE TABLE IF NOT EXISTS replay_downloads (
        game_id BIGINT PRIMARY KEY,
        profile_id_used BIGINT,
        raw_path VARCHAR,
        download_date DATE,
        downloaded_at TIMESTAMP,
        status VARCHAR,
        size_bytes BIGINT,
        sha256 VARCHAR,
        source VARCHAR,
        sample_group VARCHAR,
        attempt_count INTEGER,
        last_error VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS replay_candidate_labels (
        game_id BIGINT,
        sample_group VARCHAR,
        reason VARCHAR,
        priority INTEGER,
        created_at TIMESTAMP,
        PRIMARY KEY (game_id, sample_group)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS replay_unobtainable_games (
        game_id BIGINT PRIMARY KEY,
        marked_at TIMESTAMP,
        reason VARCHAR,
        detail VARCHAR,
        source VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS replay_discovery_profile_windows (
        profile_id BIGINT,
        horizon_days INTEGER,
        sampled_at TIMESTAMP,
        games_checked INTEGER,
        accepted INTEGER,
        quota_rejected INTEGER,
        duplicates INTEGER,
        unbucketable INTEGER,
        PRIMARY KEY (profile_id, horizon_days)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS top_player_identities (
        canonical_profile_id BIGINT,
        profile_id BIGINT,
        leaderboard_rank INTEGER,
        rating INTEGER,
        source VARCHAR,
        discovered_at TIMESTAMP,
        PRIMARY KEY (canonical_profile_id, profile_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS replay_parse_runs (
        game_id BIGINT,
        parser_version VARCHAR,
        parsed_at TIMESTAMP,
        status VARCHAR,
        output_dir VARCHAR,
        event_count BIGINT,
        first_event_time DOUBLE,
        last_event_time DOUBLE,
        last_error VARCHAR,
        PRIMARY KEY (game_id, parser_version)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS replay_harvest_runs (
        run_id VARCHAR PRIMARY KEY,
        started_at TIMESTAMP,
        finished_at TIMESTAMP,
        target_count INTEGER,
        downloaded_count INTEGER,
        failed_count INTEGER,
        rate_limit_seconds DOUBLE,
        notes VARCHAR
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS replay_outcome_fetches (
        game_id BIGINT,
        source VARCHAR,
        profile_id_used BIGINT,
        fetched_at TIMESTAMP,
        status VARCHAR,
        winner_profile_id BIGINT,
        loser_profile_id BIGINT,
        last_error VARCHAR,
        PRIMARY KEY (game_id, source)
    )
    """,
]


INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_replay_downloads_status ON replay_downloads(status)",
    "CREATE INDEX IF NOT EXISTS idx_replay_downloads_group ON replay_downloads(sample_group)",
    "CREATE INDEX IF NOT EXISTS idx_replay_labels_group ON replay_candidate_labels(sample_group)",
    "CREATE INDEX IF NOT EXISTS idx_replay_unobtainable_source ON replay_unobtainable_games(source)",
    "CREATE INDEX IF NOT EXISTS idx_replay_discovery_windows_sampled ON replay_discovery_profile_windows(sampled_at)",
    "CREATE INDEX IF NOT EXISTS idx_top_player_profile ON top_player_identities(profile_id)",
    "CREATE INDEX IF NOT EXISTS idx_replay_outcomes_status ON replay_outcome_fetches(status)",
]


def init_schema(conn: duckdb.DuckDBPyConnection) -> None:
    for sql in DDL:
        conn.execute(sql)
    for sql in INDEXES:
        conn.execute(sql)
