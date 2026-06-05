from __future__ import annotations

import duckdb

TABLES_TO_DROP = [
    "player_stats",
    "player_stats_ext",
    "civ_matchup_priors",
    "training_features",
    "h2h_priors",
    "player_civ_raw_games",
    "player_civ_extra",
    "player_map_archetype_stats",
    "player_raw_games",
    "civ_choice_candidate_rows",
    "civ_choice_training_matrix",
    "civ_choice_player_games",
    "civ_global_rates_by_season",
    "civ_global_rates_by_patch",
    "civ_first_seen",
    "map_metadata",
    "patch_metadata",
    "map_patch_priors",
]

ESSENTIAL_TABLES = {
    "games",
    "participants",
    "replay_candidate_labels",
    "replay_downloads",
    "replay_parse_runs",
    "top_player_identities",
    "replay_harvest_runs",
}


def shrink_db(conn: duckdb.DuckDBPyConnection, dry_run: bool = False) -> list[str]:
    """Drop ML feature tables not used by the replay harvester.

    Returns the list of tables that were (or would be) dropped.
    """
    existing = {row[0] for row in conn.execute("SHOW TABLES").fetchall()}
    to_drop = [t for t in TABLES_TO_DROP if t in existing]
    unknown = existing - ESSENTIAL_TABLES - set(TABLES_TO_DROP)

    print(f"Tables to drop ({len(to_drop)}):")
    for t in to_drop:
        try:
            count = conn.execute(f"SELECT count(*) FROM {t}").fetchone()[0]
            print(f"  {t:40s}  {count:>12,} rows")
        except Exception:
            print(f"  {t}")

    if unknown:
        print(f"\nUnknown tables (not touched): {sorted(unknown)}")

    if dry_run:
        print("\n(dry-run — no changes made)")
        return to_drop

    print()
    for t in to_drop:
        conn.execute(f"DROP TABLE IF EXISTS {t}")
        print(f"  Dropped {t}")

    print("\nRunning CHECKPOINT...")
    conn.execute("CHECKPOINT")
    print("Running VACUUM...")
    conn.execute("VACUUM")
    print("Done.")
    return to_drop
