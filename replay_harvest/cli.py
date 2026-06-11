from __future__ import annotations

import argparse
from pathlib import Path

from .candidates import label_balanced_candidates, summarize_labels
from .config import DB_PATH, SAMPLE_GROUP_BALANCED, SAMPLE_GROUP_RECENT, SAMPLE_GROUP_TOP100
from .db import get_conn
from .downloader import backfill_summary_files, download_group
from .outcomes import hydrate_outcomes, training_label_rows
from .parser import parse_downloaded
from .discovery import discover_tiered_games
from .recent import discover_recent_games
from .reports import write_report
from .schema import init_schema
from .shrink_db import shrink_db
from .top_players import label_top100_games


def _conn(args, read_only: bool = False):
    return get_conn(Path(args.db) if args.db else DB_PATH, read_only=read_only)


def _cmd_init_schema(args) -> None:
    conn = _conn(args)
    init_schema(conn)
    conn.close()
    print("Replay harvest schema initialized.")


def _cmd_label_balanced(args) -> None:
    conn = _conn(args)
    init_schema(conn)
    counts = label_balanced_candidates(
        conn,
        limit=args.limit,
        season=args.season,
        patch=args.patch,
        sample_group=args.group,
    )
    conn.close()
    print(counts)


def _cmd_label_top100(args) -> None:
    conn = _conn(args)
    init_schema(conn)
    counts = label_top100_games(conn)
    conn.close()
    print(counts)


def _cmd_candidates(args) -> None:
    conn = _conn(args, read_only=True)
    rows = summarize_labels(conn)
    conn.close()
    for row in rows:
        print(row)


def _cmd_download(args) -> None:
    conn = _conn(args)
    init_schema(conn)
    counts = download_group(
        conn,
        sample_group=args.group,
        limit=args.limit,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
    )
    conn.close()
    print(counts)


def _cmd_backfill_summaries(args) -> None:
    conn = _conn(args)
    init_schema(conn)
    counts = backfill_summary_files(
        conn,
        sleep_min=args.sleep_min,
        sleep_max=args.sleep_max,
    )
    conn.close()
    print(counts)


def _cmd_discover(args) -> None:
    conn = _conn(args)
    init_schema(conn)
    result = discover_tiered_games(
        conn,
        days=args.days,
        target_per_tier=args.target_per_tier,
        per_player=args.per_player,
        sleep_seconds=args.sleep_seconds,
        group=args.group,
    )
    conn.close()
    print(f"top50:   players={result['top50']['players']}  games={result['top50']['games_found']}")
    for tier in ("elite", "high", "mid", "low_mid", "low"):
        t = result.get(tier, {})
        print(f"{tier:8s}: games={t.get('games_found', 0)}")
    print(f"total_new={result['total_new']}  total_pending={result['total_pending']}")


def _cmd_discover_recent(args) -> None:
    conn = _conn(args)
    init_schema(conn)
    counts = discover_recent_games(
        conn,
        seed_limit=args.seed_limit,
        per_player=args.per_player,
        days=args.days,
        sleep_seconds=args.sleep_seconds,
        group=args.group,
    )
    conn.close()
    print(counts)


def _cmd_shrink_db(args) -> None:
    conn = _conn(args)
    shrink_db(conn, dry_run=args.dry_run)
    conn.close()


def _cmd_report(args) -> None:
    conn = _conn(args, read_only=True)
    report = write_report(conn)
    conn.close()
    print(report)


def _cmd_parse_downloaded(args) -> None:
    conn = _conn(args)
    init_schema(conn)
    counts = parse_downloaded(
        conn,
        limit=args.limit,
        parser_version=args.parser_version,
        sample_group=args.group,
        catalog_dir=Path(args.catalog_dir) if args.catalog_dir else None,
        raw=args.raw,
    )
    conn.close()
    print(counts)


def _cmd_hydrate_outcomes(args) -> None:
    conn = _conn(args)
    init_schema(conn)
    counts = hydrate_outcomes(
        conn,
        limit=args.limit,
        sample_group=args.group,
        sleep_seconds=args.sleep_seconds,
        use_official_fallback=not args.no_official_fallback,
    )
    conn.close()
    print(counts)


def _cmd_training_labels(args) -> None:
    conn = _conn(args, read_only=True)
    rows = training_label_rows(conn, sample_group=args.group, limit=args.limit)
    conn.close()
    for row in rows:
        print(row)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m replay_harvest",
        description="Harvest Age of Empires IV replay files for model training.",
    )
    parser.add_argument("--db", default=None, help="DuckDB path, default: /home/shiwen/GitHub/AOE4/aoe4.duckdb")

    sub = parser.add_subparsers(dest="command", required=True)

    p_init = sub.add_parser("init-schema", help="Create replay harvest tables")
    p_init.set_defaults(func=_cmd_init_schema)

    p_bal = sub.add_parser("label-balanced", help="Label balanced RM 1v1 replay candidates")
    p_bal.add_argument("--limit", type=int, default=10_000)
    p_bal.add_argument("--season", type=int, default=None)
    p_bal.add_argument("--patch", default=None)
    p_bal.add_argument("--group", default=SAMPLE_GROUP_BALANCED)
    p_bal.set_defaults(func=_cmd_label_balanced)

    p_top = sub.add_parser("label-top100", help="Label all games for top 100 canonical players")
    p_top.set_defaults(func=_cmd_label_top100)

    p_cand = sub.add_parser("candidates", help="Print candidate label summary")
    p_cand.set_defaults(func=_cmd_candidates)

    p_disc = sub.add_parser("discover", help="Discover recent RM 1v1 games stratified by skill tier")
    p_disc.add_argument("--days", type=int, default=7)
    p_disc.add_argument("--target-per-tier", type=int, default=100)
    p_disc.add_argument("--per-player", type=int, default=25)
    p_disc.add_argument("--sleep-seconds", type=float, default=1.0)
    p_disc.add_argument("--group", default=SAMPLE_GROUP_RECENT)
    p_disc.set_defaults(func=_cmd_discover)

    p_recent = sub.add_parser("discover-recent", help="Discover current recent RM 1v1 games from seeded player IDs")
    p_recent.add_argument("--seed-limit", type=int, default=200)
    p_recent.add_argument("--per-player", type=int, default=25)
    p_recent.add_argument("--days", type=int, default=10)
    p_recent.add_argument("--sleep-seconds", type=float, default=1.0)
    p_recent.add_argument("--group", default=SAMPLE_GROUP_RECENT)
    p_recent.set_defaults(func=_cmd_discover_recent)

    p_dl = sub.add_parser("download", help="Download labeled replay candidates")
    p_dl.add_argument("--group", default=SAMPLE_GROUP_BALANCED, choices=[SAMPLE_GROUP_BALANCED, SAMPLE_GROUP_TOP100, SAMPLE_GROUP_RECENT])
    p_dl.add_argument("--limit", type=int, default=1000)
    p_dl.add_argument("--sleep-min", type=float, default=15.0)
    p_dl.add_argument("--sleep-max", type=float, default=30.0)
    p_dl.set_defaults(func=_cmd_download)

    p_backfill = sub.add_parser("backfill-summaries", help="Fetch replay summary files for existing downloaded replays")
    p_backfill.add_argument("--sleep-min", type=float, default=15.0)
    p_backfill.add_argument("--sleep-max", type=float, default=30.0)
    p_backfill.set_defaults(func=_cmd_backfill_summaries)

    p_parse = sub.add_parser("parse-downloaded", help="Parse downloaded replay files and record parser status")
    p_parse.add_argument("--group", default=None, choices=[SAMPLE_GROUP_BALANCED, SAMPLE_GROUP_TOP100, SAMPLE_GROUP_RECENT])
    p_parse.add_argument("--limit", type=int, default=100)
    p_parse.add_argument("--parser-version", default="aoe4_parser_cli")
    p_parse.add_argument("--catalog-dir", default=None)
    p_parse.add_argument("--raw", action="store_true")
    p_parse.set_defaults(func=_cmd_parse_downloaded)

    p_out = sub.add_parser("hydrate-outcomes", help="Fetch and store missing win/loss labels for downloaded replays")
    p_out.add_argument("--group", default=None, choices=[SAMPLE_GROUP_BALANCED, SAMPLE_GROUP_TOP100, SAMPLE_GROUP_RECENT])
    p_out.add_argument("--limit", type=int, default=100)
    p_out.add_argument("--sleep-seconds", type=float, default=1.0)
    p_out.add_argument("--no-official-fallback", action="store_true")
    p_out.set_defaults(func=_cmd_hydrate_outcomes)

    p_labels = sub.add_parser("training-labels", help="Print valid downloaded replay outcome labels")
    p_labels.add_argument("--group", default=None, choices=[SAMPLE_GROUP_BALANCED, SAMPLE_GROUP_TOP100, SAMPLE_GROUP_RECENT])
    p_labels.add_argument("--limit", type=int, default=None)
    p_labels.set_defaults(func=_cmd_training_labels)

    p_report = sub.add_parser("report", help="Write replay sample reports")
    p_report.set_defaults(func=_cmd_report)

    p_shrink = sub.add_parser("shrink-db", help="Drop ML feature tables to reduce DB size")
    p_shrink.add_argument("--dry-run", action="store_true", help="Print what would be dropped without making changes")
    p_shrink.set_defaults(func=_cmd_shrink_db)

    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
