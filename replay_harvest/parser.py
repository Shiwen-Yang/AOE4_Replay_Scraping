from __future__ import annotations

from datetime import datetime
from pathlib import Path
import subprocess
from typing import Callable, Sequence

import duckdb

from .config import AOE4_PARSING_CLI_PROJECT, DEFAULT_PARSER_VERSION, PARSED_REPLAY_DIR


Runner = Callable[..., subprocess.CompletedProcess]


def _count_jsonl_events(output_dir: Path) -> int:
    total = 0
    for path in output_dir.rglob("*.jsonl"):
        with path.open("r", encoding="utf-8") as handle:
            total += sum(1 for _ in handle)
    return total


def _record_parse(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    parser_version: str,
    status: str,
    output_dir: Path,
    event_count: int | None,
    error: str | None,
) -> None:
    conn.execute(
        """
        DELETE FROM replay_parse_runs
        WHERE game_id = ? AND parser_version = ?
        """,
        [game_id, parser_version],
    )
    conn.execute(
        """
        INSERT INTO replay_parse_runs
            (game_id, parser_version, parsed_at, status, output_dir, event_count,
             first_event_time, last_event_time, last_error)
        VALUES (?, ?, ?, ?, ?, ?, NULL, NULL, ?)
        """,
        [
            game_id,
            parser_version,
            datetime.utcnow(),
            status,
            str(output_dir),
            event_count,
            error,
        ],
    )


def parse_one(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    raw_path: Path,
    output_root: Path = PARSED_REPLAY_DIR,
    parser_version: str = DEFAULT_PARSER_VERSION,
    parser_project: Path = AOE4_PARSING_CLI_PROJECT,
    catalog_dir: Path | None = None,
    raw: bool = False,
    runner: Runner = subprocess.run,
) -> str:
    output_dir = output_root / str(game_id)
    output_dir.mkdir(parents=True, exist_ok=True)
    cmd: list[str] = [
        "dotnet",
        "run",
        "--project",
        str(parser_project),
        "--",
        "--input",
        str(raw_path),
        "--output",
        str(output_dir),
    ]
    if catalog_dir is not None:
        cmd.extend(["--catalog-dir", str(catalog_dir)])
    if raw:
        cmd.append("--raw")

    result = runner(cmd, capture_output=True, text=True)
    if result.returncode == 0:
        event_count = _count_jsonl_events(output_dir)
        _record_parse(conn, game_id, parser_version, "parsed", output_dir, event_count, None)
        return "parsed"

    stderr = getattr(result, "stderr", "") or getattr(result, "stdout", "") or "parser failed"
    _record_parse(conn, game_id, parser_version, "failed", output_dir, None, stderr[:1000])
    return "failed"


def downloaded_replays_to_parse(
    conn: duckdb.DuckDBPyConnection,
    parser_version: str,
    limit: int,
    sample_group: str | None = None,
) -> list[tuple[int, Path]]:
    group_sql = ""
    params: list[object] = [parser_version]
    if sample_group is not None:
        group_sql = "AND d.sample_group = ?"
        params.append(sample_group)
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT d.game_id, d.raw_path
        FROM replay_downloads d
        LEFT JOIN replay_parse_runs p
          ON p.game_id = d.game_id
         AND p.parser_version = ?
        WHERE d.status = 'downloaded'
          AND d.raw_path IS NOT NULL
          AND p.game_id IS NULL
          {group_sql}
        ORDER BY d.downloaded_at ASC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [(int(game_id), Path(raw_path)) for game_id, raw_path in rows]


def parse_downloaded(
    conn: duckdb.DuckDBPyConnection,
    limit: int,
    parser_version: str = DEFAULT_PARSER_VERSION,
    sample_group: str | None = None,
    output_root: Path = PARSED_REPLAY_DIR,
    parser_project: Path = AOE4_PARSING_CLI_PROJECT,
    catalog_dir: Path | None = None,
    raw: bool = False,
    runner: Runner = subprocess.run,
) -> dict[str, int]:
    counts = {"parsed": 0, "failed": 0}
    for game_id, raw_path in downloaded_replays_to_parse(conn, parser_version, limit, sample_group):
        status = parse_one(
            conn,
            game_id,
            raw_path,
            output_root=output_root,
            parser_version=parser_version,
            parser_project=parser_project,
            catalog_dir=catalog_dir,
            raw=raw,
            runner=runner,
        )
        counts[status] = counts.get(status, 0) + 1
    return counts

