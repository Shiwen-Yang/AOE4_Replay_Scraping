from __future__ import annotations

import json
from pathlib import Path

import duckdb

from .config import REPORT_DIR


def build_report(conn: duckdb.DuckDBPyConnection) -> dict[str, object]:
    by_status = conn.execute(
        """
        SELECT sample_group, status, count(*) AS games
        FROM replay_downloads
        GROUP BY sample_group, status
        ORDER BY sample_group, status
        """
    ).fetchall()
    labels = conn.execute(
        """
        SELECT sample_group, reason, count(*) AS games
        FROM replay_candidate_labels
        GROUP BY sample_group, reason
        ORDER BY sample_group, reason
        """
    ).fetchall()
    by_season = conn.execute(
        """
        SELECT l.sample_group, g.season, count(DISTINCT l.game_id) AS games
        FROM replay_candidate_labels l
        JOIN games g ON g.game_id = l.game_id
        GROUP BY l.sample_group, g.season
        ORDER BY l.sample_group, g.season
        """
    ).fetchall()
    by_map = conn.execute(
        """
        SELECT l.sample_group, g.map, count(DISTINCT l.game_id) AS games
        FROM replay_candidate_labels l
        JOIN games g ON g.game_id = l.game_id
        GROUP BY l.sample_group, g.map
        ORDER BY l.sample_group, games DESC
        LIMIT 100
        """
    ).fetchall()

    return {
        "downloads_by_status": [
            {"sample_group": r[0], "status": r[1], "games": int(r[2])}
            for r in by_status
        ],
        "labels": [
            {"sample_group": r[0], "reason": r[1], "games": int(r[2])}
            for r in labels
        ],
        "labels_by_season": [
            {"sample_group": r[0], "season": r[1], "games": int(r[2])}
            for r in by_season
        ],
        "labels_by_map_top100": [
            {"sample_group": r[0], "map": r[1], "games": int(r[2])}
            for r in by_map
        ],
    }


def write_report(conn: duckdb.DuckDBPyConnection, report_dir: Path = REPORT_DIR) -> dict[str, object]:
    report_dir.mkdir(parents=True, exist_ok=True)
    report = build_report(conn)
    json_path = report_dir / "replay_sample_report.json"
    md_path = report_dir / "replay_sample_report.md"
    json_path.write_text(json.dumps(report, indent=2, sort_keys=True), encoding="utf-8")

    lines = ["# Replay Sample Report", ""]
    for section, rows in report.items():
        lines.append(f"## {section}")
        if not rows:
            lines.append("")
            lines.append("_No rows._")
            lines.append("")
            continue
        keys = list(rows[0].keys())
        lines.append("| " + " | ".join(keys) + " |")
        lines.append("| " + " | ".join(["---"] * len(keys)) + " |")
        for row in rows:
            lines.append("| " + " | ".join(str(row.get(k, "")) for k in keys) + " |")
        lines.append("")
    md_path.write_text("\n".join(lines), encoding="utf-8")
    return report

