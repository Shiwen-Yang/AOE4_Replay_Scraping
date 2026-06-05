from __future__ import annotations

from pathlib import Path

import duckdb

from .config import DB_PATH


def get_conn(db_path: Path | str | None = None, read_only: bool = False) -> duckdb.DuckDBPyConnection:
    path = str(db_path or DB_PATH)
    conn = duckdb.connect(path, read_only=read_only)
    conn.execute("SET threads TO 4")
    conn.execute("SET memory_limit = '8GB'")
    return conn
