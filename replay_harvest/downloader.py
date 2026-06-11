from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import random
import re
import threading
import time
from pathlib import Path
from urllib.parse import urlencode
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb

from .candidates import game_ids_for_labels
from .config import RAW_REPLAY_DIR, REPLAY_DOWNLOAD_URL, REPLAY_FILES_URL, SUMMARY_REPLAY_DIR, USER_AGENT


class RetryableDownloadError(RuntimeError):
    def __init__(self, status: str, pause_seconds: float):
        super().__init__(status)
        self.status = status
        self.pause_seconds = pause_seconds


RATE_LIMIT_HEADERS = [
    "Retry-After",
    "X-RateLimit-Limit",
    "X-RateLimit-Remaining",
    "X-RateLimit-Reset",
    "X-RateLimit-Used",
]

REPLAY_FILENAME_RE = re.compile(r"^AgeIV_Replay_(\d+)\.gz$")


def _today_raw_dir(raw_root: Path = RAW_REPLAY_DIR) -> Path:
    path = raw_root / date.today().isoformat()
    path.mkdir(parents=True, exist_ok=True)
    return path


def _today_summary_dir(summary_root: Path = SUMMARY_REPLAY_DIR) -> Path:
    path = summary_root / date.today().isoformat()
    path.mkdir(parents=True, exist_ok=True)
    return path


def choose_profile_id(conn: duckdb.DuckDBPyConnection, game_id: int) -> int | None:
    row = conn.execute(
        """
        SELECT profile_id
        FROM participants
        WHERE game_id = ?
        ORDER BY rating DESC NULLS LAST, profile_id ASC
        LIMIT 1
        """,
        [game_id],
    ).fetchone()
    return int(row[0]) if row else None


def fetch_replay_bytes(
    game_id: int,
    profile_id: int,
    url_template: str = REPLAY_DOWNLOAD_URL,
    user_agent: str = USER_AGENT,
) -> bytes:
    url = url_template.format(game_id=game_id, profile_id=profile_id)
    req = Request(url, headers={"User-Agent": user_agent})
    with urlopen(req, timeout=120) as resp:
        return resp.read()


def fetch_replay_files_payload(
    game_id: int,
    replay_files_url: str = REPLAY_FILES_URL,
    user_agent: str = USER_AGENT,
) -> dict:
    params = urlencode({
        "title": "age4",
        "matchIDs": json.dumps([int(game_id)], separators=(",", ":")),
    })
    req = Request(f"{replay_files_url}?{params}", headers={"User-Agent": user_agent})
    with urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode("utf-8"))


def fetch_url_bytes(url: str, user_agent: str = USER_AGENT, timeout: float = 60) -> bytes:
    req = Request(url, headers={"User-Agent": user_agent})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def replay_summary_items(payload: dict, game_id: int) -> list[dict[str, int | str]]:
    items: list[dict[str, int | str]] = []
    for item in payload.get("replayFiles", []):
        try:
            datatype = int(item.get("datatype"))
            size = int(item.get("size", -1))
            profile_id = int(item["profile_id"])
            match_id = int(item.get("matchhistory_id", game_id))
        except (TypeError, ValueError, KeyError):
            continue
        url = item.get("url")
        if datatype != 1 or size <= 0 or not url or match_id != int(game_id):
            continue
        items.append({
            "profile_id": profile_id,
            "matchhistory_id": match_id,
            "url": str(url),
            "size": size,
        })
    return items


def _call_replay_files_fetcher(fetcher, game_id: int, user_agent: str) -> dict:
    try:
        return fetcher(game_id, user_agent=user_agent)
    except TypeError:
        return fetcher(game_id)


def _call_url_fetcher(fetcher, url: str, user_agent: str) -> bytes:
    try:
        return fetcher(url, user_agent=user_agent)
    except TypeError:
        return fetcher(url)


def _utc_now() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _selected_headers(headers) -> dict[str, str]:
    if not headers:
        return {}
    selected: dict[str, str] = {}
    for name in RATE_LIMIT_HEADERS:
        value = headers.get(name)
        if value is not None:
            selected[name] = str(value)
    return selected


def _append_jsonl(path: Path, event: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(event, sort_keys=True) + "\n")


def _record_download(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    profile_id: int | None,
    raw_path: Path | None,
    status: str,
    sample_group: str,
    size_bytes: int | None = None,
    sha256: str | None = None,
    source: str = "official_replay_endpoint",
    error: str | None = None,
) -> None:
    existing = conn.execute(
        "SELECT attempt_count FROM replay_downloads WHERE game_id = ?",
        [game_id],
    ).fetchone()
    attempt_count = (int(existing[0]) if existing and existing[0] is not None else 0) + 1
    conn.execute("DELETE FROM replay_downloads WHERE game_id = ?", [game_id])
    conn.execute(
        """
        INSERT INTO replay_downloads
            (game_id, profile_id_used, raw_path, download_date, downloaded_at, status,
             size_bytes, sha256, source, sample_group, attempt_count, last_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            game_id,
            profile_id,
            str(raw_path) if raw_path else None,
            date.today(),
            datetime.utcnow(),
            status,
            size_bytes,
            sha256,
            source,
            sample_group,
            attempt_count,
            error,
        ],
    )


def _record_summary_download(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    profile_id: int,
    summary_path: Path | None,
    status: str,
    size_bytes: int | None = None,
    sha256: str | None = None,
    source: str = "worldsedgelink_replay_files",
    error: str | None = None,
) -> None:
    existing = conn.execute(
        """
        SELECT attempt_count
        FROM replay_summary_downloads
        WHERE game_id = ? AND profile_id = ?
        """,
        [game_id, profile_id],
    ).fetchone()
    attempt_count = (int(existing[0]) if existing and existing[0] is not None else 0) + 1
    conn.execute(
        "DELETE FROM replay_summary_downloads WHERE game_id = ? AND profile_id = ?",
        [game_id, profile_id],
    )
    conn.execute(
        """
        INSERT INTO replay_summary_downloads
            (game_id, profile_id, summary_path, downloaded_at, status, size_bytes,
             sha256, source, attempt_count, last_error)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            game_id,
            profile_id,
            str(summary_path) if summary_path else None,
            datetime.utcnow(),
            status,
            size_bytes,
            sha256,
            source,
            attempt_count,
            error,
        ],
    )


def _download_summary_files_only(
    game_id: int,
    summary_root: Path = SUMMARY_REPLAY_DIR,
    user_agent: str = USER_AGENT,
    replay_files_fetcher=fetch_replay_files_payload,
    url_fetcher=fetch_url_bytes,
    skip_profile_ids: set[int] | None = None,
    raise_retryable_errors: bool = False,
) -> tuple[list[dict[str, object]], list[dict[str, object]], int]:
    payload = _call_replay_files_fetcher(replay_files_fetcher, game_id, user_agent)
    items = replay_summary_items(payload, game_id)
    downloaded: list[dict[str, object]] = []
    failed: list[dict[str, object]] = []
    skipped = 0
    out_dir = _today_summary_dir(summary_root)
    skip_profile_ids = set(skip_profile_ids or set())

    for item in items:
        profile_id = int(item["profile_id"])
        if profile_id in skip_profile_ids:
            skipped += 1
            continue
        final_path = out_dir / f"M_{game_id}_profile_{profile_id}_summary.gz"
        try:
            if final_path.exists() and final_path.stat().st_size > 0:
                data = final_path.read_bytes()
                downloaded.append({
                    "profile_id": profile_id,
                    "path": str(final_path),
                    "size_bytes": len(data),
                    "sha256": hashlib.sha256(data).hexdigest(),
                })
                skipped += 1
                continue

            data = _call_url_fetcher(url_fetcher, str(item["url"]), user_agent)
            if len(data) == 0:
                raise ValueError("empty response body")
            if not data.startswith(b"\x1f\x8b"):
                raise ValueError(f"not a gzip file (starts with {data[:4].hex()})")
            tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
            tmp_path.write_bytes(data)
            tmp_path.replace(final_path)
            downloaded.append({
                "profile_id": profile_id,
                "path": str(final_path),
                "size_bytes": len(data),
                "sha256": hashlib.sha256(data).hexdigest(),
            })
        except HTTPError as exc:
            if raise_retryable_errors and (exc.code in (403, 429) or 500 <= exc.code <= 599):
                raise
            failed.append({"profile_id": profile_id, "error": f"http_{exc.code}"})
        except (URLError, TimeoutError) as exc:
            if raise_retryable_errors:
                raise
            failed.append({"profile_id": profile_id, "error": str(exc)[:200]})
        except (ValueError, OSError) as exc:
            failed.append({"profile_id": profile_id, "error": str(exc)[:200]})

    return downloaded, failed, skipped


def download_summary_files(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    summary_root: Path = SUMMARY_REPLAY_DIR,
    user_agent: str = USER_AGENT,
    replay_files_fetcher=fetch_replay_files_payload,
    url_fetcher=fetch_url_bytes,
    source: str = "worldsedgelink_replay_files",
    skip_existing: bool = False,
    raise_retryable_errors: bool = False,
) -> dict[str, object]:
    skip_profile_ids: set[int] = set()
    if skip_existing:
        skip_profile_ids = {
            int(row[0])
            for row in conn.execute(
                """
                SELECT profile_id
                FROM replay_summary_downloads
                WHERE game_id = ? AND status = 'downloaded'
                """,
                [game_id],
            ).fetchall()
        }
    try:
        downloaded, failed, skipped = _download_summary_files_only(
            game_id,
            summary_root=summary_root,
            user_agent=user_agent,
            replay_files_fetcher=replay_files_fetcher,
            url_fetcher=url_fetcher,
            skip_profile_ids=skip_profile_ids,
            raise_retryable_errors=raise_retryable_errors,
        )
    except HTTPError as exc:
        if raise_retryable_errors:
            raise
        return {"status": "failed", "downloaded": 0, "failed": 1, "skipped": 0, "error": f"http_{exc.code}"}
    except (URLError, TimeoutError) as exc:
        if raise_retryable_errors:
            raise
        return {"status": "failed", "downloaded": 0, "failed": 1, "skipped": 0, "error": str(exc)[:200]}
    except (ValueError, OSError, json.JSONDecodeError) as exc:
        return {"status": "failed", "downloaded": 0, "failed": 1, "skipped": 0, "error": str(exc)[:200]}

    for result in downloaded:
        _record_summary_download(
            conn,
            game_id,
            int(result["profile_id"]),
            Path(str(result["path"])),
            "downloaded",
            size_bytes=int(result["size_bytes"]),
            sha256=str(result["sha256"]),
            source=source,
        )
    for result in failed:
        _record_summary_download(
            conn,
            game_id,
            int(result["profile_id"]),
            None,
            "failed",
            source=source,
            error=str(result["error"]),
        )

    status = "downloaded" if downloaded else "missing" if not failed else "failed"
    return {
        "status": status,
        "downloaded": len(downloaded),
        "failed": len(failed),
        "skipped": skipped,
    }


def download_one(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    sample_group: str,
    raw_root: Path = RAW_REPLAY_DIR,
    fetcher=fetch_replay_bytes,
    harvest_summaries: bool = False,
    summary_root: Path = SUMMARY_REPLAY_DIR,
    user_agent: str = USER_AGENT,
    replay_files_fetcher=fetch_replay_files_payload,
    summary_url_fetcher=fetch_url_bytes,
) -> str:
    existing = conn.execute(
        "SELECT status FROM replay_downloads WHERE game_id = ?",
        [game_id],
    ).fetchone()
    if existing and existing[0] == "downloaded":
        return "skipped"

    profile_id = choose_profile_id(conn, game_id)
    if profile_id is None:
        _record_download(conn, game_id, None, None, "failed", sample_group, error="no_participant")
        return "failed"

    final_path = _today_raw_dir(raw_root) / f"AgeIV_Replay_{game_id}.gz"
    if final_path.exists() and final_path.stat().st_size > 0:
        data = final_path.read_bytes()
        _record_download(
            conn,
            game_id,
            profile_id,
            final_path,
            "downloaded",
            sample_group,
            size_bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        )
        if harvest_summaries:
            download_summary_files(
                conn,
                game_id,
                summary_root=summary_root,
                user_agent=user_agent,
                replay_files_fetcher=replay_files_fetcher,
                url_fetcher=summary_url_fetcher,
            )
        return "downloaded"

    try:
        data = fetcher(game_id, profile_id)
        if len(data) == 0:
            raise ValueError("empty response")
        if not data.startswith(b"\x1f\x8b"):
            raise ValueError("response is not gzip data")
        tmp_path = final_path.with_suffix(final_path.suffix + ".tmp")
        tmp_path.write_bytes(data)
        tmp_path.replace(final_path)
        _record_download(
            conn,
            game_id,
            profile_id,
            final_path,
            "downloaded",
            sample_group,
            size_bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
        )
        if harvest_summaries:
            download_summary_files(
                conn,
                game_id,
                summary_root=summary_root,
                user_agent=user_agent,
                replay_files_fetcher=replay_files_fetcher,
                url_fetcher=summary_url_fetcher,
            )
        return "downloaded"
    except HTTPError as exc:
        _record_download(conn, game_id, profile_id, None, "failed", sample_group, error=f"http_{exc.code}")
        if exc.code == 429:
            raise RetryableDownloadError("http_429", pause_seconds=15 * 60) from exc
        if exc.code == 403:
            raise RetryableDownloadError("http_403", pause_seconds=60 * 60) from exc
        if 500 <= exc.code <= 599:
            raise RetryableDownloadError(f"http_{exc.code}", pause_seconds=5 * 60) from exc
        return "failed"
    except (URLError, TimeoutError, ValueError, OSError) as exc:
        _record_download(conn, game_id, profile_id, None, "failed", sample_group, error=str(exc))
        return "failed"


def download_group(
    conn: duckdb.DuckDBPyConnection,
    sample_group: str,
    limit: int,
    sleep_min: float,
    sleep_max: float,
    raw_root: Path = RAW_REPLAY_DIR,
    fetcher=fetch_replay_bytes,
    retry_pause_seconds: float | None = None,
    harvest_summaries: bool = True,
    summary_root: Path = SUMMARY_REPLAY_DIR,
    user_agent: str = USER_AGENT,
    replay_files_fetcher=fetch_replay_files_payload,
    summary_url_fetcher=fetch_url_bytes,
) -> dict[str, int]:
    counts = {"downloaded": 0, "failed": 0, "skipped": 0}
    for idx, game_id in enumerate(game_ids_for_labels(conn, sample_group, limit)):
        print(f"{datetime.utcnow().isoformat()}Z attempt {idx + 1}/{limit} game_id={game_id}", flush=True)
        try:
            status = download_one(
                conn,
                game_id,
                sample_group,
                raw_root=raw_root,
                fetcher=fetcher,
                harvest_summaries=harvest_summaries,
                summary_root=summary_root,
                user_agent=user_agent,
                replay_files_fetcher=replay_files_fetcher,
                summary_url_fetcher=summary_url_fetcher,
            )
        except RetryableDownloadError as exc:
            status = "failed"
            pause = retry_pause_seconds if retry_pause_seconds is not None else exc.pause_seconds
            print(
                f"{datetime.utcnow().isoformat()}Z retryable_error={exc.status} "
                f"game_id={game_id} pause_seconds={pause}",
                flush=True,
            )
            counts[status] = counts.get(status, 0) + 1
            time.sleep(pause)
            continue
        counts[status] = counts.get(status, 0) + 1
        print(f"{datetime.utcnow().isoformat()}Z result={status} game_id={game_id} counts={counts}", flush=True)
        if idx < limit - 1:
            pause = random.uniform(sleep_min, sleep_max)
            print(f"{datetime.utcnow().isoformat()}Z sleeping {pause:.1f}s", flush=True)
            time.sleep(pause)
    print(f"{datetime.utcnow().isoformat()}Z final_counts={counts}", flush=True)
    return counts


def _raw_replay_game_ids(raw_root: Path = RAW_REPLAY_DIR) -> set[int]:
    if not raw_root.exists():
        return set()
    game_ids: set[int] = set()
    for path in raw_root.rglob("*.gz"):
        match = REPLAY_FILENAME_RE.match(path.name)
        if match and path.stat().st_size > 0:
            game_ids.add(int(match.group(1)))
    return game_ids


def summary_backfill_game_ids(
    conn: duckdb.DuckDBPyConnection,
    raw_root: Path = RAW_REPLAY_DIR,
    resume: bool = True,
) -> list[int]:
    db_ids = {
        int(row[0])
        for row in conn.execute(
            "SELECT game_id FROM replay_downloads WHERE status = 'downloaded'"
        ).fetchall()
    }
    ids = sorted(db_ids | _raw_replay_game_ids(raw_root))
    if resume:
        attempted_ids = {
            int(row[0])
            for row in conn.execute(
                """
                SELECT game_id
                FROM replay_summary_backfill_log
                WHERE status IN ('downloaded', 'missing', 'partial', 'failed')
                """
            ).fetchall()
        }
        ids = [game_id for game_id in ids if game_id not in attempted_ids]
    return ids


def _record_summary_backfill_start(conn: duckdb.DuckDBPyConnection, game_id: int) -> None:
    existing = conn.execute(
        "SELECT attempt_count FROM replay_summary_backfill_log WHERE game_id = ?",
        [game_id],
    ).fetchone()
    attempt_count = (int(existing[0]) if existing and existing[0] is not None else 0) + 1
    conn.execute("DELETE FROM replay_summary_backfill_log WHERE game_id = ?", [game_id])
    conn.execute(
        """
        INSERT INTO replay_summary_backfill_log
            (game_id, status, started_at, finished_at, summary_downloaded,
             summary_failed, summary_skipped, attempt_count, last_error)
        VALUES (?, 'running', ?, NULL, 0, 0, 0, ?, NULL)
        """,
        [game_id, datetime.utcnow(), attempt_count],
    )


def _record_summary_backfill_result(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    result: dict[str, object],
) -> None:
    downloaded = int(result.get("downloaded", 0))
    failed = int(result.get("failed", 0))
    skipped = int(result.get("skipped", 0))
    if failed and downloaded:
        status = "partial"
    elif failed:
        status = "failed"
    elif downloaded:
        status = "downloaded"
    else:
        status = "missing"
    conn.execute(
        """
        UPDATE replay_summary_backfill_log
        SET status = ?, finished_at = ?, summary_downloaded = ?,
            summary_failed = ?, summary_skipped = ?, last_error = ?
        WHERE game_id = ?
        """,
        [
            status,
            datetime.utcnow(),
            downloaded,
            failed,
            skipped,
            result.get("error"),
            game_id,
        ],
    )


def backfill_summary_files(
    conn: duckdb.DuckDBPyConnection,
    sleep_min: float = 15.0,
    sleep_max: float = 30.0,
    raw_root: Path = RAW_REPLAY_DIR,
    summary_root: Path = SUMMARY_REPLAY_DIR,
    user_agent: str = USER_AGENT,
    replay_files_fetcher=fetch_replay_files_payload,
    summary_url_fetcher=fetch_url_bytes,
) -> dict[str, int]:
    counts = {"games": 0, "summary_downloaded": 0, "summary_failed": 0, "summary_skipped": 0}
    game_ids = summary_backfill_game_ids(conn, raw_root=raw_root)
    total = len(game_ids)
    for idx, game_id in enumerate(game_ids):
        print(f"{datetime.utcnow().isoformat()}Z summary_backfill {idx + 1}/{total} game_id={game_id}", flush=True)
        _record_summary_backfill_start(conn, game_id)
        result = download_summary_files(
            conn,
            game_id,
            summary_root=summary_root,
            user_agent=user_agent,
            replay_files_fetcher=replay_files_fetcher,
            url_fetcher=summary_url_fetcher,
            source="summary_backfill",
            skip_existing=True,
        )
        _record_summary_backfill_result(conn, game_id, result)
        counts["games"] += 1
        counts["summary_downloaded"] += int(result.get("downloaded", 0))
        counts["summary_failed"] += int(result.get("failed", 0))
        counts["summary_skipped"] += int(result.get("skipped", 0))
        print(f"{datetime.utcnow().isoformat()}Z summary_result={result} counts={counts}", flush=True)
        if idx < total - 1:
            pause = random.uniform(sleep_min, sleep_max)
            print(f"{datetime.utcnow().isoformat()}Z sleeping {pause:.1f}s", flush=True)
            time.sleep(pause)
    print(f"{datetime.utcnow().isoformat()}Z final_summary_counts={counts}", flush=True)
    return counts


def _download_summary_files_for_sidecar(
    game_id: int,
    summary_root: Path,
    user_agent: str,
    replay_files_fetcher=fetch_replay_files_payload,
    url_fetcher=fetch_url_bytes,
) -> dict[str, object]:
    try:
        downloaded, failed, skipped = _download_summary_files_only(
            game_id,
            summary_root=summary_root,
            user_agent=user_agent,
            replay_files_fetcher=replay_files_fetcher,
            url_fetcher=url_fetcher,
        )
    except HTTPError as exc:
        return {
            "summary_status": "failed",
            "summary_downloaded": 0,
            "summary_failed": 1,
            "summary_skipped": 0,
            "summary_error": f"http_{exc.code}",
            "summaries": [],
        }
    except (URLError, TimeoutError, ValueError, OSError, json.JSONDecodeError) as exc:
        return {
            "summary_status": "failed",
            "summary_downloaded": 0,
            "summary_failed": 1,
            "summary_skipped": 0,
            "summary_error": str(exc)[:200],
            "summaries": [],
        }

    if downloaded:
        status = "downloaded"
    elif failed:
        status = "failed"
    else:
        status = "missing"
    return {
        "summary_status": status,
        "summary_downloaded": len(downloaded),
        "summary_failed": len(failed),
        "summary_skipped": skipped,
        "summaries": downloaded,
        "summary_failures": failed,
    }


def download_job_list(
    job_path: Path,
    raw_root: Path = RAW_REPLAY_DIR,
    summary_root: Path = SUMMARY_REPLAY_DIR,
    sleep_min: float = 40.0,
    sleep_max: float = 70.0,
    user_agent: str = USER_AGENT,
    fetcher=fetch_replay_bytes,
    harvest_summaries: bool = True,
    replay_files_fetcher=fetch_replay_files_payload,
    summary_url_fetcher=fetch_url_bytes,
    on_event=None,
    stop_event: threading.Event | None = None,
    on_429: str = "sleep",          # "sleep" or "stop"
    on_429_minutes: float = 15.0,
    pre_done_ids: set[int] | None = None,
) -> dict[str, int]:
    """Download replays from a portable job-list JSON file.

    Progress is persisted to a sidecar <job_id>.progress.json in the same
    directory as job_path so sessions can be resumed after interruption.
    pre_done_ids: game_ids already known to be downloaded (e.g. from the DB),
                  merged with the sidecar so they are skipped without an API call.
    """
    job = json.loads(job_path.read_text())
    job_id = job.get("job_id", job_path.stem)
    sidecar_path = job_path.parent / f"{job_id}.progress.json"
    event_log_path = job_path.parent / f"{job_id}.events.jsonl"

    if sidecar_path.exists():
        progress_data: dict = json.loads(sidecar_path.read_text())
    else:
        progress_data = {"job_id": job_id, "results": {}}
    progress_data["event_log"] = event_log_path.name

    done_ids = {
        int(k)
        for k, v in progress_data.get("results", {}).items()
        if v.get("status") == "downloaded"
    }
    summary_done_ids = {
        int(k)
        for k, v in progress_data.get("results", {}).items()
        if v.get("status") == "downloaded"
        and v.get("summary_status") in ("downloaded", "missing")
    }
    if pre_done_ids:
        done_ids.update(pre_done_ids)

    counts: dict[str, int] = {"downloaded": 0, "failed": 0, "skipped": 0}
    games = job.get("games", [])

    def _log(evt: dict) -> None:
        _append_jsonl(event_log_path, {"at": _utc_now(), "job_id": job_id, **evt})

    def _save():
        progress_data["updated_at"] = datetime.utcnow().isoformat()
        sidecar_path.write_text(json.dumps(progress_data, indent=2))

    def _emit(evt: dict) -> None:
        if on_event:
            on_event({**evt, "counts": dict(counts), "total": len(games)})

    def _sleep(seconds: float) -> bool:
        """Sleep for `seconds`; return True if stop was requested early."""
        if stop_event:
            return stop_event.wait(timeout=seconds)
        time.sleep(seconds)
        return False

    _log({
        "event": "session_start",
        "total_games": len(games),
        "sleep_min": sleep_min,
        "sleep_max": sleep_max,
        "on_429": on_429,
        "on_429_minutes": on_429_minutes,
    })

    for idx, entry in enumerate(games):
        if stop_event and stop_event.is_set():
            _log({"event": "paused", "index": idx, "reason": "stop_requested"})
            _emit({"type": "paused", "index": idx})
            break

        game_id = int(entry["game_id"])
        profile_id = int(entry["profile_id"])

        if game_id in done_ids:
            existing_progress = progress_data.get("results", {}).get(str(game_id), {})
            summary_result: dict[str, object] = {
                "summary_status": "not_attempted",
                "summary_downloaded": 0,
                "summary_failed": 0,
                "summary_skipped": 0,
                "summaries": [],
            }
            if harvest_summaries and game_id not in summary_done_ids:
                summary_result = _download_summary_files_for_sidecar(
                    game_id,
                    summary_root=summary_root,
                    user_agent=user_agent,
                    replay_files_fetcher=replay_files_fetcher,
                    url_fetcher=summary_url_fetcher,
                )
                progress_data["results"][str(game_id)] = {
                    "status": existing_progress.get("status") or "skipped",
                    "path": existing_progress.get("path"),
                    "error": existing_progress.get("error") or "full_replay_already_downloaded",
                    "at": _utc_now(),
                    **summary_result,
                }
                _save()
                _log({
                    "event": "summary_result",
                    "index": idx,
                    "game_id": game_id,
                    "status": summary_result.get("summary_status"),
                    "downloaded": summary_result.get("summary_downloaded", 0),
                    "failed": summary_result.get("summary_failed", 0),
                    "skipped": summary_result.get("summary_skipped", 0),
                    "reason": "full_replay_already_downloaded",
                    "error": summary_result.get("summary_error"),
                })
            counts["skipped"] += 1
            _log({
                "event": "game_skipped",
                "index": idx,
                "game_id": game_id,
                "profile_id": profile_id,
                "reason": "already_downloaded",
                "summary_status": summary_result.get("summary_status"),
                "summary_downloaded": summary_result.get("summary_downloaded", 0),
                "summary_failed": summary_result.get("summary_failed", 0),
            })
            _emit({
                "type": "log",
                "game_id": game_id,
                "status": "skipped",
                "index": idx,
                "summary_status": summary_result.get("summary_status"),
                "summary_downloaded": summary_result.get("summary_downloaded", 0),
                "summary_failed": summary_result.get("summary_failed", 0),
            })
            continue

        final_path = _today_raw_dir(raw_root) / f"AgeIV_Replay_{game_id}.gz"
        status = "failed"
        error: str | None = None
        path_str: str | None = None
        summary_result: dict[str, object] = {
            "summary_status": "not_attempted",
            "summary_downloaded": 0,
            "summary_failed": 0,
            "summary_skipped": 0,
            "summaries": [],
        }

        for attempt in range(2):
            _log({
                "event": "attempt_start",
                "index": idx,
                "attempt": attempt + 1,
                "game_id": game_id,
                "profile_id": profile_id,
            })
            try:
                if final_path.exists() and final_path.stat().st_size > 0:
                    status = "downloaded"
                    path_str = str(final_path)
                    _log({
                        "event": "local_file_used",
                        "index": idx,
                        "attempt": attempt + 1,
                        "game_id": game_id,
                        "profile_id": profile_id,
                        "path": path_str,
                        "size_bytes": final_path.stat().st_size,
                    })
                    break

                data = fetcher(game_id, profile_id, user_agent=user_agent)
                if len(data) == 0:
                    raise ValueError("empty response body")
                if not data.startswith(b"\x1f\x8b"):
                    raise ValueError(f"not a gzip file (starts with {data[:4].hex()})")
                tmp = final_path.with_suffix(final_path.suffix + ".tmp")
                tmp.write_bytes(data)
                tmp.replace(final_path)
                status = "downloaded"
                path_str = str(final_path)
                _log({
                    "event": "attempt_success",
                    "index": idx,
                    "attempt": attempt + 1,
                    "game_id": game_id,
                    "profile_id": profile_id,
                    "size_bytes": len(data),
                })
                break

            except HTTPError as exc:
                error = f"http_{exc.code}"
                headers = _selected_headers(exc.headers)
                http_event = {
                    "event": "http_error",
                    "index": idx,
                    "attempt": attempt + 1,
                    "game_id": game_id,
                    "profile_id": profile_id,
                    "http_status": exc.code,
                    "reason": exc.reason,
                    "headers": headers,
                }
                if exc.code == 429:
                    if on_429 == "stop":
                        _log({**http_event, "action": "stop"})
                        _emit({"type": "log", "game_id": game_id, "status": "rate_limited",
                               "message": "429 — stopping as configured", "index": idx,
                               "headers": headers})
                        if stop_event:
                            stop_event.set()
                        break
                    elif attempt == 0:
                        mins = on_429_minutes
                        _log({**http_event, "action": "sleep_then_retry", "sleep_seconds": mins * 60})
                        _emit({"type": "log", "game_id": game_id, "status": "rate_limited",
                               "message": f"429 — sleeping {mins:.0f} min then retrying", "index": idx,
                               "headers": headers})
                        if _sleep(mins * 60):
                            _log({
                                "event": "sleep_interrupted",
                                "index": idx,
                                "game_id": game_id,
                                "profile_id": profile_id,
                                "reason": "stop_requested",
                            })
                            break
                        continue
                    else:
                        _log({**http_event, "action": "final_failed"})
                elif exc.code == 404:
                    error = "http_404_replay_unavailable"
                    _log({**http_event, "action": "skip_unavailable"})
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": "404 — replay not available (may have expired)", "index": idx})
                elif exc.code == 403:
                    error = "http_403_access_denied"
                    _log({**http_event, "action": "skip_access_denied"})
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": "403 — access denied, skipping", "index": idx})
                elif 500 <= exc.code <= 599 and attempt == 0:
                    _log({**http_event, "action": "sleep_then_retry", "sleep_seconds": 120})
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": f"http_{exc.code} — server error, retrying in 2 min", "index": idx})
                    if _sleep(120):
                        _log({
                            "event": "sleep_interrupted",
                            "index": idx,
                            "game_id": game_id,
                            "profile_id": profile_id,
                            "reason": "stop_requested",
                        })
                        break
                    continue
                else:
                    _log({**http_event, "action": "final_failed"})
                break

            except TimeoutError:
                error = "timeout"
                _log({
                    "event": "timeout",
                    "index": idx,
                    "attempt": attempt + 1,
                    "game_id": game_id,
                    "profile_id": profile_id,
                    "action": "sleep_then_retry" if attempt == 0 else "final_failed",
                })
                if attempt == 0:
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": "timeout — retrying in 30s", "index": idx})
                    if _sleep(30):
                        _log({
                            "event": "sleep_interrupted",
                            "index": idx,
                            "game_id": game_id,
                            "profile_id": profile_id,
                            "reason": "stop_requested",
                        })
                        break
                    continue
                break

            except URLError as exc:
                error = f"network_error: {str(exc.reason)[:80]}"
                _log({
                    "event": "network_error",
                    "index": idx,
                    "attempt": attempt + 1,
                    "game_id": game_id,
                    "profile_id": profile_id,
                    "reason": str(exc.reason)[:200],
                    "action": "sleep_then_retry" if attempt == 0 else "final_failed",
                })
                if attempt == 0:
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": f"network error — retrying in 30s", "index": idx})
                    if _sleep(30):
                        _log({
                            "event": "sleep_interrupted",
                            "index": idx,
                            "game_id": game_id,
                            "profile_id": profile_id,
                            "reason": "stop_requested",
                        })
                        break
                    continue
                break

            except (ValueError, OSError) as exc:
                error = str(exc)[:200]
                _log({
                    "event": "download_error",
                    "index": idx,
                    "attempt": attempt + 1,
                    "game_id": game_id,
                    "profile_id": profile_id,
                    "error": error,
                    "action": "final_failed",
                })
                break

        if status == "downloaded" and harvest_summaries:
            summary_result = _download_summary_files_for_sidecar(
                game_id,
                summary_root=summary_root,
                user_agent=user_agent,
                replay_files_fetcher=replay_files_fetcher,
                url_fetcher=summary_url_fetcher,
            )
            _log({
                "event": "summary_result",
                "index": idx,
                "game_id": game_id,
                "status": summary_result.get("summary_status"),
                "downloaded": summary_result.get("summary_downloaded", 0),
                "failed": summary_result.get("summary_failed", 0),
                "skipped": summary_result.get("summary_skipped", 0),
                "error": summary_result.get("summary_error"),
            })

        finished_at = _utc_now()
        progress_data["results"][str(game_id)] = {
            "status": status,
            "path": path_str,
            "error": error,
            "at": finished_at,
            **summary_result,
        }
        _save()
        counts[status] = counts.get(status, 0) + 1
        _log({
            "event": "game_result",
            "index": idx,
            "game_id": game_id,
            "profile_id": profile_id,
            "status": status,
            "error": error,
            "path": path_str,
            "summary_status": summary_result.get("summary_status"),
            "summary_downloaded": summary_result.get("summary_downloaded", 0),
            "summary_failed": summary_result.get("summary_failed", 0),
            "counts": dict(counts),
        })
        _emit({"type": "log", "game_id": game_id, "status": status, "error": error,
               "path": path_str, "index": idx,
               "summary_status": summary_result.get("summary_status"),
               "summary_downloaded": summary_result.get("summary_downloaded", 0),
               "summary_failed": summary_result.get("summary_failed", 0)})

        if stop_event and stop_event.is_set():
            _log({"event": "paused", "index": idx, "reason": "stop_requested"})
            _emit({"type": "paused", "index": idx})
            break

        if idx < len(games) - 1:
            pause = random.uniform(sleep_min, sleep_max)
            _log({
                "event": "sleep_between_games",
                "index": idx,
                "game_id": game_id,
                "seconds": round(pause, 1),
            })
            _emit({"type": "sleep", "seconds": round(pause, 1), "index": idx})
            _sleep(pause)

    _log({"event": "session_done", "counts": counts, "total_games": len(games)})
    _emit({"type": "done", "counts": counts, "total": len(games)})
    return counts
