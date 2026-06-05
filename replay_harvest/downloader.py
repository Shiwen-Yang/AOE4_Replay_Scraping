from __future__ import annotations

from datetime import date, datetime
import hashlib
import json
import random
import threading
import time
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import duckdb

from .candidates import game_ids_for_labels
from .config import RAW_REPLAY_DIR, REPLAY_DOWNLOAD_URL, USER_AGENT


class RetryableDownloadError(RuntimeError):
    def __init__(self, status: str, pause_seconds: float):
        super().__init__(status)
        self.status = status
        self.pause_seconds = pause_seconds


def _today_raw_dir(raw_root: Path = RAW_REPLAY_DIR) -> Path:
    path = raw_root / date.today().isoformat()
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


def download_one(
    conn: duckdb.DuckDBPyConnection,
    game_id: int,
    sample_group: str,
    raw_root: Path = RAW_REPLAY_DIR,
    fetcher=fetch_replay_bytes,
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
) -> dict[str, int]:
    counts = {"downloaded": 0, "failed": 0, "skipped": 0}
    for idx, game_id in enumerate(game_ids_for_labels(conn, sample_group, limit)):
        print(f"{datetime.utcnow().isoformat()}Z attempt {idx + 1}/{limit} game_id={game_id}", flush=True)
        try:
            status = download_one(conn, game_id, sample_group, raw_root=raw_root, fetcher=fetcher)
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


def download_job_list(
    job_path: Path,
    raw_root: Path = RAW_REPLAY_DIR,
    sleep_min: float = 40.0,
    sleep_max: float = 70.0,
    user_agent: str = USER_AGENT,
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

    if sidecar_path.exists():
        progress_data: dict = json.loads(sidecar_path.read_text())
    else:
        progress_data = {"job_id": job_id, "results": {}}

    done_ids = {
        int(k)
        for k, v in progress_data.get("results", {}).items()
        if v.get("status") == "downloaded"
    }
    if pre_done_ids:
        done_ids.update(pre_done_ids)

    counts: dict[str, int] = {"downloaded": 0, "failed": 0, "skipped": 0}
    games = job.get("games", [])

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

    for idx, entry in enumerate(games):
        if stop_event and stop_event.is_set():
            _emit({"type": "paused", "index": idx})
            break

        game_id = int(entry["game_id"])
        profile_id = int(entry["profile_id"])

        if game_id in done_ids:
            counts["skipped"] += 1
            _emit({"type": "log", "game_id": game_id, "status": "skipped", "index": idx})
            continue

        final_path = _today_raw_dir(raw_root) / f"AgeIV_Replay_{game_id}.gz"
        status = "failed"
        error: str | None = None
        path_str: str | None = None

        for attempt in range(2):
            try:
                if final_path.exists() and final_path.stat().st_size > 0:
                    status = "downloaded"
                    path_str = str(final_path)
                    break

                data = fetch_replay_bytes(game_id, profile_id, user_agent=user_agent)
                if len(data) == 0:
                    raise ValueError("empty response body")
                if not data.startswith(b"\x1f\x8b"):
                    raise ValueError(f"not a gzip file (starts with {data[:4].hex()})")
                tmp = final_path.with_suffix(final_path.suffix + ".tmp")
                tmp.write_bytes(data)
                tmp.replace(final_path)
                status = "downloaded"
                path_str = str(final_path)
                break

            except HTTPError as exc:
                error = f"http_{exc.code}"
                if exc.code == 429:
                    if on_429 == "stop":
                        _emit({"type": "log", "game_id": game_id, "status": "rate_limited",
                               "message": "429 — stopping as configured", "index": idx})
                        if stop_event:
                            stop_event.set()
                        break
                    elif attempt == 0:
                        mins = on_429_minutes
                        _emit({"type": "log", "game_id": game_id, "status": "rate_limited",
                               "message": f"429 — sleeping {mins:.0f} min then retrying", "index": idx})
                        if _sleep(mins * 60):
                            break
                        continue
                elif exc.code == 404:
                    error = "http_404_replay_unavailable"
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": "404 — replay not available (may have expired)", "index": idx})
                elif exc.code == 403:
                    error = "http_403_access_denied"
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": "403 — access denied, skipping", "index": idx})
                elif 500 <= exc.code <= 599 and attempt == 0:
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": f"http_{exc.code} — server error, retrying in 2 min", "index": idx})
                    if _sleep(120):
                        break
                    continue
                break

            except TimeoutError:
                error = "timeout"
                if attempt == 0:
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": "timeout — retrying in 30s", "index": idx})
                    if _sleep(30):
                        break
                    continue
                break

            except URLError as exc:
                error = f"network_error: {str(exc.reason)[:80]}"
                if attempt == 0:
                    _emit({"type": "log", "game_id": game_id, "status": "warn",
                           "message": f"network error — retrying in 30s", "index": idx})
                    if _sleep(30):
                        break
                    continue
                break

            except (ValueError, OSError) as exc:
                error = str(exc)[:200]
                break

        progress_data["results"][str(game_id)] = {
            "status": status,
            "path": path_str,
            "error": error,
            "at": datetime.utcnow().isoformat(),
        }
        _save()
        counts[status] = counts.get(status, 0) + 1
        _emit({"type": "log", "game_id": game_id, "status": status, "error": error,
               "path": path_str, "index": idx})

        if stop_event and stop_event.is_set():
            _emit({"type": "paused", "index": idx})
            break

        if idx < len(games) - 1:
            pause = random.uniform(sleep_min, sleep_max)
            _emit({"type": "sleep", "seconds": round(pause, 1), "index": idx})
            _sleep(pause)

    _emit({"type": "done", "counts": counts, "total": len(games)})
    return counts
