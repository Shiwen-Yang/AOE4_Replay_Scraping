from __future__ import annotations

import asyncio
import hashlib
import json
import queue
import random
import re
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Ensure replay_harvest is importable when running from any directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from replay_harvest.config import DB_PATH, RAW_REPLAY_DIR, REPORT_DIR, SAMPLE_GROUP_RECENT, SUMMARY_REPLAY_DIR
from replay_harvest.db import get_conn

from replay_harvest.downloader import (
    _record_summary_backfill_result,
    _record_summary_backfill_start,
    download_job_list,
    download_summary_files,
    summary_backfill_game_ids,
)
from replay_harvest.schema import init_schema

STATIC_DIR = Path(__file__).parent / "static"

app = FastAPI(title="AOE4 Replay Harvest")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


# ── Session state ──────────────────────────────────────────────────────────────

@dataclass
class DownloadSession:
    job_id: str | None = None
    sample_group: str = SAMPLE_GROUP_RECENT
    event_queue: queue.Queue = field(default_factory=queue.Queue)
    stop_event: threading.Event = field(default_factory=threading.Event)
    thread: threading.Thread | None = None
    progress: dict[str, int] = field(default_factory=lambda: {
        "downloaded": 0, "failed": 0, "skipped": 0, "total": 0,
    })


SESSION = DownloadSession()

# coordinator in-memory jobs: job_id → job dict (populated by /api/generate-jobs)
PENDING_JOBS: dict[str, dict] = {}

# discovery background task state
DISCOVERY: dict[str, Any] = {"status": "idle", "result": None, "error": None}
SUMMARY_BACKFILL: dict[str, Any] = {
    "status": "idle",
    "counts": None,
    "error": None,
    "current_game_id": None,
    "index": 0,
    "total": 0,
}
TOP50_CACHE: dict[str, Any] = {"ids": set(), "fetched_at": None, "error": None}
TOP50_CACHE_TTL = timedelta(minutes=15)


def _current_top50_profile_ids() -> set[int]:
    fetched_at = TOP50_CACHE.get("fetched_at")
    if (
        fetched_at is not None
        and datetime.now(timezone.utc) - fetched_at < TOP50_CACHE_TTL
        and TOP50_CACHE.get("ids")
    ):
        return set(TOP50_CACHE["ids"])
    from replay_harvest.discovery import current_top50_profile_ids

    try:
        ids = current_top50_profile_ids()
        TOP50_CACHE.update({"ids": set(ids), "fetched_at": datetime.now(timezone.utc), "error": None})
        return set(ids)
    except Exception as exc:
        TOP50_CACHE["error"] = str(exc)
        return set(TOP50_CACHE.get("ids") or [])


# ── Pages ──────────────────────────────────────────────────────────────────────

@app.get("/")
def coordinator_page():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/friend")
def friend_page():
    return FileResponse(STATIC_DIR / "friend.html")


# ── Discovery (coordinator only) ───────────────────────────────────────────────

class DiscoverRequest(BaseModel):
    quota_grid: dict[str, dict[str, int]]
    top50_target: int = 0
    horizon_days: list[int] | None = None
    sleep_seconds: float = 1.0


@app.post("/api/discover/start")
def api_discover_start(req: DiscoverRequest):
    if DISCOVERY["status"] in ("running", "stopping"):
        raise HTTPException(409, "discovery already running")
    stop_event = threading.Event()
    DISCOVERY.update({"status": "running", "result": None, "error": None,
                       "phase": "starting", "phases_done": 0, "phases_total": None,
                       "stop_event": stop_event, "details": None})

    def run():
        from replay_harvest.discovery import discover_quota_games

        def on_status(details: dict[str, Any]) -> None:
            DISCOVERY.update({
                "phase": details.get("phase", "running"),
                "details": details,
            })

        conn = None
        try:
            top50_ids = _current_top50_profile_ids()
            conn = get_conn(DB_PATH)
            init_schema(conn)
            result = discover_quota_games(
                conn,
                quota_grid=req.quota_grid,
                top50_target=req.top50_target,
                top50_profile_ids=top50_ids,
                horizon_days=req.horizon_days,
                sleep_seconds=req.sleep_seconds,
                on_status=on_status,
                stop_event=stop_event,
            )
            status = "stopped" if result.get("stopped") else "done"
            DISCOVERY.update({"status": status, "result": result, "error": None,
                               "phase": "finished", "details": result})
        except Exception as exc:
            DISCOVERY.update({"status": "error", "result": None, "error": str(exc)})
        finally:
            if conn is not None:
                conn.close()
            DISCOVERY.pop("stop_event", None)

    threading.Thread(target=run, daemon=True).start()
    return {"status": "started"}


@app.get("/api/discover/status")
def api_discover_status():
    return {k: v for k, v in DISCOVERY.items() if k != "stop_event"}


@app.get("/api/quota-inventory")
def api_quota_inventory():
    from replay_harvest.discovery import quota_inventory

    top50_ids = _current_top50_profile_ids()
    conn = get_conn(DB_PATH)
    try:
        init_schema(conn)
        result = quota_inventory(conn, top50_profile_ids=top50_ids)
    finally:
        conn.close()
    result["top50_source"] = {
        "count": len(top50_ids),
        "error": TOP50_CACHE.get("error"),
        "fetched_at": TOP50_CACHE.get("fetched_at").isoformat()
        if TOP50_CACHE.get("fetched_at") else None,
    }
    return result


@app.post("/api/discover/stop")
def api_discover_stop():
    stop_event = DISCOVERY.get("stop_event")
    if DISCOVERY.get("status") != "running" or stop_event is None:
        return {"status": DISCOVERY.get("status", "idle")}
    stop_event.set()
    DISCOVERY.update({"status": "stopping"})
    return {"status": "stopping"}


@app.get("/api/assigned")
def api_assigned():
    from replay_harvest.discovery import get_assigned_games
    conn = get_conn(DB_PATH)
    try:
        init_schema(conn)
        result = get_assigned_games(conn)
    finally:
        conn.close()
    return result


class ResetAssignedRequest(BaseModel):
    game_ids: list[int]


@app.post("/api/reset-assigned")
def api_reset_assigned(req: ResetAssignedRequest):
    if not req.game_ids:
        return {"reset": 0}
    conn = get_conn(DB_PATH)
    placeholders = ",".join("?" * len(req.game_ids))
    conn.execute(
        f"DELETE FROM replay_downloads WHERE game_id IN ({placeholders}) AND status = 'assigned'",
        req.game_ids,
    )
    conn.close()
    return {"reset": len(req.game_ids)}


class MarkUnobtainableRequest(BaseModel):
    game_ids: list[int]
    reason: str = "manual_skip"
    detail: str | None = None


@app.post("/api/mark-unobtainable")
def api_mark_unobtainable(req: MarkUnobtainableRequest):
    if not req.game_ids:
        return {"marked": 0}
    reason = (req.reason or "manual_skip").strip()[:80]
    detail = (req.detail or "").strip()
    conn = get_conn(DB_PATH)
    now = datetime.now(timezone.utc)
    marked = 0
    try:
        init_schema(conn)
        for game_id in {int(gid) for gid in req.game_ids}:
            existing = conn.execute(
                "SELECT status FROM replay_downloads WHERE game_id = ? LIMIT 1",
                [game_id],
            ).fetchone()
            if existing and existing[0] == "downloaded":
                continue
            conn.execute("DELETE FROM replay_unobtainable_games WHERE game_id = ?", [game_id])
            conn.execute(
                """
                INSERT INTO replay_unobtainable_games
                    (game_id, marked_at, reason, detail, source)
                VALUES (?, ?, ?, ?, 'manual')
                """,
                [game_id, now, reason, detail[:400] or None],
            )
            marked += 1
    finally:
        conn.close()
    return {"marked": marked}


@app.get("/api/pending")
def api_pending():
    from replay_harvest.discovery import get_pending_games
    # If the session was paused and the thread is still wrapping up, wait for it
    # so the sidecar import finishes before we query pending games.
    if SESSION.thread and SESSION.thread.is_alive() and SESSION.stop_event.is_set():
        SESSION.thread.join(timeout=10)
    # If the thread already finished but we want to make sure the sidecar is
    # reflected (e.g. user calls Show Pending multiple times), re-import it.
    # INSERT OR IGNORE makes this idempotent.
    if SESSION.job_id and not (SESSION.thread and SESSION.thread.is_alive()):
        sidecar_path = REPORT_DIR / f"{SESSION.job_id}.progress.json"
        _import_sidecar(sidecar_path, SESSION.sample_group, "coordinator_session")
    conn = get_conn(DB_PATH)
    try:
        init_schema(conn)
        result = get_pending_games(conn)
    finally:
        conn.close()
    return result


# ── Job generation ─────────────────────────────────────────────────────────────

class GenerateJobsRequest(BaseModel):
    games: list[dict]
    splits: int = 2
    group: str = "recent_rm_1v1"


def _normalize_job_games(games: list[dict]) -> tuple[list[dict[str, int]], int]:
    normalized: list[dict[str, int]] = []
    skipped = 0
    for game in games:
        try:
            game_id = int(game["game_id"])
            profile_id = int(game["profile_id"])
        except (KeyError, TypeError, ValueError):
            skipped += 1
            continue
        normalized.append({"game_id": game_id, "profile_id": profile_id})
    return normalized, skipped


def _filter_unobtainable_games(games: list[dict[str, int]]) -> tuple[list[dict[str, int]], int]:
    if not games:
        return games, 0
    conn = get_conn(DB_PATH)
    try:
        init_schema(conn)
        game_ids = [game["game_id"] for game in games]
        placeholders = ",".join("?" * len(game_ids))
        excluded = {
            int(row[0])
            for row in conn.execute(
                f"SELECT game_id FROM replay_unobtainable_games WHERE game_id IN ({placeholders})",
                game_ids,
            ).fetchall()
        }
    finally:
        conn.close()
    if not excluded:
        return games, 0
    return [game for game in games if game["game_id"] not in excluded], len(excluded)


def _mark_games_assigned(game_ids: list[int], group: str) -> None:
    if not game_ids:
        return
    conn = get_conn(DB_PATH)
    now = datetime.now(timezone.utc)
    try:
        for game_id in game_ids:
            exists = conn.execute(
                "SELECT 1 FROM replay_downloads WHERE game_id = ? LIMIT 1",
                [game_id],
            ).fetchone()
            if exists:
                continue
            conn.execute(
                """
                INSERT INTO replay_downloads
                    (game_id, profile_id_used, raw_path, download_date, downloaded_at, status,
                     size_bytes, sha256, source, sample_group, attempt_count, last_error)
                VALUES (?, NULL, NULL, ?, ?, 'assigned', NULL, NULL, 'job_assignment', ?, 0, NULL)
                """,
                [game_id, now.date(), now, group],
            )
    finally:
        conn.close()


@app.post("/api/generate-jobs")
def api_generate_jobs(req: GenerateJobsRequest):
    games, skipped_invalid = _normalize_job_games(req.games)
    games, skipped_unobtainable = _filter_unobtainable_games(games)
    if not games:
        if skipped_unobtainable:
            raise HTTPException(400, "all selected games are marked unobtainable")
        raise HTTPException(400, "no games with valid game_id and profile_id provided")

    k = max(1, min(req.splits, len(games)))
    base = len(games) // k
    remainder = len(games) % k

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    jobs = []
    start = 0

    for i in range(k):
        size = base + (1 if i < remainder else 0)
        chunk = games[start:start + size]
        start += size

        job_id = f"job_{i + 1}_of_{k}_{ts}"
        job = {
            "version": "1",
            "job_id": job_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "group": req.group,
            "total_games": len(chunk),
            "games": chunk,
        }
        path = REPORT_DIR / f"{job_id}.json"
        path.write_text(json.dumps(job, indent=2))
        PENDING_JOBS[job_id] = job
        jobs.append({"job_id": job_id, "total_games": len(chunk)})

    # Mark all distributed game_ids as 'assigned' so they won't be included
    # in future pending queries or handed out in a second round of jobs.
    _mark_games_assigned([g["game_id"] for g in games], req.group)

    return {"jobs": jobs, "skipped_invalid": skipped_invalid, "skipped_unobtainable": skipped_unobtainable}


class SaveJobRequest(BaseModel):
    games: list[dict]
    group: str = "recent_rm_1v1"


@app.post("/api/save-job")
def api_save_job(req: SaveJobRequest):
    """Persist an in-memory job list to disk so it survives a server restart."""
    games, skipped_invalid = _normalize_job_games(req.games)
    games, skipped_unobtainable = _filter_unobtainable_games(games)
    if not games:
        if skipped_unobtainable:
            raise HTTPException(400, "all selected games are marked unobtainable")
        raise HTTPException(400, "no games with valid game_id and profile_id provided")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    job_id = f"coordinator_{ts}"
    job = {
        "version": "1",
        "job_id": job_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "group": req.group,
        "total_games": len(games),
        "games": games,
    }
    path = REPORT_DIR / f"{job_id}.json"
    path.write_text(json.dumps(job, indent=2))
    PENDING_JOBS[job_id] = job

    # Games were already marked 'assigned' when the original job was generated;
    # this is safe to call again and ensures any new ones are covered.
    _mark_games_assigned([g["game_id"] for g in games], req.group)

    return {
        "job_id": job_id,
        "total_games": len(games),
        "skipped_invalid": skipped_invalid,
        "skipped_unobtainable": skipped_unobtainable,
    }


@app.get("/api/jobs/{job_id}")
def api_download_job(job_id: str):
    path = REPORT_DIR / f"{job_id}.json"
    if not path.exists():
        raise HTTPException(404, "job not found")
    return FileResponse(path, filename=f"{job_id}.json", media_type="application/json")


@app.get("/api/sidecar/{job_id}")
def api_download_sidecar(job_id: str):
    path = REPORT_DIR / f"{job_id}.progress.json"
    if not path.exists():
        raise HTTPException(404, "sidecar not found — session may not have started yet")
    return FileResponse(path, filename=f"{job_id}.progress.json", media_type="application/json")


@app.get("/api/event-log/{job_id}")
def api_download_event_log(job_id: str):
    path = REPORT_DIR / f"{job_id}.events.jsonl"
    if not path.exists():
        raise HTTPException(404, "event log not found — session may not have started yet")
    return FileResponse(path, filename=f"{job_id}.events.jsonl", media_type="application/x-ndjson")


class ImportProgressRequest(BaseModel):
    job_id: str
    downloaded: list[int]
    failed: list[int]
    skipped: list[int] = []
    sample_group: str = SAMPLE_GROUP_RECENT
    results: dict[str, Any] | None = None


def _write_to_replay_downloads(
    downloaded: list[int],
    failed: list[int],
    sample_group: str,
    source: str,
) -> dict[str, int]:
    if not downloaded and not failed:
        return {"imported_downloaded": 0, "imported_failed": 0}
    conn = get_conn(DB_PATH)
    now = datetime.now(timezone.utc)
    init_schema(conn)
    # UPDATE existing rows (e.g. assigned → downloaded/failed).
    # Never downgrade an already-downloaded game to failed.
    if downloaded:
        conn.executemany(
            "UPDATE replay_downloads SET status='downloaded', downloaded_at=?, source=? WHERE game_id=?",
            [(now, source, gid) for gid in downloaded],
        )
    if failed:
        conn.executemany(
            "UPDATE replay_downloads SET status='failed', downloaded_at=?, source=?, last_error='imported_as_failed' WHERE game_id=? AND status != 'downloaded'",
            [(now, source, gid) for gid in failed],
        )
    # INSERT for any game_ids that had no row yet
    all_ids = downloaded + failed
    placeholders = ",".join("?" * len(all_ids))
    existing = {int(r[0]) for r in conn.execute(
        f"SELECT game_id FROM replay_downloads WHERE game_id IN ({placeholders})", all_ids
    ).fetchall()}
    new_downloaded = [(gid, now.date(), now, "downloaded", source, sample_group)
                      for gid in downloaded if gid not in existing]
    new_failed     = [(gid, now.date(), now, "failed",     source, sample_group)
                      for gid in failed     if gid not in existing]
    if new_downloaded or new_failed:
        conn.executemany(
            """
            INSERT INTO replay_downloads
                (game_id, profile_id_used, raw_path, download_date, downloaded_at, status,
                 size_bytes, sha256, source, sample_group, attempt_count, last_error)
            VALUES (?, NULL, NULL, ?, ?, ?, NULL, NULL, ?, ?, 1, NULL)
            """,
            new_downloaded + new_failed,
        )
    conn.close()
    return {"imported_downloaded": len(downloaded), "imported_failed": len(failed)}


def _write_to_summary_downloads(
    results: dict[str, Any] | None,
    source: str,
) -> dict[str, int]:
    if not results:
        return {"imported_summary_downloaded": 0, "imported_summary_failed": 0}

    downloaded_rows: list[tuple[int, int, str | None, int | None, str | None]] = []
    failed_rows: list[tuple[int, int, str | None]] = []
    for gid_str, record in results.items():
        try:
            game_id = int(gid_str)
        except (TypeError, ValueError):
            continue
        if not isinstance(record, dict):
            continue
        for summary in record.get("summaries", []) or []:
            try:
                profile_id = int(summary["profile_id"])
            except (TypeError, ValueError, KeyError):
                continue
            downloaded_rows.append((
                game_id,
                profile_id,
                str(summary["path"]) if summary.get("path") else None,
                int(summary["size_bytes"]) if summary.get("size_bytes") is not None else None,
                str(summary["sha256"]) if summary.get("sha256") else None,
            ))
        for failure in record.get("summary_failures", []) or []:
            try:
                profile_id = int(failure["profile_id"])
            except (TypeError, ValueError, KeyError):
                continue
            failed_rows.append((game_id, profile_id, str(failure.get("error") or "summary_download_failed")[:200]))

    if not downloaded_rows and not failed_rows:
        return {"imported_summary_downloaded": 0, "imported_summary_failed": 0}

    conn = get_conn(DB_PATH)
    now = datetime.now(timezone.utc)
    init_schema(conn)
    for game_id, profile_id, path, size_bytes, sha256 in downloaded_rows:
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
            VALUES (?, ?, ?, ?, 'downloaded', ?, ?, ?, ?, NULL)
            """,
            [game_id, profile_id, path, now, size_bytes, sha256, source, attempt_count],
        )
    for game_id, profile_id, error in failed_rows:
        existing = conn.execute(
            """
            SELECT attempt_count, status
            FROM replay_summary_downloads
            WHERE game_id = ? AND profile_id = ?
            """,
            [game_id, profile_id],
        ).fetchone()
        if existing and existing[1] == "downloaded":
            continue
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
            VALUES (?, ?, NULL, ?, 'failed', NULL, NULL, ?, ?, ?)
            """,
            [game_id, profile_id, now, source, attempt_count, error],
        )
    conn.close()
    return {
        "imported_summary_downloaded": len(downloaded_rows),
        "imported_summary_failed": len(failed_rows),
    }


def _import_sidecar(sidecar_path: Path, sample_group: str, source: str) -> dict[str, int]:
    if not sidecar_path.exists():
        return {
            "imported_downloaded": 0,
            "imported_failed": 0,
            "imported_summary_downloaded": 0,
            "imported_summary_failed": 0,
        }
    try:
        sidecar = json.loads(sidecar_path.read_text())
        results = sidecar.get("results", {})
        downloaded = [int(k) for k, v in results.items() if v.get("status") == "downloaded"]
        failed = [int(k) for k, v in results.items() if v.get("status") == "failed"]
        replay_result = _write_to_replay_downloads(downloaded, failed, sample_group, source)
        summary_result = _write_to_summary_downloads(results, source)
        return {**replay_result, **summary_result}
    except Exception:
        return {
            "imported_downloaded": 0,
            "imported_failed": 0,
            "imported_summary_downloaded": 0,
            "imported_summary_failed": 0,
        }


@app.post("/api/import-progress")
def api_import_progress(req: ImportProgressRequest):
    result = _write_to_replay_downloads(
        req.downloaded, req.failed, req.sample_group, "imported_from_friend"
    )
    summary_result = _write_to_summary_downloads(req.results, "imported_from_friend")
    return {**result, **summary_result}


_REPLAY_FILENAME_RE = re.compile(r"^AgeIV_Replay_(\d+)\.gz$")
_SUMMARY_FILENAME_RE = re.compile(r"^M_(\d+)_profile_(\d+)_summary\.gz$")


@app.post("/api/reconcile-disk")
def api_reconcile_disk():
    """Scan replay folders on disk, mark any found game_ids as downloaded in the DB."""
    found_ids: set[int] = set()
    for path in RAW_REPLAY_DIR.rglob("*.gz"):
        m = _REPLAY_FILENAME_RE.match(path.name)
        if m:
            found_ids.add(int(m.group(1)))
    found_summaries: dict[tuple[int, int], Path] = {}
    for path in SUMMARY_REPLAY_DIR.rglob("*.gz"):
        m = _SUMMARY_FILENAME_RE.match(path.name)
        if m and path.stat().st_size > 0:
            found_summaries[(int(m.group(1)), int(m.group(2)))] = path

    if not found_ids and not found_summaries:
        return {
            "scanned": 0,
            "newly_marked": 0,
            "already_known": 0,
            "summary_scanned": 0,
            "summary_newly_marked": 0,
            "summary_already_known": 0,
        }

    conn = get_conn(DB_PATH)
    init_schema(conn)
    rows = conn.execute(
        "SELECT game_id, status FROM replay_downloads"
    ).fetchall()
    already_downloaded = {int(r[0]) for r in rows if r[1] == "downloaded"}
    wrong_status      = {int(r[0]) for r in rows if r[1] != "downloaded"}

    # Games on disk with no DB entry at all → INSERT
    to_insert = found_ids - already_downloaded - wrong_status
    # Games on disk that exist in DB with wrong status → UPDATE to downloaded
    to_update = found_ids & wrong_status

    now = datetime.now(timezone.utc)
    if to_insert:
        conn.executemany(
            """
            INSERT INTO replay_downloads
                (game_id, profile_id_used, raw_path, download_date, downloaded_at, status,
                 size_bytes, sha256, source, sample_group, attempt_count, last_error)
            VALUES (?, NULL, NULL, ?, ?, 'downloaded', NULL, NULL, 'disk_scan', ?, 1, NULL)
            """,
            [(gid, now.date(), now, SAMPLE_GROUP_RECENT) for gid in to_insert],
        )
    if to_update:
        conn.executemany(
            "UPDATE replay_downloads SET status = 'downloaded', downloaded_at = ?, source = 'disk_scan' WHERE game_id = ?",
            [(now, gid) for gid in to_update],
        )

    summary_rows = conn.execute(
        """
        SELECT game_id, profile_id, status
        FROM replay_summary_downloads
        """
    ).fetchall()
    already_known_summaries = {
        (int(row[0]), int(row[1]))
        for row in summary_rows
        if row[2] == "downloaded"
    }
    summary_to_insert = set(found_summaries) - {
        (int(row[0]), int(row[1]))
        for row in summary_rows
    }
    summary_to_update = set(found_summaries) - already_known_summaries - summary_to_insert
    for key in summary_to_insert | summary_to_update:
        path = found_summaries[key]
        data = path.read_bytes()
        game_id, profile_id = key
        conn.execute(
            "DELETE FROM replay_summary_downloads WHERE game_id = ? AND profile_id = ?",
            [game_id, profile_id],
        )
        conn.execute(
            """
            INSERT INTO replay_summary_downloads
                (game_id, profile_id, summary_path, downloaded_at, status, size_bytes,
                 sha256, source, attempt_count, last_error)
            VALUES (?, ?, ?, ?, 'downloaded', ?, ?, 'disk_scan', 1, NULL)
            """,
            [game_id, profile_id, str(path), now, len(data), hashlib.sha256(data).hexdigest()],
        )

    newly_marked = len(to_insert) + len(to_update)
    summary_newly_marked = len(summary_to_insert) + len(summary_to_update)

    # Count how many of the newly marked were in the pending list
    pending_reduced = 0
    if newly_marked:
        changed = list(to_insert | to_update)
        placeholders = ",".join("?" * len(changed))
        pending_reduced = conn.execute(
            f"SELECT count(*) FROM replay_candidate_labels WHERE game_id IN ({placeholders})",
            changed,
        ).fetchone()[0]

    conn.close()
    return {
        "scanned": len(found_ids),
        "newly_marked": newly_marked,
        "already_known": len(already_downloaded & found_ids),
        "pending_reduced": int(pending_reduced),
        "summary_scanned": len(found_summaries),
        "summary_newly_marked": summary_newly_marked,
        "summary_already_known": len(already_known_summaries & set(found_summaries)),
    }


class SummaryBackfillRequest(BaseModel):
    sleep_min: float = 15.0
    sleep_max: float = 30.0
    user_agent: str = "AOE4ReplayHarvest/0.1 (contact@example.com)"
    on_429: str = "sleep"
    on_429_minutes: float = 15.0


@app.post("/api/backfill-summaries/start")
def api_backfill_summaries_start(req: SummaryBackfillRequest):
    if SUMMARY_BACKFILL.get("status") in ("running", "stopping"):
        raise HTTPException(409, "summary backfill already running")

    stop_event = threading.Event()
    SUMMARY_BACKFILL.update({
        "status": "running",
        "counts": {"games": 0, "summary_downloaded": 0, "summary_failed": 0, "summary_skipped": 0},
        "error": None,
        "current_game_id": None,
        "index": 0,
        "total": 0,
        "last_event": None,
        "last_error": None,
        "sleep_until": None,
        "stop_event": stop_event,
    })

    def run() -> None:
        conn = None
        try:
            conn = get_conn(DB_PATH)
            init_schema(conn)
            game_ids = summary_backfill_game_ids(conn, raw_root=RAW_REPLAY_DIR)
            total = len(game_ids)
            SUMMARY_BACKFILL.update({"total": total})
            counts = {"games": 0, "summary_downloaded": 0, "summary_failed": 0, "summary_skipped": 0}
            for idx, game_id in enumerate(game_ids):
                if stop_event.is_set():
                    SUMMARY_BACKFILL.update({"status": "stopped"})
                    break
                SUMMARY_BACKFILL.update({
                    "index": idx + 1,
                    "current_game_id": game_id,
                    "counts": dict(counts),
                })
                _record_summary_backfill_start(conn, game_id)
                result: dict[str, object] | None = None
                post_result_pause = 0.0
                for attempt in range(2):
                    try:
                        SUMMARY_BACKFILL.update({
                            "last_event": "attempt_start",
                            "last_error": None,
                            "sleep_until": None,
                            "attempt": attempt + 1,
                        })
                        result = download_summary_files(
                            conn,
                            game_id,
                            summary_root=SUMMARY_REPLAY_DIR,
                            user_agent=req.user_agent,
                            source="webapp_summary_backfill",
                            skip_existing=True,
                            raise_retryable_errors=True,
                        )
                        break
                    except HTTPError as exc:
                        error = f"http_{exc.code}"
                        if exc.code == 429 and req.on_429 == "stop":
                            result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": error}
                            SUMMARY_BACKFILL.update({
                                "status": "stopping",
                                "last_event": "rate_limited_stop",
                                "last_error": error,
                            })
                            stop_event.set()
                            break
                        if exc.code == 429 and attempt == 0:
                            pause = max(0.0, req.on_429_minutes * 60)
                            SUMMARY_BACKFILL.update({
                                "last_event": "rate_limited_sleep",
                                "last_error": error,
                                "sleep_until": (datetime.now(timezone.utc) + timedelta(seconds=pause)).isoformat(),
                            })
                            if stop_event.wait(timeout=pause):
                                result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": "stopped_during_429_sleep"}
                                break
                            continue
                        if exc.code == 403:
                            post_result_pause = 60 * 60
                            result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": error}
                            SUMMARY_BACKFILL.update({
                                "last_event": "http_403_pause_after_result",
                                "last_error": error,
                            })
                            break
                        if 500 <= exc.code <= 599 and attempt == 0:
                            pause = 120
                            SUMMARY_BACKFILL.update({
                                "last_event": "server_error_sleep",
                                "last_error": error,
                                "sleep_until": (datetime.now(timezone.utc) + timedelta(seconds=pause)).isoformat(),
                            })
                            if stop_event.wait(timeout=pause):
                                result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": "stopped_during_server_error_sleep"}
                                break
                            continue
                        result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": error}
                        break
                    except TimeoutError:
                        if attempt == 0:
                            pause = 30
                            SUMMARY_BACKFILL.update({
                                "last_event": "timeout_sleep",
                                "last_error": "timeout",
                                "sleep_until": (datetime.now(timezone.utc) + timedelta(seconds=pause)).isoformat(),
                            })
                            if stop_event.wait(timeout=pause):
                                result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": "stopped_during_timeout_sleep"}
                                break
                            continue
                        result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": "timeout"}
                        break
                    except URLError as exc:
                        error = f"network_error: {str(exc.reason)[:80]}"
                        if attempt == 0:
                            pause = 30
                            SUMMARY_BACKFILL.update({
                                "last_event": "network_error_sleep",
                                "last_error": error,
                                "sleep_until": (datetime.now(timezone.utc) + timedelta(seconds=pause)).isoformat(),
                            })
                            if stop_event.wait(timeout=pause):
                                result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": "stopped_during_network_sleep"}
                                break
                            continue
                        result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": error}
                        break
                if result is None:
                    result = {"downloaded": 0, "failed": 1, "skipped": 0, "error": "stopped"}
                _record_summary_backfill_result(conn, game_id, result)
                counts["games"] += 1
                counts["summary_downloaded"] += int(result.get("downloaded", 0))
                counts["summary_failed"] += int(result.get("failed", 0))
                counts["summary_skipped"] += int(result.get("skipped", 0))
                SUMMARY_BACKFILL.update({
                    "counts": dict(counts),
                    "last_event": "game_result",
                    "last_error": result.get("error"),
                    "sleep_until": None,
                })
                if stop_event.is_set():
                    SUMMARY_BACKFILL.update({"status": "stopped"})
                    break
                if post_result_pause > 0:
                    SUMMARY_BACKFILL.update({
                        "last_event": "http_403_sleep",
                        "sleep_until": (datetime.now(timezone.utc) + timedelta(seconds=post_result_pause)).isoformat(),
                    })
                    if stop_event.wait(timeout=post_result_pause):
                        SUMMARY_BACKFILL.update({"status": "stopped"})
                        break
                if idx < total - 1:
                    pause = random.uniform(req.sleep_min, req.sleep_max)
                    SUMMARY_BACKFILL.update({
                        "last_event": "sleep_between_games",
                        "sleep_until": (datetime.now(timezone.utc) + timedelta(seconds=pause)).isoformat(),
                    })
                    if stop_event.wait(timeout=max(0.0, pause)):
                        SUMMARY_BACKFILL.update({"status": "stopped"})
                        break
            else:
                SUMMARY_BACKFILL.update({"status": "done", "current_game_id": None, "counts": dict(counts)})
        except Exception as exc:
            SUMMARY_BACKFILL.update({"status": "error", "error": str(exc)})
        finally:
            if conn is not None:
                conn.close()
            SUMMARY_BACKFILL.pop("stop_event", None)

    threading.Thread(target=run, daemon=True).start()
    return {"status": "started"}


@app.post("/api/backfill-summaries/stop")
def api_backfill_summaries_stop():
    stop_event = SUMMARY_BACKFILL.get("stop_event")
    if SUMMARY_BACKFILL.get("status") != "running" or stop_event is None:
        return {"status": SUMMARY_BACKFILL.get("status", "idle")}
    stop_event.set()
    SUMMARY_BACKFILL.update({"status": "stopping"})
    return {"status": "stopping"}


@app.get("/api/backfill-summaries/status")
def api_backfill_summaries_status():
    response = {k: v for k, v in SUMMARY_BACKFILL.items() if k != "stop_event"}
    try:
        conn = get_conn(DB_PATH, read_only=True)
        rows = conn.execute(
            """
            SELECT status, count(*)
            FROM replay_summary_backfill_log
            GROUP BY status
            ORDER BY status
            """
        ).fetchall()
        conn.close()
        response["log_counts"] = {str(status): int(count) for status, count in rows}
    except Exception:
        response["log_counts"] = {}
    return response


# ── Download ───────────────────────────────────────────────────────────────────

class StartRequest(BaseModel):
    job_id: str | None = None
    job_content: dict | None = None
    sleep_min: float = 40.0
    sleep_max: float = 70.0
    user_agent: str = "AOE4ReplayHarvest/0.1 (contact@example.com)"
    on_429: str = "sleep"           # "sleep" or "stop"
    on_429_minutes: float = 15.0


def _reset_session() -> None:
    SESSION.stop_event.set()
    if SESSION.thread and SESSION.thread.is_alive():
        SESSION.thread.join(timeout=3)
    SESSION.stop_event = threading.Event()
    while True:
        try:
            SESSION.event_queue.get_nowait()
        except queue.Empty:
            break
    SESSION.progress = {"downloaded": 0, "failed": 0, "skipped": 0, "total": 0}
    SESSION.thread = None
    SESSION.job_id = None


@app.post("/api/start")
def api_start(req: StartRequest):
    if SESSION.thread and SESSION.thread.is_alive():
        raise HTTPException(409, "download already running — pause first")

    # Resolve job data from job_id reference or uploaded content
    if req.job_id:
        job = PENDING_JOBS.get(req.job_id)
        if job is None:
            path = REPORT_DIR / f"{req.job_id}.json"
            if not path.exists():
                raise HTTPException(404, f"job {req.job_id!r} not found")
            job = json.loads(path.read_text())
    elif req.job_content:
        job = req.job_content
    else:
        raise HTTPException(400, "provide job_id or job_content")

    _reset_session()
    SESSION.job_id = job.get("job_id", "unknown")
    SESSION.progress["total"] = job.get("total_games", len(job.get("games", [])))

    # Persist job to disk so the sidecar path is deterministic
    job_path = REPORT_DIR / f"{SESSION.job_id}.json"
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    if not job_path.exists():
        job_path.write_text(json.dumps(job, indent=2))

    sleep_min = req.sleep_min
    sleep_max = req.sleep_max
    user_agent = req.user_agent
    on_429 = req.on_429
    on_429_minutes = req.on_429_minutes
    stop_event = SESSION.stop_event

    def on_event(evt: dict) -> None:
        status = evt.get("status")
        if status in ("downloaded", "failed", "skipped"):
            SESSION.progress[status] = SESSION.progress.get(status, 0) + 1
        SESSION.event_queue.put(evt)

    SESSION.sample_group = job.get("group", SAMPLE_GROUP_RECENT)

    # Pre-query replay_downloads so already-downloaded games are skipped instantly,
    # even if they were downloaded on a different day or under a different job.
    job_game_ids = {int(g["game_id"]) for g in job.get("games", [])}
    try:
        _db = get_conn(DB_PATH)
        _rows = _db.execute(
            "SELECT game_id FROM replay_downloads WHERE status = 'downloaded'"
        ).fetchall()
        _db.close()
        pre_done_ids = {int(r[0]) for r in _rows} & job_game_ids
    except Exception:
        pre_done_ids = set()

    def run() -> None:
        download_job_list(
            job_path=job_path,
            raw_root=RAW_REPLAY_DIR,
            summary_root=SUMMARY_REPLAY_DIR,
            sleep_min=sleep_min,
            sleep_max=sleep_max,
            user_agent=user_agent,
            on_event=on_event,
            stop_event=stop_event,
            on_429=on_429,
            on_429_minutes=on_429_minutes,
            pre_done_ids=pre_done_ids,
        )
        # Sync sidecar → DB so "Show Pending" reflects what was downloaded
        sidecar_path = REPORT_DIR / f"{SESSION.job_id}.progress.json"
        _import_sidecar(sidecar_path, SESSION.sample_group, "coordinator_session")

    SESSION.thread = threading.Thread(target=run, daemon=True)
    SESSION.thread.start()
    return {"status": "started", "job_id": SESSION.job_id, "total": SESSION.progress["total"]}


@app.post("/api/pause")
def api_pause():
    SESSION.stop_event.set()
    return {"status": "pausing"}


@app.get("/api/status")
def api_status():
    return {
        **SESSION.progress,
        "running": SESSION.thread is not None and SESSION.thread.is_alive(),
        "job_id": SESSION.job_id,
    }


# ── SSE event stream ───────────────────────────────────────────────────────────

@app.get("/api/events")
async def api_events():
    async def generator():
        yield "data: {\"type\": \"connected\"}\n\n"
        while True:
            try:
                evt = SESSION.event_queue.get_nowait()
                yield f"data: {json.dumps(evt)}\n\n"
            except queue.Empty:
                await asyncio.sleep(0.2)
                yield ": heartbeat\n\n"

    return StreamingResponse(
        generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
