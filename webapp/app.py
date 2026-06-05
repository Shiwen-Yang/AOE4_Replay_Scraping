from __future__ import annotations

import asyncio
import json
import queue
import re
import sys
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

# Ensure replay_harvest is importable when running from any directory
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from replay_harvest.config import DB_PATH, RAW_REPLAY_DIR, REPORT_DIR, SAMPLE_GROUP_RECENT
from replay_harvest.db import get_conn
from replay_harvest.downloader import download_job_list
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


# ── Pages ──────────────────────────────────────────────────────────────────────

@app.get("/")
def coordinator_page():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/friend")
def friend_page():
    return FileResponse(STATIC_DIR / "friend.html")


# ── Discovery (coordinator only) ───────────────────────────────────────────────

class DiscoverRequest(BaseModel):
    days: int = 7
    target_per_tier: int = 100
    per_player: int = 25
    sleep_seconds: float = 1.0


@app.post("/api/discover/start")
def api_discover_start(req: DiscoverRequest):
    if DISCOVERY["status"] == "running":
        raise HTTPException(409, "discovery already running")
    DISCOVERY.update({"status": "running", "result": None, "error": None,
                       "phase": "starting", "phases_done": 0, "phases_total": 6})

    def run():
        from replay_harvest.discovery import discover_tiered_games, PHASES_TOTAL

        def on_phase(phase_name: str, phases_done: int, phases_total: int) -> None:
            DISCOVERY.update({
                "phase": phase_name,
                "phases_done": phases_done,
                "phases_total": phases_total,
            })

        try:
            conn = get_conn(DB_PATH)
            init_schema(conn)
            result = discover_tiered_games(
                conn,
                days=req.days,
                target_per_tier=req.target_per_tier,
                per_player=req.per_player,
                sleep_seconds=req.sleep_seconds,
                on_phase=on_phase,
            )
            conn.close()
            DISCOVERY.update({"status": "done", "result": result, "error": None,
                               "phases_done": PHASES_TOTAL, "phases_total": PHASES_TOTAL})
        except Exception as exc:
            DISCOVERY.update({"status": "error", "result": None, "error": str(exc)})

    threading.Thread(target=run, daemon=True).start()
    return {"status": "started"}


@app.get("/api/discover/status")
def api_discover_status():
    return DISCOVERY


@app.get("/api/assigned")
def api_assigned():
    from replay_harvest.discovery import get_assigned_games
    conn = get_conn(DB_PATH, read_only=True)
    result = get_assigned_games(conn)
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
    conn = get_conn(DB_PATH, read_only=True)
    result = get_pending_games(conn)
    conn.close()
    return result


# ── Job generation ─────────────────────────────────────────────────────────────

class GenerateJobsRequest(BaseModel):
    games: list[dict]
    splits: int = 2
    group: str = "recent_rm_1v1"


@app.post("/api/generate-jobs")
def api_generate_jobs(req: GenerateJobsRequest):
    if not req.games:
        raise HTTPException(400, "no games provided")

    k = max(1, min(req.splits, len(req.games)))
    base = len(req.games) // k
    remainder = len(req.games) % k

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    jobs = []
    start = 0

    for i in range(k):
        size = base + (1 if i < remainder else 0)
        chunk = req.games[start:start + size]
        start += size

        job_id = f"job_{i + 1}_of_{k}_{ts}"
        job = {
            "version": "1",
            "job_id": job_id,
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "group": req.group,
            "total_games": len(chunk),
            "games": [{"game_id": g["game_id"], "profile_id": g["profile_id"]} for g in chunk],
        }
        path = REPORT_DIR / f"{job_id}.json"
        path.write_text(json.dumps(job, indent=2))
        PENDING_JOBS[job_id] = job
        jobs.append({"job_id": job_id, "total_games": len(chunk)})

    # Mark all distributed game_ids as 'assigned' so they won't be included
    # in future pending queries or handed out in a second round of jobs.
    all_game_ids = [int(g["game_id"]) for g in req.games]
    if all_game_ids:
        conn = get_conn(DB_PATH)
        now = datetime.now(timezone.utc)
        conn.executemany(
            """
            INSERT OR IGNORE INTO replay_downloads
                (game_id, profile_id_used, raw_path, download_date, downloaded_at, status,
                 size_bytes, sha256, source, sample_group, attempt_count, last_error)
            VALUES (?, NULL, NULL, ?, ?, 'assigned', NULL, NULL, 'job_assignment', ?, 0, NULL)
            """,
            [(gid, now.date(), now, req.group) for gid in all_game_ids],
        )
        conn.close()

    return {"jobs": jobs}


class SaveJobRequest(BaseModel):
    games: list[dict]
    group: str = "recent_rm_1v1"


@app.post("/api/save-job")
def api_save_job(req: SaveJobRequest):
    """Persist an in-memory job list to disk so it survives a server restart."""
    if not req.games:
        raise HTTPException(400, "no games provided")

    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    job_id = f"coordinator_{ts}"
    job = {
        "version": "1",
        "job_id": job_id,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "group": req.group,
        "total_games": len(req.games),
        "games": [{"game_id": g["game_id"], "profile_id": g["profile_id"]} for g in req.games],
    }
    path = REPORT_DIR / f"{job_id}.json"
    path.write_text(json.dumps(job, indent=2))
    PENDING_JOBS[job_id] = job

    # Games were already marked 'assigned' when the original job was generated;
    # INSERT OR IGNORE is safe to call again and ensures any new ones are covered.
    all_game_ids = [int(g["game_id"]) for g in req.games]
    if all_game_ids:
        conn = get_conn(DB_PATH)
        now = datetime.now(timezone.utc)
        conn.executemany(
            """
            INSERT OR IGNORE INTO replay_downloads
                (game_id, profile_id_used, raw_path, download_date, downloaded_at, status,
                 size_bytes, sha256, source, sample_group, attempt_count, last_error)
            VALUES (?, NULL, NULL, ?, ?, 'assigned', NULL, NULL, 'job_assignment', ?, 0, NULL)
            """,
            [(gid, now.date(), now, req.group) for gid in all_game_ids],
        )
        conn.close()

    return {"job_id": job_id, "total_games": len(req.games)}


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


class ImportProgressRequest(BaseModel):
    job_id: str
    downloaded: list[int]
    failed: list[int]
    skipped: list[int] = []
    sample_group: str = SAMPLE_GROUP_RECENT


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


def _import_sidecar(sidecar_path: Path, sample_group: str, source: str) -> dict[str, int]:
    if not sidecar_path.exists():
        return {"imported_downloaded": 0, "imported_failed": 0}
    try:
        sidecar = json.loads(sidecar_path.read_text())
        results = sidecar.get("results", {})
        downloaded = [int(k) for k, v in results.items() if v.get("status") == "downloaded"]
        failed = [int(k) for k, v in results.items() if v.get("status") == "failed"]
        return _write_to_replay_downloads(downloaded, failed, sample_group, source)
    except Exception:
        return {"imported_downloaded": 0, "imported_failed": 0}


@app.post("/api/import-progress")
def api_import_progress(req: ImportProgressRequest):
    result = _write_to_replay_downloads(
        req.downloaded, req.failed, req.sample_group, "imported_from_friend"
    )
    return result


_REPLAY_FILENAME_RE = re.compile(r"^AgeIV_Replay_(\d+)\.gz$")


@app.post("/api/reconcile-disk")
def api_reconcile_disk():
    """Scan replay folders on disk, mark any found game_ids as downloaded in the DB."""
    found_ids: set[int] = set()
    for path in RAW_REPLAY_DIR.rglob("*.gz"):
        m = _REPLAY_FILENAME_RE.match(path.name)
        if m:
            found_ids.add(int(m.group(1)))

    if not found_ids:
        return {"scanned": 0, "newly_marked": 0, "already_known": 0}

    conn = get_conn(DB_PATH)
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

    newly_marked = len(to_insert) + len(to_update)

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
    }


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
        _db = get_conn(DB_PATH, read_only=True)
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
