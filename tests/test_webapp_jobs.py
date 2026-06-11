from __future__ import annotations

import duckdb
import time
from fastapi.testclient import TestClient
from urllib.error import HTTPError

import webapp.app as web_app


def setup_temp_app(monkeypatch, tmp_path):
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE replay_downloads (
            game_id BIGINT,
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
        """
    )
    conn.close()

    report_dir = tmp_path / "reports"
    monkeypatch.setattr(web_app, "DB_PATH", db_path)
    monkeypatch.setattr(web_app, "REPORT_DIR", report_dir)
    web_app.PENDING_JOBS.clear()
    web_app.SUMMARY_BACKFILL.update({
        "status": "idle",
        "counts": None,
        "error": None,
        "current_game_id": None,
        "index": 0,
        "total": 0,
    })
    return TestClient(web_app.app), report_dir


def test_generate_jobs_skips_games_without_profile_id(monkeypatch, tmp_path):
    client, report_dir = setup_temp_app(monkeypatch, tmp_path)

    response = client.post(
        "/api/generate-jobs",
        json={
            "games": [
                {"game_id": 1, "profile_id": 10},
                {"game_id": 2, "profile_id": None},
                {"game_id": 3},
            ],
            "splits": 2,
            "group": "recent_rm_1v1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["skipped_invalid"] == 2
    assert len(body["jobs"]) == 1
    assert body["jobs"][0]["total_games"] == 1
    assert (report_dir / f"{body['jobs'][0]['job_id']}.json").exists()


def test_generate_jobs_returns_json_error_when_no_games_are_valid(monkeypatch, tmp_path):
    client, _ = setup_temp_app(monkeypatch, tmp_path)

    response = client.post(
        "/api/generate-jobs",
        json={"games": [{"game_id": 2, "profile_id": None}], "splits": 2},
    )

    assert response.status_code == 400
    assert response.headers["content-type"].startswith("application/json")
    assert response.json()["detail"] == "no games with valid game_id and profile_id provided"


def test_mark_unobtainable_records_manual_exclusions_without_touching_download_rows(monkeypatch, tmp_path):
    client, _ = setup_temp_app(monkeypatch, tmp_path)
    conn = duckdb.connect(str(web_app.DB_PATH))
    conn.execute(
        """
        INSERT INTO replay_downloads
        VALUES (1, NULL, NULL, current_date, current_timestamp, 'assigned', NULL, NULL,
                'job_assignment', 'recent_rm_1v1', 0, NULL)
        """
    )
    conn.close()

    response = client.post(
        "/api/mark-unobtainable",
        json={"game_ids": [1, 2], "reason": "replay_expired", "detail": "manual check"},
    )

    assert response.status_code == 200
    assert response.json()["marked"] == 2
    conn = duckdb.connect(str(web_app.DB_PATH))
    download_rows = conn.execute(
        "SELECT game_id, status, source, last_error FROM replay_downloads ORDER BY game_id"
    ).fetchall()
    excluded_rows = conn.execute(
        """
        SELECT game_id, reason, detail, source
        FROM replay_unobtainable_games
        ORDER BY game_id
        """
    ).fetchall()
    conn.close()
    assert download_rows == [
        (1, "assigned", "job_assignment", None),
    ]
    assert excluded_rows == [
        (1, "replay_expired", "manual check", "manual"),
        (2, "replay_expired", "manual check", "manual"),
    ]


def test_generate_jobs_skips_manually_unobtainable_games(monkeypatch, tmp_path):
    client, report_dir = setup_temp_app(monkeypatch, tmp_path)
    conn = duckdb.connect(str(web_app.DB_PATH))
    web_app.init_schema(conn)
    conn.execute(
        """
        INSERT INTO replay_unobtainable_games
        VALUES (1, current_timestamp, 'download_404', '404', 'manual')
        """
    )
    conn.close()

    response = client.post(
        "/api/generate-jobs",
        json={
            "games": [
                {"game_id": 1, "profile_id": 10},
                {"game_id": 2, "profile_id": 20},
            ],
            "splits": 2,
            "group": "recent_rm_1v1",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["skipped_unobtainable"] == 1
    assert len(body["jobs"]) == 1
    job_path = report_dir / f"{body['jobs'][0]['job_id']}.json"
    assert '"game_id": 2' in job_path.read_text()


def test_import_progress_records_summary_results(monkeypatch, tmp_path):
    client, _ = setup_temp_app(monkeypatch, tmp_path)

    response = client.post(
        "/api/import-progress",
        json={
            "job_id": "job_summary",
            "downloaded": [10],
            "failed": [],
            "results": {
                "10": {
                    "status": "downloaded",
                    "summaries": [
                        {
                            "profile_id": 1001,
                            "path": "data/replays/summaries/2026-06-10/M_10_profile_1001_summary.gz",
                            "size_bytes": 123,
                            "sha256": "abc",
                        }
                    ],
                    "summary_failures": [
                        {"profile_id": 1002, "error": "http_404"}
                    ],
                }
            },
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert body["imported_summary_downloaded"] == 1
    assert body["imported_summary_failed"] == 1
    conn = duckdb.connect(str(web_app.DB_PATH))
    rows = conn.execute(
        """
        SELECT game_id, profile_id, status, size_bytes, last_error
        FROM replay_summary_downloads
        ORDER BY profile_id
        """
    ).fetchall()
    conn.close()
    assert rows == [
        (10, 1001, "downloaded", 123, None),
        (10, 1002, "failed", None, "http_404"),
    ]


def test_summary_backfill_endpoint_runs_background_task(monkeypatch, tmp_path):
    client, _ = setup_temp_app(monkeypatch, tmp_path)

    monkeypatch.setattr(
        web_app,
        "summary_backfill_game_ids",
        lambda conn, raw_root: [1, 2],
    )

    def fake_download_summary_files(conn, game_id, **kwargs):
        return {"downloaded": 1, "failed": 0, "skipped": 0}

    monkeypatch.setattr(web_app, "download_summary_files", fake_download_summary_files)

    response = client.post(
        "/api/backfill-summaries/start",
        json={"sleep_min": 0, "sleep_max": 0, "user_agent": "test"},
    )

    assert response.status_code == 200
    deadline = time.time() + 2
    status = {}
    while time.time() < deadline:
        status = client.get("/api/backfill-summaries/status").json()
        if status["status"] == "done":
            break
        time.sleep(0.01)

    assert status["status"] == "done"
    assert status["total"] == 2
    assert status["counts"]["games"] == 2
    assert status["counts"]["summary_downloaded"] == 2
    assert status["log_counts"]["downloaded"] == 2
    conn = duckdb.connect(str(web_app.DB_PATH))
    rows = conn.execute(
        """
        SELECT game_id, status, summary_downloaded, summary_failed, summary_skipped
        FROM replay_summary_backfill_log
        ORDER BY game_id
        """
    ).fetchall()
    conn.close()
    assert rows == [
        (1, "downloaded", 1, 0, 0),
        (2, "downloaded", 1, 0, 0),
    ]


def test_summary_backfill_stop_on_429_records_failed_log(monkeypatch, tmp_path):
    client, _ = setup_temp_app(monkeypatch, tmp_path)
    monkeypatch.setattr(web_app, "summary_backfill_game_ids", lambda conn, raw_root: [3])

    def rate_limited(conn, game_id, **kwargs):
        raise HTTPError("url", 429, "Too Many Requests", hdrs=None, fp=None)

    monkeypatch.setattr(web_app, "download_summary_files", rate_limited)

    response = client.post(
        "/api/backfill-summaries/start",
        json={"sleep_min": 0, "sleep_max": 0, "user_agent": "test", "on_429": "stop", "on_429_minutes": 0},
    )

    assert response.status_code == 200
    deadline = time.time() + 2
    status = {}
    while time.time() < deadline:
        status = client.get("/api/backfill-summaries/status").json()
        if status["status"] == "stopped":
            break
        time.sleep(0.01)

    assert status["status"] == "stopped"
    assert status["counts"]["summary_failed"] == 1
    conn = duckdb.connect(str(web_app.DB_PATH))
    row = conn.execute(
        """
        SELECT status, summary_failed, last_error
        FROM replay_summary_backfill_log
        WHERE game_id = 3
        """
    ).fetchone()
    conn.close()
    assert row == ("failed", 1, "http_429")


def test_summary_backfill_sleep_on_429_retries(monkeypatch, tmp_path):
    client, _ = setup_temp_app(monkeypatch, tmp_path)
    monkeypatch.setattr(web_app, "summary_backfill_game_ids", lambda conn, raw_root: [4])
    calls = {"count": 0}

    def rate_limited_once(conn, game_id, **kwargs):
        calls["count"] += 1
        if calls["count"] == 1:
            raise HTTPError("url", 429, "Too Many Requests", hdrs=None, fp=None)
        return {"downloaded": 1, "failed": 0, "skipped": 0}

    monkeypatch.setattr(web_app, "download_summary_files", rate_limited_once)

    response = client.post(
        "/api/backfill-summaries/start",
        json={"sleep_min": 0, "sleep_max": 0, "user_agent": "test", "on_429": "sleep", "on_429_minutes": 0},
    )

    assert response.status_code == 200
    deadline = time.time() + 2
    status = {}
    while time.time() < deadline:
        status = client.get("/api/backfill-summaries/status").json()
        if status["status"] == "done":
            break
        time.sleep(0.01)

    assert status["status"] == "done"
    assert calls["count"] == 2
    assert status["counts"]["summary_downloaded"] == 1
