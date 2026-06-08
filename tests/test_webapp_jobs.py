from __future__ import annotations

import duckdb
from fastapi.testclient import TestClient

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
