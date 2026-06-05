# AOE4 Replay Scraping

Collaborative tool for harvesting Age of Empires IV replay files. One person runs the coordinator webapp to discover games and distribute work; friends download their assigned share and send back a small progress report.

## Requirements

- Python 3.11+
- `pip install fastapi uvicorn duckdb`

The `aoe4.duckdb` database file must be present in the project root. It holds the game/participant metadata used to drive discovery.

## Starting the server

Run this from the project root:

```bash
python3 -m uvicorn webapp.app:app --port 8000
```

Then open:
- **Coordinator**: http://localhost:8000/
- **Friend**: http://localhost:8000/friend

---

## Coordinator workflow

### 1. Discover games

Click **Run Discovery** to pull recent ranked 1v1 games from the AoE4World API. Configure:

| Setting | Default | What it does |
|---|---|---|
| Days lookback | 7 | How far back to look for games |
| Games per tier | 100 | Target count per skill tier (elite / high / mid / low-mid / low) |
| API sleep | 1.5 s | Pause between every AoE4World API call |

Discovery is stratified by rank — top 50 players get complete coverage, and the remaining tiers each contribute roughly equal game counts. An estimated runtime is shown before you start.

### 2. Review results

After discovery completes, the table shows all newly found games. The tier summary bar shows how many came from each rank band.

Use **Show Pending** at any time to see everything not yet downloaded or assigned. Use **Show Assigned** to see games that are currently handed out to active jobs.

### 3. Split into jobs and distribute

Set **Split into N jobs** and click **Generate Job Files**. Each job gets a download link. Send the `.json` file to each friend (Discord, email, etc.).

Keep one job for yourself — click **Use this job now** next to it. This saves a job file to disk and loads it into Section 2 so you can start downloading immediately.

> **If the server crashes mid-download:** the job file was written to `data/replays/reports/` before download started. Re-upload it via the job file picker in Section 2 to resume — already-downloaded games are automatically skipped.

### 4. Download your share

In Section 2, set your contact info (included in the User-Agent sent to the API — keeps requests polite and identifiable). Then click **Start Download**. You can pause and resume at any time.

### 5. Import a friend's progress

When a friend finishes, they send you a `.progress.json` file. Go to **Sync & Import → Import Friend's Progress**, upload the file, and click Import. Their games are marked done in the database and won't be re-queued in future runs.

### 6. Reconcile disk

If you copied replay files in from outside the system (e.g. from a friend's machine directly), run **Check & Update Pending** to scan `data/replays/` and sync the database with what's actually on disk.

---

## Friend workflow

1. Receive a `job_XXXXX.json` file from the coordinator.
2. Open http://localhost:8000/friend
3. Under **Load Job**, upload the `.json` file.
4. Set your contact info under **Contact**.
5. Click **Start Download** and let it run. You can pause and resume freely.
6. When the session finishes, a **Download Progress Report** button appears. Download the `.progress.json` file and send it back to the coordinator.

Replays are saved to `data/replays/raw/<date>/`.

---

## Assigned / pending states

| State | Meaning |
|---|---|
| pending | Discovered, not yet downloaded or assigned |
| assigned | Included in a generated job — won't be handed out again |
| downloaded | Successfully downloaded |
| failed | Download attempted but errored |

Resetting assigned games (via **Show Assigned → Reset to pending**) removes them from active jobs and makes them available for the next job generation.

---

## Data layout

```
aoe4.duckdb                   game/participant metadata + download tracking
data/replays/
  raw/<date>/                 downloaded .gz replay files
  reports/
    job_*.json                job files handed out to friends
    coordinator_*.json        coordinator's own job (saved for crash recovery)
    *.progress.json           per-session download progress sidecars
```

---

## User-Agent

The downloader identifies itself to the Age of Empires API with a contact-bearing User-Agent. You can set it via environment variable if you want to override the default:

```bash
export AOE4_REPLAY_HARVEST_USER_AGENT="AOE4ReplayHarvest/0.1 (you@example.com)"
```

Otherwise, set your contact info in the webapp UI — it builds the User-Agent header per-session.
