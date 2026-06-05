# Replay Harvest

Tools for acquiring raw Age of Empires IV replay files against the existing
`aoe4.duckdb` match warehouse.

The harvester uses existing `games` and `participants` rows as the metadata
source of truth. It adds replay-specific bookkeeping tables, labels candidate
matches, downloads raw replay files slowly, and records parser status after
running the replay parser.

Raw files are written under `data/replays/raw/YYYY-MM-DD/`. Parsed files are
written under `data/replays/parsed/<game_id>/`. DuckDB remains the source of
truth for download and parse status.

## Commands

Initialize replay bookkeeping tables:

```bash
python -m replay_harvest init-schema
```

Label a balanced RM 1v1 sample by rating bucket:

```bash
python -m replay_harvest label-balanced --limit 10000
```

Label complete coverage for the current top 100 canonical AoE4World players,
including linked alternate accounts where AoE4World exposes them:

```bash
python -m replay_harvest label-top100
```

Show labeled candidate counts:

```bash
python -m replay_harvest candidates
```

Discover current recent RM 1v1 games from profile IDs in the old warehouse:

```bash
python -m replay_harvest discover-recent --seed-limit 200 --per-player 25 --days 10
```

Download labeled replays slowly:

```bash
python -m replay_harvest download --group balanced_10k --limit 1000 --sleep-min 15 --sleep-max 30
```

Use `--group top100_complete` to download the top-player coverage set.
Use `--group recent_rm_1v1` to download currently discovered recent games.

Parse downloaded files:

```bash
python -m replay_harvest parse-downloaded --group balanced_10k --limit 100
```

Write sample/download reports:

```bash
python -m replay_harvest report
```

Reports are written to `data/replays/reports/`.

## Safety Defaults

Downloads run as a single worker. The default command examples use 15-30 seconds
between replay requests. Failed downloads are recorded in `replay_downloads`
with `last_error`, and successful downloads are deduplicated by `game_id`.

Set a contact-bearing User-Agent before using live APIs:

```bash
export AOE4_REPLAY_HARVEST_USER_AGENT='AOE4ReplayHarvest/0.1 (yangshw0223@gmail.com; Discord: shiweny#8556)'
```
