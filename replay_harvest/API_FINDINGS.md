# Age IV Match and Replay API Findings

These notes summarize the useful public APIs found while investigating fresh
Age of Empires IV matches. The most important discovery is that replay summary
files are exposed by the WorldsEdgeLink community API as `datatype = 1` replay
files.

## API Roots

```text
WorldsEdgeLink community API:
https://aoe-api.worldsedgelink.com

Official Age site stats API:
https://api.ageofempires.com
```

Use a contact-bearing `User-Agent` for all requests and keep request rates low.
LibreMatch guidance recommends self-throttling and conservative request rates.

## Match Details by Match ID

Prefer this endpoint when the `match_id` is already known:

```text
GET https://aoe-api.worldsedgelink.com/community/leaderboard/getMatchHistory?title=age4&matchIDs=[237506997]
```

URL-encoded:

```text
https://aoe-api.worldsedgelink.com/community/leaderboard/getMatchHistory?title=age4&matchIDs=%5B237506997%5D
```

Useful response fields:

| Field | Meaning |
|---|---|
| `matchHistory[].id` | Match/game id |
| `matchHistory[].startgametime` | Unix start timestamp |
| `matchHistory[].completiontime` | Unix completion timestamp |
| `matchHistory[].mapname` | Often generic, e.g. `generated_map` |
| `matchHistory[].options` | Base64 zlib-compressed match options JSON |
| `matchHistory[].slotinfo` | Base64 zlib-compressed slot/player config |
| `matchHistory[].matchhistoryreportresults[]` | Per-player result, team, civilization, counters |
| `matchHistory[].matchhistorymember[]` | Per-player rating, streak, wins/losses, outcome |
| `matchHistory[].matchurls[]` | Replay URLs, including full and summary files |
| `profiles[]` | Player metadata keyed by `profile_id` |

For AoE4, `options` and `slotinfo` are critical because the top-level
`mapname` may be only `generated_map`.

## Replay Summary Files

Use this endpoint to request signed replay file URLs:

```text
GET https://aoe-api.worldsedgelink.com/community/leaderboard/getReplayFiles?title=age4&matchIDs=[237506997]
```

URL-encoded:

```text
https://aoe-api.worldsedgelink.com/community/leaderboard/getReplayFiles?title=age4&matchIDs=%5B237506997%5D
```

The response contains `replayFiles[]`.

| Field | Meaning |
|---|---|
| `profile_id` | Perspective/uploader profile id |
| `matchhistory_id` | Match id |
| `url` | Signed temporary download URL |
| `size` | File size in bytes; ignore `-1` |
| `datatype` | `0` = full replay, `1` = summary file |

To download summary files, filter to:

```python
item["datatype"] == 1 and item["size"] > 0
```

The signed URLs expire. The top-level `expiryUnix` says when the current URLs
expire. Refresh with another `getReplayFiles` call when needed.

Example downloader:

```python
import json
import pathlib
import urllib.request

match_id = 237506997
out_dir = pathlib.Path("data/replays/summaries") / str(match_id)
out_dir.mkdir(parents=True, exist_ok=True)

ua = "AOE4ReplayHarvest/0.1 (contact@example.com)"
api = (
    "https://aoe-api.worldsedgelink.com/community/leaderboard/getReplayFiles"
    f"?title=age4&matchIDs=%5B{match_id}%5D"
)

req = urllib.request.Request(api, headers={"User-Agent": ua})
with urllib.request.urlopen(req, timeout=30) as resp:
    payload = json.loads(resp.read().decode("utf-8"))

for item in payload.get("replayFiles", []):
    if item.get("datatype") != 1 or item.get("size", -1) <= 0:
        continue

    profile_id = item["profile_id"]
    path = out_dir / f"M_{match_id}_profile_{profile_id}_summary.gz"
    req = urllib.request.Request(item["url"], headers={"User-Agent": ua})
    with urllib.request.urlopen(req, timeout=60) as resp:
        path.write_bytes(resp.read())
```

## Patch and Build from Replay URL

Replay URLs include the build and patch in the path:

```text
.../age4/replay/windows/4.0.0/10604/M_237506997_...gz
```

For the example above:

| Value | Meaning |
|---|---|
| `4.0.0` | Replay platform/build path segment |
| `10604` | Patch/build id useful for the model DB |

This is useful because `getMatchHistory` does not expose a direct `patch`
field.

## Decoding `options`

`options` is base64-encoded zlib-compressed JSON:

```python
import base64
import json
import zlib

options_json = zlib.decompress(base64.b64decode(match["options"]))
options = json.loads(options_json.rstrip(b"\x00").decode("utf-8"))
```

Useful fields seen in AoE4:

| Field | Meaning |
|---|---|
| `mapName` | Real map key, e.g. `forest_mountain` |
| `localizedMapName` | Localized display map name, e.g. `茂密树林` |
| `mapGenLayout` | Numeric generated-map layout id |
| `mapGenBio` | Numeric biome id |
| `isUsingRandomMap` | Random-map flag |

For match `237506997`, decoded `options` showed:

```json
{
  "mapName": "forest_mountain",
  "localizedMapName": "茂密树林",
  "isUsingRandomMap": 0
}
```

## Decoding `slotinfo`

`slotinfo` is also base64-encoded zlib-compressed data. The decompressed string
starts with a prefix like `12,` followed by a JSON array and may have a trailing
null byte.

```python
import base64
import json
import zlib

slot_text = zlib.decompress(base64.b64decode(match["slotinfo"])).decode(
    "utf-8",
    errors="replace",
)
slots = json.loads(slot_text[slot_text.find("[") : slot_text.rfind("]") + 1])

for slot in slots:
    if slot.get("profileInfo.id", -1) == -1:
        continue

    metadata_text = base64.b64decode(slot["metaData"]).decode(
        "utf-8",
        errors="replace",
    )
    metadata_text = metadata_text[
        metadata_text.find("{") : metadata_text.rfind("}") + 1
    ]
    metadata = json.loads(metadata_text)

    profile_id = slot["profileInfo.id"]
    race_id = slot["raceID"]
    random_civ = bool(metadata.get("m_isRaceRandomlySelected"))
```

Useful `slotinfo` fields:

| Field | Meaning |
|---|---|
| `profileInfo.id` | Player profile id |
| `teamID` | Team id |
| `raceID` | Civilization id |
| `metaData.m_isRaceRandomlySelected` | Random-civ flag |
| `metaData.m_randomStartPosition` | Random start position flag |
| `metaData.m_inputDeviceType` | Input device type |
| `metaData.m_hardwareType` | Hardware type |

For match `237506997`, both players had:

```text
m_isRaceRandomlySelected = 0
raceID = 131384
```

So the game was French vs French with no random-civ selection.

## Official Age Site `GetMatchDetail`

The official stats endpoint is useful as a lightweight fallback for player names,
civilization names, win/loss, and replay availability:

```text
POST https://api.ageofempires.com/api/GameStats/AgeIV/GetMatchDetail
Content-Type: application/json

{"matchId":237506997,"game":"age4","profileId":25124006}
```

It returned the fresh match quickly, but it lacks several model-critical fields:

| Missing or weak field | Better source |
|---|---|
| Patch | Replay URL path from `getReplayFiles` / `matchurls` |
| Real map key | Decoded `options.mapName` |
| Rating | `getMatchHistory.matchhistorymember[]` |
| Random civ | Decoded `slotinfo.metaData.m_isRaceRandomlySelected` |

## Verified Fresh Match Example

For `match_id = 237506997`, `profile_id = 25124006`, queried on 2026-06-10:

| Field | Value |
|---|---|
| Started UTC | `2026-06-10T11:03:59+00:00` |
| Duration | `1000` seconds |
| Real map | `forest_mountain` |
| Localized map | `茂密树林` |
| Patch path segment | `10604` |
| Player 25124006 | French, win, `1946 -> 1959` |
| Player 18282246 | French, loss, `1899 -> 1886` |
| Random civ | false for both players |

