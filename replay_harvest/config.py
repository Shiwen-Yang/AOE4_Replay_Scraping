from pathlib import Path
import os


BASE_DIR = Path(__file__).resolve().parent.parent
DB_PATH = BASE_DIR / "aoe4.duckdb"
REPLAY_DATA_DIR = BASE_DIR / "data" / "replays"
RAW_REPLAY_DIR = REPLAY_DATA_DIR / "raw"
PARSED_REPLAY_DIR = REPLAY_DATA_DIR / "parsed"
REPORT_DIR = REPLAY_DATA_DIR / "reports"
AOE4_PARSING_REPO = BASE_DIR.parent / "AOE4_Parsing"
AOE4_PARSING_CLI_PROJECT = AOE4_PARSING_REPO / "src" / "AoE4ReplayParser.Cli"

REPLAY_DOWNLOAD_URL = (
    "https://api.ageofempires.com/api/GameStats/AgeIV/GetMatchReplay/"
    "?matchId={game_id}&profileId={profile_id}"
)

SAMPLE_GROUP_BALANCED = "balanced_10k"
SAMPLE_GROUP_TOP100 = "top100_complete"
SAMPLE_GROUP_RECENT = "recent_rm_1v1"
DEFAULT_PARSER_VERSION = "aoe4_parser_cli"

RATING_BUCKETS = [
    ("low", None, 729),
    ("mid_low", 729, 904),
    ("mid", 904, 1072),
    ("high", 1072, 1400),
    ("elite", 1400, None),
]

# These endpoints are configurable because AoE4World API response shapes can
# change. The parser in top_players.py accepts a few common shapes.
AOE4WORLD_LEADERBOARD_URL = "https://aoe4world.com/api/v0/leaderboards/rm_solo?page={page}"
AOE4WORLD_PLAYER_URL = "https://aoe4world.com/api/v0/players/{profile_id}"

DEFAULT_USER_AGENT = "AOE4ReplayHarvest/0.1 (yangshw0223@gmail.com; Discord: shiweny#8556)"
USER_AGENT = os.environ.get("AOE4_REPLAY_HARVEST_USER_AGENT", DEFAULT_USER_AGENT)
