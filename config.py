"""
Central config — tracked players and stat targets.
"""

PLAYERS: dict[str, dict] = {
    "wembanyama": {
        "name":   "Victor Wembanyama",
        "nba_id": 1641705,
        "team":   "SA",
    },
    "jokic": {
        "name":   "Nikola Jokic",
        "nba_id": 203999,
        "team":   "DEN",
    },
    "doncic": {
        "name":   "Luka Doncic",
        "nba_id": 1629029,
        "team":   "LAL",
    },
    "sga": {
        "name":   "Shai Gilgeous-Alexander",
        "nba_id": 1628983,
        "team":   "OKC",
    },
    "giannis": {
        "name":   "Giannis Antetokounmpo",
        "nba_id": 203507,
        "team":   "MIL",
    },
}

# All team abbreviations we care about (used by schedule crawler)
TRACKED_TEAMS: set[str] = {p["team"] for p in PLAYERS.values()}

STAT_TARGETS: list[str] = ["PTS", "REB", "AST", "STL", "BLK", "FG3M"]
