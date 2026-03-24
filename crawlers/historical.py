"""
One-time crawl: Victor Wembanyama game logs via ESPN's public API.
No API key needed.

Run once:
    python -m crawlers.historical
"""

import time
from pathlib import Path

import requests
import pandas as pd

from ingestion.s3 import upload_parquet

WEMBY_ESPN_ID = 5104157
LOCAL_OUT     = Path("data/raw/game_logs")

# ESPN uses the ending year of the season (2026 = 2025-26).
# Dynamically resolve so this never needs to be updated manually.
def _current_espn_seasons() -> list[int]:
    from datetime import date
    today = date.today()
    # NBA season crosses calendar years: new season starts in Oct
    current = today.year if today.month <= 9 else today.year + 1
    return [current, current - 1]

SEASONS = _current_espn_seasons()

ESPN_GAMELOG  = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{player_id}/gamelog"


def fetch_gamelog(player_id: int, season: int) -> pd.DataFrame:
    resp = requests.get(
        ESPN_GAMELOG.format(player_id=player_id),
        params={"season": season},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()

    stat_names = data.get("names", [])
    events_meta = data.get("events", {})

    # Collect stats per event from all season type categories
    ALLOWED_SEASON_TYPES = {"regular season", "playoffs", "postseason"}

    stats_map = {}
    for stype in data.get("seasonTypes", []):
        display = stype.get("displayName", "").lower()
        if not any(a in display for a in ALLOWED_SEASON_TYPES):
            continue  # skip preseason, All-Star, international exhibitions
        for cat in stype.get("categories", []):
            for entry in cat.get("events", []):
                eid  = str(entry.get("eventId"))
                vals = entry.get("stats", [])
                stats_map[eid] = dict(zip(stat_names, vals))

    rows = []
    for eid, stats in stats_map.items():
        meta = events_meta.get(eid, {})
        game_date = meta.get("gameDate", "")[:10]
        if not game_date:
            continue

        def split_stat(key):
            """Handle combined stats like '5-7' -> return first number."""
            val = stats.get(key, None)
            if val is None:
                return None
            if isinstance(val, str) and "-" in val:
                return pd.to_numeric(val.split("-")[0], errors="coerce")
            return pd.to_numeric(val, errors="coerce")

        rows.append({
            "GAME_ID":    eid,
            "GAME_DATE":  game_date,
            "OPP":        meta.get("opponent", {}).get("abbreviation", ""),
            "HOME_AWAY":  "away" if meta.get("atVs") == "@" else "home",
            "WL":         meta.get("gameResult", ""),
            "MIN":        stats.get("minutes"),
            "PTS":        pd.to_numeric(stats.get("points"), errors="coerce"),
            "REB":        pd.to_numeric(stats.get("totalRebounds"), errors="coerce"),
            "AST":        pd.to_numeric(stats.get("assists"), errors="coerce"),
            "STL":        pd.to_numeric(stats.get("steals"), errors="coerce"),
            "BLK":        pd.to_numeric(stats.get("blocks"), errors="coerce"),
            "FGM":        split_stat("fieldGoalsMade-fieldGoalsAttempted"),
            "FGA":        pd.to_numeric(stats.get("fieldGoalsAttempted") or (stats.get("fieldGoalsMade-fieldGoalsAttempted") or "").split("-")[-1] if stats.get("fieldGoalsMade-fieldGoalsAttempted") else None, errors="coerce"),
            "FG3M":       split_stat("threePointFieldGoalsMade-threePointFieldGoalsAttempted"),
            "FG3A":       pd.to_numeric((stats.get("threePointFieldGoalsMade-threePointFieldGoalsAttempted") or "").split("-")[-1] if stats.get("threePointFieldGoalsMade-threePointFieldGoalsAttempted") else None, errors="coerce"),
            "FTM":        split_stat("freeThrowsMade-freeThrowsAttempted"),
            "FTA":        pd.to_numeric((stats.get("freeThrowsMade-freeThrowsAttempted") or "").split("-")[-1] if stats.get("freeThrowsMade-freeThrowsAttempted") else None, errors="coerce"),
            "TOV":        pd.to_numeric(stats.get("turnovers"), errors="coerce"),
            "PF":         pd.to_numeric(stats.get("fouls"), errors="coerce"),
            "PLAYER_ID":   str(player_id),
            "PLAYER_NAME": "Victor Wembanyama",
            "SEASON":      f"{season-1}-{str(season)[2:]}",
        })

    df = pd.DataFrame(rows).sort_values("GAME_DATE").reset_index(drop=True)
    return df


def run():
    LOCAL_OUT.mkdir(parents=True, exist_ok=True)

    for season in SEASONS:
        season_str = f"{season-1}-{str(season)[2:]}"
        print(f"\n=== Season {season_str} ===")
        try:
            df = fetch_gamelog(WEMBY_ESPN_ID, season)
            print(f"  Fetched {len(df)} games")
            if df.empty:
                print("  No data, skipping.")
                continue
            print(f"  Sample: {df[['GAME_DATE','PTS','REB','AST']].tail(3).to_string()}")
        except Exception as e:
            import traceback; traceback.print_exc()
            print(f"  [ERROR] {e}")
            continue

        local_path = LOCAL_OUT / f"wemby_{season_str.replace('-', '_')}.parquet"
        df.to_parquet(local_path, index=False)
        print(f"  Saved -> {local_path}")

        s3_key = f"raw/game_logs/player=wembanyama/season={season_str}/data.parquet"
        upload_parquet(local_path, s3_key)
        print(f"  Uploaded -> s3://{s3_key}")

        time.sleep(2)


if __name__ == "__main__":
    run()
