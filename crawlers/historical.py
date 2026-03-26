"""
Crawl Victor Wembanyama game logs via stats.nba.com.
No API key needed — just requires browser-like headers.

Run:
    python -m crawlers.historical
"""

import time
from datetime import date
from pathlib import Path

import requests
import pandas as pd

from ingestion.s3 import upload_parquet

WEMBY_NBA_ID = 1641705
LOCAL_OUT    = Path("data/raw/game_logs")

NBA_HEADERS = {
    "Host":              "stats.nba.com",
    "User-Agent":        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":            "application/json, text/plain, */*",
    "Accept-Language":   "en-US,en;q=0.9",
    "Accept-Encoding":   "gzip, deflate, br",
    "x-nba-stats-origin":"stats",
    "x-nba-stats-token": "true",
    "Referer":           "https://stats.nba.com/",
    "Connection":        "keep-alive",
}

GAMELOG_URL = "https://stats.nba.com/stats/playergamelog"


def _current_seasons() -> list[str]:
    today = date.today()
    current_end = today.year if today.month <= 9 else today.year + 1
    seasons = []
    for end in [current_end, current_end - 1, current_end - 2]:
        seasons.append(f"{end-1}-{str(end)[2:]}")
    return seasons


def fetch_gamelog(player_id: int, season: str) -> pd.DataFrame:
    """Fetch game log from stats.nba.com for one season."""
    resp = requests.get(
        GAMELOG_URL,
        headers=NBA_HEADERS,
        params={
            "PlayerID":   player_id,
            "Season":     season,
            "SeasonType": "Regular Season",
        },
        timeout=30,
    )
    resp.raise_for_status()
    data     = resp.json()
    result   = data["resultSets"][0]
    cols     = result["headers"]
    rows     = result["rowSet"]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=cols)

    # Normalize to our schema
    df = df.rename(columns={"Player_ID": "PLAYER_ID", "Game_ID": "GAME_ID"})
    df["GAME_DATE"]   = pd.to_datetime(df["GAME_DATE"]).dt.strftime("%Y-%m-%d")
    df["PLAYER_NAME"] = "Victor Wembanyama"
    df["SEASON"]      = season

    keep = ["GAME_ID", "GAME_DATE", "MATCHUP", "WL", "MIN",
            "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
            "OREB", "DREB", "REB", "AST", "STL", "BLK",
            "TOV", "PF", "PTS", "PLUS_MINUS",
            "PLAYER_ID", "PLAYER_NAME", "SEASON"]
    df = df[[c for c in keep if c in df.columns]]
    df = df.sort_values("GAME_DATE").reset_index(drop=True)
    return df


def run():
    LOCAL_OUT.mkdir(parents=True, exist_ok=True)
    seasons = _current_seasons()

    for season in seasons:
        print(f"\n=== Season {season} ===")
        try:
            df = fetch_gamelog(WEMBY_NBA_ID, season)
            print(f"  Fetched {len(df)} games")
            if df.empty:
                print("  No data, skipping.")
                continue
            print(f"  Sample:\n{df[['GAME_DATE','PTS','REB','AST','PLUS_MINUS']].tail(3).to_string()}")
        except Exception as e:
            import traceback; traceback.print_exc()
            print(f"  [ERROR] {e}")
            continue

        season_tag   = season.replace("-", "_")
        local_path   = LOCAL_OUT / f"wemby_{season_tag}.parquet"
        df.to_parquet(local_path, index=False)
        print(f"  Saved -> {local_path}")

        s3_key = f"raw/game_logs/player=wembanyama/season={season}/data.parquet"
        upload_parquet(local_path, s3_key)
        print(f"  Uploaded -> s3://{s3_key}")

        time.sleep(1)


if __name__ == "__main__":
    run()
