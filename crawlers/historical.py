"""
Fetch historical game logs from stats.nba.com for all tracked players.
No API key needed — requires browser-like headers.

Run (all players):
    python -m crawlers.historical

Run (one player):
    python -m crawlers.historical wembanyama
"""

import sys
import time
from datetime import date
from pathlib import Path

import requests
import pandas as pd

from ingestion.s3 import upload_parquet
from config import PLAYERS

LOCAL_OUT = Path("data/raw/game_logs")

NBA_HEADERS = {
    "Host":               "stats.nba.com",
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "Accept-Encoding":    "gzip, deflate, br",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
    "Referer":            "https://stats.nba.com/",
    "Connection":         "keep-alive",
}

GAMELOG_URL = "https://stats.nba.com/stats/playergamelog"

KEEP_COLS = [
    "GAME_ID", "GAME_DATE", "MATCHUP", "WL", "MIN",
    "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
    "OREB", "DREB", "REB", "AST", "STL", "BLK",
    "TOV", "PF", "PTS", "PLUS_MINUS",
    "PLAYER_ID", "PLAYER_NAME", "SEASON",
]


def _current_seasons() -> list[str]:
    today = date.today()
    current_end = today.year if today.month <= 9 else today.year + 1
    return [
        f"{end - 1}-{str(end)[2:]}"
        for end in [current_end, current_end - 1, current_end - 2]
    ]


def fetch_gamelog(player_id: int, player_name: str, season: str) -> pd.DataFrame:
    """Fetch one season of game logs for a player from stats.nba.com."""
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
    result = resp.json()["resultSets"][0]
    rows   = result["rowSet"]

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows, columns=result["headers"])
    df = df.rename(columns={"Player_ID": "PLAYER_ID", "Game_ID": "GAME_ID"})
    df["GAME_DATE"]   = pd.to_datetime(df["GAME_DATE"]).dt.strftime("%Y-%m-%d")
    df["PLAYER_NAME"] = player_name
    df["PLAYER_ID"]   = player_id
    df["SEASON"]      = season

    df = df[[c for c in KEEP_COLS if c in df.columns]]
    return df.sort_values("GAME_DATE").reset_index(drop=True)


def run_player(player_key: str) -> None:
    """Fetch and save all seasons for one player."""
    cfg = PLAYERS[player_key]
    player_id   = cfg["nba_id"]
    player_name = cfg["name"]
    seasons     = _current_seasons()

    print(f"\n{'='*50}")
    print(f"  {player_name} ({player_key})")
    print(f"{'='*50}")

    LOCAL_OUT.mkdir(parents=True, exist_ok=True)

    for season in seasons:
        print(f"  Season {season}...")
        try:
            df = fetch_gamelog(player_id, player_name, season)
        except Exception as e:
            print(f"    [ERROR] {e}")
            time.sleep(2)
            continue

        if df.empty:
            print("    No data.")
            continue

        print(f"    {len(df)} games  |  last: {df['GAME_DATE'].iloc[-1]}")

        season_tag = season.replace("-", "_")
        local_path = LOCAL_OUT / f"{player_key}_{season_tag}.parquet"
        df.to_parquet(local_path, index=False)

        s3_key = f"raw/game_logs/player={player_key}/season={season}/data.parquet"
        upload_parquet(local_path, s3_key)
        print(f"    Saved -> data/{s3_key}")

        time.sleep(1)  # be polite to stats.nba.com


def run(player_keys: list[str] | None = None) -> None:
    """Fetch game logs for all (or specified) tracked players."""
    targets = player_keys or list(PLAYERS.keys())
    print(f"Fetching game logs for: {targets}")
    for key in targets:
        if key not in PLAYERS:
            print(f"[WARN] Unknown player key '{key}' — skipping")
            continue
        run_player(key)
    print("\nDone.")


if __name__ == "__main__":
    keys = sys.argv[1:] or None
    run(keys)
