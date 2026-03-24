"""
Crawl today's NBA schedule via ESPN's public API.
Filters for San Antonio Spurs games only.
No API key needed.
"""

import time
from datetime import date

import requests
import pandas as pd

from ingestion.s3 import upload_parquet

ESPN_URL  = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"
SPURS_ABV = "SA"


def get_today() -> date:
    return date.today()


def fetch_games(target_date: date, retries: int = 3) -> pd.DataFrame:
    date_str = target_date.strftime("%Y%m%d")
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(ESPN_URL, params={"dates": date_str}, timeout=30)
            resp.raise_for_status()
            events = resp.json().get("events", [])

            rows = []
            for event in events:
                comps = event.get("competitions", [{}])[0]
                teams = {c["team"]["abbreviation"]: c for c in comps.get("competitors", [])}
                if SPURS_ABV not in teams:
                    continue
                home = next((c for c in comps["competitors"] if c["homeAway"] == "home"), {})
                away = next((c for c in comps["competitors"] if c["homeAway"] == "away"), {})
                rows.append({
                    "GAME_ID":         event.get("id"),
                    "GAME_DATE":       target_date.isoformat(),
                    "HOME_TEAM":       home.get("team", {}).get("displayName", ""),
                    "HOME_TEAM_ABV":   home.get("team", {}).get("abbreviation", ""),
                    "VISITOR_TEAM":    away.get("team", {}).get("displayName", ""),
                    "VISITOR_TEAM_ABV": away.get("team", {}).get("abbreviation", ""),
                    "STATUS":          event.get("status", {}).get("type", {}).get("description", ""),
                })
            return pd.DataFrame(rows)
        except Exception as e:
            print(f"[ERROR] fetch_games attempt {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(5 * attempt)
    return pd.DataFrame()


def run():
    target = get_today()
    print(f"Fetching Spurs schedule for {target}")

    games = fetch_games(target)
    if games.empty:
        print("No Spurs games today.")
        return games

    print(f"Found {len(games)} Spurs game(s)")
    date_str = target.strftime("%Y_%m_%d")

    s3_key = f"raw/schedule/date={date_str}/games.parquet"
    import tempfile, pathlib
    tmp = pathlib.Path(tempfile.mktemp(suffix=".parquet"))
    games.to_parquet(tmp, index=False)
    upload_parquet(tmp, s3_key)
    print(f"Uploaded -> s3://{s3_key}")

    return games


if __name__ == "__main__":
    run()
