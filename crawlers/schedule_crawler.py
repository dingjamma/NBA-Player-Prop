"""
Crawl today's NBA schedule via ESPN's public API.
Filters for games that include any of our 5 tracked players' teams.
No API key needed.
"""

import time
from datetime import date

import requests
import pandas as pd

from ingestion.s3 import upload_parquet
from config import TRACKED_TEAMS

ESPN_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"


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

                # Keep game if any tracked team is playing
                if not TRACKED_TEAMS.intersection(teams.keys()):
                    continue

                home = next((c for c in comps["competitors"] if c["homeAway"] == "home"), {})
                away = next((c for c in comps["competitors"] if c["homeAway"] == "away"), {})
                rows.append({
                    "GAME_ID":            event.get("id"),
                    "GAME_DATE":          target_date.isoformat(),
                    "HOME_TEAM":          home.get("team", {}).get("displayName", ""),
                    "HOME_TEAM_ABV":      home.get("team", {}).get("abbreviation", ""),
                    "VISITOR_TEAM":       away.get("team", {}).get("displayName", ""),
                    "VISITOR_TEAM_ABV":   away.get("team", {}).get("abbreviation", ""),
                    "STATUS":             event.get("status", {}).get("type", {}).get("description", ""),
                })
            return pd.DataFrame(rows)
        except Exception as e:
            print(f"[ERROR] fetch_games attempt {attempt}/{retries}: {e}")
            if attempt < retries:
                time.sleep(5 * attempt)
    return pd.DataFrame()


def run() -> pd.DataFrame:
    target = date.today()
    print(f"Fetching schedule for {target} (tracked teams: {sorted(TRACKED_TEAMS)})")

    games = fetch_games(target)
    if games.empty:
        print("No tracked players have games today.")
        return games

    print(f"Found {len(games)} game(s) with tracked players")

    date_str = target.strftime("%Y_%m_%d")
    import tempfile
    from pathlib import Path
    tmp = Path(tempfile.mktemp(suffix=".parquet"))
    games.to_parquet(tmp, index=False)
    upload_parquet(tmp, f"raw/schedule/date={date_str}/games.parquet")
    print(f"Saved -> data/raw/schedule/date={date_str}/games.parquet")

    return games


if __name__ == "__main__":
    run()
