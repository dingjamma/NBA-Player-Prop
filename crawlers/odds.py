"""
Fetch NBA player prop lines from The Odds API.
Sign up at https://the-odds-api.com/ — free tier: 500 req/month.

Markets crawled:
  player_points, player_rebounds, player_assists,
  player_threes, player_blocks, player_steals
"""

import os
import requests
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv

from ingestion.s3 import upload_parquet

load_dotenv()

API_KEY = os.getenv("ODDS_API_KEY")
BASE_URL = "https://api.the-odds-api.com/v4"
SPORT = "basketball_nba"
SPURS_KEYWORDS = ["san antonio", "spurs"]

PROP_MARKETS = [
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_blocks",
    "player_steals",
]

# Books to pull lines from (pick the ones you have access to)
BOOKMAKERS = "draftkings,fanduel,betmgm,pinnacle"


def fetch_events() -> list[dict]:
    """Get tomorrow's NBA event IDs."""
    url = f"{BASE_URL}/sports/{SPORT}/events"
    resp = requests.get(url, params={"apiKey": API_KEY}, timeout=15)
    resp.raise_for_status()
    print(f"  Odds API requests remaining: {resp.headers.get('x-requests-remaining')}")
    return resp.json()


def fetch_props(event_id: str) -> list[dict]:
    """Fetch player prop lines for a specific game."""
    url = f"{BASE_URL}/sports/{SPORT}/events/{event_id}/odds"
    params = {
        "apiKey": API_KEY,
        "regions": "us",
        "markets": ",".join(PROP_MARKETS),
        "bookmakers": BOOKMAKERS,
        "oddsFormat": "american",
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        print(f"  [WARN] event {event_id}: {e}")
        return {}


def parse_props(event_data: dict) -> list[dict]:
    rows = []
    game_id = event_data.get("id")
    home = event_data.get("home_team")
    away = event_data.get("away_team")
    commence = event_data.get("commence_time")

    for bm in event_data.get("bookmakers", []):
        book = bm["key"]
        for market in bm.get("markets", []):
            market_key = market["key"]
            for outcome in market.get("outcomes", []):
                rows.append({
                    "game_id": game_id,
                    "home_team": home,
                    "away_team": away,
                    "commence_time": commence,
                    "bookmaker": book,
                    "market": market_key,
                    "player_name": outcome.get("description"),
                    "side": outcome["name"],        # Over / Under
                    "line": outcome.get("point"),   # e.g. 24.5
                    "odds": outcome.get("price"),   # american odds
                })
    return rows


def run() -> pd.DataFrame:
    if not API_KEY:
        print("[ERROR] ODDS_API_KEY not set in .env")
        return pd.DataFrame()

    print("Fetching NBA events...")
    events = fetch_events()
    print(f"  {len(events)} events found")

    all_rows = []
    for event in events:
        home = event.get("home_team", "").lower()
        away = event.get("away_team", "").lower()
        if not any(kw in home or kw in away for kw in SPURS_KEYWORDS):
            continue
        print(f"  Spurs game found: {event['home_team']} vs {event['away_team']}")
        data = fetch_props(event["id"])
        if data:
            all_rows.extend(parse_props(data))

    df = pd.DataFrame(all_rows)
    print(f"  Total prop lines: {len(df)}")

    if df.empty:
        return df

    date_str = datetime.now().strftime("%Y_%m_%d")
    import tempfile, pathlib
    tmp = pathlib.Path(tempfile.mktemp(suffix=".parquet"))
    df.to_parquet(tmp, index=False)
    upload_parquet(tmp, f"raw/odds/date={date_str}/props.parquet")
    print(f"  Uploaded prop lines -> S3")

    return df


if __name__ == "__main__":
    run()
