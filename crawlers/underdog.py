"""
Fetch active NBA player prop lines from Underdog Fantasy.
No API key required — public endpoint.

Supported stats: Points, Rebounds, Assists, 3-Pointers Made
(STL and BLK are not offered by Underdog)

Run:
    python -m crawlers.underdog
"""

import requests
import pandas as pd
from datetime import datetime

from ingestion.s3 import upload_parquet
from config import PLAYERS

UD_API     = "https://api.underdogfantasy.com/v1/over_under_lines"
UD_HEADERS = {"User-Agent": "Mozilla/5.0"}

# Underdog display names → our internal stat keys
STAT_COL: dict[str, str] = {
    "Points":          "PTS",
    "Rebounds":        "REB",
    "Assists":         "AST",
    "3-Pointers Made": "FG3M",
}

# Last names of tracked players for quick filtering
_TRACKED_LAST_NAMES = {p["name"].split()[-1].lower() for p in PLAYERS.values()}


def fetch_lines() -> pd.DataFrame:
    """Fetch all active NBA Underdog Fantasy lines. Returns a DataFrame."""
    r = requests.get(UD_API, headers=UD_HEADERS, timeout=15)
    r.raise_for_status()
    data = r.json()

    players     = {p["id"]: p for p in data.get("players", [])}
    appearances = {a["id"]: a for a in data.get("appearances", [])}
    games       = {g["id"]: g for g in data.get("games", [])}

    rows = []
    for line in data.get("over_under_lines", []):
        if line.get("status") != "active":
            continue

        ou       = line.get("over_under", {})
        app_stat = ou.get("appearance_stat", {})
        stat_raw = app_stat.get("display_stat", "")
        if stat_raw not in STAT_COL:
            continue

        line_val = line.get("stat_value")
        if line_val is None:
            continue

        app_id = app_stat.get("appearance_id", "")
        app    = appearances.get(app_id, {})
        player = players.get(app.get("player_id", ""), {})

        if player.get("sport_id") != "NBA":
            continue

        name = f"{player.get('first_name', '')} {player.get('last_name', '')}".strip()
        if not name:
            continue

        match_id  = app.get("match_id")
        game      = games.get(match_id, {})
        team_id   = app.get("team_id", "")
        is_home   = game.get("home_team_id") == team_id
        game_abbr = game.get("abbreviated_title", "")

        rows.append({
            "name":     name,
            "game":     game_abbr,
            "is_home":  is_home,
            "stat":     stat_raw,
            "line":     float(line_val),
            "match_id": match_id,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # One row per player+stat (Underdog sometimes has alternate lines — take middle)
    df = (
        df.groupby(["name", "stat"])
        .apply(lambda g: g.iloc[len(g) // 2])
        .reset_index(drop=True)
    )
    return df


def run() -> pd.DataFrame:
    print("Fetching Underdog Fantasy lines...")
    df = fetch_lines()

    if df.empty:
        print("  No lines found.")
        return df

    print(f"  {len(df)} lines across {df['name'].nunique()} players")

    # Show lines for our tracked players
    tracked = df[df["name"].str.split().str[-1].str.lower().isin(_TRACKED_LAST_NAMES)]
    if not tracked.empty:
        print(f"  Tracked players with lines: {tracked['name'].unique().tolist()}")

    import tempfile
    from pathlib import Path
    date_str = datetime.now().strftime("%Y_%m_%d")
    tmp = Path(tempfile.mktemp(suffix=".parquet"))
    df.to_parquet(tmp, index=False)
    upload_parquet(tmp, f"raw/odds/date={date_str}/props.parquet")
    print(f"  Saved -> data/raw/odds/date={date_str}/props.parquet")

    return df


if __name__ == "__main__":
    run()
