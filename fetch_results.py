"""
Fetch actual game results and log vs AI predictions + Vegas lines.

Runs after games end (schedule via Task Scheduler at 11pm MT).
Appends one row per stat to: data/results/results.csv

Columns: date, opponent, stat, ai_pred, vegas_line, ai_pick, actual, ai_correct
"""

import sys
import os
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from datetime import date, timedelta
import pandas as pd
import requests
from dotenv import load_dotenv
load_dotenv()

STATS      = ["PTS", "REB", "AST", "STL", "BLK", "FG3M"]
WEMBY_ID   = "5104157"
RESULTS_PATH = Path("data/results/results.csv")

MARKET_TO_STAT = {
    "player_points":   "PTS",
    "player_rebounds": "REB",
    "player_assists":  "AST",
    "player_steals":   "STL",
    "player_blocks":   "BLK",
    "player_threes":   "FG3M",
}


def _current_espn_season() -> int:
    today = date.today()
    return today.year if today.month <= 9 else today.year + 1


def fetch_actual_stats(game_date: date) -> dict | None:
    """Pull Wemby's box score for game_date from ESPN."""
    season = _current_espn_season()
    url = (
        f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba"
        f"/athletes/{WEMBY_ID}/gamelog?season={season}"
    )
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  [results] ESPN fetch failed: {e}")
        return None

    date_str = game_date.isoformat()  # "2026-03-23"

    # Walk through events
    events     = data.get("events", {})
    categories = data.get("seasonTypes", [])

    # Flatten all stat labels
    labels = []
    for st in categories:
        for cat in st.get("categories", []):
            for name in cat.get("names", []):
                labels.append(name)
        break  # first season type has the label list

    for st in categories:
        for cat in st.get("categories", []):
            for event in cat.get("events", []):
                ev_id  = str(event.get("id", ""))
                ev_date = events.get(ev_id, {}).get("gameDate", "")[:10]
                if ev_date != date_str:
                    continue
                stats_raw = event.get("stats", [])
                row = {}
                for i, val in enumerate(stats_raw):
                    if i < len(labels):
                        row[labels[i]] = val
                if row:
                    return _parse_stats(row)

    print(f"  [results] No game found for {date_str} in ESPN gamelog.")
    return None


def _parse_stats(raw: dict) -> dict:
    """Map ESPN stat keys → our keys."""
    def _f(key: str) -> float | None:
        v = raw.get(key)
        try:
            return float(v) if v not in (None, "--", "") else None
        except Exception:
            return None

    return {
        "PTS":  _f("PTS"),
        "REB":  _f("REB"),
        "AST":  _f("AST"),
        "STL":  _f("STL"),
        "BLK":  _f("BLK"),
        "FG3M": _f("3PM"),
    }


def load_predictions(game_date: date) -> pd.DataFrame:
    dk = game_date.strftime("%Y_%m_%d")
    p = Path(f"data/processed/predictions/date={dk}/predictions.parquet")
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def load_odds(game_date: date) -> pd.DataFrame:
    dk = game_date.strftime("%Y_%m_%d")
    p = Path(f"data/raw/odds/date={dk}/props.parquet")
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def get_vegas_lines(odds: pd.DataFrame) -> dict:
    lines = {}
    for market, stat in MARKET_TO_STAT.items():
        rows = odds[odds["market"] == market] if not odds.empty else pd.DataFrame()
        if rows.empty:
            continue
        over_rows = rows[rows["side"].str.lower() == "over"]
        if over_rows.empty:
            continue
        lines[stat] = round(float(over_rows["line"].median()), 1)
    return lines


def get_opponent(odds: pd.DataFrame) -> str:
    if odds.empty:
        return "UNK"
    row = odds.iloc[0]
    home = str(row.get("home_team", "")).upper()
    away = str(row.get("away_team", "")).upper()
    spurs_home = "SAN ANTONIO" in home or "SPURS" in home
    opp_raw = away if spurs_home else home
    parts = opp_raw.strip().split()
    return parts[-1] if parts else opp_raw


def append_results(rows: list[dict]) -> None:
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(rows)
    if RESULTS_PATH.exists():
        existing = pd.read_csv(RESULTS_PATH)
        # Deduplicate by date + stat
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["date", "stat"], keep="last")
        combined.to_csv(RESULTS_PATH, index=False)
    else:
        new_df.to_csv(RESULTS_PATH, index=False)
    print(f"  [results] Saved to {RESULTS_PATH}")


def run(game_date: date | None = None) -> None:
    if game_date is None:
        game_date = date.today()

    print(f"\n[results] Fetching results for {game_date}...")

    preds = load_predictions(game_date)
    odds  = load_odds(game_date)

    if preds.empty:
        print("  [results] No predictions found — skipping.")
        return

    pred_row    = preds.iloc[0]
    vegas_lines = get_vegas_lines(odds)
    opponent    = get_opponent(odds)
    actual      = fetch_actual_stats(game_date)

    if actual is None:
        print("  [results] Could not fetch actual stats.")
        return

    rows = []
    for stat in STATS:
        ai_pred   = pred_row.get(f"pred_{stat.lower()}")
        vegas_line = vegas_lines.get(stat)
        act_val   = actual.get(stat)
        ai_pick   = None
        ai_correct = None

        if ai_pred is not None and vegas_line is not None:
            ai_pick = "OVER" if float(ai_pred) > vegas_line else "UNDER"

        if ai_pick is not None and act_val is not None and vegas_line is not None:
            hit_over  = act_val > vegas_line
            ai_correct = (ai_pick == "OVER" and hit_over) or (ai_pick == "UNDER" and not hit_over)

        rows.append({
            "date":       game_date.isoformat(),
            "opponent":   opponent,
            "stat":       stat,
            "ai_pred":    round(float(ai_pred), 2) if ai_pred is not None else None,
            "vegas_line": vegas_line,
            "ai_pick":    ai_pick,
            "actual":     act_val,
            "ai_correct": ai_correct,
        })

        status = "CORRECT" if ai_correct else ("WRONG" if ai_correct is False else "N/A")
        print(f"  {stat:5s}  AI={ai_pred or '?':>5}  Vegas={vegas_line or '?':>5}  "
              f"Actual={act_val or '?':>5}  Pick={ai_pick or '?':>6}  [{status}]")

    append_results(rows)


if __name__ == "__main__":
    # Optional: pass a date as argument e.g. python fetch_results.py 2026-03-23
    if len(sys.argv) > 1:
        run(date.fromisoformat(sys.argv[1]))
    else:
        run()
