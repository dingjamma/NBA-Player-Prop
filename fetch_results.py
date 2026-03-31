"""
Fetch actual game results and log vs model predictions + Underdog lines.

Runs after games end (Task Scheduler at 11pm MT).
Appends rows to: data/results/results.csv

Columns: date, player_name, opponent, stat,
         model_pred, ud_line, model_pick, actual, model_correct
"""

import sys
from datetime import date
from pathlib import Path

import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

from config import PLAYERS

STATS        = ["PTS", "REB", "AST", "STL", "BLK", "FG3M"]
RESULTS_PATH = Path("data/results/results.csv")

# Underdog display names → our stat keys
UD_STAT_MAP = {
    "Points":          "PTS",
    "Rebounds":        "REB",
    "Assists":         "AST",
    "3-Pointers Made": "FG3M",
}

# ESPN athlete IDs for each tracked player
ESPN_IDS = {
    "wembanyama": "5104157",
    "jokic":      "3112335",
    "doncic":     "3945274",
    "sga":        "4278073",
    "giannis":    "3032977",
}


def _current_espn_season() -> int:
    today = date.today()
    return today.year if today.month <= 9 else today.year + 1


def fetch_actual_stats(player_key: str, game_date: date) -> dict | None:
    """Pull a player's box score for game_date from ESPN."""
    espn_id = ESPN_IDS.get(player_key)
    if not espn_id:
        print(f"  [results] No ESPN ID for '{player_key}'")
        return None

    season = _current_espn_season()
    url = (
        f"https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba"
        f"/athletes/{espn_id}/gamelog?season={season}"
    )
    try:
        r = requests.get(url, timeout=15)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"  [results] ESPN fetch failed for {player_key}: {e}")
        return None

    date_str    = game_date.isoformat()
    events_dict = data.get("events", {})
    labels      = data.get("labels", [])

    for st in data.get("seasonTypes", []):
        for cat in st.get("categories", []):
            for event in cat.get("events", []):
                ev_id   = str(event.get("eventId", event.get("id", "")))
                ev_date = events_dict.get(ev_id, {}).get("gameDate", "")[:10]
                if ev_date != date_str:
                    continue
                row = dict(zip(labels, event.get("stats", [])))
                if row:
                    return _parse_espn_stats(row)

    print(f"  [results] No game found for {player_key} on {date_str}")
    return None


def _parse_espn_stats(raw: dict) -> dict:
    def _f(key: str) -> float | None:
        v = raw.get(key)
        if v in (None, "--", ""):
            return None
        if isinstance(v, str) and "-" in v:
            try:
                return float(v.split("-")[0])
            except Exception:
                return None
        try:
            return float(v)
        except Exception:
            return None

    return {
        "PTS":  _f("PTS"),
        "REB":  _f("REB"),
        "AST":  _f("AST"),
        "STL":  _f("STL"),
        "BLK":  _f("BLK"),
        "FG3M": _f("3PT"),
    }


def load_predictions(game_date: date) -> pd.DataFrame:
    dk = game_date.strftime("%Y_%m_%d")
    p  = Path(f"data/processed/predictions/date={dk}/predictions.parquet")
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def load_underdog_lines(game_date: date) -> pd.DataFrame:
    dk = game_date.strftime("%Y_%m_%d")
    p  = Path(f"data/raw/odds/date={dk}/props.parquet")
    return pd.read_parquet(p) if p.exists() else pd.DataFrame()


def get_ud_lines_for_player(odds: pd.DataFrame, player_name: str) -> dict[str, float]:
    """Return {stat: line} for a player from Underdog data."""
    if odds.empty or "name" not in odds.columns:
        return {}
    last = player_name.split()[-1]
    rows = odds[odds["name"].str.contains(last, case=False, na=False)]
    lines: dict[str, float] = {}
    for _, row in rows.iterrows():
        stat_key = UD_STAT_MAP.get(row.get("stat", ""))
        if stat_key:
            lines[stat_key] = float(row["line"])
    return lines


def append_results(new_rows: list[dict]) -> None:
    RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(new_rows)
    if RESULTS_PATH.exists():
        existing = pd.read_csv(RESULTS_PATH)
        combined = pd.concat([existing, new_df], ignore_index=True)
        combined = combined.drop_duplicates(
            subset=["date", "player_name", "stat"], keep="last"
        )
        combined.to_csv(RESULTS_PATH, index=False)
    else:
        new_df.to_csv(RESULTS_PATH, index=False)
    print(f"  [results] Saved to {RESULTS_PATH}")


def run(game_date: date | None = None) -> None:
    if game_date is None:
        game_date = date.today()

    print(f"\n[results] Fetching results for {game_date}...")

    preds = load_predictions(game_date)
    odds  = load_underdog_lines(game_date)

    if preds.empty:
        print("  [results] No predictions found — skipping.")
        return

    all_rows = []

    for _, pred_row in preds.iterrows():
        player_key  = pred_row.get("player_key", "")
        player_name = pred_row.get("player_name", "")

        if not player_key or player_key not in PLAYERS:
            continue

        print(f"\n  {player_name}")
        ud_lines = get_ud_lines_for_player(odds, player_name)
        actual   = fetch_actual_stats(player_key, game_date)

        if actual is None:
            print(f"    Could not fetch actual stats.")
            continue

        for stat in STATS:
            model_pred = pred_row.get(f"pred_{stat.lower()}")
            ud_line    = ud_lines.get(stat)
            act_val    = actual.get(stat)
            model_pick  = None
            model_correct = None

            if model_pred is not None and ud_line is not None:
                model_pick = "OVER" if float(model_pred) > ud_line else "UNDER"

            if model_pick is not None and act_val is not None and ud_line is not None:
                hit_over      = act_val > ud_line
                model_correct = (model_pick == "OVER" and hit_over) or \
                                (model_pick == "UNDER" and not hit_over)

            status = "CORRECT" if model_correct else ("WRONG" if model_correct is False else "N/A")
            print(
                f"    {stat:5s}  pred={str(model_pred or '?'):>5}  "
                f"line={str(ud_line or '?'):>5}  "
                f"actual={str(act_val or '?'):>5}  "
                f"pick={str(model_pick or '?'):>6}  [{status}]"
            )

            all_rows.append({
                "date":          game_date.isoformat(),
                "player_name":   player_name,
                "player_key":    player_key,
                "stat":          stat,
                "model_pred":    round(float(model_pred), 2) if model_pred is not None else None,
                "ud_line":       ud_line,
                "model_pick":    model_pick,
                "actual":        act_val,
                "model_correct": model_correct,
            })

    if all_rows:
        append_results(all_rows)
    else:
        print("\n  [results] No results to save.")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run(date.fromisoformat(sys.argv[1]))
    else:
        run()
