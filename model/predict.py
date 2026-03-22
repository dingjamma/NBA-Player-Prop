"""
Run inference for Victor Wembanyama using trained XGBoost models.
Fetches his recent game logs from ESPN (cloud-friendly, no API key).

Usage:
    python -m model.predict
"""

import io
import pickle
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

import requests
import pandas as pd
import xgboost as xgb

from ingestion.s3 import _client, BUCKET, upload_parquet
from model.features import build_features, get_feature_cols, TARGET_STATS


WEMBY_ESPN_ID  = 5104157
WEMBY_NAME     = "Victor Wembanyama"
LOOKBACK_GAMES = 20
ESPN_GAMELOG   = "https://site.web.api.espn.com/apis/common/v3/sports/basketball/nba/athletes/{player_id}/gamelog"


def load_model(stat: str) -> xgb.XGBRegressor:
    model = xgb.XGBRegressor()
    tmp = Path(tempfile.mktemp(suffix=".json"))
    _client().download_file(BUCKET, f"models/{stat}/model.json", str(tmp))
    model.load_model(str(tmp))
    tmp.unlink(missing_ok=True)
    return model


def load_encoder():
    obj = _client().get_object(Bucket=BUCKET, Key="models/label_encoder.pkl")
    return pickle.loads(obj["Body"].read())


def fetch_recent_logs(n_games: int = LOOKBACK_GAMES) -> pd.DataFrame:
    """Fetch Wemby's last N games from ESPN."""
    all_rows = []
    for season in [2025, 2024]:
        resp = requests.get(
            ESPN_GAMELOG.format(player_id=WEMBY_ESPN_ID),
            params={"season": season},
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()

        stat_names  = data.get("names", [])
        events_meta = data.get("events", {})
        stats_map   = {}

        for stype in data.get("seasonTypes", []):
            if "star" in stype.get("displayName", "").lower():
                continue
            for cat in stype.get("categories", []):
                for entry in cat.get("events", []):
                    eid  = str(entry.get("eventId"))
                    vals = entry.get("stats", [])
                    stats_map[eid] = dict(zip(stat_names, vals))

        for eid, stats in stats_map.items():
            meta = events_meta.get(eid, {})
            game_date = meta.get("gameDate", "")[:10]
            if not game_date:
                continue

            def split_first(key):
                val = stats.get(key)
                if val is None:
                    return None
                if isinstance(val, str) and "-" in val:
                    return pd.to_numeric(val.split("-")[0], errors="coerce")
                return pd.to_numeric(val, errors="coerce")

            def split_last(key):
                val = stats.get(key)
                if val is None:
                    return None
                if isinstance(val, str) and "-" in val:
                    return pd.to_numeric(val.split("-")[-1], errors="coerce")
                return pd.to_numeric(val, errors="coerce")

            all_rows.append({
                "GAME_ID":    eid,
                "GAME_DATE":  game_date,
                "OPP":        meta.get("opponent", {}).get("abbreviation", "UNK"),
                "HOME_AWAY":  "away" if meta.get("atVs") == "@" else "home",
                "WL":         meta.get("gameResult", ""),
                "MIN":        stats.get("minutes"),
                "PTS":        pd.to_numeric(stats.get("points"), errors="coerce"),
                "REB":        pd.to_numeric(stats.get("totalRebounds"), errors="coerce"),
                "AST":        pd.to_numeric(stats.get("assists"), errors="coerce"),
                "STL":        pd.to_numeric(stats.get("steals"), errors="coerce"),
                "BLK":        pd.to_numeric(stats.get("blocks"), errors="coerce"),
                "FGM":        split_first("fieldGoalsMade-fieldGoalsAttempted"),
                "FGA":        split_last("fieldGoalsMade-fieldGoalsAttempted"),
                "FG3M":       split_first("threePointFieldGoalsMade-threePointFieldGoalsAttempted"),
                "FG3A":       split_last("threePointFieldGoalsMade-threePointFieldGoalsAttempted"),
                "FTM":        split_first("freeThrowsMade-freeThrowsAttempted"),
                "FTA":        split_last("freeThrowsMade-freeThrowsAttempted"),
                "TOV":        pd.to_numeric(stats.get("turnovers"), errors="coerce"),
                "PF":         pd.to_numeric(stats.get("fouls"), errors="coerce"),
                "PLAYER_ID":   str(WEMBY_ESPN_ID),
                "PLAYER_NAME": WEMBY_NAME,
                "SEASON":      f"{season-1}-{str(season)[2:]}",
            })

        if len(all_rows) >= n_games:
            break
        time.sleep(1)

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    df = df.sort_values("GAME_DATE", ascending=False).head(n_games)
    return df


def run(games_df: pd.DataFrame = None) -> pd.DataFrame:
    tomorrow = date.today() + timedelta(days=1)

    if games_df is None or games_df.empty:
        print("[predict] No games provided, skipping inference.")
        return pd.DataFrame()

    # Check Spurs are actually playing
    spurs_playing = any(
        "spurs" in str(row.get("HOME_TEAM", "")).lower() or
        "spurs" in str(row.get("VISITOR_TEAM", "")).lower() or
        "SA"    in str(row.get("HOME_TEAM_ABV", "")) or
        "SA"    in str(row.get("VISITOR_TEAM_ABV", ""))
        for _, row in games_df.iterrows()
    )
    if not spurs_playing:
        print("[predict] Spurs not playing tomorrow, skipping.")
        return pd.DataFrame()

    print("Loading models...")
    models      = {stat: load_model(stat) for stat in TARGET_STATS}
    le          = load_encoder()
    feature_cols = get_feature_cols()

    print(f"Fetching Wemby's last {LOOKBACK_GAMES} games from ESPN...")
    logs = fetch_recent_logs(LOOKBACK_GAMES)
    if logs.empty or len(logs) < 3:
        print("[predict] Not enough recent data for Wemby.")
        return pd.DataFrame()
    print(f"  {len(logs)} games loaded")

    df_feat, _ = build_features(logs, le=le)
    last_row   = df_feat.sort_values("GAME_DATE").iloc[[-1]]
    available  = [c for c in feature_cols if c in last_row.columns]
    X          = last_row[available].values

    result = {
        "player_id":   str(WEMBY_ESPN_ID),
        "player_name": WEMBY_NAME,
        "game_date":   str(tomorrow),
    }
    for stat, model in models.items():
        try:
            val = float(model.predict(X)[0])
            result[f"pred_{stat.lower()}"] = round(max(val, 0), 2)
        except Exception as e:
            print(f"  [WARN] {stat}: {e}")
            result[f"pred_{stat.lower()}"] = None

    df = pd.DataFrame([result])
    print(f"\n  Wemby predictions for {tomorrow}:")
    for stat in TARGET_STATS:
        print(f"    {stat}: {result.get(f'pred_{stat.lower()}')}")

    date_str = tomorrow.strftime("%Y_%m_%d")
    tmp = Path(tempfile.mktemp(suffix=".parquet"))
    df.to_parquet(tmp, index=False)
    upload_parquet(tmp, f"processed/predictions/date={date_str}/predictions.parquet")
    print(f"  Saved predictions -> data/processed/predictions/date={date_str}/")

    return df


if __name__ == "__main__":
    from crawlers.schedule_crawler import run as get_games
    games = get_games()
    run(games)
