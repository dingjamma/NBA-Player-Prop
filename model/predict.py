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


WEMBY_NBA_ID   = 1641705
WEMBY_NAME     = "Victor Wembanyama"
LOOKBACK_GAMES = 20
NBA_GAMELOG    = "https://stats.nba.com/stats/playergamelog"
NBA_HEADERS    = {
    "Host":               "stats.nba.com",
    "User-Agent":         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":             "application/json, text/plain, */*",
    "Accept-Language":    "en-US,en;q=0.9",
    "x-nba-stats-origin": "stats",
    "x-nba-stats-token":  "true",
    "Referer":            "https://stats.nba.com/",
}


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
    """Fetch Wemby's last N games from stats.nba.com."""
    today = date.today()
    current_end = today.year if today.month <= 9 else today.year + 1
    seasons = [f"{current_end-1}-{str(current_end)[2:]}",
               f"{current_end-2}-{str(current_end-1)[2:]}"]

    frames = []
    for season in seasons:
        try:
            resp = requests.get(
                NBA_GAMELOG,
                headers=NBA_HEADERS,
                params={"PlayerID": WEMBY_NBA_ID, "Season": season, "SeasonType": "Regular Season"},
                timeout=30,
            )
            resp.raise_for_status()
            result = resp.json()["resultSets"][0]
            cols   = result["headers"]
            rows   = result["rowSet"]
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=cols)
            df = df.rename(columns={"Player_ID": "PLAYER_ID", "Game_ID": "GAME_ID"})
            df["GAME_DATE"]   = pd.to_datetime(df["GAME_DATE"]).dt.strftime("%Y-%m-%d")
            df["PLAYER_NAME"] = WEMBY_NAME
            df["SEASON"]      = season
            frames.append(df)
        except Exception as e:
            print(f"  [WARN] Failed to fetch season {season}: {e}")
        time.sleep(1)
        if sum(len(f) for f in frames) >= n_games:
            break

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    df = df.sort_values("GAME_DATE", ascending=False).head(n_games)
    return df


def run(games_df: pd.DataFrame = None) -> pd.DataFrame:
    game_date = date.today()

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
        print("[predict] Spurs not playing game_date, skipping.")
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
        "player_id":   str(WEMBY_NBA_ID),
        "player_name": WEMBY_NAME,
        "game_date":   str(game_date),
    }
    for stat, model in models.items():
        try:
            val = float(model.predict(X)[0])
            result[f"pred_{stat.lower()}"] = round(max(val, 0), 2)
        except Exception as e:
            print(f"  [WARN] {stat}: {e}")
            result[f"pred_{stat.lower()}"] = None

    df = pd.DataFrame([result])
    print(f"\n  Wemby predictions for {game_date}:")
    for stat in TARGET_STATS:
        print(f"    {stat}: {result.get(f'pred_{stat.lower()}')}")

    date_str = game_date.strftime("%Y_%m_%d")
    tmp = Path(tempfile.mktemp(suffix=".parquet"))
    df.to_parquet(tmp, index=False)
    upload_parquet(tmp, f"processed/predictions/date={date_str}/predictions.parquet")
    print(f"  Saved predictions -> data/processed/predictions/date={date_str}/")

    return df


if __name__ == "__main__":
    from crawlers.schedule_crawler import run as get_games
    games = get_games()
    run(games)
