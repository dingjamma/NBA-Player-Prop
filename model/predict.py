"""
Nightly inference — runs all tracked players who have a game today.

For each player with a game:
  1. Fetch their last 20 games from stats.nba.com
  2. Build features
  3. Run their 6 XGBoost models
  4. Save predictions

Usage:
    python -m model.predict
"""

import io
import pickle
import tempfile
import time
from datetime import date
from pathlib import Path

import requests
import pandas as pd
import xgboost as xgb

from ingestion.s3 import _client, BUCKET, upload_parquet
from model.features import build_features, get_feature_cols, TARGET_STATS
from config import PLAYERS, TRACKED_TEAMS

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


def _current_seasons() -> list[str]:
    today = date.today()
    end = today.year if today.month <= 9 else today.year + 1
    return [f"{end - 1}-{str(end)[2:]}", f"{end - 2}-{str(end - 1)[2:]}"]


def fetch_recent_logs(player_id: int, player_name: str) -> pd.DataFrame:
    """Fetch the last LOOKBACK_GAMES games for a player from stats.nba.com."""
    frames = []
    for season in _current_seasons():
        try:
            resp = requests.get(
                NBA_GAMELOG,
                headers=NBA_HEADERS,
                params={"PlayerID": player_id, "Season": season, "SeasonType": "Regular Season"},
                timeout=30,
            )
            resp.raise_for_status()
            result = resp.json()["resultSets"][0]
            rows   = result["rowSet"]
            if not rows:
                continue
            df = pd.DataFrame(rows, columns=result["headers"])
            df = df.rename(columns={"Player_ID": "PLAYER_ID", "Game_ID": "GAME_ID"})
            df["GAME_DATE"]   = pd.to_datetime(df["GAME_DATE"]).dt.strftime("%Y-%m-%d")
            df["PLAYER_NAME"] = player_name
            df["PLAYER_ID"]   = player_id
            df["SEASON"]      = season
            frames.append(df)
        except Exception as e:
            print(f"  [WARN] {player_name} season {season}: {e}")
        time.sleep(0.5)
        if sum(len(f) for f in frames) >= LOOKBACK_GAMES:
            break

    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    return df.sort_values("GAME_DATE", ascending=False).head(LOOKBACK_GAMES)


def load_model(player_key: str, stat: str) -> xgb.XGBRegressor | None:
    model_path = Path(f"data/models/{player_key}/{stat}/model.json")
    if not model_path.exists():
        return None
    model = xgb.XGBRegressor()
    model.load_model(str(model_path))
    return model


def load_encoder(player_key: str):
    enc_path = Path(f"data/models/{player_key}/label_encoder.pkl")
    if not enc_path.exists():
        return None
    return pickle.loads(enc_path.read_bytes())


def predict_player(player_key: str) -> dict | None:
    """Run inference for one player. Returns prediction dict or None."""
    cfg         = PLAYERS[player_key]
    player_id   = cfg["nba_id"]
    player_name = cfg["name"]

    print(f"\n  {player_name}...")

    le = load_encoder(player_key)
    if le is None:
        print(f"    [SKIP] No encoder found — run model.train first")
        return None

    models = {stat: load_model(player_key, stat) for stat in TARGET_STATS}
    if all(m is None for m in models.values()):
        print(f"    [SKIP] No models found — run model.train first")
        return None

    logs = fetch_recent_logs(player_id, player_name)
    if logs.empty or len(logs) < 3:
        print(f"    [SKIP] Not enough recent data ({len(logs)} games)")
        return None

    print(f"    {len(logs)} recent games loaded")

    df_feat, _ = build_features(logs, le=le)
    feature_cols = get_feature_cols()
    last_row  = df_feat.sort_values("GAME_DATE").iloc[[-1]]
    available = [c for c in feature_cols if c in last_row.columns]
    X         = last_row[available].values

    result = {
        "player_key":  player_key,
        "player_name": player_name,
        "player_id":   str(player_id),
        "game_date":   str(date.today()),
    }
    for stat, model in models.items():
        if model is None:
            result[f"pred_{stat.lower()}"] = None
            continue
        try:
            val = float(model.predict(X)[0])
            result[f"pred_{stat.lower()}"] = round(max(val, 0), 2)
        except Exception as e:
            print(f"    [WARN] {stat}: {e}")
            result[f"pred_{stat.lower()}"] = None

    preds_str = "  ".join(
        f"{s}={result.get(f'pred_{s.lower()}')}"
        for s in TARGET_STATS
    )
    print(f"    {preds_str}")
    return result


def _teams_playing_today(games_df: pd.DataFrame) -> set[str]:
    """Return team abbreviations that have a game today."""
    teams: set[str] = set()
    for _, row in games_df.iterrows():
        teams.add(str(row.get("HOME_TEAM_ABV", "")))
        teams.add(str(row.get("VISITOR_TEAM_ABV", "")))
    return teams


def run(games_df: pd.DataFrame | None = None) -> pd.DataFrame:
    game_date = date.today()

    if games_df is None or games_df.empty:
        print("[predict] No games provided, skipping inference.")
        return pd.DataFrame()

    teams_today = _teams_playing_today(games_df)
    active_keys = [
        key for key, cfg in PLAYERS.items()
        if cfg["team"] in teams_today
    ]

    if not active_keys:
        print("[predict] No tracked players have games today.")
        return pd.DataFrame()

    print(f"[predict] Players with games today: {active_keys}")

    all_results = []
    for key in active_keys:
        result = predict_player(key)
        if result:
            all_results.append(result)

    if not all_results:
        print("[predict] No predictions generated.")
        return pd.DataFrame()

    df = pd.DataFrame(all_results)
    date_str = game_date.strftime("%Y_%m_%d")
    tmp = Path(tempfile.mktemp(suffix=".parquet"))
    df.to_parquet(tmp, index=False)
    upload_parquet(tmp, f"processed/predictions/date={date_str}/predictions.parquet")
    print(f"\n  Saved {len(df)} player predictions -> data/processed/predictions/date={date_str}/")

    return df


if __name__ == "__main__":
    from crawlers.schedule_crawler import run as get_games
    games = get_games()
    run(games)
