"""
Train one XGBoost model per stat per player — 30 models total (5 players × 6 stats).

Models stored at: data/models/{player_key}/{stat}/model.json
Encoder stored at: data/models/{player_key}/label_encoder.pkl

Usage:
    python -m model.train              # all players
    python -m model.train wembanyama   # one player
"""

import io
import pickle
import sys
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error

from ingestion.s3 import list_keys, download_parquet, _client, BUCKET
from model.features import build_features, get_feature_cols, TARGET_STATS
from config import PLAYERS


def load_player_logs(player_key: str) -> pd.DataFrame:
    """Load all season parquets for one player."""
    prefix = f"raw/game_logs/player={player_key}/"
    keys = list_keys(prefix)
    if not keys:
        print(f"  [WARN] No game logs found for '{player_key}'")
        return pd.DataFrame()

    frames = []
    for key in keys:
        tmp = Path(tempfile.mktemp(suffix=".parquet"))
        download_parquet(key, tmp)
        frames.append(pd.read_parquet(tmp))
        tmp.unlink(missing_ok=True)

    df = pd.concat(frames, ignore_index=True)
    print(f"  Loaded {len(df)} games across {df['SEASON'].nunique()} seasons")
    return df


def train_stat_model(
    df: pd.DataFrame,
    target: str,
    feature_cols: list[str],
) -> xgb.XGBRegressor:
    """Train and CV-validate XGBoost for one stat target."""
    valid = df[feature_cols + [target]].dropna()
    X = valid[feature_cols].values
    y = valid[target].values

    model = xgb.XGBRegressor(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        min_child_weight=5,
        random_state=42,
        n_jobs=-1,
        early_stopping_rounds=30,
    )

    tscv = TimeSeriesSplit(n_splits=5)
    maes = []
    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        model.fit(
            X[train_idx], y[train_idx],
            eval_set=[(X[val_idx], y[val_idx])],
            verbose=False,
        )
        maes.append(mean_absolute_error(y[val_idx], model.predict(X[val_idx])))
        print(f"    Fold {fold + 1} MAE: {maes[-1]:.3f}")

    print(f"  {target} — mean CV MAE: {np.mean(maes):.3f}")

    # Final fit on all data
    model.set_params(early_stopping_rounds=None)
    model.fit(X, y, verbose=False)
    return model


def save_model(model: xgb.XGBRegressor, player_key: str, stat: str) -> None:
    tmp = Path(tempfile.mktemp(suffix=".json"))
    model.save_model(str(tmp))
    key = f"models/{player_key}/{stat}/model.json"
    _client().upload_file(str(tmp), BUCKET, key)
    tmp.unlink(missing_ok=True)
    print(f"  Saved -> data/{key}")


def save_encoder(le, player_key: str) -> None:
    buf = io.BytesIO()
    pickle.dump(le, buf)
    buf.seek(0)
    _client().put_object(
        Bucket=BUCKET,
        Key=f"models/{player_key}/label_encoder.pkl",
        Body=buf.read(),
    )
    print(f"  Saved encoder -> data/models/{player_key}/label_encoder.pkl")


def train_player(player_key: str) -> None:
    """Train 6 models for one player."""
    name = PLAYERS[player_key]["name"]
    print(f"\n{'='*55}")
    print(f"  Training {name} ({player_key})")
    print(f"{'='*55}")

    raw = load_player_logs(player_key)
    if raw.empty:
        print("  Skipping — no data.")
        return

    df, le = build_features(raw)
    feature_cols = [c for c in get_feature_cols() if c in df.columns]

    for stat in TARGET_STATS:
        if stat not in df.columns:
            print(f"  [SKIP] {stat} not in data")
            continue
        print(f"\n  {stat}...")
        model = train_stat_model(df, stat, feature_cols)
        save_model(model, player_key, stat)

    save_encoder(le, player_key)


def run(player_keys: list[str] | None = None) -> None:
    targets = player_keys or list(PLAYERS.keys())
    print(f"Training models for: {targets}")
    for key in targets:
        if key not in PLAYERS:
            print(f"[WARN] Unknown player key '{key}' — skipping")
            continue
        train_player(key)
    print("\nAll models trained.")


if __name__ == "__main__":
    keys = sys.argv[1:] or None
    run(keys)
