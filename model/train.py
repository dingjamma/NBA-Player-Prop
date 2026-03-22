"""
Train one XGBoost model per target stat.

Targets: PTS, REB, AST, STL, BLK, FG3M

Usage:
    python -m model.train

Reads:   s3://nba-player-prop/raw/game_logs/**/*.parquet
Writes:  s3://nba-player-prop/models/{stat}/model.json
         s3://nba-player-prop/models/label_encoder.pkl
"""

import io
import pickle
import tempfile
from pathlib import Path

import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_absolute_error

from ingestion.s3 import list_keys, download_parquet, upload_parquet
from ingestion.s3 import _client, BUCKET
from model.features import build_features, get_feature_cols, TARGET_STATS


def load_all_game_logs() -> pd.DataFrame:
    """Load all historical game log parquets from S3."""
    keys = list_keys("raw/game_logs/")
    print(f"  Found {len(keys)} parquet files")

    frames = []
    for key in keys:
        tmp_path = Path(tempfile.mktemp(suffix=".parquet"))
        download_parquet(key, tmp_path)
        frames.append(pd.read_parquet(tmp_path))
        tmp_path.unlink(missing_ok=True)

    return pd.concat(frames, ignore_index=True)


def train_stat_model(df: pd.DataFrame, target: str, feature_cols: list[str]) -> xgb.XGBRegressor:
    """Train XGBoost for a single stat target."""
    valid = df[feature_cols + [target]].dropna()
    X = valid[feature_cols].values
    y = valid[target].values

    # Time-series cross-validation (5 folds)
    tscv = TimeSeriesSplit(n_splits=5)
    maes = []

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

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_train, X_val = X[train_idx], X[val_idx]
        y_train, y_val = y[train_idx], y[val_idx]

        model.fit(
            X_train, y_train,
            eval_set=[(X_val, y_val)],
            verbose=False,
        )
        preds = model.predict(X_val)
        mae = mean_absolute_error(y_val, preds)
        maes.append(mae)
        print(f"    Fold {fold+1} MAE: {mae:.3f}")

    print(f"  {target} — mean CV MAE: {np.mean(maes):.3f}")

    # Final fit on all data (no early stopping)
    model.set_params(early_stopping_rounds=None)
    model.fit(X, y, verbose=False)
    return model


def upload_model(model: xgb.XGBRegressor, stat: str):
    tmp = Path(tempfile.mktemp(suffix=".json"))
    model.save_model(str(tmp))
    s3_key = f"models/{stat}/model.json"
    _client().upload_file(str(tmp), BUCKET, s3_key)
    tmp.unlink(missing_ok=True)
    print(f"  Saved -> data/{s3_key}")


def upload_encoder(le):
    buf = io.BytesIO()
    pickle.dump(le, buf)
    buf.seek(0)
    _client().put_object(Bucket=BUCKET, Key="models/label_encoder.pkl", Body=buf.read())
    print(f"  Saved label encoder -> data/models/label_encoder.pkl")


def run():
    print("Loading game logs...")
    raw = load_all_game_logs()
    print(f"  {len(raw)} total player-game rows")

    print("Building features...")
    df, le = build_features(raw)
    feature_cols = [c for c in get_feature_cols() if c in df.columns]

    print("Training models...")
    for stat in TARGET_STATS:
        if stat not in df.columns:
            print(f"  [SKIP] {stat} not in data")
            continue
        print(f"\n  Training {stat} model...")
        model = train_stat_model(df, stat, feature_cols)
        upload_model(model, stat)

    upload_encoder(le)
    print("\nAll models trained and uploaded.")


if __name__ == "__main__":
    run()
