"""
Feature engineering for NBA player prop prediction.

Input:  raw game logs DataFrame (from nba_api)
Output: feature DataFrame ready for XGBoost

Features built per player-game row:
  - Rolling averages (last 5 / last 10 games): PTS, REB, AST, STL, BLK, FG3M, MIN
  - Season averages up to that game (no lookahead)
  - Rest days since last game
  - Home / away flag
  - Opponent team ID (label encoded)
  - Back-to-back flag
  - Usage proxy: FGA + FTA*0.44 + TOV
"""

import pandas as pd
import numpy as np
from sklearn.preprocessing import LabelEncoder

STAT_COLS = ["PTS", "REB", "AST", "STL", "BLK", "FG3M", "MIN", "TOV", "FGA", "FTA"]
ROLL_WINDOWS = [5, 10]
TARGET_STATS = ["PTS", "REB", "AST", "STL", "BLK", "FG3M"]


def _parse_matchup(matchup: str):
    """Extract home flag and opponent abbreviation from MATCHUP string.
    Format: 'GSW vs. LAL'  (home)  or  'GSW @ LAL'  (away)
    """
    if "@" in matchup:
        parts = matchup.split("@")
        return 0, parts[1].strip()  # away
    elif "vs." in matchup:
        parts = matchup.split("vs.")
        return 1, parts[1].strip()  # home
    return -1, "UNK"


def build_features(df: pd.DataFrame, le: LabelEncoder = None) -> tuple[pd.DataFrame, LabelEncoder]:
    """
    Build feature matrix from raw game log DataFrame.

    Args:
        df:  raw game logs, all players, all seasons
        le:  optional pre-fitted LabelEncoder for opponent teams

    Returns:
        (features_df, label_encoder)
    """
    df = df.copy()
    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
    df = df.sort_values(["PLAYER_ID", "GAME_DATE"]).reset_index(drop=True)

    # Normalize column names from different data sources
    if "MATCHUP" in df.columns:
        df[["HOME", "OPP"]] = df["MATCHUP"].apply(
            lambda m: pd.Series(_parse_matchup(m))
        )
    else:
        df["HOME"] = (df["HOME_AWAY"] == "home").astype(int)
        # OPP already exists in ESPN data

    if "SEASON_ID" not in df.columns:
        df["SEASON_ID"] = df["SEASON"]

    # Parse MIN if it's in "MM:SS" string format
    if df["MIN"].dtype == object:
        df["MIN"] = df["MIN"].apply(
            lambda x: float(str(x).split(":")[0]) + float(str(x).split(":")[1]) / 60
            if isinstance(x, str) and ":" in str(x) else pd.to_numeric(x, errors="coerce")
        )

    # Rest days
    df["PREV_GAME_DATE"] = df.groupby("PLAYER_ID")["GAME_DATE"].shift(1)
    df["REST_DAYS"] = (df["GAME_DATE"] - df["PREV_GAME_DATE"]).dt.days.fillna(7).clip(upper=14)

    # Back-to-back
    df["B2B"] = (df["REST_DAYS"] == 1).astype(int)

    # Usage proxy
    df["USAGE_PROXY"] = df["FGA"] + df["FTA"] * 0.44 + df["TOV"]

    # Rolling averages — shift(1) to avoid lookahead
    for col in STAT_COLS + ["USAGE_PROXY"]:
        if col not in df.columns:
            continue
        grp = df.groupby("PLAYER_ID")[col]
        for w in ROLL_WINDOWS:
            df[f"{col}_ROLL{w}"] = (
                grp.shift(1)
                   .rolling(w, min_periods=1)
                   .mean()
                   .reset_index(level=0, drop=True)
            )

    # Season averages up to (but not including) current game
    for col in STAT_COLS:
        if col not in df.columns:
            continue
        df[f"{col}_SEASON_AVG"] = (
            df.groupby(["PLAYER_ID", "SEASON_ID"])[col]
              .transform(lambda x: x.shift(1).expanding().mean())
        )

    # Games played this season (proxy for minutes stabilization)
    df["GAMES_PLAYED"] = (
        df.groupby(["PLAYER_ID", "SEASON_ID"]).cumcount()
    )

    # Career averages vs this specific opponent (no lookahead — shift before expanding mean)
    for col in TARGET_STATS:
        if col not in df.columns:
            continue
        df[f"{col}_VS_OPP"] = (
            df.groupby(["PLAYER_ID", "OPP"])[col]
              .transform(lambda x: x.shift(1).expanding().mean())
        )

    # Games played vs this opponent (sample size signal)
    df["GAMES_VS_OPP"] = (
        df.groupby(["PLAYER_ID", "OPP"]).cumcount()
    )

    # Encode opponent
    if le is None:
        le = LabelEncoder()
        df["OPP_ENC"] = le.fit_transform(df["OPP"].astype(str))
    else:
        known = set(le.classes_)
        df["OPP"] = df["OPP"].apply(lambda x: x if x in known else "UNK")
        if "UNK" not in known:
            le.classes_ = np.append(le.classes_, "UNK")
        df["OPP_ENC"] = le.transform(df["OPP"].astype(str))

    return df, le


def get_feature_cols() -> list[str]:
    """Return the list of feature column names used by the model."""
    cols = ["HOME", "REST_DAYS", "B2B", "GAMES_PLAYED", "OPP_ENC", "USAGE_PROXY", "GAMES_VS_OPP"]
    for col in STAT_COLS + ["USAGE_PROXY"]:
        for w in ROLL_WINDOWS:
            cols.append(f"{col}_ROLL{w}")
    for col in STAT_COLS:
        cols.append(f"{col}_SEASON_AVG")
    for col in TARGET_STATS:
        cols.append(f"{col}_VS_OPP")
    return cols
