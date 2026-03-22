"""
Generate a concise human-readable stats report from model predictions + odds.
Runs concurrently alongside MiroFish simulation.
Uploads to S3 as reports/stats/{date}/report.md
"""

from datetime import date, timedelta
from pathlib import Path
import tempfile

import pandas as pd

from ingestion.s3 import upload_text


def generate(predictions: pd.DataFrame, odds: pd.DataFrame) -> str:
    """Return the report as a Markdown string."""
    tomorrow = date.today() + timedelta(days=1)
    date_str = tomorrow.strftime("%Y-%m-%d")

    from nba_api.stats.static import players
    name_map = {p["id"]: p["full_name"] for p in players.get_active_players()}

    lines = [f"# NBA Props Stats Report — {date_str}", ""]

    if predictions is None or predictions.empty:
        lines.append("_No predictions generated._")
        return "\n".join(lines)

    # Merge odds: get DraftKings lines per player/market
    dk_odds = {}
    if odds is not None and not odds.empty:
        dk = odds[odds["bookmaker"] == "draftkings"] if "draftkings" in odds["bookmaker"].values else odds
        for _, row in dk[dk["side"] == "Over"].iterrows():
            if row["player_name"]:
                dk_odds[(row["player_name"].lower(), row["market"])] = row["line"]

    market_map = {
        "pred_pts":  ("player_points",   "PTS"),
        "pred_reb":  ("player_rebounds",  "REB"),
        "pred_ast":  ("player_assists",   "AST"),
        "pred_stl":  ("player_steals",    "STL"),
        "pred_blk":  ("player_blocks",    "BLK"),
        "pred_fg3m": ("player_threes",    "3PM"),
    }

    lines += ["| Player | Stat | Prediction | Line | Edge | Direction |",
              "|--------|------|-----------|------|------|-----------|"]

    for _, row in predictions.iterrows():
        pid = int(row["player_id"])
        pname = name_map.get(pid, f"Player {pid}")
        for col, (market, label) in market_map.items():
            if col not in row or row[col] is None:
                continue
            pred = row[col]
            line = dk_odds.get((pname.lower(), market))
            if line is None:
                continue
            diff = pred - line
            direction = "**OVER**" if diff > 0 else "**UNDER**"
            lines.append(f"| {pname} | {label} | {pred:.1f} | {line:.1f} | {diff:+.1f} | {direction} |")

    content = "\n".join(lines)

    s3_key = f"reports/stats/{date_str}/report.md"
    upload_text(content, s3_key)
    print(f"  Stats report uploaded → s3://{s3_key}")

    return content
