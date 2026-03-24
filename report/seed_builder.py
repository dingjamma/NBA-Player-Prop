"""
Build the MiroFish seed file by merging:
  1. Model predictions (from S3 processed/predictions/)
  2. Prop lines / odds (from crawlers/odds.py)
  3. Injury report (from crawlers/injuries.py)
  4. News articles (last 7 days, from crawlers/news.py)

Output: a structured Markdown file saved to S3 and returned as a local path.
MiroFish consumes this as its seed material.

Format chosen: Markdown — MiroFish's text_processor handles it well,
and it's human-readable for debugging.
"""

import tempfile
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from ingestion.s3 import upload_text

TOMORROW = date.today() + timedelta(days=1)


def _player_name_map(predictions: pd.DataFrame) -> dict[str, str]:
    """Build a player_id → full_name lookup from the predictions DataFrame."""
    if "player_id" in predictions.columns and "player_name" in predictions.columns:
        return dict(zip(predictions["player_id"].astype(str), predictions["player_name"]))
    return {}


def _format_edge(pred_val: float | None, line: float | None, stat: str) -> str:
    """Return a short edge string, e.g. '+3.1 Over' or '-1.2 Under'."""
    if pred_val is None or line is None:
        return "N/A"
    diff = pred_val - line
    direction = "Over" if diff > 0 else "Under"
    return f"{diff:+.1f} {direction} ({pred_val:.1f} vs line {line:.1f})"


def build(
    predictions: pd.DataFrame,
    odds: pd.DataFrame,
    injuries: pd.DataFrame,
    news: pd.DataFrame,
) -> Path:
    """
    Merge all data sources into a single Markdown seed file.

    Returns the local path to the generated file.
    """
    date_str = TOMORROW.strftime("%Y-%m-%d")
    name_map = _player_name_map(predictions) if predictions is not None and not predictions.empty else {}

    lines = []

    # ------------------------------------------------------------------ header
    lines += [
        f"# NBA Player Prop Analysis — {date_str}",
        "",
        "> This document is the seed material for MiroFish simulation.",
        "> It contains model predictions, current prop lines, injury status,",
        "> and recent NBA news. Use it to simulate player performance outcomes",
        "> and generate betting insights.",
        "",
    ]

    # --------------------------------------------------------------- injuries
    lines += ["## Injury Report", ""]
    if injuries is not None and not injuries.empty:
        out_players = injuries[injuries["status"].str.lower().str.contains("out", na=False)]
        q_players   = injuries[injuries["status"].str.lower().str.contains("questionable", na=False)]

        if not out_players.empty:
            lines.append("### Confirmed Out")
            for _, row in out_players.iterrows():
                lines.append(f"- **{row['player_name']}** ({row['team']}) — {row['reason'] or 'No reason listed'}")
            lines.append("")

        if not q_players.empty:
            lines.append("### Questionable")
            for _, row in q_players.iterrows():
                lines.append(f"- **{row['player_name']}** ({row['team']}) — {row['reason'] or 'No reason listed'}")
            lines.append("")
    else:
        lines += ["_No injury data available._", ""]

    # ------------------------------------------------- predictions vs lines
    lines += ["## Player Prop Model vs. Book Lines", ""]

    stat_map = {
        "pts":  "player_points",
        "reb":  "player_rebounds",
        "ast":  "player_assists",
        "stl":  "player_steals",
        "blk":  "player_blocks",
        "fg3m": "player_threes",
    }

    if predictions is not None and not predictions.empty:
        # Build a lookup: (player_name_lower, market) → (line, side)
        odds_lookup: dict[tuple, float] = {}
        if odds is not None and not odds.empty:
            dk = odds[odds["bookmaker"] == "draftkings"] if "draftkings" in odds["bookmaker"].values else odds
            for _, row in dk.iterrows():
                if row["side"] == "Over" and row["player_name"]:
                    key = (row["player_name"].lower(), row["market"])
                    odds_lookup[key] = row["line"]

        top_edges = []

        for _, row in predictions.iterrows():
            pid = str(row["player_id"])
            pname = name_map.get(pid, row.get("player_name", f"Player {pid}"))

            player_lines = []
            for short, market in stat_map.items():
                col = f"pred_{short}"
                if col not in row or row[col] is None:
                    continue
                pred_val = row[col]
                line = odds_lookup.get((pname.lower(), market))
                edge_str = _format_edge(pred_val, line, short.upper())

                # Flag strong edges (model differs from line by > 2.5)
                strong = (
                    line is not None
                    and abs(pred_val - line) >= 2.5
                )
                flag = " ⚑" if strong else ""
                player_lines.append(f"  - {short.upper()}: {edge_str}{flag}")

                if strong:
                    top_edges.append({
                        "player": pname,
                        "stat": short.upper(),
                        "pred": pred_val,
                        "line": line,
                        "diff": pred_val - line,
                    })

            if player_lines:
                lines.append(f"### {pname}")
                lines.extend(player_lines)
                lines.append("")

        # Top edges summary table
        if top_edges:
            top_edges.sort(key=lambda x: abs(x["diff"]), reverse=True)
            lines += ["## Top Edges (|model − line| ≥ 2.5)", ""]
            lines.append("| Player | Stat | Model | Line | Edge |")
            lines.append("|--------|------|-------|------|------|")
            for e in top_edges[:20]:
                direction = "Over" if e["diff"] > 0 else "Under"
                lines.append(
                    f"| {e['player']} | {e['stat']} | {e['pred']:.1f} | {e['line']:.1f} | {e['diff']:+.1f} {direction} |"
                )
            lines.append("")
    else:
        lines += ["_No predictions available._", ""]

    # ------------------------------------------------------------------ news
    lines += ["## Recent NBA News (Last 7 Days)", ""]
    if news is not None and not news.empty:
        for _, article in news.head(30).iterrows():
            pub = article.get("published_at", "")[:10]
            lines.append(f"- **[{pub}]** {article['title']}")
            if article.get("summary"):
                summary = str(article["summary"])[:200].replace("\n", " ")
                lines.append(f"  > {summary}")
        lines.append("")
    else:
        lines += ["_No news articles available._", ""]

    # ----------------------------------------------------------------- footer
    lines += [
        "---",
        f"_Generated {date_str} | NBA Player Prop Pipeline_",
    ]

    # ------------------------------------------------------------------ save
    content = "\n".join(lines)

    local_path = Path(tempfile.mktemp(suffix=".md"))
    local_path.write_text(content, encoding="utf-8")

    s3_key = f"reports/seed/{date_str}/seed.md"
    upload_text(content, s3_key)
    print(f"  Seed file -> {local_path} ({len(content):,} chars)")
    print(f"  Uploaded seed -> s3://{s3_key}")

    return local_path
