"""
Nightly video generator for Wemby AI picks.

Flow:
  1. Merge predictions + odds → picks dict
  2. Generate cinematic 9:16 background via Seedance (fal.ai)
  3. Render stats card overlay (Pillow)
  4. Composite with moviepy → data/videos/YYYY_MM_DD_wemby.mp4

Gates:
  - Skips if no Wemby prop lines in odds
  - Skips if game has already started (commence_time in the past)
  - Archives old videos from prior games on each run
"""

from __future__ import annotations

import os
import shutil
import tempfile
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
import pandas as pd

OUTPUT_DIR = Path("data/videos")

# ── Market → internal stat key ────────────────────────────────────────────────
MARKET_TO_STAT = {
    "player_points":   "PTS",
    "player_rebounds": "REB",
    "player_assists":  "AST",
    "player_steals":   "STL",
    "player_blocks":   "BLK",
    "player_threes":   "FG3M",
}

PRED_COL = {
    "PTS":  "pred_pts",
    "REB":  "pred_reb",
    "AST":  "pred_ast",
    "STL":  "pred_stl",
    "BLK":  "pred_blk",
    "FG3M": "pred_fg3m",
}

WEMBY_KEYWORDS = ["wembanyama", "wemby"]


# ── Data prep ─────────────────────────────────────────────────────────────────

def _is_wemby(name: str) -> bool:
    n = str(name).lower()
    return any(kw in n for kw in WEMBY_KEYWORDS)


def prepare_picks(
    predictions: pd.DataFrame,
    odds: pd.DataFrame,
) -> tuple[Optional[dict], Optional[dict]]:
    """
    Returns (picks, game_info) or (None, None) if data is insufficient.

    picks = {"PTS": {"ai": 24.2, "line": 21.5, "pick": "OVER"}, ...}
    game_info = {"opponent": "LAL", "home_away": "vs", "date": "Mar 22"}
    """
    if predictions is None or predictions.empty:
        print("  [video] No predictions available.")
        return None, None

    if odds is None or odds.empty:
        print("  [video] No odds available — skipping video.")
        return None, None

    wemby_odds = odds[odds["player_name"].apply(_is_wemby)].copy()
    if wemby_odds.empty:
        print("  [video] No Wemby props in odds — skipping video.")
        return None, None

    # Gate: skip if game has already started or is in the past
    if "commence_time" in wemby_odds.columns:
        raw_ct = wemby_odds["commence_time"].dropna().iloc[0] if not wemby_odds["commence_time"].dropna().empty else None
        if raw_ct is not None:
            try:
                game_time = pd.to_datetime(raw_ct, utc=True)
                now_utc   = datetime.now(timezone.utc)
                if game_time <= now_utc:
                    print(f"  [video] Game already started ({game_time.isoformat()}) — skipping video.")
                    return None, None
                print(f"  [video] Game tip-off: {game_time.strftime('%b %d %H:%M UTC')}")
            except Exception:
                pass  # if parsing fails, continue anyway

    pred_row = predictions.iloc[0]

    picks = {}
    for market, stat in MARKET_TO_STAT.items():
        market_rows = wemby_odds[wemby_odds["market"] == market]
        if market_rows.empty:
            continue

        # Consensus line: median of Over lines across bookmakers
        over_rows = market_rows[market_rows["side"].str.lower() == "over"]
        if over_rows.empty:
            continue
        line = round(float(over_rows["line"].median()), 1)

        pred_col = PRED_COL.get(stat)
        if pred_col not in pred_row.index or pd.isna(pred_row[pred_col]):
            continue
        ai_val = float(pred_row[pred_col])

        picks[stat] = {
            "ai":   round(ai_val, 1),
            "line": line,
            "pick": "OVER" if ai_val > line else "UNDER",
        }

    if not picks:
        print("  [video] No matchable picks found.")
        return None, None

    # Game info from odds (home/away teams)
    sample_row   = wemby_odds.iloc[0]
    home         = str(sample_row.get("home_team", "")).upper()
    away         = str(sample_row.get("away_team", "")).upper()
    spurs_home   = "SAN ANTONIO" in home or "SPURS" in home
    opponent_raw = away if spurs_home else home

    # Abbreviate opponent: take last word if it has spaces
    parts    = opponent_raw.strip().split()
    opponent = parts[-1] if parts else opponent_raw
    home_away = "vs" if spurs_home else "@"

    game_date = date.today() + timedelta(days=1)
    game_info = {
        "opponent": opponent,
        "home_away": home_away,
        "date": game_date.strftime("%b %-d") if os.name != "nt" else game_date.strftime("%b %d").lstrip("0"),
    }

    return picks, game_info


# ── fal.ai Seedance ───────────────────────────────────────────────────────────

def _seedance_prompt(picks: dict, game_info: dict) -> str:
    opp = game_info.get("opponent", "opponent")
    return (
        f"Victor Wembanyama NBA basketball player, cinematic slow motion, "
        f"dramatic arena spotlight, dark atmosphere, San Antonio Spurs, "
        f"crowd in background, vertical portrait framing, 4K cinematic, "
        f"no text, no graphics, photoreal"
    )


def generate_background_video(picks: dict, game_info: dict) -> Optional[Path]:
    """Calls Seedance 1.0 Lite via fal.ai REST API. Returns local .mp4 path."""
    fal_key = os.getenv("FAL_KEY")
    if not fal_key:
        print("  [video] FAL_KEY not set — skipping video generation.")
        return None

    try:
        import fal_client
    except ImportError:
        print("  [video] fal-client not installed. Run: pip install fal-client")
        return None

    prompt = _seedance_prompt(picks, game_info)
    print(f"  [video] Generating Seedance background...")
    print(f"  [video] Prompt: {prompt[:80]}...")

    # Try models in order until one succeeds
    MODELS = [
        ("fal-ai/kling-video/v3/pro",   {"prompt": prompt, "aspect_ratio": "9:16", "duration": "5s"}),
        ("fal-ai/seedance-1-0-pro",      {"prompt": prompt, "aspect_ratio": "9:16", "duration": "5s", "seed": 42}),
    ]

    result = None
    for model_id, args in MODELS:
        try:
            print(f"  [video] Trying {model_id}...")
            result = fal_client.run(model_id, arguments=args)
            print(f"  [video] {model_id} succeeded.")
            break
        except Exception as model_err:
            print(f"  [video] {model_id} failed: {model_err}")

    if result is None:
        print("  [video] All video models failed — skipping.")
        return None

    try:
        video_url = result["video"]["url"]
        print(f"  [video] Video ready. Downloading...")

        resp = requests.get(video_url, timeout=60)
        resp.raise_for_status()

        tmp = Path(tempfile.mktemp(suffix="_bg.mp4"))
        tmp.write_bytes(resp.content)
        print(f"  [video] Background saved: {tmp}")
        return tmp

    except Exception as e:
        print(f"  [video] Download/save failed: {e}")
        return None


# ── Composite ─────────────────────────────────────────────────────────────────

def composite_video(
    bg_path: Path,
    overlay_path: Path,
    output_path: Path,
) -> Optional[Path]:
    """Overlay the stats card PNG on the background video using moviepy."""
    try:
        from moviepy.editor import VideoFileClip, ImageClip, CompositeVideoClip
    except ImportError:
        print("  [video] moviepy not installed. Run: pip install moviepy")
        return None

    try:
        bg      = VideoFileClip(str(bg_path))
        overlay = (
            ImageClip(str(overlay_path))
            .set_duration(bg.duration)
            .set_position("center")
        )
        final = CompositeVideoClip([bg, overlay], size=bg.size)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        final.write_videofile(
            str(output_path),
            fps=24,
            codec="libx264",
            audio=False,
            logger=None,
        )
        bg.close()
        final.close()
        print(f"  [video] Final video: {output_path}")
        return output_path

    except Exception as e:
        print(f"  [video] Composite failed: {e}")
        return None


# ── Cleanup ───────────────────────────────────────────────────────────────────

def archive_old_videos() -> None:
    """Move videos from previous game days into data/videos/archive/."""
    today_str = datetime.now().strftime("%Y_%m_%d")
    archive   = OUTPUT_DIR / "archive"

    if not OUTPUT_DIR.exists():
        return

    for f in OUTPUT_DIR.glob("*_wemby.mp4"):
        # Filename format: YYYY_MM_DD_wemby.mp4
        if not f.name.startswith(today_str):
            archive.mkdir(parents=True, exist_ok=True)
            dest = archive / f.name
            shutil.move(str(f), str(dest))
            print(f"  [video] Archived old video: {f.name}")


# ── Entry point ───────────────────────────────────────────────────────────────

def run(predictions: pd.DataFrame, odds: pd.DataFrame) -> Optional[Path]:
    """
    Main entry point called from nightly pipeline.
    Returns output path or None if skipped/failed.
    """
    from video.card import save_card

    # Clean up any videos from previous game days first
    archive_old_videos()

    picks, game_info = prepare_picks(predictions, odds)
    if picks is None:
        return None

    print(f"  [video] {len(picks)} picks ready: " +
          ", ".join(f"{s} {v['pick']}" for s, v in picks.items()))

    # Generate Seedance background
    bg_path = generate_background_video(picks, game_info)
    if bg_path is None:
        return None

    # Render stats card overlay
    date_str     = datetime.now().strftime("%Y_%m_%d")
    overlay_path = Path(tempfile.mktemp(suffix="_overlay.png"))
    card_path    = save_card(picks, game_info, overlay_path)

    # Composite
    output_path = OUTPUT_DIR / f"{date_str}_wemby.mp4"
    result      = composite_video(bg_path, card_path, output_path)

    # Cleanup temp files
    bg_path.unlink(missing_ok=True)
    overlay_path.unlink(missing_ok=True)

    return result
