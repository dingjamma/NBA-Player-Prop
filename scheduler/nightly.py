"""
Nightly pipeline — runs at midnight via Claude Code cron.

Order of operations:
  1. Crawl tomorrow's schedule (skip if no Spurs game)
  2. Crawl injury report
  3. Crawl prop lines (The Odds API)
  5. Run model inference (Wemby only)
  6. Build seed file for MiroFish
  7. Trigger MiroFish (http://localhost:5001)

Run manually:
    python -m scheduler.nightly --now
"""

import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()


def run_pipeline():
    print(f"\n{'='*60}")
    print(f"NBA Player Prop Pipeline — {datetime.now().isoformat()}")
    print(f"{'='*60}")
    _run()


def _run():
    # Step 1: Schedule
    print("\n[1/6] Fetching today's schedule...")
    from crawlers.schedule_crawler import run as crawl_schedule
    games = crawl_schedule()
    if games is None or games.empty:
        print("  No Spurs game today. Pipeline exiting early.")
        return

    # Step 2: Injuries
    print("\n[2/4] Fetching injury report...")
    from crawlers.injuries import run as crawl_injuries
    crawl_injuries()

    # Step 3: Prop lines
    print("\n[3/4] Fetching prop lines...")
    from crawlers.odds import run as crawl_odds
    odds = crawl_odds()

    # Step 4: Model inference
    print("\n[4/5] Running model inference...")
    from model.predict import run as run_predictions
    predictions = run_predictions(games)

    # Step 5: Generate prediction video (only if Wemby prop lines exist)
    if odds is not None and not odds.empty:
        print("\n[5/5] Generating prediction video...")
        from video.generator import run as generate_video
        video_path = generate_video(predictions, odds)
        if video_path:
            print(f"  Video ready: {video_path}")
    else:
        print("\n[5/5] No odds available — skipping video generation.")

    print(f"\nPipeline complete — {datetime.now().isoformat()}")


if __name__ == "__main__":
    import sys
    if "--now" in sys.argv:
        run_pipeline()
    else:
        run_pipeline()
