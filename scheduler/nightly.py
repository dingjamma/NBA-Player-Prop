"""
Nightly pipeline — runs at midnight via Claude Code cron.

Order of operations:
  1. Crawl tomorrow's schedule (skip if no Spurs game)
  2. Crawl injury report
  3. Crawl prop lines (The Odds API)
  4. Crawl news (last 7 days)
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
    print("\n[1/7] Fetching tomorrow's schedule...")
    from crawlers.schedule_crawler import run as crawl_schedule
    games = crawl_schedule()
    if games is None or games.empty:
        print("  No Spurs game tomorrow. Pipeline exiting early.")
        return

    # Step 2: Injuries
    print("\n[2/7] Fetching injury report...")
    from crawlers.injuries import run as crawl_injuries
    injuries = crawl_injuries()

    # Step 3: Prop lines
    print("\n[3/7] Fetching prop lines...")
    from crawlers.odds import run as crawl_odds
    odds = crawl_odds()

    # Step 4: News
    print("\n[4/7] Crawling news (last 7 days)...")
    from crawlers.news import run as crawl_news
    news = crawl_news()

    # Step 5: Model inference
    print("\n[5/7] Running model inference...")
    from model.predict import run as run_predictions
    predictions = run_predictions(games)

    # Step 6: Build seed file
    print("\n[6/7] Building MiroFish seed file...")
    from report.seed_builder import build as build_seed
    seed_path = build_seed(predictions, odds, injuries, news)

    # Step 7: Trigger MiroFish (running locally on port 5001)
    print("\n[7/7] Triggering MiroFish simulation...")
    mirofish_url = os.getenv("MIROFISH_BASE_URL", "http://localhost:5001")
    os.environ["MIROFISH_BASE_URL"] = mirofish_url
    from mirofish.trigger import run as trigger_mirofish
    trigger_mirofish(seed_path)

    print(f"\nPipeline complete — {datetime.now().isoformat()}")


if __name__ == "__main__":
    import sys
    if "--now" in sys.argv:
        run_pipeline()
    else:
        run_pipeline()
