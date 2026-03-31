"""
Nightly pipeline — runs at midnight via Claude Code cron.

Order of operations:
  1. Crawl today's schedule (skip if none of our 5 players have a game)
  2. Crawl injury report
  3. Crawl Underdog Fantasy prop lines
  4. Run model inference

Run manually:
    python -m scheduler.nightly --now
"""

from datetime import datetime


def run_pipeline():
    print(f"\n{'='*60}")
    print(f"NBA Player Prop Pipeline — {datetime.now().isoformat()}")
    print(f"{'='*60}")
    _run()


def _run():
    # Step 1: Schedule
    print("\n[1/4] Fetching today's schedule...")
    from crawlers.schedule_crawler import run as crawl_schedule
    games = crawl_schedule()
    if games is None or games.empty:
        print("  No tracked players have games today. Pipeline exiting early.")
        return

    # Step 2: Injuries
    print("\n[2/4] Fetching injury report...")
    from crawlers.injuries import run as crawl_injuries
    crawl_injuries()

    # Step 3: Prop lines (Underdog Fantasy)
    print("\n[3/4] Fetching Underdog Fantasy prop lines...")
    from crawlers.underdog import run as crawl_underdog
    crawl_underdog()

    # Step 4: Model inference
    print("\n[4/4] Running model inference...")
    from model.predict import run as run_predictions
    run_predictions(games)

    print(f"\nPipeline complete — {datetime.now().isoformat()}")


if __name__ == "__main__":
    run_pipeline()
