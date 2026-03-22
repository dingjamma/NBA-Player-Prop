"""
Crawl NBA news from Google News RSS (last 7 days).
No API key needed. Returns structured list of articles.
"""

import feedparser
import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

from ingestion.s3 import upload_parquet

# Google News RSS — NBA search
GOOGLE_NEWS_URL = "https://news.google.com/rss/search?q=Wembanyama&hl=en-US&gl=US&ceid=US:en"
ESPN_RSS = "https://www.espn.com/espn/rss/nba/news"

FEEDS = [
    ("google_news", GOOGLE_NEWS_URL),
    ("espn",        ESPN_RSS),
]

WEMBY_KEYWORDS = ["wembanyama", "wemby", "victor", "spurs"]


def _is_wemby_related(title: str, summary: str) -> bool:
    text = (title + " " + summary).lower()
    return any(kw in text for kw in WEMBY_KEYWORDS)


def parse_feed(name: str, url: str, cutoff: datetime) -> list[dict]:
    articles = []
    try:
        feed = feedparser.parse(url)
        for entry in feed.entries:
            try:
                pub = parsedate_to_datetime(entry.published)
                if pub.tzinfo is None:
                    pub = pub.replace(tzinfo=timezone.utc)
                if pub < cutoff:
                    continue
                articles.append({
                    "source": name,
                    "title": entry.get("title", ""),
                    "summary": entry.get("summary", ""),
                    "url": entry.get("link", ""),
                    "published_at": pub.isoformat(),
                })
            except Exception:
                continue
    except Exception as e:
        print(f"[WARN] Feed {name} failed: {e}")
    return articles


def run() -> pd.DataFrame:
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    all_articles = []

    for name, url in FEEDS:
        articles = parse_feed(name, url, cutoff)
        print(f"  {name}: {len(articles)} articles in last 7 days")
        all_articles.extend(articles)

    df = pd.DataFrame(all_articles)
    if df.empty:
        print("[WARN] No news articles found.")
        return df

    # Keep only Wemby-related articles (for ESPN which covers all NBA)
    df = df[df.apply(lambda r: _is_wemby_related(r["title"], r["summary"]), axis=1)].copy()

    # Deduplicate by title
    df = df.drop_duplicates(subset=["title"]).reset_index(drop=True)
    print(f"  Total unique articles: {len(df)}")

    date_str = datetime.now().strftime("%Y_%m_%d")
    import tempfile, pathlib
    tmp = pathlib.Path(tempfile.mktemp(suffix=".parquet"))
    df.to_parquet(tmp, index=False)
    upload_parquet(tmp, f"raw/news/date={date_str}/articles.parquet")
    print(f"  Uploaded news -> S3")

    return df


if __name__ == "__main__":
    run()
