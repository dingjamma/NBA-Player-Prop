"""
Crawl the official NBA injury report (PDF) and parse it into a DataFrame.
NBA releases the report ~1.5 hours before first tip-off each game day.

PDF URL pattern:
  https://ak-static.cms.nba.com/referee/injury/Injury-Report_{DATE}_{TIME}.pdf
  where TIME is typically 05:30PM ET

We fetch the latest available report for tomorrow's date.
"""

import io
import re
import requests
import pdfplumber
import pandas as pd
from datetime import date, timedelta
from pathlib import Path

from ingestion.s3 import upload_parquet

NBA_INJURY_BASE = "https://ak-static.cms.nba.com/referee/injury"
# Try multiple common release times
REPORT_TIMES = ["05:30PM", "06:00PM", "04:00PM", "07:00PM"]


def build_url(report_date: date, time_str: str) -> str:
    date_str = report_date.strftime("%Y-%m-%d")
    return f"{NBA_INJURY_BASE}/Injury-Report_{date_str}_{time_str}.pdf"


def fetch_injury_pdf(report_date: date) -> bytes | None:
    for t in REPORT_TIMES:
        url = build_url(report_date, t)
        try:
            resp = requests.get(url, timeout=15)
            if resp.status_code == 200:
                print(f"  Found injury report: {url}")
                return resp.content
        except Exception:
            continue
    return None


def parse_pdf(pdf_bytes: bytes) -> pd.DataFrame:
    rows = []
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page in pdf.pages:
            table = page.extract_table()
            if not table:
                continue
            for row in table[1:]:  # skip header
                if row and len(row) >= 5:
                    rows.append({
                        "game_date": row[0],
                        "game_time": row[1],
                        "matchup": row[2],
                        "team": row[3],
                        "player_name": row[4],
                        "status": row[5] if len(row) > 5 else None,
                        "reason": row[6] if len(row) > 6 else None,
                    })
    return pd.DataFrame(rows)


def run():
    target = date.today() + timedelta(days=1)
    print(f"Fetching injury report for {target}")

    pdf_bytes = fetch_injury_pdf(target)
    if not pdf_bytes:
        # Fall back to today's report
        pdf_bytes = fetch_injury_pdf(date.today())
        if not pdf_bytes:
            print("[WARN] No injury report found.")
            return pd.DataFrame()

    df = parse_pdf(pdf_bytes)
    print(f"  Parsed {len(df)} injury entries")

    if df.empty:
        return df

    date_str = target.strftime("%Y_%m_%d")
    import tempfile, pathlib
    tmp = pathlib.Path(tempfile.mktemp(suffix=".parquet"))
    df.to_parquet(tmp, index=False)
    upload_parquet(tmp, f"raw/injuries/date={date_str}/injury_report.parquet")
    print(f"  Uploaded injury report -> S3")

    return df


if __name__ == "__main__":
    run()
