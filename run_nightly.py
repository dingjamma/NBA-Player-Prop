"""
Local nightly pipeline runner.
Schedule this in Windows Task Scheduler at midnight.

Or run manually:
    python run_nightly.py
"""

import os
import sys
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

# Force UTF-8 on Windows console so Chinese/special chars don't crash
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from scheduler.nightly import run_pipeline

if __name__ == "__main__":
    print(f"Starting pipeline at {datetime.now().isoformat()}")
    run_pipeline()
