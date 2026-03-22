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

# Add project root to path
sys.path.insert(0, os.path.dirname(__file__))

from scheduler.nightly import run_pipeline

if __name__ == "__main__":
    print(f"Starting pipeline at {datetime.now().isoformat()}")
    run_pipeline()
