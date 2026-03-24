"""
AWS Lambda handler for the Wemby nightly pipeline.

Two EventBridge rules trigger this:
  - 9am ET  (13:00 UTC) → event["job"] = "pipeline"
  - 5:59am UTC next day (11:59pm MT) → event["job"] = "results"
"""

import os
import sys
from pathlib import Path

# Lambda writes to /tmp
os.environ.setdefault("DATA_DIR", "/tmp/data")


def handler(event: dict, context) -> dict:
    job = event.get("job", "pipeline")

    if job == "pipeline":
        return _run_pipeline()
    elif job == "results":
        return _run_results()
    else:
        return {"status": "error", "message": f"Unknown job: {job}"}


def _run_pipeline() -> dict:
    try:
        from scheduler.nightly import run_pipeline
        run_pipeline()
        return {"status": "ok", "job": "pipeline"}
    except Exception as e:
        print(f"Pipeline error: {e}")
        return {"status": "error", "job": "pipeline", "message": str(e)}


def _run_results() -> dict:
    try:
        from datetime import date
        import fetch_results
        fetch_results.run(date.today())
        return {"status": "ok", "job": "results"}
    except Exception as e:
        print(f"Results error: {e}")
        return {"status": "error", "job": "results", "message": str(e)}
