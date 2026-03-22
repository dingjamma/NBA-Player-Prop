"""
Trigger the full MiroFish pipeline with the NBA seed file.

Flow:
  1. POST /api/graph/ontology/generate  — upload seed.md → project_id
  2. POST /api/graph/build              — build Zep graph → task_id
  3. Poll  GET  /api/graph/task/{id}    — wait for graph build
  4. POST /api/simulation/create        — create simulation → simulation_id
  5. POST /api/simulation/prepare/{id} — generate agent profiles
  6. POST /api/simulation/start         — run simulation
  7. Poll  GET  /api/simulation/{id}    — wait for simulation
  8. POST /api/report/generate          — generate report → report_id
  9. Poll  GET  /api/report/{id}/progress
  10. GET  /api/report/{id}             — fetch markdown (Chinese)
  11. Translate to English via LLM
  12. Upload both reports to S3

MiroFish backend must be running at MIROFISH_BASE_URL (default: http://localhost:5001)
"""

import os
import time
import requests
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
from ingestion.s3 import upload_text

load_dotenv()

SIMULATION_REQUIREMENT = (
    "Based on the player statistics, model predictions, injury reports, and recent news, "
    "simulate how NBA players will perform tonight relative to their prop lines. "
    "Identify the top Over and Under opportunities, accounting for matchup difficulty, "
    "injury impact on teammates, and recent form trends. "
    "Generate a structured prediction report with confidence levels."
)

POLL_INTERVAL = 10   # seconds between status checks
POLL_TIMEOUT  = 600  # max seconds to wait per step


def _api() -> str:
    return f"{os.environ.get('MIROFISH_BASE_URL', 'http://localhost:5001')}/api"


def _post(endpoint: str, **kwargs) -> dict:
    resp = requests.post(f"{_api()}{endpoint}", **kwargs)
    resp.raise_for_status()
    return resp.json()


def _get(endpoint: str, **kwargs) -> dict:
    resp = requests.get(f"{_api()}{endpoint}", **kwargs)
    resp.raise_for_status()
    return resp.json()


def _poll_task(task_id: str, label: str) -> dict:
    """Poll /api/graph/task/{task_id} until completed or failed."""
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        data = _get(f"/graph/task/{task_id}")["data"]
        status = data.get("status")
        progress = data.get("progress", 0)
        message = data.get("message", "")
        print(f"  [{label}] {status} {progress}% — {message}")
        if status == "completed":
            return data
        if status == "failed":
            raise RuntimeError(f"{label} failed: {data.get('error')}")
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"{label} timed out after {POLL_TIMEOUT}s")


def _poll_prepare(simulation_id: str, task_id: str) -> dict:
    """Poll prepare status until ready."""
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        data = _post("/simulation/prepare/status", json={
            "task_id": task_id,
            "simulation_id": simulation_id,
        })["data"]
        status = data.get("status")
        progress = data.get("progress", 0)
        message = data.get("message", "")
        print(f"  [prepare] {status} {progress}% — {message}")
        if status in ("completed", "ready"):
            return data
        if status == "failed":
            raise RuntimeError(f"Prepare failed: {data.get('message')}")
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Prepare timed out after {POLL_TIMEOUT}s")


def _poll_simulation(simulation_id: str) -> dict:
    """Poll simulation run-status until completed or failed."""
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        data = _get(f"/simulation/{simulation_id}/run-status")["data"]
        status = data.get("runner_status")
        round_num = data.get("current_round", 0)
        total_rounds = data.get("total_rounds", "?")
        pct = data.get("progress_percent", 0)
        print(f"  [simulation] {status} round {round_num}/{total_rounds} ({pct:.1f}%)")
        if status in ("completed", "finished", "idle") and round_num > 0:
            return data
        if status == "failed":
            raise RuntimeError(f"Simulation failed")
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Simulation timed out after {POLL_TIMEOUT}s")


def _poll_report(report_id: str, simulation_id: str) -> str:
    """Poll report generation until complete. Returns markdown content."""
    deadline = time.time() + POLL_TIMEOUT
    while time.time() < deadline:
        data = _post("/report/generate/status", json={
            "task_id": report_id,
            "simulation_id": simulation_id,
        })["data"]
        status = data.get("status")
        progress = data.get("progress", 0)
        message = data.get("message", "")
        print(f"  [report] {status} {progress}% — {message}")
        if status == "completed":
            # Fetch report by simulation_id
            report = _get(f"/report/by-simulation/{simulation_id}")["data"]
            return report.get("markdown_content", "")
        if status == "failed":
            raise RuntimeError(f"Report generation failed")
        time.sleep(POLL_INTERVAL)
    raise TimeoutError(f"Report generation timed out after {POLL_TIMEOUT}s")


def _translate_to_english(chinese_text: str) -> str:
    """Translate the Chinese report to English using the configured LLM."""
    from openai import OpenAI

    client = OpenAI(
        api_key=os.getenv("LLM_API_KEY"),
        base_url=os.getenv("LLM_BASE_URL"),
    )
    model = os.getenv("LLM_MODEL_NAME", "qwen-plus")

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a professional sports analyst translator. "
                    "Translate the following Chinese NBA prediction report to fluent English. "
                    "Preserve all Markdown formatting, tables, and headings. "
                    "Keep player names, team names, and statistical terms in English."
                ),
            },
            {"role": "user", "content": chinese_text},
        ],
        temperature=0.3,
    )
    return resp.choices[0].message.content


def run(seed_path: Path):
    tomorrow = (date.today() + timedelta(days=1)).strftime("%Y-%m-%d")
    project_name = f"NBA Props {tomorrow}"

    print(f"\n[MiroFish] Starting pipeline for {tomorrow}")
    print(f"  Seed file: {seed_path}")

    # ── Step 1: Upload seed → ontology ───────────────────────────────────────
    print("\n[1/8] Uploading seed file & generating ontology...")
    with open(seed_path, "rb") as f:
        resp = _post(
            "/graph/ontology/generate",
            files={"files": (seed_path.name, f, "text/markdown")},
            data={
                "simulation_requirement": SIMULATION_REQUIREMENT,
                "project_name": project_name,
            },
        )
    project_id = resp["data"]["project_id"]
    print(f"  project_id: {project_id}")

    # ── Step 2: Build graph ───────────────────────────────────────────────────
    print("\n[2/8] Building Zep graph...")
    resp = _post("/graph/build", json={
        "project_id": project_id,
        "graph_name": project_name,
    })
    task_id = resp["data"]["task_id"]
    _poll_task(task_id, "graph_build")

    # ── Step 3: Create simulation ─────────────────────────────────────────────
    print("\n[3/8] Creating simulation...")
    resp = _post("/simulation/create", json={"project_id": project_id})
    simulation_id = resp["data"]["simulation_id"]
    print(f"  simulation_id: {simulation_id}")

    # ── Step 4: Prepare agents ────────────────────────────────────────────────
    print("\n[4/8] Preparing agent profiles...")
    resp = _post("/simulation/prepare", json={
        "simulation_id": simulation_id,
        "use_llm_for_profiles": True,
    })
    prep_task_id = resp["data"].get("task_id")
    if prep_task_id:
        _poll_prepare(simulation_id, prep_task_id)

    # ── Step 5: Start simulation ──────────────────────────────────────────────
    print("\n[5/8] Starting simulation...")
    _post("/simulation/start", json={
        "simulation_id": simulation_id,
        "platform": "parallel",
        "max_rounds": 50,
        "enable_graph_memory_update": False,
    })
    _poll_simulation(simulation_id)

    # ── Step 6: Generate report ───────────────────────────────────────────────
    print("\n[6/8] Generating MiroFish report (Chinese)...")
    resp = _post("/report/generate", json={"simulation_id": simulation_id})
    report_id = resp["data"].get("report_id") or resp["data"].get("task_id")
    chinese_report = _poll_report(report_id, simulation_id)
    print(f"  Report length: {len(chinese_report):,} chars")

    # ── Step 7: Translate to English ──────────────────────────────────────────
    print("\n[7/8] Translating report to English...")
    english_report = _translate_to_english(chinese_report)

    # ── Step 8: Save both reports locally ────────────────────────────────────
    print("\n[8/8] Saving reports...")
    upload_text(chinese_report,  f"reports/final/{tomorrow}/report_zh.md")
    upload_text(english_report,  f"reports/final/{tomorrow}/report_en.md")
    print(f"  Saved: data/reports/final/{tomorrow}/report_zh.md")
    print(f"  Saved: data/reports/final/{tomorrow}/report_en.md")

    print(f"\n[MiroFish] Pipeline complete for {tomorrow}")
    return {
        "project_id": project_id,
        "simulation_id": simulation_id,
        "report_id": report_id,
        "chinese_report": chinese_report,
        "english_report": english_report,
    }


if __name__ == "__main__":
    # Test with a manually provided seed file
    import sys
    path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data/seed_test.md")
    run(path)
