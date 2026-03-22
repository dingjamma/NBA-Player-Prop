# NBA Player Prop Predictor

Fully automated nightly pipeline predicting Victor Wembanyama's prop bets using XGBoost and multi-agent simulation. Runs every night at midnight via Claude Code cron.

---

## What It Does

San Antonio Spurs games are the focus. Every night before a Spurs game:

1. Checks if the Spurs play tomorrow — exits early if not
2. Pulls the NBA injury report
3. Fetches live prop lines from The Odds API (DraftKings, FanDuel, BetMGM, Pinnacle)
4. Scrapes Wemby/Spurs news from the last 7 days
5. Runs XGBoost inference across 6 stat categories
6. Builds a structured seed file combining all data
7. Feeds the seed into **MiroFish** — a multi-agent simulation that produces a final prediction report

---

## Full Pipeline

```
00:02 AM — Claude Code cron fires → run_nightly.py
  │
  ├─ [1] Schedule crawler     → Spurs game tomorrow?
  ├─ [2] Injury crawler       → who's out?
  ├─ [3] Odds crawler         → prop lines from The Odds API
  ├─ [4] News crawler         → last 7 days of Wemby/Spurs news
  ├─ [5] Model inference      → XGBoost: PTS / REB / AST / STL / BLK / FG3M
  ├─ [6] Seed builder         → packages everything into seed.md
  └─ [7] MiroFish trigger     → multi-agent simulation → report_en.md + report_zh.md
                                        │
                                   data/reports/
                                        │
                              streamlit run dashboard.py
```

---

## Models

Three XGBoost regressors trained on Wemby's ESPN game logs (2023–24, 2024–25 seasons).

**45 features per prediction:**
- Rolling averages: 3, 5, 10 game windows for all stats
- Opponent difficulty (defensive rating, pace)
- Season averages and home/away splits
- Recent form momentum

**Targets:** PTS · REB · AST · STL · BLK · FG3M

**Sample predictions (March 2026):**
| Stat | Prediction |
|------|-----------|
| PTS  | 13.4 |
| REB  | 8.07 |
| AST  | 4.55 |
| STL  | 2.48 |
| BLK  | 2.19 |
| FG3M | 2.43 |

---

## Dashboard

```bash
streamlit run dashboard.py
```

- **Tab 1** — Model predictions vs. Vegas lines (OVER/UNDER/PUSH)
- **Tab 2** — MiroFish simulation report (English + Chinese)
- **Tab 3** — Wemby career history + rolling averages vs opponent
- **Tab 4** — Latest news

---

## Data Sources

| Source | Data |
|--------|------|
| ESPN public API | Wemby game logs (2023–present) |
| The Odds API | Live prop lines (4 sportsbooks) |
| NBA official PDF | Injury report |
| RSS feeds | Wemby/Spurs news (last 7 days) |
| ESPN schedule API | Tomorrow's Spurs game |

---

## Project Structure

```
NBA-Player-Prop/
├── crawlers/
│   ├── schedule_crawler.py   ← ESPN schedule, Spurs filter
│   ├── injuries.py           ← NBA injury report PDF
│   ├── odds.py               ← The Odds API prop lines
│   ├── news.py               ← RSS news scraper
│   └── historical.py         ← one-time historical game log fetch
├── model/
│   ├── features.py           ← 45-feature engineering
│   ├── train.py              ← XGBoost training (6 models)
│   └── predict.py            ← nightly inference from ESPN live data
├── report/
│   ├── seed_builder.py       ← builds MiroFish seed file
│   └── stats_report.py       ← quick stats table
├── mirofish/
│   └── trigger.py            ← MiroFish API automation (8-step pipeline)
├── ingestion/
│   └── s3.py                 ← local file store (data/ folder)
├── scheduler/
│   └── nightly.py            ← pipeline orchestrator
├── dashboard.py              ← Streamlit dashboard
├── run_nightly.py            ← entry point (called by cron)
└── data/                     ← all local data (gitignored)
    ├── raw/                  ← crawled data (parquet)
    ├── processed/            ← predictions (parquet)
    ├── models/               ← trained XGBoost models
    └── reports/              ← seed files + MiroFish reports
```

---

## Setup

```bash
pip install -r requirements.txt
```

Create a `.env` file (see `.env.example`):

```
ODDS_API_KEY=your_key
MIROFISH_BASE_URL=http://localhost:5001
LLM_API_KEY=your_qwen_key
LLM_BASE_URL=https://dashscope.aliyuncs.com/compatible-mode/v1
LLM_MODEL_NAME=qwen-plus
ZEP_API_KEY=your_zep_key
```

Train models (one-time):
```bash
python src/crawlers/historical.py   # fetch historical game logs
python -m model.train               # train all 6 XGBoost models
```

Run manually:
```bash
python run_nightly.py
```

---

## Tech Stack

`Python` · `XGBoost` · `Pandas` · `Streamlit` · `ESPN API` · `The Odds API` · `MiroFish` · `Zep GraphRAG` · `qwen-plus` · `Claude Code`
