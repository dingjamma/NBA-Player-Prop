# NBA Player Prop Predictor — Expansion Proposal

## Goal
Expand from Wemby-only predictions to 5 star players, replace The Odds API with Underdog Fantasy, and clean up dead code from earlier experiments.

**Target players:**
| Player | Team | Why |
|--------|------|-----|
| Victor Wembanyama | SAS | Already working, proven accuracy |
| Nikola Jokic | DEN | Triple-double machine, consistent stat lines |
| Luka Doncic | LAL | High usage, predictable volume |
| Shai Gilgeous-Alexander | OKC | MVP-caliber, steady production |
| Giannis Antetokounmpo | MIL | Elite stats, reliable floor |

These are all high-usage stars with consistent game-to-game stat profiles — exactly what XGBoost handles well.

---

## 3 Phases

### Phase 0 — Cleanup (1-2 days)

Remove dead code and unused dependencies from earlier MiroFish/video experiments.

**Remove:**
- `video/` directory (fal.ai Seedance video generation)
- `mirofish/` directory (dead since commit 62d4d14)
- `report/seed_builder.py` (runs but output is unused)
- `crawlers/news.py` (removed from pipeline in commit 1959af0)
- Dead deps from `requirements.txt`: `fal-client`, `moviepy`, `Pillow`, `feedparser`, `openai`, `schedule`, `nba_api`
- MiroFish/.env keys: `MIROFISH_BASE_URL`, `LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL_NAME`, `ZEP_API_KEY`, `FAL_KEY`

**Update:**
- `README.md` to reflect current state (no video, no MiroFish)
- `.env.example` to remove dead keys
- `scheduler/nightly.py` to remove video generation step
- `dashboard.py` to remove video player

**Success criteria:**
- [ ] No dead imports or unused files
- [ ] `pip install -r requirements.txt` installs only what's needed
- [ ] Pipeline still runs end-to-end for Wemby

---

### Phase 1 — Underdog Fantasy Integration (3-5 days)

Replace The Odds API with Underdog Fantasy API for prop lines.

**Why switch:**
- Underdog is the primary platform we care about
- Lines come directly from where bets are placed — no translation needed
- The Odds API free tier (500 req/month) is limiting

**Tasks:**
- [ ] Research Underdog Fantasy API (auth, endpoints, rate limits)
- [ ] Build `crawlers/underdog.py` — fetch player prop lines for target players
- [ ] Map Underdog stat categories to our 6 targets (PTS, REB, AST, STL, BLK, FG3M)
- [ ] Update `scheduler/nightly.py` to call Underdog instead of The Odds API
- [ ] Update dashboard to show Underdog lines instead of multi-book median
- [ ] Keep `crawlers/odds.py` as fallback but remove from default pipeline

**Success criteria:**
- [ ] Nightly pipeline fetches Underdog lines for all 5 players
- [ ] Dashboard shows Underdog line vs model prediction

---

### Phase 2 — Expand to 5 Players (1-2 weeks)

Generalize everything from Wemby-only to N players.

**Crawlers:**
- [ ] `crawlers/historical.py` — accept a player ID list, fetch game logs for all 5 players
- [ ] `crawlers/schedule_crawler.py` — check if ANY of the 5 players' teams play today (not just Spurs)

**Feature engineering:**
- [ ] `model/features.py` — same 45-feature pipeline, parameterized by player
- [ ] Each player gets their own rolling averages, opponent history, rest days, etc.
- [ ] Consider adding player-specific features (e.g., Jokic assist rate is more predictive than Giannis's)

**Model training:**
- [ ] Train 6 XGBoost models PER player (30 models total)
- [ ] Store as `data/models/{player_id}/{PTS,REB,AST,STL,BLK,FG3M}/model.json`
- [ ] Validate with time-series CV, compare MAE across players
- [ ] Players with fewer seasons of data (Wemby) vs more (Jokic) may need different hyperparameters

**Inference:**
- [ ] `model/predict.py` — loop over all players with games today, run their models
- [ ] Save predictions keyed by player + date

**Dashboard:**
- [ ] Player selector dropdown (or show all 5)
- [ ] Per-player predictions vs Underdog lines
- [ ] Per-player career history + rolling averages

**Success criteria:**
- [ ] Models trained for all 5 players with MAE comparable to Wemby's
- [ ] Nightly pipeline produces predictions for every player with a game that day
- [ ] Dashboard shows all 5 players

---

## Updated Directory Structure

```
NBA-Player-Prop/
├── crawlers/
│   ├── schedule_crawler.py   ← check if any target player's team plays
│   ├── injuries.py           ← NBA injury report (unchanged)
│   ├── underdog.py           ← NEW: Underdog Fantasy prop lines
│   ├── odds.py               ← KEPT as fallback, not in default pipeline
│   └── historical.py         ← MODIFIED: multi-player game log fetch
├── model/
│   ├── features.py           ← MODIFIED: parameterized by player
│   ├── train.py              ← MODIFIED: train per player
│   └── predict.py            ← MODIFIED: predict for all players with games today
├── report/
│   └── stats_report.py       ← quick stats (unchanged)
├── ingestion/
│   └── s3.py                 ← local file store (unchanged)
├── scheduler/
│   └── nightly.py            ← MODIFIED: multi-player pipeline
├── dashboard.py              ← MODIFIED: multi-player UI
├── run_nightly.py            ← entry point (unchanged)
├── fetch_results.py          ← MODIFIED: track results for all 5 players
├── config.py                 ← NEW: player roster + IDs
└── data/
    ├── raw/
    │   ├── odds/             ← Underdog lines by date
    │   └── gamelogs/         ← per-player historical data
    ├── processed/
    │   └── predictions/      ← per-player predictions by date
    ├── models/
    │   └── {player_id}/      ← 6 models per player
    └── results/
        └── results.csv       ← actuals vs predictions for all players
```

---

## Risk Register

| Risk | Severity | Mitigation |
|------|----------|------------|
| Underdog API access is restricted or undocumented | High | Research API first in Phase 1; keep The Odds API as fallback |
| Players with less history (Wemby: 2 seasons) produce weaker models | Medium | Monitor MAE per player; fall back to league averages for sparse features |
| stats.nba.com rate limits or blocks scraping | Medium | Add request delays, cache aggressively, keep ESPN as backup |
| 30 models = longer training time | Low | Still fast — XGBoost trains in seconds per model |
| Schedule crawler misses games (API changes) | Medium | Cross-check with multiple sources; alert on zero-game days during season |

---

## What NOT to Build
- LLM agent simulation (overkill — XGBoost is working)
- Video generation (not needed for personal use)
- Social media posting / content pipeline
- Multi-sport expansion (nail 5 NBA players first)
- Custom frontend (Streamlit is fine)
- Real-time in-game predictions
- Betting execution / auto-placing bets

---

## Player Config

```python
# config.py
PLAYERS = {
    "wembanyama": {
        "name": "Victor Wembanyama",
        "nba_id": 1641705,
        "team": "SAS",
    },
    "jokic": {
        "name": "Nikola Jokic",
        "nba_id": 203999,
        "team": "DEN",
    },
    "doncic": {
        "name": "Luka Doncic",
        "nba_id": 1629029,
        "team": "LAL",
    },
    "sga": {
        "name": "Shai Gilgeous-Alexander",
        "nba_id": 1628983,
        "team": "OKC",
    },
    "giannis": {
        "name": "Giannis Antetokounmpo",
        "nba_id": 203507,
        "team": "MIL",
    },
}

STAT_TARGETS = ["PTS", "REB", "AST", "STL", "BLK", "FG3M"]
```
