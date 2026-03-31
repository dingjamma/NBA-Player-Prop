"""
NBA Prop Dashboard
Run: streamlit run dashboard.py
"""

from datetime import date
from pathlib import Path

import pandas as pd
import streamlit as st

from ingestion.s3 import DATA_DIR
from config import PLAYERS, STAT_TARGETS

# Underdog uses display names; map to our internal keys
UD_STAT_MAP = {
    "Points":           "PTS",
    "Rebounds":         "REB",
    "Assists":          "AST",
    "3-Pointers Made":  "FG3M",
}
STAT_LABELS = {
    "PTS": "Points", "REB": "Rebounds", "AST": "Assists",
    "STL": "Steals", "BLK": "Blocks",   "FG3M": "3-Pointers",
}

st.set_page_config(page_title="NBA Props", page_icon="🏀", layout="wide")
st.title("🏀 NBA Player Prop Dashboard")


def local_parquet(key: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(DATA_DIR / key)
    except Exception:
        return pd.DataFrame()


def date_key(d: date) -> str:
    return d.strftime("%Y_%m_%d")


def date_iso(d: date) -> str:
    return d.isoformat()


# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.header("Settings")
game_date = st.sidebar.date_input("Game date", value=date.today())

player_options = {p["name"]: key for key, p in PLAYERS.items()}
selected_name  = st.sidebar.selectbox("Player", list(player_options.keys()))
player_key     = player_options[selected_name]
player_cfg     = PLAYERS[player_key]

st.sidebar.markdown("---")
st.sidebar.caption("Data refreshes every 5 min")
if st.sidebar.button("Force refresh"):
    st.cache_data.clear()


# ── Load data ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_all(game_date_str: str, p_key: str):
    dk    = game_date_str.replace("-", "_")
    preds = local_parquet(f"processed/predictions/date={dk}/predictions.parquet")
    odds  = local_parquet(f"raw/odds/date={dk}/props.parquet")
    sched = local_parquet(f"raw/schedule/date={dk}/games.parquet")

    seasons = ["2023-24", "2024-25", "2025-26"]
    frames = [
        local_parquet(f"raw/game_logs/player={p_key}/season={s}/data.parquet")
        for s in seasons
    ]
    logs = pd.concat([f for f in frames if not f.empty], ignore_index=True)

    # Filter predictions and odds to selected player
    if not preds.empty and "player_name" in preds.columns:
        preds = preds[preds["player_name"] == PLAYERS[p_key]["name"]]
    if not odds.empty and "name" in odds.columns:
        last = PLAYERS[p_key]["name"].split()[-1]
        odds = odds[odds["name"].str.contains(last, case=False, na=False)]

    return preds, odds, sched, logs


preds, odds, sched, logs = load_all(date_iso(game_date), player_key)

# Determine opponent from schedule
opp = None
team_abv = player_cfg["team"]
if not sched.empty:
    row  = sched.iloc[0]
    home = row.get("HOME_TEAM_ABV", "")
    away = row.get("VISITOR_TEAM_ABV", "")
    opp  = away if home == team_abv else home


# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab2 = st.tabs(["📊 Predictions", "📈 History"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Predictions vs Underdog lines
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    if opp:
        st.subheader(f"{selected_name} — {team_abv} vs. {opp} — {date_iso(game_date)}")
    else:
        st.subheader(f"{selected_name} — {date_iso(game_date)}")

    if preds.empty:
        st.warning("No predictions found for this date. Run the pipeline first.")
    else:
        pred_row = preds.iloc[0]

        def get_line(stat: str) -> float | None:
            if odds.empty or "stat" not in odds.columns:
                return None
            # Underdog uses display names; reverse map
            ud_name = next((k for k, v in UD_STAT_MAP.items() if v == stat), None)
            if ud_name is None:
                return None
            rows = odds[odds["stat"] == ud_name]
            if rows.empty:
                return None
            return round(float(rows["line"].iloc[0]), 1)

        st.markdown("#### Model prediction vs. Underdog line")
        cols = st.columns(len(STAT_TARGETS))
        for i, stat in enumerate(STAT_TARGETS):
            pred = pred_row.get(f"pred_{stat.lower()}")
            line = get_line(stat)
            with cols[i]:
                st.metric(
                    label=STAT_LABELS[stat],
                    value=f"{pred:.1f}" if pred is not None else "—",
                    delta=f"Line {line:.1f}" if line else "No line",
                    delta_color="off",
                )
                if pred is not None and line is not None:
                    diff = pred - line
                    if diff > 1:
                        st.success(f"OVER +{diff:.1f}")
                    elif diff < -1:
                        st.error(f"UNDER {diff:.1f}")
                    else:
                        st.info("PUSH")

        # Career vs opponent
        if opp and not logs.empty and "OPP" in logs.columns:
            st.markdown("---")
            st.markdown(f"#### {selected_name} vs. {opp} — career averages")
            opp_logs = logs[logs["OPP"] == opp]
            if opp_logs.empty:
                st.info(f"No games vs. {opp} in the dataset yet.")
            else:
                c1, c2 = st.columns([1, 2])
                with c1:
                    avail = [s for s in STAT_TARGETS if s in opp_logs.columns]
                    summary = opp_logs[avail].mean().round(1).to_frame("Career Avg vs " + opp)
                    st.dataframe(summary, use_container_width=True)
                with c2:
                    show_cols = ["GAME_DATE"] + [s for s in STAT_TARGETS if s in opp_logs.columns]
                    show = opp_logs[show_cols].sort_values("GAME_DATE", ascending=False)
                    st.dataframe(show.reset_index(drop=True), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — History
# ══════════════════════════════════════════════════════════════════════════════
with tab2:
    if logs.empty:
        st.info("No game log data loaded.")
    else:
        logs_sorted = logs.sort_values("GAME_DATE", ascending=False).copy()

        st.subheader("Recent Form — Last 10 Games")
        show_cols = ["GAME_DATE", "OPP", "HOME_AWAY", "WL"] + STAT_TARGETS
        recent = logs_sorted.head(10)[[c for c in show_cols if c in logs_sorted.columns]]
        st.dataframe(recent.reset_index(drop=True), use_container_width=True)

        st.subheader("Rolling Average Chart")
        chart_stat = st.selectbox("Stat", STAT_TARGETS, key="chart_stat")
        logs_asc = logs.sort_values("GAME_DATE").copy()
        if chart_stat in logs_asc.columns:
            logs_asc["roll10"] = logs_asc[chart_stat].rolling(10, min_periods=1).mean()
            chart_df = logs_asc[["GAME_DATE", chart_stat, "roll10"]].rename(
                columns={chart_stat: "Actual", "roll10": "10-game avg"}
            ).set_index("GAME_DATE")
            st.line_chart(chart_df)

        st.subheader("All Games")
        all_cols = ["GAME_DATE", "OPP", "HOME_AWAY", "WL"] + STAT_TARGETS
        st.dataframe(
            logs_sorted[[c for c in all_cols if c in logs_sorted.columns]].reset_index(drop=True),
            use_container_width=True,
        )
