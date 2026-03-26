"""
Wemby Prop Dashboard
Run: streamlit run dashboard.py
"""

from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st

from ingestion.s3 import DATA_DIR

TARGET_STATS = ["PTS", "REB", "AST", "STL", "BLK", "FG3M"]
STAT_LABELS  = {"PTS": "Points", "REB": "Rebounds", "AST": "Assists",
                "STL": "Steals",  "BLK": "Blocks",   "FG3M": "3-Pointers"}

st.set_page_config(page_title="Wemby Props", page_icon="🏀", layout="wide")
st.title("🏀 Victor Wembanyama — Prop Dashboard")


def local_parquet(key: str) -> pd.DataFrame:
    try:
        return pd.read_parquet(DATA_DIR / key)
    except Exception:
        return pd.DataFrame()


def local_text(key: str) -> str:
    try:
        return (DATA_DIR / key).read_text(encoding="utf-8")
    except Exception:
        return ""


def date_key(d: date) -> str:
    return d.strftime("%Y_%m_%d")


def date_iso(d: date) -> str:
    return d.isoformat()


# ── Sidebar ────────────────────────────────────────────────────────────────────
st.sidebar.header("Settings")
game_date = st.sidebar.date_input("Game date", value=date.today())
st.sidebar.markdown("---")
st.sidebar.caption("Data refreshes every 5 min")
if st.sidebar.button("Force refresh"):
    st.cache_data.clear()

# ── Load data ──────────────────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def load_all(game_date_str: str):
    dk = game_date_str.replace("-", "_")
    preds = local_parquet(f"processed/predictions/date={dk}/predictions.parquet")
    odds  = local_parquet(f"raw/odds/date={dk}/props.parquet")
    news  = local_parquet(f"raw/news/date={dk}/articles.parquet")
    sched = local_parquet(f"raw/schedule/date={dk}/games.parquet")
    logs  = pd.concat([
        local_parquet("raw/game_logs/player=wembanyama/season=2023-24/data.parquet"),
        local_parquet("raw/game_logs/player=wembanyama/season=2024-25/data.parquet"),
        local_parquet("raw/game_logs/player=wembanyama/season=2025-26/data.parquet"),
    ], ignore_index=True)
    if not odds.empty and "player_name" in odds.columns:
        odds = odds[odds["player_name"].str.contains("Wembanyama", na=False)]
    return preds, odds, news, sched, logs


preds, odds, news, sched, logs = load_all(date_iso(game_date))

# Figure out opponent
opp = None
if not sched.empty:
    row  = sched.iloc[0]
    home = row.get("HOME_TEAM_ABV", "")
    away = row.get("VISITOR_TEAM_ABV", "")
    opp  = away if home == "SA" else home

# ── Tabs ───────────────────────────────────────────────────────────────────────
tab1, tab3, tab4 = st.tabs(["📊 Predictions", "📈 History", "📰 News"])


# ══════════════════════════════════════════════════════════════════════════════
# TAB 1 — Predictions vs Props
# ══════════════════════════════════════════════════════════════════════════════
with tab1:
    if opp:
        st.subheader(f"San Antonio Spurs vs. {opp} — {date_iso(game_date)}")
    else:
        st.subheader(f"Game date: {date_iso(game_date)}")

    if preds.empty:
        st.warning("No predictions found for this date. Run the pipeline first.")
    else:
        pred_row = preds.iloc[0]

        def get_line(stat: str):
            market_map = {
                "PTS": "player_points", "REB": "player_rebounds",
                "AST": "player_assists",  "BLK": "player_blocks",
                "STL": "player_steals",   "FG3M": "player_threes",
            }
            if odds.empty:
                return None
            rows = odds[odds["market"] == market_map.get(stat, "")]
            if rows.empty:
                return None
            return round(rows["line"].dropna().astype(float).median(), 1)

        st.markdown("#### Model prediction vs. Vegas line")
        cols = st.columns(len(TARGET_STATS))
        for i, stat in enumerate(TARGET_STATS):
            pred = pred_row.get(f"pred_{stat.lower()}")
            line = get_line(stat)
            with cols[i]:
                st.metric(
                    label=STAT_LABELS[stat],
                    value=f"{pred:.1f}" if pred is not None else "—",
                    delta=f"Line {line:.1f}" if line else "No line yet",
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

        # Historical vs opponent
        if opp and not logs.empty:
            st.markdown("---")
            st.markdown(f"#### Wemby vs. {opp} — career averages")
            opp_logs = logs[logs["OPP"] == opp]
            if opp_logs.empty:
                st.info(f"No games vs. {opp} in the dataset yet.")
            else:
                c1, c2 = st.columns([1, 2])
                with c1:
                    summary = opp_logs[TARGET_STATS].mean().round(1).to_frame("Career Avg vs " + opp)
                    st.dataframe(summary, use_container_width=True)
                with c2:
                    show = opp_logs[["GAME_DATE"] + TARGET_STATS].sort_values("GAME_DATE", ascending=False)
                    st.dataframe(show.reset_index(drop=True), use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# TAB 2 — History
# ══════════════════════════════════════════════════════════════════════════════
with tab3:
    if logs.empty:
        st.info("No game log data loaded.")
    else:
        logs_sorted = logs.sort_values("GAME_DATE", ascending=False).copy()

        st.subheader("Recent Form — Last 10 Games")
        show_cols = ["GAME_DATE", "OPP", "HOME_AWAY", "WL"] + TARGET_STATS
        recent = logs_sorted.head(10)[[c for c in show_cols if c in logs_sorted.columns]]
        st.dataframe(recent.reset_index(drop=True), use_container_width=True)

        st.subheader("Rolling Average Chart")
        chart_stat = st.selectbox("Stat", TARGET_STATS, key="chart_stat")
        logs_asc = logs.sort_values("GAME_DATE").copy()
        if chart_stat in logs_asc.columns:
            logs_asc["roll10"] = logs_asc[chart_stat].rolling(10, min_periods=1).mean()
            chart_df = logs_asc[["GAME_DATE", chart_stat, "roll10"]].rename(
                columns={chart_stat: "Actual", "roll10": "10-game avg"}
            ).set_index("GAME_DATE")
            st.line_chart(chart_df)

        st.subheader("All Games")
        all_cols = ["GAME_DATE", "OPP", "HOME_AWAY", "WL"] + TARGET_STATS
        st.dataframe(
            logs_sorted[[c for c in all_cols if c in logs_sorted.columns]].reset_index(drop=True),
            use_container_width=True,
        )


# ══════════════════════════════════════════════════════════════════════════════
# TAB 4 — News
# ══════════════════════════════════════════════════════════════════════════════
with tab4:
    if news.empty:
        st.info("No news for this date. Run the pipeline to fetch latest articles.")
    else:
        st.subheader(f"{len(news)} Wemby articles — last 7 days")
        for _, row in news.iterrows():
            with st.expander(row.get("title", "No title")):
                st.write(row.get("summary", ""))
                url = row.get("url", "")
                if url:
                    st.markdown(f"[Read more]({url})")
                st.caption(f"{row.get('source', '')} — {row.get('published_at', '')[:10]}")


# ══════════════════════════════════════════════════════════════════════════════
# VIDEO — shown below tabs if it exists for today
# ══════════════════════════════════════════════════════════════════════════════
dk_today = date.today().strftime("%Y_%m_%d")
video_path = Path(f"data/videos/{dk_today}_wemby.mp4")
if video_path.exists():
    st.markdown("---")
    st.markdown("### Today's AI Picks Video")
    st.video(str(video_path))
