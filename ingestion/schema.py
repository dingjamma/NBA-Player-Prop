"""
Column definitions for raw data. Used to filter/validate DataFrames.
"""

# Columns we keep from nba_api PlayerGameLog
RAW_GAME_LOG_COLUMNS = [
    "PLAYER_ID",
    "SEASON_ID",
    "Game_ID",
    "GAME_DATE",
    "MATCHUP",
    "WL",
    "MIN",
    "FGM", "FGA", "FG_PCT",
    "FG3M", "FG3A", "FG3_PCT",
    "FTM", "FTA", "FT_PCT",
    "OREB", "DREB", "REB",
    "AST", "STL", "BLK",
    "TOV", "PF",
    "PTS",
    "PLUS_MINUS",
    "VIDEO_AVAILABLE",
]

# Columns we keep from ScoreboardV2 GameHeader
SCHEDULE_COLUMNS = [
    "GAME_DATE_EST",
    "GAME_SEQUENCE",
    "GAME_ID",
    "GAME_STATUS_ID",
    "GAME_STATUS_TEXT",
    "GAMECODE",
    "HOME_TEAM_ID",
    "VISITOR_TEAM_ID",
    "SEASON",
    "LIVE_PERIOD",
    "HOME_TV_BROADCASTER_ABBREVIATION",
    "AWAY_TV_BROADCASTER_ABBREVIATION",
    "NATL_TV_BROADCASTER_ABBREVIATION",
]
