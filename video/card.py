"""
Generates a transparent RGBA stats card overlay for the Wemby prediction video.

Layout (1080x1920):
  - Top third: transparent (Seedance background visible)
  - Center: dark card with AI picks vs Vegas lines
  - Bottom: transparent
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw, ImageFont

# ── Colors ────────────────────────────────────────────────────────────────────
BG_FILL        = (8, 8, 18, 210)       # near-black, slightly transparent
BORDER_COLOR   = (255, 255, 255, 40)
SEPARATOR      = (255, 255, 255, 45)
WHITE          = (255, 255, 255, 255)
DIM            = (180, 180, 195, 255)
OVER_COLOR     = (72, 220, 105, 255)   # vivid green
UNDER_COLOR    = (220, 72, 72, 255)    # vivid red
ACCENT         = (200, 160, 60, 255)   # gold/amber for header accent

# ── Stat display names ─────────────────────────────────────────────────────
STAT_LABELS = {
    "PTS":  "Points",
    "REB":  "Rebounds",
    "AST":  "Assists",
    "STL":  "Steals",
    "BLK":  "Blocks",
    "FG3M": "3-Pointers",
}

CANVAS_W = 1080
CANVAS_H = 1920


def _load_font(path: str, size: int) -> ImageFont.FreeTypeFont:
    try:
        return ImageFont.truetype(path, size)
    except OSError:
        return ImageFont.load_default()


def _fonts() -> dict:
    bold   = "C:/Windows/Fonts/arialbd.ttf"
    normal = "C:/Windows/Fonts/arial.ttf"
    return {
        "title":    _load_font(bold,   76),
        "subtitle": _load_font(normal, 36),
        "stat_key": _load_font(bold,   50),
        "stat_val": _load_font(normal, 46),
        "pick":     _load_font(bold,   46),
        "label":    _load_font(normal, 34),
        "footer":   _load_font(normal, 30),
    }


def _draw_rounded_rect(
    draw: ImageDraw.ImageDraw,
    xy: tuple[int, int, int, int],
    radius: int,
    fill: tuple,
    outline: Optional[tuple] = None,
    outline_width: int = 2,
) -> None:
    x1, y1, x2, y2 = xy
    draw.rounded_rectangle(xy, radius=radius, fill=fill,
                           outline=outline, width=outline_width)


def create_card(
    picks: dict[str, dict],
    game_info: dict,
    width: int = CANVAS_W,
    height: int = CANVAS_H,
) -> Image.Image:
    """
    picks = {
        "PTS":  {"ai": 24.2, "line": 21.5, "pick": "OVER"},
        "REB":  {"ai":  8.1, "line":  9.5, "pick": "UNDER"},
        ...
    }
    game_info = {"opponent": "LAL", "home_away": "vs", "date": "Mar 22"}
    """
    canvas = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    draw   = ImageDraw.Draw(canvas)
    fonts  = _fonts()

    # ── Card bounds ───────────────────────────────────────────────────────
    pad_x      = 60
    card_x1    = pad_x
    card_x2    = width - pad_x
    card_top   = int(height * 0.26)
    card_bot   = int(height * 0.92)
    card_w     = card_x2 - card_x1
    row_h      = 110
    inner_pad  = 44

    # ── Card background ───────────────────────────────────────────────────
    _draw_rounded_rect(
        draw,
        (card_x1, card_top, card_x2, card_bot),
        radius=28,
        fill=BG_FILL,
        outline=BORDER_COLOR,
        outline_width=2,
    )

    cy = card_top + inner_pad

    # ── Header ────────────────────────────────────────────────────────────
    title = "WEMBY AI PICKS"
    bbox  = draw.textbbox((0, 0), title, font=fonts["title"])
    tw    = bbox[2] - bbox[0]
    draw.text(((width - tw) // 2, cy), title, font=fonts["title"], fill=ACCENT)
    cy += (bbox[3] - bbox[1]) + 14

    # Accent underline
    line_w = min(tw + 80, card_w - 80)
    lx     = (width - line_w) // 2
    draw.line([(lx, cy), (lx + line_w, cy)], fill=ACCENT, width=2)
    cy += 16

    # Subtitle: opponent + date
    opp       = game_info.get("opponent", "")
    ha        = game_info.get("home_away", "vs")
    date_str  = game_info.get("date", "Tonight")
    subtitle  = f"{ha.upper()} {opp}  ·  {date_str}" if opp else f"Tonight  ·  {date_str}"
    bbox      = draw.textbbox((0, 0), subtitle, font=fonts["subtitle"])
    sw        = bbox[2] - bbox[0]
    draw.text(((width - sw) // 2, cy), subtitle, font=fonts["subtitle"], fill=DIM)
    cy += (bbox[3] - bbox[1]) + 30

    # Column headers
    col_stat  = card_x1 + inner_pad
    col_ai    = card_x1 + 260
    col_line  = card_x1 + 470
    col_pick  = card_x1 + 660

    for text, x in [("STAT", col_stat), ("AI", col_ai), ("LINE", col_line), ("PICK", col_pick)]:
        draw.text((x, cy), text, font=fonts["label"], fill=DIM)
    cy += 38

    # Separator
    draw.line([(card_x1 + inner_pad, cy), (card_x2 - inner_pad, cy)],
              fill=SEPARATOR, width=1)
    cy += 16

    # ── Stat rows ─────────────────────────────────────────────────────────
    stats_order = ["PTS", "REB", "AST", "STL", "BLK", "FG3M"]
    for stat in stats_order:
        if stat not in picks:
            continue
        info    = picks[stat]
        ai_val  = info["ai"]
        line    = info["line"]
        pick    = info["pick"]
        color   = OVER_COLOR if pick == "OVER" else UNDER_COLOR

        label = STAT_LABELS.get(stat, stat)
        draw.text((col_stat, cy), label,        font=fonts["stat_key"], fill=WHITE)
        draw.text((col_ai,   cy), f"{ai_val:.1f}", font=fonts["stat_val"], fill=WHITE)
        draw.text((col_line, cy), f"{line:.1f}", font=fonts["stat_val"], fill=DIM)

        # Pill badge for OVER/UNDER
        badge_text = pick
        bbox_b     = draw.textbbox((0, 0), badge_text, font=fonts["pick"])
        bw         = (bbox_b[2] - bbox_b[0]) + 28
        bh         = (bbox_b[3] - bbox_b[1]) + 14
        bx1        = col_pick
        by1        = cy - 4
        _draw_rounded_rect(
            draw,
            (bx1, by1, bx1 + bw, by1 + bh),
            radius=10,
            fill=(*color[:3], 45),
            outline=(*color[:3], 180),
            outline_width=2,
        )
        draw.text((bx1 + 14, by1 + 7), badge_text, font=fonts["pick"], fill=color)

        cy += row_h

    # Bottom separator
    draw.line([(card_x1 + inner_pad, cy - 10), (card_x2 - inner_pad, cy - 10)],
              fill=SEPARATOR, width=1)
    cy += 10

    # ── Footer ────────────────────────────────────────────────────────────
    footer = "AI predictions · Not financial advice"
    bbox   = draw.textbbox((0, 0), footer, font=fonts["footer"])
    fw     = bbox[2] - bbox[0]
    draw.text(((width - fw) // 2, cy), footer, font=fonts["footer"], fill=DIM)

    return canvas


def save_card(picks: dict, game_info: dict, output_path: Path) -> Path:
    img = create_card(picks, game_info)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    img.save(str(output_path), "PNG")
    return output_path
