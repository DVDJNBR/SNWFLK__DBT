"""
Skyline NYC — même identité visuelle que le project card "NYC Taxi Pipeline"
du portfolio (portfolio.dvdjnbr.fr), mais générée à partir des VRAIES
données horaires du dashboard (au lieu d'un PNG figé).

Portage simplifié du générateur original (PORTFOLIO/scripts/gen_skyline.py,
PIL) : mêmes couleurs par heure, même logique d'immeubles à paliers, mêmes
fenêtres allumées pseudo-aléatoires. Statique (pas de ciel, pas de
défilement) — juste la skyline + un motif au sol dont la densité suit le
volume de l'heure, et dont l'icône change selon la métrique affichée
(taxi / dollar / cadeau / route).
"""
import io
import math
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

ASSET_DIR = Path(__file__).parent / "assets"
TAXI_SPRITE_PATH = ASSET_DIR / "taxi-pixel.png"

# ---------------------------------------------------------------------------
# Palette — identique à gen_skyline.py (nuit indigo -> jour sable -> crépuscule)
# ---------------------------------------------------------------------------
HOUR_COLORS = [
    "#2c3758", "#2c3554", "#2c3450", "#2c324c", "#2c3048", "#464260",
    "#615578", "#877480", "#ab9482", "#a59485", "#9f948a", "#98948d",
    "#999591", "#9b9993", "#9d9b97", "#a49184", "#aa8970", "#b17f5e",
    "#ac7659", "#a56e55", "#855d68", "#6a4e7a", "#574271", "#433668",
]

WINDOW_OFF = "#2a4258"
WINDOW_LIT = "#f4c542"   # = jaune taxi, réutilisé comme accent ailleurs dans le dashboard
STROKE     = "#55555a"
SPIRE      = "#6b6b70"

SLOT_WIDTH    = 15
HEIGHT_SCALE  = 1.8
STRIP_WIDTH   = SLOT_WIDTH * 24
GROUND_Y      = 108
Y_MIN, Y_MAX  = -44, 128

NIGHT_PROB = [
    0.75, 0.8, 0.85, 0.85, 0.75, 0.55, 0.35, 0.15,
    0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    0.15, 0.35, 0.55, 0.7,
]

# Icône affichée au sol selon la métrique sélectionnée dans le dashboard.
MOTIF_BY_METRIC = {
    "TOTAL_TRIPS":   "taxi",
    "TOTAL_REVENUE": "dollar",
    "AVG_FARE":      "dollar",
    "AVG_TIP_PCT":   "gift",
    "AVG_DISTANCE":  "road",
}


def _hash_unit(seed: float) -> float:
    x = math.sin(seed) * 43758.5453
    return x - math.floor(x)


def _is_lit(i: int, hour: int, x: float, y: float) -> bool:
    return _hash_unit(i * 12.9898 + x * 3.71 + y * 7.13) < NIGHT_PROB[hour % 24]


def _windows_for(start: float, length: float, width: float):
    if length < 5 or width < 3.5:
        return []
    margin, row_gap, col_gap = 2.2, 4.2, 3.2
    usable = length - margin * 2
    rows = max(1, int(usable // row_gap) + 1)
    half_w = width / 2 - 1
    if half_w > 3:
        cols, c = [], -half_w + 1
        while c <= half_w - 1:
            cols.append(c)
            c += col_gap
    elif half_w > 1:
        cols = [-half_w * 0.55, half_w * 0.55]
    else:
        cols = [0]
    pts = []
    for r in range(rows):
        d = usable / 2 if rows == 1 else (usable * r) / (rows - 1)
        pts.extend((start + margin + d, c) for c in cols)
    return pts


def _tiers_for(L: float, peak: bool):
    if L <= 12:
        return [(0, L, 9.0)]
    width = 9.0 + (L / 71.0) * 5.5
    t1_len = L * (0.62 if peak else 0.55)
    t2_len = L - t1_len
    return [(0, t1_len, width), (t1_len, t2_len, width * 0.6)]


def _scale_heights(values):
    lo, hi = min(values), max(values)
    if hi - lo < 1e-9:
        return [45.0] * len(values)
    return [14 + (v - lo) / (hi - lo) * (71 - 14) for v in values]


def _dashed_line(draw, px, y, dash=4, gap=3, **kw):
    x = 0.0
    while x < STRIP_WIDTH:
        p0, p1 = px(x, y), px(min(x + dash, STRIP_WIDTH), y)
        draw.line([p0, p1], **kw)
        x += dash + gap


def _draw_dollar(draw, cx, cy, r, font):
    draw.ellipse([cx - r, cy - r, cx + r, cy + r], fill=(61, 220, 132, 255),
                 outline=(21, 61, 41, 255), width=max(1, int(r * 0.14)))
    bbox = draw.textbbox((0, 0), "$", font=font)
    tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
    draw.text((cx - tw / 2, cy - th / 2 - bbox[1]), "$", font=font, fill=(14, 46, 30, 255))


def _draw_gift(draw, cx, cy, r):
    draw.rectangle([cx - r, cy - r * 0.75, cx + r, cy + r], fill=(138, 110, 163, 255),
                    outline=(70, 55, 85, 255), width=max(1, int(r * 0.1)))
    rw = r * 0.32
    draw.rectangle([cx - rw / 2, cy - r * 0.75, cx + rw / 2, cy + r], fill=WINDOW_LIT)
    rh = r * 0.32
    draw.rectangle([cx - r, cy - rh / 2, cx + r, cy + rh / 2], fill=WINDOW_LIT)


def _draw_road(draw, cx, cy, w, h):
    x0, y0, x1, y1 = cx - w / 2, cy - h / 2, cx + w / 2, cy + h / 2
    draw.rounded_rectangle([x0, y0, x1, y1], radius=h * 0.3, fill=(58, 56, 72, 255),
                            outline=(30, 29, 40, 255))
    dash_w, dash_h = w * 0.3, max(1, h * 0.16)
    draw.rectangle([cx - dash_w / 2, cy - dash_h / 2, cx + dash_w / 2, cy + dash_h / 2],
                    fill=(232, 223, 174, 235))


def render_skyline_png(values_by_hour, scale=4, supersample=2, peak_labels=None, motif="taxi"):
    """values_by_hour: 24 floats indexés par heure (0..23).
    peak_labels: dict optionnel {hour: "texte"} affiché au-dessus de l'immeuble.
    motif: "taxi" | "dollar" | "gift" | "road" — icône au sol, densité liée au rang de l'heure.
    Retourne (png_bytes, (largeur_px, hauteur_px))."""
    assert len(values_by_hour) == 24
    DRAW = scale * supersample
    heights = _scale_heights(values_by_hour)
    ranked = sorted(range(24), key=lambda h: -values_by_hour[h])
    rank_of = {h: i + 1 for i, h in enumerate(ranked)}
    peak_hours = set(ranked[:3])
    peak_labels = peak_labels or {}

    W = int(STRIP_WIDTH * DRAW)
    H = int((Y_MAX - Y_MIN) * DRAW)

    def px(x, y):
        return (x * DRAW, (y - Y_MIN) * DRAW)

    im = Image.new("RGBA", (W, H), (0, 0, 0, 0))
    draw = ImageDraw.Draw(im)

    try:
        font_sm = ImageFont.load_default(size=int(3.6 * DRAW))
        font_lg = ImageFont.load_default(size=int(4.4 * DRAW))
        font_icon = ImageFont.load_default(size=int(4.8 * DRAW))
    except Exception:
        font_sm = font_lg = font_icon = ImageFont.load_default()

    # -- route ---------------------------------------------------------------
    road_y0, road_y1 = GROUND_Y, GROUND_Y + 9
    r0, r1 = px(0, road_y0), px(STRIP_WIDTH, road_y1)
    draw.rectangle([r0[0], r0[1], r1[0], r1[1]], fill=(42, 40, 54, 255))
    _dashed_line(draw, px, GROUND_Y + 4.5, dash=5, gap=5,
                 fill=(232, 223, 174, 160), width=max(1, int(0.5 * DRAW)))

    taxi_sprite = None
    if motif == "taxi":
        try:
            taxi_sprite = Image.open(TAXI_SPRITE_PATH).convert("RGBA")
        except Exception:
            taxi_sprite = None

    # -- immeubles -------------------------------------------------------------
    for h in range(24):
        xCenter = h * SLOT_WIDTH + SLOT_WIDTH / 2
        L = heights[h]
        peak = h in peak_hours
        body_color = HOUR_COLORS[h]
        for (start, length, width) in _tiers_for(L, peak):
            s, l = start * HEIGHT_SCALE, length * HEIGHT_SCALE
            x0, x1 = xCenter - width / 2, xCenter + width / 2
            y0, y1 = GROUND_Y - s - l, GROUND_Y - s
            p0, p1 = px(x0, y0), px(x1, y1)
            draw.rectangle([p0[0], p0[1], p1[0], p1[1]], fill=body_color,
                            outline=STROKE, width=max(1, int(0.45 * DRAW)))
            for (wx, wy) in _windows_for(s, l, width):
                if _hash_unit(h * 33.71 + wx * 5.19 + wy * 9.02) < 0.12:
                    continue
                lit = _is_lit(h, h, wx, wy)
                realX, realY = xCenter + wy, GROUND_Y - wx
                wp0, wp1 = px(realX - 0.7, realY - 0.7), px(realX + 0.7, realY + 0.7)
                draw.rectangle([wp0[0], wp0[1], wp1[0], wp1[1]],
                                fill=(WINDOW_LIT if lit else WINDOW_OFF))
        if peak:
            sx = xCenter
            sy1 = GROUND_Y - L * HEIGHT_SCALE
            sy2 = sy1 - 9
            draw.line([px(sx, sy1), px(sx, sy2)], fill=SPIRE, width=max(1, int(1.1 * DRAW)))

        # callout de valeur réelle au sommet des immeubles marquants
        label = peak_labels.get(h)
        if label:
            lx, ly = px(xCenter, GROUND_Y - L * HEIGHT_SCALE - 12)
            bbox = draw.textbbox((0, 0), label, font=font_lg)
            tw = bbox[2] - bbox[0]
            draw.text((lx - tw / 2, ly), label, font=font_lg, fill=(244, 197, 66, 235))

        # repère horaire
        if h % 2 == 0:
            lx, ly = px(xCenter, GROUND_Y + 14)
            txt = f"{h}h"
            emphasize = h in (0, 12)
            fnt = font_lg if emphasize else font_sm
            bbox = draw.textbbox((0, 0), txt, font=fnt)
            tw, th = bbox[2] - bbox[0], bbox[3] - bbox[1]
            fillA = 225 if emphasize else 140
            draw.text((lx - tw / 2, ly - th / 2 - bbox[1]), txt, font=fnt,
                       fill=(230, 222, 204, fillA))

        # lampadaire
        lampX = h * SLOT_WIDTH
        lampLit = _hash_unit(h * 4.71 + 1.3) < NIGHT_PROB[h % 24]
        draw.line([px(lampX, GROUND_Y), px(lampX, GROUND_Y - 5.5)],
                   fill=(120, 122, 130, 255), width=max(1, int(0.4 * DRAW)))
        headColor = (255, 224, 140, 255) if lampLit else (120, 122, 130, 255)
        hr = 0.75
        h0, h1 = px(lampX - hr, GROUND_Y - 5.5 - hr), px(lampX + hr, GROUND_Y - 5.5 + hr)
        draw.ellipse([h0[0], h0[1], h1[0], h1[1]], fill=headColor,
                      outline=(60, 60, 64, 255), width=max(1, int(0.15 * DRAW)))

        # icône au sol — densité proportionnelle au rang de l'heure pour la métrique affichée
        rank = rank_of[h]
        show_icon = (
            rank <= 6
            or (rank <= 14 and _hash_unit(h + 0.41) < 0.55)
            or (_hash_unit(h) < 0.15)
        )
        if show_icon:
            cx_px, cy_px = px(xCenter, GROUND_Y - 3.6)
            if motif == "taxi" and taxi_sprite is not None:
                sw, sh = int(12 * DRAW), int(7 * DRAW)
                resized = taxi_sprite.resize((sw, sh), Image.NEAREST)
                im.alpha_composite(resized, (int(cx_px - sw / 2), int(cy_px - sh / 2)))
            elif motif == "dollar":
                _draw_dollar(draw, cx_px, cy_px, 3.1 * DRAW, font_icon)
            elif motif == "gift":
                _draw_gift(draw, cx_px, cy_px, 3.1 * DRAW)
            elif motif == "road":
                _draw_road(draw, cx_px, cy_px, 5.5 * DRAW, 2.6 * DRAW)

    final_w, final_h = int(STRIP_WIDTH * scale), int((Y_MAX - Y_MIN) * scale)
    final = im.resize((final_w, final_h), Image.LANCZOS)
    buf = io.BytesIO()
    final.save(buf, format="PNG")
    return buf.getvalue(), (final_w, final_h)
