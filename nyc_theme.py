"""
Thème visuel partagé — dashboards NYC Yellow Taxi.
Palette reprise du skyline animé (skyline.py) : nuit indigo -> jour sable ->
crépuscule orange, avec le jaune taxi (#f4c542) comme accent principal.
"""
import streamlit as st

BG_PAGE    = "#0f0e17"
BG_PANEL   = "#1b1a29"
BG_PANEL_2 = "#221f33"

TAXI_GOLD    = "#f4c542"   # accent principal (courses, tarifs)
DOLLAR_GREEN = "#3ddc84"   # revenus / $
DUSK_ORANGE  = "#c98a5e"   # distance / secondaire chaud
NIGHT_PURPLE = "#8a6ea3"   # passagers / secondaire froid
DUSK_RED     = "#e2725b"   # baisses / "pires jours"

TEXT_LIGHT = "#EDEAE3"
TEXT_DIM   = "#9691a6"
GRID_LINE  = "rgba(255,255,255,0.06)"
GOLD_BORDER = "rgba(244,197,66,0.16)"


def inject_css():
    st.markdown(
        f"""
        <style>
        .stApp {{
            background: radial-gradient(ellipse 1200px 700px at 50% -8%, #211e35 0%, {BG_PAGE} 55%) fixed;
        }}
        h1, h2, h3 {{ color: {TEXT_LIGHT} !important; }}
        [data-testid="stCaptionContainer"], .stMarkdown p {{ color: {TEXT_DIM}; }}
        hr {{ border-color: {GOLD_BORDER} !important; }}
        [data-testid="stMetricValue"] {{ color: {TAXI_GOLD}; }}
        .stButton>button {{
            border-radius: 8px;
            border: 1px solid {GOLD_BORDER};
        }}
        .stButton>button[kind="primary"] {{
            background-color: {TAXI_GOLD};
            color: #14131d;
            border: none;
        }}
        [data-testid="stHeader"] {{ background: transparent; }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def card(label, value, detail="", color=TAXI_GOLD, icon=""):
    prefix = f"{icon}&nbsp;" if icon else ""
    st.markdown(
        f"""<div style="background:linear-gradient(155deg,{BG_PANEL},{BG_PANEL_2});
                        border:1px solid {GOLD_BORDER}; border-left:4px solid {color};
                        border-radius:10px; padding:18px 20px;
                        box-shadow:0 4px 14px rgba(0,0,0,0.35);">
              <div style="font-size:1.9rem; font-weight:800; color:{color}; line-height:1.15;">{prefix}{value}</div>
              <div style="font-size:0.82rem; font-weight:600; color:{TEXT_LIGHT}; margin-top:6px;">{label}</div>
              <div style="font-size:0.75rem; color:{TEXT_DIM}; margin-top:3px;">{detail}</div>
            </div>""",
        unsafe_allow_html=True,
    )


def style_fig(fig, **overrides):
    """Applique le thème sombre à une figure Plotly (à appeler avant les
    update_layout spécifiques à chaque graphique — le merge est récursif)."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=BG_PANEL,
        plot_bgcolor=BG_PANEL,
        font=dict(color=TEXT_LIGHT),
        xaxis=dict(gridcolor=GRID_LINE, zerolinecolor=GRID_LINE),
        yaxis=dict(gridcolor=GRID_LINE, zerolinecolor=GRID_LINE),
        **overrides,
    )
    return fig
