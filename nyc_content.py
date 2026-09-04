"""
Contenu des onglets "Contexte" et "Architecture" — partagé entre
streamlit_dashboard.py (Snowflake) et streamlit_dashboard_local.py (DuckDB).

Même esprit que les onglets À propos / Circulation de la donnée /
Architecture cloud & BDD de qlt-eau-FR-24 : montrer le pipeline de données
(ingestion, couches, schéma) en plus du dashboard, pour mettre en avant le
volet data engineering du projet.
"""
import re

import streamlit as st

import nyc_theme as theme

REPO_URL = "https://github.com/DVDJNBR/NYC_TAXI_PIPELINE"


def _html(s: str) -> str:
    """Dédente le HTML multi-lignes pour éviter que Streamlit ne le lise comme un bloc de code Markdown."""
    return re.sub(r"^[ \t]+", "", s, flags=re.MULTILINE)


TECH_BADGES = [
    ("Python", "#3776AB"),
    ("Snowflake", "#29B5E8"),
    ("dbt Core", "#FF694B"),
    ("DuckDB", "#FCC624"),
    ("Streamlit", theme.TAXI_GOLD),
    ("Plotly", "#8DA0CB"),
    ("Docker", "#2496ED"),
    ("Invoke", theme.TEXT_DIM),
]

PIPELINE_STEPS = [
    ("NYC TLC Open Data", "Fichiers Parquet mensuels, 2023–2025 (~77M lignes)", theme.TEXT_DIM),
    ("RAW", "Ingestion brute — table Snowflake ou scan DuckDB local", theme.DUSK_ORANGE),
    ("STAGING", "clean_trips — nettoyage, filtres de cohérence", theme.NIGHT_PURPLE),
    ("FINAL", "Marts agrégés : daily_summary, hourly_patterns, zone_analysis…", theme.DOLLAR_GREEN),
    ("Dashboard", "Streamlit + Plotly, requêtes mises en cache", theme.TAXI_GOLD),
]

FINAL_TABLES = [
    ("daily_summary", "Agrégat",
     "pickup_date · total_trips · total_revenue · avg_distance · avg_fare · avg_tip_pct"),
    ("hourly_patterns", "Agrégat",
     "pickup_hour · total_trips · total_revenue · avg_fare · avg_tip_pct · tranche"),
    ("zone_analysis", "Agrégat",
     "pickup_zone · total_trips · total_revenue · avg_fare · popularity_rank"),
    ("clean_trips", "Staging",
     "trajets filtrés (distance > 0, montants > 0, microsecondes corrigées)"),
]

KIND_COLORS = {
    "Agrégat": theme.DOLLAR_GREEN,
    "Staging": theme.NIGHT_PURPLE,
    "Dimension": theme.DUSK_ORANGE,
}


def render_about_tab():
    st.markdown(_html(f"""
    <div style="background:linear-gradient(155deg,{theme.BG_PANEL},{theme.BG_PANEL_2});
                border:1px solid {theme.GOLD_BORDER}; border-radius:12px; padding:22px 26px;">
      <p style="color:{theme.TEXT_LIGHT}; font-size:0.95rem; line-height:1.65; margin:0;">
        Pipeline de données bout-en-bout sur
        <b style="color:{theme.TAXI_GOLD};">77M</b> courses de taxis jaunes new-yorkais
        (2023–2025, données NYC TLC Open Data) : ingestion, architecture en couches
        <b>RAW → STAGING → FINAL</b> sur Snowflake — avec une option dbt Core pour les
        transformations et leurs tests automatiques — puis restitution dans ce dashboard
        Streamlit / Plotly. Une alternative 100&nbsp;% locale (DuckDB directement sur les
        Parquet) permet de faire tourner le même dashboard sur un VPS sans dépendre d'un
        compte Snowflake actif.
      </p>
    </div>
    """), unsafe_allow_html=True)

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    badges = "".join(
        f'<span style="display:inline-block; margin:3px 6px 3px 0; padding:5px 12px; '
        f'border-radius:999px; font-size:0.8rem; font-weight:600; color:{color}; '
        f'background:{color}22; border:1px solid {color}55;">{name}</span>'
        for name, color in TECH_BADGES
    )
    st.markdown(_html(f"<div>{badges}</div>"), unsafe_allow_html=True)

    st.markdown("<div style='height:18px'></div>", unsafe_allow_html=True)
    st.markdown(f"🔗 [Code source sur GitHub]({REPO_URL})")


def render_architecture_tab():
    st.subheader("🔀 Circulation de la donnée")

    parts = []
    for i, (title, subtitle, color) in enumerate(PIPELINE_STEPS):
        parts.append(_html(f"""
        <div style="flex:0 0 auto; min-width:150px; background:{theme.BG_PANEL_2};
                    border:1px solid {color}55; border-left:4px solid {color};
                    border-radius:10px; padding:12px 14px;">
          <div style="font-weight:700; color:{color}; font-size:0.88rem;">{title}</div>
          <div style="color:{theme.TEXT_DIM}; font-size:0.74rem; margin-top:4px;">{subtitle}</div>
        </div>
        """))
        if i < len(PIPELINE_STEPS) - 1:
            parts.append(
                f'<div style="display:flex; align-items:center; color:{theme.TEXT_DIM}; '
                f'font-size:1.1rem; padding:0 6px;">→</div>'
            )
    st.markdown(
        _html(f'<div style="display:flex; overflow-x:auto; gap:2px; padding:6px 2px 16px; align-items:stretch;">'
              + "".join(parts) + "</div>"),
        unsafe_allow_html=True,
    )

    st.subheader("🗄️ Schéma FINAL — tables analytiques")
    rows = "".join(
        _html(f"""
        <div style="display:flex; flex-wrap:wrap; align-items:center; gap:12px; padding:10px 14px;
                    border-bottom:1px solid {theme.GOLD_BORDER};">
          <span style="flex:0 0 130px; font-weight:700; color:{theme.TEXT_LIGHT};
                       font-family:monospace;">{name}</span>
          <span style="flex:0 0 auto; padding:2px 10px; border-radius:999px; font-size:0.72rem;
                       font-weight:700; color:{KIND_COLORS[kind]}; background:{KIND_COLORS[kind]}22;">{kind}</span>
          <span style="color:{theme.TEXT_DIM}; font-size:0.76rem; font-family:monospace;">{cols}</span>
        </div>
        """)
        for name, kind, cols in FINAL_TABLES
    )
    st.markdown(
        _html(f"""<div style="background:{theme.BG_PANEL}; border:1px solid {theme.GOLD_BORDER};
                              border-radius:12px; overflow:hidden;">{rows}</div>"""),
        unsafe_allow_html=True,
    )

    st.markdown("<div style='height:20px'></div>", unsafe_allow_html=True)
    st.subheader("☁️ Deux cibles de déploiement")
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(_html(f"""
        <div style="background:{theme.BG_PANEL_2}; border:1px solid {theme.GOLD_BORDER};
                    border-radius:10px; padding:16px 18px; height:100%;">
          <div style="font-weight:700; color:{theme.TAXI_GOLD};">❄️ Snowflake + dbt</div>
          <ul style="color:{theme.TEXT_DIM}; font-size:0.8rem; margin:8px 0 0; padding-left:18px; line-height:1.7;">
            <li>Warehouse <code>NYC_TAXI_WH</code>, DB <code>NYC_TAXI_DB</code></li>
            <li>Schémas <code>RAW</code> / <code>STAGING</code> / <code>FINAL</code></li>
            <li>Modèles dbt + tests automatiques (dbt_utils)</li>
            <li><code>streamlit_dashboard.py</code></li>
          </ul>
        </div>
        """), unsafe_allow_html=True)
    with c2:
        st.markdown(_html(f"""
        <div style="background:{theme.BG_PANEL_2}; border:1px solid {theme.GOLD_BORDER};
                    border-radius:10px; padding:16px 18px; height:100%;">
          <div style="font-weight:700; color:{theme.DOLLAR_GREEN};">🐳 DuckDB + Docker (VPS)</div>
          <ul style="color:{theme.TEXT_DIM}; font-size:0.8rem; margin:8px 0 0; padding-left:18px; line-height:1.7;">
            <li>Scan direct des Parquet, sans base distante</li>
            <li>Conteneur Docker derrière Caddy (reverse proxy)</li>
            <li>Volume bind-mount, téléchargement au 1ᵉʳ démarrage</li>
            <li><code>streamlit_dashboard_local.py</code></li>
          </ul>
        </div>
        """), unsafe_allow_html=True)
