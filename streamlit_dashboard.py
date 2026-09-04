"""
NYC Yellow Taxi — Dashboard analytique
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import snowflake.connector
from dotenv import load_dotenv
import os

import nyc_theme
import nyc_content
import skyline

st.set_page_config(
    page_title="NYC Yellow Taxi",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

load_dotenv()

# ---------------------------------------------------------------------------
# Lookup officiel TLC : location_id -> nom de quartier
# ---------------------------------------------------------------------------
ZONE_LOOKUP = {
    1: "Newark Airport", 2: "Jamaica Bay", 3: "Allerton/Pelham Gardens",
    4: "Alphabet City", 5: "Arden Heights", 6: "Arrochar/Fort Wadsworth",
    7: "Astoria", 8: "Astoria Park", 9: "Auburndale", 10: "Baisley Park",
    11: "Bath Beach", 12: "Battery Park", 13: "Battery Park City",
    14: "Bay Ridge", 15: "Bay Terrace/Fort Totten", 16: "Bayside",
    17: "Bedford", 18: "Bedford Park", 19: "Bellerose", 20: "Belmont",
    21: "Bensonhurst East", 22: "Bensonhurst West",
    23: "Bloomfield/Emerson Hill", 24: "Bloomingdale", 25: "Boerum Hill",
    26: "Borough Park", 27: "Breezy Point/Riis Beach", 28: "Briarwood",
    29: "Brighton Beach", 30: "Broad Channel", 31: "Bronx Park",
    32: "Bronxdale", 33: "Brooklyn Heights", 34: "Brooklyn Navy Yard",
    35: "Brownsville", 36: "Bushwick North", 37: "Bushwick South",
    38: "Cambria Heights", 39: "Canarsie", 40: "Carroll Gardens",
    41: "Central Harlem", 42: "Central Harlem North", 43: "Central Park",
    44: "Charleston/Tottenville", 45: "Chinatown", 46: "City Island",
    47: "Clason Point", 48: "Clinton East", 49: "Clinton Hill",
    50: "Clinton West", 51: "Co-Op City", 52: "College Point",
    53: "Columbia St", 54: "Coney Island", 55: "Corona",
    56: "Country Club", 57: "Crotona Park", 58: "Crotona Park East",
    59: "Crown Heights North", 60: "Crown Heights South",
    61: "Cypress Hills", 62: "Douglaston",
    63: "Downtown Brooklyn/MetroTech", 64: "DUMBO/Vinegar Hill",
    65: "Dyker Heights", 66: "East Chelsea", 67: "East Concourse",
    68: "East Chelsea", 69: "East Elmhurst", 70: "East Flatbush/Farragut",
    71: "East Flatbush/Remsen Village", 72: "East Flushing",
    73: "East Harlem North", 74: "East Harlem South", 75: "East New York",
    76: "East New York/Penn Ave", 77: "East Tremont", 78: "East Village",
    79: "East Village", 80: "Eastchester", 81: "Elmhurst",
    82: "Elmhurst/Maspeth", 83: "Eltingville/Annadale",
    84: "Erasmus", 85: "Far Rockaway",
    86: "Financial District North", 87: "Financial District South",
    88: "Flatbush/Ditmas Park", 89: "Flatiron", 90: "Flatlands",
    91: "Flushing", 92: "Flushing Meadows-Corona Park",
    93: "Fordham South", 94: "Forest Hills", 95: "Forest Park",
    96: "Fort Greene", 97: "Fresh Meadows", 98: "Freshkills Park",
    99: "Garment District", 100: "Glen Oaks", 101: "Glendale",
    102: "Governor's Island", 103: "Gowanus", 104: "Gramercy",
    105: "Gravesend", 106: "Great Kills", 107: "Gramercy",
    108: "Green-Wood Cemetery", 109: "Greenpoint",
    110: "Greenwich Village North", 111: "Greenwich Village South",
    112: "Gowanus/Fort Greene", 113: "Hamilton Heights",
    114: "Hammels/Arverne", 115: "Heartland Village/Todt Hill",
    116: "Highbridge", 117: "Highbridge Park", 118: "Hillcrest/Pomonok",
    119: "Hollis", 120: "Homecrest", 121: "Howard Beach",
    122: "Hudson Sq", 123: "Hunts Point", 124: "Inwood",
    125: "Inwood Hill Park", 126: "Jackson Heights", 127: "Jamaica",
    128: "Jamaica Estates", 129: "JFK Airport", 130: "Kensington",
    131: "Kew Gardens", 132: "JFK Airport", 133: "Kew Gardens Hills",
    134: "Kingsbridge Heights", 135: "Kingsbridge/Marble Hill",
    136: "Kips Bay", 137: "LaGuardia Airport", 138: "LaGuardia Airport",
    139: "Laurelton", 140: "Lenox Hill East", 141: "Lenox Hill West",
    142: "Lincoln Square East", 143: "Lincoln Square West",
    144: "Little Italy/NoLiTa", 145: "Long Island City/Hunters Point",
    146: "Long Island City/Queens Plaza", 147: "Longwood",
    148: "Lower East Side", 149: "Madison", 150: "Manhattan Beach",
    151: "Manhattan Valley", 152: "Manhattanville",
    153: "Marble Hill", 154: "Marine Park/Floyd Bennett Field",
    155: "Marine Park/Mill Basin", 156: "Maspeth",
    157: "Meatpacking/West Village West", 158: "Melrose South",
    159: "Middle Village", 160: "Midtown Center", 161: "Midtown Center",
    162: "Midtown East", 163: "Midtown North", 164: "Midtown South",
    165: "Midwood", 166: "Morningside Heights",
    167: "Morrisania/Melrose", 168: "Mott Haven/Port Morris",
    169: "Mount Hope", 170: "Murray Hill", 171: "Murray Hill-Queens",
    172: "New Dorp/Midland Beach", 173: "Newark Airport",
    174: "North Corona", 175: "Norwood", 176: "Oakland Gardens",
    177: "Oakwood", 178: "Ocean Hill", 179: "Ocean Parkway South",
    180: "Old Astoria", 181: "Ozone Park", 182: "Park Slope",
    183: "Parkchester", 184: "Pelham Bay", 185: "Pelham Bay Park",
    186: "Penn Station/Madison Sq West", 187: "Pelham Pkwy",
    188: "Prospect-Lefferts Gardens", 189: "Prospect Heights",
    190: "Prospect Park", 191: "Queens Village",
    192: "Queensboro Hill", 193: "Queensbridge/Ravenswood",
    194: "Randalls Island", 195: "Red Hook", 196: "Rego Park",
    197: "Richmond Hill", 198: "Ridgewood", 199: "Rikers Island",
    200: "Riverdale/North Riverdale", 201: "Rockaway Park",
    202: "Rosedale", 203: "Rossville/Woodrow", 204: "Saint Albans",
    205: "Saint George/New Brighton",
    206: "Saint Michaels Cemetery/Woodside",
    207: "Schuylerville/Edgewater Park", 208: "Seaport",
    209: "Sheepshead Bay", 210: "SoHo", 211: "Soundview/Bruckner",
    212: "Soundview/Castle Hill", 213: "South Beach/Dongan Hills",
    214: "South Jamaica", 215: "South Ozone Park",
    216: "South Williamsburg", 217: "Springfield Gardens North",
    218: "Springfield Gardens South",
    219: "Spuyten Duyvil/Kingsbridge", 220: "Stapleton",
    221: "Starrett City", 222: "Steinway",
    223: "Stuyvesant Heights",
    224: "Stuyvesant Town/Peter Cooper Village",
    225: "Sunnyside", 226: "Sunset Park East", 227: "Sunset Park West",
    228: "Sutton Place/Turtle Bay North", 229: "Midwood",
    230: "Times Sq/Theatre District", 231: "TriBeCa/Civic Center",
    232: "Two Bridges/Seward Park", 233: "UN/Turtle Bay South",
    234: "Union Sq", 235: "University Heights/Morris Heights",
    236: "Upper East Side North", 237: "Upper East Side South",
    238: "Upper West Side North", 239: "Upper West Side South",
    240: "Van Cortlandt Park", 241: "Van Cortlandt Village",
    242: "Van Nest/Morris Park", 243: "Washington Heights North",
    244: "Washington Heights South", 245: "West Brighton",
    246: "West Concourse", 247: "West Farms/Bronx River",
    248: "West Village", 249: "West Village",
    250: "Westchester Village/Unionport", 251: "Westerleigh",
    252: "Whitestone", 253: "Willets Point",
    254: "Williamsbridge/Olinville", 255: "Williamsburg North",
    256: "Williamsburg South", 257: "Windsor Terrace",
    258: "Woodhaven", 259: "Woodlawn/Wakefield", 260: "Woodside",
    261: "World Trade Center", 262: "Yorkville East",
    263: "Yorkville West",
}

# ---------------------------------------------------------------------------
# Connexion Snowflake
# ---------------------------------------------------------------------------
@st.cache_resource
def get_connection():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="NYC_TAXI_WH",
        database="NYC_TAXI_DB",
        schema="DBT_DBREAU",
        role="ACCOUNTADMIN"
    )

@st.cache_data(ttl=3600)
def query(sql):
    conn = get_connection()
    cur = conn.cursor()
    cur.execute(sql)
    cols = [d[0] for d in cur.description]
    return pd.DataFrame(cur.fetchall(), columns=cols)  # type: ignore

# Correction du bug de chargement Parquet : microsecondes interprétées comme secondes
_TS = ("DATEADD('second',"
       " DATEDIFF('second', '1970-01-01'::TIMESTAMP_NTZ, {col}) / 1000000,"
       " '1970-01-01'::TIMESTAMP_NTZ)")

_DATE_FILTER = (
    f"AND {_TS.format(col='TPEP_PICKUP_DATETIME')}::DATE >= '2023-01-01' "
    f"AND {_TS.format(col='TPEP_PICKUP_DATETIME')}::DATE <  '2025-11-01'"
)

@st.cache_data(ttl=3600)
def load_data():
    pickup_date = _TS.format(col="TPEP_PICKUP_DATETIME")
    pickup_hour = f"HOUR({pickup_date})"

    daily = query(f"""
        SELECT
            {pickup_date}::DATE                              AS pickup_date,
            COUNT(*)                                         AS total_trips,
            SUM(TOTAL_AMOUNT)                                AS total_revenue,
            AVG(TRIP_DISTANCE)                               AS avg_distance,
            AVG(TOTAL_AMOUNT)                                AS avg_fare,
            AVG(TIP_AMOUNT / NULLIF(FARE_AMOUNT, 0) * 100)  AS avg_tip_pct
        FROM NYC_TAXI_DB.RAW.YELLOW_TAXI_TRIPS
        WHERE TRIP_DISTANCE > 0 AND TOTAL_AMOUNT > 0
          {_DATE_FILTER}
        GROUP BY 1
        ORDER BY 1
    """)

    hourly = query(f"""
        SELECT
            {pickup_hour}  AS pickup_hour,
            COUNT(*)          AS total_trips,
            SUM(TOTAL_AMOUNT) AS total_revenue,
            AVG(TOTAL_AMOUNT) AS avg_fare,
            AVG(TIP_AMOUNT / NULLIF(FARE_AMOUNT, 0) * 100) AS avg_tip_pct,
            AVG(TRIP_DISTANCE) AS avg_distance,
            CASE
                WHEN {pickup_hour} BETWEEN 0  AND 5  THEN 'Nuit (0h-6h)'
                WHEN {pickup_hour} BETWEEN 6  AND 9  THEN 'Matin (6h-10h)'
                WHEN {pickup_hour} BETWEEN 10 AND 16 THEN 'Journée (10h-17h)'
                WHEN {pickup_hour} BETWEEN 17 AND 20 THEN 'Soir (17h-21h)'
                ELSE                                       'Soirée (21h-0h)'
            END             AS tranche
        FROM NYC_TAXI_DB.RAW.YELLOW_TAXI_TRIPS
        WHERE TRIP_DISTANCE > 0
          {_DATE_FILTER}
        GROUP BY 1, 7
        ORDER BY 1
    """)

    zones = query(f"""
        SELECT
            PULOCATIONID                             AS zone_id,
            COUNT(*)                                 AS total_trips,
            SUM(TOTAL_AMOUNT)                        AS total_revenue,
            AVG(TOTAL_AMOUNT)                        AS avg_fare,
            AVG(TRIP_DISTANCE)                       AS avg_distance,
            AVG(TIP_AMOUNT / NULLIF(FARE_AMOUNT, 0) * 100) AS avg_tip_pct
        FROM NYC_TAXI_DB.RAW.YELLOW_TAXI_TRIPS
        WHERE TRIP_DISTANCE > 0
          {_DATE_FILTER}
        GROUP BY 1
        ORDER BY total_trips DESC
        LIMIT 100
    """)

    profile = query(f"""
        SELECT
            COUNT(*)                                                     AS total_trips,
            AVG(TRIP_DISTANCE)                                           AS avg_distance,
            AVG(TOTAL_AMOUNT)                                            AS avg_fare,
            AVG(TIP_AMOUNT / NULLIF(FARE_AMOUNT, 0) * 100)              AS avg_tip_pct,
            SUM(CASE WHEN TIP_AMOUNT > 0 THEN 1 ELSE 0 END) * 100.0
                / COUNT(*)                                               AS pct_avec_pourboire,
            SUM(CASE WHEN PAYMENT_TYPE = 1 THEN 1 ELSE 0 END) * 100.0
                / COUNT(*)                                               AS pct_carte,
            SUM(CASE WHEN PULOCATIONID IN (132, 138)
                       OR DOLOCATIONID IN (132, 138) THEN 1 ELSE 0 END)
                * 100.0 / COUNT(*)                                       AS pct_aeroport_jfk,
            SUM(CASE WHEN PULOCATIONID = 137
                       OR DOLOCATIONID = 137 THEN 1 ELSE 0 END)
                * 100.0 / COUNT(*)                                       AS pct_aeroport_lga,
            AVG(PASSENGER_COUNT)                                         AS avg_passagers
        FROM NYC_TAXI_DB.RAW.YELLOW_TAXI_TRIPS
        WHERE TRIP_DISTANCE > 0 AND TOTAL_AMOUNT > 0
          {_DATE_FILTER}
    """)

    return daily, hourly, zones, profile


# ---------------------------------------------------------------------------
# Interface
# ---------------------------------------------------------------------------
def main():
    nyc_theme.inject_css()
    st.title("🚕 NYC Yellow Taxi")

    tab_about, tab_arch, tab_dash = st.tabs(["🧭 Contexte", "🏗️ Architecture", "📊 Dashboard"])

    with tab_about:
        nyc_content.render_about_tab()

    with tab_arch:
        nyc_content.render_architecture_tab()

    with tab_dash:
        render_dashboard_tab()


def render_dashboard_tab():
    with st.spinner("Chargement des données..."):
        try:
            daily, hourly, zones, profile = load_data()
            daily["PICKUP_DATE"] = pd.to_datetime(daily["PICKUP_DATE"], errors="coerce")
            daily = daily.dropna(subset=["PICKUP_DATE"])
            # Filtre défensif : on coupe à fin oct. 2025 même si le cache est ancien
            daily = daily[daily["PICKUP_DATE"] <= pd.Timestamp("2025-10-31")]
            # Exclure les jours avec données incomplètes (< 30 % de la médiane)
            _med = daily["TOTAL_TRIPS"].median()  # type: ignore
            daily = daily[daily["TOTAL_TRIPS"] >= _med * 0.30]
            if daily.empty:  # type: ignore
                st.warning("Aucune donnée valide dans daily_summary.")
                st.stop()
        except Exception as e:
            st.error(f"Erreur de chargement : {e}")
            st.stop()

    total_trips = daily["TOTAL_TRIPS"].sum()
    date_min    = daily["PICKUP_DATE"].min().strftime("%b %Y")
    date_max    = daily["PICKUP_DATE"].max().strftime("%b %Y")
    n_days      = len(daily)
    st.caption(
        f"{total_trips/1e6:.1f}M courses analysées · "
        f"{n_days} jours de données · "
        f"{date_min} – {date_max} · "
        f"Source : NYC Taxi & Limousine Commission (TLC)"
    )

    fd    = daily
    card  = nyc_theme.card

    # ------------------------------------------------------------------
    # Section 1 : Indicateurs clés
    # ------------------------------------------------------------------
    st.header("💰 Indicateurs clés")
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        card("Courses", f"{fd['TOTAL_TRIPS'].sum():,.0f}",
             "sur la période sélectionnée", nyc_theme.TAXI_GOLD, "🚕")
    with c2:
        rev = fd["TOTAL_REVENUE"].sum()
        card("Revenus totaux", f"${rev/1e6:.1f}M",
             f"soit ${rev/fd['TOTAL_TRIPS'].sum():.2f} / course", nyc_theme.DOLLAR_GREEN, "💵")
    with c3:
        card("Distance moyenne", f"{fd['AVG_DISTANCE'].mean():.1f} mi",
             "par trajet", nyc_theme.DUSK_ORANGE, "📍")
    with c4:
        card("Tarif moyen", f"${fd['AVG_FARE'].mean():.2f}",
             "toutes charges incluses", nyc_theme.DOLLAR_GREEN, "💲")
    with c5:
        card("Pourboire moyen", f"{fd['AVG_TIP_PCT'].mean():.1f}%",
             "du tarif de base", nyc_theme.NIGHT_PURPLE, "🎁")

    st.divider()

    # ------------------------------------------------------------------
    # Section 2 : Patterns d'activité
    # ------------------------------------------------------------------
    st.header("🌆 Patterns d'activité")

    METRIC_OPTIONS = ["TOTAL_TRIPS", "TOTAL_REVENUE", "AVG_FARE", "AVG_TIP_PCT", "AVG_DISTANCE"]
    METRIC_LABELS  = {
        "TOTAL_TRIPS":   "Nombre de courses",
        "TOTAL_REVENUE": "Revenus ($)",
        "AVG_FARE":      "Tarif moyen ($)",
        "AVG_TIP_PCT":   "Pourboire moyen (%)",
        "AVG_DISTANCE":  "Distance moyenne (mi)",
    }
    METRIC_NAMES = {
        "TOTAL_TRIPS":   "Nb de courses",
        "TOTAL_REVENUE": "Revenus ($)",
        "AVG_FARE":      "Tarif moyen ($)",
        "AVG_TIP_PCT":   "Pourboire (%)",
        "AVG_DISTANCE":  "Distance (mi)",
    }

    # Valeur courante depuis session_state (persiste entre les reruns)
    metric_choice = st.session_state.get("metric_pills", "TOTAL_TRIPS")
    metric_label  = METRIC_LABELS[metric_choice]

    col1, col2 = st.columns(2)

    with col1:
        fd_copy = fd.copy()
        fd_copy["jour_semaine"] = fd_copy["PICKUP_DATE"].dt.day_name()  # type: ignore
        day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
        day_fr    = ["Lun.", "Mar.", "Mer.", "Jeu.", "Ven.", "Sam.", "Dim."]

        weekly = (
            fd_copy.groupby("jour_semaine")[metric_choice]  # type: ignore
            .mean()
            .reindex(day_order)
            .reset_index()
        )
        weekly["jour_fr"] = day_fr
        mean_val = weekly[metric_choice].mean()
        weekly["color"] = weekly[metric_choice].apply(
            lambda v: nyc_theme.TAXI_GOLD if v >= mean_val else "#4a4760"
        )
        weekly["delta"] = ((weekly[metric_choice] - mean_val) / mean_val * 100).round(1)

        fig_week = go.Figure()
        fig_week.add_trace(go.Bar(
            x=weekly["jour_fr"],
            y=weekly[metric_choice],
            marker_color=weekly["color"],
            text=weekly["delta"].apply(lambda d: f"+{d}%" if d >= 0 else f"{d}%"),
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>" + metric_label + " : %{y:,.0f}<extra></extra>",
        ))
        fig_week.add_hline(
            y=mean_val,
            line_dash="dot",
            line_color=nyc_theme.TEXT_DIM,
            annotation_text="moy.",
            annotation_position="right",
            annotation_font_size=11,
        )
        y_min = weekly[metric_choice].min()
        y_max = weekly[metric_choice].max()
        nyc_theme.style_fig(fig_week)
        fig_week.update_layout(
            title=f"{metric_label} — profil hebdomadaire",
            height=380,
            showlegend=False,
            yaxis_range=[y_min * 0.96, y_max * 1.08],
            yaxis_title=metric_label,
            xaxis_title="",
        )
        st.plotly_chart(fig_week, use_container_width=True)

    with col2:
        hourly_vals = (
            hourly.set_index("PICKUP_HOUR")[metric_choice]
            .reindex(range(24), fill_value=0)
        )
        values_by_hour = hourly_vals.tolist()
        peak_h   = int(hourly_vals.idxmax())
        trough_h = int(hourly_vals.idxmin())

        def _fmt_metric(v):
            if metric_choice == "TOTAL_REVENUE":
                return f"${v/1e3:,.0f}k".replace(",", " ")
            if metric_choice == "AVG_FARE":
                return f"${v:,.2f}"
            if metric_choice == "AVG_TIP_PCT":
                return f"{v:.1f}%"
            if metric_choice == "AVG_DISTANCE":
                return f"{v:.1f} mi"
            return f"{v:,.0f}".replace(",", " ")

        peak_labels = {peak_h: _fmt_metric(values_by_hour[peak_h])}
        if trough_h != peak_h:
            peak_labels[trough_h] = _fmt_metric(values_by_hour[trough_h])

        motif = skyline.MOTIF_BY_METRIC.get(metric_choice, "taxi")
        png_bytes, _ = skyline.render_skyline_png(
            values_by_hour, peak_labels=peak_labels, motif=motif
        )
        st.image(png_bytes, use_container_width=True)
        st.caption(
            (
                f"🌇 {metric_label} par heure — pic à {peak_h}h "
                f"({_fmt_metric(values_by_hour[peak_h])}) · creux à {trough_h}h "
                f"({_fmt_metric(values_by_hour[trough_h])})"
            ).replace("$", r"\$")  # évite l'interprétation LaTeX ($...$) de st.caption
        )

    # Sélecteur de métrique — boutons natifs centrés entre les deux rangées
    st.markdown(
        "<p style='text-align:center; font-size:0.95rem; font-weight:600; "
        f"color:{nyc_theme.TAXI_GOLD}; margin:20px 0 8px;'>"
        "Sélectionner la métrique à afficher sur les graphiques</p>",
        unsafe_allow_html=True,
    )
    _, btn_col, _ = st.columns([1, 5, 1])
    with btn_col:
        cols = st.columns(len(METRIC_OPTIONS))
        for col, opt in zip(cols, METRIC_OPTIONS):
            with col:
                btn_type = "primary" if opt == metric_choice else "secondary"
                if st.button(METRIC_NAMES[opt], key=f"m_{opt}",
                             type=btn_type, use_container_width=True):
                    st.session_state["metric_pills"] = opt
                    st.rerun()
    metric_choice = st.session_state.get("metric_pills", metric_choice)
    metric_label  = METRIC_LABELS[metric_choice]
    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    # -- Évolution pleine largeur ------------------------------------------
    fd_sorted = fd.sort_values("PICKUP_DATE").copy()  # type: ignore
    fd_sorted["MA7"] = fd_sorted[metric_choice].rolling(7, center=True, min_periods=1).mean()

    date_global_min = fd_sorted["PICKUP_DATE"].min()
    date_global_max = fd_sorted["PICKUP_DATE"].max()

    PERIODS = {"1M": 1, "3M": 3, "6M": 6, "1A": 12, "Tout": 0}
    if "evo_period" not in st.session_state:
        st.session_state["evo_period"] = "6M"
    if "evo_offset" not in st.session_state:
        st.session_state["evo_offset"] = 0

    cur_period = st.session_state["evo_period"]
    cur_offset = st.session_state["evo_offset"]

    if cur_period == "Tout":
        x_min, x_max = date_global_min, date_global_max
    else:
        months = PERIODS[cur_period]
        x_max = date_global_max - pd.DateOffset(months=cur_offset * months)
        x_min = x_max - pd.DateOffset(months=months)
        x_min = max(x_min, date_global_min)
        x_max = min(x_max, date_global_max)

    # Contrôles période — radio discret + ← → à droite
    ctl_l, ctl_r = st.columns([6, 1])
    with ctl_l:
        sel_period = st.radio(
            "Période", options=list(PERIODS.keys()),
            index=list(PERIODS.keys()).index(cur_period),
            horizontal=True, key="evo_radio",
            label_visibility="collapsed",
        )
        if sel_period != cur_period:
            st.session_state["evo_period"] = sel_period
            st.session_state["evo_offset"] = 0
            st.rerun()
    with ctl_r:
        nav_l, nav_r = st.columns(2)
        can_prev = cur_period != "Tout" and x_min > date_global_min
        can_next = cur_period != "Tout" and cur_offset > 0
        with nav_l:
            if st.button("←", key="evo_prev", disabled=not can_prev,
                         use_container_width=True):
                st.session_state["evo_offset"] += 1
                st.rerun()
        with nav_r:
            if st.button("→", key="evo_next", disabled=not can_next,
                         use_container_width=True):
                st.session_state["evo_offset"] -= 1
                st.rerun()

    # Min/max sur la fenêtre visible
    fd_vis = fd_sorted[
        (fd_sorted["PICKUP_DATE"] >= x_min) &
        (fd_sorted["PICKUP_DATE"] <= x_max)
    ]
    ann_max = ann_min = None
    if not fd_vis.empty:
        idx_max = fd_vis[metric_choice].idxmax()  # type: ignore
        idx_min = fd_vis[metric_choice].idxmin()  # type: ignore
        ann_max = dict(
            x=fd_vis.loc[idx_max, "PICKUP_DATE"], y=fd_vis.loc[idx_max, metric_choice],
            text=f"▲ {fd_vis.loc[idx_max, 'PICKUP_DATE'].strftime('%-d %b %Y')}",
            showarrow=True, arrowhead=2, arrowcolor="#059669",
            font=dict(size=10, color="#059669"),
            bgcolor="white", bordercolor="#059669", borderwidth=1, ax=0, ay=-36,
        )
        ann_min = dict(
            x=fd_vis.loc[idx_min, "PICKUP_DATE"], y=fd_vis.loc[idx_min, metric_choice],
            text=f"▼ {fd_vis.loc[idx_min, 'PICKUP_DATE'].strftime('%-d %b %Y')}",
            showarrow=True, arrowhead=2, arrowcolor="#DC2626",
            font=dict(size=10, color="#DC2626"),
            bgcolor="white", bordercolor="#DC2626", borderwidth=1, ax=0, ay=36,
        )

    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(
        x=fd_sorted["PICKUP_DATE"], y=fd_sorted[metric_choice],
        mode="lines", line=dict(color="rgba(237,234,227,0.30)", width=1), name="Quotidien",
        hovertemplate="%{x|%d %b %Y}<br>" + metric_label + " : %{y:,.1f}<extra></extra>",
    ))
    fig_line.add_trace(go.Scatter(
        x=fd_sorted["PICKUP_DATE"], y=fd_sorted["MA7"],
        mode="lines", line=dict(color=nyc_theme.TAXI_GOLD, width=2.5), name="Moy. 7 jours",
        hovertemplate="%{x|%d %b %Y}<br>Moy. 7j : %{y:,.1f}<extra></extra>",
    ))
    if ann_max:
        fig_line.add_annotation(**ann_max)
    if ann_min:
        fig_line.add_annotation(**ann_min)
    def bar_rank(data, title, color_scale, height):
        data = data.copy()
        data["date_fr"] = data["PICKUP_DATE"].dt.strftime("%-d %b %Y")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=data[metric_choice], y=data["date_fr"], orientation="h",
            marker=dict(color=data[metric_choice],
                        colorscale=color_scale, showscale=False),
            hovertemplate="<b>%{y}</b><br>" + metric_label + " : %{x:,.1f}<extra></extra>",
        ))
        nyc_theme.style_fig(fig)
        fig.update_layout(
            title=title, height=height,
            xaxis_title="", yaxis_title="",
            margin=dict(l=0, t=36, b=4, r=8),
            font=dict(size=10, color=nyc_theme.TEXT_LIGHT),
        )
        return fig

    top10 = fd.nlargest(10, metric_choice).sort_values(metric_choice)  # type: ignore
    bot10 = fd.nsmallest(10, metric_choice).sort_values(metric_choice, ascending=False)  # type: ignore

    evo_col, rank_col = st.columns(2)

    with evo_col:
        nyc_theme.style_fig(fig_line)
        fig_line.update_layout(
            title=f"{metric_label} — évolution quotidienne",
            height=420,
            yaxis_title=metric_label, xaxis_title="",
            xaxis=dict(range=[x_min, x_max], autorange=False),
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            margin=dict(t=40, b=10),
        )
        st.plotly_chart(fig_line, use_container_width=True)

    with rank_col:
        r1, r2 = st.columns(2)
        with r1:
            st.plotly_chart(bar_rank(top10, "10 meilleurs jours",
                                     [[0, nyc_theme.BG_PANEL_2], [1, nyc_theme.DOLLAR_GREEN]], 420),
                            use_container_width=True)
        with r2:
            st.plotly_chart(bar_rank(bot10, "10 pires jours",
                                     [[0, nyc_theme.BG_PANEL_2], [1, nyc_theme.DUSK_RED]], 420),
                            use_container_width=True)

    st.divider()

    # ------------------------------------------------------------------
    # Section 4 : Géographie
    # ------------------------------------------------------------------
    st.header("🗺️ Quartiers")

    zones["zone_name"] = zones["ZONE_ID"].map(ZONE_LOOKUP).fillna(zones["ZONE_ID"].astype(str))  # type: ignore

    fig_tree = px.treemap(
        zones,
        path=[px.Constant("NYC"), "zone_name"],
        values="TOTAL_TRIPS",
        color="AVG_FARE",
        color_continuous_scale=[[0.0, "#3a3650"], [0.5, "#9f948a"], [1.0, nyc_theme.TAXI_GOLD]],
        template="plotly_dark",
    )
    fig_tree.update_traces(
        root_color=nyc_theme.BG_PAGE,
        hovertemplate="<b>%{label}</b><br>Courses : %{value:,.0f}<extra></extra>",
    )
    nyc_theme.style_fig(fig_tree)
    fig_tree.update_layout(height=500, margin=dict(t=10, l=0, r=0, b=0))
    fig_tree.update_coloraxes(colorbar=dict(title="Tarif moy. ($)", thickness=12, len=0.6))
    st.plotly_chart(fig_tree, use_container_width=True)
    st.markdown(
        "**100 zones affichées sur ~260 zones TLC NYC · "
        "surface = volume de courses · couleur = tarif moyen (doré = plus cher)**"
    )

    st.divider()

    # ------------------------------------------------------------------
    # Section 5 : Portrait type d'un trajet NYC
    # ------------------------------------------------------------------
    st.header("🚖 Portrait type d'un trajet à New York")

    p = profile.iloc[0]

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        card("Distance moyenne", f"{p['AVG_DISTANCE']:.1f} mi",
             "par course", nyc_theme.TAXI_GOLD, "📏")
    with c2:
        card("Tarif moyen", f"${p['AVG_FARE']:.2f}",
             "toutes charges incluses", nyc_theme.DOLLAR_GREEN, "💲")
    with c3:
        card("Courses avec pourboire", f"{p['PCT_AVEC_POURBOIRE']:.0f}%",
             f"pourboire moy. {p['AVG_TIP_PCT']:.1f}% du tarif", nyc_theme.DOLLAR_GREEN, "🎁")
    with c4:
        card("Paiement par carte", f"{p['PCT_CARTE']:.0f}%",
             f"{100 - p['PCT_CARTE']:.0f}% en espèces", nyc_theme.DOLLAR_GREEN, "💳")

    st.markdown("<div style='height:16px'></div>", unsafe_allow_html=True)

    c1, c2, c3, c4 = st.columns(4)
    with c1:
        card("Passagers / course", f"{p['AVG_PASSAGERS']:.1f}",
             "en moyenne", nyc_theme.NIGHT_PURPLE, "👥")
    with c2:
        card("Courses via JFK", f"{p['PCT_AEROPORT_JFK']:.1f}%",
             "départ ou arrivée", nyc_theme.NIGHT_PURPLE, "✈️")
    with c3:
        card("Courses via LaGuardia", f"{p['PCT_AEROPORT_LGA']:.1f}%",
             "départ ou arrivée", nyc_theme.NIGHT_PURPLE, "✈️")
    with c4:
        pct_city = 100 - p['PCT_AEROPORT_JFK'] - p['PCT_AEROPORT_LGA']
        card("Courses intra-ville", f"{pct_city:.0f}%",
             "sans aéroport", nyc_theme.NIGHT_PURPLE, "🏙️")

    st.markdown("<br>", unsafe_allow_html=True)

    # Mini chart : répartition paiement
    pay_data = pd.DataFrame({
        "Mode": ["Carte bancaire", "Espèces", "Autre"],
        "Part (%)": [p["PCT_CARTE"], 100 - p["PCT_CARTE"] - 3, 3],
    })
    fig_pay = px.bar(
        pay_data,
        x="Part (%)",
        y="Mode",
        orientation="h",
        color="Mode",
        color_discrete_sequence=[nyc_theme.TAXI_GOLD, nyc_theme.DOLLAR_GREEN, nyc_theme.TEXT_DIM],
        title="Répartition des modes de paiement",
        text="Part (%)",
    )
    fig_pay.update_traces(texttemplate="%{text:.0f}%", textposition="inside")
    nyc_theme.style_fig(fig_pay)
    fig_pay.update_layout(
        height=180, showlegend=False,
        xaxis=dict(range=[0, 100], title=""),
        yaxis_title="",
        margin=dict(l=0, t=40, b=10),
    )
    _, col_pay, _ = st.columns([0.25, 0.5, 0.25])
    with col_pay:
        st.plotly_chart(fig_pay, use_container_width=True)

    st.markdown("---")
    st.caption("Source : NYC Taxi & Limousine Commission (TLC) — Yellow Taxi Trip Records")


if __name__ == "__main__":
    main()
