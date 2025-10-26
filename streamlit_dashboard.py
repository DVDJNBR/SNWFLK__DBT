"""
🚕 NYC Taxi Dashboard Streamlit
Dashboard interactif pour l'analyse des données NYC Taxi
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import snowflake.connector
from dotenv import load_dotenv
import os
from datetime import datetime, date

# Configuration de la page
st.set_page_config(
    page_title="🚕 NYC Taxi Dashboard",
    page_icon="🚕",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Chargement des variables d'environnement
load_dotenv()

@st.cache_data
def get_snowflake_connection():
    """Connexion Snowflake avec cache"""
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="NYC_TAXI_WH",
        database="NYC_TAXI_DB",
        schema="FINAL",  # Utiliser les tables FINAL créées par le script Python
        role="NYCTRANSFORM"
    )

@st.cache_data(ttl=3600)  # Cache 1 heure
def query_to_df(query):
    """Exécuter une requête et retourner un DataFrame avec cache"""
    conn = get_snowflake_connection()
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        data = cursor.fetchall()
        return pd.DataFrame(data, columns=columns)
    finally:
        conn.close()

@st.cache_data(ttl=3600)
def load_data():
    """Charger toutes les données nécessaires"""
    
    # Données quotidiennes
    daily_data = query_to_df("""
        SELECT pickup_date, total_trips, total_revenue, avg_distance, avg_tip_percentage
        FROM daily_summary 
        ORDER BY pickup_date
    """)
    
    # Données horaires
    hourly_data = query_to_df("""
        SELECT pickup_hour, total_trips, avg_revenue, avg_speed, time_period
        FROM hourly_patterns 
        ORDER BY pickup_hour
    """)
    
    # Top zones
    zone_data = query_to_df("""
        SELECT pickup_zone, total_trips, total_revenue, avg_distance, popularity_rank
        FROM zone_analysis 
        ORDER BY total_trips DESC 
        LIMIT 50
    """)
    
    return daily_data, hourly_data, zone_data

# Interface utilisateur
def main():
    # Titre principal
    st.title("🚕 NYC Taxi Dashboard")
    st.markdown("**Analyse interactive des données NYC Taxi (2024-2025)**")
    
    # Chargement des données
    with st.spinner("📊 Chargement des données..."):
        try:
            daily_data, hourly_data, zone_data = load_data()
            
            # Conversion des dates
            daily_data['PICKUP_DATE'] = pd.to_datetime(daily_data['PICKUP_DATE'])
            
        except Exception as e:
            st.error(f"❌ Erreur de connexion: {e}")
            st.info("💡 Vérifiez que le pipeline a été exécuté et que les tables FINAL existent")
            st.stop()
    
    # Sidebar avec contrôles
    st.sidebar.header("🎛️ Contrôles")
    
    # Filtre de dates
    min_date = daily_data['PICKUP_DATE'].min().date()
    max_date = daily_data['PICKUP_DATE'].max().date()
    
    date_range = st.sidebar.date_input(
        "📅 Période d'analyse",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Filtre des zones
    top_zones = st.sidebar.slider("🏆 Nombre de zones à afficher", 5, 20, 10)
    
    # Métrique à analyser
    metric = st.sidebar.selectbox(
        "📊 Métrique principale",
        ["TOTAL_TRIPS", "TOTAL_REVENUE", "AVG_DISTANCE", "AVG_TIP_PERCENTAGE"],
        format_func=lambda x: {
            "TOTAL_TRIPS": "Nombre de trajets",
            "TOTAL_REVENUE": "Revenus totaux", 
            "AVG_DISTANCE": "Distance moyenne",
            "AVG_TIP_PERCENTAGE": "Pourboire moyen"
        }[x]
    )
    
    # Filtrer les données selon la période
    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered_daily = daily_data[
            (daily_data['PICKUP_DATE'].dt.date >= start_date) & 
            (daily_data['PICKUP_DATE'].dt.date <= end_date)
        ]
    else:
        filtered_daily = daily_data
    
    # KPIs principaux
    st.header("📊 KPIs Principaux")
    
    col1, col2, col3, col4 = st.columns(4)
    
    total_trips = filtered_daily['TOTAL_TRIPS'].sum()
    total_revenue = filtered_daily['TOTAL_REVENUE'].sum()
    avg_distance = filtered_daily['AVG_DISTANCE'].mean()
    avg_tip = filtered_daily['AVG_TIP_PERCENTAGE'].mean()
    
    with col1:
        st.metric("🚕 Total Trajets", f"{total_trips:,.0f}")
    
    with col2:
        st.metric("💰 Revenus Totaux", f"${total_revenue:,.0f}")
    
    with col3:
        st.metric("📏 Distance Moyenne", f"{avg_distance:.1f} miles")
    
    with col4:
        st.metric("💡 Pourboire Moyen", f"{avg_tip:.1f}%")
    
    # Graphiques principaux
    st.header("📈 Analyses Temporelles")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Évolution temporelle
        fig_time = px.line(
            filtered_daily, 
            x='PICKUP_DATE', 
            y=metric,
            title=f"Évolution - {metric}",
            template="plotly_white"
        )
        fig_time.update_layout(height=400)
        st.plotly_chart(fig_time, use_container_width=True)
    
    with col2:
        # Patterns horaires
        colors = {
            'Rush Matinal': '#ff7f0e', 
            'Journée': '#2ca02c', 
            'Rush Soir': '#d62728', 
            'Soirée': '#9467bd', 
            'Nuit': '#8c564b'
        }
        
        fig_hourly = px.bar(
            hourly_data,
            x='PICKUP_HOUR',
            y='TOTAL_TRIPS',
            color='TIME_PERIOD',
            color_discrete_map=colors,
            title="Demande par Heure",
            template="plotly_white"
        )
        fig_hourly.update_layout(height=400)
        st.plotly_chart(fig_hourly, use_container_width=True)
    
    # Analyses par zones
    st.header("🗺️ Analyses Géographiques")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Top zones
        top_zone_data = zone_data.head(top_zones)
        
        fig_zones = px.bar(
            top_zone_data,
            x='TOTAL_TRIPS',
            y=[f"Zone {zone}" for zone in top_zone_data['PICKUP_ZONE']],
            orientation='h',
            title=f"Top {top_zones} Zones les Plus Actives",
            template="plotly_white"
        )
        fig_zones.update_layout(height=500)
        st.plotly_chart(fig_zones, use_container_width=True)
    
    with col2:
        # Revenus par zone (top 10)
        top_revenue_zones = zone_data.nlargest(10, 'TOTAL_REVENUE')
        
        fig_revenue = px.pie(
            top_revenue_zones,
            values='TOTAL_REVENUE',
            names=[f"Zone {zone}" for zone in top_revenue_zones['PICKUP_ZONE']],
            title="Répartition des Revenus (Top 10)",
            template="plotly_white"
        )
        fig_revenue.update_layout(height=500)
        st.plotly_chart(fig_revenue, use_container_width=True)
    
    # Tableau détaillé
    st.header("📋 Données Détaillées")
    
    tab1, tab2, tab3 = st.tabs(["📅 Données Quotidiennes", "🏆 Top Zones", "⏰ Patterns Horaires"])
    
    with tab1:
        st.dataframe(
            filtered_daily.sort_values('PICKUP_DATE', ascending=False),
            use_container_width=True,
            hide_index=True
        )
    
    with tab2:
        st.dataframe(
            zone_data.head(20),
            use_container_width=True,
            hide_index=True
        )
    
    with tab3:
        st.dataframe(
            hourly_data,
            use_container_width=True,
            hide_index=True
        )
    
    # Insights automatiques
    st.header("🎯 Insights Clés")
    
    # Calculs d'insights
    busiest_day = filtered_daily.loc[filtered_daily['TOTAL_TRIPS'].idxmax(), 'PICKUP_DATE'].strftime('%Y-%m-%d')
    best_zone = zone_data.iloc[0]['PICKUP_ZONE']
    peak_hour = hourly_data.loc[hourly_data['TOTAL_TRIPS'].idxmax(), 'PICKUP_HOUR']
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info(f"📅 **Jour le plus actif**: {busiest_day}")
    
    with col2:
        st.info(f"🏆 **Zone la plus populaire**: Zone {best_zone}")
    
    with col3:
        st.info(f"⏰ **Heure de pointe**: {peak_hour}h")
    
    # Footer
    st.markdown("---")
    st.markdown("**🚕 NYC Taxi Dashboard** - Données 2024-2025 | Powered by Streamlit + Snowflake")

if __name__ == "__main__":
    main()