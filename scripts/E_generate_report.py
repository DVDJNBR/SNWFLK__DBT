"""
Étape E : Génération de graphiques simples
Objectif : Créer quelques visualisations matplotlib basiques
"""

import snowflake.connector
from loguru import logger
from dotenv import load_dotenv
import os
from pathlib import Path
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches

load_dotenv()

def main():
    logger.info("📊 Génération des graphiques matplotlib")
    
    # Connexion Snowflake
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="NYC_TAXI_WH",
        database="NYC_TAXI_DB",
        role="NYCTRANSFORM"
    )
    
    cursor = conn.cursor()
    
    # Créer le dossier reports
    reports_dir = Path("reports")
    reports_dir.mkdir(exist_ok=True)
    
    # 1. Graphique qualité des données
    logger.info("📈 Graphique qualité des données...")
    
    cursor.execute("SELECT COUNT(*) FROM RAW.yellow_taxi_trips")
    total_raw = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM STAGING.clean_trips")
    total_clean = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM RAW.yellow_taxi_trips WHERE passenger_count IS NULL OR ratecodeid IS NULL OR store_and_fwd_flag IS NULL OR congestion_surcharge IS NULL OR airport_fee IS NULL")
    null_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM RAW.yellow_taxi_trips WHERE fare_amount < 0 OR total_amount < 0")
    negative_count = cursor.fetchone()[0]
    
    # Graphique en camembert
    plt.figure(figsize=(10, 6))
    
    plt.subplot(1, 2, 1)
    labels = ['Données propres', 'Valeurs manquantes', 'Montants négatifs']
    sizes = [total_clean, null_count, negative_count]
    colors = ['#2ecc71', '#f39c12', '#e74c3c']
    
    # S'assurer que toutes les valeurs sont positives
    sizes = [max(0, s) for s in sizes]
    
    plt.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
    plt.title('Qualité des Données NYC Taxi')
    
    # 2. Top zones
    plt.subplot(1, 2, 2)
    cursor.execute("SELECT pickup_zone, total_trips FROM FINAL.zone_analysis ORDER BY total_trips DESC LIMIT 5")
    top_zones = cursor.fetchall()
    
    zones = [f"Zone {row[0]}" for row in top_zones]
    trips = [row[1] for row in top_zones]
    
    plt.bar(zones, trips, color='#3498db')
    plt.title('Top 5 Zones par Nombre de Trajets')
    plt.xticks(rotation=45)
    plt.ylabel('Nombre de trajets')
    
    plt.tight_layout()
    plt.savefig(reports_dir / 'data_quality_overview.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    # 3. Patterns horaires
    logger.info("⏰ Graphique patterns horaires...")
    
    cursor.execute("SELECT pickup_hour, total_trips FROM FINAL.hourly_patterns ORDER BY pickup_hour")
    hourly_data = cursor.fetchall()
    
    hours = [row[0] for row in hourly_data]
    trips_by_hour = [row[1] for row in hourly_data]
    
    plt.figure(figsize=(12, 6))
    plt.plot(hours, trips_by_hour, marker='o', linewidth=2, markersize=6, color='#e74c3c')
    plt.title('Demande de Taxis par Heure de la Journée')
    plt.xlabel('Heure')
    plt.ylabel('Nombre de trajets')
    plt.grid(True, alpha=0.3)
    plt.xticks(range(0, 24))
    
    # Zones colorées pour les périodes
    plt.axvspan(6, 9, alpha=0.2, color='orange', label='Rush matinal')
    plt.axvspan(16, 19, alpha=0.2, color='red', label='Rush soir')
    plt.axvspan(20, 23, alpha=0.2, color='purple', label='Soirée')
    
    plt.legend()
    plt.tight_layout()
    plt.savefig(reports_dir / 'hourly_patterns.png', dpi=150, bbox_inches='tight')
    plt.close()
    
    conn.close()
    
    logger.success(f"✅ Graphiques générés dans {reports_dir}/")
    logger.info("📁 Fichiers créés:")
    logger.info("  - data_quality_overview.png")
    logger.info("  - hourly_patterns.png")

if __name__ == "__main__":
    main()