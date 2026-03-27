"""
Étape 1.2 : Chargement des Données (2024-2025)
Objectif : Charger tous les mois de janvier 2024 à aujourd'hui dans RAW.yellow_taxi_trips
"""

import httpx
import snowflake.connector
from pathlib import Path
from loguru import logger
from dotenv import load_dotenv
import os

load_dotenv()

def load_month(year_month, conn):
    """Charger un mois de données"""
    cursor = conn.cursor()
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
    local_file = Path(f"temp_{year_month.replace('-', '_')}.parquet")
    
    try:
        # Télécharger
        logger.info(f"📥 Téléchargement {year_month}...")
        with httpx.stream("GET", url, timeout=300.0) as response:
            response.raise_for_status()
            with open(local_file, "wb") as file:
                for chunk in response.iter_bytes(chunk_size=8192):
                    file.write(chunk)
        
        # Upload vers Snowflake
        stage_name = f"stage_{year_month.replace('-', '_')}"
        cursor.execute(f"CREATE OR REPLACE TEMP STAGE {stage_name}")
        cursor.execute(f"PUT file://{local_file.absolute()} @{stage_name} AUTO_COMPRESS=FALSE")
        
        cursor.execute(f"""
            COPY INTO yellow_taxi_trips
            FROM @{stage_name}
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        """)
        
        # Nettoyer
        local_file.unlink()
        logger.success(f"✅ {year_month} chargé")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur {year_month}: {e}")
        if local_file.exists():
            local_file.unlink()
        return False

def main():
    logger.info("🚀 Étape 1.2 : Chargement des données NYC Taxi 2024-2025")
    
    # Connexion Snowflake
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="NYC_TAXI_WH",
        database="NYC_TAXI_DB",
        schema="RAW",
        role="NYCTRANSFORM"
    )
    
    cursor = conn.cursor()
    cursor.execute("TRUNCATE TABLE yellow_taxi_trips")
    logger.info("🧹 Table RAW.yellow_taxi_trips vidée")
    
    # Tous les mois 2024
    months_2024 = [f"2024-{i:02d}" for i in range(1, 13)]
    # Mois 2025 disponibles (jusqu'à septembre)
    months_2025 = [f"2025-{i:02d}" for i in range(1, 10)]
    
    all_months = months_2024 + months_2025
    logger.info(f"📅 Mois à charger: {len(all_months)}")
    
    successful = 0
    for month in all_months:
        if load_month(month, conn):
            successful += 1
    
    # Compter le total final
    cursor.execute("SELECT COUNT(*) FROM yellow_taxi_trips")
    row_count = cursor.fetchone()
    total_count = row_count[0] if row_count else 0
    
    logger.success(f"✅ Chargement terminé: {successful}/{len(all_months)} mois - {total_count:,} lignes totales")
    conn.close()

if __name__ == "__main__":
    main()