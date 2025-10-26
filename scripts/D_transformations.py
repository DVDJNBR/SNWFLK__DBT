"""
Étape 1.4 : Transformations de Base
Objectif : Créer les tables STAGING.clean_trips et les tables FINAL selon le brief
"""

import snowflake.connector
from loguru import logger
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()

def create_staging_clean_trips(conn):
    """Créer STAGING.clean_trips avec les filtres du brief"""
    cursor = conn.cursor()
    
    logger.info("🧹 Création de STAGING.clean_trips...")
    
    # Lire et exécuter le fichier SQL
    sql_file = Path("SQL/dbt/staging_clean_trips.sql")
    sql_content = sql_file.read_text()
    cursor.execute(sql_content)
    
    # Compter les résultats
    cursor.execute("SELECT COUNT(*) FROM STAGING.clean_trips")
    clean_count = cursor.fetchone()[0]
    
    logger.success(f"✅ STAGING.clean_trips créée: {clean_count} lignes")
    return clean_count

def create_final_tables(conn):
    """Créer les tables FINAL selon le brief"""
    cursor = conn.cursor()
    
    logger.info("📊 Création des tables FINAL...")
    
    # Liste des fichiers SQL à exécuter
    sql_files = [
        ("final_daily_summary.sql", "daily_summary", "jours"),
        ("final_zone_analysis.sql", "zone_analysis", "zones"),
        ("final_hourly_patterns.sql", "hourly_patterns", "heures")
    ]
    
    for sql_file, table_name, unit in sql_files:
        # Lire et exécuter le fichier SQL
        sql_path = Path(f"SQL/dbt/{sql_file}")
        sql_content = sql_path.read_text()
        cursor.execute(sql_content)
        
        # Compter les résultats
        cursor.execute(f"SELECT COUNT(*) FROM FINAL.{table_name}")
        count = cursor.fetchone()[0]
        logger.success(f"✅ FINAL.{table_name} créée: {count} {unit}")

def main():
    logger.info("🔄 Étape 1.4 : Transformations de Base")
    
    # Connexion Snowflake
    conn = snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        warehouse="NYC_TAXI_WH",
        database="NYC_TAXI_DB",
        role="NYCTRANSFORM"
    )
    
    # Créer les tables selon le brief
    clean_count = create_staging_clean_trips(conn)
    create_final_tables(conn)
    
    conn.close()
    
    logger.success("✅ Transformations terminées - Architecture RAW → STAGING → FINAL complète!")

if __name__ == "__main__":
    main()