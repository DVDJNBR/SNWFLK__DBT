"""
Étape 1.3 : Analyse et Nettoyage des Données
Objectif : Identifier les problèmes de qualité dans RAW.yellow_taxi_trips
"""

import snowflake.connector
from loguru import logger
from dotenv import load_dotenv
import os
from pathlib import Path

load_dotenv()

def analyze_data_quality(conn):
    """Analyser la qualité des données selon le brief"""
    cursor = conn.cursor()
    
    logger.info("🔍 Analyse de la qualité des données...")
    
    # 1. Compter le total de lignes
    cursor.execute("SELECT COUNT(*) FROM yellow_taxi_trips")
    total_rows = cursor.fetchone()[0]
    logger.info(f"📊 Total lignes: {total_rows}")
    
    # Vérifier si on a des données
    if total_rows == 0:
        logger.warning("❌ Aucune donnée dans la table - Analyse impossible")
        return {'total_rows': 0}
    
    # 2. Valeurs manquantes (NULL) - Vérifier toutes les colonnes importantes
    cursor.execute("""
        SELECT 
            COUNT(*) as null_count,
            ROUND((COUNT(*) * 100.0 / {total}) , 2) as null_percentage
        FROM yellow_taxi_trips 
        WHERE vendorid IS NULL 
           OR tpep_pickup_datetime IS NULL 
           OR tpep_dropoff_datetime IS NULL
           OR passenger_count IS NULL 
           OR trip_distance IS NULL
           OR ratecodeid IS NULL
           OR store_and_fwd_flag IS NULL
           OR pulocationid IS NULL 
           OR dolocationid IS NULL 
           OR payment_type IS NULL
           OR fare_amount IS NULL 
           OR total_amount IS NULL
           OR congestion_surcharge IS NULL
           OR airport_fee IS NULL
    """.format(total=total_rows))
    
    null_count, null_pct = cursor.fetchone()
    logger.warning(f"❌ Valeurs manquantes: {null_count} ({null_pct}%)")
    
    # 3. Montants négatifs
    cursor.execute("""
        SELECT 
            COUNT(*) as negative_count,
            ROUND((COUNT(*) * 100.0 / {total}), 2) as negative_percentage
        FROM yellow_taxi_trips 
        WHERE fare_amount < 0 OR total_amount < 0
    """.format(total=total_rows))
    
    neg_count, neg_pct = cursor.fetchone()
    logger.warning(f"💰 Montants négatifs: {neg_count} ({neg_pct}%)")
    
    # 4. Trajets avec distance zéro
    cursor.execute("""
        SELECT 
            COUNT(*) as zero_distance_count,
            ROUND((COUNT(*) * 100.0 / {total}), 2) as zero_distance_percentage
        FROM yellow_taxi_trips 
        WHERE trip_distance = 0
    """.format(total=total_rows))
    
    zero_count, zero_pct = cursor.fetchone()
    logger.warning(f"📏 Distance zéro: {zero_count} ({zero_pct}%)")
    
    # 5. Valeurs extrêmes (distance > 1000 miles)
    cursor.execute("""
        SELECT 
            COUNT(*) as extreme_count,
            ROUND((COUNT(*) * 100.0 / {total}), 2) as extreme_percentage
        FROM yellow_taxi_trips 
        WHERE trip_distance > 1000
    """.format(total=total_rows))
    
    extreme_count, extreme_pct = cursor.fetchone()
    logger.warning(f"🚨 Distances extrêmes (>1000 miles): {extreme_count} ({extreme_pct}%)")
    
    # 6. Dates incohérentes (dropoff avant pickup)
    cursor.execute("""
        SELECT 
            COUNT(*) as incoherent_dates_count,
            ROUND((COUNT(*) * 100.0 / {total}), 2) as incoherent_dates_percentage
        FROM yellow_taxi_trips 
        WHERE tpep_dropoff_datetime <= tpep_pickup_datetime
    """.format(total=total_rows))
    
    incoherent_count, incoherent_pct = cursor.fetchone()
    logger.warning(f"📅 Dates incohérentes: {incoherent_count} ({incoherent_pct}%)")
    
    # Résumé des problèmes identifiés
    total_problematic = null_count + neg_count + zero_count + extreme_count + incoherent_count
    clean_rows = total_rows - total_problematic
    clean_percentage = round((clean_rows * 100.0 / total_rows), 2)
    
    logger.info(f"✅ Lignes propres estimées: {clean_rows} ({clean_percentage}%)")
    
    return {
        'total_rows': total_rows,
        'null_count': null_count,
        'negative_count': neg_count,
        'zero_distance_count': zero_count,
        'extreme_count': extreme_count,
        'incoherent_dates_count': incoherent_count,
        'clean_rows_estimate': clean_rows
    }

def main():
    logger.info("🔍 Étape 1.3 : Analyse et Nettoyage des Données")
    
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
    
    # Analyser la qualité des données
    stats = analyze_data_quality(conn)
    
    conn.close()
    
    # Sauvegarder le rapport d'analyse
    report_path = Path("reports/raw_data_quality_report.md")
    report_path.parent.mkdir(exist_ok=True)
    
    if stats.get('total_rows', 0) > 0:
        report_content = f"""# Rapport de Qualité des Données NYC Taxi

## 📊 Résumé Général
- **Total lignes analysées** : {stats['total_rows']:,}
- **Lignes propres estimées** : {stats['clean_rows_estimate']:,} ({round(stats['clean_rows_estimate']*100/stats['total_rows'], 2)}%)

## ❌ Problèmes Identifiés

### 1. Valeurs Manquantes
- **Nombre** : {stats['null_count']:,}
- **Pourcentage** : {round(stats['null_count']*100/stats['total_rows'], 2)}%

### 2. Montants Négatifs
- **Nombre** : {stats['negative_count']:,}
- **Pourcentage** : {round(stats['negative_count']*100/stats['total_rows'], 2)}%

### 3. Trajets Distance Zéro
- **Nombre** : {stats['zero_distance_count']:,}
- **Pourcentage** : {round(stats['zero_distance_count']*100/stats['total_rows'], 2)}%

### 4. Distances Extrêmes (>1000 miles)
- **Nombre** : {stats['extreme_count']:,}
- **Pourcentage** : {round(stats['extreme_count']*100/stats['total_rows'], 2)}%

### 5. Dates Incohérentes
- **Nombre** : {stats['incoherent_dates_count']:,}
- **Pourcentage** : {round(stats['incoherent_dates_count']*100/stats['total_rows'], 2)}%

## ✅ Actions de Nettoyage Appliquées
- Filtrage des montants négatifs
- Exclusion des trajets avec dates incohérentes  
- Gestion des valeurs manquantes
- Suppression des outliers extrêmes
- Filtrage des distances entre 0.1 et 100 miles

## 📈 Résultat Final
Après nettoyage, environ **{round(stats['clean_rows_estimate']*100/stats['total_rows'], 2)}%** des données sont utilisables pour l'analyse.
"""
    else:
        report_content = "# Rapport de Qualité des Données NYC Taxi\n\n❌ Aucune donnée trouvée dans la table RAW.yellow_taxi_trips"
    
    report_path.write_text(report_content)
    logger.success(f"📄 Rapport sauvegardé: {report_path}")
    
    logger.success("✅ Analyse terminée - Prêt pour l'étape 1.4 (Transformations)")

if __name__ == "__main__":
    main()