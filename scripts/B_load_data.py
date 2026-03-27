"""
Étape 1.2 : Chargement des Données (2024-2025)
Objectif : Télécharger tous les mois de janvier 2024 à aujourd'hui en local (format Parquet)
"""

import httpx
from pathlib import Path
from loguru import logger

def load_month(year_month):
    """Charger un mois de données"""
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year_month}.parquet"
    data_dir = Path("data/yellow_taxi")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    local_file = data_dir / f"yellow_tripdata_{year_month.replace('-', '_')}.parquet"
    
    if local_file.exists():
        logger.info(f"✅ {year_month} déjà téléchargé ({local_file.stat().st_size / 1024 / 1024:.1f} MB)")
        return True
        
    try:
        # Télécharger
        logger.info(f"📥 Téléchargement {year_month}...")
        with httpx.stream("GET", url, timeout=300.0) as response:
            response.raise_for_status()
            with open(local_file, "wb") as file:
                for chunk in response.iter_bytes(chunk_size=8192):
                    file.write(chunk)
        
        logger.success(f"✅ {year_month} téléchargé avec succès")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erreur {year_month}: {e}")
        if local_file.exists():
            local_file.unlink()
        return False

def main():
    logger.info("🚀 Étape 1.2 : Téléchargement des données NYC Taxi 2024-2025")
    
    # Tous les mois 2024
    months_2024 = [f"2024-{i:02d}" for i in range(1, 13)]
    # Mois 2025 disponibles (jusqu'à septembre)
    months_2025 = [f"2025-{i:02d}" for i in range(1, 10)]
    
    all_months = months_2024 + months_2025
    logger.info(f"📅 Mois à charger: {len(all_months)}")
    
    successful = 0
    for month in all_months:
        if load_month(month):
            successful += 1
            
    logger.success(f"✅ Chargement terminé: {successful}/{len(all_months)} mois prêts en local")

if __name__ == "__main__":
    main()