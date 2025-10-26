"""
Étape F : Transformations avec dbt Core
Objectif : Utiliser dbt pour les transformations au lieu du script Python
"""

from loguru import logger
from pathlib import Path
import subprocess
import os

def run_dbt_command(command, project_dir="nyc_taxi_pipeline"):
    """Exécuter une commande dbt"""
    try:
        result = subprocess.run(
            command,
            cwd=project_dir,
            shell=True,
            capture_output=True,
            text=True,
            check=True
        )
        logger.info(f"✅ {command}")
        if result.stdout:
            logger.debug(result.stdout)
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"❌ {command}: {e}")
        if e.stdout:
            logger.error(f"STDOUT: {e.stdout}")
        if e.stderr:
            logger.error(f"STDERR: {e.stderr}")
        return False

def main():
    logger.info("🔄 Étape F : Transformations avec dbt Core")
    
    project_dir = Path("nyc_taxi_pipeline")
    if not project_dir.exists():
        logger.error("❌ Projet dbt non trouvé")
        return
    
    # Vérifier la connexion dbt
    logger.info("🔌 Test de connexion dbt...")
    if not run_dbt_command("dbt debug"):
        logger.error("❌ Problème de connexion dbt")
        return
    
    # Installer les dépendances (si packages.yml existe)
    packages_file = project_dir / "packages.yml"
    if packages_file.exists():
        logger.info("📦 Installation des packages dbt...")
        run_dbt_command("dbt deps")
    
    # Exécuter les transformations
    logger.info("🚀 Exécution des modèles dbt...")
    if not run_dbt_command("dbt run"):
        logger.error("❌ Échec des transformations dbt")
        return
    
    # Exécuter les tests
    logger.info("🧪 Exécution des tests dbt...")
    run_dbt_command("dbt test")
    
    # Générer la documentation
    logger.info("📚 Génération de la documentation...")
    if run_dbt_command("dbt docs generate"):
        logger.success("📖 Documentation générée dans target/")
    
    logger.success("✅ Transformations dbt terminées!")

if __name__ == "__main__":
    main()