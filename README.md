# NYC Taxi Pipeline

Mode d'emploi pour reproduire le brief NYC Taxi en local avec **DuckDB** et **Streamlit**. 
(L'architecture avec Snowflake et dbt est préservée dans l'historique mais n'est pas requise pour lancer cette version optimisée locale).

## Prérequis

- Python 3.11+
- uv (gestionnaire de packages)
- Git

## Installation

```bash
git clone <repository>
cd nyc_taxi_pipeline
uv sync
```

## Étapes du projet

### 1. Téléchargement des Données

```bash
inv load-data
```
Ou bien `python scripts/B_load_data.py`

*Cette commande télécharge les données des trajets de taxi de New York (2024-2025) sous forme de fichiers Parquet dans le dossier `data/yellow_taxi/`. Ces fichiers compressés et partitionnés sont idéaux pour de l'analyse locale performante.*

### 2. Dashboard Interactif (Streamlit & DuckDB)

L'application Streamlit utilise **DuckDB** pour interroger les fichiers Parquet locaux à la volée avec des performances proches d'un entrepôt de données cloud.

```bash
inv dashboard
```
*Lance Streamlit sur http://localhost:8501*

## Captures d'écran du Dashboard

![Vue générale des KPIs](reports/placeholder_kpi.png)

![Répartition Horaires / Géographie](reports/placeholder_geo.png)

*(Insérer ci-dessus les captures définitives avant présentation)*

## Composants Additionnels (Optionnels)

### Architecture dbt & Snowflake

Dans le cadre du MVP précédent, une architecture 3 couches (RAW -> STAGING -> FINAL) a été construite dans **Snowflake** et gérée par **dbt**.
- Consulter le dossier `SQL/` pour les requêtes initiales.
- Consulter le dossier `nyc_taxi_pipeline/` pour le projet dbt complet (tests qualitatifs, modèles STAGING/MARTS).

### Scripts Analytiques & Matplotlib

Vous pouvez générer des rapports complémentaires :
```bash
inv data-analysis
inv generate-report
```
*Ces scripts sauvegardent des notes de qualité de la donnée et des graphiques statiques dans le dossier `reports/`.*

## Résultats

- **Millions de lignes** de données NYC Taxi traitables localement via Parquet
- **Dashboard Streamlit** interactif et responsif
- **Exécution ultra-rapide** grâce à DuckDB éliminant le besoin d'infrastructure cloud coûteuse