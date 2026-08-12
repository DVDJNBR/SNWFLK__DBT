#!/bin/sh
set -e

if [ -z "$(ls -A /app/data/yellow_taxi/*.parquet 2>/dev/null)" ]; then
    echo "Aucune donnée trouvée dans /app/data/yellow_taxi — téléchargement initial (peut prendre plusieurs minutes)..."
    python scripts/B_load_local_parquet.py
fi

exec streamlit run streamlit_dashboard_local.py \
    --server.port=8501 \
    --server.address=0.0.0.0 \
    --server.headless=true
