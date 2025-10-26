{{ config(materialized='table') }}

-- Table d'analyse par zone selon le brief
-- Métriques par zone de départ : volume, revenus moyens, popularité

SELECT 
    pulocationid as pickup_zone,
    COUNT(*) as total_trips,
    AVG(total_amount) as avg_revenue,
    SUM(total_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    RANK() OVER (ORDER BY COUNT(*) DESC) as popularity_rank
FROM {{ ref('stg_yellow_taxi_trips') }}
GROUP BY pulocationid
ORDER BY total_trips DESC