{{ config(materialized='table') }}

-- Table de résumé quotidien selon le brief
-- Métriques par jour : nombre de trajets, distance moyenne, revenus totaux

SELECT 
    DATE(tpep_pickup_datetime) as pickup_date,
    COUNT(*) as total_trips,
    AVG(trip_distance) as avg_distance,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_revenue,
    AVG(tip_percentage) as avg_tip_percentage
FROM {{ ref('stg_yellow_taxi_trips') }}
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY pickup_date