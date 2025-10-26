{{ config(materialized='table') }}

-- Analyse par catégories de trajets selon le brief
-- Distances et types de jours

SELECT 
    distance_category,
    day_type,
    COUNT(*) as total_trips,
    AVG(trip_distance) as avg_distance,
    AVG(total_amount) as avg_revenue,
    AVG(trip_duration_minutes) as avg_duration,
    AVG(avg_speed_mph) as avg_speed,
    AVG(tip_percentage) as avg_tip_percentage
FROM {{ ref('stg_yellow_taxi_trips') }}
GROUP BY distance_category, day_type
ORDER BY total_trips DESC