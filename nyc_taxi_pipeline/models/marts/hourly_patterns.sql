{{ config(materialized='table') }}

-- Table des patterns horaires selon le brief
-- Métriques par heure : demande, revenus, vitesse moyenne

SELECT 
    pickup_hour,
    COUNT(*) as total_trips,
    AVG(total_amount) as avg_revenue,
    AVG(avg_speed_mph) as avg_speed,
    AVG(trip_distance) as avg_distance,
    -- Catégorisation des périodes selon le brief
    CASE 
        WHEN pickup_hour BETWEEN 6 AND 9 THEN 'Rush Matinal'
        WHEN pickup_hour BETWEEN 10 AND 15 THEN 'Journée'
        WHEN pickup_hour BETWEEN 16 AND 19 THEN 'Rush Soir'
        WHEN pickup_hour BETWEEN 20 AND 23 THEN 'Soirée'
        ELSE 'Nuit'
    END as time_period
FROM {{ ref('stg_yellow_taxi_trips') }}
GROUP BY pickup_hour
ORDER BY pickup_hour