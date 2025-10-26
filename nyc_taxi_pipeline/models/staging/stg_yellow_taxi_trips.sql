{{ config(materialized='table') }}

-- Modèle staging : nettoyage des données brutes
-- Source : RAW.yellow_taxi_trips

SELECT 
    *,
    -- Enrichissements demandés dans le brief
    DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,
    EXTRACT(HOUR FROM tpep_pickup_datetime) as pickup_hour,
    EXTRACT(DOW FROM tpep_pickup_datetime) as pickup_day_of_week,
    EXTRACT(MONTH FROM tpep_pickup_datetime) as pickup_month,
    CASE 
        WHEN DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) > 0 
        THEN (trip_distance / (DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0))
        ELSE 0 
    END as avg_speed_mph,
    CASE 
        WHEN fare_amount > 0 
        THEN ROUND((tip_amount * 100.0 / fare_amount), 2)
        ELSE 0 
    END as tip_percentage,
    -- Catégorisation des distances selon le brief
    CASE 
        WHEN trip_distance <= 1 THEN 'Courts trajets'
        WHEN trip_distance BETWEEN 1.01 AND 5 THEN 'Trajets moyens'
        WHEN trip_distance BETWEEN 5.01 AND 10 THEN 'Longs trajets'
        WHEN trip_distance > 10 THEN 'Très longs trajets'
        ELSE 'Non défini'
    END as distance_category,
    -- Types de jours selon le brief
    CASE 
        WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (1,2,3,4,5) THEN 'Jours de semaine'
        WHEN EXTRACT(DOW FROM tpep_pickup_datetime) IN (0,6) THEN 'Weekend'
        ELSE 'Non défini'
    END as day_type
FROM {{ source('raw', 'yellow_taxi_trips') }}
WHERE 
    -- Filtres selon le brief
    fare_amount >= 0                                    -- Éliminer montants négatifs
    AND total_amount >= 0                               -- Éliminer montants négatifs
    AND tpep_dropoff_datetime > tpep_pickup_datetime    -- Garder pickup < dropoff
    AND trip_distance BETWEEN 0.1 AND 100              -- Distance entre 0.1 et 100 miles
    AND pulocationid IS NOT NULL                        -- Exclure zones NULL
    AND dolocationid IS NOT NULL                        -- Exclure zones NULL