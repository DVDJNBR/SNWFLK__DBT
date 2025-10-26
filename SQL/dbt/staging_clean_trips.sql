-- STAGING.clean_trips
-- Table principale nettoyée selon les critères du brief

CREATE OR REPLACE TABLE STAGING.clean_trips AS
SELECT 
    *,
    -- Enrichissements demandés dans le brief
    DATEDIFF('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,
    EXTRACT(HOUR FROM tpep_pickup_datetime) as pickup_hour,
    EXTRACT(DOW FROM tpep_pickup_datetime) as pickup_day_of_week,
    EXTRACT(MONTH FROM tpep_pickup_datetime) as pickup_month,
    CASE 
        WHEN trip_duration_minutes > 0 
        THEN (trip_distance / (trip_duration_minutes / 60.0))
        ELSE 0 
    END as avg_speed_mph,
    CASE 
        WHEN fare_amount > 0 
        THEN ROUND((tip_amount * 100.0 / fare_amount), 2)
        ELSE 0 
    END as tip_percentage
FROM RAW.yellow_taxi_trips
WHERE 
    -- Filtres selon le brief
    fare_amount >= 0                                    -- Éliminer montants négatifs
    AND total_amount >= 0                               -- Éliminer montants négatifs
    AND tpep_dropoff_datetime > tpep_pickup_datetime    -- Garder pickup < dropoff
    AND trip_distance BETWEEN 0.1 AND 100              -- Distance entre 0.1 et 100 miles
    AND pulocationid IS NOT NULL                        -- Exclure zones NULL
    AND dolocationid IS NOT NULL                        -- Exclure zones NULL
;