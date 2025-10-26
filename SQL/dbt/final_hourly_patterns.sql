-- FINAL.hourly_patterns
-- Table des patterns horaires selon le brief

CREATE OR REPLACE TABLE FINAL.hourly_patterns AS
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
FROM STAGING.clean_trips
GROUP BY pickup_hour
ORDER BY pickup_hour
;