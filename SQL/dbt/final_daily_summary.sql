-- FINAL.daily_summary
-- Table de résumé quotidien selon le brief

CREATE OR REPLACE TABLE FINAL.daily_summary AS
SELECT 
    DATE(tpep_pickup_datetime) as pickup_date,
    COUNT(*) as total_trips,
    AVG(trip_distance) as avg_distance,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_revenue,
    AVG(tip_percentage) as avg_tip_percentage
FROM STAGING.clean_trips
GROUP BY DATE(tpep_pickup_datetime)
ORDER BY pickup_date
;