-- FINAL.zone_analysis
-- Table d'analyse par zone selon le brief

CREATE OR REPLACE TABLE FINAL.zone_analysis AS
SELECT 
    pulocationid as pickup_zone,
    COUNT(*) as total_trips,
    AVG(total_amount) as avg_revenue,
    SUM(total_amount) as total_revenue,
    AVG(trip_distance) as avg_distance,
    RANK() OVER (ORDER BY COUNT(*) DESC) as popularity_rank
FROM STAGING.clean_trips
GROUP BY pulocationid
ORDER BY total_trips DESC
;