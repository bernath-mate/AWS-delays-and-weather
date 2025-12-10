SELECT 
    station_name,
    region,
    COUNT(*) as observed_days,
    COUNT(DISTINCT CASE WHEN high_temp = 1 OR high_wind = 1 OR high_precip = 1 THEN date END) 
        as extreme_weather_days,
    COUNT(DISTINCT CASE 
        WHEN (high_temp = 1 OR high_wind = 1 OR high_precip = 1) 
            AND avg_delay_minutes <= (
                SELECT AVG(avg_delay_minutes)
                FROM unified_all
                WHERE high_temp = 0 AND high_wind = 0 AND high_precip = 0
            )
        THEN date 
    END) as resilient_days,
    ROUND(
        CAST(COUNT(DISTINCT CASE 
                WHEN (high_temp = 1 OR high_wind = 1 OR high_precip = 1) 
                AND avg_delay_minutes <= (
                    SELECT AVG(avg_delay_minutes)
                    FROM unified_all
                    WHERE high_temp = 0 AND high_wind = 0 AND high_precip = 0
                )
                THEN date 
            END) AS DOUBLE) /
        NULLIF(COUNT(DISTINCT CASE WHEN high_temp = 1 OR high_wind = 1 OR high_precip = 1 THEN date END), 0) 
        * 100, 1
    ) as resilience_score_pct
FROM unified_all
GROUP BY station_name, region
ORDER BY resilience_score_pct ASC
LIMIT 10;
