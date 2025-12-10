SELECT 
    station_name,
    region,
    COUNT(*) as observed_days,
    COUNT(CASE WHEN avg_delay_minutes > 5 THEN 1 ELSE 0 END) as delayed_days,
    ROUND(100.0 * COUNT(CASE WHEN avg_delay_minutes > 5 THEN 1 ELSE 0 END) / COUNT(*), 2) as percent_delayed_days
FROM unified_all
GROUP BY station_name, region
ORDER BY percent_delayed_days DESC
LIMIT 10;
