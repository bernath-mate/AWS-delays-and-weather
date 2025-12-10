SELECT 
    region,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN avg_delay_minutes > 5 THEN avg_delay_minutes END, 0.5), 2) as median_delay,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN high_temp = 1 AND avg_delay_minutes > 5 THEN avg_delay_minutes END, 0.5), 2) as median_high_temp,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN high_wind = 1 AND avg_delay_minutes > 5 THEN avg_delay_minutes END, 0.5), 2) as median_high_wind,
    ROUND(APPROX_PERCENTILE(
        CASE WHEN high_precip = 1 AND avg_delay_minutes > 5 THEN avg_delay_minutes END, 0.5), 2) as median_high_precip,
    COUNT(DISTINCT date) as days_tracked
FROM unified_all
GROUP BY region
ORDER BY median_delay DESC;
