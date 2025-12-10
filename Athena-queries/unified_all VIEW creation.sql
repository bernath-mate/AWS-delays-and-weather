CREATE OR REPLACE VIEW unified_all AS
SELECT 
    d.date,
    d.station_name,
    sr.county,
    sr.region,
    d.total_delay_minutes,
    d.train_count,
    d.avg_delay_minutes,
    w.high_temp,
    w.high_wind,
    w.high_precip
FROM delays_all d
LEFT JOIN stations_regions sr 
    ON d.station_id = sr.station_id
LEFT JOIN weather_all w 
    ON d.date = w.date 
   AND sr.region_id = w.region_id
WHERE d.station_id IS NOT NULL
ORDER BY d.date, d.station_name;
