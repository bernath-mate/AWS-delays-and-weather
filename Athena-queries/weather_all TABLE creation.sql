CREATE EXTERNAL TABLE weather_all (
    region_id INT,    
    date DATE,
    temperature_mean_c DOUBLE,
    precipitation_sum_mm DOUBLE,
    wind_gust_max_kmh DOUBLE,
    high_temp INT,
    high_wind INT,
    high_precip INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = '\\;')
STORED AS TEXTFILE
LOCATION 's3://delays-weather-slucrx/raw-data/weather/initial/'
TBLPROPERTIES ('has_encrypted_data' = 'false', 'skip.header.line.count' = '1');
