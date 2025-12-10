CREATE EXTERNAL TABLE stations_regions (
    station_name STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    county STRING,
    region STRING,
    region_id INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS TEXTFILE
LOCATION 's3://delays-weather-slucrx/raw-data/weather/station-region'
TBLPROPERTIES ('has_encrypted_data' = 'false', 'skip.header.line.count' = '1');
