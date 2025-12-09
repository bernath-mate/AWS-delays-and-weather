import sys
import boto3
import json
import requests
import re
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import *

args = {}
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init("weather-update-weekly-ETL", {})

BUCKET = 'delays-weather-slucrx'
API_BASE = 'https://archive-api.open-meteo.com/v1/archive'

s3 = boto3.client('s3')
athena_client = boto3.client('athena', region_name='us-east-1')

try:
    print("starting weather-update-weekly-ETL (Option C: Only append new data)")
    
    # ===== EXTRACT WEEK IDENTIFIER FROM LATEST DELAY CSV =====
    try:
        print("scanning for latest delays CSV filename to extract date range")
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix='raw-data/delays/weekly-updates/',
            Delimiter='/'
        )
        
        if 'Contents' not in response or len(response['Contents']) == 0:
            raise Exception("no delay CSV files found in weekly-updates folder")
        
        # Get latest file by modification date
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        latest_file = files['Key']
        filename = latest_file.split('/')[-1]
        
        print(f"latest delay file: {filename}")
        
        # Extract dates: delay_data_YYYY-MM-DD_YYYY-MM-DD.csv
        match = re.search(r'delay_data_(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})', filename)
        
        if not match:
            raise Exception(f"cannot parse date range from filename: {filename}")
        
        START_DATE = match.group(1)
        END_DATE = match.group(2)
        WEEK_IDENTIFIER = f"{START_DATE}_{END_DATE}"
        
        print(f"extracted date range: {START_DATE} to {END_DATE}")
        print(f"week identifier: {WEEK_IDENTIFIER}")
    
    except Exception as e:
        print(f"error extracting week identifier: {str(e)}")
        raise
    
    # ===== LOAD REGION COORDINATES =====
    try:
        print("loading region coordinates from config")
        response = s3.get_object(
            Bucket=BUCKET,
            Key='/raw-data/weather/region_coordinates.json'
        )
        REGIONS = json.loads(response['Body'].read().decode('utf-8'))
        print(f"loaded {len(REGIONS)} regions")
    except Exception as e:
        print(f"error loading regions config: {str(e)}")
        raise
    
    # ===== FETCH WEATHER FROM API FOR THE NEW WEEK =====
    try:
        print(f"fetching weather from API for {len(REGIONS)} regions (dates: {START_DATE} to {END_DATE})")
        all_weather = []
        
        for region_name, coords in REGIONS.items():
            try:
                print(f"  fetching: {region_name}")
                
                params = {
                    'latitude': coords['lat'],
                    'longitude': coords['lon'],
                    'start_date': START_DATE,
                    'end_date': END_DATE,
                    'daily': 'temperature_2m_mean,wind_gusts_10m_max,precipitation_sum',
                    'timezone': 'auto'
                }
                
                response = requests.get(API_BASE, params=params, timeout=420)
                response.raise_for_status()
                data = response.json()
                
                times = data['daily']['time']
                temps = data['daily']['temperature_2m_mean']
                winds = data['daily']['wind_gusts_10m_max']
                precips = data['daily']['precipitation_sum']
                
                for i, date_str in enumerate(times):
                    record = {
                        'date': date_str,
                        'station_name': region_name,  # Region name as station_name
                        'latitude': float(coords['lat']),
                        'longitude': float(coords['lon']),
                        'temperature_mean_c': float(temps[i]),
                        'wind_gust_max_kmh': float(winds[i]),
                        'precipitation_sum_mm': float(precips[i]),
                        'extreme_temperature': 1 if (temps[i] < -15 or temps[i] > 35) else 0,
                        'extreme_wind': 1 if winds[i] > 60 else 0,
                        'extreme_precipitation': 1 if precips[i] > 20 else 0
                    }
                    all_weather.append(record)
                
                region_records = len([r for r in all_weather if r['station_name'] == region_name])
                print(f"    {region_name}: {region_records} records")
            
            except Exception as e:
                print(f"    error fetching {region_name}: {str(e)}")
                raise
        
        if not all_weather:
            raise Exception("no weather data fetched from API")
        
        print(f"total records fetched from API: {len(all_weather)}")
    
    except Exception as e:
        print(f"error fetching weather from API: {str(e)}")
        raise
    
    # ===== WRITE NEW WEEK'S WEATHER CSV TO RAW DATA =====
    try:
        print(f"writing new week's weather CSV: weather_data_{WEEK_IDENTIFIER}.csv")
        
        df_weekly_weather = spark.createDataFrame(all_weather)
        
        weekly_output_path = f"s3://{BUCKET}/raw-data/weather/weekly-updates/weather_data_{WEEK_IDENTIFIER}"
        
        df_weekly_weather.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(weekly_output_path)
        
        print(f"successfully wrote weekly weather CSV to: {weekly_output_path}")
    
    except Exception as e:
        print(f"error writing weekly weather CSV: {str(e)}")
        raise
    
    # ===== SCHEMA VALIDATION & CASTING =====
    try:
        print("applying schema and casting columns")
        df_final = df_weekly_weather.select(
            col("date").cast("date").alias("date"),
            col("station_name").cast("string").alias("station_name"),
            col("latitude").cast("double").alias("latitude"),
            col("longitude").cast("double").alias("longitude"),
            col("temperature_mean_c").cast("double").alias("temperature_mean_c"),
            col("wind_gust_max_kmh").cast("double").alias("wind_gust_max_kmh"),
            col("precipitation_sum_mm").cast("double").alias("precipitation_sum_mm"),
            col("extreme_temperature").cast("int").alias("extreme_temperature"),
            col("extreme_wind").cast("int").alias("extreme_wind"),
            col("extreme_precipitation").cast("int").alias("extreme_precipitation")
        ).repartition(1)
        print("schema casting completed")
    except Exception as e:
        print(f"failed to apply schema: {str(e)}")
        raise
    
    # ===== APPEND NEW WEEK TO processed-data =====
    try:
        output_path = f"s3://{BUCKET}/processed-data/weather_all/"
        print(f"appending new week ({len(all_weather)} rows) to: {output_path}")
        
        df_final.coalesce(1).write \
            .mode("append") \
            .option("header", "true") \
            .csv(output_path)
        
        print(f"successfully appended {len(all_weather)} new weather rows to weather_all")
    except Exception as e:
        print(f"error appending weather data: {str(e)}")
        raise
    
    # ===== REFRESH ATHENA METADATA =====
    try:
        print("running MSCK REPAIR TABLE on weather_all")
        repair_query = "MSCK REPAIR TABLE weather_all"
        response = athena_client.start_query_execution(
            QueryString=repair_query,
            QueryExecutionContext={'Database': 'delays_weather'},
            ResultConfiguration={'OutputLocation': f's3://{BUCKET}/query-results/'}
        )
        query_id = response['QueryExecutionId']
        print(f"MSCK repair started: {query_id}")
    except Exception as e:
        print(f"warning: failed to run MSCK repair: {str(e)}")
    
    print("weather-update-weekly-ETL completed successfully")

except Exception as e:
    print(f"ETL job failed: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()