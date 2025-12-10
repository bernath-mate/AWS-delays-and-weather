import sys
import time
import boto3
import json
import requests
import re
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.context import SparkContext
from pyspark.sql.functions import col

args = {}
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
job = Job(glueContext)
job.init("weather-update-weekly-ETL", {})

BUCKET = 'delays-weather-slucrx'
API_BASE = 'https://archive-api.open-meteo.com/v1/archive'

s3 = boto3.client('s3')
athena_client = boto3.client('athena', region_name='us-east-1')

def wait_for_query_completion(client, query_id, max_retries=120):
    """Wait for Athena query to complete"""
    for attempt in range(max_retries):
        try:
            response = client.get_query_execution(QueryExecutionId=query_id)
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                return status
            
            print(f"  waiting for query {query_id}... status: {status}")
            time.sleep(1)
        except Exception as e:
            print(f"  error checking query status: {str(e)}")
            time.sleep(1)
    
    raise Exception(f"query {query_id} did not complete after {max_retries} attempts")

try:
    print("starting weather-update-weekly-ETL")

    # ===== EXTRACT WEEK IDENTIFIER FROM LATEST DELAY CSV =====
    try:
        print("scanning for latest delays CSV filename to extract date range")
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix='raw-data/delays/weekly-updates/'
        )

        if 'Contents' not in response or len(response['Contents']) == 0:
            raise Exception("no delay CSV files found in weekly-updates folder")

        # Get latest file by modification date
        files = sorted(response['Contents'], key=lambda x: x['LastModified'], reverse=True)
        latest_file = files[0]['Key']
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
            Key='raw-data/weather/region_coordinates.json'
        )
        data = json.loads(response['Body'].read().decode('utf-8'))
        REGIONS = data['regions']  # Extract the regions list
        print(f"loaded {len(REGIONS)} regions")
    except Exception as e:
        print(f"error loading regions config: {str(e)}")
        raise

    # ===== FETCH WEATHER FROM API FOR THE NEW WEEK =====
    try:
        print(f"fetching weather from API for {len(REGIONS)} regions (dates: {START_DATE} to {END_DATE})")
        all_weather = []

        # region_id is derived from the order of REGIONS (0,1,2,...)
        for region_id, region_data in enumerate(REGIONS):
            try:
                region_name = region_data['region_name']
                coords = {'lat': region_data['lat'], 'lon': region_data['lon']}
                
                print(f"  fetching: {region_name} (region_id={region_id})")

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
                    t = float(temps[i])
                    w = float(winds[i])
                    p = float(precips[i])

                    record = {
                        'region_id': region_id,
                        'date': date_str,
                        'temperature_mean_c': t,
                        'wind_gust_max_kmh': w,
                        'precipitation_sum_mm': p,
                        'high_temp': 1 if (t < -5 or t > 25) else 0,
                        'high_wind': 1 if w > 45 else 0,
                        'high_precip': 1 if p > 5 else 0
                    }
                    all_weather.append(record)

            except Exception as e:
                print(f"    error fetching {region_name}: {str(e)}")
                raise

        if not all_weather:
            raise Exception("no weather data fetched from API")

        print(f"total records fetched from API: {len(all_weather)}")

    except Exception as e:
        print(f"error fetching weather from API: {str(e)}")
        raise

    # ===== SCHEMA VALIDATION & CASTING (DIRECT TO PROCESSED-DATA) =====
    try:
        print("applying schema and casting columns")
        
        df_weather = spark.createDataFrame(all_weather)
        
        df_final = df_weather.select(
            col("region_id").cast("int").alias("region_id"),
            col("date").cast("date").alias("date"),
            col("temperature_mean_c").cast("double").alias("temperature_mean_c"),
            col("wind_gust_max_kmh").cast("double").alias("wind_gust_max_kmh"),
            col("precipitation_sum_mm").cast("double").alias("precipitation_sum_mm"),
            col("high_temp").cast("int").alias("high_temp"),
            col("high_wind").cast("int").alias("high_wind"),
            col("high_precip").cast("int").alias("high_precip")
        ).repartition(1)
        
        print("schema casting completed")
    except Exception as e:
        print(f"failed to apply schema: {str(e)}")
        raise

    # ===== APPEND DIRECTLY TO processed-data/weather_all =====
    try:
        output_path = f"s3://{BUCKET}/processed-data/weather_all/"
        print(f"appending {len(all_weather)} rows directly to: {output_path}")

        df_final.coalesce(1).write \
            .mode("append") \
            .option("header", "true") \
            .csv(output_path)

        print(f"successfully appended {len(all_weather)} new weather rows to weather_all")
    except Exception as e:
        print(f"error appending weather data: {str(e)}")
        raise

    # ===== REFRESH ATHENA METADATA (WITH WAIT) =====
    try:
        print("running MSCK REPAIR TABLE on weather_all")
        repair_query = "MSCK REPAIR TABLE weather_all"
        response = athena_client.start_query_execution(
            QueryString=repair_query,
            QueryExecutionContext={'Database': 'delays_weather'},
            ResultConfiguration={'OutputLocation': f's3://{BUCKET}/query-results/'}
        )
        query_id = response['QueryExecutionId']
        print(f"MSCK repair query started: {query_id}")
        
        status = wait_for_query_completion(athena_client, query_id)
        print(f"MSCK repair completed with status: {status}")
        
    except Exception as e:
        print(f"error running MSCK repair: {str(e)}")
        raise

    print("weather-update-weekly-ETL completed successfully")

except Exception as e:
    print(f"ETL job failed: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()