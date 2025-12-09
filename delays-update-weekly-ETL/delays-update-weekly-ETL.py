import sys
import re
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
import boto3

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

s3 = boto3.client('s3')
athena_client = boto3.client('athena', region_name='us-east-1')

BUCKET = 'delays-weather-slucrx'

try:
    print("starting delays-update-weekly-ETL (Option C: Only append new data)")
    
    # ===== EXTRACT WEEK IDENTIFIER FROM LATEST DELAY CSV =====
    try:
        print("scanning for latest delays CSV filename in weekly-updates")
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
    
    # ===== READ ONLY THE NEW WEEK'S DELAY CSV =====
    try:
        print(f"reading ONLY new week's delay CSV: delay_data_{WEEK_IDENTIFIER}.csv")
        
        weekly_csv_path = f"s3://{BUCKET}/raw-data/delays/weekly-updates/delay_data_{WEEK_IDENTIFIER}.csv"
        
        df_new_week = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(weekly_csv_path)
        
        new_week_count = df_new_week.count()
        print(f"loaded new week's delays: {new_week_count} rows")
        
        if new_week_count == 0:
            raise Exception(f"no data found in: {weekly_csv_path}")
    
    except Exception as e:
        print(f"error reading new week's delays: {str(e)}")
        raise
    
    # ===== SCHEMA VALIDATION & CASTING =====
    try:
        print("applying schema and casting columns")
        df_final = df_new_week.select(
            col("date").cast("date").alias("date"),
            col("station_name").cast("string").alias("station_name"),
            col("total_delay_minutes").cast("double").alias("total_delay_minutes"),
            col("train_count").cast("int").alias("train_count"),
            col("avg_delay_minutes").cast("double").alias("avg_delay_minutes")
        ).repartition(1)
        print("schema casting completed")
    except Exception as e:
        print(f"failed to apply schema: {str(e)}")
        raise
    
    # ===== APPEND NEW WEEK TO processed-data =====
    try:
        output_path = f"s3://{BUCKET}/processed-data/delays_all/"
        print(f"appending new week ({new_week_count} rows) to: {output_path}")
        
        df_final.coalesce(1).write \
            .mode("append") \
            .option("header", "true") \
            .csv(output_path)
        
        print(f"successfully appended {new_week_count} new delay rows to delays_all")
    except Exception as e:
        print(f"failed to append delay data: {str(e)}")
        raise
    
    # ===== REFRESH ATHENA METADATA =====
    try:
        print("running MSCK REPAIR TABLE on delays_all")
        repair_query = "MSCK REPAIR TABLE delays_all"
        response = athena_client.start_query_execution(
            QueryString=repair_query,
            QueryExecutionContext={'Database': 'delays_weather'},
            ResultConfiguration={'OutputLocation': f's3://{BUCKET}/query-results/'}
        )
        query_id = response['QueryExecutionId']
        print(f"MSCK repair started: {query_id}")
    except Exception as e:
        print(f"warning: failed to run MSCK repair: {str(e)}")
    
    # ===== COMMIT JOB =====
    try:
        print("committing glue job")
        job.commit()
        print("delays-update-weekly-ETL completed successfully")
    except Exception as e:
        print(f"failed to commit job: {str(e)}")
        raise

except Exception as e:
    print(f"ETL job failed: {str(e)}")
    import traceback
    traceback.print_exc()
    job.commit()
    sys.exit(1)