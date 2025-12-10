import sys
import time
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
    print("starting delays-update-weekly-ETL")
    
    # ===== EXTRACT WEEK IDENTIFIER FROM LATEST DELAY CSV =====
    try:
        print("scanning for latest delays CSV filename in weekly-updates")
        response = s3.list_objects_v2(
            Bucket=BUCKET,
            Prefix='raw-data/delays/'
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
    
    # ===== READ ONLY THE NEW WEEK'S DELAY CSV =====
    try:
        print(f"reading ONLY new week's delay CSV: delay_data_{WEEK_IDENTIFIER}.csv")
        
        weekly_csv_path = f"s3://{BUCKET}/raw-data/delays/delay_data_{WEEK_IDENTIFIER}.csv"
        
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
            col("station_id").cast("int").alias("station_id"),
            col("total_delay_minutes").cast("double").alias("total_delay_minutes"),
            col("train_count").cast("int").alias("train_count"),
            col("avg_delay_minutes").cast("double").alias("avg_delay_minutes")
        ).repartition(1)
        print("schema casting completed")
    except Exception as e:
        print(f"failed to apply schema: {str(e)}")
        raise
    
    # ===== WRITE TO TEMP STAGING PATH =====
    try:
        temp_s3_path = f"s3://{BUCKET}/temp/delays_staging_{WEEK_IDENTIFIER}/"
        print(f"writing processed delays to temp S3 path: {temp_s3_path}")
        
        df_final.coalesce(1).write \
            .mode("overwrite") \
            .option("header", "true") \
            .csv(temp_s3_path)
        
        print(f"successfully wrote {new_week_count} rows to temp path")
    except Exception as e:
        print(f"error writing to temp path: {str(e)}")
        raise
    
    # ===== INSERT INTO ATHENA TABLE (DIRECT METHOD) =====
    try:
        print("inserting delay data into Athena table")
        
        # Step 1: Create temporary external table pointing to staged data
        temp_table_name = f"delays_staged_{WEEK_IDENTIFIER.replace('-', '_')}"
        
        create_temp_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {temp_table_name} (
            date DATE,
            station_name STRING,
            station_id INT,
            total_delay_minutes DOUBLE,
            train_count INT,
            avg_delay_minutes DOUBLE
        )
        ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
        WITH SERDEPROPERTIES ('field.delim' = ',')
        STORED AS TEXTFILE
        LOCATION 's3://{BUCKET}/temp/delays_staging_{WEEK_IDENTIFIER}/'
        TBLPROPERTIES ('skip.header.line.count' = '1')
        """
        
        print(f"creating temp table: {temp_table_name}")
        response = athena_client.start_query_execution(
            QueryString=create_temp_query,
            QueryExecutionContext={'Database': 'delays_weather'},
            ResultConfiguration={'OutputLocation': f's3://{BUCKET}/query-results/'}
        )
        query_id = response['QueryExecutionId']
        status = wait_for_query_completion(athena_client, query_id)
        print(f"temp table created with status: {status}")
        
        # Step 2: Insert from temp table into delays_all
        insert_query = f"""
        INSERT INTO delays_all
        SELECT 
            date,
            station_id,
            station_name,
            total_delay_minutes,
            train_count,
            avg_delay_minutes
        FROM {temp_table_name}
        """
        
        print(f"inserting from {temp_table_name} into delays_all")
        response = athena_client.start_query_execution(
            QueryString=insert_query,
            QueryExecutionContext={'Database': 'delays_weather'},
            ResultConfiguration={'OutputLocation': f's3://{BUCKET}/query-results/'}
        )
        query_id = response['QueryExecutionId']
        print(f"INSERT query started: {query_id}")
        
        status = wait_for_query_completion(athena_client, query_id)
        
        if status == 'SUCCEEDED':
            print(f"INSERT completed successfully - delay data is now in Athena")
        else:
            print(f"warning: INSERT query returned status: {status}")
        
        # Step 3: Drop temporary table
        drop_temp_query = f"DROP TABLE IF EXISTS {temp_table_name}"
        response = athena_client.start_query_execution(
            QueryString=drop_temp_query,
            QueryExecutionContext={'Database': 'delays_weather'},
            ResultConfiguration={'OutputLocation': f's3://{BUCKET}/query-results/'}
        )
        query_id = response['QueryExecutionId']
        wait_for_query_completion(athena_client, query_id)
        print(f"temp table dropped")
        
    except Exception as e:
        print(f"error with INSERT into delays_all: {str(e)}")
        raise
    
    print("delays-update-weekly-ETL completed successfully")

except Exception as e:
    print(f"ETL job failed: {str(e)}")
    import traceback
    traceback.print_exc()
    raise

finally:
    job.commit()