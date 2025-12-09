import sys
import boto3
import time
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

athena_client = boto3.client('athena', region_name='us-east-1')

try:
    print("starting unified_all view refresh ETL job")
    
    # Create or replace unified_all view
    try:
        print("refreshing unified_all view")
        
        view_query = """
        CREATE OR REPLACE VIEW unified_all AS
        SELECT 
            d.date,
            d.station_name,
            sr.region,
            sr.county,
            d.total_delay_minutes,
            d.train_count,
            d.avg_delay_minutes,
            w.temperature_mean_c,
            w.wind_gust_max_kmh,
            w.precipitation_sum_mm,
            w.extreme_temperature,
            w.extreme_wind,
            w.extreme_precipitation
        FROM delays_all d
        LEFT JOIN stations_regions sr 
            ON d.station_name = sr.station_name
        LEFT JOIN weather_all w 
            ON d.date = w.date 
            AND sr.region = w.station_name
        ORDER BY d.date, d.station_name;
        """
        
        response = athena_client.start_query_execution(
            QueryString=view_query,
            QueryExecutionContext={'Database': 'delays_weather'},
            ResultConfiguration={'OutputLocation': 's3://delays-weather-slucrx/query-results/'}
        )
        
        query_id = response['QueryExecutionId']
        print(f"view refresh query started: {query_id}")
        
        # Poll for completion
        max_attempts = 60
        for attempt in range(max_attempts):
            query_status = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = query_status['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                print(f"view refresh query status: {status}")
                if status == 'FAILED':
                    reason = query_status['QueryExecution']['Status']['StateChangeReason']
                    raise Exception(f"query failed: {reason}")
                break
            
            time.sleep(2)
        
        if status != 'SUCCEEDED':
            raise Exception(f"query did not complete: {status}")
        
        print("unified_all view successfully refreshed")
    
    except Exception as e:
        print(f"failed to refresh unified_all view: {str(e)}")
        raise
    
    # Commit job
    try:
        print("committing glue job")
        job.commit()
        print("etl job completed successfully")
    except Exception as e:
        print(f"failed to commit job: {str(e)}")
        raise

except Exception as e:
    print(f"ETL job failed with error: {str(e)}")
    job.commit()
    sys.exit(1)