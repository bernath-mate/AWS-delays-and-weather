import boto3
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

glue = boto3.client('glue', region_name='us-east-1')

def start_glue_job(job_name):
    """Start a Glue job and return job run ID"""
    try:
        response = glue.start_job_run(JobName=job_name)
        job_run_id = response['JobRunId']
        print(f"started {job_name}: {job_run_id}")
        return {'job_name': job_name, 'job_run_id': job_run_id, 'status': 'started'}
    except Exception as e:
        print(f"failed to start {job_name}: {str(e)}")
        return {'job_name': job_name, 'status': 'failed', 'error': str(e)}

def wait_for_job_completion(job_name, job_run_id, max_wait_seconds=3600):
    """Poll job status until completion"""
    import time
    
    start_time = time.time()
    while True:
        try:
            response = glue.get_job_run(JobName=job_name, RunId=job_run_id)
            state = response['JobRun']['JobRunState']
            
            if state in ['SUCCEEDED', 'FAILED', 'STOPPED']:
                print(f"{job_name} ({job_run_id}): {state}")
                return {'job_name': job_name, 'job_run_id': job_run_id, 'status': state}
            
            elapsed = time.time() - start_time
            if elapsed > max_wait_seconds:
                print(f"{job_name} ({job_run_id}): timeout after {max_wait_seconds}s")
                return {'job_name': job_name, 'job_run_id': job_run_id, 'status': 'timeout'}
            
            time.sleep(10)
        
        except Exception as e:
            print(f"error polling {job_name}: {str(e)}")
            return {'job_name': job_name, 'job_run_id': job_run_id, 'status': 'error', 'error': str(e)}

def lambda_handler(event, context):
    """
    Run parallel ETL jobs:
    1. delays-update-weekly-ETL (delays ETL)
    2. weather-update-weekly-ETL (weather fetch + CSV upload)
    Then sequentially run:
    3. delays-update-weekly-ETL (unified view)
    """
    
    try:
        print("starting etl orchestration")
        
        # ===== PHASE 1: PARALLEL JOBS =====
        print("\n=== phase 1: starting parallel jobs ===")
        
        parallel_jobs = [
            'weather-update-weekly-ETL',
            'delays-update-weekly-ETL'
        ]
        
        parallel_results = {}
        
        # Start parallel jobs
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {executor.submit(start_glue_job, job): job for job in parallel_jobs}
            
            for future in as_completed(futures):
                job_name = futures[future]
                result = future.result()
                parallel_results[job_name] = result
        
        print(f"parallel jobs started: {json.dumps(parallel_results, indent=2)}")
        
        # Wait for parallel jobs to complete
        print("\n=== phase 1: waiting for parallel jobs to complete ===")
        
        parallel_status = {}
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                executor.submit(
                    wait_for_job_completion, 
                    result['job_name'], 
                    result['job_run_id']
                ): result['job_name'] 
                for result in parallel_results.values() 
                if result['status'] == 'started'
            }
            
            for future in as_completed(futures):
                job_name = futures[future]
                result = future.result()
                parallel_status[job_name] = result
        
        print(f"parallel jobs completed: {json.dumps(parallel_status, indent=2)}")
        
        # Check if all parallel jobs succeeded
        failed_jobs = [job for job, result in parallel_status.items() if result['status'] != 'SUCCEEDED']
        if failed_jobs:
            print(f"warning: some parallel jobs failed: {failed_jobs}")
        
        # ===== PHASE 2: SEQUENTIAL JOB =====
        print("\n=== phase 2: starting sequential job ===")
        
        sequential_job = 'unified_all-weekly-update-ETL'
        seq_result = start_glue_job(sequential_job)
        
        if seq_result['status'] != 'started':
            raise Exception(f"failed to start sequential job: {seq_result.get('error')}")
        
        # Wait for sequential job to complete
        print("\n=== phase 2: waiting for sequential job to complete ===")
        
        seq_completion = wait_for_job_completion(
            seq_result['job_name'], 
            seq_result['job_run_id']
        )
        
        print(f"sequential job completed: {json.dumps(seq_completion, indent=2)}")
        
        # ===== FINAL SUMMARY =====
        print("\n=== ETL orchestration complete ===")
        
        summary = {
            'phase_1_parallel': parallel_status,
            'phase_2_sequential': seq_completion,
            'all_succeeded': (
                all(r['status'] == 'SUCCEEDED' for r in parallel_status.values()) and
                seq_completion['status'] == 'SUCCEEDED'
            )
        }
        
        print(f"summary: {json.dumps(summary, indent=2)}")
        
        return {
            'statusCode': 200,
            'body': json.dumps(summary)
        }
    
    except Exception as e:
        print(f"fatal error in orchestration: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
