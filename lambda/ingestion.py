import json
import boto3
import logging
import urllib.request
import datetime
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

DATALAKE_BUCKET = os.environ.get('DATALAKE_BUCKET')
BRONZE_PREFIX = os.environ.get('BRONZE_PREFIX')
BRONZE_TO_SILVER_JOB = os.environ.get('BRONZE_TO_SILVER_JOB', 'bronze_to_silver')

def lambda_handler(event, context):
    logger.info("Starting ingestion lambda")

    api_url = "http://www.vaikcam.com/api/loans/"  # Replace with your actual API

    try:
        with urllib.request.urlopen(api_url) as response:
            data = response.read().decode('utf-8')

        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')
        s3_key = f"{BRONZE_PREFIX}incoming/raw_api_data_{timestamp}.json"

        s3_client.put_object(
            Bucket=DATALAKE_BUCKET,
            Key=s3_key,
            Body=data,
            ContentType='application/json'
        )
        logger.info(f"Uploaded raw data to s3://{DATALAKE_BUCKET}/{s3_key}")

        # # Trigger Glue job bronze_to_silver
        # response = glue_client.start_job_run(JobName=BRONZE_TO_SILVER_JOB)
        # job_run_id = response['JobRunId']
        # logger.info(f"Started Glue job {BRONZE_TO_SILVER_JOB} with JobRunId: {job_run_id}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully ingested data and triggered Glue job {BRONZE_TO_SILVER_JOB}')
        }

    except Exception as e:
        logger.error(f"Ingestion error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to ingest data')
        }