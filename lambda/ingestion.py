import json
import boto3
import logging
import urllib.request
import datetime
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
BRONZE_BUCKET = os.environ.get('BRONZE_BUCKET')

def lambda_handler(event, context):
    logger.info("Starting ingestion lambda")

    api_url = "https://api.publicapis.org/entries"  # Replace with your actual API

    try:
        with urllib.request.urlopen(api_url) as response:
            data = response.read().decode('utf-8')

        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')
        s3_key = f"bronze/incoming/raw_api_data_{timestamp}.json"

        s3_client.put_object(
            Bucket=BRONZE_BUCKET,
            Key=s3_key,
            Body=data,
            ContentType='application/json'
        )
        logger.info(f"Uploaded raw data to s3://{BRONZE_BUCKET}/{s3_key}")

        # Optionally trigger Glue job (uncomment if desired)
        # glue_client = boto3.client('glue')
        # glue_response = glue_client.start_job_run(JobName='bronze_to_silver')
        # logger.info(f"Started Glue job bronze_to_silver: {glue_response['JobRunId']}")

        return {
            'statusCode': 200,
            'body': json.dumps(f'Successfully ingested data to s3://{BRONZE_BUCKET}/{s3_key}')
        }

    except Exception as e:
        logger.error(f"Ingestion error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to ingest data')
        }