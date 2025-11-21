import json
import boto3
import logging
import requests
import datetime
import os

#Hurraaayyyyyyy
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3_client = boto3.client('s3')
glue_client = boto3.client('glue')

DATALAKE_BUCKET = os.environ.get('DATALAKE_BUCKET')
BRONZE_PREFIX = os.environ.get('BRONZE_PREFIX', 'bronze/')
BRONZE_TO_SILVER_JOB = os.environ.get('BRONZE_TO_SILVER_JOB', 'sylvester_bronze_to_silver')

API_URL = os.environ.get('API_URL', 'https://www.vaikcam.com/api/loans/')
API_KEY = os.environ.get('API_KEY')   # MUST be provided as environment variable

def lambda_handler(event, context):
    logger.info("Starting ingestion lambda")

    try:
        headers = {'Authorization': f'Api-Key {API_KEY}'}

        # response = requests.get(API_URL, headers=headers, timeout=30)
        response = requests.get(API_URL, headers=headers, timeout=30, verify=False)
        response.raise_for_status()  # raise exception for HTTP errors

        data = response.text

        timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%SZ')
        s3_key = f"{BRONZE_PREFIX}incoming/raw_api_data_{timestamp}.json"

        s3_client.put_object(
            Bucket=DATALAKE_BUCKET,
            Key=s3_key,
            Body=data,
            ContentType="application/json"
        )

        logger.info(f"Uploaded raw data to s3://{DATALAKE_BUCKET}/{s3_key}")

        resp = glue_client.start_job_run(JobName=BRONZE_TO_SILVER_JOB)
        job_run_id = resp.get("JobRunId")

        logger.info(f"Started Glue job {BRONZE_TO_SILVER_JOB} JobRunId={job_run_id}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Ingested and triggered Glue job",
                "job_run_id": job_run_id
            })
        }

    except Exception as e:
        logger.exception("Ingestion failed")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }