import json
import boto3
import logging
import os

logger = logging.getLogger()
logger.setLevel(logging.INFO)

glue_client = boto3.client('glue')
SILVER_TO_GOLD_JOB = os.environ.get('SILVER_TO_GOLD_JOB', 'sylvester_silver_to_gold')

def lambda_handler(event, context):
    logger.info("Event received: %s", json.dumps(event))

    try:
        resp = glue_client.start_job_run(JobName=SILVER_TO_GOLD_JOB)
        job_run_id = resp.get('JobRunId')
        logger.info(f"Started Glue job {SILVER_TO_GOLD_JOB} JobRunId={job_run_id}")
        return {'statusCode': 200, 'body': json.dumps({'job_run_id': job_run_id})}
    except Exception as e:
        logger.exception("Failed to start silver_to_gold job")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}