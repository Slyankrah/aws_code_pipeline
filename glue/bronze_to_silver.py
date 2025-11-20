import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_BUCKET = args['BRONZE_BUCKET']
SILVER_BUCKET = args['SILVER_BUCKET']

# Read raw data from Bronze bucket (parquet files)
bronze_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [f"s3://{BRONZE_BUCKET}/bronze/"]},
    format="parquet"
)

# Simple clean: filter out rows where a sample column is null
silver_df = Filter.apply(frame=bronze_df, f=lambda x: x["your_column"] is not None)

# Write cleaned data to Silver bucket
glueContext.write_dynamic_frame.from_options(
    frame=silver_df,
    connection_type="s3",
    connection_options={"path": f"s3://{SILVER_BUCKET}/silver/"},
    format="parquet"
)

job.commit()