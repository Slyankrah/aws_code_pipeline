import sys
from awsglue.transforms import Filter
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

#Hurraaayyyyyyy
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_BUCKET = args['BRONZE_BUCKET']
SILVER_BUCKET = args['SILVER_BUCKET']

bronze_path = f"s3://{BRONZE_BUCKET}/bronze/"
silver_path = f"s3://{SILVER_BUCKET}/silver/"

bronze_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [bronze_path]},
    format="json"
)

# Example filter - adjust 'your_column' to a real column in your data to filter out nulls
filtered_df = Filter.apply(frame=bronze_df, f=lambda x: x.get("loan_id") is not None)

glueContext.write_dynamic_frame.from_options(
    frame=filtered_df,
    connection_type="s3",
    connection_options={"path": silver_path},
    format="parquet"
)

job.commit()