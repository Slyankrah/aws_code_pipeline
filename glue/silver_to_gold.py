import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'SILVER_BUCKET', 'GOLD_BUCKET'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

SILVER_BUCKET = args['SILVER_BUCKET']
GOLD_BUCKET = args['GOLD_BUCKET']

silver_path = f"s3://{SILVER_BUCKET}/silver/"
gold_path = f"s3://{GOLD_BUCKET}/gold/"

silver_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [silver_path]},
    format="parquet"
)

spark_df = silver_df.toDF()

# Replace 'your_group_column' with a real column for aggregation
gold_df = spark_df.groupBy("your_group_column").count()

gold_dynamic_df = DynamicFrame.fromDF(gold_df, glueContext, "gold_dynamic_df")

glueContext.write_dynamic_frame.from_options(
    frame=gold_dynamic_df,
    connection_type="s3",
    connection_options={"path": gold_path},
    format="parquet"
)

job.commit()