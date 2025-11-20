import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Read cleaned data from Silver bucket
silver_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [f"s3://{args['SILVER_BUCKET']}/silver/"]},
    format="parquet"
)

# Aggregate example: group by a column and count
spark_df = silver_df.toDF()
gold_df = spark_df.groupBy("your_group_column").count()

gold_dynamic_df = DynamicFrame.fromDF(gold_df, glueContext, "gold_dynamic_df")

# Write aggregated data to Gold bucket
glueContext.write_dynamic_frame.from_options(
    frame=gold_dynamic_df,
    connection_type="s3",
    connection_options={"path": f"s3://{args['GOLD_BUCKET']}/gold/"},
    format="parquet"
)

job.commit()