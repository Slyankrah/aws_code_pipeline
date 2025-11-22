import sys, boto3
from awsglue.transforms import Filter
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME', 'BRONZE_BUCKET', 'SILVER_BUCKET'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

BRONZE_BUCKET = args['BRONZE_BUCKET']
SILVER_BUCKET = args['SILVER_BUCKET']

bronze_incoming = f"s3://{BRONZE_BUCKET}/bronze/incoming/"
bronze_processed = f"s3://{BRONZE_BUCKET}/bronze/processed/"
silver_path = f"s3://{SILVER_BUCKET}/silver/"

# Load only *.json files
bronze_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={
        "paths": [bronze_incoming],
        "recurse": True,
        "groupFiles": "inPartition",
        "groupSize": "1048576",
        "excludePattern": ".*_SUCCESS$"
    },
    format="json"
)

# ----------------------------
# STOP IF NO COLUMNS
# ----------------------------
if len(bronze_df.schema().fields) == 0:
    print("❌ No columns found in incoming JSON — skipping processing")
else:
    # Apply filter safely
    filtered_df = Filter.apply(
        frame=bronze_df,
        f=lambda x: x.get("loan_id") not in (None, "")
    )

    spark_df = filtered_df.toDF()

    if spark_df.count() == 0:
        print("⚠ No valid rows after filtering — skipping write.")
    else:
        spark_df.write.mode("overwrite").option("header", "true").csv(silver_path)

# --------------------------------------------
# MOVE RAW FILES incoming → processed
# --------------------------------------------
s3 = boto3.client("s3")
response = s3.list_objects_v2(
    Bucket=BRONZE_BUCKET,
    Prefix="bronze/incoming/"
)

if "Contents" in response:
    for obj in response["Contents"]:
        key = obj["Key"]

        if key.endswith("/"):
            continue

        new_key = key.replace("incoming/", "processed/", 1)

        s3.copy_object(
            Bucket=BRONZE_BUCKET,
            CopySource={"Bucket": BRONZE_BUCKET, "Key": key},
            Key=new_key
        )

        s3.delete_object(
            Bucket=BRONZE_BUCKET,
            Key=key
        )

job.commit()