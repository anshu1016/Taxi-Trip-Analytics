import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.job import Job

# Glue setup
args = getResolvedOptions(sys.argv, ['NYC_ETL'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['NYC_ETL'], args)

# Step 1: Read raw CSV from S3
raw_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://nyc-taxi-raw-data/yellow_tripdata/"], "recurse": True},
    format_options={"withHeader": True}
)

# Step 2: Clean + Transform
# Convert to Spark DataFrame to use spark functions
df = raw_df.toDF()

# Cast datetime fields & remove nulls
from pyspark.sql.functions import col, to_timestamp

df_cleaned = df.withColumn("tpep_pickup_datetime", to_timestamp("tpep_pickup_datetime")) \
               .withColumn("tpep_dropoff_datetime", to_timestamp("tpep_dropoff_datetime")) \
               .dropna(subset=["passenger_count", "trip_distance", "fare_amount", "tpep_pickup_datetime"])

# Step 3: Convert back to DynamicFrame
final_dyf = DynamicFrame.fromDF(df_cleaned, glueContext, "final_dyf")

# Step 4: Write to S3 in Parquet format (partitioned by year/month)
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    connection_options={
        "path": "s3://nyc-taxi-processed-data/yellow_tripdata_parquet/",
        "partitionKeys": ["vendorid"]
    },
    format="parquet"
)

job.commit()


# [REMOVED_SECRET]