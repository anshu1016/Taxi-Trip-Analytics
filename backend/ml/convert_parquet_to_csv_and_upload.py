import os
import boto3
import pandas as pd
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import pyarrow.fs

aws_region = "us-east-1"
bucket_name = "nycc-taxi-processed-data"
key_prefix = "yellow_tripdata_parquet/"
def download_and_convert_parquet_to_csv(
    input_s3_uri: str,
    output_local_csv: str,
    output_s3_uri: str,
    aws_region="us-east-1"
):
    filesystem = pyarrow.fs.S3FileSystem(region=aws_region)

    # Parse S3 bucket and prefix
    if input_s3_uri.startswith("s3://"):
        input_s3_uri = input_s3_uri[5:]
    bucket, key_prefix = input_s3_uri.split("/", 1)

    print(f"🔄 Scanning S3 Parquet dataset from: s3://{bucket}/{key_prefix}")
    # 1️⃣ Setup the S3 file system
    s3 = pyarrow.fs.S3FileSystem(region=aws_region)

    # 2️⃣ Combine the bucket and prefix as a `SubTreeFileSystem`
    dataset_path = pyarrow.fs.FileSelector(key_prefix, recursive=True)

    # 3️⃣ Wrap the bucket with SubTreeFileSystem so PyArrow knows to look inside the bucket
    subtree_fs = pyarrow.fs.SubTreeFileSystem(bucket_name, s3)

    # 4️⃣ Load Parquet dataset from S3
    dataset = ds.dataset(
        key_prefix,
        format="parquet",
        partitioning="hive",
        filesystem=subtree_fs
    )

    

    print(f"📚 Reading partitioned Parquet dataset...")
    table = dataset.to_table()
    df = table.to_pandas()

    print(f"✅ Loaded {len(df)} rows with {df.shape[1]} columns")

    # Save locally
    print(f"💾 Writing CSV locally to: {output_local_csv}")
    df.to_csv(output_local_csv, index=False)

    # Upload back to S3
    print(f"☁️ Uploading to S3: {output_s3_uri}")
    s3 = boto3.client("s3", region_name=aws_region)

    out_bucket, out_key = output_s3_uri.replace("s3://", "").split("/", 1)
    s3.upload_file(output_local_csv, out_bucket, out_key)

    print("🎉 Done. File uploaded successfully.")


# ================================
# ✅ RUN SCRIPT
# ================================
if __name__ == "__main__":
    download_and_convert_parquet_to_csv(
        input_s3_uri="s3://nycc-taxi-processed-data/yellow_tripdata_parquet/",
        output_local_csv="yellow_tripdata_combined.csv",
        output_s3_uri="s3://nycc-taxi-processed-data/cleaned/yellow_tripdata_combined.csv"
    )
