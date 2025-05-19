# import requests
# import boto3
# from datetime import datetime
# import json

# # --- Configuration ---
# API_URL = "https://data.cityofnewyork.us/resource/t29m-gskq.json"
# BUCKET_NAME = "nycc-taxi-raw-data"
# S3_FOLDER = "yellow_tripdata"
# TIMESTAMP = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
# S3_KEY = f"{S3_FOLDER}/nyc_zone_data_{TIMESTAMP}.json"

# # --- Step 1: Fetch the data from NYC Open Data API ---
# response = requests.get(API_URL)
# response.raise_for_status()  # raises error if the request fails
# data = response.json()

# # --- Step 2: Upload to S3 ---
# s3 = boto3.client("s3")

# s3.put_object(
#     Bucket=BUCKET_NAME,
#     Key=S3_KEY,
#     Body=json.dumps(data),
#     ContentType="application/json"
# )

# print(f"✅ NYC zone data uploaded to s3://{BUCKET_NAME}/{S3_KEY}")

import boto3
import pandas as pd
from io import StringIO
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_csv_from_s3(bucket_name, csv_key, output_csv_key):
    try:
        logging.info("Starting the CSV processing pipeline.")

        # Initialize S3 client
        s3 = boto3.client('s3')
        logging.info("Connected to S3.")

        # Step 1: Read CSV file from S3
        logging.info(f"Reading CSV file from s3://{bucket_name}/{csv_key}")
        obj = s3.get_object(Bucket=bucket_name, Key=csv_key)
        body = obj['Body'].read().decode('utf-8')

        print("First 500 chars:", body[:500])  # Optional preview

        # Step 2: Convert to DataFrame
        df = pd.read_csv(StringIO(body))
        logging.info(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")

        # Step 3: Optionally modify DataFrame here
        # For example: df = df.dropna() or df = df.head(100)

        # Step 4: Upload cleaned CSV to S3
        logging.info(f"Uploading processed CSV to s3://{bucket_name}/{output_csv_key}")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=output_csv_key, Body=csv_buffer.getvalue())

        logging.info("Processed CSV uploaded successfully.")

    except ClientError as e:
        logging.error(f"ClientError: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

# Replace with your values
bucket_name = 'nycc-taxi-raw-data'
csv_key = 'yellow_tripdata/nyc_zone_data_2025-05-16_17-39-50.json'  # despite the .json, it's CSV
output_csv_key = 'yellow_tripdata/nyc_zone_data_cleaned.csv'

process_csv_from_s3(bucket_name, csv_key, output_csv_key)
