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
import json
from io import StringIO
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def convert_json_to_csv(bucket_name, json_key, csv_key):
    try:
        logging.info("Starting the JSON to CSV conversion process.")

        # Initialize S3 client
        s3 = boto3.client('s3')
        logging.info("Connected to S3.")

        # Step 1: Read JSON file from S3
        logging.info(f"Reading JSON file from s3://{bucket_name}/{json_key}")
        obj = s3.get_object(Bucket=bucket_name, Key=json_key)
        json_data = json.load(obj['Body'])

        # Step 2: Convert to DataFrame
        logging.info("Converting JSON to DataFrame.")
        df = pd.DataFrame(json_data)
        logging.info(f"DataFrame created with {len(df)} rows and {len(df.columns)} columns.")

        # Step 3: Convert DataFrame to CSV
        logging.info("Converting DataFrame to CSV format.")
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Step 4: Upload CSV to S3
        logging.info(f"Uploading CSV to s3://{bucket_name}/{csv_key}")
        s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())

        logging.info("CSV file uploaded successfully.")

    except ClientError as e:
        logging.error(f"ClientError: {e}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")

# Replace these with your actual paths
bucket_name = 'nycc-taxi-raw-data'
json_key = 'yellow_tripdata/nyc_zone_data_2025-05-15_19-03-23.json'  # <-- update this
csv_key = 'yellow_tripdata/converted_nyc_zone_2025-05-15_19-03-23.csv'

# Run the function
convert_json_to_csv(bucket_name, json_key, csv_key)
