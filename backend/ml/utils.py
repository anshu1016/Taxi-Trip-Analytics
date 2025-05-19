import boto3
import pandas as pd
import joblib
from io import BytesIO
import logging
from pyathena import connect


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)



from pyathena import connect
import pandas as pd

# def load_data_from_athena():
#     conn = connect(s3_staging_dir='s3://nycc-taxi-processed-data/athena-staging/', region_name='us-east-1')
#     query = "SELECT passenger_count, trip_distance, fare_amount, pickup_datetime, dropoff_datetime FROM `nyc-tax-db-glue-output_16`.cleaned_trips"
#     df = pd.read_sql(query, conn)
#     return df



def download_data_from_s3(bucket_name, key):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket_name, Key=key)
    return pd.read_csv(obj['Body'])

def upload_model_to_s3(model, bucket_name, key):
    buffer = BytesIO()
    joblib.dump(model, buffer)
    buffer.seek(0)
    
    s3 = boto3.client('s3')
    s3.upload_fileobj(buffer, Bucket=bucket_name, Key=key)
    logger.info(f"✅ Model uploaded to s3://{bucket_name}/{key}")
