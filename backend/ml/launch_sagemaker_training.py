import sagemaker
from sagemaker.sklearn.estimator import SKLearn
import boto3
import os

role = "arn:aws:iam::<your-account-id>:role/service-role/AmazonSageMaker-ExecutionRole-..."  # Update this
bucket = "nycc-taxi-processed-data"
region = "us-east-1"
script_path = "s3://nycc-taxi-processed-data/code/sagemaker_train.py"

sagemaker_session = sagemaker.Session()
sklearn_estimator = SKLearn(
    entry_point=script_path,
    role=role,
    instance_type="ml.m5.large",
    framework_version="1.2-1",
    py_version="py3",
    sagemaker_session=sagemaker_session,
    output_path=f"s3://{bucket}/models/",
    base_job_name="nycc-trip-model",
)

# Input data configuration
train_input = sagemaker.inputs.TrainingInput(
    s3_data=f"s3://{bucket}/cleaned/yellow_tripdata_.csv",
    content_type="text/csv"
)

sklearn_estimator.fit({"train": train_input})
