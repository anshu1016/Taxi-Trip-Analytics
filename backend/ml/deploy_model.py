import sagemaker
from sagemaker.sklearn.model import SKLearnModel
from sagemaker.session import Session
import boto3

boto_session = boto3.Session(region_name="us-east-1")
sagemaker_session = Session(boto_session=boto_session)

role = "arn:aws:iam::622025528920:role/service-role/AWSGlueServiceRole-NYCTaxi"  # ← Replace this with your SageMaker IAM Role ARN

model = SKLearnModel(
    model_data="s3://nycc-taxi-processed-data/deploy/model.tar.gz",  # ← tar.gz now
    role=role,
    entry_point="inference.py",
    framework_version="0.23-1",
    py_version="py3",
    sagemaker_session=sagemaker.Session()
)
from datetime import datetime

endpoint_name = f"yellow-trip-endpoint-{datetime.now().strftime('%Y%m%d%H%M%S')}"
predictor = model.deploy(
    initial_instance_count=1,
    instance_type="ml.m5.large",
    endpoint_name=endpoint_name
)
