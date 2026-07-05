import os
from dotenv import load_dotenv
load_dotenv()
import boto3


def getBoto3Session():
    boto3session = boto3.Session(
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('AWS_SECRET_KEY'),
        region_name="us-east-1",
        aws_session_token=os.getenv('AWS_SESSION_TOKEN')

    )
    s3 = boto3session.client('s3')

    return s3
# getBoto3Session