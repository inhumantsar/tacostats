import boto3

from tacostats.config import S3_BUCKET

# Moto automocks boto calls, these funcs help manage mock objects
def create_bucket(): 
    boto3.resource('s3', region_name="us-east-1").create_bucket(Bucket=S3_BUCKET)

def create_obj(key: str): 
    boto3.client('s3', region_name="us-east-1").put_object(Bucket=S3_BUCKET, Key=key)
