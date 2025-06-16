import boto3
import pandas as pd
import io

# AWS S3 Credentials
aws_access_key_id = 'YOUR_KEY_ID'
aws_secret_access_key = 'YOUR_ACCESS_KEY'
endpoint_url = 'YOUR_ENDPOINT'
bucket_name = 'fred'

# Function to Fetch CSV from S3 
def fetch_csv_from_s3(key):
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        endpoint_url=endpoint_url
    )
    response = s3.get_object(Bucket=bucket_name, Key=key)
    return pd.read_csv(io.BytesIO(response['Body'].read()))
