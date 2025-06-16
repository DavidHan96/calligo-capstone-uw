import pandas as pd
import boto3
import io

aws_access_key_id = 'YOUR_KEY_ID'
aws_secret_access_key = 'YOUR_ACCESS_KEY'
endpoint_url = 'YOUR_ENDPOINT'

s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    endpoint_url=endpoint_url
)

def load_data_from_s3(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    df = pd.read_csv(io.BytesIO(response['Body'].read()))    
    return df
