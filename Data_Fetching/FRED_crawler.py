import os
import requests
import time
import sys
import pandas as pd
import io
import boto3
import botocore
from datetime import datetime, timedelta

# Constants
API_KEY = "YOUR_API_KEY"
BASE_URL = "https://api.stlouisfed.org/fred"
SERIES_ENDPOINT = f"{BASE_URL}/category/series"
CHILDREN_ENDPOINT = f"{BASE_URL}/category/children"
CHECKPOINT_KEY = 'metadata/metadata.csv'
SERIES_LIMIT = 1000000

# AWS S3 Configuration from Environment Variables
aws_access_key_id = 'YOUR_KEY_ID'
aws_secret_access_key = 'YOUR_ACCESS_KEY'
endpoint_url = 'YOUR_ENDPOINT'
bucket_name = 'fred' 

if not all([aws_access_key_id, aws_secret_access_key, endpoint_url]):
    print("[Fatal Error] Missing AWS credentials or S3 endpoint in environment variables.")
    sys.exit(1)

s3 = boto3.resource(
    's3',
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key,
    endpoint_url=endpoint_url
)
bucket = s3.Bucket(bucket_name)

visited_categories = set()
seen_series_ids = set()
total_series = 0

# Load metadata from S3
try:
    obj = bucket.Object(CHECKPOINT_KEY)
    metadata_content = obj.get()['Body'].read().decode('utf-8')
    metadata_df = pd.read_csv(io.StringIO(metadata_content))
    metadata_df['last_updated'] = pd.to_datetime(metadata_df['last_updated'], errors='coerce')
    metadata_dict = metadata_df.set_index('id').to_dict('index')
    existing_ids = set(metadata_dict.keys())
    print("[Info] Loaded metadata from S3.")
except botocore.exceptions.ClientError as e:
    if e.response['Error']['Code'] == "NoSuchKey":
        print("[Info] No metadata.csv found in S3. Initializing.")
        metadata_df = pd.DataFrame(columns=['id','title','observation_start','observation_end','frequency','units','seasonal_adjustment','last_updated','notes'])
        metadata_dict = {}
        existing_ids = set()
    else:
        raise

def safe_put_object(key, content_bytes):
    for attempt in range(3):
        try:
            bucket.put_object(Key=key, Body=content_bytes, ContentLength=len(content_bytes))
            print(f"[S3] Successfully uploaded {key}.")
            return
        except Exception as e:
            print(f"[S3 ERROR] Attempt {attempt+1} failed to upload {key}: {e}")
            time.sleep(2)
    print(f"[S3 ERROR] Failed to upload {key} after retries.")

def safe_get(url, params, retries=5, backoff=5):
    for attempt in range(retries):
        try:
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"[ERROR] Attempt {attempt + 1} failed: {e}", flush=True)
            time.sleep(backoff * (attempt + 1))
    return None

def needs_refresh(meta):
    if pd.isna(meta.get('last_updated')):
        return True
    freq = meta.get('frequency', '')
    age = datetime.now(meta['last_updated'].tzinfo) - meta['last_updated']
    if "Daily" in freq: return timedelta(days=3) >= age >= timedelta(days=1)
    if "Weekly" in freq: return timedelta(days=14) >= age >= timedelta(days=7)
    if "Monthly" in freq: return timedelta(days=60) >= age >= timedelta(days=30)
    if "Quarterly" in freq: return timedelta(days=180) >= age >= timedelta(days=90)
    if "Annual" in freq: return timedelta(days=730) >= age >= timedelta(days=365)
    return False

def update_metadata_s3(new_row_df):
    global metadata_df
    metadata_df = pd.concat([metadata_df, new_row_df], ignore_index=True).drop_duplicates(subset='id', keep='last')
    buffer = io.StringIO()
    metadata_df.to_csv(buffer, index=False)
    content = buffer.getvalue().encode('utf-8')
    safe_put_object(CHECKPOINT_KEY, content)

def process_category(category_id, level=0):
    global total_series
    if category_id in visited_categories:
        return
    visited_categories.add(category_id)

    print(f"\n[Category] {'  '*level}Processing category {category_id}")
    series_data = safe_get(SERIES_ENDPOINT, {"api_key": API_KEY, "file_type": "json", "category_id": category_id, "limit": 1000})

    if series_data:
        for series in series_data.get("seriess", []):
            sid = series["id"]
            title = series["title"]
            if sid in seen_series_ids:
                continue
            seen_series_ids.add(sid)
            total_series += 1
            if total_series >= SERIES_LIMIT:
                print("[STOP] Reached series limit.")
                sys.exit(0)

            if sid not in existing_ids:
                row = {
                    'id': sid, 'title': title, 'observation_start': series['observation_start'],
                    'observation_end': series['observation_end'], 'frequency': series['frequency'],
                    'units': series['units'], 'seasonal_adjustment': series['seasonal_adjustment'],
                    'last_updated': series['last_updated'], 'notes': series.get('notes','').replace("\n"," ").replace(",",";")
                }
                update_metadata_s3(pd.DataFrame([row]))
                existing_ids.add(sid)
                metadata_dict[sid] = row
                fetch_obs = True
            else:
                meta = metadata_dict[sid]
                if meta.get('title', '').strip().upper().endswith("(DISCONTINUED)"):
                    print(f"[Skip] {sid} is DISCONTINUED.")
                    fetch_obs = False
                else:
                    fetch_obs = needs_refresh(meta)

            if fetch_obs:
                obs_data = safe_get(f"{BASE_URL}/series/observations", {"api_key": API_KEY, "file_type": "json", "series_id": sid})
                if obs_data and obs_data.get("observations"):
                    obs_rows = [{'series_id': sid, 'date': obs['date'], 'value': float(obs['value']) if obs['value'] not in ("",".") else None} for obs in obs_data['observations']]
                    if obs_rows:
                        buffer = io.StringIO()
                        pd.DataFrame(obs_rows).to_csv(buffer, index=False)
                        safe_put_object(f'observations/{sid}.csv', buffer.getvalue().encode('utf-8'))
                        print(f"[Saved] Observations for {sid}.")
                else:
                    print(f"[Obs] No observations for {sid}.")
            else:
                print(f"[Obs] Skipped {sid}, up-to-date.")

            if total_series % 250 == 0:
                print(f"\n[Checkpoint] Processed {total_series} series.")

    children_data = safe_get(CHILDREN_ENDPOINT, {"api_key": API_KEY, "file_type": "json", "category_id": category_id})
    if children_data:
        for child in children_data.get("categories", []):
            process_category(child["id"], level+1)

# Start crawling
try:
    print("[Start] Crawling categories from root 0")
    process_category(0)
    print(f"[Done] Total series processed: {total_series}")
except Exception as e:
    print(f"[Fatal Error] {e}")
