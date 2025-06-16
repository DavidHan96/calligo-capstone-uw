import requests
import time
import sys
from sqlalchemy import create_engine, Table, Column, String, MetaData, Float
from sqlalchemy.dialects.postgresql import insert as pg_insert 
import pandas as pd


API_KEY = "YOUR_API_KEY"
BASE_URL = "https://api.stlouisfed.org/fred"
SERIES_ENDPOINT = f"{BASE_URL}/category/series"
CHILDREN_ENDPOINT = f"{BASE_URL}/category/children"

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

visited_categories = set()
seen_series_ids = set()
total_series = 0
SERIES_LIMIT = 1000000  # limit for testing delete when done

# Setup database connection and table
engine = create_engine("postgresql+psycopg2://fred_user:fred_pass@db:5432/fred_data")
metadata = MetaData()

fred_series = Table('fred_series', metadata,
    Column('id', String, primary_key=True),
    Column('title', String),
    Column('observation_start', String),
    Column('observation_end', String),
    Column('frequency', String),
    Column('units', String),
    Column('seasonal_adjustment', String),
    Column('last_updated', String),
    Column('notes', String),
)
fred_observations = Table('fred_observations', metadata,
    Column('series_id', String),
    Column('date', String),
    Column('value', Float),
)
metadata.create_all(engine)

# Helper to insert a series (skip duplicates)
def insert_series(series, connection):
    insert_stmt = fred_series.insert().prefix_with("OR IGNORE")
    connection.execute(insert_stmt, {
        'id': series['id'],
        'title': series['title'],
        'observation_start': series['observation_start'],
        'observation_end': series['observation_end'],
        'frequency': series['frequency'],
        'units': series['units'],
        'seasonal_adjustment': series['seasonal_adjustment'],
        'last_updated': series['last_updated'],
        'notes': series.get('notes', '')
    })


def process_category(category_id, level=0):
    global total_series
    if category_id in visited_categories:
        return
    visited_categories.add(category_id)

    print(f"\n[Category] {'  '*level}Fetching series for category_id {category_id}", flush=True)

    # Fetch series metadata from the category
    series_params = {
        "api_key": API_KEY,
        "file_type": "json",
        "category_id": category_id,
        "limit": 1000
    }
    series_data = safe_get(SERIES_ENDPOINT, series_params)

    if series_data:
        for series in series_data.get("seriess", []):
            sid = series["id"]
            if sid not in seen_series_ids:
                seen_series_ids.add(sid)
                print(f"[Series] {sid} - {series['title']}", flush=True)
                total_series += 1
                if total_series >= SERIES_LIMIT:
                    print(f"\n[STOP] Reached limit of {SERIES_LIMIT} series. Halting crawl.\n", flush=True)
                    sys.exit(0)

                # Insert series metadata
                with engine.begin() as conn:
                    stmt = pg_insert(fred_series).values({
                        'id': sid,
                        'title': series['title'],
                        'observation_start': series['observation_start'],
                        'observation_end': series['observation_end'],
                        'frequency': series['frequency'],
                        'units': series['units'],
                        'seasonal_adjustment': series['seasonal_adjustment'],
                        'last_updated': series['last_updated'],
                        'notes': series.get('notes', '')
                    }).on_conflict_do_nothing(index_elements=['id'])  

                    conn.execute(stmt)

                # Fetch and store observations
                obs_params = {
                    "api_key": API_KEY,
                    "file_type": "json",
                    "series_id": sid
                }
                obs_url = f"{BASE_URL}/series/observations"
                obs_data = safe_get(obs_url, obs_params)

                if obs_data:
                    observations = obs_data.get("observations", [])
                    obs_rows = []
                    for obs in observations:
                        try:
                            obs_rows.append({
                                'series_id': sid,
                                'date': obs['date'],
                                'value': float(obs['value']) if obs['value'] not in ("", ".") else None
                            })
                        except ValueError:
                            continue  # skip malformed values

                    # Insert all observations in one batch
                    if obs_rows:
                        with engine.begin() as conn:
                            conn.execute(fred_observations.insert(), obs_rows)

                # Optional: print progress
                if total_series % 250 == 0:
                    print(f"\n [Checkpoint]  {total_series:,} series stored\n", flush=True)

    # Recurse into child categories
    children_params = {
        "api_key": API_KEY,
        "file_type": "json",
        "category_id": category_id
    }
    children_data = safe_get(CHILDREN_ENDPOINT, children_params)
    if children_data:
        for child in children_data.get("categories", []):
            process_category(child["id"], level + 1)


# Start from root
try:
    print("[Start] Beginning recursive category crawl from root (ID = 0)", flush=True)
    process_category(0)
    print(f"\n[Done] Finished processing all categories. Total unique series: {total_series}", flush=True)

except Exception as e:
    print(f"[Fatal Error] The script crashed: {e}", flush=True)

