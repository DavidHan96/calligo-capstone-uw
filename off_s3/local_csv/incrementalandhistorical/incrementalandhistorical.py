import os
import requests
import time
import sys
import pandas as pd
from datetime import datetime, timedelta

# Constants
API_KEY = "YOUR_API_KEY"
BASE_URL = "https://api.stlouisfed.org/fred"
SERIES_ENDPOINT = f"{BASE_URL}/category/series"
CHILDREN_ENDPOINT = f"{BASE_URL}/category/children"
METADATA_FILE = "metadata.csv"
SERIES_LIMIT = 1000000

visited_categories = set()
seen_series_ids = set()
total_series = 0

# Ensure output directory exists
os.makedirs("data", exist_ok=True)

# Load existing metadata
if os.path.exists(METADATA_FILE):
    metadata_df = pd.read_csv(METADATA_FILE)
    metadata_df['last_updated'] = pd.to_datetime(metadata_df['last_updated'], errors='coerce')
    metadata_dict = metadata_df.set_index('id').to_dict('index')  # Store entire row
    existing_ids = set(metadata_dict.keys())
else:
    metadata_df = pd.DataFrame()
    metadata_dict = {}
    existing_ids = set()
    with open(METADATA_FILE, "w") as f:
        f.write("id,title,observation_start,observation_end,frequency,units,seasonal_adjustment,last_updated,notes\n")

# Helper for robust GET
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

# Freshness logic
def needs_refresh(meta):
    if pd.isna(meta.get('last_updated')):
        return True  # No update date means we should fetch
    freq = meta.get('frequency', '')
    age = datetime.now(meta['last_updated'].tzinfo) - meta['last_updated']
    if "Daily" in freq: return timedelta(days=3) >= age >= timedelta(days=1)
    if "Weekly" in freq: return timedelta(days=14) >= age >= timedelta(days=7)
    if "Monthly" in freq: return timedelta(days=60) >= age >= timedelta(days=30)
    if "Quarterly" in freq: return timedelta(days=180) >= age >= timedelta(days=90)
    if "Annual" in freq: return timedelta(days=730) >= age >= timedelta(days=365)
    return False

# Core function
def process_category(category_id, level=0):
    global total_series
    if category_id in visited_categories:
        return
    visited_categories.add(category_id)

    print(f"\n[Category] {'  '*level}Fetching series for category_id {category_id}", flush=True)

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
            if sid in seen_series_ids:
                continue
            seen_series_ids.add(sid)

            print(f"[Series] {sid} - {series['title']}", flush=True)
            total_series += 1
            if total_series >= SERIES_LIMIT:
                print(f"\n[STOP] Reached limit of {SERIES_LIMIT} series. Halting crawl.\n", flush=True)
                sys.exit(0)

            if sid not in existing_ids:
                # New series, save metadata
                metadata_row = {
                    'id': sid,
                    'title': series['title'],
                    'observation_start': series['observation_start'],
                    'observation_end': series['observation_end'],
                    'frequency': series['frequency'],
                    'units': series['units'],
                    'seasonal_adjustment': series['seasonal_adjustment'],
                    'last_updated': series['last_updated'],
                    'notes': series.get('notes', '').replace("\n", " ").replace(",", ";")
                }
                pd.DataFrame([metadata_row]).to_csv(METADATA_FILE, mode='a', index=False, header=False)
                existing_ids.add(sid)
                metadata_dict[sid] = metadata_row  # Add to metadata
                fetch_obs = True  # New series, fetch observations
            else:
                meta = metadata_dict[sid]
                if meta['title'].strip().upper().endswith("(DISCONTINUED)"):
                    print(f"[Skip] {sid} is DISCONTINUED. Skipping observations.", flush=True)
                    fetch_obs = False
                else:
                    fetch_obs = needs_refresh(meta)

            if fetch_obs:
                # Fetch and save observations
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
                            continue
                    if obs_rows:
                        pd.DataFrame(obs_rows).to_csv(f"data/{sid}.csv", index=False)
                        print(f"[Obs] Saved observations for {sid}", flush=True)
                else:
                    print(f"[Obs] No observations for {sid}", flush=True)
            else:
                print(f"[Obs] Skipped {sid}, up-to-date", flush=True)

            if total_series % 250 == 0:
                print(f"\n [Checkpoint]  {total_series:,} series stored\n", flush=True)

    # Recurse into subcategories
    children_params = {
        "api_key": API_KEY,
        "file_type": "json",
        "category_id": category_id
    }
    children_data = safe_get(CHILDREN_ENDPOINT, children_params)
    if children_data:
        for child in children_data.get("categories", []):
            process_category(child["id"], level + 1)

# Kick off crawl
try:
    print("[Start] Beginning recursive category crawl from root (ID = 0)", flush=True)
    process_category(0)
    print(f"\n[Done] Finished processing all categories. Total unique series: {total_series}", flush=True)
except Exception as e:
    print(f"[Fatal Error] The script crashed: {e}", flush=True)

