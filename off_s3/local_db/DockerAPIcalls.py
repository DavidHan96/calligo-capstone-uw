try:
    import requests
    import time
    import sys

    API_KEY = "YOUR_API_KEY"
    BASE_RELEASE_URL = "https://api.stlouisfed.org/fred/releases"
    BASE_SERIES_URL = "https://api.stlouisfed.org/fred/release/series"

    def safe_get(url, params, retries=5, backoff=10):
        for attempt in range(retries):
            try:
                response = requests.get(url, params=params, timeout=10)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"[ERROR] Attempt {attempt + 1} failed: {e}", flush=True)
                time.sleep(backoff * (attempt + 1))
        return None

    all_releases = []
    offset = 0 #nothing after 100k
    limit = 1000

    print("[Fetch] Starting to collect all FRED releases via pagination...", flush=True)

    while True:
        print(f"[Fetch] Requesting releases with offset={offset}", flush=True)
        paged_params = {
            "api_key": API_KEY,
            "file_type": "json",
            "limit": limit,
            "offset": offset,
            "sort_order": "asc",         
            "sort_by": "release_id"
        }
        page_data = safe_get(BASE_RELEASE_URL, paged_params)
        if not page_data:
            print(f"[ERROR] Failed to fetch releases at offset {offset}", flush=True)
            break

        page_releases = page_data.get("releases", [])
        if not page_releases:
            print("[Fetch] No more releases found. Done paginating.", flush=True)
            break

        all_releases.extend(page_releases)
        offset += limit

    print(f"[Init] Total releases to process: {len(all_releases)}", flush=True)

    cpt = 0      # Total successful series printed
    cptb = 0     # Total failed series printed

    for release in all_releases:
        release_id = release.get("id")
        print(f"[Loop] Processing release_id {release_id}", flush=True)

        params = {
            "api_key": API_KEY,
            "file_type": "json",
            "release_id": release_id
        }

        print(f"[Request] Fetching series for release_id {release_id}", flush=True)
        data = safe_get(BASE_SERIES_URL, params)

        if not data:
            print(f"[WARN] Skipping release_id {release_id} due to repeated failures.", flush=True)
            continue

        for series in data.get("seriess", []):
            try:
                print(f"[Series] {series['id']} - {series['title']}", flush=True)
                cpt += 1
            except Exception as e:
                cptb += 1
                print(f"[ERROR] Failed to print a series in release {release_id}: {e}", flush=True)

        if cpt % 1000 == 0:
            print(f"[Checkpoint] {cpt} series fetched so far.", flush=True)

        del data

    print(f"[Done] Finished processing all releases. Final count: {cpt}. Failed count: {cptb}", flush=True)

except Exception as e:
    print(f"Fatal error the script crashed :( : {e}", flush=True)
