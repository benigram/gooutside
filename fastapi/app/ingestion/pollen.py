import requests
from datetime import datetime, timezone
from google.cloud import storage
import json
import os
from dateutil import parser

POLLEN_REGION_MAP = {
    123: {
        "region_slug": "bayern_nord",
        "city": "bamberg"
    }
}

def fetch_pollen_data():
    """
    Fetches the full pollen forecast JSON file from the DWD (Deutscher Wetterdienst).
    This file includes daily pollen forecasts for all German subregions.

    Returns:
        dict: Parsed JSON data containing all region forecasts.
              Returns empty dict on failure.
    """
    url = "https://opendata.dwd.de/climate_environment/health/alerts/s31fg.json"

    try:
        response = requests.get(url)
        response.raise_for_status()  # Raise exception on HTTP error
        return response.json()       # Full JSON: contains all partregions

    except requests.RequestException as e:
        print(f"❌ Failed to fetch pollen data: {e}")
        return {}


def parse_pollen_data(raw_data: dict, partregion_id: int = 123):
    """
    Extracts the pollen forecast for a specific region from the full DWD dataset.

    Args:
        raw_data (dict): Full JSON response from the DWD pollen API.
        partregion_id (int): ID of the region (e.g. 123 = Bamberg area).

    Returns:
        dict: Parsed pollen forecast with region name, date and today's pollen levels.
    """
    region_data = None

    # Find the matching region in the list
    for region in raw_data.get("content", []):
        if region.get("partregion_id") == partregion_id:
            region_data = region
            break

    if not region_data:
        print(f"❌ Region with ID {partregion_id} not found.")
        return {}

    today = datetime.utcnow().date().isoformat()
    mapped = POLLEN_REGION_MAP.get(partregion_id, {})

    # Build clean output format
    pollen_forecast = {
        "region": region_data.get("partregion_name", "unknown"),
        "region_slug": mapped.get("region_slug", "unknown"),
        "city": mapped.get("city", "unknown"),
        "region_id": partregion_id,
        "date": today,
        "forecast": {}
    }

    # Add today's pollen level per type
    for pollen_type, values in region_data.get("Pollen", {}).items():
        pollen_forecast["forecast"][pollen_type] = values.get("today", None)

    return [pollen_forecast]

def save_pollen_entry_to_gcs(entry: dict, bucket_name: str = "gooutside-raw"):
    """
    Saves a single pollen forecast entry to GCS in structured path:
    Format: gs://<bucket>/pollen/<region>/<date>.json
    """
    # Local credentials (only needed locally)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/opt/airflow/credentials/fastapi-gcs-key.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Use region & date for path
    region = entry["region_slug"]
    date = entry["date"]
    filename = f"pollen/{region}/{date}.json"

    # Upload
    blob = bucket.blob(filename)
    blob.upload_from_string(
        data=json.dumps(entry, indent=2),
        content_type="application/json"
    )

    print(f"✅ Saved: gs://{bucket_name}/{filename}")

def delete_old_pollen_forecasts(bucket_name: str, region: str, threshold_dt: datetime):
    """
    Deletes all pollen forecast entries from GCS older than the given UTC threshold.
    Example filename: pollen/bayern_nord/2025-04-02.json
    """
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/fastapi-gcs-key.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"pollen/{region.lower()}/"

    blobs = list(bucket.list_blobs(prefix=prefix))

    for blob in blobs:
        filename = blob.name.split("/")[-1].replace(".json", "")  # "2025-04-02"
        try:
            ts = parser.isoparse(filename).replace(tzinfo=timezone.utc)

            # Nur löschen, wenn das Datum vor dem aktuellen Tag liegt
            cutoff = threshold_dt.replace(hour=0, minute=0, second=0, microsecond=0)
            if ts < cutoff:
                print(f"🗑️ Deleting outdated pollen forecast: {blob.name}")
                blob.delete()
        except Exception as e:
            print(f"⚠️ Could not parse/delete {blob.name}: {e}")


def delete_old_pollen_parquet_folders(bucket_name: str, region: str, threshold_date: datetime.date):
    """
    Deletes all pollen forecast parquet files from GCS older than the given UTC threshold date.
    Beispielpfad: flat/pollen_forecast_bayern_nord/2025-04-08/part-*.parquet
    """
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/fastapi-gcs-key.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    prefix = f"flat/pollen_{region}/"
    blobs = list(bucket.list_blobs(prefix=prefix))

    for blob in blobs:
        parts = blob.name.split("/")
        if len(parts) < 3:
            continue

        try:
            folder_date = datetime.strptime(parts[2], "%Y-%m-%d").date()
            if folder_date < threshold_date:
                print(f"🗑️ Deleting outdated pollen parquet file: {blob.name}")
                blob.delete()
        except Exception as e:
            print(f"⚠️ Could not parse/delete {blob.name}: {e}")
