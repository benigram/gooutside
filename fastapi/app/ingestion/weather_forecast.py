import requests
from datetime import datetime, timezone
from google.cloud import storage
import json
import os
from dateutil import parser

def fetch_weather_data(lat: float = 49.89, lon: float = 10.89, date: str = None, last_date: str = None):
    """
    Fetch forecast data from Bright Sky API for a given location and date.
    Forecast timestamps are returned in UTC.
    """
    if date is None:
        date = datetime.utcnow().date().isoformat()
    
    if last_date is None:
        last_date = datetime.utcnow().date().isoformat() + timedelta(8)

    base_url = "https://api.brightsky.dev/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "date": date,
        "last_date": last_date,
        "tz": "Etc/UTC"  # store all forecast timestamps in UTC
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"❌ API Error (forecast): {e}")
        return {}

def parse_weather_forecast(api_response: dict, city: str = "bamberg"):
    """
    Parses Bright Sky forecast data. Filters for hours 6–20 UTC.
    """
    parsed_entries = []
    raw_data = api_response.get("weather", [])

    for entry in raw_data:
        timestamp = entry.get("timestamp")
        hour = int(timestamp[11:13])

        if 6 <= hour <= 20:
            parsed_entries.append({
                "timestamp": timestamp,
                "city": city,
                "condition": entry.get("condition"),
                "temperature": entry.get("temperature"),
                "dew_point": entry.get("dew_point"),
                "pressure_ml": entry.get("pressure_ml"),
                "relative_humidity": entry.get("relative_humidity"),
                "visibility": entry.get("visibility"),
                "precipitation": entry.get("precipitation"),
                "solar": entry.get("solar"),
                "sunshine": entry.get("sunshine"),
                "wind_speed": entry.get("wind_speed"),
                "wind_direction": entry.get("wind_direction"),
                "wind_gust_direction": entry.get("wind_gust_direction"),
                "wind_gust_speed": entry.get("wind_gust_speed")
            })

    return parsed_entries

def save_weather_forecast_to_gcs(entry: dict, bucket_name: str = "gooutside-raw"):
    """
    Saves a single forecast entry to GCS under weather_forecast/<city>/<timestamp>.json
    """
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/fastapi-gcs-key.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    timestamp = entry["timestamp"]
    dt = parser.isoparse(timestamp)
    safe_ts = dt.strftime("%Y-%m-%dT%H%M")  # e.g. "2025-04-08T1600"
    city = entry["city"].lower()
    filename = f"weather_forecast/{city}/{safe_ts}.json"

    blob = bucket.blob(filename)
    blob.upload_from_string(
        data=json.dumps(entry, indent=2),
        content_type="application/json"
    )

    print(f"🌤️ Saved forecast: gs://{bucket_name}/{filename}")


def delete_old_forecasts(bucket_name: str, city: str, threshold_dt: datetime):
    """
    Deletes all forecast entries from GCS older than the given UTC threshold.
    Example filename: weather_forecast/bamberg/2025-04-08T1600.json
    """
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/fastapi-gcs-key.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    prefix = f"weather_forecast/{city.lower()}/"

    blobs = list(bucket.list_blobs(prefix=prefix))

    for blob in blobs:
        filename = blob.name.split("/")[-1].replace(".json", "")  # "2025-04-08T1600"
        try:
            # Force timezone-aware datetime
            ts = parser.isoparse(filename[:13] + ":00").replace(tzinfo=timezone.utc)
            if ts < threshold_dt:
                print(f"🗑️ Deleting outdated forecast: {blob.name}")
                blob.delete()
        except Exception as e:
            print(f"⚠️ Could not parse/delete {blob.name}: {e}")
