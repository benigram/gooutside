import requests
from datetime import datetime
from google.cloud import storage
import json
import os
from dateutil import parser

def fetch_weather_data(lat: float = 49.89, lon: float = 10.89, date: str = None):
    """
    Fetches weather data from Bright Sky API for a given location and date (in UTC).
    """

    if date is None:
        date = datetime.utcnow().date().isoformat()  # e.g. "2025-04-02"

    base_url = "https://api.brightsky.dev/weather"

    params = {
        "lat": lat,
        "lon": lon,
        "date": date,
        "tz": "Etc/UTC"  # ✅ Store timestamps in UTC to match air data
    }

    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        print(f"❌ API Error: {e}")
        return {}
    


def parse_weather_data(api_response: dict, city: str = "bamberg"):
    """
    Parses Bright Sky weather response into a list of hourly weather entries.
    Filters for hours 6–20 UTC (e.g. 08–22 MESZ).
    """

    parsed_entries = []
    raw_data = api_response.get("weather", [])

    for entry in raw_data:
        timestamp = entry.get("timestamp")
        hour = int(timestamp[11:13])  # extract hour from e.g. "2025-04-02T07:00:00+00:00"

        if 6 <= hour <= 20:  # Only store daytime hours (UTC) and measured values
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


def save_weather_entry_to_gcs(entry: dict, bucket_name: str = "gooutside-raw"):
    """
    Saves a single weather entry to GCS in structured UTC path:
    Format: gs://<bucket>/weather/<city>/<timestamp>.json
    """
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/fastapi-gcs-key.json"

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    timestamp = entry["timestamp"]
    dt = parser.isoparse(timestamp)
    safe_ts = dt.strftime("%Y-%m-%dT%H%M")  # e.g. "2025-04-02T0600"
    city = entry["city"].lower()
    filename = f"weather/{city}/{safe_ts}.json"

    blob = bucket.blob(filename)
    blob.upload_from_string(
        data=json.dumps(entry, indent=2),
        content_type="application/json"
    )

    print(f"✅ Saved: gs://{bucket_name}/{filename}")

    
