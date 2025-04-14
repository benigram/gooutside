import requests
from datetime import datetime, timezone, timedelta
from google.cloud import storage
import json
import os
from dateutil import parser
import pytz

def fetch_air_data(station_id: int = 443, date: str = None):
    """
    Fetches air quality data from the German UBA API for a given station and date.
    
    Args:
        station_id (int): The ID of the measuring station (default: 443 = Bamberg).
        date (str): Date in "YYYY-MM-DD" format. Defaults to today if not provided.
    
    Returns:
        dict: JSON response from the UBA API, or an empty dict if the request fails.
    """

    # If no date is provided, use today's date in local German time (Europe/Berlin)
    if date is None:
        date = datetime.now(pytz.timezone("Europe/Berlin")).date().isoformat()


    # Base URL for UBA Air Quality API (v3)
    base_url = "https://umweltbundesamt.api.proxy.bund.dev/api/air_data/v3/airquality/json"

    # API query parameters
    params = {
        "station": station_id,
        "date_from": date,
        "time_from": 6,      # Start at 7 AM (MEZ) = 6 AM (UTC)
        "date_to": date,
        "time_to": 23,       # End at 22 PM (MEZ) = 21 AM (UTC)
        "index": "code",     # Request the numeric index (0–5)
        "lang": "en"         # Use English (affects some metadata)
    }

    try:
        # Perform the GET request
        response = requests.get(base_url, params=params)
        response.raise_for_status()  # Raise exception on HTTP error
        return response.json()       # Return parsed JSON data
                                     # Timestamps are in MEZ (UTC+1) 

    except requests.RequestException as e:
        print(f"❌ API Error: {e}")
        return {}  # Return empty dict on failure
    
component_map = {
    1: "PM10",
    5: "NO2",
    9: "PM2.5"
}

# MEZ = UTC+1, no summer time!
MEZ = timezone(timedelta(hours=1))
    
def parse_uba_data(api_response: dict, station_id: int, station_code: str, city: str):
    """
    Parses raw UBA API response into a structured list of data entries.
    Interprets timestamps as MEZ (UTC+1) and converts to UTC.
    
    Returns:
        List of entries with timestamp in UTC (ISO format).
    """
    parsed_entries = []

    # Get raw hourly data for the given station
    hourly_data = api_response.get("data", {}).get(str(station_id), {})

    for timestamp, values in hourly_data.items():
        
        # Parse timestamp from API (naive, but known to be MEZ = UTC+1)
        #naive_dt = datetime.fromisoformat(timestamp)
        #dt_mez = naive_dt.replace(tzinfo=MEZ)

        # Convert to UTC
        #dt_utc = dt_mez.astimezone(timezone.utc)

        berlin = pytz.timezone("Europe/Berlin")

        # Parse and localize to Berlin time (handles DST)
        naive_dt = datetime.fromisoformat(timestamp)
        dt_local = berlin.localize(naive_dt)
        dt_utc = dt_local.astimezone(timezone.utc)

        hour_local = dt_local.hour

        if 6 <= hour_local <= 23:

            aqi = values[1]
            components_raw = values[3:]

            components = {}

            for entry in components_raw:
                comp_id = entry[0]
                value = entry[1]

                if comp_id in component_map:
                    name = component_map[comp_id]
                    components[name] = value

            parsed_entry = {
                "station_id": station_id,
                "station_code": station_code,
                "city": city,
                "timestamp": dt_utc.isoformat(),  # stored in UTC!
                "aqi": aqi,
                "components": components
            }

            parsed_entries.append(parsed_entry)

    return parsed_entries


def save_air_entry_to_gcs(entry: dict, bucket_name: str = "gooutside-raw"):
    """
    Saves a single air quality entry to GCS in structured path.
    
    e.g. timestamp is "2024-01-01T06:00:00+00:00"
    the timestamp is in UTC, so in Baveria it is "2024-01-01T07:00:00+00:00"
    """
    # Optional: Set credentials (only needed in dev/local)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "credentials/fastapi-gcs-key.json"

    # Create GCS client
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    # Parse and format timestamp (already in UTC)
    timestamp = entry["timestamp"]
    dt = parser.isoparse(timestamp)
    safe_ts = dt.strftime("%Y-%m-%dT%H%M")  # e.g., 2025-04-08T1600

    # Generate file path: air/bamberg/2025-04-02T06.json
    city = entry["city"].lower()
    filename = f"air/{city}/{safe_ts}.json"

    # Upload to GCS
    blob = bucket.blob(filename)
    blob.upload_from_string(
        data=json.dumps(entry, indent=2),
        content_type="application/json"
    )

    print(f"✅ Saved: gs://{bucket_name}/{filename}")



