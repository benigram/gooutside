from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, timezone
from ingestion.weather import fetch_weather_data, parse_weather_data, save_weather_entry_to_gcs
import logging

default_args = {
    "owner": "beni",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="weather_ingestion_dag",
    start_date=datetime(2025, 4, 4, 8),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["weather"],
    description="Ingests hourly weather data from Bright Sky and saves to GCS",
)
def weather_dag():
    @task()
    def ingest():
        log = logging.getLogger(__name__)

        # Current UTC hour (rounded down to :00)
        now_utc = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        date_str = now_utc.date().isoformat()

        log.info(f"🌍 Current UTC hour: {now_utc.isoformat()}")

        raw = fetch_weather_data(date=date_str)
        parsed = parse_weather_data(raw, city="bamberg")

        # Filter: only store the current hour
        matching = [entry for entry in parsed if entry["timestamp"] == now_utc.isoformat()]
        log.info(f"⏱ Found {len(matching)} matching entries for this hour")

        for entry in matching:
            log.info(f"💾 Saving weather entry for {entry['timestamp']}")
            save_weather_entry_to_gcs(entry)

    ingest()

weather_dag = weather_dag()

