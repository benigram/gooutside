from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, timezone
from ingestion.weather_forecast import (
    fetch_weather_data,
    parse_weather_forecast,
    save_weather_forecast_to_gcs,
    delete_old_forecasts,
)
import logging
from dateutil import parser

default_args = {
    "owner": "beni",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="weather_forecast_ingestion_dag",
    start_date=datetime(2025, 4, 10, 8),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["weather", "forecast"],
    description="Ingests and maintains up-to-date weather forecast data from Bright Sky and saves to GCS",
)
def forecast_dag():
    @task()
    def ingest():
        log = logging.getLogger(__name__)

        now_utc = datetime.now(timezone.utc).date()
        last_date = (now_utc + timedelta(days=8)).isoformat()
        date_str = now_utc.isoformat()

        log.info(f"🌤️ Fetching forecast from {date_str} to {last_date}")

        raw = fetch_weather_data(date=date_str, last_date=last_date)
        parsed = parse_weather_forecast(raw, city="bamberg")

        # Only future forecast entries from next full hour
        now_utc_dt = datetime.now(timezone.utc)
        filtered = [
            entry for entry in parsed
            if parser.isoparse(entry["timestamp"]) >= now_utc_dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
        ]

        if not filtered:
            log.warning("⚠️ No forecast entries found after current hour.")
            return
        
        log.info(f"📦 Filtered {len(filtered)} future forecast entries")

        for entry in filtered:
            log.info(f"💾 Saving forecast for {entry['timestamp']}")
            save_weather_forecast_to_gcs(entry)

    @task()
    def clean():
       now = datetime.now(timezone.utc)
       delete_old_forecasts(bucket_name="gooutside-raw", city="bamberg", threshold_dt=now)

    ingest() >> clean()

forecast_dag = forecast_dag()

