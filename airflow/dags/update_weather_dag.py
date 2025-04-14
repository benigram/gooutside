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
    dag_id="update_weather_dag",
    start_date=datetime(2025, 4, 10, 3),
    schedule_interval="0 3 * * *",
    catchup=False,
    default_args=default_args,
    tags=["weather", "retro"],
    description="Reloads hourly weather data for yesterday to capture late changes",
)

def update_weather_dag():
    @task()
    def update_yesterday():
        log = logging.getLogger(__name__)

         # Reload data for the previous day (UTC)
        yesterday = (datetime.now(timezone.utc) - timedelta(days=1)).date()
        date_str = yesterday.isoformat()

        log.info(f"🔁 Reloading historical weather data for {date_str}")

        raw = fetch_weather_data(date=date_str)
        parsed = parse_weather_data(raw, city="bamberg")

        for entry in parsed:
            ts = entry["timestamp"]
            log.info(f"💾 Overwriting entry for {ts}")
            save_weather_entry_to_gcs(entry)  # automatically overwrites

    update_yesterday()

update_weather_dag = update_weather_dag()