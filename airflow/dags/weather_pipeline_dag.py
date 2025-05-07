from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
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
    dag_id="weather_pipeline_dag",
    start_date=datetime(2025, 4, 13, 6),  # z. B. 6 Uhr
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["weather"],
    description="Ingests hourly weather data from Bright Sky and transforms to Parquet",
)
def weather_dag():
    @task()
    def ingest(**context):
        log = logging.getLogger(__name__)

        # Airflow execution time (UTC)
        exec_time_utc = context["execution_date"].replace(minute=0, second=0, microsecond=0)
        date_str = exec_time_utc.date().isoformat()

        log.info(f"🌍 Execution time (UTC): {exec_time_utc.isoformat()}")

        # Fetch full-day data (Bright Sky returns 24h at once)
        raw = fetch_weather_data(date=date_str)
        parsed = parse_weather_data(raw, city="bamberg")

        # Filter to current execution hour
        matching = [entry for entry in parsed if entry["timestamp"] == exec_time_utc.isoformat()]
        log.info(f"⏱ Found {len(matching)} matching entries for this hour")

        for entry in matching:
            log.info(f"💾 Saving weather entry for {entry['timestamp']}")
            save_weather_entry_to_gcs(entry)

    # Trigger the Spark transform after ingestion
    transform = BashOperator(
        task_id="transform_weather_to_parquet",
        bash_command=(
            'docker exec gooutside-spark '
            'spark-submit /opt/spark-app/transform_weather.py "{{ ds }}"'
        )
    )

    ingest() >> transform

weather_dag = weather_dag()

