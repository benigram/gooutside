from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta, timezone
from ingestion.air import fetch_air_data, parse_uba_data, save_air_entry_to_gcs
import logging

default_args = {
    "owner": "beni",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="air_pipeline_dag",
    schedule_interval="@hourly",
    start_date=datetime(2025, 4, 12, 6),
    catchup=False,
    default_args=default_args,
    tags=["air"],
    description="Ingests hourly air quality data from UBA and transforms to Parquet now",
)
def air_dag():
    @task()
    def ingest(**context):
        log = logging.getLogger(__name__)

        # Execution timestamp in UTC
        utc_time = context["execution_date"]
        MEZ = timezone(timedelta(hours=1))
        local_time = utc_time.astimezone(MEZ)
        date_str = local_time.date().isoformat()

        log.info(f"📅 Execution time: {utc_time.isoformat()}, MEZ date: {date_str}")

        raw = fetch_air_data(station_id=443, date=date_str)
        parsed = parse_uba_data(
            raw,
            city="bamberg",
            station_id=443,
            station_code="DEBY009"
        )

        log.info(f"📦 Parsed {len(parsed)} entries")

        for entry in parsed:
            log.info(f"💾 Saving air entry for {entry['timestamp']}")
            save_air_entry_to_gcs(entry)

    transform = BashOperator(
        task_id='transform_air_to_parquet',
        bash_command=(
            'docker exec gooutside-spark '
            'spark-submit /opt/spark-app/transform_air.py "{{ ts }}"'
        )
    )

    ingest() >> transform

air_dag = air_dag()







