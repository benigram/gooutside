from airflow.decorators import dag, task
from datetime import datetime, timedelta, timezone
from ingestion.air import fetch_air_data, parse_uba_data, save_air_entry_to_gcs
import logging

default_args = {
    "owner": "beni",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="air_ingestion_dag",
    schedule_interval="@hourly",
    start_date=datetime(2025, 4, 9, 6),
    catchup=False,
    default_args=default_args,
    tags=["air"],
    description="Ingests all available air quality data from UBA for the current day and saves to GCS",
)
def air_dag():
    @task()
    def ingest():
        log = logging.getLogger(__name__)

        # UBA API expects MEZ (UTC+1, fixed, no DST)
        MEZ = timezone(timedelta(hours=1))
        today_mez = datetime.now(MEZ).date().isoformat()

        log.info(f"📅 Fetching UBA data for MEZ day: {today_mez}")

        raw = fetch_air_data(station_id=443, date=today_mez)
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

    ingest()

air_dag = air_dag()






