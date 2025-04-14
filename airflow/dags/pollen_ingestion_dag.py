from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging
from ingestion.pollen import fetch_pollen_data, parse_pollen_data, save_pollen_entry_to_gcs

default_args = {
    "owner": "beni",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="pollen_ingestion_dag",
    schedule_interval="0 8 * * *", # 8 am UTC , 9 am in winter, 10 am Sommer
    start_date=datetime(2025, 4, 4),
    catchup=False, #no historical data
    default_args=default_args,
    tags=["pollen"],
    description="Ingests daily pollen data from DWD and saves to GCS",
)
def pollen_dag():
    @task()
    def ingest():
        log = logging.getLogger(__name__)

        raw = fetch_pollen_data()
        parsed = parse_pollen_data(raw, partregion_id=123)

        if not parsed:
            log.warning("⚠️ No pollen data parsed for region ID 123.")
            return
        
        for entry in parsed:
            log.info(f"🌼 Pollen forecast for {entry['city']} on {entry['date']}")
            log.info(f"   Region: {entry['region']} (slug: {entry['region_slug']})")
            log.info(f"   {len(entry['forecast'])} pollen types found")
            for pollen_type, level in entry["forecast"].items():
                log.info(f"     - {pollen_type}: {level}")

            save_pollen_entry_to_gcs(entry)
            log.info(f"✅ Saved entry to GCS for {entry['date']}")


    ingest()

pollen_dag = pollen_dag()
