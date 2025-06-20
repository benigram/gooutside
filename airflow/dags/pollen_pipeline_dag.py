from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta, timezone
import logging
from ingestion.pollen import fetch_pollen_data, parse_pollen_data, save_pollen_entry_to_gcs, delete_old_pollen_forecasts, delete_old_pollen_parquet_folders
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

default_args = {
    "owner": "beni",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="pollen_pipeline_dag",
    schedule_interval="0 6 * * *", # 8 am UTC , 7 am in winter, 8 am Sommer
    start_date=datetime(2025, 4, 4),
    catchup=False, #no historical data
    default_args=default_args,
    tags=["pollen"],
    description="Ingests daily pollen data and transforms to Parquet in one DAG",
)
def pollen_pipeline():

    @task()
    def ingest(ds=None, **kwargs):
        log = logging.getLogger(__name__)
        log.info(f"📥 Start Ingestion for {ds}")

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
    
    @task()
    def clean():
        now = datetime.now(timezone.utc)
        delete_old_pollen_forecasts(bucket_name="gooutside-raw", region="bayern_nord", threshold_dt=now)
        delete_old_pollen_parquet_folders(bucket_name="gooutside-processed", region="bayern_nord", threshold_date=now.date())

    transform = BashOperator(
        task_id='transform_to_parquet',
        bash_command=(
            'docker exec gooutside-spark '
            'spark-submit /opt/spark-app/transform_pollen.py {{ next_ds }}'
        )
    )

    dbt_run = BashOperator(
        task_id="dbt_run_pollen",
        bash_command="cd /opt/dbt && dbt run --select tag:pollen",
        do_xcom_push=True
    )


    ingest() >> clean() >> transform >> dbt_run

pollen_pipeline = pollen_pipeline()
