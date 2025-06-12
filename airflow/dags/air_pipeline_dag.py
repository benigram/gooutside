from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta, timezone
from ingestion.air import fetch_air_data, parse_uba_data, save_air_entry_to_gcs, delete_old_air, delete_old_parquet_air
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import logging
import json
import pytz

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

    @task()
    def clean():
        now = datetime.now(timezone.utc)
        delete_old_air(bucket_name="gooutside-raw", city="bamberg", threshold_dt=now)
        delete_old_parquet_air(bucket_name="gooutside-processed", city="bamberg", threshold_date=now.date())


    transform = BashOperator(
        task_id='transform_air_to_parquet',
        bash_command=(
            'docker exec gooutside-spark '
            'spark-submit /opt/spark-app/transform_air.py "{{ ts }}"'
        )
    )
    
    delete_existing = BigQueryInsertJobOperator(
        task_id="delete_existing_air_for_day",
        configuration={
            "query": {
                "query": (
                    "DELETE FROM `gooutside-dev.gooutside_raw.air_bamberg` "
                    "WHERE date = '{{ ds }}'"
                ),
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ALL_DONE
    )

    load_to_bq = BigQueryInsertJobOperator(
        task_id="bq_load_air_parquet",
        configuration={
            "load": {
                "sourceUris": [
                    "gs://gooutside-processed/flat/air_bamberg/{{ ds }}/part-*.parquet"
                ],
                "destinationTable": {
                    "projectId": "gooutside-dev",
                    "datasetId": "gooutside_raw",
                    "tableId": "air_bamberg"
                },
                "sourceFormat": "PARQUET",
                "writeDisposition": "WRITE_APPEND"
            }
        },
        gcp_conn_id="google_cloud_default"
    )

    dbt_run = BashOperator(
        task_id="dbt_run_air",
        bash_command="cd /opt/dbt && dbt run --select tag:air",
        do_xcom_push=True
    )

    ingest() >> clean() >> transform >> delete_existing >> load_to_bq >> dbt_run

air_dag = air_dag()







