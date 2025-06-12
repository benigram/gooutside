from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, timezone
from airflow.utils.trigger_rule import TriggerRule
from ingestion.weather_forecast import (
    fetch_weather_data,
    parse_weather_forecast,
    save_weather_forecast_to_gcs,
    delete_old_forecasts,
    delete_old_parquet_folders,
)
import logging
from dateutil import parser

default_args = {
    "owner": "beni",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="weather_forecast_pipeline_dag",
    start_date=datetime(2025, 4, 10, 8),
    schedule_interval="@hourly",
    catchup=False,
    default_args=default_args,
    tags=["weather", "forecast"],
    description="Ingests and maintains up-to-date weather forecast data from Bright Sky and saves to GCS and Parquet",
)
def forecast_dag():
    @task()
    def ingest(**context):
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
            if parser.isoparse(entry["timestamp"]) >= now_utc_dt.replace(minute=0, second=0, microsecond=0)
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
        delete_old_parquet_folders(bucket_name="gooutside-processed", city="bamberg", threshold_date=now.date())

    

    transform = BashOperator(
        task_id="transform_forecast_to_parquet",
        bash_command=(
            'docker exec gooutside-spark '
            'spark-submit /opt/spark-app/transform_weather_forecast.py {{ ds }}'
        )
    )

    delete_existing_data = BigQueryInsertJobOperator(
        task_id="delete_existing_weather_for_day",
        configuration={
            "query": {
                "query": """
                    DELETE FROM `gooutside-dev.gooutside_raw.weather_forecast_bamberg`
                    WHERE TRUE
                """,
                "useLegacySql": False,
            }
        },
        gcp_conn_id="google_cloud_default",
        trigger_rule=TriggerRule.ALL_DONE
    )


    # Forecast Load
    load_tasks = []
    for n in range(8):
        forecast_date = (datetime.utcnow().date() + timedelta(days=n)).isoformat()

        load_task = BigQueryInsertJobOperator(
            task_id=f"bq_load_forecast_{forecast_date}",
            configuration={
                "load": {
                    "sourceUris": [
                        f"gs://gooutside-processed/flat/weather_forecast_bamberg/{forecast_date}/part-*.parquet"
                    ],
                    "destinationTable": {
                        "projectId": "gooutside-dev",
                        "datasetId": "gooutside_raw",
                        "tableId": "weather_forecast_bamberg"
                    },
                    "sourceFormat": "PARQUET",
                    "writeDisposition": "WRITE_APPEND"
                }
            },
            gcp_conn_id="google_cloud_default"
        )
        load_tasks.append(load_task)
    
    dbt_run = BashOperator(
        task_id="dbt_run_weather",
        bash_command="cd /opt/dbt && dbt run --select tag:weather",
        do_xcom_push=True
    )

    ingest() >> clean() >> transform >> delete_existing_data >> load_tasks >> dbt_run

forecast_dag = forecast_dag()


