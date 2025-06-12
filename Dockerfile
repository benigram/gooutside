FROM apache/airflow:2.10.5

USER root

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential python3-dev && apt-get clean

# Wechsel zu airflow user für sichere Installation
USER airflow

# 📦 dbt für BigQuery installieren (über pip, als airflow-user)
RUN pip install --no-cache-dir --upgrade "protobuf<5.0" dbt-bigquery==1.9.1






