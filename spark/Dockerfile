FROM bitnami/spark:latest

USER root

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Add GCS connector manually
ADD jars/gcs-connector-hadoop3-latest.jar /opt/bitnami/spark/jars/