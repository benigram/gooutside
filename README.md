# 🌤️ GoOutside – Your Daily Environmental Dashboard

**GoOutside** is a modular data engineering project that collects, processes, analyzes, and visualizes real-time weather, air quality, and pollen data. Ideal for generating insights or forecasts about when it's best to go outside.


## 📊 Key Features

- Automated data ingestion from public APIs (weather, air quality, pollen)
- Raw data stored in a **Google Cloud Storage** data lake (JSON)
- Processed with **Apache Spark** and stored as Parquet
- Modeled into clean **BigQuery tables** using **dbt**
- Machine learning model trained on environmental features
- **Orchestrated with Airflow**
- Interactive dashboard built with **Streamlit**



## ⚙️ Tech Stack

| Tool            | Purpose                          |
|-----------------|----------------------------------|
| Docker        | Containerization & local dev     |
| Airflow       | Workflow orchestration           |
| Spark         | Data transformation (JSON → Parquet) |
| dbt           | Data modeling (BigQuery)         |
| BigQuery      | Data warehouse                   |
| Streamlit     | Dashboard (UI layer)             |
| FastAPI       | API for ingestion & inference    |
| scikit-learn / XGBoost | ML training pipeline   |

