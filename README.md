# 🌤️ GoOutside – Your Daily Environmental Dashboard

**GoOutside** is a modular data engineering project that collects, processes, analyzes, and visualizes real-time weather, air quality, and pollen data. It's built for anyone interested in environmental insights — whether you're an athlete, a parent, or just someone planning their day.



## 📊 Key Features

- **Automated ingestion** from public APIs (DWD, Umweltbundesamt, BrightSky) using Python & `requests`
- **Data lake architecture**: JSON + Parquet in Google Cloud Storage (GCS)
- **Apache Spark** for fast batch transformation from raw to columnar formats
- **Machine learning pipeline** to forecast air quality and pollen trends
- **Data modeling** with `dbt` into clean, analytics-ready BigQuery tables
- **Airflow DAGs** to orchestrate ingestion, transformation & modeling
- **Streamlit dashboard** for interactive environmental insights
- **FastAPI** as optional REST interface (e.g. for manual ingestion or ML prediction)



## 🧰 Tech Stack

| Tool / Service          | Role                                          |
|--------------------------|-----------------------------------------------|
| **Docker**              | Containerization & local reproducibility      |
| **Airflow**             | Workflow orchestration (ETL)                  |
| **Spark**               | Data transformation (JSON → Parquet)          |
| **Python + requests**   | API ingestion layer                           |
| **FastAPI**             | Optional HTTP interface for triggering/prediction |
| **Google Cloud Storage**| Raw + processed storage (data lake)           |
| **BigQuery**            | Cloud data warehouse                          |
| **dbt**                 | SQL modeling (staging, DWH, OBT)              |
| **scikit-learn / XGBoost** | ML model training & forecasting            |
| **Streamlit**           | Dashboard frontend                            |

## ⚙️ Project Architecture
![GoOutside Architecture](gooutside-architecture.png)


## 🚀 Getting Started

```bash
# Clone the repo
git clone https://github.com/yourname/gooutside.git
cd gooutside

# Start Airflow & other services
docker compose up airflow-init
docker compose up
```

## ✨ Project Status

| Component                | Status          |
|--------------------------|-----------------|
| Planning & Setup         | ✅ done          |
| API Integration          | ✅ done (via `requests`) |
| Spark Transformation     | ✅ done          |
| Airflow DAGs             | ⏳ in progress   |
| BigQuery & dbt Models    | ⏳ in progress   |
| Streamlit Dashboard      | ⏳ in progress   |
| Machine Learning Pipeline| 🔜 Planned       |
| Deployment & Hosting     | 🔜 Planned       |