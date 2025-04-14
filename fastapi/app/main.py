from fastapi import FastAPI
from app.ingestion.air import fetch_air_data, parse_uba_data, save_air_entry_to_gcs
from app.ingestion.pollen import fetch_pollen_data, parse_pollen_data, save_pollen_entry_to_gcs
from app.ingestion.weather import fetch_weather_data, parse_weather_data, save_weather_entry_to_gcs

app = FastAPI(title="GoOutside Ingestion API")

@app.get("/")
def root():
    return {"message": "GoOutside API is running 🚀"}

@app.get("/ingest/air")
def ingest_air():
    raw = fetch_air_data()
    parsed = parse_uba_data(raw, city="bamberg")
    for entry in parsed:
        save_air_entry_to_gcs(entry)
    return {"status": "✅ Air data ingested"}

@app.get("/ingest/pollen")
def ingest_pollen():
    raw = fetch_pollen_data()
    parsed = parse_pollen_data(raw, region_id=123)
    for entry in parsed:
        save_pollen_entry_to_gcs(entry)
    return {"status": "✅ Pollen data ingested"}

@app.get("/ingest/weather")
def ingest_weather():
    raw = fetch_weather_data()
    parsed = parse_weather_data(raw, city="bamberg")
    for entry in parsed:
        save_weather_entry_to_gcs(entry)
    return {"status": "✅ Weather data ingested"}
