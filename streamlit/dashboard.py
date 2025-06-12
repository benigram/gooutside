import streamlit as st
from google.cloud import bigquery
import streamlit.components.v1 as components
from datetime import datetime, timedelta, timezone
import pandas as pd
from dateutil import parser
import pytz
import altair as alt
from streamlit.components.v1 import html

# Aktuelles Datum
today = datetime.utcnow().date()

# Page config
st.set_page_config(page_title="Wetter in Bamberg", page_icon="🌤️", layout="wide")

st.markdown("""
    <style>
    
    .stApp {
        background: linear-gradient(180deg, #a1c4fd 0%, #c2e9fb 100%);
        color: #ffffff;
    }
    .stApp * {
        color: #ffffff !important;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    }
    header.stAppHeader {
        background: rgba(255, 255, 255, 0.1);
        backdrop-filter: blur(12px);
        border-bottom: 1px solid rgba(255,255,255,0.2);
    }
    </style>
""", unsafe_allow_html=True)

# Farbzuordnung für AQI
def get_color(level: str) -> str:
    return {
        "sehr gut": "#A9D18E",
        "gut": "#C6E0B4",
        "mäßig": "#FFD966",
        "schlecht": "#F4B084",
        "sehr schlecht": "#FF6F61"
    }.get(level.lower(), "#CCCCCC")

# BigQuery-Client
client = bigquery.Client(project="gooutside-dev")




# Actual weather ---------------------------------------------------------------------------
query_weather_latest = f"""
SELECT condition, temperature, city, feels_like, measurement_ts
FROM `gooutside-dev.gooutside_marts.mrt_weather_now`
WHERE date = '{today}' AND city = 'bamberg'
LIMIT 1
"""
df_weather_latest = client.query(query_weather_latest).to_dataframe()

# Default-Werte setzen
time_info = "Keine Messzeit"
condition_display = "Keine Daten"
temperature_display = "-"
feels_like_display = "-"
city = "Bamberg"

# Falls Daten vorhanden → echte Werte überschreiben
if not df_weather_latest.empty and pd.notna(df_weather_latest.loc[0, "temperature"]):
    condition = df_weather_latest.loc[0, "condition"]
    temperature = df_weather_latest.loc[0, "temperature"]
    feels_like = df_weather_latest.loc[0, "feels_like"]
    city = df_weather_latest.loc[0, "city"].capitalize()
    utc_ts = df_weather_latest.loc[0, "measurement_ts"]

    condition_display = condition.capitalize() if pd.notna(condition) else "Unbekannt"
    temperature_display = f"{temperature:.1f} °C" if pd.notna(temperature) else "Keine Daten"
    feels_like_display = f"{feels_like:.1f} °C" if pd.notna(feels_like) else "Keine Daten"
    time_info = utc_ts.strftime("%d.%m.%Y • %H Uhr")

# HTML-Block anzeigen
st.markdown(f"""
    <div style="
        background: rgba(255, 255, 255, 0.15);
        backdrop-filter: blur(12px);
        border-radius: 20px;
        padding: 24px 20px;
        margin-bottom: 16px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        text-align: center;
        color: #ffffff;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        line-height: 1;
    ">
        <div style="font-size: 14px; font-weight: 600; opacity: 0.8; margin-bottom: 4px;">
            {time_info}
        </div>
        <div style="font-size: 28px; font-weight: 600; margin-bottom: 6px;">
            {city}
        </div>
        <div style="font-size: 18px; opacity: 0.9; font-weight: 600">
            {condition_display}
        </div>
        <div style="font-size: 60px; font-weight: 700; margin-bottom: 6px;">
            {temperature_display.replace(' °C', '°')}
        </div>
        <div style="font-size: 14px; opacity: 0.8; margin-bottom: 2px;">
            Gefühlt
        </div>
        <div style="font-size: 32px; font-weight: 600; margin-bottom: 4px;">
            {feels_like_display.replace(' °C', '°')}
        </div>
    </div>
""", unsafe_allow_html=True)


# Weather today ---------------------------------------------------------------------------
query_weather_today = f"""
SELECT *
FROM `gooutside-dev.gooutside_marts.mrt_weather_today`
"""

df_weather_today = client.query(query_weather_today).to_dataframe()
df_weather_today = df_weather_today.sort_values("hour")


# Falls leer, fülle Stunden mit Platzhaltern (6 bis 20 Uhr)
if df_weather_today.empty:
    df_weather_today = pd.DataFrame({
        "hour": list(range(6, 21)),
        "condition": ["Keine Daten"] * 15,
        "temperature": ["–"] * 15
    })
else:
    df_weather_today = df_weather_today.sort_values("hour")

# HTML & CSS
html = """
<style>
.forecast-scroll {
    overflow-x: auto;
    white-space: nowrap;
    scrollbar-width: none;
    -ms-overflow-style: none;
    padding-bottom: 12px;
}
.forecast-scroll::-webkit-scrollbar {
    display: none;
}
.forecast-box {
    display: inline-block;
    background: rgba(255, 255, 255, 0.15);
    backdrop-filter: blur(12px);
    border-radius: 20px;
    padding: 20px 10px 0px 10px;
    margin-right: 8px;
    width: 100px;
    height: 120px;
    position: relative;
    text-align: center;
    color: #ffffff;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    box-shadow: 0 4px 20px rgba(0,0,0,0.1);
}
.forecast-hour {
    position: absolute;
    top: 12px;
    left: 0;
    width: 100%;
    font-size: 14px;
    font-weight: 600;
    opacity: 0.8;
}
.forecast-content {
    margin-top: 25px;
}
</style>
<div class="forecast-scroll">
"""

# Inhalt einfügen
for _, row in df_weather_today.iterrows():
    condition = str(row['condition']).capitalize()
    html += f"""
    <div class="forecast-box">
        <div class="forecast-hour">{row['hour']} Uhr</div>
        <div class="forecast-content">
            <div style="font-size: 18px; font-weight: 600;">{condition}</div>
            <div style="font-size: 32px; font-weight: 600;">{row['temperature']}°</div>
        </div>
    </div>
    """

html += "</div>"

# Anzeige in Streamlit
components.html(html, height=160)




# Weather Details ---------------------------------------------------------------------------
query_weather_details = f"""
SELECT *
FROM `gooutside-dev.gooutside_marts.mrt_weather_now`
WHERE date = '{today}' AND city = 'bamberg'
LIMIT 1
"""
df_weather_details = client.query(query_weather_details).to_dataframe()

# Defaults
visibility_display = "Keine Daten"
humidity_display = "Keine Daten"
precipitation_display = "Keine Daten"
solar_display = "Keine Daten"
sun_display = "Keine Daten"
wind_display = "Keine Daten"

if not df_weather_details.empty:
    row = df_weather_details.loc[0]

    # Sichtweite
    visibility = row.get("visibility")
    if pd.notna(visibility):
        visibility_km = visibility / 1000
        visibility_display = f"{visibility_km:.1f} km"


    # Feuchtigkeit & Taupunkt
    humidity = row.get("relative_humidity")
    dew_point = row.get("dew_point")
    if pd.notna(humidity) and pd.notna(dew_point):
        humidity_display = (
            f"{humidity:.0f} %<br>"
            f"<span style='font-size: 14px; opacity: 0.8; display: block; line-height: 1.2; margin-top: 6px;'>"
            f"Taupunkt: {dew_point:.0f} °C</span>"
    )

    # Niederschlag
    precipitation = row.get("precipitation")
    if pd.notna(precipitation):
        precipitation_display = f"{precipitation:.1f} mm"

    # Sonnenstrahlung & -dauer
    solar = row.get("solar")
    sunshine = row.get("sunshine")
    if pd.notna(solar) and pd.notna(sunshine):
        sun_text = f"{sunshine:.0f} min" if sunshine > 0 else "Keine Sonne"
        solar_display = (
            f"{sun_text}<br>"
            f"<span style='font-size: 14px; opacity: 0.8; display: block; line-height: 1.2; margin-top: 6px;'>"
            f"Dabei {solar:.1f} kWh/m²</span>"
        )

    # Sonnenauf- und untergang
    sunrise = row.get("sunrise_time")
    sunset = row.get("sunset_time")
    if pd.notna(sunrise) and pd.notna(sunset):
        sunrise_time = sunrise.strftime("%H:%M")
        sunset_time = sunset.strftime("%H:%M")
        sun_display = (
            f"{sunset_time}<br>"
            f"<span style='font-size: 14px; opacity: 0.8; display: block; line-height: 1.2; margin-top: 6px;'>"
            f"Sonnenaufgang: {sunrise_time}</span>"
        )

    # Wind
    wind_speed = row.get("wind_speed_kmh")
    wind_direction = row.get("wind_direction")
    wind_compass = row.get("wind_compass")
    if pd.notna(wind_speed) and pd.notna(wind_direction) and pd.notna(wind_compass):
        wind_display = (
            f"{wind_speed:.0f} km/h<br>"
            f"<span style='font-size: 14px; opacity: 0.8; display: block; line-height: 1.2; margin-top: 6px;'>"
            f"Richtung: {wind_direction:.0f}° {wind_compass}</span>"
        )

# Display
weather_metrics = {
    "Sichtweite": visibility_display,
    "Feuchtigkeit": humidity_display,
    "Niederschlag": precipitation_display,
    "Sonnenschein letzte Stunde": solar_display,
    "Sonnenuntergang": sun_display,
    "Windgeschwindigkeit": wind_display
}

metric_items = list(weather_metrics.items())
num_columns = 3
rows = [metric_items[i:i + num_columns] for i in range(0, len(metric_items), num_columns)]

for row in rows:
    cols = st.columns(num_columns)
    for idx, (label, value_html) in enumerate(row):
        col = cols[idx]
        col.markdown(f"""
        <div style="
            background: rgba(255, 255, 255, 0.15);
            backdrop-filter: blur(12px);
            border-radius: 20px;
            padding: 20px;
            margin-bottom: 16px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            text-align: center;
            height: 120px;
            position: relative;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            align-items: center;
            color: #ffffff;
            line-height: 1;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        ">
            <div style="
                position: absolute;
                top: 12px;
                left: 0;
                width: 100%;
                font-size: 14px;
                font-weight: 600;
                opacity: 0.8;
            ">
                {label}
            </div>
            <div style="
                margin-top: 25px;
                font-size: 32px;
                font-weight: 600;
            ">
                {value_html}
            </div>
        </div>
    """, unsafe_allow_html=True)




# Forecast: Next Days ---------------------------------------------------------------------------
st.markdown("#### 5-Tage-Wettervorhersage")

query_forecast = f"""
SELECT
    date,
    condition,
    min_temp,
    max_temp
FROM
    `gooutside-dev.gooutside_marts.mrt_weather_forecast`
WHERE
    date >= CURRENT_DATE("Europe/Berlin")
ORDER BY date
LIMIT 5
"""

df_forecast = client.query(query_forecast).to_dataframe()

# Forecast mit Fallbacks aufbauen
forecast_metrics = []

for i in range(5):
    try:
        row = df_forecast.iloc[i]
        day = pd.to_datetime(row["date"]).strftime("%a, %d.%m.")
        condition = row["condition"].capitalize()
        min_temp = f"{round(row['min_temp'])}°"
        max_temp = f"{round(row['max_temp'])}°"

        value_html = (
            f"<div style='font-size:28px; font-weight:600;'>{condition}</div>"
            f"<div style='font-size:14px; margin-top:6px;'>↓ {min_temp} • ↑ {max_temp}</div>"
        )
    except Exception:
        # Wenn kein Eintrag vorhanden ist
        day = (datetime.utcnow().date() + timedelta(days=i)).strftime("%a, %d.%m.")
        value_html = "<div style='font-size:18px; font-weight:600;'>Keine Daten</div>"

    forecast_metrics.append((day, value_html))

# Darstellung in 5 Boxen nebeneinander
num_columns = 5
rows = [forecast_metrics[i:i + num_columns] for i in range(0, len(forecast_metrics), num_columns)]

for row in rows:
    cols = st.columns(len(row))  # flexibel je nach Tagesanzahl
    for idx, (label, value_html) in enumerate(row):
        col = cols[idx]
        col.markdown(f"""
        <div style="
            background: rgba(255, 255, 255, 0.15);
            backdrop-filter: blur(12px);
            border-radius: 20px;
            padding: 20px;
            margin-bottom: 16px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            text-align: center;
            height: 120px;
            position: relative;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            align-items: center;
            color: #ffffff;
            line-height: 1;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        ">
            <div style="
                position: absolute;
                top: 12px;
                left: 0;
                width: 100%;
                font-size: 14px;
                font-weight: 600;
                opacity: 0.8;
            ">
                {label}
            </div>
            <div style="
                margin-top: 25px;
                font-size: 18px;
                font-weight: 600;
            ">
                {value_html}
            </div>
        </div>
        """, unsafe_allow_html=True)



# Air quality ---------------------------------------------------------------------------

# Darstellung
st.markdown("#### Luftqualität")

query_latest = f"""
SELECT aqi_level, measurement_ts
FROM `gooutside-dev.gooutside_marts.mrt_air_latest`
WHERE date = '{today}' AND city = 'bamberg'
LIMIT 1
"""
df_latest = client.query(query_latest).to_dataframe()

if df_latest.empty or pd.isna(df_latest.loc[0, "aqi_level"]):
    status = "Keine Daten"
    color = "#999999"
    time_info = ""
else:
    status = df_latest.loc[0, "aqi_level"].capitalize()
    color = get_color(status)

    utc_ts = df_latest.loc[0, "measurement_ts"]
    if utc_ts.tzinfo is None:
        utc_ts = utc_ts.tz_localize("UTC")
    berlin_ts = utc_ts.astimezone(pytz.timezone("Europe/Berlin"))
    time_info = berlin_ts.strftime("%H:%M Uhr")

# Empfehlungen basierend auf AQI
level_key = status.lower() if status != "Keine Daten" else "keine daten"
empfehlungen = {
    "sehr gut": [
        ("🚴", "Outdoor-Aktivitäten genießen"),
        ("🪟", "Fenster öffnen für frische Luft")
    ],
    "gut": [
        ("🚶", "Outdoor-Aktivitäten unbedenklich"),
        ("🪟", "Lüften empfohlen")
    ],
    "mäßig": [
        ("🏃‍♂️", "Anstrengung im Freien reduzieren"),
        ("😷", "Sensible Personen ggf. Maske tragen")
    ],
    "schlecht": [
        ("🏠", "Wenn möglich drinnen bleiben"),
        ("😷", "Schutzmaßnahmen für empfindliche Personen")
    ],
    "sehr schlecht": [
        ("❌", "Auf Outdoor-Aktivitäten verzichten"),
        ("💨", "Luftreiniger verwenden oder Räume abdichten")
    ],
    "keine daten": [
        ("ℹ️", "Keine aktuellen Daten verfügbar"),
        ("📉", "Bitte später erneut prüfen")
    ]
}
empfehlungspaare = empfehlungen.get(level_key, [])


# Berechne HTML für Empfehlungen
empfehlung_html = "".join([
    f"<div style='margin-bottom: 6px; font-size: 18px;'>"
    f"<span style='font-size: 18px;'>{symbol}</span> "
    f"<span style='font-weight: 500; margin-left: 6px;'>{text}</span>"
    f"</div>"
    for symbol, text in empfehlungspaare
])

# Gemeinsamer Block für AQI + Empfehlung im einheitlichen Stil
st.markdown(f"""
<div class="aqi-wrapper" style="
    display: flex; 
    flex-wrap: wrap; 
    gap: 20px;
    margin-bottom: 16px;
    justify-content: space-between;
">

  <!-- Box 1: Luftqualität -->
  <div style="
        flex: 1; 
        background: rgba(255, 255, 255, 0.15); 
        backdrop-filter: blur(12px); 
        padding: 24px 20px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        border-radius: 20px; 
        text-align: center;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        color: #ffffff;
        position: relative;
        min-width: 300px;
    ">
        <div style="
            position: absolute;
            top: 12px;
            left: 0;
            width: 100%;
            font-size: 14px;
            font-weight: 600;
            opacity: 0.8;">
            Luftqualität
        </div>
        <div style="
            margin-top: 25px;
            font-size: 32px;
            font-weight: 600;
            color: {color};">
            {status}
        </div>
        {f'<div style=\"font-size: 14px; font-weight: 500; opacity: 0.8; margin-top: 6px;\">Gemessen um {time_info}</div>' if time_info else ''}
  </div>

  <!-- Box 2: Gesundheitsempfehlung -->
  <div style="
        flex: 1; 
        background: rgba(255, 255, 255, 0.15); 
        backdrop-filter: blur(12px); 
        padding: 24px 20px;
        box-shadow: 0 4px 20px rgba(0,0,0,0.1);
        border-radius: 20px; 
        text-align: left;
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        color: #ffffff;
        min-width: 300px;
    ">
    <div style="
            position: absolute;
            top: 12px;
            left: 0;
            width: 100%;
            font-size: 14px;
            font-weight: 600;
            text-align: center;
            opacity: 0.8;">
            Gesundheitsempfehlung
        </div>
    <div style="
        display: flex; 
        flex-direction: column;
        margin-top: 25px;
        gap: 1px;">
        {empfehlung_html}
    </div>
  </div>

</div>
""", unsafe_allow_html=True)


# Feinstaub ---------------------------------------------------------------

# Farbzuordnung für Messwerte (vereinfacht)
def get_pollutant_color(value):
    if pd.isna(value):
        return "#999999"
    elif value < 20:
        return "#A9D18E"  # gut
    elif value < 40:
        return "#FFD966"  # mittel
    else:
        return "#FF6F61"  # hoch

# Daten abfragen
query_air = f"""
SELECT NO2, PM10, PM25
FROM `gooutside-dev.gooutside_marts.mrt_air_latest`
WHERE date = '{today}' AND city = 'bamberg'
LIMIT 1
"""

df_air = client.query(query_air).to_dataframe()

# Falls keine Daten: leeres Dict mit None-Werten
if df_air.empty:
    air_values = {"NO₂": None, "PM10": None, "PM2.5": None}
else:
    row = df_air.iloc[0]
    air_values = {
        "NO₂": row.get("NO2"),
        "PM10": row.get("PM10"),
        "PM2.5": row.get("PM25"),
    }


cols = st.columns(3)
for i, (label, value) in enumerate(air_values.items()):
    color = get_pollutant_color(value)
    if pd.notna(value):
        value_display = f"<span>{int(value)} µg/m³</span>"
    else:
        value_display = "Keine Daten"

    cols[i].markdown(f"""
        <div style="
            background: rgba(255, 255, 255, 0.15);
            backdrop-filter: blur(12px);
            border-radius: 20px;
            padding: 20px;
            margin-bottom: 16px;
            box-shadow: 0 4px 20px rgba(0,0,0,0.1);
            text-align: center;
            height: 120px;
            position: relative;
            display: flex;
            flex-direction: column;
            justify-content: flex-start;
            align-items: center;
            color: #ffffff;
            line-height: 1;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
        ">
        <div style="
            position: absolute;
            top: 12px;
            left: 0;
            width: 100%;
            font-size: 14px;
            font-weight: 600;
            opacity: 0.8;
        ">{label}
          </div>
            <div style="
                margin-top: 25px;
                font-size: 28px;
                font-weight: 600;
            ">
                {value_display}
            </div>
        </div>
    """, unsafe_allow_html=True)           


# Pollen ---------------------------------------------------------------------------
st.markdown('#### Pollenflugvorhersage')

# BigQuery-Abfrage
client = bigquery.Client(project="gooutside-dev")
query = f"""
SELECT
    Esche, Graeser, Beifuss, Birke, Ambrosia, Erle, Hasel, Roggen
FROM
    `gooutside-dev.gooutside_marts.mrt_pollen`
WHERE
    date = '{today}' AND city = 'bamberg'
LIMIT 1
"""
df = client.query(query).to_dataframe()

# Wenn leer: leere Werte, sonst erste Zeile nehmen
if df.empty:
    pollen_series = pd.Series({
        "Esche": None,
        "Graeser": None,
        "Beifuss": None,
        "Birke": None,
        "Ambrosia": None,
        "Erle": None,
        "Hasel": None,
        "Roggen": None
    })
else:
    pollen_series = df.iloc[0]

# Layout: 4 Karten pro Reihe
pollen_items = list(pollen_series.items())
num_columns = 4
rows = [pollen_items[i:i + num_columns] for i in range(0, len(pollen_items), num_columns)]

for row in rows:
    cols = st.columns(num_columns)
    for idx, (pollenart, belastung) in enumerate(row):
        col = cols[idx]

        if pd.isna(belastung):
            value_html = '<span style="color: #999999;">Keine Daten</span>'
        else:
            value_html = f'<span style="font-weight: 600;">{belastung.capitalize()}</span>'

        col.markdown(f"""
            <div style="
                background: rgba(255, 255, 255, 0.15);
                backdrop-filter: blur(12px);
                border-radius: 20px;
                padding: 20px;
                margin-bottom: 16px;
                box-shadow: 0 4px 20px rgba(0,0,0,0.1);
                text-align: center;
                height: 120px;
                position: relative;
                display: flex;
                flex-direction: column;
                justify-content: flex-start;
                align-items: center;
                color: #ffffff;
                line-height: 1;
                font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
            ">
            <div style="
                position: absolute;
                top: 12px;
                left: 0;
                width: 100%;
                font-size: 14px;
                font-weight: 600;
                opacity: 0.8;
            ">
                {pollenart}
            </div>
            <div style="
                margin-top: 25px;
                font-size: 28px;
                font-weight: 600;
            ">
                {value_html}
            </div>
        </div>
    """, unsafe_allow_html=True)

# Notes ---------------------------------------------------------------------------
st.markdown("""
<div style="
    background: rgba(255, 255, 255, 0.1);
    backdrop-filter: blur(12px);
    box-shadow: 0 4px 20px rgba(0,0,0,0.1);
    padding: 12px 20px;
    border-radius: 15px;
    margin-top: 24px;
    font-size: 13px;
    color: rgba(255, 255, 255, 0.75);
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
    line-height: 1.4;
">
    <div style="
        font-size: 14px;
        font-weight: 600;
        opacity: 0.8;
        "> Quellen: 
    </div>
    <div style="
        font-size: 12px;
        font-weight: 600;
        ">
        Deutscher Wetterdienst (DWD) <br>
        Umweltbundesamt<br>
        Bright Sky
    </div>
</div>
""", unsafe_allow_html=True)



