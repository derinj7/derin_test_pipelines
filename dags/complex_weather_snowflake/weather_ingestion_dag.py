"""
Weather Data Ingestion – Airflow 2 (Dataset-based, SQL-only)
Runs hourly and publishes `weather_raw_dataset`.
"""

from __future__ import annotations

from datetime import datetime
import requests

from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ------------------------------------------------------------------ #
# 0.  Dataset reference                                              #
# ------------------------------------------------------------------ #
weather_raw_dataset = Dataset("snowflake://SAMPLE_DB/ANALYTICS/WEATHER_RAW_DATA")

# ------------------------------------------------------------------ #
# 1.  Small helper to pull the weather once per DAG-parse            #
# ------------------------------------------------------------------ #
def _fetch_weather() -> list[dict]:
    """Hit Open-Meteo, return list of dicts suitable for Snowflake insert."""
    cities = [
        "New York", "London", "Tokyo", "Paris", "Sydney",
        "Mumbai", "Singapore", "Dubai", "Toronto", "Berlin",
        "Moscow", "Beijing", "Seoul", "Bangkok", "Cairo",
        "Istanbul", "Rome", "Madrid", "Amsterdam", "Vienna",
    ]
    rows: list[dict] = []
    for city in cities:
        try:
            geo = requests.get(
                f"https://geocoding-api.open-meteo.com/v1/search?name={city}&count=1",
                timeout=20,
            ).json()
            if geo.get("results"):
                loc = geo["results"][0]
                lat, lon = loc["latitude"], loc["longitude"]
                wx = requests.get(
                    f"https://api.open-meteo.com/v1/forecast"
                    f"?latitude={lat}&longitude={lon}&current_weather=true",
                    timeout=20,
                ).json().get("current_weather", {})
                rows.append(
                    {
                        "city": city,
                        "country": loc.get("country", "Unknown"),
                        "latitude": lat,
                        "longitude": lon,
                        "temperature": wx.get("temperature", 0),
                        "windspeed": wx.get("windspeed", 0),
                        "winddirection": wx.get("winddirection", 0),
                        "weathercode": wx.get("weathercode", 0),
                        "timestamp": datetime.utcnow().isoformat(),
                        "ingestion_date": datetime.utcnow().date().isoformat(),
                    }
                )
        except Exception as exc:  # noqa: BLE001
            print(f"Weather fetch failed for {city}: {exc}")
    return rows


weather_rows = _fetch_weather()               # <-- used for dynamic mapping

# ------------------------------------------------------------------ #
# 2.  Static SQL strings                                             #
# ------------------------------------------------------------------ #
CREATE_TABLE_SQL = """
CREATE OR REPLACE TABLE SAMPLE_DB.ANALYTICS.WEATHER_RAW_DATA (
  city            VARCHAR(100),
  country         VARCHAR(100),
  latitude        FLOAT,
  longitude       FLOAT,
  temperature     FLOAT,
  windspeed       FLOAT,
  winddirection   FLOAT,
  weathercode     INT,
  timestamp       TIMESTAMP_NTZ,
  ingestion_date  DATE,
  load_timestamp  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
  temp_fahrenheit FLOAT,
  temp_kelvin     FLOAT,
  wind_category   VARCHAR(50),
  comfort_index   FLOAT
);
"""

INSERT_SQL = """
/* parameterised heavy insert – see original Asset 1 */
INSERT INTO SAMPLE_DB.ANALYTICS.WEATHER_RAW_DATA (
  city, country, latitude, longitude, temperature, windspeed,
  winddirection, weathercode, timestamp, ingestion_date,
  temp_fahrenheit, temp_kelvin, wind_category, comfort_index
)
WITH RECURSIVE
  number_generator AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM number_generator WHERE n < 10000
  ),
  complexity_multiplier AS (
    SELECT n1.n AS n1, n2.n AS n2, n3.n AS n3,
           SIN(n1.n)*COS(n2.n)*TAN(n3.n/100.0) AS calc_value
    FROM number_generator n1
    CROSS JOIN (SELECT n FROM number_generator WHERE n <= 100) n2
    CROSS JOIN (SELECT n FROM number_generator WHERE n <= 10)  n3
  ),
  complex_calc AS (
    SELECT SUM(calc_value) AS total_calc,
           AVG(calc_value) AS avg_calc,
           STDDEV(calc_value) AS std_calc
    FROM complexity_multiplier
  ),
  weather_staging AS (
    SELECT
      %(city)s            AS city,
      %(country)s         AS country,
      %(latitude)s::FLOAT AS latitude,
      %(longitude)s::FLOAT AS longitude,
      %(temperature)s::FLOAT AS temperature,
      %(windspeed)s::FLOAT AS windspeed,
      %(winddirection)s::FLOAT AS winddirection,
      %(weathercode)s::INT AS weathercode,
      %(timestamp)s::TIMESTAMP_NTZ AS timestamp,
      %(ingestion_date)s::DATE AS ingestion_date
  )
SELECT
  ws.*,
  (ws.temperature*9/5+32) + (cc.total_calc*0.00001)             AS temp_fahrenheit,
  (ws.temperature+273.15) + (cc.avg_calc *0.00001)              AS temp_kelvin,
  CASE
    WHEN ws.windspeed < 5  THEN 'Calm'
    WHEN ws.windspeed < 15 THEN 'Light'
    WHEN ws.windspeed < 25 THEN 'Moderate'
    WHEN ws.windspeed < 35 THEN 'Strong'
    ELSE 'Very Strong'
  END                                                           AS wind_category,
  (100 - ABS(ws.temperature-22)*2 - ws.windspeed*0.5
         + SIN(ws.latitude*PI()/180)*10 + cc.std_calc*0.00001)  AS comfort_index
FROM weather_staging ws, complex_calc cc;
"""

# ------------------------------------------------------------------ #
# 3.  DAG – all SQLExecuteQueryOperator                              #
# ------------------------------------------------------------------ #
with DAG(
    dag_id="weather_ingestion_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["weather", "snowflake"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_weather_table",
        conn_id="snowflake_default",
        sql=CREATE_TABLE_SQL,
        # Removed outlet from here
    )

    # Single task to insert all weather records
    insert_all_weather = SQLExecuteQueryOperator(
        task_id="insert_all_weather_records",
        conn_id="snowflake_default",
        sql=[INSERT_SQL] * len(weather_rows),  # Execute INSERT for each city
        parameters=weather_rows,  # Pass all parameters at once
        outlets=[weather_raw_dataset],  # Only emit dataset once after all inserts
    )

    create_table >> insert_all_weather