"""
Weather Analytics / Transformation – Airflow 2 (Dataset-aware)
Consumes `weather_raw_dataset`, produces `weather_analytics_dataset`.
"""

from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

# ------------------------------------------------------------------ #
# 0.  Dataset references                                             #
# ------------------------------------------------------------------ #
weather_raw_dataset      = Dataset("snowflake://SAMPLE_DB/ANALYTICS/WEATHER_RAW_DATA")
weather_analytics_dataset = Dataset("snowflake://SAMPLE_DB/ANALYTICS/WEATHER_ANALYTICS")

# ------------------------------------------------------------------ #
# 1.  Static SQL                                                     #
# ------------------------------------------------------------------ #
CREATE_ANALYTICS_SQL = """
CREATE OR REPLACE TABLE SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS (
  analysis_date        DATE,
  city                 VARCHAR(100),
  country              VARCHAR(100),
  latitude             FLOAT,
  longitude            FLOAT,
  avg_temperature      FLOAT,
  min_temperature      FLOAT,
  max_temperature      FLOAT,
  temperature_variance FLOAT,
  avg_windspeed        FLOAT,
  max_windspeed        FLOAT,
  comfort_score        FLOAT,
  climate_zone         VARCHAR(50),
  record_count         INT,
  processing_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
"""

TRANSFORMATION_SQL = """
/* identical to Asset 2's heavy insert */
INSERT INTO SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
WITH RECURSIVE
  number_gen AS (
    SELECT 1 AS n
    UNION ALL
    SELECT n + 1 FROM number_gen WHERE n < 20000
  ),
  compute_load AS (
    SELECT n1.n AS num1,
           n2.n AS num2,
           SIN(n1.n*0.001)*COS(n2.n*0.001) AS calc_value
    FROM (SELECT n FROM number_gen WHERE n <= 1000) n1
    CROSS JOIN (SELECT n FROM number_gen WHERE n <= 20)  n2
  ),
  calc_summary AS (
    SELECT SUM(calc_value) AS total_calc,
           AVG(calc_value) AS avg_calc
    FROM compute_load
  ),
  weather_summary AS (
    SELECT
      city,
      country,
      latitude,
      longitude,
      AVG(temperature)      AS avg_temperature,
      MIN(temperature)      AS min_temperature,
      MAX(temperature)      AS max_temperature,
      VARIANCE(temperature) AS temperature_variance,
      AVG(windspeed)        AS avg_windspeed,
      MAX(windspeed)        AS max_windspeed,
      AVG(comfort_index)    AS comfort_score,
      CASE
        WHEN ABS(latitude) > 60  THEN 'Polar'
        WHEN ABS(latitude) > 40  THEN 'Temperate'
        WHEN ABS(latitude) > 23.5 THEN 'Subtropical'
        ELSE 'Tropical'
      END                   AS climate_zone,
      COUNT(*)              AS record_count
    FROM SAMPLE_DB.ANALYTICS.WEATHER_RAW_DATA
    WHERE ingestion_date >= DATEADD(day, -7, CURRENT_DATE())
    GROUP BY city, country, latitude, longitude
  )
SELECT
  CURRENT_DATE()                         AS analysis_date,
  w.city,
  w.country,
  w.latitude,
  w.longitude,
  w.avg_temperature + (c.avg_calc * 0.00001)  AS avg_temperature,
  w.min_temperature,
  w.max_temperature,
  w.temperature_variance,
  w.avg_windspeed,
  w.max_windspeed,
  w.comfort_score  + (c.total_calc*0.00001)    AS comfort_score,
  w.climate_zone,
  w.record_count,
  CURRENT_TIMESTAMP()                     AS processing_timestamp
FROM weather_summary w, calc_summary c;
"""

HEAVY_FIBONACCI_SQL = """
WITH RECURSIVE fibonacci AS (
  SELECT 0 AS n, 0 AS fib_n, 1 AS fib_n_plus_1
  UNION ALL
  SELECT n+1, fib_n_plus_1, fib_n + fib_n_plus_1
  FROM fibonacci
  WHERE n < 45
),
city_fibonacci AS (
  SELECT
    w.city,
    w.avg_temperature,
    f.n,
    f.fib_n,
    w.avg_temperature*f.fib_n AS temp_fib_product
  FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS w
  CROSS JOIN fibonacci f
)
SELECT
  city,
  COUNT(*)        AS calculations,
  SUM(temp_fib_product) AS total_product,
  AVG(temp_fib_product) AS avg_product
FROM city_fibonacci
GROUP BY city;
"""

SUMMARY_SQL = """
SELECT
  COUNT(DISTINCT city) AS cities_analyzed,
  AVG(avg_temperature) AS global_avg_temp,
  MIN(min_temperature) AS global_min_temp,
  MAX(max_temperature) AS global_max_temp,
  SUM(record_count)    AS total_records
FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
WHERE analysis_date = CURRENT_DATE();
"""

# ------------------------------------------------------------------ #
# 2.  DAG                                                            #
# ------------------------------------------------------------------ #
with DAG(
    dag_id="weather_analytics_dag",
    start_date=datetime(2024, 1, 1),
    schedule=[weather_raw_dataset],        # data-aware trigger
    catchup=False,
    tags=["weather", "snowflake"],
) as dag:

    create_table = SQLExecuteQueryOperator(
        task_id="create_weather_analytics_table",
        conn_id="snowflake_default",
        sql=CREATE_ANALYTICS_SQL,
        outlets=[weather_analytics_dataset],
    )

    transform = SQLExecuteQueryOperator(
        task_id="weather_transformation",
        conn_id="snowflake_default",
        sql=TRANSFORMATION_SQL,
        outlets=[weather_analytics_dataset],
    )

    heavy_compute = SQLExecuteQueryOperator(
        task_id="fibonacci_weather_computation",
        conn_id="snowflake_default",
        sql=HEAVY_FIBONACCI_SQL,
    )

    summary_stats = SQLExecuteQueryOperator(
        task_id="weather_summary_stats",
        conn_id="snowflake_default",
        sql=SUMMARY_SQL,
    )

    create_table >> transform >> [heavy_compute, summary_stats]
