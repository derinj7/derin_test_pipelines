"""
Weather Reporting – Airflow 2
Pure-SQL DAG that queries analytics data and pushes results to XCom.

Trigger: DATASET  snowflake://SAMPLE_DB/ANALYTICS/WEATHER_ANALYTICS   (produced by the analytics DAG)
"""

from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.datasets import Dataset
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.dummy import DummyOperator

# ------------------------------------------------------------------ #
# Dataset that the analytics DAG publishes                           #
# ------------------------------------------------------------------ #
weather_analytics_dataset = Dataset("snowflake://SAMPLE_DB/ANALYTICS/WEATHER_ANALYTICS")

# ------------------------------------------------------------------ #
# SQL strings                                                        #
# ------------------------------------------------------------------ #
TEMP_RANKING_SQL = """
SELECT city,
       country,
       ROUND(avg_temperature, 1)            AS avg_temp_c,
       ROUND(avg_temperature*9/5+32, 1)     AS avg_temp_f,
       ROUND(avg_windspeed, 1)              AS avg_wind_kmh,
       climate_zone
FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
WHERE analysis_date = CURRENT_DATE()
ORDER BY avg_temperature DESC
LIMIT 10;
"""

EXTREMES_SQL = """
(SELECT 'Hottest City' AS category,
        city,
        country,
        ROUND(max_temperature, 1)        AS temperature_c,
        ROUND(max_temperature*9/5+32,1) AS temperature_f
 FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
 WHERE analysis_date = CURRENT_DATE()
 ORDER BY max_temperature DESC
 LIMIT 1)
UNION ALL
(SELECT 'Coldest City' AS category,
        city,
        country,
        ROUND(min_temperature, 1)        AS temperature_c,
        ROUND(min_temperature*9/5+32,1) AS temperature_f
 FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
 WHERE analysis_date = CURRENT_DATE()
 ORDER BY min_temperature ASC
 LIMIT 1);
"""

CLIMATE_SQL = """
SELECT climate_zone,
       COUNT(DISTINCT city)           AS city_count,
       ROUND(AVG(avg_temperature),1)  AS avg_temp,
       ROUND(AVG(comfort_score),1)    AS avg_comfort
FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
WHERE analysis_date = CURRENT_DATE()
GROUP BY climate_zone
ORDER BY city_count DESC;
"""

WIND_SQL = """
(SELECT 'Windiest City' AS category,
        city,
        country,
        ROUND(max_windspeed,1) AS wind_speed_kmh
 FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
 WHERE analysis_date = CURRENT_DATE()
 ORDER BY max_windspeed DESC
 LIMIT 1)
UNION ALL
(SELECT 'Calmest City'  AS category,
        city,
        country,
        ROUND(avg_windspeed,1) AS wind_speed_kmh
 FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
 WHERE analysis_date = CURRENT_DATE()
 ORDER BY avg_windspeed ASC
 LIMIT 1);
"""

VARIANCE_SQL = """
SELECT city,
       country,
       ROUND(COALESCE(temperature_variance,0),2) AS temp_variance,
       ROUND(avg_temperature,1)                  AS avg_temp,
       CASE
         WHEN temperature_variance > 5 THEN 'High Variability'
         WHEN temperature_variance > 2 THEN 'Moderate Variability'
         WHEN temperature_variance IS NULL THEN 'No Data'
         ELSE 'Low Variability'
       END AS variance_category
FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
WHERE analysis_date = CURRENT_DATE()
  AND temperature_variance IS NOT NULL
ORDER BY temperature_variance DESC
LIMIT 5;
"""

SUMMARY_SQL = """
SELECT COUNT(DISTINCT city)         AS total_cities,
       COUNT(DISTINCT country)      AS total_countries,
       COUNT(DISTINCT climate_zone) AS climate_zones,
       SUM(record_count)            AS total_records,
       ROUND(AVG(avg_temperature),1)AS global_avg_temp_c,
       ROUND(MIN(min_temperature),1)AS global_min_temp,
       ROUND(MAX(max_temperature),1)AS global_max_temp,
       ROUND(AVG(comfort_score),1)  AS avg_comfort_score
FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
WHERE analysis_date = CURRENT_DATE();
"""

COMFORT_SQL = """
SELECT city,
       country,
       ROUND(comfort_score,1)       AS comfort,
       ROUND(avg_temperature,1)     AS avg_temp,
       ROUND(avg_windspeed,1)       AS avg_wind
FROM SAMPLE_DB.ANALYTICS.WEATHER_ANALYTICS
WHERE analysis_date = CURRENT_DATE()
ORDER BY comfort_score DESC
LIMIT 5;
"""

# ------------------------------------------------------------------ #
# DAG – one SQLExecuteQueryOperator per query                        #
# ------------------------------------------------------------------ #
with DAG(
    dag_id="weather_reporting_dag",
    description="Pure-SQL reporting on weather analytics data",
    start_date=datetime(2024, 1, 1),
    schedule=[weather_analytics_dataset],   # data-aware trigger
    catchup=False,
    default_args={"retries": 0, "email_on_failure": False},
    tags=["weather", "reporting", "snowflake"],
) as dag:

    temp_ranking   = SQLExecuteQueryOperator(
        task_id="temp_ranking",
        conn_id="snowflake_default",
        sql=TEMP_RANKING_SQL,
        do_xcom_push=True,
    )

    extremes       = SQLExecuteQueryOperator(
        task_id="temperature_extremes",
        conn_id="snowflake_default",
        sql=EXTREMES_SQL,
        do_xcom_push=True,
    )

    climate_dist   = SQLExecuteQueryOperator(
        task_id="climate_zone_distribution",
        conn_id="snowflake_default",
        sql=CLIMATE_SQL,
        do_xcom_push=True,
    )

    wind_patterns  = SQLExecuteQueryOperator(
        task_id="wind_patterns",
        conn_id="snowflake_default",
        sql=WIND_SQL,
        do_xcom_push=True,
    )

    variance_top5  = SQLExecuteQueryOperator(
        task_id="variance_top5",
        conn_id="snowflake_default",
        sql=VARIANCE_SQL,
        do_xcom_push=True,
    )

    summary_stats  = SQLExecuteQueryOperator(
        task_id="summary_statistics",
        conn_id="snowflake_default",
        sql=SUMMARY_SQL,
        do_xcom_push=True,
    )

    comfort_top5   = SQLExecuteQueryOperator(
        task_id="comfort_top5",
        conn_id="snowflake_default",
        sql=COMFORT_SQL,
        do_xcom_push=True,
    )

    # Add a final completion task (no outlet needed as this is the last DAG)
    complete = DummyOperator(
        task_id="reporting_complete"
    )

    # Dependencies
    [temp_ranking, extremes, climate_dist, wind_patterns,
     variance_top5, comfort_top5] >> summary_stats >> complete