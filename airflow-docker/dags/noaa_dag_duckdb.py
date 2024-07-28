import os
import csv
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import duckdb


config = {**os.environ}


def get_data_duckdb(**kwargs):
  noaa_url = f"https://noaa-ghcn-pds.s3.amazonaws.com/csv/by_year/{kwargs['year']}.csv"

  duckdb.execute(f"""
    ATTACH 'dbname={config["PG_DBNAME"]} user={config["PG_USERNAME"]} password={config["PG_PASSWORD"]} host=postgres_db port={config["PG_PORT"]}' AS postgres_db (TYPE POSTGRES);
                 
    INSERT INTO postgres_db.noaa_daily_raw (
      stationid, 
      noaa_date, 
      element, 
      data_value, 
      m_flag, 
      q_flag, 
      s_flag, 
      obs_time            
    )
    SELECT
      ID,
      DATE,
      ELEMENT,
      DATA_VALUE,
      M_FLAG,
      Q_FLAG,
      S_FLAG,
      OBS_TIME
    FROM '{noaa_url}'
    WHERE
      Date = '{kwargs['ds_nodash']}'
  """
  )


with DAG(
    dag_id="noaa_duckdb_dag",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    is_paused_upon_creation=True,
    catchup=False,
    tags=['noaa']
) as dag:
    get_daily_data = PythonOperator(
      task_id="get_daily_data",
      python_callable=get_data_duckdb,
      provide_context=True,
      op_kwargs={'year': 2024}
    )