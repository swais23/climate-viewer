import requests
from datetime import datetime
import pandas as pd
from io import StringIO
import duckdb
from utils.noaa_utils import get_db_conn
from airflow import DAG
from airflow.operators.python import PythonOperator
from queries.noaa_stations_query import noaa_stations_query
from config import NOAA_STATIONS_FILE_URL, NOAA_STATIONS_FILE_DEFINITION
import logging


logger = logging.getLogger('airflow.task')


def noaa_stations() -> None:

  logger.info(f"Pulling NOAA station lookup data from {NOAA_STATIONS_FILE_URL}")
  # Pull text content from .txt file at specified URL
  response = requests.get(NOAA_STATIONS_FILE_URL, verify=False)
  # Place into in-memory file so that Pandas fixed-width file (fwf) function can read it
  stations_file = StringIO(response.text)

  column_specs = [
    (station_column.start_position, station_column.end_position) 
    for station_column in NOAA_STATIONS_FILE_DEFINITION
  ]
  column_names = [station_column.column_name for station_column in NOAA_STATIONS_FILE_DEFINITION]

  # load fixed-width file into Pandas dataframe, which DuckDB will then read
  df = pd.read_fwf(stations_file, colspecs=column_specs, names=column_names)
  # df = df.astype({'WMO_ID': 'Int64'})  # this integer type is necessary to allow nulls

  # explicitly close in-memory file
  stations_file.close()

  # Format query with column names from config file
  stations_query = noaa_stations_query.format(
    station_columns=', '.join(column_names)
  )

  duckdb.execute(get_db_conn())

  # Explicitly log query text since DAG is not using SQL Operator, which logs query automatically
  logger.info(f'Query to execute: \n {stations_query}')
  logger.info(f'Query start time: {datetime.now()}')
  duckdb.execute(stations_query)
  logger.info(f'Query end time: {datetime.now()}')


with DAG(
  dag_id='noaa_stations_dag',
  start_date=datetime(2024, 1, 1),
  schedule='0 0 1 * *',  # Once a month at midnight on the 1st day of the month
  is_paused_upon_creation=True,
  catchup=False,
  tags=['noaa']
) as dag:
  
  noaa_stations_load = PythonOperator(
    task_id='noaa_stations_load',
    python_callable=noaa_stations,
    provide_context=True
  )
