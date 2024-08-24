import requests
from datetime import datetime
import pandas as pd
from io import StringIO
import duckdb
from utils.noaa_utils import get_db_conn
from airflow import DAG
from airflow.operators.python import PythonOperator


column_specs = [
  (0, 11),
  (12, 20),
  (21, 30),
  (31, 37),
  (38, 40),
  (41, 71),
  (72, 75),
  (76, 79),
  (80, 85)
]

column_names = [
  'ID',
  'LATITUDE',
  'LONGITUDE',
  'ELEVATION',
  'STATE',
  'NAME',
  'GSN_FLAG',
  'HCN_CRN_FLAG',
  'WMO_ID'
]


def noaa_stations() -> None:

  response = requests.get('https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt', verify=False)
  stations_file = StringIO(response.text)

  df = pd.read_fwf(stations_file, colspecs=column_specs, names=column_names)
  df = df.astype({'WMO_ID': 'Int64'})  # this integer type is necessary to allow nulls

  stations_file.close()  # explicitly close in-memory file
  print(df.head())  # TODO: Replace this with logging
  print('\n\n', df.info())

  duckdb.execute(get_db_conn())
  duckdb.execute("""
    INSERT INTO climate_viewer.lkp.stations (
      stationid
      ,latitude
      ,longitude
      ,elevation
      ,state_code
      ,station_name
      ,gsn_flag
      ,hcn_crn_flag
      ,wmo_id
    )
    SELECT
      ID,
      LATITUDE,
      LONGITUDE,
      ELEVATION,
      STATE,
      NAME,
      GSN_FLAG,
      HCN_CRN_FLAG,
      WMO_ID
    FROM
      df
  """)


with DAG(
  dag_id='noaa_stations_dag',
  start_date=datetime(2024, 1, 1),
  schedule=None,  # TODO: Learn chron syntax and set to run monthly
  is_paused_upon_creation=True,
  catchup=False,
  tags=['noaa']
) as dag:
  
  noaa_stations_load = PythonOperator(
    task_id='noaa_stations_load',
    python_callable=noaa_stations,
    provide_context=True
  )


# IV. FORMAT OF "ghcnd-stations.txt"

# ------------------------------
# Variable   Columns   Type
# ------------------------------
# ID            1-11   Character
# LATITUDE     13-20   Real
# LONGITUDE    22-30   Real
# ELEVATION    32-37   Real
# STATE        39-40   Character
# NAME         42-71   Character
# GSN FLAG     73-75   Character
# HCN/CRN FLAG 77-79   Character
# WMO ID       81-85   Character
# ------------------------------