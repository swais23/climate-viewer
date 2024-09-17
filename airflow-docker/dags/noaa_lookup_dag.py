import requests
import pandas as pd
import duckdb
import logging
from datetime import datetime
from io import StringIO
from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from queries.noaa_raw_sensor import sensor_query
from queries.noaa_lookup_checks import stations_freshness, countries_freshness, states_freshness
from queries.noaa_lookup import noaa_lookup_query
from utils.noaa_utils import get_db_conn
from config import NOAA_LOOKUP_CONFIG


logger = logging.getLogger('airflow.task')


def freshness_check(table: str, query: str, **kwargs) -> str:

  duckdb.execute(get_db_conn())

  if table != 'states':
    query = query.format(execution_date=kwargs['ds'])

  logger.info(f'Checking freshness of {table} for {kwargs['ds']}')
  logger.info(f'Query to execute: \n {query}')

  logger.info(f'Query start time: {datetime.now()}')
  query_results = duckdb.execute(query).fetchall()
  logger.info(f'Query end time: {datetime.now()}')

  logger.info(f'Count of missing {table}: {len(query_results):,}')

  if len(query_results) > 0:
     return f'{table}_group.{table}_lookup_load'
  
  else:
    return f'{table}_group.{table}_nothing_to_load'


def noaa_lookup(lookup_config) -> None:

  logger.info(f'Pulling NOAA {lookup_config.table_name} lookup data from {lookup_config.file_url}')
  # Pull text content from .txt file at specified URL
  response = requests.get(lookup_config.file_url, verify=False)
  # Place into in-memory file so that Pandas fixed-width file (fwf) function can read it
  file = StringIO(response.text)

  column_specs = [
    (column.start_position, column.end_position) 
    for column in lookup_config.column_definition
  ]
  column_names = [column.column_name for column in lookup_config.column_definition]

  # Load fixed-width file into Pandas dataframe, which DuckDB will then read
  df = pd.read_fwf(file, colspecs=column_specs, names=column_names)

  file.close()

  formatted_query = noaa_lookup_query.format(
    table=lookup_config.table_name,
    columns=', '.join(column_names)
  )

  duckdb.execute(get_db_conn())

  # Explicitly log query text since DAG is not using SQL Operator, which logs query automatically
  logger.info(f'Query to execute: \n {formatted_query}')
  logger.info(f'Query start time: {datetime.now()}')
  duckdb.execute(formatted_query)
  logger.info(f'Query end time: {datetime.now()}')


with DAG(
  dag_id='noaa_lookup_dag',
  start_date=datetime(2024, 1, 1),
  schedule='0 0 * * *',  # TODO: Determine a logical schedule
  is_paused_upon_creation=True,
  catchup=False,
  tags=['noaa']
) as dag:

    noaa_raw_sensor = SqlSensor(
      task_id='noaa_raw_sensor',
      conn_id='postgres',
      sql=sensor_query.format(execution_date='{{ ds }}'),
      mode='reschedule',
      poke_interval=60 * 2,  # poke every 2 minutes
      timeout=60 * 60  # timeout after 1 hour
    )

    lookup_groups = {}
    freshness_queries = {
      'stations': stations_freshness, 
      'countries': countries_freshness, 
      'states': states_freshness
    }

    for config in NOAA_LOOKUP_CONFIG:

      table = config.table_name
      freshness_query = freshness_queries[table]
       
      with TaskGroup(f'{table}_group', tooltip=f'{table}_group') as lookup_group:

        check_freshness = BranchPythonOperator(
          task_id=f'{table}_check_freshness',
          python_callable=freshness_check,
          provide_context=True,
          op_args=[table, freshness_query],
          trigger_rule=TriggerRule.NONE_FAILED
        )

        nothing_to_load = DummyOperator(
          task_id=f'{table}_nothing_to_load'
        )

        lookup_load = PythonOperator(
          task_id=f'{table}_lookup_load',
          python_callable=noaa_lookup,
          op_args=[config]
        )

        check_freshness >> [nothing_to_load, lookup_load]
        lookup_groups[table] = lookup_group

    noaa_raw_sensor >> lookup_groups['stations'] >> lookup_groups['states']
    noaa_raw_sensor >> lookup_groups['countries']
  