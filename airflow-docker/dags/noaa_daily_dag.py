import duckdb
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from utils.noaa_utils import get_noaa_url, get_db_conn, get_noaa_pivot_case_statements
from queries.noaa_raw import noaa_raw_query
from queries.noaa_pivot_query import noaa_pivot_query
from config import NOAA_ELEMENTS


logger = logging.getLogger('airflow.task')


def noaa_daily_raw(query: str, **kwargs) -> None:

  duckdb.execute(get_db_conn())

  execution_date = kwargs['ds_nodash']
  execution_year = datetime.strptime(execution_date, '%Y%m%d').year

  noaa_url = get_noaa_url(execution_year)
  logger.info(f'NOAA url: {noaa_url}')

  formatted_query = query.format(
    noaa_url=noaa_url,
    start_date=execution_date,
    end_date=execution_date
  )

  logger.info(f'Loading data for {execution_date}')
  logger.info(f'Query to execute: \n {formatted_query}')

  logger.info(f'Query start time: {datetime.now()}')
  duckdb.execute(formatted_query)
  logger.info(f'Query end time: {datetime.now()}')


pivot_query = noaa_pivot_query.format(
  noaa_pivot_case_statements=get_noaa_pivot_case_statements(NOAA_ELEMENTS),
  column_list=', '.join([element[1] for element in NOAA_ELEMENTS]),
  start_date='{{ ds }}',
  end_date='{{ ds }}'
)


with DAG(
    dag_id='noaa_daily_dag',
    start_date=datetime(2024, 1, 1),
    schedule='0 0 * * *', # TODO: Determine a logical schedule
    is_paused_upon_creation=True,
    catchup=False,
    tags=['noaa']
) as dag:
    
    noaa_raw_load = PythonOperator(
      task_id='noaa_raw_load',
      python_callable=noaa_daily_raw,
      provide_context=True,
      op_args=[noaa_raw_query]
    )

    noaa_pivot_data = SQLExecuteQueryOperator(
      task_id='noaa_pivot_data',
      conn_id='postgres',
      sql=pivot_query
    )

    noaa_raw_load >> noaa_pivot_data
