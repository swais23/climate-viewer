import duckdb
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.noaa_utils import get_noaa_url, get_db_conn
from queries.noaa_raw import noaa_raw_query

logger = logging.getLogger(__name__)

params = {
    'start_date': '2024-01-01',
    'end_date': '2024-01-01' 
} # Default values indicate expected date format


def noaa_backfill_raw(query: str, **kwargs) -> None:

  duckdb.execute(get_db_conn())

  start_date = datetime.strptime(kwargs['params']['start_date'], '%Y-%m-%d')
  end_date = datetime.strptime(kwargs['params']['end_date'], '%Y-%m-%d')

  for year in range(start_date.year, end_date.year + 1):

    noaa_url = get_noaa_url(year)

    query_interval_start = max(start_date, datetime(year, 1, 1)).strftime('%Y%m%d')
    query_interval_end = min(end_date, datetime(year, 12, 31)).strftime('%Y%m%d')

    formatted_query = query.format(
      noaa_url=noaa_url,
      start_date=query_interval_start,
      end_date=query_interval_end
    )

    logging.info(f'Backfilling range {query_interval_start} - {query_interval_end}.')

    duckdb.execute(formatted_query)


with DAG(
    dag_id='noaa_backfill_dag',
    start_date=datetime(2024, 1, 1),
    schedule=None,
    is_paused_upon_creation=True,
    catchup=False,
    tags=['noaa_raw', 'backfill'],
    params=params
) as dag:

    noaa_raw_load = PythonOperator(
      task_id='noaa_raw_load',
      python_callable=noaa_backfill_raw,
      provide_context=True,
      op_args=[noaa_raw_query]
    )
