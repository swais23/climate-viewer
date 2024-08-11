import os
from typing import List, Tuple


env = {**os.environ}


def get_noaa_url(year: int) -> str:

  url = f'https://noaa-ghcn-pds.s3.amazonaws.com/csv/by_year/{year}.csv'

  return url


def get_db_conn() -> str:

  connection = f"""
    ATTACH
    'dbname={env["PG_DBNAME"]} user={env["PG_USERNAME"]} password={env["PG_PASSWORD"]} host=postgres_db port={env["PG_PORT"]}'
    AS postgres_db (TYPE POSTGRES)
  """

  return connection


def get_noaa_pivot_case_statements(element_list: List[Tuple]) -> str:
  
  case_statements = [
    f"sum(case when \"element\" = '{element}' then data_value * {multiplier} end) AS {column_name}"
    for element, column_name, multiplier in element_list
  ]

  return ', '.join(case_statements)

