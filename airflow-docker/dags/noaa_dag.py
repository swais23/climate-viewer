import os
import csv
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# from operators.noaa_csv import get_csv_date

# for testing
# def get_date(**kwargs):
#   return kwargs['ds_nodash']

def get_csv_date(**kwargs):

  file_path = f"/opt/airflow/data/{kwargs['file_name']}"

  with requests.get(f"https://noaa-ghcn-pds.s3.amazonaws.com/csv/by_year/{kwargs['year']}.csv", stream=True) as r:
    with open(file_path, 'w', newline='') as f:

      r_decoded = (line.decode('utf-8') for line in r.iter_lines())
      reader = csv.reader(r_decoded, delimiter=',')
      writer = csv.writer(f, delimiter=',')

      for idx, row in enumerate(reader):
        if idx == 0 or row[1] == kwargs['ds_nodash']:
          writer.writerow(row)

  return file_path


with DAG(
    dag_id="noaa_dag",
    start_date=datetime(2024, 1, 1),
    schedule="0 0 * * *",
    is_paused_upon_creation=True,
    catchup=False,
    tags=['noaa']
) as dag:
    get_csv_date = PythonOperator(
      task_id="get_csv_date",
      python_callable=get_csv_date,
      provide_context=True,
      op_kwargs={'year': 2024, 'file_name': 'test.csv'}
    )