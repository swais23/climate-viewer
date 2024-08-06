from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import os
from sqlalchemy import create_engine, text
# NOTE: for local testing
from dotenv import load_dotenv

load_dotenv("./airflow-docker/.env")

env = {**os.environ}


engine = create_engine(f"postgresql://{env["PG_USERNAME"]}:{env["PG_PASSWORD"]}@localhost/{env["PG_DBNAME"]}")


def noaa_pivot_data(query: str, db_engine):
    with db_engine.connect() as connection:
        df = pd.read_sql_query(query, connection)
        print(df.head())


# NOTE: for local testing
if __name__ == "__main__":
    
    # with engine.connect() as connection:
    #     result = connection.execute(text("SELECT * FROM public.noaa_daily_raw LIMIT 10"))
    #     for row in result:
    #         print(row)
    noaa_pivot_data("SELECT * FROM public.noaa_daily_raw LIMIT 10", engine)


# PROBLEM
# Need SQLAlchemy version to be above 2.0 to work properly with Pandas, but seems that
# airflow needs a lower version of SQLAlchemy