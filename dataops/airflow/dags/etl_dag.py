from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import pandas as pd
import io

from etl_utils import extract_data, load_data

with DAG(
    dag_id="simple_adventureworks111",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["bt4301", "etl", "test"],
) as dag:

    @task
    def extract_task():
        df = extract_data()
        return df.to_json(orient="records")

    @task
    def load_task(records_json):
        df = pd.read_json(io.StringIO(records_json), orient="records")
        load_data(df)

    extracted_data = extract_task()

    load_task(extracted_data)