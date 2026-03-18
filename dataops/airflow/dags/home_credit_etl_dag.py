import sys
from pathlib import Path
from datetime import datetime, timedelta

_DATAOPS_DIR = Path(__file__).resolve().parent.parent.parent  
if str(_DATAOPS_DIR) not in sys.path:
    sys.path.insert(0, str(_DATAOPS_DIR))

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from home_credit_etl_functions import (
    DEFAULT_STAGING_DIR,
    DATA_PATH,
    extract,
    transform,
    load,
    get_default_mysql_config,
)

STAGING_DIR = DEFAULT_STAGING_DIR
LOAD_BATCH_SIZE = 10_000  
LOAD_BATCH_DELAY_SECONDS = 0.5


def run_extract(**context):
    extract(data_path=DATA_PATH, staging_dir=STAGING_DIR)


def run_transform(**context):
    transform(staging_dir=STAGING_DIR)


def run_load(**context):
    load(
        staging_dir=STAGING_DIR,
        mysql_config=get_default_mysql_config(),
        batch_size=LOAD_BATCH_SIZE,
        batch_delay_seconds=LOAD_BATCH_DELAY_SECONDS,
    )


with DAG(
    dag_id="home_credit_etl",
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    description="Extract CSV -> Transform (clean) -> Load into home_credit MySQL in batches",
    schedule='*/10 * * * *',
    start_date=datetime(2026, 3, 15, 7, 48),
    tags=["home_credit", "etl", "mysql"],
    catchup=False,
) as dag:
    task_extract = PythonOperator(
        task_id="extract",
        python_callable=run_extract,
    )
    task_transform = PythonOperator(
        task_id="transform",
        python_callable=run_transform,
    )
    task_load = PythonOperator(
        task_id="load",
        python_callable=run_load,
    )

    task_extract >> task_transform >> task_load
