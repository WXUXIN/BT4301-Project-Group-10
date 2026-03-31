from airflow import DAG
from airflow.decorators import task
from datetime import datetime
from sqlalchemy import create_engine
import os

from customer_churn_etl_functions import (
    DATAWAREHOUSE_DB,
    get_period_date_range,
    get_max_period,
    extract_customer_churn_batch,
    transform_dim_customer,
    transform_dim_account,
    transform_dim_service,
    transform_dim_behavior,
    transform_fact_customer_churn,
    load_new_dimension_rows,
    load_new_fact_rows
)

COUNTER_FILE = "/tmp/airflow_churn_period_counter.txt"


# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id="customer_churn_incremental_etl",
    start_date=datetime(2026, 3, 1),
    schedule="*/1 * * * *",   # every 1 minute for testing, every run ingests 1 monthly period
    catchup=False,
    max_active_runs=1,
    tags=["bt4301", "etl", "customer_churn", "watermarking"],
) as dag:

    @task
    def get_current_period():
        """
        Read the current period from a local counter file.
        If the file does not exist, start at period 1.
        """
        if not os.path.exists(COUNTER_FILE):
            period = 1
        else:
            with open(COUNTER_FILE, "r") as f:
                period = int(f.read().strip()) + 1

        with open(COUNTER_FILE, "w") as f:
            f.write(str(period))

        print(f"Current period: {period}")
        return period

    @task
    def etl_process(period):
        """
        Perform extract, transform, and load for one monthly batch.
        Row-level fingerprints are generated at load time for each inserted
        warehouse row to establish the trusted watermark baseline.
        """
        max_period = get_max_period()

        # stop condition
        if period > max_period:
            print("All periods have already been processed.")
            return

        # create target engine once
        dwh_engine = create_engine(DATAWAREHOUSE_DB, echo=False)

        # get monthly date range
        start_date, end_date = get_period_date_range(period)
        print(f"Processing signup_date from {start_date} to {end_date}")

        # ==========================
        # EXTRACT
        # ==========================
        df_batch = extract_customer_churn_batch(start_date, end_date)

        if df_batch.empty:
            print("No rows found for this monthly period.")
            return

        print(f"Extracted {len(df_batch)} rows.")

        # ==========================
        # TRANSFORM
        # ==========================
        df_dim_customer = transform_dim_customer(df_batch)
        df_dim_account = transform_dim_account(df_batch)
        df_dim_service = transform_dim_service(df_batch)
        df_dim_behavior = transform_dim_behavior(df_batch)
        df_fact_churn = transform_fact_customer_churn(df_batch)

        # ==========================
        # LOAD DIMENSIONS + WATERMARKING
        # ==========================
        load_new_dimension_rows(
            df_dim_customer,
            "dim_customer",
            ["customer_id"],
            dwh_engine
        )

        load_new_dimension_rows(
            df_dim_account,
            "dim_account",
            ["customer_id"],
            dwh_engine
        )

        load_new_dimension_rows(
            df_dim_service,
            "dim_service",
            ["customer_id"],
            dwh_engine
        )

        load_new_dimension_rows(
            df_dim_behavior,
            "dim_behavior",
            ["customer_id"],
            dwh_engine
        )

        # ==========================
        # LOAD FACT + WATERMARKING
        # ==========================
        load_new_fact_rows(
            df_fact_churn,
            "fact_customer_churn",
            ["customer_id"],
            dwh_engine
        )

    period = get_current_period()
    etl_process(period)































# import sys
# from pathlib import Path
# from datetime import datetime, timedelta

# _DATAOPS_DIR = Path(__file__).resolve().parent.parent.parent  
# if str(_DATAOPS_DIR) not in sys.path:
#     sys.path.insert(0, str(_DATAOPS_DIR))

# from airflow import DAG
# from airflow.providers.standard.operators.python import PythonOperator

# from home_credit_etl_functions import (
#     DEFAULT_STAGING_DIR,
#     DATA_PATH,
#     extract,
#     transform,
#     load,
#     get_default_mysql_config,
# )

# STAGING_DIR = DEFAULT_STAGING_DIR
# LOAD_BATCH_SIZE = 10_000  
# LOAD_BATCH_DELAY_SECONDS = 0.5


# def run_extract(**context):
#     extract(data_path=DATA_PATH, staging_dir=STAGING_DIR)


# def run_transform(**context):
#     transform(staging_dir=STAGING_DIR)


# def run_load(**context):
#     load(
#         staging_dir=STAGING_DIR,
#         mysql_config=get_default_mysql_config(),
#         batch_size=LOAD_BATCH_SIZE,
#         batch_delay_seconds=LOAD_BATCH_DELAY_SECONDS,
#     )


# with DAG(
#     dag_id="home_credit_etl",
#     default_args={
#         "owner": "airflow",
#         "retries": 1,
#     },
#     description="Extract CSV -> Transform (clean) -> Load into home_credit MySQL in batches",
#     schedule='*/10 * * * *',
#     start_date=datetime(2026, 3, 15, 7, 48),
#     tags=["home_credit", "etl", "mysql"],
#     catchup=False,
# ) as dag:
#     task_extract = PythonOperator(
#         task_id="extract",
#         python_callable=run_extract,
#     )
#     task_transform = PythonOperator(
#         task_id="transform",
#         python_callable=run_transform,
#     )
#     task_load = PythonOperator(
#         task_id="load",
#         python_callable=run_load,
#     )

#     task_extract >> task_transform >> task_load
