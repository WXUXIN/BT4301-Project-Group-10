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
    load_new_fact_rows,
    log_lineage,
    build_train_churn_model
)
from customer_churn_mlops_functions import (
    should_retrain,
    next_retrain_period,
    run_mlops_pipeline,
    log_training_to_db,
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

        etl_run_id = f"period_{period}_{datetime.now().strftime('%Y%m%d%H%M%S')}"
        dag_id = "customer_churn_incremental_etl"
        task_id = "etl_process"
        source_name = "customer_churn_1M.csv"
        source_type = "csv"

        # ==========================
        # EXTRACT
        # ==========================
        df_batch = extract_customer_churn_batch(start_date, end_date)

        if df_batch.empty:
            print("No rows found for this monthly period.")
            return

        extracted_rows = len(df_batch)
        print(f"Extracted {extracted_rows} rows.")

        # ==========================
        # TRANSFORM
        # ==========================
        df_dim_customer = transform_dim_customer(df_batch)
        df_dim_account = transform_dim_account(df_batch)
        df_dim_service = transform_dim_service(df_batch)
        df_dim_behavior = transform_dim_behavior(df_batch)
        df_fact_churn = transform_fact_customer_churn(df_batch)

        # ==========================
        # LOAD DIMENSIONS + WATERMARKING + LINEAGE
        # ==========================
        inserted_dim_customer = load_new_dimension_rows(
            df_dim_customer,
            "dim_customer",
            ["customer_id"],
            dwh_engine
        )

        log_lineage(
            dwh_engine=dwh_engine,
            etl_run_id=etl_run_id,
            dag_id=dag_id,
            task_id=task_id,
            source_name=source_name,
            source_type=source_type,
            target_table="dim_customer",
            transformation_name="transform_dim_customer",
            period=period,
            period_start_date=start_date.date(),
            period_end_date=end_date.date(),
            input_row_count=extracted_rows,
            output_row_count=len(df_dim_customer),
            rows_inserted=inserted_dim_customer,
            status="success"
        )

        inserted_dim_account = load_new_dimension_rows(
            df_dim_account,
            "dim_account",
            ["customer_id"],
            dwh_engine
        )

        log_lineage(
            dwh_engine=dwh_engine,
            etl_run_id=etl_run_id,
            dag_id=dag_id,
            task_id=task_id,
            source_name=source_name,
            source_type=source_type,
            target_table="dim_account",
            transformation_name="transform_dim_account",
            period=period,
            period_start_date=start_date.date(),
            period_end_date=end_date.date(),
            input_row_count=extracted_rows,
            output_row_count=len(df_dim_account),
            rows_inserted=inserted_dim_account,
            status="success"
        )

        inserted_dim_service = load_new_dimension_rows(
            df_dim_service,
            "dim_service",
            ["customer_id"],
            dwh_engine
        )

        log_lineage(
            dwh_engine=dwh_engine,
            etl_run_id=etl_run_id,
            dag_id=dag_id,
            task_id=task_id,
            source_name=source_name,
            source_type=source_type,
            target_table="dim_service",
            transformation_name="transform_dim_service",
            period=period,
            period_start_date=start_date.date(),
            period_end_date=end_date.date(),
            input_row_count=extracted_rows,
            output_row_count=len(df_dim_service),
            rows_inserted=inserted_dim_service,
            status="success"
        )

        inserted_dim_behavior = load_new_dimension_rows(
            df_dim_behavior,
            "dim_behavior",
            ["customer_id"],
            dwh_engine
        )

        log_lineage(
            dwh_engine=dwh_engine,
            etl_run_id=etl_run_id,
            dag_id=dag_id,
            task_id=task_id,
            source_name=source_name,
            source_type=source_type,
            target_table="dim_behavior",
            transformation_name="transform_dim_behavior",
            period=period,
            period_start_date=start_date.date(),
            period_end_date=end_date.date(),
            input_row_count=extracted_rows,
            output_row_count=len(df_dim_behavior),
            rows_inserted=inserted_dim_behavior,
            status="success"
        )

        # ==========================
        # LOAD FACT + WATERMARKING
        # ==========================
        inserted_fact = load_new_fact_rows(
            df_fact_churn,
            "fact_customer_churn",
            ["customer_id"],
            dwh_engine
        )

        log_lineage(
            dwh_engine=dwh_engine,
            etl_run_id=etl_run_id,
            dag_id=dag_id,
            task_id=task_id,
            source_name=source_name,
            source_type=source_type,
            target_table="fact_customer_churn",
            transformation_name="transform_fact_customer_churn",
            period=period,
            period_start_date=start_date.date(),
            period_end_date=end_date.date(),
            input_row_count=extracted_rows,
            output_row_count=len(df_fact_churn),
            rows_inserted=inserted_fact,
            status="success"
        )
        # ==========================
        # BUILD TRAINING DATASET
        # ==========================
        build_train_churn_model(dwh_engine)

        # Return period so the downstream MLOps task automatically
        # depends on this task completing first.
        return period

    @task
    def trigger_model_training(period):
        """
        Decide whether to retrain the XGBoost model based on the current period.

        Retraining schedule: period 1 (initial), then 4, 7, 10, …
        (every 3 ingested periods).  This task always runs after etl_process
        because it receives `period` as the output of that task — Airflow
        uses this data-flow dependency to enforce execution order.

        When retraining is triggered:
          1. run_mlops_pipeline loads the full train_churn_model table,
             trains the pipeline, and logs everything to MLflow.
          2. The new model version is compared against the current champion
             and promoted if its ROC AUC is higher.
          3. An audit record is written to mlops_training_log in MySQL.
        """
        if not should_retrain(period):
            next_p = next_retrain_period(period)
            print(
                f"[MLOps] Period {period}: retraining not scheduled. "
                f"Next retraining at period {next_p}."
            )
            return

        print(f"[MLOps] Period {period}: retraining triggered.")
        dwh_engine = create_engine(DATAWAREHOUSE_DB, echo=False)
        run_id, metrics = run_mlops_pipeline(period, dwh_engine)
        print(
            f"[MLOps] Training complete for period {period}. "
            f"ROC AUC={metrics['roc_auc']:.4f}  |  run_id={run_id}"
        )

    period = get_current_period()
    trained_period = etl_process(period)
    trigger_model_training(trained_period)































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
