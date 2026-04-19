from airflow import DAG
from airflow.decorators import task
from datetime import datetime
import os
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

COUNTER_FILE = "/tmp/airflow_churn_period_counter.txt"


# =========================================================
# DAG
# =========================================================
with DAG(
    dag_id="customer_churn_incremental_etl",
    start_date=datetime(2026, 3, 1),
    schedule="*/1 * * * *",  # every 1 minute for testing, every run ingests 1 monthly period
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
        # deferred imports: pandas, numpy, sqlalchemy loaded only at runtime
        from sqlalchemy import create_engine
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
            build_train_churn_model,
        )

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
            df_dim_customer, "dim_customer", ["customer_id"], dwh_engine
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
            status="success",
        )

        inserted_dim_account = load_new_dimension_rows(
            df_dim_account, "dim_account", ["customer_id"], dwh_engine
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
            status="success",
        )

        inserted_dim_service = load_new_dimension_rows(
            df_dim_service, "dim_service", ["customer_id"], dwh_engine
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
            status="success",
        )

        inserted_dim_behavior = load_new_dimension_rows(
            df_dim_behavior, "dim_behavior", ["customer_id"], dwh_engine
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
            status="success",
        )

        # ==========================
        # LOAD FACT + WATERMARKING
        # ==========================
        inserted_fact = load_new_fact_rows(
            df_fact_churn, "fact_customer_churn", ["customer_id"], dwh_engine
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
            status="success",
        )
        # ==========================
        # BUILD TRAINING DATASET
        # ==========================
        build_train_churn_model(dwh_engine)

    period = get_current_period()
    etl_done = etl_process(period)

    trigger_mlops = TriggerDagRunOperator(
        task_id="trigger_mlops_pipeline",
        trigger_dag_id="customer_churn_mlops_pipeline",
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    @task
    def sync_champion_if_promoted():
        from customer_churn_etl_functions import sync_champion_if_promoted as do_sync

        return do_sync()

    etl_done >> trigger_mlops >> sync_champion_if_promoted()