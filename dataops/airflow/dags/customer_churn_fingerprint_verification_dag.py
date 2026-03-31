from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowException
from sqlalchemy import create_engine

from customer_churn_etl_functions import DATAWAREHOUSE_DB
from customer_churn_fingerprint_verification_functions import (
    TABLES_FINGERPRINT_AUDIT,
    verify_fingerprints_for_table,
)


with DAG(
    dag_id="customer_churn_fingerprint_verification",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["bt4301", "customer_churn", "watermark", "verification"],
) as dag:

    @task(task_id="verify_all_tables")
    def verify_all_tables():
        """
        For each table, re-hash business columns and compare to `row_fp`.
        """
        engine = create_engine(DATAWAREHOUSE_DB, echo=False)

        all_summaries = []
        failed = []

        for table in sorted(TABLES_FINGERPRINT_AUDIT):
            ok, summary = verify_fingerprints_for_table(engine, table)
            all_summaries.append(summary)
            print(f"[fingerprint] {summary}")
            if not ok:
                failed.append(summary)

        if failed:
            raise AirflowException(
                "Row fingerprint verification failed (stored row_fp does not match recomputed). "
                f"Details: {failed}"
            )

        return all_summaries

    verify_all_tables()
